/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2008-2019 SciDB, Inc.
* All Rights Reserved.
*
* SciDB is free software: you can redistribute it and/or modify
* it under the terms of the AFFERO GNU General Public License as published by
* the Free Software Foundation.
*
* SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the AFFERO GNU General Public License for the complete license terms.
*
* You should have received a copy of the AFFERO GNU General Public License
* along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
*
* END_COPYRIGHT
*/


// local
#include "DLAErrors.h"

// This module
#include <dlaScaLA/scalapackEmulation/scalapackEmulation.hpp>
#include <dlaScaLA/slaving/pdgesvdMaster.hpp>
#include <dlaScaLA/slaving/pdgesvdSlave.hpp>
#include <scalapackUtil/ScaLAPACKLogical.hpp> // for checkScaLAPACKPhysicalInputs()
#include <scalapackUtil/ScaLAPACKPhysical.hpp>
#include <scalapackUtil/reformat.hpp>
#include <scalapackUtil/scalapackFromCpp.hpp>

// This project
#include <array/MemArray.h>
#include <array/OpArray.h>
#include <array/SynchableArray.h>
#include <query/AutochunkFixer.h>
#include <query/Expression.h>
#include <query/Query.h>
#include <system/Exceptions.h>
#include <system/Utils.h>
#include <util/shm/SharedMemoryIpc.h>

// external utilities
#include <log4cxx/logger.h>

// de-facto standards
#include <boost/shared_array.hpp>

//
// NOTE: code sections marked REFACTOR are being identified as
//       candidates to be moved into MPIOperator and ScaLAPACKOperator
//       base classes.  This is one of the scheduled items for
//       DLA/ScaLAPACK milestone D (Cheshire milestone 4 timeframe)
//

namespace scidb
{
using std::numeric_limits;
using std::string;

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.libmath.ops.gesvd"));


static const bool DBG = false;

/**
 *  A Physical SVD operator implemented using ScaLAPACK
 *
 *  @note The interesting work is done in invokeMPI(), called by execute()
 */
class SVDPhysical : public ScaLAPACKPhysical
{
public:
    SVDPhysical(const std::string& logicalName, const std::string& physicalName, const Parameters& parameters, const ArrayDesc& schema)
    :
        ScaLAPACKPhysical(logicalName, physicalName, parameters, schema,
                          RuleNotHigherThanWide) // see NOTE below
    {
        // NOTE:
        // its critical that the last argument to ScaLAPACKPhysical is the process
        // grid rule 'NotHigherThanWide'.
        //
        // Due to the way the ScaLAPACK algorithm calculates the singular values independently
        // at each processor, if the calculation for a matrix that is taller-than-wide
        // is distributed over more processes vertically than horizontally, it may
        // may calculate different singular values in different processes.
        // By choosing this rule, we make sure the process grid is no taller than
        // square, and that seems to prevent the problem from occuring.
        //
        // If the problem does occur, scaLAPACK returns INFO equal to min(M,N)+1
        // and an exception about results that could not be guaranteed accurate were discarded.
        // There is no known work-around by the user.
    }
    std::shared_ptr<Array> invokeMPI(std::vector< std::shared_ptr<Array> >& inputArrays,
                                std::shared_ptr<Query>& query,
                                std::string& whichMatrix,
                                ArrayDesc& outSchema);

    virtual std::shared_ptr<Array> execute(std::vector< std::shared_ptr<Array> >& inputArrays, std::shared_ptr<Query> query);

private:
    bool        producesU(std::string& whichMatrix) const {
                    return whichMatrix == "U" || whichMatrix == "left";
                }
    bool        producesVT(std::string& whichMatrix) const {
                    return whichMatrix == "VT" || whichMatrix == "right";
                }
    bool        producesSigma(std::string& whichMatrix) const {
                    return whichMatrix == "S" || whichMatrix == "SIGMA" || whichMatrix == "values";
                }
};




// TODO: fix GEMMPhysical.cpp as well and factor this nicely
slpp::int_t upToMultiple(slpp::int_t size, slpp::int_t blocksize)
{
    return (size+blocksize-1)/blocksize * blocksize;
}

std::shared_ptr<Array>  SVDPhysical::invokeMPI(std::vector< std::shared_ptr<Array> >& inputArrays,
                                          std::shared_ptr<Query>& query,
                                          std::string& whichMatrix,
                                          ArrayDesc& outSchema)
{
    //
    // Everything about the execute() method concerning the MPI execution of the arrays
    // is factored into this method.  This does not include the re-distribution of data
    // chunks into the ScaLAPACK distribution scheme, as the supplied redistributedInputs
    // must already be in that scheme.
    //
    // + intersects the array chunkGrids with the maximum process grid
    // + sets up the ScaLAPACK grid accordingly and if not participating, return early
    // + start and connect to an MPI slave process
    // + create ScaLAPACK descriptors for the input arrays
    // + convert the redistributedInputs into in-memory ScaLAPACK layout in shared memory
    // + call a "master" routine that passes the ScaLAPACK operator name, parameters,
    //   and shared memory descriptors to the ScaLAPACK MPI process that will do the
    //   actual computation.
    // + wait for successful completion
    // + construct an "OpArray" that make and Array API view of the output memory.
    // + return that output array.
    //
    LOG4CXX_TRACE(logger, "SVDPhysical::invokeMPI() begin");

    //
    // get dimension information about the input array(s)
    //
    std::shared_ptr<Array> arrayA = inputArrays[0];

    // find M,N from input array
    LOG4CXX_DEBUG(logger, "SVDPhysical::invokeMPI() nRow(A)=" <<  nRow(arrayA));
    LOG4CXX_DEBUG(logger, "SVDPhysical::invokeMPI() nCol(A)=" <<  nCol(arrayA));
    slpp::int_t M = slpp::int_cast(nRow(arrayA));
    slpp::int_t N = slpp::int_cast(nCol(arrayA));

    // find MB,NB from input array, which is the chunk size
    const slpp::int_t MB= chunkRow(arrayA);
    const slpp::int_t NB= chunkCol(arrayA);

    checkInputArray(arrayA);

    //
    //.... Set up ScaLAPACK array descriptors ........................................
    //


    //
    // Initialize the (emulated) BLACS and get the proces grid info
    //
    slpp::context_t blacsContext = doBlacsInit(inputArrays, query, "SVDPhysical");
    bool isParticipatingInScaLAPACK = blacsContext.isParticipating();
    if (isParticipatingInScaLAPACK) {
        checkBlacsInfo(query, blacsContext, "SVDPhysical");
    }

    slpp::int_t NPROW=-1, NPCOL=-1, MYPROW=-1 , MYPCOL=-1 ;
    scidb_blacs_gridinfo_(blacsContext, NPROW, NPCOL, MYPROW, MYPCOL);

    LOG4CXX_DEBUG(logger, "SVDPhysical::invokeMPI() NPROW="<<NPROW<<", NPCOL="<<NPCOL);
    // SDB-5704
    // if the A matrix has
    //     M (height) less than MB*NPROW or
    //     N (width) less  than NB*NPCOL
    // then the compute grid is not a perfect match for the matrix
    // and we have to improve the grid generation
    // This is starting to occurr when people use large chunk sizes for skinny problems
    // For the moment, we'll just add some warnings.
    if(M < MB * NPROW) {
        LOG4CXX_WARN(logger, "SVDPhysical::invokeMPI() SVD matrix shorter than process grid");
    }
    if(N < NB * NPCOL) {
        LOG4CXX_WARN(logger, "SVDPhysical::invokeMPI() SVD matrix narrower than process grid");
    }

    //
    // launch MPISlave if we participate
    // TODO: move this down into the ScaLAPACK code ... something that does
    //       the doBlacsInit, launchMPISlaves, and the check that they agree
    //
    bool isParticipatingInMPI = launchMPISlaves(query, NPROW*NPCOL);
    if (isParticipatingInScaLAPACK != isParticipatingInMPI) {
        LOG4CXX_DEBUG(logger, "SVDPhysical::invokeMPI():"
                              << " isParticipatingInScaLAPACK " << isParticipatingInScaLAPACK
                              << " isParticipatingInMPI " << isParticipatingInMPI);
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED)
                   << "SVDPhysical::invokeMPI(): internal inconsistency in MPI slave launch.");
    }

    if (isParticipatingInMPI) {
        LOG4CXX_DEBUG(logger, "SVDPhysical::invokeMPI(): participating in MPI");
    } else {
        LOG4CXX_WARN(logger, "SVDPhysical::invokeMPI(): not participating in MPI, check chunksize vs grid for potential performance gain");

        std::shared_ptr<Array> tmpRedistedInput = redistributeInputArray(inputArrays[0],
                                                                         outSchema.getDistribution(),
                                                                         query, "SVDPhysical");
        bool wasConverted = (tmpRedistedInput != inputArrays[0]) ;  // only when redistribute was actually done (sometimes optimized away)
        if (wasConverted) {
            SynchableArray* syncArray = safe_dynamic_cast<SynchableArray*>(tmpRedistedInput.get());
            syncArray->sync();
        }
        // free potentially large amount of memory
        inputArrays[0].reset();
        tmpRedistedInput.reset();

        unlaunchMPISlavesNonParticipating();
        return std::shared_ptr<Array>(new MemArray(outSchema,query));
    }

    //
    //.... Set up ScaLAPACK array descriptors ........................................
    //

    // these formulas for LLD (loacal leading dimension) and LTD (local trailing dimension)
    // are found in the headers of the scalapack functions such as pdgesvd_()
    // TODO: factor these formulae
    const slpp::int_t one = 1 ;
    slpp::int_t MIN_MN = std::min(M,N);

    const slpp::int_t RSRC = 0 ;
    const slpp::int_t CSRC = 0 ;
    // LLD(A)
    slpp::int_t LLD_A = std::max(one, scidb_numroc_( M, MB, MYPROW, RSRC, NPROW ));
    if(DBG) std::cerr << "M:"<<M <<" MB:"<<MB << " MYPROW:"<<MYPROW << " NPROW:"<< NPROW << std::endl;
    if(DBG) std::cerr << "--> LLD_A = " << LLD_A << std::endl;

    // LLD(U)
    slpp::int_t LLD_U = LLD_A;

    // LLD(VT)
    slpp::int_t LLD_VT = std::max(one, scidb_numroc_( MIN_MN, MB, MYPROW, RSRC, NPROW ));
    if(DBG) std::cerr << "MIN_MN:"<<MIN_MN <<" MB:"<<MB << " MYPROW:"<<MYPROW << " NPROW:"<< NPROW << std::endl;
    if(DBG) std::cerr << "-->LLD_VT = " << LLD_VT << std::endl;

    // LTD(A)
    slpp::int_t LTD_A = std::max(one, scidb_numroc_( N, NB, MYPCOL, CSRC, NPCOL ));
    if(DBG) std::cerr << "N:"<<N <<" NB:"<<NB << " MYPCOL:"<<MYPCOL << " NPCOL:"<<NPCOL<< std::endl;
    if(DBG) std::cerr << "-->LTD_A = " << LTD_A << std::endl;

     // LTD(U)
    slpp::int_t LTD_U = std::max(one, scidb_numroc_( MIN_MN, NB, MYPCOL, CSRC, NPCOL ));
    if(DBG) std::cerr << "MIN_MN:"<<MIN_MN <<" NB:"<<NB << " MYPCOL:"<<MYPCOL << " NPCOL:"<<NPCOL<< std::endl;
    if(DBG) std::cerr << "-->LTD_U = " << LTD_U << std::endl;


    // create ScaLAPACK array descriptors
    slpp::int_t descinitINFO = 0; // an output implemented as non-const ref (due to Fortran calling conventions)

    slpp::desc_t DESC_A;
    scidb_descinit_(DESC_A,  M, N,      MB, NB, 0, 0, blacsContext, LLD_A, descinitINFO);
    if (descinitINFO != 0) {
        LOG4CXX_ERROR(logger, "SVDPhysical::invokeMPI: scidb_descinit(DESC_A) failed, INFO " << descinitINFO
                                                                            << " DESC_A " << DESC_A);
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED) << "SVDPhysical::invokeMPI: scidb_descinit(DESC_A) failed");
    }
    LOG4CXX_DEBUG(logger, "SVDPhysical::invokeMPI(): DESC_A=" << DESC_A);

    slpp::desc_t DESC_U;
    scidb_descinit_(DESC_U,  M, MIN_MN, MB, NB, 0, 0, blacsContext, LLD_U, descinitINFO);
    if (descinitINFO != 0) {
        LOG4CXX_ERROR(logger, "SVDPhysical::invokeMPI: scidb_descinit(DESC_U) failed, INFO " << descinitINFO
                                                                            << " DESC_U " << DESC_U);
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED) << "scidb_descinit(DESC_U) failed");
    }
    LOG4CXX_DEBUG(logger, "SVDPhysical::invokeMPI(): DESC_U=" << DESC_U);

    slpp::desc_t DESC_VT;
    scidb_descinit_(DESC_VT, MIN_MN, N, MB, NB, 0, 0, blacsContext, LLD_VT, descinitINFO);
    if (descinitINFO != 0) {
        LOG4CXX_ERROR(logger, "SVDPhysical::invokeMPI: scidb_descinit(DESC_VT) failed, INFO " << descinitINFO
                                                                            << " DESC_VT " << DESC_VT);
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED) << "SVDPhysical::invokeMPI(): scidb_descinit(DESC_VT) failed");
    }
    LOG4CXX_DEBUG(logger, "SVDPhysical::invokeMPI(): DESC_VT=" << DESC_VT);

    slpp::desc_t DESC_S; // S is different: global, not distributed, so its LLD(S) == LEN(S) == MIN(M,N)
    scidb_descinit_(DESC_S,  MIN_MN, 1, MB, NB, 0, 0, blacsContext, MIN_MN, descinitINFO);
    if (descinitINFO != 0) {
        LOG4CXX_ERROR(logger, "SVDPhysical::invokeMPI: scidb_descinit(DESC_S) failed, INFO " << descinitINFO
                                                                            << " DESC_S " << DESC_S);
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED) << "SVDPhysical::invokeMPI(): descinit(DESC_S) failed");
    }
    LOG4CXX_DEBUG(logger, "SVDPhysical::invokeMPI(): DESC_S=" << DESC_S);

    //REFACTOR
    if(DBG) {
        std::cerr << "#### SVDPhysical::invokeMPI #########################################" << std::endl;
        std::cerr << "MB:" << MB << std::endl;
        std::cerr << "NB:" << NB << std::endl;
        std::cerr << "MYPROW:" << MYPROW << std::endl;
        std::cerr << "NPROW:" << NPROW << std::endl;
    }


    if(DBG) {
        std::cerr << "LOCAL SIZES:@@@@@@@@@@@@@@@@@@@" << std::endl ;
        std::cerr << "XX MIN_MN = " << MIN_MN << std::endl;
        std::cerr << "XX LLD_A   = " << LLD_A << std::endl;
        std::cerr << "XX LTD_U   = " << LTD_U << std::endl;
        std::cerr << "XX LLD_VT  = " << LLD_VT << std::endl;
        std::cerr << "XX LTD_A   = " << LTD_A << std::endl;
    }

    // local sizes
    const slpp::int_t SIZE_A  = LLD_A  * LTD_A ;
    const slpp::int_t SIZE_U  = LLD_A  * LTD_U ;
    const slpp::int_t SIZE_VT = LLD_VT * LTD_A ;

    //
    // Create IPC buffers
    //
    enum dummy {BUF_ARGS=0, BUF_MAT_A, BUF_MAT_S, BUF_MAT_U, BUF_MAT_VT, NUM_BUFS };
    size_t elemBytes[NUM_BUFS];
    size_t nElem[NUM_BUFS];
    std::string dbgNames[NUM_BUFS];

    elemBytes[BUF_ARGS] = 1 ; nElem[BUF_ARGS] = sizeof(scidb::PdgesvdArgs) ; dbgNames[BUF_ARGS] = "PdgesvdArgs";
    typedef scidb::SharedMemoryPtr<double> shmSharedPtr_t ;

    const size_t ALLOC_A = upToMultiple(SIZE_A, MB*NB);    // always the input

    // which outputs?
    const size_t ALLOC_S  = upToMultiple(MIN_MN, MB*NB);   // pdgesvd_() always produces sigma
    const size_t ALLOC_U  = producesU(whichMatrix)  ? upToMultiple(SIZE_U,  MB*NB) : 1 ;
    const size_t ALLOC_VT = producesVT(whichMatrix) ? upToMultiple(SIZE_VT, MB*NB) : 1 ;

    // size round ups may have occurred, check size limits again
    if(bufferTooLargeForScalapack(ALLOC_A)) {
        LOG4CXX_DEBUG(logger, "SVDPhysical::invokeMPI():" << " LLD_A: " << LLD_A
                                                        << " LTD_A: " << LTD_A);
        LOG4CXX_DEBUG(logger, "SVDPhysical::invokeMPI():"
                               << " ALLOC_A: " << ALLOC_A << " * 8 = " << 8*ALLOC_A
                               << " vs numeric_limit<slpp::int_t>::max() " << numeric_limits<slpp::int_t>::max());
        LOG4CXX_DEBUG(logger, "SVDPhysical::invokeMPI(): vs numeric_limits<slpp::int_t>::max()  " << numeric_limits<slpp::int_t>::max());
        throw (SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_OPERATION_FAILED) << "per-instance share of input matrix exceeds library limit");
    }
    if(bufferTooLargeForScalapack(ALLOC_S)) {
        throw (SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_OPERATION_FAILED) << "per-instance share of singular value storage exceeds library limit");
    }

    if(producesU(whichMatrix)  && bufferTooLargeForScalapack(ALLOC_U)) {
        throw (SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_OPERATION_FAILED) << "per-instance share of U matrix exceeds library limit");
    }
    if(producesVT(whichMatrix) && bufferTooLargeForScalapack(ALLOC_VT)) {
        throw (SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_OPERATION_FAILED) << "per-instance share of VT matrix exceeds library limit");
    }

    elemBytes[BUF_MAT_A] = sizeof(double) ; nElem[BUF_MAT_A] = ALLOC_A ; dbgNames[BUF_MAT_A] = "A" ;
    elemBytes[BUF_MAT_S] = sizeof(double) ; nElem[BUF_MAT_S] = ALLOC_S ; dbgNames[BUF_MAT_S] = "S" ;
    elemBytes[BUF_MAT_U] = sizeof(double) ; nElem[BUF_MAT_U] = ALLOC_U ; dbgNames[BUF_MAT_U] = "U" ;
    elemBytes[BUF_MAT_VT]= sizeof(double) ; nElem[BUF_MAT_VT]= ALLOC_VT; dbgNames[BUF_MAT_VT]= "VT" ;

    std::vector<MPIPhysical::SMIptr_t> shmIpc = allocateMPISharedMemory(NUM_BUFS, elemBytes, nElem, dbgNames);

    //
    // Zero inputs, to emulate a sparse matrix implementation (but slower)
    // and then extract the non-missing info onto that.
    // Set outputs to NaN, to catch invalid cells being returned

    void *argsBuf = shmIpc[BUF_ARGS]->get();
    double* A = reinterpret_cast<double*>(shmIpc[BUF_MAT_A]->get());
    double* S = reinterpret_cast<double*>(shmIpc[BUF_MAT_S]->get()); shmSharedPtr_t Sx(shmIpc[BUF_MAT_S]);
    double* U = reinterpret_cast<double*>(shmIpc[BUF_MAT_U]->get()); shmSharedPtr_t Ux(shmIpc[BUF_MAT_U]);
    double* VT = reinterpret_cast<double*>(shmIpc[BUF_MAT_VT]->get());shmSharedPtr_t VTx(shmIpc[BUF_MAT_VT]);

    std::shared_ptr<Array> tmpRedistedInput = redistributeInputArray(arrayA,
                                                                     outSchema.getDistribution(),
                                                                     query,
                                                                     "SVDPhysical");

    bool wasConverted = (tmpRedistedInput != arrayA) ;  // only when redistribute was actually done (sometimes optimized away)

    setInputMatrixToAlgebraDefault(A, nElem[BUF_MAT_A]);
    extractArrayToScaLAPACK(tmpRedistedInput, A, DESC_A, NPROW, NPCOL, MYPROW, MYPCOL, query);

    if (wasConverted) {
        SynchableArray* syncArray = safe_dynamic_cast<SynchableArray*>(tmpRedistedInput.get());
        syncArray->sync();
    }
    // free potentially large amount of memory
    inputArrays[0].reset();
    arrayA.reset();
    tmpRedistedInput.reset();

    // only bother clearing the output matrices we are going to use
    // TODO: even better, clear only the parts that might not be set by the svd computation,
    //       but that's a bit of work depending on whether the input matrix is
    //       over- or under-determined, things like that
    if (producesSigma(whichMatrix)) {
        setOutputMatrixToAlgebraDefault(S, nElem[BUF_MAT_S], logger);
    }
    if (producesU(whichMatrix)) {
        setOutputMatrixToAlgebraDefault(U, nElem[BUF_MAT_U], logger);
    }
    if (producesVT(whichMatrix)) {
        setOutputMatrixToAlgebraDefault(VT, nElem[BUF_MAT_VT], logger);
    }

    // debug that the reformat worked correctly:
    if(DBG) {
        LOG4CXX_DEBUG(logger, "SVDPhysical::invokeMPI: debug reformatted array start");
        for(int ii=0; ii < SIZE_A; ii++) {
            std::cerr << "("<< MYPROW << "," << MYPCOL << ") A["<<ii<<"] = " << A[ii] << std::endl;
        }
        LOG4CXX_DEBUG(logger, "SVDPhysical::invokeMPI: debug reformatted array end");
    }

    //
    //.... Call PDGESVD to compute the SVD of A .............................
    //
    LOG4CXX_DEBUG(logger, "SVDPhysical::invokeMPI: calling pdgesvdMaster M,N " << M << "," << N << "MB,NB:" << MB << "," << NB);
    std::shared_ptr<MpiSlaveProxy> slave = _ctx->getSlave(_launchId);
    slpp::int_t MYPE = slpp::int_cast(query->getInstanceID()) ;  // we map 1-to-1 between instanceID and MPI rank
    slpp::int_t INFO = DEFAULT_BAD_INFO ;
    pdgesvdMaster(query.get(), _ctx, slave, _ipcName, argsBuf,
                  NPROW, NPCOL, MYPROW, MYPCOL, MYPE,
                  producesU(whichMatrix)  ? 'V' : 'N',
                  producesVT(whichMatrix) ? 'V' : 'N',
                  M, N,
                  A,  one, one, DESC_A, S,
                  U,  one, one, DESC_U,
                  VT, one, one, DESC_VT,
                  INFO);

    std::string operatorName("pdgesvd");
    if (INFO == (std::min(M,N)+1)) {
        // special error case diagnostic specific to pdgesvd complaining of
        // eigenvalue heterogeneity.
        // Only cure known so far is to distribute computation to fewer processes, which is
        // being done already by the RuleNotHigherThanWide option to the ScaLAPACKPhysical ctor.
        // We do not know of a user-level workaround at the time this was written.
        // Additional study of the ScaLAPACK SVD algorithm would be required.
        std::stringstream ss;
        ss << operatorName << "() ";
        ss << "runtime error " << INFO ;
        ss << "SVD results could not be guaranteed to be accurate. Please report this error if it occurs.";
        throw (SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_OPERATION_FAILED) << ss.str());
    } else if (INFO > 0) {
        // special error case diagnostic specific to pdgesvd
        std::stringstream ss;
        ss << operatorName << "() ";
        ss << "runtime error " << INFO ;
        ss << " DBDSQR did not converge ";
        throw (SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_OPERATION_FAILED) << ss.str());
    } else {
        raiseIfBadResultInfo(INFO, "pdgesvd");
    }

    boost::shared_array<char> resPtrDummy(reinterpret_cast<char*>(NULL));
    typedef scidb::ReformatFromScalapack<shmSharedPtr_t> reformatOp_t ;

    std::shared_ptr<Array> result;
    size_t resultShmIpcIndx = shmIpc.size(); // by default, we will not hold onto any ShmIpc for a result,
                                             // modify this if we determine we have output data, below.
    if (producesSigma(whichMatrix))
    {
        if(DBG) std::cerr << "sequential values from 'value/S' memory" << std::endl;
        for(int ii=0; ii < MIN_MN; ii++) {
            if(DBG) std::cerr << "S["<<ii<<"] = " << S[ii] << std::endl;
        }

        if(DBG) std::cerr << "using pdelgetOp to reformat svd vals from memory to scidb array , start" << std::endl ;
        // TODO JHM ;
        // Check that we have any singluar values to output
        // this means checking for non-zeros in S or some other
        // indicator from the pdgesvd
        // if (myLen == 0)
        //     return std::shared_ptr<Array>(new MemArray(outSchema));
        //

        // NOTE:
        // The S output vector is not a distributed array like A, U, &VT are
        // The entire S vector is returned on every instance, so we have to make
        // sure that only one instance does the work of converting any particular
        // chunk.
        // To manage this we introduced the "global" flag to the pdelgetOp operator
        // which is also then able to avoid actually the significant overhead of
        // the ScaLAPACK SPMD pdelget() and instead simply subscripts the array.
        //
        // We treat the S vector, as mathematicians do, as a column vector, as this
        // stays consistent with ScaLAPACK.
        // TODO: move this case after U and VT, as it is easier to understand after
        //       understanding them, without having to also understand
        //       the special cases of output from global data and as a vector

        //
        // an OpArray is a SplitArray that is filled on-the-fly by calling the operator
        // so all we have to do is create one with an upper-left corner equal to the
        // global position of the first local block we have.  so we need to map
        // our "processor" coordinate into that position, which we do by multiplying
        // by the chunkSize
        //
        Dimensions const& dimsS = outSchema.getDimensions();

        Coordinates first(1);
        first[0] = dimsS[0].getStartMin() + MYPROW * dimsS[0].getChunkInterval();

        Coordinates last(1);
        last[0] = dimsS[0].getStartMin() + MIN_MN - 1;

        // the process grid may be larger than the size of output in chunks...
        // e.g gesvd(<1x40 matrix>, 'U') results in a 1x1 result from only one process,
        // even though all processes on which the 40 columns are distributed have to participate in the
        // calculation
        // first coordinate for the first chunk that my instance handles
        bool isParticipatingInOutput = first[0] <= last[0];

        // unlike the U and VT matrices, which are distributed in ScaLAPACK, the S vector is *replicated* on
        // every ScaLAPACK processing grid column.
        // If all instances return their copy of it, there will be too much data returned.
        // Therefore we further restrict OpArray to take data from only a single column of the ScaLAPACK processor grid.
        // That is sufficient, and any more produces an error
        isParticipatingInOutput = isParticipatingInOutput && (MYPCOL==0) ;

        if(isParticipatingInOutput) {
            Coordinates iterDelta(1);
            iterDelta[0] = NPROW * dimsS[0].getChunkInterval();

            LOG4CXX_DEBUG(logger, "SVDPhysical::invokeMPI(S): Creating OpArray from "<<first[0]<<" to "<<last[0]<<" delta "<<iterDelta[0]);
            reformatOp_t      pdelgetOp(Sx, DESC_S, dimsS[0].getStartMin(), 0,
                                        NPROW, NPCOL, MYPROW, MYPCOL, /*isGlobal*/true);
            result = std::shared_ptr<Array>(new OpArray<reformatOp_t>(outSchema, resPtrDummy, pdelgetOp,
                                                                 first, last, iterDelta, query));
            resultShmIpcIndx = BUF_MAT_S; // this ShmIpc memory cannot be released at the end of the method
        } else {
            // In this case, instance corresponds to one of the ScaLAPACK processor grid columns after the first one.
            // returning any data from this copy of the S vector is not compatible with SplitArray from which
            // OpArray is derived.
            // (note that these nodes still participated in the global generation of the S vector, it is merely that
            //  the ScaLAPACK algorithm produces replicas of it due to the way the algorithm works).
            LOG4CXX_DEBUG(logger, "SVDPhysical::invokeMPI(SIGMA): at process grid ("<<MYPROW<<","<<MYPCOL<<") Creating empty MemArray");
            result = std::shared_ptr<Array>(new MemArray(outSchema,query)); // empty array to return
            assert(resultShmIpcIndx == shmIpc.size());
        }
    }

    else if (producesU(whichMatrix))
    {
        if(DBG) std::cerr << "--------------------------------------" << std::endl;
        if(DBG) std::cerr << "sequential values from 'left/U' memory" << std::endl;
        for(int ii=0; ii < SIZE_U; ii++) {
            if(DBG) std::cerr << "U["<<ii<<"] = " << U[ii] << std::endl;
        }
        if(DBG) std::cerr << "--------------------------------------" << std::endl;
        if(DBG) std::cerr << "using pdelgetOp to reformat svd left from memory to scidb array , start" << std::endl ;
        //
        // an OpArray is a SplitArray that is filled on-the-fly by calling the operator
        // so all we have to do is create one with an upper-left corner equal to the
        // global position of the first local block we have.  so we need to map
        // our "processor" coordinate into that position, which we do by multiplying
        // by the chunkSize
        //
        Dimensions const& dimsU = outSchema.getDimensions();

        Coordinates first(2);
        first[0] = dimsU[0].getStartMin() + MYPROW * dimsU[0].getChunkInterval();
        first[1] = dimsU[1].getStartMin() + MYPCOL * dimsU[1].getChunkInterval();

        Coordinates last(2);
        last[0] = dimsU[0].getStartMin() + dimsU[0].getLength() - 1;
        last[1] = dimsU[1].getStartMin() + dimsU[1].getLength() - 1;

        // the process grid may be larger than the size of output in chunks...
        // e.g gesvd(<1x40 matrix>, 'U') results in a 1x1 result from only one process,
        // even though all processes on which the 40 columns are distributed have to participate in the
        // calculation
        bool isParticipatingInOutput = first[0] <= last[0] && first[1] <= last[1] ;
        if (isParticipatingInOutput) {
            Coordinates iterDelta(2);
            iterDelta[0] = NPROW * dimsU[0].getChunkInterval();
            iterDelta[1] = NPCOL * dimsU[1].getChunkInterval();
            LOG4CXX_DEBUG(logger, "SVDPhysical::invokeMPI(U): Creating OpArray from ("<<first[0]<<","<<first[1]<<") to (" << last[0] <<"," <<last[1]<<") delta:"<<iterDelta[0]<<","<<iterDelta[1]);
            reformatOp_t      pdelgetOp(Ux, DESC_U, dimsU[0].getStartMin(), dimsU[1].getStartMin(),
                                        NPROW, NPCOL, MYPROW, MYPCOL);
            result = std::shared_ptr<Array>(new OpArray<reformatOp_t>(outSchema, resPtrDummy, pdelgetOp,
                                                                 first, last, iterDelta, query));
            resultShmIpcIndx = BUF_MAT_U;  // this ShmIpc memory cannot be released at the end of the method
        } else {
            LOG4CXX_DEBUG(logger, "SVDPhysical::invokeMPI(U): participated, but not in output array, creating empty output array: first ("<<first[0]<<","<<first[1]<<"), last(" << last[0] <<"," <<last[1]<<")");
            result = std::shared_ptr<Array>(new MemArray(outSchema,query));   // empty array to return
            assert(resultShmIpcIndx == shmIpc.size());
        }
    }

    else if (producesVT(whichMatrix))
    {
        if(DBG) std::cerr << "sequential values from 'right/VT' memory" << std::endl;
        for(int ii=0; ii < SIZE_VT; ii++) {
            if(DBG) std::cerr << "VT["<<ii<<"] = " << VT[ii] << std::endl;
        }

        if(DBG) std::cerr << "reformat ScaLAPACK svd right to scidb array , start" << std::endl ;

        // see corresponding comment (on OpArray) in prior else if clause
        Dimensions const& dimsVT = outSchema.getDimensions();

        Coordinates first(2);
        first[0] = dimsVT[0].getStartMin() + MYPROW * dimsVT[0].getChunkInterval();
        first[1] = dimsVT[1].getStartMin() + MYPCOL * dimsVT[1].getChunkInterval();

        // TODO JHM ; clean up use of last
        Coordinates last(2);
        last[0] = dimsVT[0].getStartMin() + dimsVT[0].getLength() - 1;
        last[1] = dimsVT[1].getStartMin() + dimsVT[1].getLength() - 1;

        // the process grid may be larger than the size of output in chunks...
        // see comment in the 'U' case above for an example.
        bool isParticipatingInOutput = first[0] <= last[0] && first[1] <= last[1] ;
        if (isParticipatingInOutput) {
            Coordinates iterDelta(2);
            iterDelta[0] = NPROW * dimsVT[0].getChunkInterval();
            iterDelta[1] = NPCOL * dimsVT[1].getChunkInterval();
            LOG4CXX_DEBUG(logger, "SVDPhysical::invokeMPI(VT): Creating OpArray from ("<<first[0]<<","<<first[1]<<") to (" << last[0] <<"," <<last[1]<<") delta:"<<iterDelta[0]<<","<<iterDelta[1]);
            reformatOp_t    pdelgetOp(VTx, DESC_VT, dimsVT[0].getStartMin(), dimsVT[1].getStartMin(),
                                      NPROW, NPCOL, MYPROW, MYPCOL);
            result = std::shared_ptr<Array>(new OpArray<reformatOp_t>(outSchema, resPtrDummy, pdelgetOp,
                                                                 first, last, iterDelta, query));
            resultShmIpcIndx = BUF_MAT_VT; // this ShmIpc memory cannot be released at the end of the method
        } else {
            LOG4CXX_DEBUG(logger, "SVDPhysical::invokeMPI(VT): participated, but not in output array, creating empty output array: first ("<<first[0]<<","<<first[1]<<"), last(" << last[0] <<"," <<last[1]<<")");
            result = std::shared_ptr<Array>(new MemArray(outSchema,query));   // empty array to return
            assert(resultShmIpcIndx == shmIpc.size());
        }
    }

    // TODO: common pattern in ScaLAPACK operators: factor to base class
    releaseMPISharedMemoryInputs(shmIpc, resultShmIpcIndx);
    unlaunchMPISlaves();

    LOG4CXX_DEBUG(logger, "SVDPhysical::invokeMPI() end");

    return result;
}


std::shared_ptr<Array> SVDPhysical::execute(std::vector< std::shared_ptr<Array> >& inputArrays, std::shared_ptr<Query> query)
{
    //
    // + converts inputArrays to dtScaLAPACK distribution
    // + calls invokeMPI()
    // + returns the output OpArray.
    //
    const bool DBG = false ;
    LOG4CXX_TRACE(logger, "GEMMPhysical::execute(): begin");

    AutochunkFixer af(getControlCookie());
    af.fix(_schema, inputArrays);

    // Need to re-check intervals due to autochunking.  Too bad preSingleExecute doesn't let us see schemas.
    enum dummy { SINGLE_MATRIX = 1 };
    checkScaLAPACKPhysicalInputs(inputArrays, query, SINGLE_MATRIX, SINGLE_MATRIX);

    // before redistributing the inputs, lets make sure the matrix sizes won't overwhelm the ScaLAPACK integer size:
    // TODO: move this to logical operator as much as possible
    // TODO: factor this to ScaLAPACKPhysical::checkArrayInput() as much as possible
    LOG4CXX_DEBUG(logger, "SVDPhysical::execute(): numeric_limits<slpp::int_t>::max() " << numeric_limits<slpp::int_t>::max());
    procRowCol_t gridSize = getBlacsGridSize(inputArrays, query, "ScaLAPACKLogical"); // max number of rows and cols
    LOG4CXX_DEBUG(logger, "SVDPhysical::execute(): " << "gridSize.row: " << gridSize.row << ", gridSize.col: " << gridSize.col);

    { // indent matches up with gemm version until factored.
        const Dimensions& dims = inputArrays[0]->getArrayDesc().getDimensions();
        size_t maxLocalRows=std::max(size_t(1),
                                     scidb_numroc_max(dims[ROW].getLength(),
                                                      dims[ROW].getChunkInterval(),
                                                      gridSize.row));
        size_t maxLocalCols=std::max(size_t(1),
                                     scidb_numroc_max(dims[COL].getLength(),
                                                      dims[COL].getChunkInterval(),
                                                      gridSize.col));
        if(bufferTooLargeForScalapack(maxLocalRows * maxLocalCols)) {
            LOG4CXX_ERROR(logger, "SVDPhysical::execute(): array maxLocalRows: " << maxLocalRows << ", maxLocalCols: " << maxLocalCols);
            throw (SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_OPERATION_FAILED) << "per-instance  share of input matrix exceeds library size limit");
        } else {
            // DEBUG versions of error case loggin proved helpful for finding the max useable size for end-users
            LOG4CXX_DEBUG(logger, "SVDPhysical::execute(): array maxLocalRows: " << maxLocalRows << ", maxLocalCols: " << maxLocalCols);
        }
    }

    LOG4CXX_DEBUG(logger, "SVDPhysical::execute():"
                           << " chunksize (" << inputArrays[0]->getArrayDesc().getDimensions()[0].getChunkInterval()
                           << ", "           << inputArrays[0]->getArrayDesc().getDimensions()[1].getChunkInterval()
                           << ")");

    //
    // call invokeMPI()
    //
    string whichMatrix = ((std::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[0])->getExpression()->evaluate().getString();

    assert(query->getDefaultArrayResidency()->isEqual(_schema.getResidency()));
    assert(isScaLAPACK(_schema.getDistribution()->getDistType()));

    // invokeMPI does not manage an empty bitmap yet, but it is specified in _schema.
    // so to make it compatible, we first create a copy of _schema without the empty tag attribute
    Attributes attrsNoEmptyTag = _schema.getAttributes(true /*exclude empty bitmap*/);
    ArrayDesc schemaNoEmptyTag(_schema.getName(), attrsNoEmptyTag, _schema.getDimensions(),
                               _schema.getDistribution(),
                               _schema.getResidency());

    // and now invokeMPI produces an array without empty bitmap except when it is not participating
    std::shared_ptr<Array> arrayNoEmptyTag = invokeMPI(inputArrays, query, whichMatrix, schemaNoEmptyTag);

    // now we place a wrapper array around arrayNoEmptyTag, that adds a fake emptyTag (true everywhere)
    // but otherwise passes through requests for iterators on the other attributes.
    // And yes, the class name is the complete opposite of what it shold be.
    std::shared_ptr<Array> result;
    if (arrayNoEmptyTag->getArrayDesc().getEmptyBitmapAttribute() == NULL) {
        result = std::make_shared<NonEmptyableArray>(arrayNoEmptyTag);
    } else {
        result = arrayNoEmptyTag;
    }

    assert(query->getDefaultArrayResidency()->isEqual(result->getArrayDesc().getResidency()));
    assert(isScaLAPACK(result->getArrayDesc().getDistribution()->getDistType()));

    // return the scidb array
    Dimensions const& resultDims = result->getArrayDesc().getDimensions();
    if (whichMatrix == "values") {
        if(DBG) std::cerr << "returning result array size: " << resultDims[0].getLength() << std::endl ;
    } else if ( whichMatrix == "left" || whichMatrix == "right" ) {
        if(DBG) std::cerr << "returning result array size: " << resultDims[1].getLength() <<
                     "," << resultDims[0].getLength() << std::endl ;
    }

    if(DBG) std::cerr << "SVDPhysical::execute end ---------------------------------------" << std::endl;
    return result;
}

REGISTER_PHYSICAL_OPERATOR_FACTORY(SVDPhysical, "gesvd", "SVDPhysical");

} // namespace
