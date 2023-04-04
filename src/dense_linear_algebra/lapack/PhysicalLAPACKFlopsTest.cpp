/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2016-2019 SciDB, Inc.
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

#include <query/PhysicalOperator.h>
#include <array/MemArray.h>

#include <log4cxx/logger.h>

#include <algorithm>


using namespace std;
using namespace scidb;

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("LAPACKFlopsTest"));


// TODO - have to find/start the "right" file/place for this, but not
//        at this time.
//        Providing headers for LAPACK functions was explicitly excluded
//        from the scope of the MKL for 13.6.  (agreement with Bryan)
//        as there are higher priorities than working this out (e.g. multiply)
//        so for right now, we just make a local declaration and live with it.
namespace lpp { typedef int32_t int_t; }
extern "C" {
    // dgesvd_ lapack (not ScaLAPACK)
    void dgesvd_(const char &jobU, const char &jobVT,
                 const lpp::int_t& M, const lpp::int_t &N,
                 double *A, const lpp::int_t& lda,
                 double *S,
                 double *U,  const lpp::int_t& ldu,
                 double *VT, const lpp::int_t& ldvt,
                 double *WORK, const lpp::int_t &LWORK, lpp::int_t &INFO);
}

/// returns the current reading of CLOCK_REALTIME in seconds
/// (as double is more convenient than existing helper routines)
static double getTimeInSecs()
{
    struct timespec ts;
    if (clock_gettime(CLOCK_REALTIME, &ts) == -1) {
        int err = errno;
        stringstream ss;
        ss << "clock_gettime(CLOCK_REALTIME,...) failed, errno="<<err;
        assert(false);
        throw std::runtime_error(ss.str());
    }
    return (static_cast<double>(ts.tv_sec) + static_cast<double>(ts.tv_nsec) * 1e-9);
}

namespace scidb
{

/// for the overall documentation of this operator, see the LAPACKFlopsTestLogical
class PhysicalLAPACKFlopsTest: public PhysicalOperator
{
  public:
    PhysicalLAPACKFlopsTest(const std::string& logicalName, const std::string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    DistType inferSynthesizedDistType(std::vector<DistType> const& /*inDist*/, size_t /*depth*/) const override
    {
        DistType result =  _schema.getDistribution()->getDistType();
        LOG4CXX_TRACE(logger, "PhysicalLAPACKFlopsTest::inferSynthesizedDistType returning "
                              << distTypeToStr(result));
        return result;
    }

    std::shared_ptr<Array> execute(std::vector< std::shared_ptr<Array> >& inputArrays, std::shared_ptr<Query> query)
    {
        assert(inputArrays.size()==0);

        //
        // PART I, set up matrices A,B,C
        //
        const lpp::int_t size=1*KiB;   // memory: size squared is 1Mdoubles, or 8MiB
                                       // time:   size cubed is 1G * 29 * 2 FLOP = 58 GFLOP
                                       // so about 9s  on a Skylake @ 7 gesvd GFLOP/s

        // the following test problem, matA * matB, is designed to defeat some zero-checkin
        // code that evades the full computation in some LAPACKes.  This allows us to tell
        // the real rate.

        // Matrix A, alternating +/- 1
        // largest singular value should be size
        std::vector<double> matA(size*size, 0);
        for(size_t i=0; i < size; i++) {
            for(size_t j=0; j < size; j++) {
                matA[size*i+j] = (i+j)%2 ? 1 : -1;
            }
        }

        // Matrix S, result
        std::vector<double> vecS(size, 0);

        // Matrix U, result
        // Matrix VT, result
        std::vector<double> matU(size*size, 0);
        std::vector<double> matVT(size*size, 0);

        // WORK
        // for a square problem, |WORK| <= 5*size
        std::vector<double> vecWORK(5*size, 0);

        //
        // PART II, calculate dgesvd
        //
        const double startSec = getTimeInSecs();
        const size_t reps = 1;
        for(size_t i=0; i<reps; i++) {
            lpp::int_t INFO;
            dgesvd_('A', 'A',
                    size, size,
                    matA.data(), size,
                    vecS.data(),
                    matU.data(), size,
                    matVT.data(), size,
                    vecWORK.data(), 5*size, INFO);
            ASSERT_EXCEPTION(INFO==0, "dgesvd_ failed");
        }
        const double takenSec = getTimeInSecs()-startSec;

        // calculate and log the performance, in case a rough estimate of LAPACK performance is desired.
        double flops_per_rep = 29.0*size*size*size;
        LOG4CXX_INFO(logger, "PhysicalLAPACKFlopsTest "<<reps<<" reps of "<<flops_per_rep*1e-9<<" dgesvd_ GFLOP each, in "<<takenSec);
        double flops_per_s = flops_per_rep*reps/takenSec;
        LOG4CXX_INFO(logger, "PhysicalLAPACKFlopsTest "<<flops_per_s*1e-9<<" dgesvd_ GFLOP/s per instance");

        // Now check that the calculation is correct.
        // Matrix C should be exactly 2*A, because accumulated error
        // of near-zeros should be << 1/2 LSB of the result.
        if (fabs(vecS[0] - double(size)) > 1e-10) {
            LOG4CXX_ERROR(logger, "PhysicalLAPACKFlopsTest vecS[0] != size, ="<<vecS[0]);
            LOG4CXX_ERROR(logger, "PhysicalLAPACKFlopsTest fabs =" << fabs(vecS[0] - double(size)));
            throw (SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_OPERATION_FAILED)
                         << "_lapackflopstest not working correctly 1");
        }
        for(size_t i=1; i < size; i++) {
            if(vecS[i] > 1e-10) {        // the rest should be near zero
                LOG4CXX_ERROR(logger, "PhysicalLAPACKFlopsTest vecS[i]="<<vecS[i]);
                throw (SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_OPERATION_FAILED)
                             << "_lapackflopstest not working correctly 2");
            }
        }

        //
        // PART III return flops_per_s, in the instanceID row, and "dgesvd" column
        //
        std::shared_ptr<Array> outputArray(new MemArray(_schema, query));
        const auto& fda = outputArray->getArrayDesc().getAttributes().firstDataAttribute();
        std::shared_ptr<ArrayIterator> outputArrayIter = outputArray->getIterator(fda);

        Coordinates position(1); // 2 if we add columns of operators we are timing
        position[0] = query->getInstanceID();  // rows: instances
        // position[1] = 0;  // columns: NID "gemm" would be mapped to some TID_UINT64 column
        // but we'd need to add a mapping array as well

        // Create a chunk and get a ChunkIterator for it
        std::shared_ptr<ChunkIterator> outputChunkIter = outputArrayIter->newChunk(position).getIterator(query, 0);

        Value value;
        value.setDouble(flops_per_s);
        outputChunkIter->setPosition(position); // set cell position
        outputChunkIter->writeItem(value);
        outputChunkIter->flush();

        return outputArray;
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalLAPACKFlopsTest, "_lapackflopstest", "_lapackflopstest_impl");

} // namespace scidb
