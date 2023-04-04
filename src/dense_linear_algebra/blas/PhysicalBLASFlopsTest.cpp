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

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("BLASFlopsTest"));


// TODO - have to find/start the "right" file/place for this, but not
//        at this time.
//        Providing headers for BLAS functions was explicitly excluded
//        from the scope of the MKL for 13.6.  (agreement with Bryan)
//        as there are higher priorities than working this out (e.g. multiply)
//        so for right now, we just make a local declaration and live with it.
namespace blas { typedef int32_t int_t; }
extern "C" {
    void dgemm_(const char& transa, const char& transb,
                const blas::int_t& m, const blas::int_t& n, const blas::int_t& k, const double& alpha,
                const double* a, const blas::int_t& lda,
                const double* b, const blas::int_t& ldb, const double& beta,
                      double* c, const blas::int_t& lbc);
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

/// for the overall documentation of this operator, see the BLASFlopsTestLogical
class PhysicalBLASFlopsTest: public PhysicalOperator
{
  public:
    PhysicalBLASFlopsTest(const std::string& logicalName, const std::string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    DistType inferSynthesizedDistType(std::vector<DistType> const& /*inDist*/, size_t /*depth*/) const override
    {
        DistType result =  _schema.getDistribution()->getDistType();
        LOG4CXX_TRACE(logger, "PhysicalBLASFlopsTest::inferSynthesizedDistType returning "
                              << distTypeToStr(result));
        return result;
    }

    std::shared_ptr<Array> execute(std::vector< std::shared_ptr<Array> >& inputArrays, std::shared_ptr<Query> query)
    {
        assert(inputArrays.size()==0);

        //
        // PART I, set up matrices A,B,C
        //
        const size_t size=4*KiB;   // memory: size squared is 16 M doubles, or 128MiB
                                   // time:   size cubed is 4096 G * 2 FLOP = 8192 GFLOP,

        // the following test problem, matA * matB, is designed to defeat some zero-checkin
        // code that evades the full computation in some BLASes.  This allows us to tell
        // the real rate.

        // Matrix A, all 7.0s
        std::vector<double> matA(size*size, 7.0);

        // Matrix B, 2.0 on the diagonal, near-zero elsewhere
        // (exact zero throws off FLOPS/sec with platform BLAS
        //  which has some kind of speedup for that case)
        std::vector<double> matB(size*size, 1e-300);
        for(size_t i=0; i < size*size; i+= size+1) {
            matB[i] = 2.0; // on the diagonal
        }

        // Matrix C, all 0.0
        std::vector<double> matC(size*size, 0.0);
        LOG4CXX_DEBUG(logger, "PhysicalBLASFlopsTest pre-dgemm_, inputs ready " << size*size*sizeof(double)/1e6 << "MB" );

        //
        // PART II, calculate dgemm
        //

        // NOTE: gemm computes C <- alpha A * B + beta C
        const blas::int_t n=size;
        const blas::int_t m=size;
        const blas::int_t k=size;
        double alpha = 1.0;
        double beta = 0.0;

        const double startSec = getTimeInSecs();
        const size_t reps = 1;
        for(size_t i=0; i<reps; i++) {
            dgemm_('N', 'N', m, n, k, alpha, matA.data(), /*lda*/m,
                                             matB.data(), /*ldb*/n,
                                       beta, matC.data(), /*ldc*/k);
        }
        const double takenSec = getTimeInSecs()-startSec;

        // calculate and log the performance, in case a rough estimate of BLAS performance is desired.
        double flops_per_rep = 2.0*n*m*k;
        LOG4CXX_DEBUG(logger, "PhysicalBLASFlopsTest "<<reps<<" reps of "<<flops_per_rep*1e-9<<" dgemm_ GFLOP each, in "<<takenSec);
        double flops_per_s = flops_per_rep*reps/takenSec;
        LOG4CXX_DEBUG(logger, "PhysicalBLASFlopsTest "<<flops_per_s*1e-9<<" dgemm_ GFLOP/s per instance");

        // Now check that the calculation is correct.
        // Matrix C should be exactly 2*A, because accumulated error
        // of near-zeros should be << 1/2 LSB of the result.
        for(size_t i=0; i < size*size; i++) { // access the diagonal
            if (matC[i] != 2*matA[i]) {
                LOG4CXX_ERROR(logger, "PhysicalBLASFlopsTest matA["<<i<<"]= "<<matA[i]<<", matC[] = "<<matC[i]);
                LOG4CXX_ERROR(logger, "PhysicalBLASFlopsTest, dgemm_ is not working");
                throw (SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_OPERATION_FAILED) << "no such GridSize rule");
                break;
            }
        }
        // the calculation was correct

        //
        // PART III return flops_per_s, in the instanceID row, and "dgemm" column
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

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalBLASFlopsTest, "_blasflopstest", "_blasflopstest_impl");

} // namespace scidb
