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

/*
 * @file OptUnitTests.h
 */

#ifndef OPT_UNIT_TESTS_H_
#define OPT_UNIT_TESTS_H_

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <algorithm>
#include <memory>
#include <boost/filesystem.hpp>

#include <query/PhysicalOperator.h>
#include <query/Query.h>
#include <query/PhysicalQueryPlan.h>
#include <query/QueryProcessor.h>
#include <rbac/Session.h>
#include <rbac/SessionProperties.h>
#include <rbac/UserDesc.h>
#include <system/SystemCatalog.h>
#include <query/TypeSystem.h>
#include <system/Config.h>
#include <system/Cluster.h>

#include <util/compression/BuiltinCompressors.h>
#include "query/optimizer/HabilisOptimizer.h"

namespace scidb
{

class OptimizerTests: public CppUnit::TestFixture
{

    typedef std::shared_ptr<PhysicalOperator> PhysOpPtr;
    typedef std::shared_ptr<PhysicalQueryPlanNode> PhysNodePtr;

CPPUNIT_TEST_SUITE(OptimizerTests);
        CPPUNIT_TEST(testBoundaries);
        CPPUNIT_TEST(testBoundaries2);
        CPPUNIT_TEST(testBasic);
        CPPUNIT_TEST(testSubArrayReshapeSgInsertions);
        CPPUNIT_TEST(testInputSgInsert);
//        CPPUNIT_TEST(testTwoPhase);
//        CPPUNIT_TEST(testMultiply);
        CPPUNIT_TEST(testReplication);
    CPPUNIT_TEST_SUITE_END();

private:
    std::shared_ptr<QueryProcessor> _queryProcessor;
    std::shared_ptr<Optimizer> _habilisDisabled;
    std::shared_ptr<Optimizer> _habilis;

    ArrayDesc _dummyArray;
    Coordinates _dummyArrayStart;
    Coordinates _dummyArrayEnd;
    ArrayID _dummyArrayId;

    ArrayDesc _dummyShiftedArray;
    Coordinates _dummyShiftedArrayStart;
    Coordinates _dummyShiftedArrayEnd;
    ArrayID _dummyShiftedArrayId;

    ArrayDesc _smallArray;
    Coordinates _smallArrayStart;
    Coordinates _smallArrayEnd;
    ArrayID _smallArrayId;

    ArrayDesc _singleDim;
    Coordinates _singleDimStart;
    Coordinates _singleDimEnd;
    ArrayID _singleDimId;

    ArrayDesc _partiallyFilledArray;
    Coordinates _partiallyFilledStart;
    Coordinates _partiallyFilledEnd;
    ArrayID _partiallyFilledId;

    ArrayDesc _dummyFlippedArray;
    Coordinates _dummyFlippedStart;
    Coordinates _dummyFlippedEnd;
    ArrayID _dummyFlippedId;

    ArrayDesc _dummyReplicatedArray;
    ArrayID _dummyReplicatedArrayId;

public:

    static ArrayID s_addArray (const ArrayDesc & desc);

    inline static ArrayID s_addArray (const ArrayDesc & desc, const Coordinates & start, const Coordinates & end);

    void setUp();
    

    void tearDown();

    std::shared_ptr<Query> getQuery();

    std::shared_ptr<PhysicalPlan> generatePPlanFor(const char* queryString, bool afl=true, bool optEnable=true);

    int countDfNodes(std::shared_ptr<PhysicalPlan> pp);

    int countTotalNodes(PhysNodePtr node);

    //TODO: make this equivalence logic visible to the outside world? Overload operator==?
    bool equivalent(const AttributeDesc& lhs, const AttributeDesc& rhs);

    bool equivalent(const DimensionDesc& lhs, const DimensionDesc& rhs);

    bool equivalent(const ArrayDesc& lhs, const ArrayDesc& rhs);

    void testBoundariesHelper(int64_t dim0Start, int64_t dim0End, int64_t dim0Chunk, int64_t dim1Start, int64_t dim1End, int64_t dim1Chunk,
                              int64_t start0, int64_t start1, int64_t end0, int64_t end1, uint64_t expectedNumCells, uint64_t expectedNumChunks);

    void testBoundaries();

    void testBoundaries2();

    void testBasic();

    void testThreeInstanceSgInsert(const char* query, const char* opName);

    void testSubArrayReshapeSgInsertions();

    void testInputSgInsert();

    void testTwoPhase();

    void testMultiply();

    void testReplication();
};
CPPUNIT_TEST_SUITE_REGISTRATION(OptimizerTests);

}

#endif /* OPT_UNIT_TESTS_H_ */
