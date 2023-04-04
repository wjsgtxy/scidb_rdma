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
 * @file OptUnitTests.cpp
 */

#include <query/optimizer/OptUnitTests.h>

#include <fcntl.h>

namespace scidb
{



#define ASSERT_OPERATOR(instance, opName) (CPPUNIT_ASSERT( instance ->getPhysicalOperator()->getPhysicalName() == opName ))

    ArrayID OptimizerTests::s_addArray (const ArrayDesc & desc)
    {
        SystemCatalog* systemCat = SystemCatalog::getInstance();
        std::string nsName("public");
        if (systemCat->containsArray(nsName, desc.getName()))
        {
            systemCat->deleteArray(nsName, desc.getName());
        }
        ArrayDesc d2 = desc;

        ArrayID uAId = systemCat->getNextArrayId();

        d2.setIds(uAId, uAId, VersionID(0));
        systemCat->addArray(d2);
        return d2.getId();
    }

    ArrayID OptimizerTests::s_addArray (const ArrayDesc & desc, const Coordinates & start, const Coordinates & end)
    {
        ArrayID id = s_addArray(desc);
        SystemCatalog::getInstance()->updateArrayBoundariesAndIntervals(desc, PhysicalBoundaries(start, end));
        return id;
    }

    void OptimizerTests::setUp()
    {
        _queryProcessor = QueryProcessor::create();


        std::shared_ptr<const InstanceLiveness> liveness(Cluster::getInstance()->getInstanceLiveness());
        std::vector<InstanceID> instances(liveness->getNumLive());

        const scidb::InstanceLiveness::LiveInstances& liveInstances = liveness->getLiveInstances();
        size_t i=0;
        for (InstanceLivenessEntry const& entry : liveInstances) {
            instances[i++] = entry.getInstanceId();
        }
        ArrayResPtr residency = createDefaultResidency(PointerRange<InstanceID>(instances));

        //DUMMY
        Attributes dummyArrayAttributes;
        dummyArrayAttributes.push_back(AttributeDesc(
                                           "att0", TID_INT64, 0,
                                           CompressorType::NONE));
        dummyArrayAttributes.push_back(AttributeDesc(
                                           "att1", TID_INT64, 0,
                                           CompressorType::NONE));

        Dimensions dummyArrayDimensions;
        dummyArrayDimensions.push_back(DimensionDesc("x", 0, 0, 8, 9, 1, 0));
        dummyArrayDimensions.push_back(DimensionDesc("y", 0, 1, 9, 9, 1, 0));

        _dummyArray = ArrayDesc("opttest_dummy_array", dummyArrayAttributes, dummyArrayDimensions,
                                createDistribution(defaultDistType()), residency);
        _dummyArrayStart.push_back(0);
        _dummyArrayStart.push_back(1);
        _dummyArrayEnd.push_back(8);
        _dummyArrayEnd.push_back(9);
        _dummyArrayId = s_addArray(_dummyArray, _dummyArrayStart, _dummyArrayEnd);

        //DUMMY_SHIFTED
        Dimensions dummyShiftedArrayDimensions;
        dummyShiftedArrayDimensions.push_back(DimensionDesc("x", 5, 5, 12, 14, 1, 0));
        dummyShiftedArrayDimensions.push_back(DimensionDesc("y", 5, 6, 13, 14, 1, 0));

        _dummyShiftedArray = ArrayDesc("opttest_dummy_shifted_array", dummyArrayAttributes, dummyShiftedArrayDimensions,
                                       createDistribution(defaultDistType()), residency);
        _dummyShiftedArrayStart.push_back(5);
        _dummyShiftedArrayStart.push_back(6);
        _dummyShiftedArrayEnd.push_back(12);
        _dummyShiftedArrayEnd.push_back(13);
        _dummyShiftedArrayId = s_addArray(_dummyShiftedArray, _dummyShiftedArrayStart, _dummyShiftedArrayEnd);

        //SMALL
        Dimensions smallArrayDimensions;
        smallArrayDimensions.push_back(DimensionDesc("x", 0, 0, 0, 2, 1, 0));
        smallArrayDimensions.push_back(DimensionDesc("y", 0, 1, 2, 2, 1, 0));

        _smallArray = ArrayDesc("opttest_small_array", dummyArrayAttributes, smallArrayDimensions,
                                createDistribution(defaultDistType()), residency);
        _smallArrayStart.push_back(0);
        _smallArrayStart.push_back(1);
        _smallArrayEnd.push_back(0);
        _smallArrayEnd.push_back(2);
        _smallArrayId = s_addArray(_smallArray, _smallArrayStart, _smallArrayEnd);

        //SINGLEDIM
        Dimensions singleDimDimensions;
        singleDimDimensions.push_back(DimensionDesc("x", 0, 0, 3, 3, 1, 0));

        _singleDim = ArrayDesc("opttest_single_dim", dummyArrayAttributes, singleDimDimensions,
                               createDistribution(defaultDistType()), residency);
        _singleDimStart.push_back(0);
        _singleDimEnd.push_back(3);
        _singleDimId = s_addArray(_singleDim, _singleDimStart, _singleDimEnd);

        //PARTIALLYFILLED
        Dimensions partiallyFilledDimensions;
        partiallyFilledDimensions.push_back(DimensionDesc("x", 0, 0, 9, 9, 3, 0));
        partiallyFilledDimensions.push_back(DimensionDesc("y", 0, 0, 9, 9, 3, 0));

        _partiallyFilledArray = ArrayDesc("opttest_partially_filled", dummyArrayAttributes, partiallyFilledDimensions,
                                          createDistribution(defaultDistType()), residency);
        _partiallyFilledStart.push_back(0);
        _partiallyFilledStart.push_back(0);
        _partiallyFilledEnd.push_back(9);
        _partiallyFilledEnd.push_back(9);
        _partiallyFilledId = s_addArray(_partiallyFilledArray, _partiallyFilledStart, _partiallyFilledEnd);

        //DUMMYFLIPPED
        Dimensions dummyFlippedDimensions;
        dummyFlippedDimensions.push_back(DimensionDesc("att0", 0, 5, 1, 0));
        dummyFlippedDimensions.push_back(DimensionDesc("att1", 0, 5, 1, 0));

        Attributes dummyFlippedAttributes;
        dummyFlippedAttributes.push_back(
            AttributeDesc("x", TID_INT64, 0, CompressorType::NONE));
        dummyFlippedAttributes.push_back(
            AttributeDesc("y", TID_INT64, 0, CompressorType::NONE));
        dummyFlippedAttributes.addEmptyTagAttribute();

        _dummyFlippedArray = ArrayDesc("opttest_dummy_flipped", dummyFlippedAttributes, dummyFlippedDimensions,
                                       createDistribution(defaultDistType()), residency);
        _dummyFlippedId = s_addArray(_dummyFlippedArray);

        ArrayDistPtr replicatedDist = scidb::createDistribution(dtReplication);

        _dummyReplicatedArray = ArrayDesc("opttest_dummy_replicated_array", dummyArrayAttributes, dummyArrayDimensions,
                                          replicatedDist,
                                          residency);
        _dummyReplicatedArrayId = s_addArray(_dummyReplicatedArray,_dummyArrayStart, _dummyArrayEnd);

        ////////////////

        HabilisOptimizer *hopt = new HabilisOptimizer();
        hopt->_featureMask=0;
        _habilisDisabled = std::shared_ptr<Optimizer>(hopt);

        _habilis = std::shared_ptr<Optimizer>(new HabilisOptimizer());
    }

    void OptimizerTests::tearDown()
    {
        SystemCatalog* systemCat = SystemCatalog::getInstance();

        systemCat->deleteArray(_dummyArrayId);
        systemCat->deleteArray(_dummyShiftedArrayId);
        systemCat->deleteArray(_smallArrayId);
        systemCat->deleteArray(_singleDimId);
        systemCat->deleteArray(_partiallyFilledId);
        systemCat->deleteArray(_dummyFlippedId);
        systemCat->deleteArray(_dummyReplicatedArrayId);
    }

    std::shared_ptr<Query> OptimizerTests::getQuery()
    {
        std::shared_ptr<Query> result;
        std::shared_ptr<const InstanceLiveness> liveness(Cluster::getInstance()->getInstanceLiveness());
        int32_t longErrorCode = SCIDB_E_NO_ERROR;
        result = Query::createFakeQuery(INVALID_INSTANCE, 0, liveness, &longErrorCode);
        if (longErrorCode != SCIDB_E_NO_ERROR &&
            longErrorCode != SCIDB_LE_INVALID_FUNCTION_ARGUMENT) {
                // NetworkManger::createWorkQueue() may complain about null queue
                // we can ignore that error because NetworkManager is not used
            throw SYSTEM_EXCEPTION(SCIDB_LE_UNKNOWN_ERROR, longErrorCode);
        }
        UserDesc user(rbac::DBA_USER, rbac::DBA_ID);
        std::shared_ptr<Session> sess =
            std::make_shared<Session>(user, SessionProperties::NORMAL, false);
        result->attachSession(sess);
        return result;
    }

    std::shared_ptr<PhysicalPlan> OptimizerTests::generatePPlanFor(const char* queryString, bool afl, bool optEnable)
    {
        std::shared_ptr<Query> query = getQuery();

        query->queryString = queryString;
        _queryProcessor->parseLogical(query, true);

        _queryProcessor->inferTypes(query);

        const bool isDdl = query->logicalPlan->isDdl();

        if (optEnable) {
            _queryProcessor->createPhysicalPlan(_habilis, query);
        } else {
            _queryProcessor->createPhysicalPlan(_habilisDisabled, query);
        }

        query->logicalPlan.reset();

        if (optEnable) {
            _queryProcessor->optimize(_habilis, query, isDdl);
        } else {
            _queryProcessor->optimize(_habilisDisabled, query, isDdl);
        }

        auto result = query->getCurrentPhysicalPlan();
        // NOT IN ORIGINAL query->validate();
        return result;
    }

    int OptimizerTests::countDfNodes(std::shared_ptr<PhysicalPlan> pp)
    {
        int result = 1;
        std::shared_ptr<PhysicalQueryPlanNode> node = pp->getRoot();

        while (node->getChildren().size() > 0)
        {
            node = node->getChildren()[0];
            result++;
        }
        return result;
    }

    int OptimizerTests::countTotalNodes(PhysNodePtr node)
    {
        int result = 1;
        for (size_t i = 0; i < node->getChildren().size(); i++)
        {
            result += countTotalNodes(node->getChildren()[i]);
        }
        return result;
    }

    //TODO: make this equivalence logic visible to the outside world? Overload operator==?
    bool OptimizerTests::equivalent(const AttributeDesc& lhs, const AttributeDesc& rhs)
    {
        if (lhs.getId() != rhs.getId() || lhs.getType() != rhs.getType() || lhs.getFlags() != rhs.getFlags() || lhs.getDefaultCompressionMethod()
                != rhs.getDefaultCompressionMethod())
        {
            return false;
        }

        return true;
    }

    bool OptimizerTests::equivalent(const DimensionDesc& lhs, const DimensionDesc& rhs)
    {
        if (  lhs.getChunkInterval() != rhs.getChunkInterval() ||
              lhs.getChunkOverlap() != rhs.getChunkOverlap() ||
              lhs.getCurrEnd() != rhs.getCurrEnd() ||
              lhs.getCurrStart() != rhs.getCurrStart() ||
              lhs.getEndMax() != rhs.getEndMax() ||
              lhs.getLength() != rhs.getLength() ||
              lhs.getStartMin() != rhs.getStartMin() ||
              lhs.getStartMin() != rhs.getStartMin())
        {
            return false;
        }

        return true;
    }

    bool OptimizerTests::equivalent(const ArrayDesc& lhs, const ArrayDesc& rhs)
    {
        if (lhs.getAttributes().size() != rhs.getAttributes().size())
        {
            return false;
        }
        for (size_t i = 0; i < lhs.getAttributes().size(); i++)
        {
            if (!equivalent(lhs.getAttributes().findattr(i), rhs.getAttributes().findattr(i)))
            {
                return false;
            }
        }

        if (lhs.getDimensions().size() != rhs.getDimensions().size())
        {
            return false;
        }
        for (size_t i = 0; i < lhs.getDimensions().size(); i++)
        {
            if (!equivalent(lhs.getDimensions()[i], rhs.getDimensions()[i]))
            {
                return false;
            }
        }

        if (lhs.getEmptyBitmapAttribute() == NULL)
        {
            if (rhs.getEmptyBitmapAttribute() != NULL)
            {
                return false;
            }
        }
        else if (rhs.getEmptyBitmapAttribute() == NULL)
        {
            return false;
        }
        else if (!equivalent(*lhs.getEmptyBitmapAttribute(), *rhs.getEmptyBitmapAttribute()))
        {
            return false;
        }

        if (lhs.getFlags() != rhs.getFlags() || lhs.getSize() != rhs.getSize())
        {
            return false;
        }

        return true;
    }

    void OptimizerTests::testBoundariesHelper(int64_t dim0Start, int64_t dim0End, int64_t dim0Chunk, int64_t dim1Start, int64_t dim1End, int64_t dim1Chunk,
                              int64_t start0, int64_t start1, int64_t end0, int64_t end1, uint64_t expectedNumCells, uint64_t expectedNumChunks)
    {
        Dimensions dummyArrayDimensions;
        dummyArrayDimensions.push_back(DimensionDesc("x", dim0Start, dim0End, dim0Chunk, 0));
        dummyArrayDimensions.push_back(DimensionDesc("y", dim1Start, dim1End, dim1Chunk, 0));

        Coordinates start;
        start.push_back(start0);
        start.push_back(start1);

        Coordinates end;
        end.push_back(end0);
        end.push_back(end1);

        PhysicalBoundaries bounds(start, end);
        uint64_t numCells = bounds.getNumCells();
        uint64_t numChunks = 0;
        bool thrown = false;
        try {
            numChunks = bounds.getNumChunks(dummyArrayDimensions);
        }
        catch (PhysicalBoundaries::UnknownChunkIntervalException&) {
            thrown = true;
        }

        CPPUNIT_ASSERT(!thrown);
        CPPUNIT_ASSERT(numCells == expectedNumCells);
        CPPUNIT_ASSERT(numChunks == expectedNumChunks);
    }

    void OptimizerTests::testBoundaries()
    {
        int64_t dim0Start, dim0End, dim0Chunk, dim1Start, dim1End, dim1Chunk, start0, start1, end0, end1;
        uint64_t expectedNumCells, expectedNumChunks;

#define TEST() testBoundariesHelper(dim0Start, dim0End, dim0Chunk, dim1Start, dim1End, dim1Chunk, start0, start1, end0, end1, expectedNumCells, expectedNumChunks)

        dim0Start = 0;
        dim0End = 9;
        dim0Chunk = 1;
        dim1Start = 0;
        dim1End = 9;
        dim1Chunk = 1;
        start0 = 3;
        start1 = 3;
        end0 = 4;
        end1 = 4;
        expectedNumCells = 4;
        expectedNumChunks = 4;
        TEST();

        start0 = 0;
        start1 = 0;
        end0 = 0;
        end1 = 0;
        expectedNumCells = 1;
        expectedNumChunks = 1;
        TEST();

        start0 = 0;
        start1 = 0;
        end0 = 9;
        end1 = 9;
        expectedNumCells = 100;
        expectedNumChunks = 100;
        TEST();

        start0 = 0;
        start1 = 0;
        end0 = -1;
        end1 = -1;
        expectedNumCells = 0;
        expectedNumChunks = 0;
        TEST();

        end0 = 3;
        TEST();

        dim0Start = 0;
        dim0End = 9;
        dim0Chunk = 3;
        dim1Start = 0;
        dim1End = 9;
        dim1Chunk = 3;
        start0 = 3;
        start1 = 3;
        end0 = 4;
        end1 = 4;
        expectedNumCells = 4;
        expectedNumChunks = 1;
        TEST();

        start0 = 2;
        start1 = 2;
        end0 = 7;
        end1 = 7;
        expectedNumCells = 36;
        expectedNumChunks = 9;
        TEST();

        start0 = 1;
        start1 = 2;
        end0 = 3;
        end1 = 9;
        expectedNumCells = 24;
        expectedNumChunks = 8;
        TEST();
    }

    void OptimizerTests::testBoundaries2()
    {
        Coordinates start1; start1.push_back(-75); start1.push_back(-74);
        Coordinates end1; end1.push_back(25); end1.push_back(26);

        Coordinates start2; start2.push_back(0); start2.push_back(1);
        Coordinates end2; end2.push_back(100); end2.push_back(101);

        Coordinates inside;   inside.push_back(20);     inside.push_back(20);
        Coordinates outside1; outside1.push_back(-100); outside1.push_back(-100);
        Coordinates outside2; outside2.push_back(20);   outside2.push_back(150);

        PhysicalBoundaries bounds1(start1, end1, 0.25);
        PhysicalBoundaries bounds2(start2, end2, 0.25);

        PhysicalBoundaries intersection = intersectWith(bounds1, bounds2);
        CPPUNIT_ASSERT(intersection.getStartCoords()[0] == 0);
        CPPUNIT_ASSERT(intersection.getStartCoords()[1] == 1);
        CPPUNIT_ASSERT(intersection.getEndCoords()[0] == 25);
        CPPUNIT_ASSERT(intersection.getEndCoords()[1] == 26);
        CPPUNIT_ASSERT(intersection.getDensity() == 1.0);

        PhysicalBoundaries union_b = unionWith(bounds1, bounds2);

        CPPUNIT_ASSERT(union_b.getStartCoords()[0] == -75);
        CPPUNIT_ASSERT(union_b.getStartCoords()[1] == -74);
        CPPUNIT_ASSERT(union_b.getEndCoords()[0] == 100);
        CPPUNIT_ASSERT(union_b.getEndCoords()[1] == 101);
        CPPUNIT_ASSERT(union_b.getDensity() > 0.16 && union_b.getDensity() < 0.17);

        PhysicalBoundaries xproduct = crossWith(bounds1, bounds2);
        CPPUNIT_ASSERT(xproduct.getStartCoords()[0] == -75);
        CPPUNIT_ASSERT(xproduct.getStartCoords()[1] == -74);
        CPPUNIT_ASSERT(xproduct.getStartCoords()[2] == 0);
        CPPUNIT_ASSERT(xproduct.getStartCoords()[3] == 1);

        CPPUNIT_ASSERT(xproduct.getEndCoords()[0] == 25);
        CPPUNIT_ASSERT(xproduct.getEndCoords()[1] == 26);
        CPPUNIT_ASSERT(xproduct.getEndCoords()[2] == 100);
        CPPUNIT_ASSERT(xproduct.getEndCoords()[3] == 101);
        CPPUNIT_ASSERT(xproduct.getDensity() == bounds1.getDensity() * bounds2.getDensity());

        CPPUNIT_ASSERT(bounds1.isInsideBox(inside) );
        CPPUNIT_ASSERT(bounds2.isInsideBox(inside) );
        CPPUNIT_ASSERT(!bounds1.isInsideBox(outside1) );
        CPPUNIT_ASSERT(!bounds1.isInsideBox(outside2) );
        CPPUNIT_ASSERT(!bounds2.isInsideBox(outside1) );
        CPPUNIT_ASSERT(!bounds2.isInsideBox(outside2) );

        Dimensions dimOrig;
        dimOrig.push_back(DimensionDesc("dim1", -100,299,1,0));
        dimOrig.push_back(DimensionDesc("dim2", -124,275,1,0));

        Dimensions dimEnd;
        dimEnd.push_back(DimensionDesc("dim1", -5000,154999,1,0));

        PhysicalBoundaries reshapedBounds = bounds1.reshape(dimOrig,dimEnd);
        PhysicalBoundaries reshapedBack = reshapedBounds.reshape(dimEnd, dimOrig);

        CPPUNIT_ASSERT(reshapedBounds.getStartCoords()[0] == 5050);
        CPPUNIT_ASSERT(reshapedBounds.getEndCoords()[0] == 45150);

        CPPUNIT_ASSERT_DOUBLES_EQUAL(
            static_cast<double>(reshapedBounds.getNumCells()) * reshapedBounds.getDensity(),
            static_cast<double>(bounds1.getNumCells()) * bounds1.getDensity(),
            std::numeric_limits<double>::epsilon());

        CPPUNIT_ASSERT(reshapedBack.getStartCoords()[0] == bounds1.getStartCoords()[0] );
        CPPUNIT_ASSERT(reshapedBack.getEndCoords()[0] == bounds1.getEndCoords()[0] );

        CPPUNIT_ASSERT_DOUBLES_EQUAL(
            static_cast<double>(reshapedBack.getNumCells()) * reshapedBack.getDensity(),
            static_cast<double>(bounds1.getNumCells()) * bounds1.getDensity(),
            std::numeric_limits<double>::epsilon());
    }

    void OptimizerTests::testBasic()
    {
        std::shared_ptr<PhysicalPlan> pp = generatePPlanFor("scan(opttest_dummy_array)", true, false);
        CPPUNIT_ASSERT(pp->isDdl()==false);
        CPPUNIT_ASSERT(countDfNodes(pp) == 1);
        ASSERT_OPERATOR(pp->getRoot(), "physicalScan");
        CPPUNIT_ASSERT(pp->getRoot()->hasParent() == false);
        ArrayDesc opSchema = pp->getRoot()->getPhysicalOperator()->getSchema();
        CPPUNIT_ASSERT(equivalent(opSchema, _dummyArray));

        pp = generatePPlanFor("scan(opttest_dummy_array)");
        CPPUNIT_ASSERT(pp->isDdl()==false);
        CPPUNIT_ASSERT(countDfNodes(pp) == 1);
        ASSERT_OPERATOR(pp->getRoot(), "physicalScan");
        CPPUNIT_ASSERT(pp->getRoot()->hasParent() == false);
        opSchema = pp->getRoot()->getPhysicalOperator()->getSchema();
        CPPUNIT_ASSERT(equivalent(opSchema, _dummyArray));
    }

    void OptimizerTests::testThreeInstanceSgInsert(const char* query, const char* opName)
    {
        std::shared_ptr<PhysicalPlan> pp = generatePPlanFor(query, true, false);

        CPPUNIT_ASSERT(countDfNodes(pp) == 2);
        PhysNodePtr expectedSubArrayNode = pp->getRoot();
        ASSERT_OPERATOR(expectedSubArrayNode,opName);
        PhysOpPtr expectedSubArrayOp = expectedSubArrayNode->getPhysicalOperator();
        CPPUNIT_ASSERT(expectedSubArrayNode->getChildren().size() == 1);
        CPPUNIT_ASSERT(expectedSubArrayNode->hasParent() == false);

        PhysNodePtr expectedScanNode = expectedSubArrayNode->getChildren()[0];
        PhysOpPtr expectedScanOp = expectedScanNode->getPhysicalOperator();

        ASSERT_OPERATOR(expectedScanNode, "physicalScan");
        CPPUNIT_ASSERT(expectedScanNode->getParent()==expectedSubArrayNode);
        CPPUNIT_ASSERT(expectedScanNode->getChildren().size() == 0);
        CPPUNIT_ASSERT(equivalent (expectedScanOp->getSchema(), _dummyArray));
        CPPUNIT_ASSERT(!equivalent (expectedScanOp->getSchema(), expectedSubArrayOp->getSchema()));

        pp = generatePPlanFor(query);

        CPPUNIT_ASSERT(countDfNodes(pp) == 2);
        expectedSubArrayNode = pp->getRoot();
        ASSERT_OPERATOR(expectedSubArrayNode,opName);
        expectedSubArrayOp = expectedSubArrayNode->getPhysicalOperator();
        CPPUNIT_ASSERT(expectedSubArrayNode->getChildren().size() == 1);
        CPPUNIT_ASSERT(expectedSubArrayNode->hasParent() == false);

        expectedScanNode = expectedSubArrayNode->getChildren()[0];
        expectedScanOp = expectedScanNode->getPhysicalOperator();

        ASSERT_OPERATOR(expectedScanNode, "physicalScan");
        CPPUNIT_ASSERT(expectedScanNode->getParent()==expectedSubArrayNode);
        CPPUNIT_ASSERT(expectedScanNode->getChildren().size() == 0);
        CPPUNIT_ASSERT(equivalent (expectedScanOp->getSchema(), _dummyArray));
        CPPUNIT_ASSERT(!equivalent (expectedScanOp->getSchema(), expectedSubArrayOp->getSchema()));
    }

    void OptimizerTests::testSubArrayReshapeSgInsertions()
    {
        testThreeInstanceSgInsert("subarray(opttest_dummy_array, 5,5,10,10)", "physicalSubArray");
        testThreeInstanceSgInsert("reshape(opttest_dummy_array, opttest_dummy_shifted_array)", "physicalReshape");
    }

    void OptimizerTests::testInputSgInsert()
    {
        if (!boost::filesystem::exists("/tmp/tmpfile"))
        {
            int fd = open("/tmp/tmpfile", O_RDWR|O_TRUNC|O_EXCL|O_CREAT, 0666);
            close(fd);
        }

        std::shared_ptr<PhysicalPlan> pp = generatePPlanFor("input(opttest_dummy_array, '/tmp/tmpfile')", /*afl*/true, /*optEnabled*/false);

        CPPUNIT_ASSERT(countDfNodes(pp) == 1);
        PhysNodePtr expectedInputNode = pp->getRoot();
        ASSERT_OPERATOR(expectedInputNode, "impl_input");
        PhysOpPtr expectedInputOp = expectedInputNode->getPhysicalOperator();

        CPPUNIT_ASSERT(equivalent (expectedInputOp->getSchema(), _dummyArray) );
        CPPUNIT_ASSERT(expectedInputNode->hasParent() == false);
        CPPUNIT_ASSERT(expectedInputNode->getChildren().size() == 0);

        pp = generatePPlanFor("input(opttest_dummy_array, '/tmp/tmpfile')");

        CPPUNIT_ASSERT(countDfNodes(pp) == 1);
        expectedInputNode = pp->getRoot();
        ASSERT_OPERATOR(expectedInputNode, "impl_input");

        expectedInputOp = expectedInputNode->getPhysicalOperator();

        CPPUNIT_ASSERT(equivalent (expectedInputOp->getSchema(), _dummyArray) );
        CPPUNIT_ASSERT(expectedInputNode->hasParent() == false);
        CPPUNIT_ASSERT(expectedInputNode->getChildren().size() == 0);
    }

    void OptimizerTests::testTwoPhase()
    {
        std::ostringstream out;
        std::shared_ptr<PhysicalPlan> pp;
        PhysNodePtr root, child;

        pp = generatePPlanFor("sum(opttest_single_dim)");
        //        pp->toString(out);
        //        std::cout<<out.str();
        //        out.str("");
        root = pp->getRoot();
        ASSERT_OPERATOR(root,"physicalSum2");
        root = root->getChildren()[0];
        ASSERT_OPERATOR(root,"physicalSum");
        root = root->getChildren()[0];
        ASSERT_OPERATOR(root,"physicalScan");
        CPPUNIT_ASSERT(root->getChildren().size() == 0);

        pp = generatePPlanFor("join(opttest_single_dim, sort (opttest_single_dim,att0) )");
        //        pp->toString(out);
        //        std::cout<<out.str();
        //        out.str("");
        root = pp->getRoot();
        ASSERT_OPERATOR(root,"physicalJoin");
        child = root->getChildren()[0];
        ASSERT_OPERATOR(child,"physicalScan");
        CPPUNIT_ASSERT(child->getChildren().size() == 0);
        child = root->getChildren()[1];
        ASSERT_OPERATOR(child,"impl_sg");
        child = child->getChildren()[0];
        ASSERT_OPERATOR(child,"physicalSort2");
        child = child->getChildren()[0];
        ASSERT_OPERATOR(child,"physicalSort");
        child = child->getChildren()[0];
        ASSERT_OPERATOR(child,"physicalScan");
        CPPUNIT_ASSERT(child->getChildren().size() == 0);

        pp = generatePPlanFor("join( subarray(opttest_single_dim,0,0) , sum(opttest_single_dim))");
        //        pp->toString(out);
        //        std::cout<<out.str();
        //        out.str("");
        root = pp->getRoot();
        ASSERT_OPERATOR(root,"physicalJoin");
        child = root->getChildren()[0];
        ASSERT_OPERATOR(child,"physicalSubArray");
        child = child->getChildren()[0];
        ASSERT_OPERATOR(child,"physicalScan");
        CPPUNIT_ASSERT(child->getChildren().size() == 0);
        child = root->getChildren()[1];
        ASSERT_OPERATOR(child,"impl_sg");
        child = child->getChildren()[0];
        ASSERT_OPERATOR(child,"physicalSum2");
        child = child->getChildren()[0];
        ASSERT_OPERATOR(child,"physicalSum");
        child = child->getChildren()[0];
        ASSERT_OPERATOR(child,"physicalScan");
        CPPUNIT_ASSERT(child->getChildren().size() == 0);

        pp = generatePPlanFor("join( subarray(opttest_single_dim,0,1) , subarray (sort (opttest_single_dim,att0), 1,2 ))");
        //        pp->toString(out);
        //        std::cout<<out.str();
        //        out.str("");
        root = pp->getRoot();
        ASSERT_OPERATOR(root,"physicalJoin");
        child = root->getChildren()[0];
        ASSERT_OPERATOR(child,"physicalSubArray");
        child = child->getChildren()[0];
        ASSERT_OPERATOR(child,"physicalScan");
        CPPUNIT_ASSERT(child->getChildren().size() == 0);
        child = root->getChildren()[1];
        ASSERT_OPERATOR(child, "impl_sg");
        child = child->getChildren()[0];
        ASSERT_OPERATOR(child, "physicalSubArray");
        child = child->getChildren()[0];
        ASSERT_OPERATOR(child, "impl_sg");
        child = child->getChildren()[0];
        ASSERT_OPERATOR(child, "physicalSort2");
        child = child->getChildren()[0];
        ASSERT_OPERATOR(child, "physicalSort");
        CPPUNIT_ASSERT(child->getDataWidth() == child->getChildren()[0]->getDataWidth());
        child = child->getChildren()[0];
        ASSERT_OPERATOR(child, "physicalScan");
        CPPUNIT_ASSERT(child->getChildren().size()==0);

        pp = generatePPlanFor("join( subarray(load(opttest_single_dim,'dummy_file_path'), 0,1) , subarray(opttest_single_dim,1,2))");
        //        pp->toString(out);
        //        std::cout<<out.str();
        //        out.str("");
        root = pp->getRoot();
        child = root->getChildren()[0];
        ASSERT_OPERATOR(child, "physicalSubArray");
        child = child->getChildren()[0];

        ASSERT_OPERATOR(child, "impl_sg");

        child = child->getChildren()[0];
        ASSERT_OPERATOR(child, "impl_input");
        child = root->getChildren()[1];
        ASSERT_OPERATOR(child, "impl_sg");
        child = child->getChildren()[0];
        ASSERT_OPERATOR(child, "physicalSubArray");
        child = child->getChildren()[0];
        ASSERT_OPERATOR(child, "physicalScan");
    }

    void OptimizerTests::testMultiply()
    {
        std::ostringstream out;
        std::shared_ptr<PhysicalPlan> pp;
        PhysNodePtr root, child;

        //SG results of subarray to match results of multply
        pp = generatePPlanFor(
                 "join ( apply(multiply(project(opttest_small_array,att0), project(opttest_small_array,att0)), att1, instanceid()), subarray(project(opttest_dummy_array,att0), 2,2,4,4))");
//                pp->toString(out);
//                std::cout<<out.str();
//                out.str("");
        root = pp->getRoot();
        child = root->getChildren()[0];
        ASSERT_OPERATOR(child, "physicalApply");
        child = child->getChildren()[0];
        ASSERT_OPERATOR(child, "impl_sg");

        //Multiply of two subarrays - no sgs needed
        pp = generatePPlanFor(
                 "multiply ( subarray(project(opttest_dummy_array,att0), 0,0,2,2), subarray(project(opttest_dummy_array,att0), 0,0,2,2))");
        //        pp->toString(out);
        //        std::cout<<out.str();
        //        out.str("");
        root = pp->getRoot();
        child = root->getChildren()[0];
        ASSERT_OPERATOR(child, "physicalSubArray");
        child = root->getChildren()[1];
        ASSERT_OPERATOR(child, "physicalSubArray");

        pp = generatePPlanFor(
                 "join(transpose(multiply(project(opttest_dummy_array,att0), project(opttest_dummy_array,att0))),transpose(project(opttest_dummy_array,att0)))");
//                pp->toString(out);
//                std::cout<<out.str();
//                out.str("");
        //LHS is a transpose that emits byrow distribution
        //RHS is a transpose that has violated round robin -- put SG on top of it.
        root = pp->getRoot();
        child = root->getChildren()[0];
        ASSERT_OPERATOR(child, "impl_sg");
        child = root->getChildren()[1];
        ASSERT_OPERATOR(child, "impl_sg");
    }

    void OptimizerTests::testReplication()
    {
        std::shared_ptr<PhysicalPlan> pp;
        PhysNodePtr root;
        std::ostringstream out;


        //Verify that the store of a replicated array has no SG or reduce required
        pp = generatePPlanFor( "store(opttest_dummy_replicated_array, some_weird_array_we_hope_does_not_exist)");
        root = pp->getRoot();
        ASSERT_OPERATOR(root, "physicalStore");
        root = root->getChildren()[0];
        ASSERT_OPERATOR(root, "physicalScan");

        //Verify that there's a reduceDistro inserted between scan and aggregate so we get the correct count
        pp = generatePPlanFor( "aggregate(opttest_dummy_replicated_array, count(*))");
//        pp->toString(out);
//        std::cout<<out.str();
//        out.str("");
        root = pp->getRoot();
        ASSERT_OPERATOR(root, "physical_aggregate");
        root = root->getChildren()[0];
        ASSERT_OPERATOR(root, "physicalReduceDistro");
        root = root->getChildren()[0];
        ASSERT_OPERATOR(root, "physicalScan");

        //Verify there's reduce distro inserted on the right
        pp = generatePPlanFor( "join(opttest_dummy_array, opttest_dummy_replicated_array)");
//        pp->toString(out);
//        std::cout<<out.str();
//        out.str("");
        root = pp->getRoot();
        ASSERT_OPERATOR(root, "physicalJoin");
        PhysNodePtr child = root->getChildren()[0];
        ASSERT_OPERATOR(child, "physicalScan");
        child = root->getChildren()[1]->getChildren()[0];
        ASSERT_OPERATOR(child, "physicalReduceDistro");

        //Verify there's reduce distro inserted on both sides
        pp = generatePPlanFor( "merge(opttest_dummy_replicated_array, opttest_dummy_replicated_array)");
//        pp->toString(out);
//        std::cout<<out.str();
//        out.str("");
        root = pp->getRoot();
        ASSERT_OPERATOR(root, "physicalMerge");
        child = root->getChildren()[0]->getChildren()[0];
        ASSERT_OPERATOR(child, "physicalReduceDistro");
        child = root->getChildren()[1]->getChildren()[0];
        ASSERT_OPERATOR(child, "physicalReduceDistro");

        //Verify there's reduce distro inserted on the left
        pp = generatePPlanFor( "join(opttest_dummy_replicated_array, opttest_dummy_array)");
//        pp->toString(out);
//        std::cout<<out.str();
//        out.str("");
        root = pp->getRoot();
        ASSERT_OPERATOR(root, "physicalJoin");
        child = root->getChildren()[0];
        ASSERT_OPERATOR(child, "impl_materialize");
        child = child->getChildren()[0];
        ASSERT_OPERATOR(child, "physicalReduceDistro");
        child = child->getChildren()[0];
        ASSERT_OPERATOR(child, "physicalScan");

        child = root->getChildren()[1];
        ASSERT_OPERATOR(child, "physicalScan");

        pp = generatePPlanFor( "merge(opttest_dummy_array, opttest_dummy_replicated_array)");
//        pp->toString(out);
//        std::cout<<out.str();
//        out.str("");
        root = pp->getRoot();
        ASSERT_OPERATOR(root, "physicalMerge");
        child = root->getChildren()[0];
        ASSERT_OPERATOR(child, "physicalScan");
        child = root->getChildren()[1]->getChildren()[0];
        ASSERT_OPERATOR(child, "physicalReduceDistro");
    }

}
