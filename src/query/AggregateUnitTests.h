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
 * @file AggregateUnitTests.h
 *
 * @author poliocough@gmail.com
 */

#ifndef AGGREGATE_UNIT_TESTS_H_
#define AGGREGATE_UNIT_TESTS_H_

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <cmath>
#include <limits>

#include <query/Aggregate.h>

namespace scidb {


class AggregateTests: public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(AggregateTests);
    CPPUNIT_TEST(testIntegerSum);
    CPPUNIT_TEST(testFloatSum);
    CPPUNIT_TEST(testIntegerAvg);
    CPPUNIT_TEST(testDoubleAvg);
    CPPUNIT_TEST(testIntegerCompositeSumAvgDc);
    CPPUNIT_TEST(testIntegerCompositeMerge);
    CPPUNIT_TEST_SUITE_END();

    void testIntegerSum()
    {
        AggregateLibrary* al = AggregateLibrary::getInstance();
        Type tInt32 = TypeLibrary::getType(TID_INT32);

        AggregatePtr sum = al->createAggregate("sum", tInt32);
        CPPUNIT_ASSERT(sum.get() != 0);

        CPPUNIT_ASSERT(sum->getInputType() == TypeLibrary::getType(TID_INT32));
        CPPUNIT_ASSERT(sum->getStateType() == TypeLibrary::getType(TID_BINARY));
        CPPUNIT_ASSERT(sum->getResultType() == TypeLibrary::getType(TID_INT64));

        Value input(sum->getInputType());
        Value state(sum->getStateType());
        Value final(sum->getResultType());

        sum->initializeState(state);
        sum->finalResult(final, state);
        CPPUNIT_ASSERT(isDefaultFor(final,sum->getResultType().typeId()));

        sum->initializeState(state);
        input = TypeLibrary::getDefaultValue(sum->getInputType().typeId());
        sum->accumulateIfNeeded(state, input);
        sum->accumulateIfNeeded(state, input);
        CPPUNIT_ASSERT(!isDefaultFor(state,sum->getStateType().typeId()));
        input.setNull();
        sum->accumulateIfNeeded(state, input);
        CPPUNIT_ASSERT(!isDefaultFor(state,sum->getStateType().typeId()));

        Value state2(sum->getStateType());
        sum->initializeState(state2);
        CPPUNIT_ASSERT(!isDefaultFor(state2,sum->getStateType().typeId()));
        sum->mergeIfNeeded(state, state2);

        sum->finalResult(final, state);
        CPPUNIT_ASSERT(isDefaultFor(final,sum->getResultType().typeId()));

        sum->initializeState(state);
        input = TypeLibrary::getDefaultValue(sum->getInputType().typeId());
        sum->accumulateIfNeeded(state, input);
        input.setInt32(5);
        sum->accumulateIfNeeded(state, input);
        input.setInt32(3);
        sum->accumulateIfNeeded(state, input);

        state2 = Value(sum->getStateType());
        sum->mergeIfNeeded(state2, state);
        sum->mergeIfNeeded(state, state2);

        sum->finalResult(final, state);

        CPPUNIT_ASSERT(final.getInt64() == 16);
    }

    void testFloatSum()
    {
        AggregateLibrary* al = AggregateLibrary::getInstance();
        Type tFloat = TypeLibrary::getType(TID_FLOAT);

        AggregatePtr sum = al->createAggregate("sum", tFloat);
        CPPUNIT_ASSERT(sum.get() != 0);

        CPPUNIT_ASSERT(sum->getInputType() == TypeLibrary::getType(TID_FLOAT));
        CPPUNIT_ASSERT(sum->getStateType() == TypeLibrary::getType(TID_BINARY));
        CPPUNIT_ASSERT(sum->getResultType() == TypeLibrary::getType(TID_DOUBLE));

        Value input(sum->getInputType());
        Value state(sum->getStateType());
        Value final(sum->getResultType());

        sum->initializeState(state);
        sum->finalResult(final, state);
        CPPUNIT_ASSERT(isDefaultFor(final,sum->getResultType().typeId()));

        sum->initializeState(state);
        input = TypeLibrary::getDefaultValue(sum->getInputType().typeId());
        sum->accumulateIfNeeded(state, input);
        sum->accumulateIfNeeded(state, input);
        sum->finalResult(final, state);
        CPPUNIT_ASSERT(isDefaultFor(final,sum->getResultType().typeId()));

        sum->initializeState(state);
        input = TypeLibrary::getDefaultValue(sum->getInputType().typeId());
        sum->accumulateIfNeeded(state, input);
        input.setFloat(5.1f);
        sum->accumulateIfNeeded(state, input);
        input.setFloat(3.1f);
        sum->accumulateIfNeeded(state, input);

        Value state2(sum->getStateType());
        sum->mergeIfNeeded(state2, state);
        sum->mergeIfNeeded(state, state2);

        sum->finalResult(final, state);

        //We incur a float epsilon every time we convert from float to double (two times multiplied by two)
        CPPUNIT_ASSERT( std::fabs(final.getDouble() - 16.4 ) < 4*std::numeric_limits<float>::epsilon() );
    }

    void testIntegerAvg()
    {
        AggregateLibrary* al = AggregateLibrary::getInstance();
        Type tInt32 = TypeLibrary::getType(TID_INT32);

        AggregatePtr avg = al->createAggregate("avg", tInt32);
        CPPUNIT_ASSERT(avg.get() != 0);

        CPPUNIT_ASSERT(avg->getInputType() == TypeLibrary::getType(TID_INT32));
//      CPPUNIT_ASSERT(sum->getStateType() == TypeLibrary::getType(TID_INT64));
        CPPUNIT_ASSERT(avg->getResultType() == TypeLibrary::getType(TID_DOUBLE));

        Value input(avg->getInputType());
        Value state(avg->getStateType());
        Value final(avg->getResultType());

        avg->initializeState(state);
        input.setInt32(5);
        avg->accumulateIfNeeded(state, input);

        input.setInt32(3);
        avg->accumulateIfNeeded(state, input);

        input.setInt32(0);
        avg->accumulateIfNeeded(state, input);

        avg->finalResult(final, state);
        CPPUNIT_ASSERT( std::fabs(final.getDouble() - (8.0 / 3.0)) < 4*std::numeric_limits<float>::epsilon() );
    }

    void testDoubleAvg()
    {
        AggregateLibrary* al = AggregateLibrary::getInstance();
        Type tDouble = TypeLibrary::getType(TID_DOUBLE);

        AggregatePtr avg = al->createAggregate("avg", tDouble);
        CPPUNIT_ASSERT(avg.get() != 0);

        CPPUNIT_ASSERT(avg->getInputType() == TypeLibrary::getType(TID_DOUBLE));
        CPPUNIT_ASSERT(avg->getResultType() == TypeLibrary::getType(TID_DOUBLE));

        Value input(avg->getInputType());
        Value state(avg->getStateType());
        Value final(avg->getResultType());

        avg->initializeState(state);
        input.setDouble(5.0);
        avg->accumulateIfNeeded(state, input);

        input.setDouble(3.0);
        avg->accumulateIfNeeded(state, input);

        input.setDouble(0);
        avg->accumulateIfNeeded(state, input);

        avg->finalResult(final, state);
        CPPUNIT_ASSERT( std::fabs(final.getDouble() - (8.0 / 3.0)) < 4*std::numeric_limits<float>::epsilon() );
    }

    void testIntegerCompositeSumAvgDc()
    {
        AggregateLibrary& agLib = *AggregateLibrary::getInstance();
        Type tInt = TypeLibrary::getType(TID_INT64);

        AggregatePtr agg = agLib.createAggregate("_compose", tInt);
        CPPUNIT_ASSERT(agg.get() != 0);
        CompositeAggregate* compAgg = dynamic_cast<CompositeAggregate*>(agg.get());
        CPPUNIT_ASSERT(compAgg != nullptr);
        CPPUNIT_ASSERT(compAgg->getInputType().isVoid());

        compAgg->add(agLib.createAggregate("sum", tInt))
            .add(agLib.createAggregate("avg", tInt))
            .add(agLib.createAggregate("approxdc", tInt));
        CPPUNIT_ASSERT(compAgg->getInputType().typeId() == tInt.typeId());

        // Add subaggregate with incompatible input type?  No way, bucko.
        bool raised = false;
        Type tDouble = TypeLibrary::getType(TID_DOUBLE);
        try {
            compAgg->add(agLib.createAggregate("sum", tDouble));
        } catch (...) {
            raised = true;
        }
        CPPUNIT_ASSERT(raised);

        // Composite with void input type is void until a non-void is added.
        Type tVoid = TypeLibrary::getType(TID_VOID);
        AggregatePtr agg2 = agLib.createAggregate("_compose", tVoid);
        CPPUNIT_ASSERT(agg2->getInputType().isVoid());
        CompositeAggregate* compAgg2 = dynamic_cast<CompositeAggregate*>(agg2.get());
        compAgg2->add(agLib.createAggregate("sum", tDouble));
        CPPUNIT_ASSERT(compAgg2->getInputType().typeId() == tDouble.typeId());

        Value input(tInt);
        Value state;
        CPPUNIT_ASSERT(state.isNull());

        // One hundred distinct values [0..99] repeated 10 times.
        for (int64_t i = 0; i < 1000; ++i) {
            input.setInt64(i % 100);
            agg->accumulateIfNeeded(state, input);
        }

        std::vector<Value> finals;
        compAgg->finalResults(finals, state);
        CPPUNIT_ASSERT(finals[0].getInt64() == 49500); // sum
        CPPUNIT_ASSERT(finals[1].getDouble() == 49.5); // avg
        CPPUNIT_ASSERT(finals[2].getUint64() == 100);  // approxdc
    }

    void testIntegerCompositeMerge()
    {
        AggregateLibrary& agLib = *AggregateLibrary::getInstance();
        Type tInt = TypeLibrary::getType(TID_INT64);

        AggregatePtr a1, a2;
        CompositeAggregate *agg1, *agg2;
        a1 = agLib.createAggregate("_compose", tInt);
        agg1 = dynamic_cast<CompositeAggregate*>(a1.get());
        agg1->add(agLib.createAggregate("sum", tInt))
            .add(agLib.createAggregate("avg", tInt))
            .add(agLib.createAggregate("approxdc", tInt));
        a2 = a1->clone();
        agg2 = dynamic_cast<CompositeAggregate*>(a2.get());
        CPPUNIT_ASSERT(a1.get() && a2.get());
        CPPUNIT_ASSERT(agg1 != agg2);

        Value input(tInt);
        Value state1;
        Value state2;

        // One hundred distinct values [0..99] repeated 10 times, but
        // in two groups.
        for (int64_t i = 0; i < 500; ++i) {
            input.setInt64(i % 50); // only 50 unique values!
            agg1->accumulateIfNeeded(state1, input);
        }
        for (int64_t i = 500; i < 1000; ++i) {
            input.setInt64(i % 100);
            agg2->accumulateIfNeeded(state2, input);
        }

        std::vector<Value> finals;
        agg1->finalResults(finals, state1);
        CPPUNIT_ASSERT(finals[0].getInt64() == 12250); // sum
        CPPUNIT_ASSERT(finals[1].getDouble() == 24.5); // avg
        CPPUNIT_ASSERT(finals[2].getUint64() == 50);   // approxdc

        finals.clear();
        agg2->finalResults(finals, state2);
        CPPUNIT_ASSERT(finals[0].getInt64() == 24750); // sum
        CPPUNIT_ASSERT(finals[1].getDouble() == 49.5); // avg
        CPPUNIT_ASSERT(finals[2].getUint64() == 100);  // approxdc

        // Move it around in local memory!
        Value state3(state2);

        finals.clear();
        agg1->mergeIfNeeded(state1, state3);
        agg1->finalResults(finals, state1);
        CPPUNIT_ASSERT(finals[0].getInt64() == 37000); // sum
        CPPUNIT_ASSERT(finals[1].getDouble() == 37);   // avg
        CPPUNIT_ASSERT(finals[2].getUint64() == 100);  // approxdc
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(AggregateTests);

}  // namespace scidb

#endif /* AGGREGATE_UNIT_TESTS_H_ */
