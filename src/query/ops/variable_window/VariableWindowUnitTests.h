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
 * VariableWindowUnitTests.h
 *  Created on: Feb 9, 2011
 *      Author: poliocough@gmail.com
 *  Extracted from VariableWindow.h and surrounded with CPPUNIT infrastructure by Donghui on 10/22/2015.
 */

#ifndef VARIABLE_WINDOW_UNIT_TESTS_H_
#define VARIABLE_WINDOW_UNIT_TESTS_H_

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>
#include "VariableWindow.h"

namespace scidb {

class VariableWindowUnitTests: public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(VariableWindowUnitTests);
    CPPUNIT_TEST(testRightEdge);
    CPPUNIT_TEST(testMessageMarshalling);
    CPPUNIT_TEST_SUITE_END();

public:
    void testRightEdge()
    {
        AggregateLibrary* al = AggregateLibrary::getInstance();
        Type tDouble = TypeLibrary::getType(TID_DOUBLE);
        AggregatePtr sa = al->createAggregate("sum", tDouble);
        CPPUNIT_ASSERT(sa.get() != 0);

        std::vector<AggregatePtr> sum;
        sum.push_back(sa);

        WindowEdge re;

        Value v1;
        v1.setDouble(1);
        re.addPreceding(v1);
        v1.setNull();
        re.addPreceding(v1);
        v1.setDouble(2);
        re.addPreceding(v1);

        v1.setDouble(3);
        re.addCentral(v1, 0, 0);

        CPPUNIT_ASSERT(re.getNumCoords()==1);
        CPPUNIT_ASSERT(re.getNumValues()==4);

        std::shared_ptr<AggregatedValue> p = re.churn(3, 0, sum);
        CPPUNIT_ASSERT(p->coord==0 && p->instanceId==0 && p->vals[0].getDouble() == 6);
        CPPUNIT_ASSERT(re.getNumCoords()==0);
        CPPUNIT_ASSERT(re.getNumValues()==3);

        re.addCentral(v1, 1, 2);

        p = re.churn(10, 1, sum);
        CPPUNIT_ASSERT(p->coord==1 && p->instanceId==2 && p->vals[0].getDouble() == 8);
        CPPUNIT_ASSERT(re.getNumCoords()==0);
        CPPUNIT_ASSERT(re.getNumValues()==4);

        v1.setDouble(4);
        re.addCentral(v1, 2, 0);

        v1.setNull();
        re.addCentral(v1, 3, 0);

        size_t size = re.getBinarySize();
        char* buf = new char[size];
        char* end = re.marshall(buf);
        CPPUNIT_ASSERT( (size_t) (end - buf) == size);

        WindowEdge re2;
        re2.unMarshall(buf);
        CPPUNIT_ASSERT(re.getNumCoords() == re2.getNumCoords() );
        CPPUNIT_ASSERT(re.getNumValues() == re2.getNumValues() );

        while(re.getNumCoords())
        {
            std::shared_ptr<AggregatedValue> p1 = re.churn(4, 1, sum);
            std::shared_ptr<AggregatedValue> p2 = re2.churn(4, 1, sum);

            CPPUNIT_ASSERT(p1->coord==p2->coord);
            CPPUNIT_ASSERT(p1->instanceId==p2->instanceId);
            CPPUNIT_ASSERT(p1->vals[0]==p2->vals[0]);
        }

        re2.clear();
        CPPUNIT_ASSERT(re2.getNumValues() == 0);

        delete[] buf;

        //simulate 2 preceding + 1 following
        v1.setDouble(1);
        re2.addPreceding(v1);

        v1.setNull();
        re2.addPreceding(v1);

        v1.setDouble(2);
        re2.addCentral(v1, 0, 1);

        v1.setDouble(3);
        re2.addCentral(v1, 1, 0);

        v1.setDouble(4);
        re2.addCentral(v1, 2, 2);

        v1.setDouble(5);
        re2.addFollowing(v1);

        size = re2.getBinarySize();
        buf = new char [size];
        re2.marshall(buf);
        re2.clear();
        re2.unMarshall(buf);

        //1+n+2+3
        p = re2.churn(2, 1, sum);
        CPPUNIT_ASSERT(p->coord==0 && p->instanceId==1 && p->vals[0].getDouble() == 6 && re2.getNumFollowing()==2);

        //n+2+3+4
        p = re2.churn(2, 1, sum);
        CPPUNIT_ASSERT(p->coord==1 && p->instanceId==0 && p->vals[0].getDouble() == 9 && re2.getNumFollowing()==1);

        //2+3+4+5
        p = re2.churn(2, 1, sum);
        CPPUNIT_ASSERT(p->coord==2 && p->instanceId==2 && p->vals[0].getDouble() == 14 && re2.getNumFollowing()==0);

        delete[] buf;
    }

    void grindAndCompare(VariableWindowMessage const& message, size_t nDims)
    {
        size_t binarySize = message.getBinarySize(nDims,1);
        char* buf = new char[binarySize];
        char* buf2 = message.marshall(nDims, 1, buf);
        CPPUNIT_ASSERT( (size_t) (buf2-buf) == binarySize );
        VariableWindowMessage message2;
        message2.unMarshall(buf, nDims, 1);

        std::cout<<"\nSRC Message: "<<message<<std::endl;
        std::cout<<"\nDST Message: "<<message2<<std::endl;
        std::cout.flush();

        cerr << "Press ENTER to continue: ";
        cerr.flush();
        cin.get();

        CPPUNIT_ASSERT(message == message2);
        delete[] buf;
    }

    void testMessageMarshalling()
    {
        VariableWindowMessage message;
        size_t nDims = 3;
        grindAndCompare(message, nDims);

        std::shared_ptr <ChunkEdge> chunkEdge0 (new ChunkEdge());
        Coordinates coords(nDims);

        coords[0]=0;
        coords[1]=0;
        coords[2]=0;

        Value val;
        val.setDouble(0.0);

        std::shared_ptr<WindowEdge> windowEdge0 (new WindowEdge());
        windowEdge0->addPreceding(val);
        val.setDouble(0.1);
        windowEdge0->addPreceding(val);
        val.setNull();
        windowEdge0->addCentral(val, 0, 0);
        val.setDouble(0.3);
        windowEdge0->addCentral(val, 1, 0);
        windowEdge0->addCentral(val, 2, 0);
        val.setNull();
        windowEdge0->addFollowing(val);
        (*chunkEdge0)()[coords] = windowEdge0;

        coords[1]=1;
        std::shared_ptr<WindowEdge> windowEdge1 (new WindowEdge());
        val.setDouble(0.5);
        windowEdge1->addPreceding(val);
        (*chunkEdge0)()[coords] = windowEdge1;

        coords[1]=0;
        message._chunkEdges[coords]=chunkEdge0;
        grindAndCompare(message, nDims);

        std::shared_ptr <ChunkEdge> chunkEdge1 (new ChunkEdge());
        std::shared_ptr<WindowEdge> windowEdge3 (new WindowEdge());
        val.setDouble(0.6);
        windowEdge3->addCentral(val, 3, 0);
        coords[0]=3;
        coords[1]=3;
        coords[2]=4;
        (*chunkEdge1)()[coords] = windowEdge3;
        coords[2]=3;
        message._chunkEdges[coords]=chunkEdge1;
        grindAndCompare(message, nDims);

        coords[0]=0;
        coords[1]=0;
        coords[2]=0;
        message._computedValues[coords] = std::shared_ptr<Coords2ValueVector> (new Coords2ValueVector());

        std::vector<Value> vals(1,val);

        (*message._computedValues[coords])[coords] = vals;
        grindAndCompare(message, nDims);
        Coordinates coords2 = coords;
        coords2[1]=1;
        vals[0].setNull();
        (*message._computedValues[coords])[coords2] = vals;
        grindAndCompare(message, nDims);
        message._computedValues[coords2]= std::shared_ptr<Coords2ValueVector> (new Coords2ValueVector());
        vals[0].setDouble(3.4);
        (*message._computedValues[coords2])[coords2] = vals;
        grindAndCompare(message, nDims);
    }
};  // class VariableWindowUnitTests

CPPUNIT_TEST_SUITE_REGISTRATION(VariableWindowUnitTests);

}  // namespace scidb

#endif  // VariableWindowUnitTests
