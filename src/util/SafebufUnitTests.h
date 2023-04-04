/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2017-2019 SciDB, Inc.
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

#ifndef SAFEBUF_UNIT_TESTS_H
#define SAFEBUF_UNIT_TESTS_H

#include <util/safebuf.h>
#include <string>

namespace scidb {

struct SafebufTests : public CppUnit::TestFixture
{
    void setUp() {}
    void tearDown() {}

    void testEmpty();
    void testCopyCtor();
    void testGrabString();
    void testReturnValueOptimization();
    void testStringCow();

    CPPUNIT_TEST_SUITE(SafebufTests);
    CPPUNIT_TEST(testEmpty);
    CPPUNIT_TEST(testCopyCtor);
    CPPUNIT_TEST(testGrabString);
    CPPUNIT_TEST(testReturnValueOptimization);
    CPPUNIT_TEST(testStringCow);
    CPPUNIT_TEST_SUITE_END();

private:
    static safebuf s_stringToSafebuf(std::string const& s);
};

void SafebufTests::testEmpty()
{
    using std::string;

    safebuf empty;
    CPPUNIT_ASSERT(empty.c_str() != nullptr);
    CPPUNIT_ASSERT(*empty.c_str() == '\0');

    string sEmpty;
    safebuf sb;
    sb.grab(sEmpty);
    CPPUNIT_ASSERT(!::strcmp(sb.c_str(), ""));
    CPPUNIT_ASSERT(sb.size() == 1);
    CPPUNIT_ASSERT(*static_cast<const char*>(sb.data()) == '\0');

    sb.wipe();
    CPPUNIT_ASSERT(!::strcmp(sb.c_str(), ""));
    CPPUNIT_ASSERT(*static_cast<const char*>(sb.data()) == '\0');
}

void SafebufTests::testGrabString()
{
    using std::string;

    safebuf sb;
    string five(5, '/');
    CPPUNIT_ASSERT(five == "/////");
    sb.grab(five);
    CPPUNIT_ASSERT(!::strcmp(sb.c_str(), "/////"));
    // Assumes default fill char for strings is '%'.
    CPPUNIT_ASSERT(!::strcmp(five.c_str(), "%%%%%"));

    string six("666666");
    sb.grab(six, '&');
    CPPUNIT_ASSERT(!::strcmp(sb.c_str(),  "666666"));
    CPPUNIT_ASSERT(!::strcmp(six.c_str(), "&&&&&&"));
    CPPUNIT_ASSERT(six.size() == 6);
}

void SafebufTests::testCopyCtor()
{
    using std::string;

    string cheese("red leicester");
    safebuf sb(cheese);
    CPPUNIT_ASSERT(cheese == "%%%%%%%%%%%%%");
    CPPUNIT_ASSERT(!::strcmp(sb.c_str(), "red leicester"));

    safebuf sb2(sb);
    CPPUNIT_ASSERT(!::strcmp(sb.c_str(), "red leicester"));
    CPPUNIT_ASSERT(!::strcmp(sb2.c_str(), "red leicester"));
    sb2.wipe();
    CPPUNIT_ASSERT(!::strcmp(sb.c_str(), "red leicester"));
    CPPUNIT_ASSERT(!::strcmp(sb2.c_str(), ""));
}

safebuf SafebufTests::s_stringToSafebuf(std::string const& s)
{
    safebuf result;
    result.grab(s);
    return result;
}

void SafebufTests::testReturnValueOptimization()
{
    using std::string;

    string cheese("incident");
    safebuf sb;
    sb = s_stringToSafebuf(cheese);
    CPPUNIT_ASSERT(!::strcmp(sb.c_str(), "incident"));
    CPPUNIT_ASSERT(cheese == "%%%%%%%%");
}

void SafebufTests::testStringCow()
{
    using std::string;

    // This test just proves that our std::string implementation tries
    // to minimize copying.  Therefore safebuf users have to be extra
    // careful, since const_cast<>ing is involved.  (Maybe it should
    // be called unsafebuf?)

    string s1("American Milking Devon");
    string s2(s1);
    safebuf sb;
    sb.grab(s2);
    CPPUNIT_ASSERT(!::strcmp(sb.c_str(), "American Milking Devon"));
    CPPUNIT_ASSERT(s2 == "%%%%%%%%%%%%%%%%%%%%%%");
#if (defined(__GNUC__) && __GNUC__ < 5)
    // GCC prior to version 5 used a reference counted implementation of std::string,
    // but it is actually a non-conforming c++-11 implementation.
    // This was fixed in GCC 5.
    //     https://stackoverflow.com/a/29199733
    CPPUNIT_ASSERT(s1 == "%%%%%%%%%%%%%%%%%%%%%%");   // <--- SURPRISE!!!
#else
    CPPUNIT_ASSERT(s1 == "American Milking Devon");
#endif

    // If you need to get the data into a safebuf but don't want to
    // clobber anything, force creation of a stack temporary using
    // std::string::c_str(), like so:

    string s3("Norwegian Red");
    string s4(s3);
    safebuf sb2;
    sb2.grab(s4.c_str());       // <-- note c_str() use
    CPPUNIT_ASSERT(!::strcmp(sb2.c_str(), "Norwegian Red"));
    CPPUNIT_ASSERT(s4 == "Norwegian Red"); // <--- SURPRISE, *not* clobbered
    CPPUNIT_ASSERT(s3 == "Norwegian Red"); // also untouched
}

} // namespace

CPPUNIT_TEST_SUITE_REGISTRATION(scidb::SafebufTests);

#endif /* ! SAFEBUF_UNIT_TESTS_H */
