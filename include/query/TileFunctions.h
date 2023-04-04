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
 * @file TileFunctions.h
 *
 * @author roman.simakov@gmail.com
 *
 * @brief Templates of implementation tile functions
 */

#ifndef TILEFUNCTIONS_H
#define TILEFUNCTIONS_H

#include <query/TypeSystem.h>
#include <query/FunctionDescription.h>
#include <array/RLE.h>
#include <system/Utils.h>  // For SCIDB_ASSERT

namespace scidb
{

struct VarValue
{
    std::shared_ptr<Value> value;
    std::shared_ptr< std::vector<char> > varPart;
    bool operator==(const VarValue& other) const {
        ASSERT_EXCEPTION_FALSE("varPart items cannot be compared");
    }

};

///////////////////////////////////////////////////////////////////
// Below are a set of helper template functions for code generation related to adding and setting values.
///////////////////////////////////////////////////////////////////

template <typename T>
size_t addPayloadValues(RLEPayload* p, size_t n = 1)
{
    return p->addRawValues(n);
}

template <> inline
size_t addPayloadValues<bool>(RLEPayload* p, size_t n)
{
    return p->addBoolValues(n);
}

template <> inline
size_t addPayloadValues<VarValue>(RLEPayload* p, size_t n)
{
    // XXX HACK alert!
    // Note the size is set to 0 because we just want to know
    // the next index for value insertion without growing
    // the internal datastructure.
    // setPayloadValue() will do it for us.
    return p->addRawVarValues(0);
}

template <typename T>
void setPayloadValue(RLEPayload* p, size_t index, T value) {
    T* data = (T*)p->getRawValue(index);
    *data = value;
}

// Specialization for bool
template <> inline
void setPayloadValue<bool>(RLEPayload* p, size_t index, bool value) {
    char* data = p->getFixData();
    char mask = static_cast<char>(1 << (index & 7));
    if (value) {
        data[index >> 3] |= mask;
    } else {
        data[index >> 3] &= static_cast<char>(~mask);
    }
}

template <> inline
void setPayloadValue<VarValue>(RLEPayload* p, size_t index, VarValue value)
{
    p->appendValue(*value.varPart, *value.value, index);
}

template <typename T>
T getPayloadValue(const ConstRLEPayload* p, size_t index) {
    return *(T*)p->getRawValue(index);
}
// Specialization for bool
template <> inline
bool getPayloadValue<bool>(const ConstRLEPayload* p, size_t index) {
    return p->checkBit(index);
}

template <> inline
VarValue getPayloadValue<VarValue>(const ConstRLEPayload* p, size_t index) {
    VarValue res;
    res.value = std::shared_ptr<Value>(new Value());
    p->getValueByIndex(*res.value, index);
    return res;
}

///////////////////////////////////////////////////////////////////
// Below are unary template functions.
///////////////////////////////////////////////////////////////////

template<typename T, typename TR>
struct UnaryMinus
{
    static TR func(T v)
    {
        return -v;
    }
};

template<typename T, typename TR>
struct UnaryFunctionCall
{
    typedef TR (*FunctionPointer)(T);
    template<FunctionPointer F>
    struct Function
    {
        template<typename T1, typename TR1>
        struct Op
        {
            static TR1 func(T1 v)
            {
                return F(v);
            }
        };
    };
};

template<typename T, typename TR>
struct UnaryConverter
{
    static TR func(T v)
    {
        return v;
    }
};

/**
 * Template of function for unary operations.
 */
template<template <typename T, typename TR> class O, typename T, typename TR>
void rle_unary_func(const Value** args,  Value* result, void*)
{
    const Value& v = *args[0];
    Value& res = *result;
    res.getTile()->clear();
    res.getTile()->assignSegments(*v.getTile());
    const size_t valuesCount = v.getTile()->getValuesCount();
    addPayloadValues<TR>(res.getTile(), valuesCount);
    size_t i = 0;
    T* s = (T*)v.getTile()->getFixData();
    T* end = s + valuesCount;
    while (s < end) {
        setPayloadValue<TR>(res.getTile(), i++, O<T, TR>::func(*s++));
    }
}

void rle_unary_bool_not(const Value** args,  Value* result, void*);

///////////////////////////////////////////////////////////////////
// Below are binary template functions.
///////////////////////////////////////////////////////////////////
template<typename T1, typename T2, typename TR>
struct BinaryFunctionBase
{
    // is cummutative with null?
    static bool requiresSpecialNullOperandProcessing(){
        return false;
    }
    static TR getSpecialOperandWithNull()
    {
        return TR();
    }
};

template<typename T1, typename T2, typename TR>
struct BinaryPlus : public BinaryFunctionBase<T1, T2, TR>
{
    static TR func(T1 v1, T2 v2)
    {
        return v1 + v2;
    }
};

template<typename T1, typename T2, typename TR>
struct BinaryMinus : public BinaryFunctionBase<T1, T2, TR>
{
    static TR func(T1 v1, T2 v2)
    {
        return v1 - v2;
    }
};

template<typename T1, typename T2, typename TR>
struct BinaryMult : public BinaryFunctionBase<T1, T2, TR>
{
    static TR func(T1 v1, T2 v2)
    {
        return v1 * v2;
    }
};

template<typename T1, typename T2, typename TR>
struct BinaryDiv : public BinaryFunctionBase<T1, T2, TR>
{
    static TR func(T1 v1, T2 v2)
    {
        if (0 == v2)
            throw USER_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_DIVISION_BY_ZERO);
        return v1 / v2;
    }
};

// Specialization for double and float data type to avoid error and producing NaN values according to IEEE-754
template<>
struct BinaryDiv<double, double, double>  : public BinaryFunctionBase<double, double, double>
{
    static double func(double v1, double v2)
    {
        return v1 / v2;
    }
};

template<>
struct BinaryDiv<float, float, float> : public BinaryFunctionBase<float, float, float>
{
    static float func(float v1, float v2)
    {
        return v1 / v2;
    }
};

template<typename T1, typename T2, typename TR>
struct BinaryMod : public BinaryFunctionBase<T1, T2, TR>
{
    static TR func(T1 v1, T2 v2)
    {
        if (0 == v2)
            throw USER_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_DIVISION_BY_ZERO);
        return v1 % v2;
    }
};

template<typename T1, typename T2, typename TR>
struct BinaryAnd : public BinaryFunctionBase<T1, T2, TR>
{
    static TR func(T1 v1, T2 v2)
    {
        return v1 && v2;
    }
};
template<>
struct BinaryAnd<bool,bool,bool> : public BinaryFunctionBase<bool,bool,bool>
{
    static bool func(bool v1, bool v2)
    {
        return v1 && v2;
    }
    static bool requiresSpecialNullOperandProcessing(){
        return true;
    }
    static bool getSpecialOperandWithNull()
    {
        return false;
    }
};

template<typename T1, typename T2, typename TR>
struct BinaryOr  : public BinaryFunctionBase<T1, T2, TR>
{
    static TR func(T1 v1, T2 v2)
    {
        return v1 || v2;
    }
};

template<>
struct BinaryOr<bool, bool, bool> : public BinaryFunctionBase<bool, bool, bool>
{
    static bool func(bool v1, bool v2)
    {
        return v1 || v2;
    }
    static bool requiresSpecialNullOperandProcessing(){
        return true;
    }
    static bool getSpecialOperandWithNull()
    {
        return true;
    }
};

template<typename T1, typename T2, typename TR>
struct BinaryLess : public BinaryFunctionBase<T1, T2, TR>
{
    static TR func(T1 v1, T2 v2)
    {
        return v1 < v2;
    }
};

template<typename T1, typename T2, typename TR>
struct BinaryLessOrEq : public BinaryFunctionBase<T1, T2, TR>
{
    static TR func(T1 v1, T2 v2)
    {
        return v1 <= v2;
    }
};

template<typename T1, typename T2, typename TR>
struct BinaryEq : public BinaryFunctionBase<T1, T2, TR>
{
    static TR func(T1 v1, T2 v2)
    {
        return v1 == v2;
    }
};

template<typename T1, typename T2, typename TR>
struct BinaryNotEq : public BinaryFunctionBase<T1, T2, TR>
{
    static TR func(T1 v1, T2 v2)
    {
        return v1 != v2;
    }
};

template<typename T1, typename T2, typename TR>
struct BinaryGreater : public BinaryFunctionBase<T1, T2, TR>
{
    static TR func(T1 v1, T2 v2)
    {
        return v1 > v2;
    }
};

template<typename T1, typename T2, typename TR>
struct BinaryGreaterOrEq : public BinaryFunctionBase<T1, T2, TR>
{
    static TR func(T1 v1, T2 v2)
    {
        return v1 >= v2;
    }
};

template<typename T1, typename T2, typename TR>
struct BinaryFunctionCall : public BinaryFunctionBase<T1, T2, TR>
{
    typedef TR (*FunctionPointer)(T1, T2);
    template<FunctionPointer F>
    struct Function
    {
        template<typename T1_, typename T2_, typename TR1_>
        struct Op : public BinaryFunctionBase<T1_, T2_, TR1_>
        {
            static TR1_ func(T1_ v1, T2_ v2)
            {
                return F(v1, v2);
            }
        };
    };
};

template<typename T1, typename T2, typename TR>
struct BinaryStringPlus : public BinaryFunctionBase<T1, T2, TR>
{
    static VarValue func(VarValue v1, VarValue v2)
    {
        const std::string str1(v1.value->getString());
        const std::string str2(v2.value->getString());
        VarValue res;
        res.value = std::shared_ptr<Value>(new Value());
        res.value->setString((str1 + str2).c_str());
        return res;
    }
};

template<typename T>
void setVarPart(T& v, const std::shared_ptr<std::vector<char> >& part)
{
    // Nothing for every datatype except VarType which is specified below
}

template<> inline
void setVarPart<VarValue>(VarValue& v, const std::shared_ptr<std::vector<char> >& part)
{
    v.varPart = part;
}

/**
 * Fast binary operations
 * i1, i2, ir is index of result in bytes.
 * length is number of elements
 */
template<template <typename T1, typename T2, typename TR> class O, typename T1, typename T2, typename TR>
bool fastDenseBinary(size_t length, const char* p1, size_t i1, const char* p2, size_t i2, char* pr, size_t ir)
{
    // in general case it's impossible to apply fast operations
    return false;
}

template<class B>
bool fastDenseBinaryBool(size_t length, const char* p1, size_t i1, const char* p2, size_t i2, char* pr, size_t ir)
{
    const size_t bitSize = sizeof(unsigned int) * 8;
    if ( (i1 % bitSize == 0) && (i2 % bitSize == 0) && (ir % bitSize == 0) && (length % bitSize == 0) )
    {
        const unsigned int* pi1 = reinterpret_cast<const unsigned int*>(p1) + i1 / bitSize;
        const unsigned int* pi2 = reinterpret_cast<const unsigned int*>(p2) + i2 / bitSize;
        unsigned int* pir = reinterpret_cast<unsigned int*>(pr) + ir / bitSize;
        for (size_t i = 0; i < length / bitSize; i++) {
            *pir++ = B::op(*pi1++, *pi2++);
        }
        return true;
    }
    else
    {
        // in this case it's impossible to apply fast operations.
        return false;
    }
}

/** Boolean fast operation templates */
struct FastBinaryAnd
{
    static unsigned int op(unsigned int a1, unsigned int a2) {
        return a1 & a2;
    }
};

template<> inline
bool fastDenseBinary<BinaryAnd, bool, bool, bool>(size_t length, const char* p1, size_t i1, const char* p2, size_t i2, char* pr, size_t ir)
{
    return fastDenseBinaryBool<FastBinaryAnd>(length, p1, i1, p2, i2, pr, ir);
}

struct FastBinaryOr
{
    static unsigned int op(unsigned int a1, unsigned int a2) {
        return a1 | a2;
    }
};

template<> inline
bool fastDenseBinary<BinaryOr, bool, bool, bool>(size_t length, const char* p1, size_t i1, const char* p2, size_t i2, char* pr, size_t ir)
{
    return fastDenseBinaryBool<FastBinaryOr>(length, p1, i1, p2, i2, pr, ir);
}

/**
 *  @brief Binary Function on (old) Tile Values
 *
 * This template is for binary function for types with fixed size.
 * This function cannot preserve RLE structure.
 * Arguments of this function should be extracted using the same empty bitmask.
 * In other words they should be aligned during unpack.
 *
 *  @tparam T1 type of first operand
 *  @tparam T2 type of second operand
 *  @tparam TR return type
 *  @tparam O Template functor to operate on the first and second operand which must implement:
 *          -  bool requiresSpecialNullOperandProcessing()
 *          -  TR  getSpecialOperandWithNull()
 *          -  TR func(T1 v1, T2 v2);
 * @param args input @c Value objects to be used as operands
 * @param result output @c Value result of functor application
 * @param UNUSED (Execution Cost)
 */
template <template <typename T1, typename T2, typename TR> class O,
          typename T1,
          typename T2,
          typename TR>
void rle_binary_func(const Value** args, Value* result, void*)
{
    const Value& v1 = *args[0];
    const Value& v2 = *args[1];
    Value& res = *result;
    res.getTile()->clear();
    std::shared_ptr<std::vector<char>> varPart(new std::vector<char>());

    if (v1.getTile()->count() == 0 || v1.getTile()->nSegments() == 0 ||
        v2.getTile()->count() == 0 || v2.getTile()->nSegments() == 0) {

        res.getTile()->flush(0);
        return;
    }

    size_t i1 = 0;
    size_t i2 = 0;
    size_t segmentA_length;
    size_t segmentB_length;
    RLEPayload::Segment segmentA = v1.getTile()->getSegment(i1, segmentA_length);
    RLEPayload::Segment segmentB = v2.getTile()->getSegment(i2, segmentB_length);
    uint64_t chunkSize = 0;

    // Checking against max length is the equivalent to verifying if this is a special
    // case tile.  Special case tiles of this sort are created in TypeSystem.cpp in the
    // method makeTileConstant(const TypeId &, const Value &).
    if (segmentA_length == CoordinateBounds::getMaxLength()) {
        // v1 is constant with max length. Align it pPosition to v2
        segmentA.setPPosition(segmentB.pPosition());
    } else if (segmentB_length == CoordinateBounds::getMaxLength()) {
        // v2 is constant with max length. Align it pPosition to v1
        segmentB.setPPosition(segmentA.pPosition());
    }

    while (true) {
        // At this point segmentA and segmentB should be aligned.
        // Segment with shorter length will be iterated and the segment with longer length
        // will be re-aligned for the next iteration of this loop.
        assert(segmentA.pPosition() == segmentB.pPosition());

        const uint64_t length = std::min(segmentA_length, segmentB_length);
        if (length == 0) {
            break;
        }
        RLEPayload::Segment rs;

        if (segmentA.null()) {
            SCIDB_ASSERT(segmentA.isRun());
        }
        if (segmentB.null()) {
            SCIDB_ASSERT(segmentB.isRun());
        }
        // Check NULL cases.
        if ((segmentA.null() && segmentB.null()) ||
            ((segmentA.null() || segmentB.null()) &&
             (!O<T1, T2, TR>::requiresSpecialNullOperandProcessing()))) {
            // both operands are null,
            //  or no special processing is necessary when one of the operands is null.
            rs.setPPosition(segmentA.pPosition());
            rs.setRun(true);
            rs.setNull(true);
            // The documentation says missing reason code is 0 regardless of the value of
            // the missing reason of the original null values
            rs.setValueIndex(0);
        } else if (segmentA.null()) {
            // first is null; second is not null.
            if (segmentB.isRun()) {
                TR r = getPayloadValue<TR>(v2.getTile(), segmentB.valueIndex());
                TR special = O<T1, T2, TR>::getSpecialOperandWithNull();
                if (r == special) {
                    // NULL AND false => false
                    // NULL OR true ==> true
                    rs.setPPosition(segmentB.pPosition());
                    rs.setRun(true);
                    rs.setNull(false);
                    rs.setValueIndex(addPayloadValues<TR>(res.getTile()));
                    setPayloadValue<TR>(res.getTile(), rs.valueIndex(), r);
                } else {
                    // true AND NULL => NULL
                    // null AND TRUE => NULL
                    rs.setPPosition(segmentB.pPosition());
                    rs.setRun(true);
                    rs.setNull(true);
                    // The documentation says missing reason code is 0 regardless of the
                    // value of the missing reason of the original null values
                    rs.setValueIndex(0);
                }
            } else {
                // Segment B is a Literal, but the result is either a boolean or NULL
                // so the original segment may need to be broken into multiple
                // segments of either Null or SpeicalValue.
                // For Example:
                //   true OR NULL => true, true AND NULL => NULL
                //   true AND NULL ==> NULL, false AND NULL ==> false
                rs.setPPosition(segmentB.pPosition());
                size_t j = segmentB.valueIndex();
                size_t end = j + length;
                while (j < end) {
                    TR firstVal = getPayloadValue<TR>(v2.getTile(), j);
                    TR special = O<T1, T2, TR>::getSpecialOperandWithNull();
                    TR track = firstVal;
                    size_t count = 0;
                    // determine the length of the segment to break out.
                    while ((firstVal == track) && (j < end)) {
                        track = getPayloadValue<TR>(v2.getTile(), ++j);
                        ++count;
                    }
                    if (firstVal == special) {
                        // adding a boolean segment (False in the case of BinaryAnd, True
                        // in the case of BinaryOr).
                        // rs.PPosition is adjusted for each new broken out segment
                        rs.setNull(false);
                        rs.setRun(true);
                        rs.setValueIndex(addPayloadValues<TR>(res.getTile()));
                        setPayloadValue<TR>(res.getTile(), rs.valueIndex(), firstVal);
                        if (j != end) {
                            // Only add a broken out segment if it is not the last.
                            // The last segment will be added at the end of the loop.
                            res.getTile()->addSegment(rs);
                            // Set PPosition for for next segment
                            rs.addToPPosition(count);
                        }
                    } else {
                        // adding a null segment
                        rs.setNull(true);
                        rs.setRun(true);
                        // The documentation says missing reason code is 0 regardless of
                        // the value of the missing reason of the original null values
                        rs.setValueIndex(0);
                        if (j != end) {
                            // Only add a broken out segment if it is not the last.
                            // The last segment will be added at the end of the loop.
                            res.getTile()->addSegment(rs);
                            // Set PPosition for for next segment
                            rs.addToPPosition(count);
                        }
                    }
                }
            }
        } else if (segmentB.null()) {
            // first is not null,  second is null
            if (segmentA.isRun()) {
                TR r = getPayloadValue<TR>(v1.getTile(), segmentA.valueIndex());
                TR special = O<T1, T2, TR>::getSpecialOperandWithNull();
                if (r == special) {
                    // false AND NULL => false
                    // true OR NULL ==> true
                    rs.setPPosition(segmentA.pPosition());
                    rs.setRun(true);
                    rs.setNull(false);
                    rs.setValueIndex(addPayloadValues<TR>(res.getTile()));
                    setPayloadValue<TR>(res.getTile(), rs.valueIndex(), r);
                } else {
                    // true AND NULL ==> NULL
                    // false OR NULL ==> NULL
                    rs.setPPosition(segmentA.pPosition());
                    rs.setRun(true);
                    rs.setNull(true);
                    // The documentation says missing reason code is 0 regardless of the
                    // value of the missing reason of the original null values
                    rs.setValueIndex(0);
                }
            } else {
                rs.setPPosition(segmentA.pPosition());
                size_t j = segmentA.valueIndex();
                size_t end = j + length;
                while (j < end) {
                    TR firstVal = getPayloadValue<TR>(v1.getTile(), j);
                    TR special = O<T1, T2, TR>::getSpecialOperandWithNull();
                    TR track = firstVal;
                    size_t count = 0;
                    // determine the length of the segment to break out.
                    while ((firstVal == track) && (j < end)) {
                        track = getPayloadValue<TR>(v1.getTile(), ++j);
                        ++count;
                    }
                    if (firstVal == special) {
                        // adding a boolean segment (False in the case of BinaryAnd, True
                        // in the case of BinaryOr).
                        // rs.PPosition is adjusted for each new broken out segment
                        rs.setNull(false);
                        rs.setRun(true);
                        rs.setValueIndex(addPayloadValues<TR>(res.getTile()));
                        setPayloadValue<TR>(res.getTile(), rs.valueIndex(), firstVal);
                        if (j != end) {
                            // Only add a broken out segment if it is not the last.
                            // The last segment will be added at the end of the loop.
                            res.getTile()->addSegment(rs);
                            // Set for for next segment
                            rs.addToPPosition(count);
                        }
                    } else {
                        // adding a null segment
                        rs.setNull(true);
                        rs.setRun(true);
                        // The documentation says missing reason code is 0 regardless of
                        // the value of the missing reason of the original null values
                        rs.setValueIndex(0);
                        if (j != end) {
                            // Only add a broken out segment if it is not the last.
                            // The last segment will be added at the end of the loop.
                            res.getTile()->addSegment(rs);
                            // Set for for next segment
                            rs.addToPPosition(count);
                        }
                    }
                }
            }
        } else {
            // Neither operand is NULL and we can evaluate
            rs.setNull(false);
            rs.setPPosition(segmentA.pPosition());
            if (segmentA.isRun() && segmentB.isRun()) {
                // Both Segments are runs of a single value.
                rs.setRun(true);
                rs.setValueIndex(addPayloadValues<TR>(res.getTile()));
                TR r =
                    O<T1, T2, TR>::func(getPayloadValue<T1>(v1.getTile(), segmentA.valueIndex()),
                                        getPayloadValue<T2>(v2.getTile(), segmentB.valueIndex()));
                setVarPart<TR>(r, varPart);
                setPayloadValue<TR>(res.getTile(), rs.valueIndex(), r);
            } else if (segmentA.isRun() /*&& !segmentB.isRun()*/) {
                rs.setRun(false);
                rs.setValueIndex(addPayloadValues<TR>(res.getTile(), length));
                size_t i = rs.valueIndex();
                size_t j = segmentB.valueIndex();
                const size_t end = j + length;
                while (j < end) {
                    TR r = O<T1, T2, TR>::func(
                        getPayloadValue<T1>(v1.getTile(), segmentA.valueIndex()),
                        getPayloadValue<T2>(v2.getTile(), j++));
                    setVarPart<TR>(r, varPart);
                    setPayloadValue<TR>(res.getTile(), i++, r);
                }
            } else if (/*!segmentA.isRun() &&*/ segmentB.isRun()) {
                rs.setRun(false);
                rs.setValueIndex(addPayloadValues<TR>(res.getTile(), length));
                size_t i = rs.valueIndex();
                size_t j = segmentA.valueIndex();
                const size_t end = j + length;
                while (j < end) {
                    TR r = O<T1, T2, TR>::func(
                        getPayloadValue<T1>(v1.getTile(), j++),
                        getPayloadValue<T2>(v2.getTile(), segmentB.valueIndex()));
                    setVarPart<TR>(r, varPart);
                    setPayloadValue<TR>(res.getTile(), i++, r);
                }
            } else { // !segmentA.isRun() && !segmentB.isRun()
                rs.setRun(false);
                rs.setValueIndex(addPayloadValues<TR>(res.getTile(), length));
                size_t i = rs.valueIndex();
                size_t j1 = segmentA.valueIndex();
                size_t j2 = segmentB.valueIndex();
                if (!fastDenseBinary<O, T1, T2, TR>(
                        length,
                        const_cast<const char*>(v1.getTile()->getFixData()),
                        j1,
                        const_cast<const char*>(v2.getTile()->getFixData()),
                        j2,
                        res.getTile()->getFixData(),
                        rs.valueIndex())) {
                    const size_t end = j1 + length;
                    while (j1 < end) {
                        TR r = O<T1, T2, TR>::func(getPayloadValue<T1>(v1.getTile(), j1++),
                                                   getPayloadValue<T2>(v2.getTile(), j2++));
                        setVarPart<TR>(r, varPart);
                        setPayloadValue<TR>(res.getTile(), i++, r);
                    }
                }
            }
        }
        res.getTile()->addSegment(rs);
        chunkSize = rs.pPosition() + length;

        // Moving to the next segments
        if (segmentA_length == segmentB_length) {
            if (++i1 >= v1.getTile()->nSegments())
                break;
            if (++i2 >= v2.getTile()->nSegments())
                break;
            segmentA = v1.getTile()->getSegment(i1, segmentA_length);
            segmentB = v2.getTile()->getSegment(i2, segmentB_length);
        } else if (segmentA_length < segmentB_length) {
            if (++i1 >= v1.getTile()->nSegments())
                break;
            segmentA = v1.getTile()->getSegment(i1, segmentA_length);
            segmentB.addToPPosition(length);
            segmentB_length -= length;
            if (!segmentB.isRun())
                segmentB.setValueIndex(segmentB.valueIndex() + length);
        } else {
            if (++i2 >= v2.getTile()->nSegments())
                break;
            segmentB = v2.getTile()->getSegment(i2, segmentB_length);
            segmentA.addToPPosition(length);
            segmentA_length -= length;
            if (!segmentA.isRun())
                segmentA.setValueIndex(segmentA.valueIndex() + length);
        }
    }
    res.getTile()->flush(chunkSize);
    if (varPart->size()) {
        res.getTile()->setVarPart(*varPart);
    }
}

void inferIsNullArgTypes(const ArgTypes& factInputArgs, std::vector<ArgTypes>& possibleInputArgs, std::vector<TypeId>& possibleResultArgs);
void rle_unary_bool_is_null(const Value** args, Value* result, void*);
void rle_unary_null_to_any(const Value** args, Value* result, void*);

///////////////////////////////////////////////////////////////////
// Below are aggregator classes.
///////////////////////////////////////////////////////////////////

template <typename TS, typename TSR>
class AggSum
{
public:
    struct State {
        TSR _sum;
    };

    static void init(State& state)
    {
        state._sum = 0;
    }

    static void aggregate(State& state, const TS& value)
    {
        state._sum += static_cast<TSR>(value);
    }

    static void multAggregate(State& state, const TS& value, uint64_t count)
    {
        state._sum += static_cast<TSR>(value) * count;
    }

    static void merge(State& state, const State& new_state)
    {
        state._sum += new_state._sum;
    }

    static bool final(const State& state, TSR& result)
    {
        result = state._sum;
        return true;
    }

    static bool final(Value::reason, TSR& result)
    {
        result = 0;
        return true;
    }
};

template <typename TS, typename TSR>
class AggProd
{
public:
    struct State {
        TSR _prod;
    };

    static void init(State& state)
    {
        state._prod = 1;
    }

    static void aggregate(State& state, const TS& value)
    {
        state._prod *= static_cast<TSR>(value);
    }

    static void multAggregate(State& state, const TS& value, uint64_t count)
    {
        if (count==0)
        {
            return;
        }
        state._prod *= static_cast<TSR>( pow(static_cast<double>(value), static_cast<double>(count)) );
    }

    static void merge(State& state, const State& newState)
    {
        state._prod *= newState._prod;
    }

    static bool final(const State& state, TSR& result)
    {
        result = state._prod;
        return true;
    }

    static bool final(Value::reason, TSR& result)
    {
        result = 0;
        return true;
    }
};

template <typename TS, typename TSR>
class AggCount
{
public:
    struct State {
        uint64_t _count;
    };

    static void init(State& state)
    {
        state._count = 0;
    }

    static void aggregate(State& state, const TS& value)
    {
        state._count++;
    }

    static void multAggregate(State& state, const TS& value, uint64_t count)
    {
        state._count += count;
    }

    static void merge(State& state, const State& new_state)
    {
        state._count += new_state._count;
    }

    static bool final(const State& state, TSR& result)
    {
        result = state._count;
        return true;
    }

    static bool final(Value::reason, TSR& result)
    {
        result = 0;
        return true;
    }
};

template<typename T> inline
bool isNanValue(T value)
{
    return false;
}

template<> inline
bool isNanValue<double>(double value)
{
    return std::isnan(value);
}

template<> inline
bool isNanValue<float>(float value)
{
    return std::isnan(value);
}

template <typename TS, typename TSR>
class AggMin
{
public:
    struct State {
        TSR _min;
    };

    static void init(State& state, const TS& firstValue)
    {
        state._min = firstValue;
    }

    static void aggregate(State& state, const TS& value)
    {
        if (value < state._min || isNanValue(value))
            state._min = value;
    }

    static void multAggregate(State& state, const TS& value, uint64_t count)
    {
        if (value < state._min || isNanValue(value))
            state._min = value;
    }

    static void merge(State& state, const State& new_state)
    {
        if (new_state._min < state._min || isNanValue(new_state._min))
            state._min = new_state._min;
    }

    static bool final(const State& state, TSR& result)
    {
        result = state._min;
        return true;
    }

    static bool final(Value::reason, TSR&)
    {
        return false;
    }
};

template <typename TS, typename TSR>
class AggMax
{
public:
    struct State {
        TSR _max;
    };

    static void init(State& state, const TS& firstValue)
    {
        state._max = firstValue;
    }

    static void aggregate(State& state, const TS& value)
    {
        if (value > state._max || isNanValue(value))
            state._max = value;
    }

    static void multAggregate(State& state, const TS& value, uint64_t count)
    {
        if (value > state._max || isNanValue(value))
            state._max = value;
    }

    static void merge(State& state, const State& new_state)
    {
        if (new_state._max > state._max || isNanValue(new_state._max))
            state._max = new_state._max;
    }

    static bool final(const State& state, TSR& result)
    {
        result = state._max;
        return true;
    }

    static bool final(Value::reason, TSR&)
    {
        return false;
    }
};

template <typename TS, typename TSR>
class AggAvg
{
public:
    struct State {
        TSR _sum;
        uint64_t _count;
    };

    static void init(State& state)
    {
        state._sum = TSR();
        state._count = 0;
    }

    static void aggregate(State& state, const TS& value)
    {
        state._sum += static_cast<TSR>(value);
        state._count++;
    }

    static void multAggregate(State& state, const TS& value, uint64_t count)
    {
        state._sum += static_cast<TSR>(value) * count;
        state._count += count;
    }

    static void merge(State& state, const State& new_state)
    {
        state._sum += new_state._sum;
        state._count += new_state._count;
    }

    static bool final(const State& state, TSR& result)
    {
        if (state._count == 0)
            return false;
        result = static_cast<TSR>(state._sum) / state._count;
        return true;
    }

    static bool final(Value::reason, TSR&)
    {
        return false;
    }
};

template <typename TS, typename TSR>
class AggVar
{
public:
    struct State {
        TSR _m;
        TSR _m2;
        uint64_t _count;
    };

    static void init(State& state)
    {
        state._m = TSR();
        state._m2 = TSR();
        state._count = 0;
    }

    static void aggregate(State& state, const TS& value)
    {
        state._m += static_cast<TSR>(value);
        state._m2 += static_cast<TSR>(value * value);
        state._count++;
    }

    static void multAggregate(State& state, const TS& value, uint64_t count)
    {
        state._m += static_cast<TSR>(value) * count;
        state._m2 += static_cast<TSR>(value * value) * count;
        state._count += count;
    }

    static void merge(State& state, const State& new_state)
    {
        state._m += new_state._m;
        state._m2 += new_state._m2;
        state._count += new_state._count;
    }

    static bool final(const State& state, TSR& result)
    {
        if (state._count <= 1)
            return false;
        const TSR x = state._m / state._count;
        const TSR s = state._m2 / state._count - x * x;
        result = s * state._count / (state._count - 1) ;
        return true;
    }

    static bool final(Value::reason, TSR&)
    {
        return false;
    }
};

template <typename TS, typename TSR>
class AggStDev
{
public:
    struct State {
        TSR _m;
        TSR _m2;
        uint64_t _count;
    };

    static void init(State& state)
    {
        state._m = TSR();
        state._m2 = TSR();
        state._count = 0;
    }

    static void aggregate(State& state, const TS& value)
    {
        state._m += static_cast<TSR>(value);
        state._m2 += static_cast<TSR>(value * value);
        state._count++;
    }

    static void multAggregate(State& state, const TS& value, uint64_t count)
    {
        state._m += static_cast<TSR>(value) * count;
        state._m2 += static_cast<TSR>(value * value) * count;
        state._count += count;
    }

    static void merge(State& state, const State& new_state)
    {
        state._m += new_state._m;
        state._m2 += new_state._m2;
        state._count += new_state._count;
    }

    static bool final(const State& state, TSR& result)
    {
        if (state._count <= 1)
            return false;
        const TSR x = state._m / state._count;
        const TSR s = state._m2 / state._count - x * x;
        result = sqrt(s * state._count / (state._count - 1) );
        return true;
    }

    static bool final(Value::reason, TSR&)
    {
        return false;
    }
};

} // namespace

#endif
