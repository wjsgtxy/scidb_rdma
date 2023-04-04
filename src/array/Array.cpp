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

/**
 * @file Array.cpp
 *
 * @brief class Array
 */

#include <array/Array.h>

#include <vector>
#include <string.h>

#include <log4cxx/logger.h>

#include <array/ConstArrayIterator.h>
#include <array/ArrayIterator.h>
#include <array/ConstItemIterator.h>
#include <array/ConstChunk.h>
#include <array/ChunkIterator.h>

#include <query/TypeSystem.h>
#include <system/Exceptions.h>

using namespace std;

namespace scidb
{

    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.array.Array"));

    Array::Array(ArrayPtr const& ap)
    {
        pushPipe(ap);
    }

    Array::Array(ArrayPtr const& ap1, ArrayPtr const& ap2)
    {
        // Few arrays have more than two input arrays.  For those that
        // do, use the default ctor and call pushInput() as many times
        // as needed.

        pushPipe(ap1);
        pushPipe(ap2);
    }

    void Array::pushPipe(ArrayPtr const& ary)
    {
        _inputArrays.push_back(ary);
    }

    ArrayPtr const& Array::getPipe(size_t index) const
    {
        SCIDB_ASSERT(index < _inputArrays.size());
        return _inputArrays[index];
    }

    void Array::setPipe(size_t index, ArrayPtr const& ary)
    {
        SCIDB_ASSERT(index < _inputArrays.size());
        _inputArrays[index] = ary;
    }

    std::string const& Array::getName() const
    {
        return getArrayDesc().getName();
    }

    string Array::getClassName() const
    {
        return demangle_cpp(typeid(*this).name());
    }

    string Array::printPipe(unsigned int indent) const
    {
        string result(indent + 1, '>');
        result += getClassName() + pipeInfo();
        if (_inputArrays.empty()) {
            if (isMaterialized()) {
                // Trivially allowed to have no inputs, no
                // hasInputPipe() check needed.
                result += " (materialized)";
            } else {
                // Array subclasses MUST use Array to manage input
                // pipe(s), or explicitly declare their intent not ot.
                SCIDB_ASSERT(!hasInputPipe());
                result += " (no inputs)";
            }
        } else {
            SCIDB_ASSERT(!isMaterialized());
            for (auto const& a : _inputArrays) {
                result += '\n';
                if (a) {
                    result += a->printPipe(indent + 1);
                } else {
                    // Special case for SplitArray, a DelegateArray
                    // that has no backing input array.  Ugh.
                    result += string(indent + 2, '>') + " (null_pipe)";
                }
            }
        }
        return result;
    }

    ArrayID Array::getHandle() const
    {
        return getArrayDesc().getId();
    }

    void Array::appendHorizontal(ArrayPtr const& input,
                                 CoordinateSet* newChunkCoordinates) {
        append(input, false, newChunkCoordinates);
    }

    void Array::appendVertical(ArrayPtr const& input,
                               CoordinateSet* newChunkCoordinates) {
        append(input, true, newChunkCoordinates);
    }

    // private implementation
    // with luck, will become appendHorizontal and appendVertical
    // will be eliminated
    void Array::append(ArrayPtr const& input,
                       bool const vertical,
                       CoordinateSet* newChunkCoordinates)
    {
        if (vertical) {
            LOG4CXX_WARN(logger, "Array::append() vertical mode is deprecated");
            assert(input->getSupportedAccess() >= MULTI_PASS);
            const auto& attrs = getArrayDesc().getAttributes();
            for (const auto& attr : attrs) {
                std::shared_ptr<ArrayIterator> dst = getIterator(attr);
                std::shared_ptr<ConstArrayIterator> src = input->getConstIterator(attr);
                while (!src->end())
                {
                    if(newChunkCoordinates && attr.getId() == attrs.firstDataAttribute().getId())
                    {
                        newChunkCoordinates->insert(src->getPosition());
                    }
                    dst->copyChunk(src->getChunk());
                    ++(*src);
                }
            }
        }
        else
        {
            size_t nAttrs = getArrayDesc().getAttributes().size();
            std::vector< std::shared_ptr<ArrayIterator> > dstIterators(nAttrs);
            std::vector< std::shared_ptr<ConstArrayIterator> > srcIterators(nAttrs);
            const auto& attrs = getArrayDesc().getAttributes();
            for (const auto& attr : attrs)
            {
                dstIterators[attr.getId()] = getIterator(attr);
                srcIterators[attr.getId()] = input->getConstIterator(attr);
            }
            const auto& fda = attrs.firstDataAttribute();
            while (!srcIterators[fda.getId()]->end())
            {
                if(newChunkCoordinates)
                {
                    newChunkCoordinates->insert(srcIterators[fda.getId()]->getPosition());
                }
                for (const auto& attr : attrs)
                {
                    std::shared_ptr<ArrayIterator>& dst = dstIterators[attr.getId()];
                    std::shared_ptr<ConstArrayIterator>& src = srcIterators[attr.getId()];
                    if (dst && src) {
                        dst->copyChunk(src->getChunk());
                        ++(*src);
                    }
                }
            }
        }
    }

    std::shared_ptr<CoordinateSet> Array::getChunkPositions() const
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR) << "calling getChunkPositions on an invalid array";
    }

    std::shared_ptr<CoordinateSet> Array::findChunkPositions() const
    {
        if (hasChunkPositions())
        {
            return getChunkPositions();
        }
        if (getSupportedAccess() == SINGLE_PASS)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_UNSUPPORTED_INPUT_ARRAY) << "findChunkPositions";
        }
        // Heuristic: make an effort to scan the empty tag. If there is no empty tag - scan the
        // smallest fixed-sized attribute.  If an attribute is small in size (bool or uint8),
        // chances are it takes less disk scan time and/or less compute time to pass over it.
        ArrayDesc const& schema = getArrayDesc();
        AttributeDesc const* attributeToScan = nullptr;
        if (!schema.getAttributes().hasEmptyIndicator())
        {
            //The array doesn't have an empty tag. Let's pick the smallest fixed-size attribute.
            attributeToScan = &schema.getAttributes().firstDataAttribute();
            size_t scannedAttributeSize = attributeToScan->getSize();
            auto aiter = schema.getAttributes().begin();
            ++aiter;  // skip the first data attributes since we've already looked at it
            while (aiter != schema.getAttributes().end())
            {
                AttributeDesc const& candidate = *aiter;
                size_t candidateSize = candidate.getSize();
                if ( candidateSize != 0 && (candidateSize < scannedAttributeSize || scannedAttributeSize == 0))
                {
                    attributeToScan = &candidate;
                    scannedAttributeSize = candidateSize;
                }
            }
        }
        else {
            attributeToScan = schema.getAttributes().getEmptyBitmapAttribute();
        }
        assert(attributeToScan != NULL);
        std::shared_ptr<CoordinateSet> result(new CoordinateSet());
        //Iterate over the target attribute, find the position of each chunk, add all chunk positions to result
        std::shared_ptr<ConstArrayIterator> iter = getConstIterator(*attributeToScan);
        while( ! iter->end() )
        {
            result->insert(iter->getPosition());
            ++(*iter);
        }
        return result;
    }

    size_t Array::extractData(AttributeID attrID, void* buf,
                              Coordinates const& first, Coordinates const& last,
                              Array::extractInit_t init,
                              extractNull_t null) const
    {
        ArrayDesc const& arrDesc = getArrayDesc();
        auto iter = std::find_if(arrDesc.getAttributes().begin(),
                                 arrDesc.getAttributes().end(),
                                 [attrID] (const auto& elem) {
                                     return elem.getId() == attrID;
                                 });
        assert(iter != arrDesc.getAttributes().end());
        AttributeDesc const& attrDesc = *iter;
        Type attrType( TypeLibrary::getType(attrDesc.getType()));
        Dimensions const& dims = arrDesc.getDimensions();
        size_t nDims = dims.size();
        if (attrType.variableSize()) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION,
                                 SCIDB_LE_EXTRACT_EXPECTED_FIXED_SIZE_ATTRIBUTE);
        }

        if (attrType.bitSize() < 8) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION,
                                 SCIDB_LE_EXTRACT_UNEXPECTED_BOOLEAN_ATTRIBUTE);
        }

        if (first.size() != nDims || last.size() != nDims) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION,
                                 SCIDB_LE_WRONG_NUMBER_OF_DIMENSIONS);
        }

        size_t bufSize = 1;
        for (size_t j = 0; j < nDims; j++) {
            if (last[j] < first[j] ||
                (first[j] - dims[j].getStartMin()) % dims[j].getChunkInterval() != 0) {
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_UNALIGNED_COORDINATES)
                    << dims[j];
            }
            bufSize *= last[j] - first[j] + 1;
        }

        size_t attrSize = attrType.byteSize();

        switch(init) {
        case EXTRACT_INIT_ZERO:
            memset(buf, 0, bufSize*attrSize);
            break;
        case EXTRACT_INIT_NAN:
            {
                TypeEnum typeEnum = typeId2TypeEnum(attrType.typeId());
                if (typeEnum == TE_FLOAT) {
                    float * bufFlt = reinterpret_cast<float*>(buf);
                    std::fill(bufFlt, bufFlt+bufSize, NAN);
                } else if (typeEnum == TE_DOUBLE) {
                    double * bufDbl = reinterpret_cast<double*>(buf);
                    std::fill(bufDbl, bufDbl+bufSize, NAN);
                } else {
                    assert(false); // there is no such thing as NAN for these types.  The calling programmer made a serious error.
                    SCIDB_UNREACHABLE();
                }
            }
            break;
        default:
            SCIDB_UNREACHABLE(); // all cases should have been enumerated.;
        }

        size_t nExtracted = 0;
        for (std::shared_ptr<ConstArrayIterator> i = getConstIterator(attrDesc);
             !i->end(); ++(*i)) {
            size_t j, chunkOffs = 0;
            ConstChunk const& chunk = i->getChunk();
            Coordinates const& chunkPos = i->getPosition();
            for (j = 0; j < nDims; j++) {
                if (chunkPos[j] < first[j] || chunkPos[j] > last[j]) {
                    break;
                }
                chunkOffs *= last[j] - first[j] + 1;
                chunkOffs += chunkPos[j] - first[j];
            }
            if (j == nDims) {
                for (std::shared_ptr<ConstChunkIterator> ci =
                         chunk.getConstIterator(ChunkIterator::IGNORE_OVERLAPS |
                                                ChunkIterator::IGNORE_NULL_VALUES);
                     !ci->end(); ++(*ci)) {
                    Value const& v = ci->getItem();
                    if (!v.isNull()) {
                        Coordinates const& itemPos = ci->getPosition();
                        size_t itemOffs = 0;
                        for (j = 0; j < nDims; j++) {
                            itemOffs *= last[j] - first[j] + 1;
                            itemOffs += itemPos[j] - first[j];
                        }
                        memcpy((char*)buf + itemOffs*attrSize,
                               ci->getItem().data(), attrSize);
                    } else if (null==EXTRACT_NULL_AS_NAN) {
                        Coordinates const& itemPos = ci->getPosition();
                        size_t itemOffs = 0;
                        for (j = 0; j < nDims; j++) {
                            itemOffs *= last[j] - first[j] + 1;
                            itemOffs += itemPos[j] - first[j];
                        }
                        TypeEnum typeEnum = typeId2TypeEnum(attrType.typeId());
                        // historically, no alignment guarantee on buf
                        char * itemAddr = (char*)buf + itemOffs*attrSize;
                        if (typeEnum == TE_FLOAT) {
                            float nan=NAN;
                            std::copy((char*)&nan, (char*)&nan + sizeof(nan), itemAddr);
                        } else if (typeEnum == TE_DOUBLE) {
                            double nan=NAN;
                            std::copy((char*)&nan, (char*)&nan + sizeof(nan), itemAddr);
                        } else {
                            SCIDB_UNREACHABLE(); // there is no such thing as NaN for other types.  The calling programmer made a serious error.
                        }
                    } else { // EXTRACT_NULL_AS_EXCEPTION
                        throw USER_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "NULL to non-nullable operator";
                    }
                }
                nExtracted += 1;
            }
        }
        return nExtracted;
    }

    std::shared_ptr<ArrayIterator> Array::getIterator(const AttributeDesc& attr)
    {
        return getIteratorImpl(attr);
    }

    std::shared_ptr<ArrayIterator> Array::getIteratorImpl(const AttributeDesc& attr)
    {
        throw USER_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Array::getIteratorImpl";
    }

    std::shared_ptr<ConstArrayIterator> Array::getConstIterator(const AttributeDesc& attr) const
    {
        return getConstIteratorImpl(attr);
    }

    std::shared_ptr<ConstItemIterator> Array::getItemIterator(const AttributeDesc& attr, int iterationMode) const
    {
        return getItemIteratorImpl(attr, iterationMode);
    }

    std::shared_ptr<ConstItemIterator> Array::getItemIteratorImpl(const AttributeDesc& attrID, int iterationMode) const
    {
        return std::shared_ptr<ConstItemIterator>(new ConstItemIterator(*this, attrID, iterationMode));
    }

    bool Array::isCountKnown() const
    {
        // if an array keeps track of the count in substantially less time than traversing
        // all chunks of an attribute, then that array should override this method
        // and return true, and override count() [below] with a faster method.
        return false;
    }

    size_t Array::count() const
    {
        // if there is a way to get the count in O(1) time, or without traversing
        // a set of chunks, then that should be done here, when that becomes possible

        if (getSupportedAccess() == SINGLE_PASS)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_UNSUPPORTED_INPUT_ARRAY) << "findChunkElements";
        }

        //Heuristic: make an effort to scan the empty tag. If there is no empty tag - scan the smallest fixed-sized attribute.
        //If an attribute is small in size (bool or uint8), chances are it takes less disk scan time and/or less compute time to pass over it.
        ArrayDesc const& schema = getArrayDesc();
        AttributeDesc const* attributeToScan = nullptr;
        if (!schema.getAttributes().hasEmptyIndicator())
        {
            //The array doesn't have an empty tag. Let's pick the smallest fixed-size attribute.
            attributeToScan = &schema.getAttributes().firstDataAttribute();
            size_t scannedAttributeSize = attributeToScan->getSize();
            auto iter = schema.getAttributes().begin();
            ++iter;  // already looked at the first one
            while (iter != schema.getAttributes().end())
            {
                AttributeDesc const& candidate = *iter;
                ++iter;
                size_t candidateSize = candidate.getSize();
                if ( candidateSize != 0 && (candidateSize < scannedAttributeSize || scannedAttributeSize == 0))
                {
                    attributeToScan = &candidate;
                    scannedAttributeSize = candidateSize;
                }
            }
        }
        else {
            attributeToScan = schema.getEmptyBitmapAttribute();
        }
        assert(attributeToScan != NULL);
        size_t result = 0;
        //Iterate over the target attribute, find nElements of each chunk, add such values to the result
        std::shared_ptr<ConstArrayIterator> iter = getConstIterator(*attributeToScan);
        while( ! iter->end() )
        {
            ConstChunk const& curChunk = iter->getChunk();
            result += curChunk.count();
            ++(*iter);
        }
        return result;
    }


    void Array::printArrayToLogger() const
    {
        // This function is only usable in debug builds, otherwise it is a no-op
#ifndef NDEBUG
        auto nattrs = getArrayDesc().getAttributes(true).size();

        vector< std::shared_ptr<ConstArrayIterator> > arrayIters(nattrs);
        vector< std::shared_ptr<ConstChunkIterator> > chunkIters(nattrs);
        vector<TypeId> attrTypes(nattrs);

        LOG4CXX_DEBUG(logger, "[printArray] name (" << this->getName() << ")");

        for (const auto& attr : getArrayDesc().getAttributes(true))
        {
            arrayIters[attr.getId()] = this->getConstIterator(attr);
            attrTypes[attr.getId()] = attr.getType();
        }

        while (!arrayIters[0]->end())
        {
            for (const auto& attr : getArrayDesc().getAttributes(true))
            {
                chunkIters[attr.getId()] = arrayIters[attr.getId()]->getChunk().getConstIterator();
            }

            while (!chunkIters[0]->end())
            {
                vector<Value> item(nattrs);
                stringstream ssvalue;
                stringstream sspos;

                ssvalue << "( ";
                for (const auto& attr : getArrayDesc().getAttributes(true))
                {
                    item[attr.getId()] = chunkIters[attr.getId()]->getItem();
                    ssvalue << item[attr.getId()].toString(attrTypes[attr.getId()]) << " ";
                }
                ssvalue << ")";

                sspos << "( ";
                for (size_t i = 0; i < chunkIters[0]->getPosition().size(); i++)
                {
                    sspos << chunkIters[0]->getPosition()[i] << " ";
                }
                sspos << ")";

                LOG4CXX_DEBUG(logger, "[PrintArray] pos " << sspos.str() << " val " << ssvalue.str());

                for (const auto& attr : getArrayDesc().getAttributes(true))
                {
                    ++(*chunkIters[attr.getId()]);
                }
            }

            for (const auto& attr : getArrayDesc().getAttributes(true))
            {
                ++(*arrayIters[attr.getId()]);
            }
        }
#endif
    }

} // namespace scidb
