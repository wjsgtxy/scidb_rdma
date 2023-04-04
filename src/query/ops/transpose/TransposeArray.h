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
 * @file TransposeArray.h
 *
 * @brief Transpose array implementation
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 * @author poliocough@gmail.com
 */

#ifndef TRANSPOSE_ARRAY_H
#define TRANSPOSE_ARRAY_H

#include <map>

#include <array/DelegateArray.h>

namespace scidb {


/**
 * Internal structure used by operator transpose. Not documented.
 */
class TransposeArray: public Array
{
public:
    TransposeArray(ArrayDesc const& arrayDesc,
            std::shared_ptr<Array>const& input,
            std::shared_ptr<CoordinateSet>const& inputChunkPositions,
            std::shared_ptr<Query>const& query):
        Array(input),
        _arrayDesc(arrayDesc),
        _nDimensions(input->getArrayDesc().getDimensions().size()),
        _outputChunkPositions(new CoordinateSet())
    {
        SCIDB_ASSERT(query);
        _query=query;

        Coordinates outCoords(_nDimensions);
        SCIDB_ASSERT(inputChunkPositions);
        for (auto const& inCoords : *inputChunkPositions)
        {
            transposeCoordinates(inCoords, outCoords);
            _outputChunkPositions->insert( outCoords );
        }
    }

    virtual ~TransposeArray()
    {}

    ArrayDesc const& getArrayDesc() const override
    {
        return _arrayDesc;
    }

    bool hasChunkPositions() const override
    {
        return true;
    }

    std::shared_ptr<CoordinateSet> getChunkPositions() const override
    {
        return std::shared_ptr<CoordinateSet> (new CoordinateSet( *_outputChunkPositions ));
    }

    std::shared_ptr<ConstArrayIterator> getConstIteratorImpl(const AttributeDesc& attrID) const override
    {
        //The TransposeArrayIterator will only fill in the extra empty bitmask if emptyTagID is not the same as attrID.
        //If the array is not emptyable, or if attrID is already the empty bitmask - don't bother.
        const AttributeDesc* emptyTagID = &attrID;
        if (_arrayDesc.getEmptyBitmapAttribute())
        {
            emptyTagID = _arrayDesc.getEmptyBitmapAttribute();
        }

        SCIDB_ASSERT(emptyTagID);
        return std::shared_ptr<ConstArrayIterator>( new TransposeArrayIterator(_outputChunkPositions,
                                                                                 getPipe(0)->getConstIterator(attrID),
                                                                                 _query,
                                                                                 this,
                                                                                 attrID,
                                                                                 *emptyTagID));
    }

    void transposeCoordinates(Coordinates const& in, Coordinates& out) const
    {
        SCIDB_ASSERT(in.size() == _nDimensions && out.size() == _nDimensions);
        for (size_t i = 0; i < _nDimensions; ++i)
        {
            out[_nDimensions-i-1] = in[i];
        }
    }

private:
    ArrayDesc _arrayDesc;
    size_t const _nDimensions;
    std::shared_ptr<CoordinateSet> _outputChunkPositions;

    class TransposeArrayIterator: public ConstArrayIterator
    {
    public:
        TransposeArrayIterator(std::shared_ptr<CoordinateSet> const& outputChunkPositions,
                               std::shared_ptr<ConstArrayIterator> inputArrayIterator,
                               std::weak_ptr<Query> const& query,
                               TransposeArray const* transposeArray,
                               const AttributeDesc& attributeID,
                               const AttributeDesc& emptyTagID):
            ConstArrayIterator(*transposeArray),
            _outputChunkPositions(outputChunkPositions),
            _outputChunkPositionsIterator(_outputChunkPositions->begin()),
            _inputArrayIterator(inputArrayIterator),
            _query(query),
            _transposeArray(transposeArray),
            _attributeID(attributeID),
            _emptyTagID(emptyTagID),
            _chunkInitialized(false)
        {
            SCIDB_ASSERT(transposeArray);
        }

        bool end() override
        {
            return _outputChunkPositionsIterator == _outputChunkPositions->end();
        }

        void operator ++() override
        {
            if( end() )
            {
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_CHUNK);
            }
            _chunkInitialized = false;
            ++_outputChunkPositionsIterator;
        }

        void restart() override
        {
            _chunkInitialized = false;
            _outputChunkPositionsIterator == _outputChunkPositions->begin();
        }

        Coordinates const& getPosition() override
        {
            if( end() )
            {
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_CHUNK);
            }
            return (*_outputChunkPositionsIterator);
        }

        bool setPosition(Coordinates const& pos) override
        {
            _chunkInitialized = false;
            Coordinates chunkPosition = pos;
            _transposeArray->getArrayDesc().getChunkPositionFor(chunkPosition);
            _outputChunkPositionsIterator = _outputChunkPositions->find(chunkPosition);
            return !end();
        }

        ConstChunk const& getChunk() override;

    private:
        std::shared_ptr<CoordinateSet> _outputChunkPositions;
        CoordinateSet::const_iterator _outputChunkPositionsIterator;
        std::shared_ptr<ConstArrayIterator> _inputArrayIterator;
        std::weak_ptr<Query> _query;
        TransposeArray const* _transposeArray;
        AttributeDesc _attributeID;
        AttributeDesc _emptyTagID;
        bool _chunkInitialized;
        MemChunk _outputChunk;
        MemChunk _emptyTagChunk;
    };
};


}

#endif
