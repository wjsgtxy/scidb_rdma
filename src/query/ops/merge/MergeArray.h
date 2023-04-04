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
 * @file MergeArray.h
 *
 * @brief The implementation of the array iterator for the merge operator
 *
 */

#ifndef MERGE_ARRAY_H_
#define MERGE_ARRAY_H_

#include <string>
#include <vector>

#include <array/DelegateArray.h>

namespace scidb
{


class MergeArray;
class MergeArrayIterator;
class MergeChunkIterator;


class MergeChunkIterator : public DelegateChunkIterator
{
  public:
    bool end() override;
    bool isEmpty() const override;
    Value const& getItem() override;
    void operator ++() override;
    void restart() override;
    bool setPosition(Coordinates const& pos) override;
    Coordinates const& getPosition() override;

    MergeChunkIterator(std::vector< ConstChunk const* > const& inputChunks, DelegateChunk const* chunk, int iterationMode);

  private:
    int currIterator;
    int mode;
    std::vector< std::shared_ptr<ConstChunkIterator> > iterators;
};

class MergeChunk : public DelegateChunk
{
  public:
    std::vector< ConstChunk const* > inputChunks;

    MergeChunk(DelegateArray const& array, DelegateArrayIterator const& iterator, AttributeID attrID)
    : DelegateChunk(array, iterator, attrID, false) {}
};


class MergeArrayIterator : public DelegateArrayIterator
{
  public:
    bool end() override;
    void operator ++() override;
    void restart() override;
    bool setPosition(Coordinates const& pos) override;
    Coordinates const& getPosition() override;
    ConstChunk const& getChunk() override;
    MergeArrayIterator(MergeArray const& array, const AttributeDesc& attrID);

  private:
    MergeChunk chunk;
    std::vector< std::shared_ptr<ConstArrayIterator> > iterators;
    int currIterator;
    bool isEmptyable;
    ConstChunk const* currentChunk;
};

class MergeArray : public DelegateArray
{
    friend class MergeArrayIterator;
  public:
    virtual DelegateChunkIterator* createChunkIterator(DelegateChunk const* chunk, int iterationMode) const;
    DelegateArrayIterator* createArrayIterator(const AttributeDesc& id) const override;

    /**
     * Get the least restrictive access mode that the array supports. This iterates over all of the input
     * arrays and returns the access mode of the most restrictive input.
     * @return the most restrictive access mode taken across all of the input arrays
     */
    virtual Access getSupportedAccess() const
    {
        Access minimum = RANDOM;
        for(size_t i = 0; i < getPipeCount(); ++i)
        {
            Access arrayAccess = getPipe(i)->getSupportedAccess();
            if (arrayAccess < minimum)
            {
                minimum = arrayAccess;
            }
        }
        return minimum;
    }

    MergeArray(ArrayDesc const& desc, std::vector< std::shared_ptr<Array> > const& inputArrays);
};

}

#endif
