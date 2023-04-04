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
 * JoinArray.h
 *
 *  Created on: Oct 22, 2010
 *      Author: Knizhnik
 */
#ifndef _JOIN_ARRAY_H_
#define _JOIN_ARRAY_H_

#include "array/DelegateArray.h"

namespace scidb {

class JoinEmptyableArray;
class JoinEmptyableArrayIterator;

class JoinChunkIterator : public DelegateChunkIterator
{
  public:
    void operator ++() override;
    bool end() override;
    void restart() override;
    bool setPosition(Coordinates const& pos) override;
    JoinChunkIterator(JoinEmptyableArrayIterator const& arrayIterator,
                      DelegateChunk const* chunk,
                      int iterationMode);

  protected:
    bool join();
    void alignIterators();

    std::shared_ptr<ConstChunkIterator> joinIterator;
    int mode;
    bool hasCurrent;
};


class JoinBitmapChunkIterator : public JoinChunkIterator
{
  public:
    Value& getItem() override;

    JoinBitmapChunkIterator(JoinEmptyableArrayIterator const& arrayIterator,
                            DelegateChunk const* chunk,
                            int iterationMode);

  private:
     Value value;
};


class JoinEmptyableArrayIterator : public DelegateArrayIterator
{
    friend class JoinChunkIterator;
    friend class JoinEmptyableArray;
  public:
    bool setPosition(Coordinates const& pos) override;
    void restart() override;
    void operator ++() override;
    bool end() override;
    ConstChunk const& getChunk() override;
    JoinEmptyableArrayIterator(JoinEmptyableArray const& array,
                               const AttributeDesc& attrID,
                               std::shared_ptr<ConstArrayIterator> inputIterator,
                               std::shared_ptr<ConstArrayIterator> joinIterator,
                               bool chunkLevelJoin);

  private:
    void alignIterators();

    std::shared_ptr<ConstArrayIterator> _joinIterator;
    bool _hasCurrent;
    bool _chunkLevelJoin;
};

class JoinEmptyableArray : public DelegateArray
{
    friend class JoinEmptyableArrayIterator;
    friend class JoinChunkIterator;
  public:
    DelegateChunkIterator* createChunkIterator(DelegateChunk const* chunk, int iterationMode) const override;
    DelegateArrayIterator* createArrayIterator(const AttributeDesc& id) const override;

    JoinEmptyableArray(ArrayDesc const& desc, std::shared_ptr<Array> left, std::shared_ptr<Array> right);

  private:
    std::shared_ptr<Array> _left;
    std::shared_ptr<Array> _right;
    AttributeID _nLeftAttributes;
    const AttributeDesc* _leftEmptyTagPosition;
    const AttributeDesc* _rightEmptyTagPosition;
    const AttributeDesc* _emptyTagPosition;
};

}

#endif
