#ifndef ___ACCUMULATORARRAY_H___
#define ___ACCUMULATORARRAY_H___
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

#ifndef SCIDB_CLIENT

#include <array/StreamArray.h>

namespace scidb {
//
// Array implementation materializing current chunk
//
class AccumulatorArray : public StreamArray
{
  public:
    AccumulatorArray(std::shared_ptr<Array> pipe,
                     std::shared_ptr<Query>const& query);

  protected:
    ConstChunk const* nextChunk(const AttributeDesc& attId, MemChunk& chunk) override;

  private:
    std::vector<std::shared_ptr<ConstArrayIterator> > iterators;
};
}  // scidb
#endif //SCIDB_CLIENT

#endif
