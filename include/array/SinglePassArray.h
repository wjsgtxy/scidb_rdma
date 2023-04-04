#ifndef SINGLE_PASS_ARRAY_H
#define SINGLE_PASS_ARRAY_H
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
/// An Array that can enforce the horizontal consumption of attributes
/// (i.e. all attribute chunks at the same position can be forced to be consumed before continuing to the next position)
class SinglePassArray : public StreamArray
{
public:
    void setEnforceHorizontalIteration(bool on) { _enforceHorizontalIteration=on; }
    bool isEnforceHorizontalIteration() const   { return _enforceHorizontalIteration; }
protected:
    /**
     * Get the current sequential index of the attribute "row" being consumed
     */
    virtual size_t getCurrentRowIndex() const = 0;

    /**
     * Move to the specified attribute "row", so that the chunks from that row can be consumed.
     * The subclasses are required to successfully advance forward the row.
     * (if isEnforceHorizontalIteration()==true, only when all attributes of the current row are consumed).
     * @param rowIndex the sequential row index
     * @return false if no more rows/chunks are available (i.e. EOF); true otherwise
     */
    virtual bool moveNext(size_t rowIndex) = 0;

    /**
     * Get chunk for a given attribute from the specified row
     * @param attr attribute ID
     * @param rowIndex attribute row index
     * @return chunk ref
     */
    virtual ConstChunk const& getChunk(AttributeID attr, size_t rowIndex) = 0;

    /// Constructor
    SinglePassArray(ArrayDesc const& arr);

    /// @see Array::getConstIterator
    std::shared_ptr<ConstArrayIterator> getConstIteratorImpl(const AttributeDesc& attId) const override;

    /**
     * @see StreamArray::nextChunk
     * @throws scidb::StreamArray::RetryException if setEnforceHorizontalIteration(true) has been called AND
     * nextChunk() needs to be called again because the next chunk(row) is not ready.
     * The next chunk should be ready when all the attribute chunks at the same position are consumed
     * (i.e. the entire attribute "row" is consumed).
     * @note This method is NOT thread-safe.
     */
    ConstChunk const* nextChunk(const AttributeDesc& attId, MemChunk& chunk) override;

    /// @return true if the chunk is considered non-empty
    bool hasValues(const ConstChunk* chunk);
private:
    mutable Mutex _mutex;
    bool _enforceHorizontalIteration;
    size_t _consumed;
    std::vector<size_t> _rowIndexPerAttribute;
private:
    SinglePassArray();
    SinglePassArray(const SinglePassArray&);
    SinglePassArray& operator=(const SinglePassArray&);
};
}  // scidb
#endif //SCIDB_CLIENT

#endif
