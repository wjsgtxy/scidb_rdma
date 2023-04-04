/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2014-2019 SciDB, Inc.
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
 * ProjectArray.h
 *
 *  Created on: Nov 25, 2014
 *      Author: Donghui Zhang
 */

#ifndef PROJECTARRAY_H_
#define PROJECTARRAY_H_

#include <vector>
#include <memory>

#include <array/DelegateArray.h>

namespace scidb
{
/**
 * A DelegateArray that is used to deliver a subset of the attributes from an input array, and/or to switch attribute IDs.
 * Internally, it keeps a vector, that projects a dest attrID to a src attrID.
 *
 * @note projection use case:
 *   - You have a std::shared_ptr<Array> called src, with three attributes <Name, Address, EmptyBitmap>.
 *   - You want to get a std::shared_ptr<Array> called dest, with two attributes <Name, EmptyBitmap>.
 *   - Solution: return a ProjectArray with projection = [0, 2].
 *   - Explanation: the dest attribute 0 comes from src attribute 0, and
 *     dest attribute 1 (i.e. index in projection) comes from src attribute 2 (i.e. the value at projection[1]).
 * @note switching-order use case:
 *   - You have the same src array with three attributes <Name, Address, EmptyBitmap>.
 *   - You want to get a dest array with attributes <Address, Name, EmptyBitmap>.
 *   - Solution: return a ProjectArray with projection = [1, 0, 2].
 */
class ProjectArray : public DelegateArray
{
private:
    /**
     * _projection[attrIdInDestArray] = attrIdInSourceArray.
     */
    std::vector<AttributeID> _projection;

public:
    virtual DelegateArrayIterator* createArrayIterator(const AttributeDesc& id) const
    {
        if (id.getId() >= _projection.size()) {
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ARRAYS_NOT_CONFORMANT)
                << "Target attribute is outside of the projection space";
        }
        SCIDB_ASSERT(_projection[id.getId()] < getPipe(0)->getArrayDesc().getAttributes().size());
        const auto& projectedAttr = getPipe(0)->getArrayDesc().getAttributes().findattr((size_t)_projection[id.getId()]);
        return new DelegateArrayIterator(*this, id, getPipe(0)->getConstIterator(projectedAttr));
    }

    ProjectArray(ArrayDesc const& desc, std::shared_ptr<Array> const& array, std::vector<AttributeID> const& projection)
    : DelegateArray(desc, array, true),
      _projection(projection)
    {
    }
};

} // namespace
#endif /* PROJECTARRAY_H_ */
