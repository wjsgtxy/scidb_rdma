/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2015-2019 SciDB, Inc.
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
 * SerializedPtrConverter.h
 *
 *  Created on: May 29, 2015
 *      Author: Donghui Zhang
 */

#ifndef INCLUDE_UTIL_SERIALIZEDPTRCONVERTER_H_
#define INCLUDE_UTIL_SERIALIZEDPTRCONVERTER_H_

#include <memory>
#include <map>

namespace scidb
{
/**
 * Given a type, help the receiver serialize into a std::shared_ptr<T> object.
 * In particular, if two pointers point to the same object, the generated shared_ptr objects should be the same.
 * The class helps achieve that goal by maintaining a map from (void*) to std::shared_ptr<T>.
 * @note we use (void*) instead of (T*), just in case some one overloads operator<(T*,T*).
 * @note It is the caller's job to do synchronization.
 */
template<class T>
class SerializedPtrConverter
{
    typedef std::map<void*, std::shared_ptr<T> > RawPtrToSharedPtr;
    RawPtrToSharedPtr _map;

public:
    /**
     * Given a raw ptr, return the corresponding shared_ptr.
     * @param rawPtr  the raw pointer.
     * @return the corresponding shared_ptr object.
     * @note take a note of rawPtr-->sharedPtr, if the rawPtr is not equal to any existing one.
     */
    std::shared_ptr<T> getSharedPtr(T* const rawPtr)
    {
        typename RawPtrToSharedPtr::iterator it = _map.find(rawPtr);
        if (it == _map.end()) {
            std::shared_ptr<T> shared = std::shared_ptr<T>(rawPtr);
            _map[rawPtr] = shared;
            return shared;
        }
        return it->second;
    }

    /**
     * Clear all recorded <rawPtr, shardPtr> pairs.
     */
    void clear()
    {
        _map.clear();
    }
};

} // namespace scidb

#endif /* INCLUDE_UTIL_SERIALIZEDPTRCONVERTER_H_ */
