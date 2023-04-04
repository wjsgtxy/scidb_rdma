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

#ifndef SYNCHABLE_ARRAY_H
#define SYNCHABLE_ARRAY_H

#include <array/Array.h>

namespace scidb {

/// An interface for an Array which requires a (implementation-dependent) synchronization point
class SynchableArray : virtual public Array
{
public:
    virtual void sync() = 0;
    virtual ~SynchableArray() {}
    SynchableArray(const SynchableArray&) = delete;
    SynchableArray& operator=(const SynchableArray&) = delete;
protected:
    SynchableArray() {}
};

}  // scidb

#endif  /* ! SYNCHABLE_ARRAY_H */
