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

/**
 * @file ScopedQueryThread.h
 */

#ifndef SCOPED_QUERY_THREAD_H
#define SCOPED_QUERY_THREAD_H

#include <query/Query.h>


namespace scidb {


class ScopedActiveQueryThread
{
public:
    ScopedActiveQueryThread(std::shared_ptr<Query>& query);
    ~ScopedActiveQueryThread();

private:
    // State for cooperating with Job::StackHelper in managing the
    // per-thread query pointer.
    std::weak_ptr<Query> _savedQuery;
    bool _restoreQuery { false };

    // *This* RAII object (unlike StackHelper) can never be nested
    // twice on the same stack.  This thread_local ensures that.
    static thread_local bool _activated;
};

} // namespace scidb

#endif // SCOPED_QUERY_THREAD_H
