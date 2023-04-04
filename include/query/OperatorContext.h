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
 * @file OperatorContext.h
 *
 * @brief Operator ID
 */

#ifndef OPERATOR_CONTEXT_H_
#define OPERATOR_CONTEXT_H_

namespace scidb
{

/**
 * Base class for context operator may need to provide in order to implement
 * remote operations in support of things like scatter-gather (SG).
 * A facility is provided so that OperatorID's, which are sent in messages
 * can be looked up to find the relevant operator object, much as
 * can be done with QueryIDs.
 *
 * Originally, these were supported by Query, but that had the limitation that
 * only one OperatorContext could be active per query at any one time, preventing
 * pipelining of queries with multiple SGs  (and every query that returns data
 * to the user has an implicit final SG back to the coordinator).
 *
 * The one-per-query limitation of the past also gave rise to hacks like "storing SG".
 * The SG supported by OperatorContext does not have those issues.
 *
 * Since the particular operator knows what derived type of operator context it
 * provided, it is safe for the operator to downcast the base OperatorContext into
 * its own relevant derived type of OperatorContext.
 *
 */
class OperatorContext
{
public:
    OperatorContext() {}
    virtual ~OperatorContext() = 0;
};

} // namespace scidb

#endif /* OPERATOR_CONTEXT_H_ */
