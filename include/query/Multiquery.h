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

#include <array/ArrayID.h>
#include <array/VersionID.h>
#include <network/proto/scidb_msg.pb.h>
#include <query/QueryID.h>
#include <system/SystemCatalog.h>
#include <util/Pqxx.h>

#include <list>
#include <memory>


/**
 * Multiquery Support Library.
 *
 *
 * Multiquery Execution at a High Level
 *
 * Multi-queries work by allowing one query, the multiquery, to manage
 * the locks and part of the execution for one or more subqueries.
 * When a user submits a multiquery via the mquery() operator,
 * SciDB parses the query into a multiquery and some number of subqueries, one
 * for each AFL statement argument that is passed to mquery().
 * Each one of these subqueries executes to completion before the next
 * one begins.  Once all subqueries have executed, the multiquery completes
 * its own execution, and the entire multiquery is committed.
 *
 * Consider the following example:
 *
 *    mquery(insert(build(f,i),f),
 *           insert(project(apply(f, vnew, 2*i+1), vnew), f))
 *
 * Over the course of this query's lifetime, SciDB will create a multiquery
 * and two subqueries, one for each insert AFL statement.
 * All subqueries do not execute at the same time, rather the first will
 * run to completion, then the second will run afterward, followed last
 * by the multiquery itself committing.
 *
 * The SciDB executor will execute the first subquery and it will create new
 * array versions in the catalog, as well as something called a provisional
 * array version in the provisional_arrays table.  This table exists to track
 * the arrays created by a multiquery for rollback and error handling.  Should
 * a subsequent subquery abort, then previous subqueries in the same
 * multiquery will be rolled-back using the information found in this table.
 * When the multiquery commits, the entries in this table corresponding to the
 * provisional arrays created by this multiquery's subqueries will be erased.
 * This same information is used for startup rollback on an instance failure, too.
 *
 * During query execution, both the multiquery and the currently active subquery
 * will appear in the list('queries') output and either one may be selected for
 * cancelation should the user wish.  From the user's perspective, canceling the
 * current running subquery of a multiquery or canceling the multiquery itself have
 * the same effect and, in both cases, rollback mechanics will be invoked.
 *
 * Assuming that a multiquery statement successfully runs to completion
 * and commits, the provisional_arrays table in the catalog will have no entries
 * pertaining to that now-committed mquery().
 *
 *
 * Multiquery Execution Details
 *
 * When the Translator parses the AFL input string and detects a multiquery,
 * it allows nesting of insert() and delete() operators within within the multiquery.
 * Those are the only permitted subqueries.
 *
 * When the executor infers types over the logical query plan output of the
 * Translator, it marks the query as a multiquery if the root node is a logical
 * operator named "mquery".  If it's any other operator, then the Query is processed
 * as usual.
 *
 * During query preparation in the SciDB executor, the multiquery will take ownership
 * of all locks required for arrays modified by the enclosed query statements.
 *
 * After the multiquery begins execution, the SciDB executor inspects the
 * query and if it is a multiquery, it then spawns a subquery.
 * The new subquery then takes on the N'th query statement from the multiquery
 * and prepares it, with the exception that any locks it tries
 * to acquire take on the Query ID of the multiquery and belong to the multiquery.  This
 * works because while array locks have array version information in them, that
 * information isn't what makes an array lock unique in the catalog (see meta.sql
 * for details).  Thus, the multiquery and the subquery share the array lock, with
 * the multiquery having ownership of it and the subquery using it for acquiring the
 * latest array version information during its execution.
 *
 * After subquery preparation, the physical plan for the subquery is sent to
 * all of the worker instances for execution.  The instances reconstitute the
 * subquery and re-establish its relationship with the multiquery.
 * The coordinator continues execution as well and once all instances have
 * finished executing, the subquery commit finalizers execute.  They do a couple
 * of things:  the query commits the same as any normal query would with respect
 * to new catalog versions but also the provisional array entries are added as
 * mentioned above.  Once this is complete, the SciDB executor continues to the
 * next subquery for the multiquery in the same manner until all subqueries have
 * executed.  Each subquery lives only for the lifetime of its query execution
 * and one subquery is destroyed before the next is spawned and executed.  This
 * guarantees that some later subquery will see the results of an earlier one
 * in the same multiquery.
 *
 * When subquery execution completes and its commit finalizers execute, new
 * error handlers are added to the multiquery to rollback the result of the
 * subquery should a later subquery abort.  A new finalizer is added to
 * the multiquery, which removes the provisional array catalog entries when
 * the multiquery commits; when the multiquery commits successfully, the provisional
 * arrays created by the subquery are no longer provisional.
 */


namespace scidb {

class ArrayDesc;
class LockDesc;
class Query;

using LockPtr = std::shared_ptr<LockDesc>;
using PhysicalPlanRecordPtr = std::shared_ptr<scidb_msg::PhysicalPlan>;
using ProvArray = std::tuple<ArrayUAID, ArrayID, VersionID>;
using ProvArrays = std::vector<ProvArray>;
using QueryPtr = std::shared_ptr<Query>;

namespace mst {

/**
 * Forward a query error on a subquery to its responsible multiquery.
 *
 * When a query of any kind aborts on an instance, it notifies the coordinator.
 * If the coordinator is the instance suffering the failure, it notifies itself;
 * otherwise it sends a message to the coordinator which then, eventually, aborts
 * the query.  This invokes the error handlers, one of which broadcasts a message
 * back to all of the instances to abort as well.  In the case that the aborting
 * query is a subquery, then the multiquery needs to know to abort and rollback.
 * This function is called by the abort broadcast handler on a subquery to
 * notify the multiquery of the error.
 *
 * @param subQuery A pointer to the aborting query.
 */
void forwardError(const QueryPtr& subQuery);

/**
 * Returns a pointer to the active subquery for the given query if the subquery
 * exists, or the passed-in pointer if the subquery does not exist.
 *
 * @param query A query pointer.
 *
 * @return A pointer to the passed query's subquery if it exists or a pointer to
 * the query passed-in.
 */
QueryPtr getActiveQuery(const QueryPtr& query);

/**
 * If the query argument is a subquery, return the id of its corresponding
 * multiquery, otherwise return the argument's own id.
 *
 * @param query A query pointer.
 *
 * @return The QueryID of the argument's multiquery if the argument is a subquery or
 * the QueryID of the argument itself.
 */
QueryID getLockingQueryID(const QueryPtr& query);

/**
 * Record provisional array metadata, created by a subquery during a compound query's
 * lifetime, into the catalog for a given instance.
 *
 * @param subQuery The subquery generating the provisional array.
 * @param lock The LockDesc pointer to the array version information.
 */
void installFinalizers(const QueryPtr& subQuery,
                       const LockPtr& lock);

/**
 * When an instance restarts, it will inspect its entries in the provisional_array
 * catalog table and rollback its local array data corresponding to those entries.
 *
 * @param instanceID This instance's ID as noted in the data store header.
 */
void rollbackOnStartup(const InstanceID& instanceID);

/**
 * Invoked by a worker instance when processing the mtPreparePhysicalPlan message,
 * this prepares the query to run on a worker instance by extracting any relevant
 * mquery() information from the message and attaching it to the query.
 *
 * @param query A query to setup on this worker instance.
 * @param record A reference to the physical plan message sent by the coordinator
 * to this worker.
 */
void setupQueryOnWorker(const QueryPtr& query,
                        const PhysicalPlanRecordPtr& record);

/**
 * Invoked by the coordinator, this updates the physical plan message with
 * multiquery information.
 *
 * @param query A query pointer.
 * @param record A pointer to the physical plan message that the coordinator
 * will populate.
 */
void updatePhysPlanMessage(const QueryPtr& query,
                           const PhysicalPlanRecordPtr& record);

}  // namespace mst
}  // namespace scidb
