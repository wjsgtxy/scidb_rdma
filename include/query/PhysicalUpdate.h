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

#ifndef PHYSICAL_UPDATE_H_
#define PHYSICAL_UPDATE_H

#include <query/PhysicalOperator.h>

namespace scidb
{
class LockDesc;

/**
 * A base class for all operators which modify arrays in the system
 * (currently store(), insert(), delete(), and add_attributes()).
 * It takes care of most of the required steps to maintain the system catalog.
 *
 * @note If you derive your physical operator from this class, make
 *       sure to set _properties.updater to true in your logical
 *       operator!
 */
class PhysicalUpdate: public PhysicalOperator
{
    bool _preambleDone;         // Sanity check: did subclass run the preamble?
    bool _deferLockUpdate;      // Indicates preamble should update the lock.

protected:
    std::string _unversionedArrayName;
    ArrayUAID _arrayUAID;
    ArrayID   _arrayID;
    VersionID _lastVersion;
    ArrayDesc _unversionedSchema;
    std::shared_ptr<LockDesc> _lock;

public:
    /// @see scidb::PhysicalOperator::PhysicalOperator
    PhysicalUpdate(const std::string& logicalName,
                   const std::string& physicalName,
                   const Parameters& parameters,
                   const ArrayDesc& schema,
                   const std::string& catalogArrayName);

    std::vector<uint8_t> isReplicatedInputOk(size_t numChildren) const override;

    /// @see scidb::PhysicalOperator::inferSynthesizedDistType
    DistType inferSynthesizedDistType(std::vector<DistType> const& inDist, size_t depth) const override;

    /**
     * Updates the "schema" for the array to be created/updated based on the system catalog
     * It expects the (coordinator) array lock to be already acquired by the query.
     * @see scidb::PhysicalOperator::preSingleExecute
     */
    virtual void preSingleExecute(std::shared_ptr<Query> query);

    /**
     * Performs the necessary steps to update the catalog *if* the query commits
     * @see scidb::PhysicalOperator::postSingleExecute
     */
    virtual void postSingleExecute(std::shared_ptr<Query> query);
    /**
     * Must be implemented in a child class
     * @see scidb::PhysicalOperator::postSingleExecute
     */
    virtual std::shared_ptr< Array> execute(
            std::vector< std::shared_ptr< Array> >&,
            std::shared_ptr<Query>)
    {
        ASSERT_EXCEPTION_FALSE("PhysicalUpdate::execute() must be implemented in a subclass");
    }

    /**
     * On the coordinator collect the array boundaries from all the workers,
     * merge them and record in the versioned array schema.
     * @param [in/out] schema versioned array schema
     * @param [in/out] bounds local array boundaries;
     * on the coordinator contains the global boundaries on exit
     * @param query context
     */
    static void updateSchemaBoundaries(ArrayDesc& schema,
                                       PhysicalBoundaries& bounds,
                                       std::shared_ptr<Query>& query);

    /**
     * Record array in the transient array cache. Implements a callback
     * that is suitable for use as a query finalizer.
     * @see scidb::QueryFinalizer
     */
    void recordTransient(const std::shared_ptr<Array>& array,
                         const std::shared_ptr<Query>& query);

    /**
     * Update transient array in the transient array cache.
     * Updates the transient array attributes in the catalog.
     */
    void updateTransient(const std::shared_ptr<Query>& query);

protected:

    /**
     * Implements a callback that is suitable for use as a query finalizer.
     * Adds the new array/version to the catalog transactionally,
     * i.e. in a Query::Finalizer if the query commits.
     */
    void recordPersistent(const std::shared_ptr<Query>& query);

    /**
     * Runs deferred work from preSingleExecute().
     *
     * @description Some work done by preSingleExecute() must be deferred if the input array is
     * autochunked, since chunk intervals are not yet known.  This method runs the deferred work.
     *
     * @note ALL SUBCLASS execute() METHODS *MUST* CALL THIS as the first action of their execute() method!
     */
    void executionPreamble(std::shared_ptr<Array>&, std::shared_ptr<Query>& query);
};

} // namespace

#endif /* PHSYICAL_UPDATE_H_ */
