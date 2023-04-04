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
 * @file PhysicalInput.cpp
 *
 * @author roman.simakov@gmail.com
 *
 * Physical implementation of INPUT operator for inputing data from text file
 * which is located on coordinator
 */

#include "InputArray.h"
#include "InputSettings.h"

#include <query/PhysicalOperator.h>
#include <query/LogicalOperator.h>
#include <query/PhysicalUpdate.h>
#include <query/Query.h>
#include <query/QueryProcessor.h>
#include <system/Cluster.h>
#include <util/IoFormats.h>
#include <util/PathUtils.h>

#include <log4cxx/logger.h>
#include <string.h>

using namespace std;

namespace scidb
{

// Logger for network subsystem. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.ops.impl_input"));
static constexpr char const * const cls = "PhysicalInput::";

class PhysicalInput : public PhysicalOperator
{
public:
    PhysicalInput(std::string const& logicalName,
                  std::string const& physicalName,
                  Parameters const& parameters,
                  ArrayDesc const& schema)
        : PhysicalOperator(logicalName, physicalName, parameters, schema)
    { }

    bool isSingleThreaded() const override
    {
        return false;
    }

    void inspectLogicalOp(LogicalOperator const& lop) override
    {
        // Learn the InputSettings computed in our logical operator.  Putting this info into the
        // control cookie ensures that all workers can learn it too.  Finally, use it to init our
        // own _settings object so it's ready for preSingleExecute().

        string inspectable(lop.getInspectable());
        setControlCookie(inspectable);
        _settings.fromString(inspectable);
    }

    int64_t getSourceInstanceID() const
    {
        SCIDB_ASSERT(_settings.isValid());
        return _settings.getInstanceId();
    }

    bool isForcingSg() const
    {
        // Ordinary arrays: always SG on parallel load, since the
        // distribution could be violated.  Low performance, but
        // currently the only way to ensure correct distribution.
        //
        // Dataframes: Goal is to *avoid* SG on input, since we expect
        // the data to be redimensioned anyhow.  However, we *do* want
        // to SG if the input format contains explicit chunk
        // coordinates.  We want to get the dataframe chunks back to
        // their home instances prior to any possible store(), to
        // accurately recreate the dataframe.  Otherwise a dataframe
        // whose chunks were more or less evenly balanced around the
        // cluster will now be forced onto one instance.  While that
        // would be OK from a correctness standpoint, it might lead to
        // unexpected performance horror later.
        //
        // (In practice, at the present time the only *supported*
        // format for which hasCoordinates() returns true is 'opaque'.
        // You may wish to double check.)

        SCIDB_ASSERT(_settings.isValid());
        return _settings.dataframeMode()
            ? iofmt::hasCoordinates(_settings.getFormat())
            : _settings.isParallel();
    }

    bool canToggleStrict() const override
    {
        return true;            // Yes, optimizer can call getIsStrict().
    }

    bool getIsStrict() const override
    {
        SCIDB_ASSERT(_settings.isValid());
        return _settings.getIsStrict();
    }

    /// @see OperatorDist
    DistType inferSynthesizedDistType(std::vector<DistType> const& /*inDist*/, size_t /*depth*/) const override
    {
        std::vector<RedistributeContext> emptyRC;
        std::vector<ArrayDesc> emptyAD;
        auto context = getOutputDistribution(emptyRC, emptyAD); // avoiding duplication of logic
        DistType result = context.getArrayDistribution()->getDistType();
        LOG4CXX_TRACE(logger, cls << __func__ << ": Returning distType=" << result);
        return result;
    }

    virtual RedistributeContext getOutputDistribution(
            std::vector<RedistributeContext> const& inputDistribution,
            std::vector<ArrayDesc> const& inputSchema) const
    {
        SCIDB_ASSERT(inputDistribution.empty());
        SCIDB_ASSERT(inputSchema.empty());
        SCIDB_ASSERT(_schema.getDistribution());

        InstanceID sourceInstanceID = getSourceInstanceID();
        LOG4CXX_TRACE(logger, cls << __func__ << ": sourceInstanceID: " << sourceInstanceID);

        bool forcingSG = isForcingSg();
        LOG4CXX_TRACE(logger, cls << __func__ << ": forcingSG: " << forcingSG);
        if(forcingSG) {
            LOG4CXX_TRACE(logger, cls << __func__ << ": A: using psUndefined to force SG");
            ArrayDistPtr undefDist = createDistribution(dtUndefined);
            if (!_preferredInputDist) {
                // save the original for execute (coordinator only)
                _preferredInputDist = _schema.getDistribution();
                SCIDB_ASSERT(_preferredInputDist);
            }
            _schema.setDistribution(undefDist);
            LOG4CXX_TRACE(logger, cls << __func__ << ": B _schema. updated to dtUndefined");
        } else {
            DistType expected = _settings.dataframeMode() ? dtDataframe : dtLocalInstance;
            SCIDB_ASSERT(_schema.getDistribution()->getDistType() == expected);
        }
        return RedistributeContext(_schema.getDistribution(), _schema.getResidency());
    }

    std::shared_ptr<Array> execute(vector< std::shared_ptr<Array> >& inputArrays,
                                     std::shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 0);
        assert(_parameters.size() >= 2);
        if (!_settings.isValid()) {
            // Already valid on the coordinator (we did it in inspectLogicalOp() so it'd be ready
            // for preSingleExecute()).  For other workers, initialize _settings here.
            _settings.fromString(getControlCookie());
            SCIDB_ASSERT(_settings.isValid());
            LOG4CXX_DEBUG(logger, cls << __func__ << ": settings: " << _settings);
        }

        assert(_parameters[1]->getParamType() == PARAM_PHYSICAL_EXPRESSION);
        std::shared_ptr<OperatorParamPhysicalExpression> paramExpr =
            (std::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[1];
        assert(paramExpr->isConstant());
        const string fileName = _settings.getPath();

        InstanceID myInstanceID = query->getInstanceID();
        InstanceID sourceInstanceID = getSourceInstanceID();
        LOG4CXX_TRACE(logger, cls << __func__ << ": sourceInstanceID: " << sourceInstanceID
                      << ", myInstanceID: " << myInstanceID);

        SCIDB_ASSERT(_schema.getDistribution().get());
        if (sourceInstanceID == COORDINATOR_INSTANCE_MASK) {
            LOG4CXX_TRACE(logger, cls << __func__ << ": 6A. COORDINATOR_INSTANCE_MASK");
            sourceInstanceID = (query->isCoordinator() ? query->getInstanceID() : query->getCoordinatorID());
        } else if (sourceInstanceID != ALL_INSTANCE_MASK) {
            LOG4CXX_TRACE(logger, cls << __func__ << ": 6B. ! ALL_INSTANCE_MASK");
            SCIDB_ASSERT(isValidPhysicalInstance(sourceInstanceID));
            auto distType = _schema.getDistribution()->getDistType();
            // local instance cases...
            if (isLocal(distType)) {
                // if it was valid, why are we changing it to logical here?
                sourceInstanceID = safe_dynamic_cast<const LocalArrayDistribution*>(
                    _schema.getDistribution().get())->getLogicalInstanceId();
                LOG4CXX_TRACE(logger, cls << __func__ <<
                              ": 6B(i). local, sourceInstID=" << sourceInstanceID);
            }
            else {
                // The only other permitted local(ish) distribution.
                SCIDB_ASSERT(isDataframe(distType));
                sourceInstanceID = myInstanceID;
                LOG4CXX_TRACE(logger, cls << __func__ <<
                              ": 6B(ii). dataframe, sourceInstID=" << sourceInstanceID);
            }
        } else {
            LOG4CXX_TRACE(logger, cls << __func__ << ": 6C. parallel, all instances case");
            // Parallel load of non-opaque dataframes does *not* force an SG.
            SCIDB_ASSERT(isForcingSg() || _settings.dataframeMode());
        }

        int64_t maxErrors = _settings.getMaxErrors();
        bool enforceDataIntegrity = _settings.getIsStrict();
        LOG4CXX_TRACE(logger, cls << __func__ << ": 7. isStrict: " << _settings.getIsStrict());

        std::shared_ptr<Array> result;
        bool emptyArray = (sourceInstanceID != ALL_INSTANCE_MASK &&
                           sourceInstanceID != myInstanceID);

        if (_preferredInputDist) {
            // can only happen on coordinator, we restore the distribution just prior to execution
            // now that the converter insertion is over
            _schema.setDistribution(_preferredInputDist);
        }

        ArrayDistPtr distForNextChunk;
        if (isForcingSg()) {
            if (_settings.dataframeMode()) {
                LOG4CXX_TRACE(logger, cls << __func__ << ": 10C: parallel dtDataframe");
                distForNextChunk = createDistribution(dtDataframe);
            } else {
                LOG4CXX_TRACE(logger, cls << __func__ << ": 10A: during a forced SG using defaultDistTypeInput");
                distForNextChunk = createDistribution(defaultDistTypeInput());
            }
        } else {
            LOG4CXX_TRACE(logger, cls << __func__ << ": 10B: using the original _schema distribution");
            SCIDB_ASSERT(_schema.getDistribution().get()); // will not happen on non-coordinator
            SCIDB_ASSERT(not isUndefined(_schema.getDistribution().get()->getDistType()));
            distForNextChunk = _schema.getDistribution();    // the original, whatever it was
        }

        InputArray* ary = new InputArray(_schema,
                                         distForNextChunk,
                                         _settings.getFormat(), query, shared_from_this(),
                                         emptyArray,
                                         enforceDataIntegrity,
                                         maxErrors,
                                         _settings.isParallel());
        result.reset(ary);

        if (emptyArray) {
            // No need to actually open the file.  (In fact, if the file is a pipe and
            // double-buffering is enabled, opening it would wrongly steal data intended for
            // some other instance!  See ticket #4466, aka SDB-1408.)
            SCIDB_ASSERT(ary->inEmptyMode());
        } else {
            try
            {
                ary->openFile(fileName);
            }
            catch(const Exception& e)
            {
                if (e.getLongErrorCode() != SCIDB_LE_CANT_OPEN_FILE)
                {
                    // Only expecting an open failure, but whatever---pass it up.
                    e.raise();
                }

                if (sourceInstanceID == myInstanceID)
                {
                    // If mine is the one-and-only load instance, let
                    // callers see the open failure.
                    e.raise();
                }

                // No *local* file to load, so return the "inEmptyMode"
                // InputArray result. (Formerly that was needed because
                // the "shadow array" feature relied on it.  It currently
                // does not matter, but might if shadow array
                // functionality were re-implemented.)
                // The open failure itself has already been logged.

                assert(ary->inEmptyMode()); // ... regardless of emptyArray value above.
            }
        }

        // Forcing SG implies psUndefined, which is the optimizer's
        // cue to insert an SG.
        SCIDB_ASSERT(!isForcingSg() ||
                     isUndefined(result->getArrayDesc().getDistribution()->getDistType()));

        return result;
    }

private:

    mutable ArrayDistPtr _preferredInputDist;
    mutable InputSettings _settings;
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalInput, "input", "impl_input")

} //namespace
