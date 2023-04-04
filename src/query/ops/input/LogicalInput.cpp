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
 * @file LogicalInput.cpp
 * @author roman.simakov@gmail.com
 * @brief Input operator for reading data from external files.
 */

#include "LogicalInput.h"

#include "ChunkLoader.h"
#include "InputArray.h"

#include <array/ArrayDistribution.h>
#include <array/Dense1MChunkEstimator.h>
#include <query/LogicalOperator.h>
#include <query/Query.h>
#include <query/UserQueryException.h>
#include <rbac/Rights.h>
#include <system/Cluster.h>
#include <system/Resources.h>
#include <system/Warnings.h>

#include <log4cxx/logger.h>

using namespace std;
namespace bfs = boost::filesystem;

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.ops.input"));
static constexpr char const * const cls = "LogicalInput::";

namespace scidb
{

LogicalInput::LogicalInput(const string& logicalName, const string& alias)
  : LogicalOperator(logicalName, alias)
{ }

PlistSpec const* LogicalInput::makePlistSpec()
{
    static PlistSpec argSpec {

        { "", // positionals
          RE(RE::LIST, {
             RE(PP(PLACEHOLDER_SCHEMA)),
             RE(PP(PLACEHOLDER_CONSTANT, TID_STRING)), // filename
             RE(RE::QMARK, {
                RE(RE::OR, {
                   RE(PP(PLACEHOLDER_CONSTANT, TID_INT64)), // physical instance_id
                   RE(RE::GROUP, {
                      RE(PP(PLACEHOLDER_CONSTANT, TID_INT32)), // server_id
                      RE(PP(PLACEHOLDER_CONSTANT, TID_INT32))  // server_instance_id
                   })
                }),
                RE(RE::QMARK, {
                   RE(PP(PLACEHOLDER_CONSTANT, TID_STRING)), // format
                   RE(RE::QMARK, {
                      RE(PP(PLACEHOLDER_CONSTANT, TID_INT64)), // maxErrors
                      RE(RE::QMARK, {
                         RE(PP(PLACEHOLDER_CONSTANT, TID_BOOL)) // strict
                      })
                   })
                })
             })
          })
        },

        // keywords
        { InputSettings::KW_INSTANCE,
            RE(RE::OR, {
               RE(PP(PLACEHOLDER_CONSTANT, TID_INT64)), // physical instance_id
               RE(RE::GROUP, {
                  RE(PP(PLACEHOLDER_CONSTANT, TID_INT32)), // server_id
                  RE(PP(PLACEHOLDER_CONSTANT, TID_INT32))  // server_instance_id
               })
            })
        },
        { InputSettings::KW_FORMAT, RE(PP(PLACEHOLDER_CONSTANT, TID_STRING)) },
        { InputSettings::KW_MAX_ERRORS, RE(PP(PLACEHOLDER_CONSTANT, TID_INT64)) },
        { InputSettings::KW_STRICT, RE(PP(PLACEHOLDER_CONSTANT, TID_BOOL)) },
        { InputSettings::KW_CHUNK_HINT,
          RE(RE::GROUP, { RE(RE::PLUS, { RE(PP(PLACEHOLDER_CONSTANT, TID_INT64)) }) })
        },
    };

    return &argSpec;
}

ArrayDesc LogicalInput::inferSchema(vector<ArrayDesc> inputSchemas, std::shared_ptr<Query> query)
{
    SCIDB_ASSERT(inputSchemas.empty());

    _settings.parse(_parameters, _kwParameters, *query);

    InstanceID instanceID = _settings.getInstanceId();
    if (instanceID == ALL_INSTANCE_MASK)
    {
        //Distributed loading let's check file existence on all instances
        map<InstanceID, bool> instancesMap;
        Resources::getInstance()->fileExists(_settings.getPath(), instancesMap, query);

        bool fileDetected = false;
        vector<InstanceID> instancesWithoutFile;
        for (auto const& mapping : instancesMap)
        {
            if (mapping.second)
            {
                if (!fileDetected)
                    fileDetected = true;
            }
            else
            {
                //Remembering file name on each missing file
                LOG4CXX_WARN(logger, "File '" << _settings.getPath()
                             << "' not found on instance #" << mapping.first);
                instancesWithoutFile.push_back(mapping.first);
            }
        }

        //Such file not found on any instance. Failing with exception
        if (!fileDetected)
        {
            throw USER_QUERY_EXCEPTION(
                SCIDB_SE_INFER_SCHEMA, SCIDB_LE_FILE_NOT_FOUND,
                _parameters[1]->getParsingContext()) << _settings.getPath();
        }

        //If some instances missing this file posting appropriate warning
        if (instancesWithoutFile.size())
        {
            stringstream instancesList;
            for (size_t i = 0, count = instancesWithoutFile.size();  i < count; ++i)
            {
                instancesList << Iid(instancesWithoutFile[i]) << (i == count - 1 ? "" : ", ");
            }
            LOG4CXX_WARN(logger, "File " << _settings.getPath()
                         << " not found on instances " << instancesList.str());
            query->postWarning(SCIDB_WARNING(SCIDB_LE_FILE_NOT_FOUND_ON_INSTANCES)
                               << _settings.getPath() << instancesList.str());
        }

        // Even if the chunks are distributed perfectly across the instances, there is no guarantee that
        // the distribution will be level (for example, the HASHED distribution).
        // Therefore, the array size cannot be bounded ... instances need to keep moving beyond the dimensions
        // that would be filled densely and skip forward an arbitrary amount to find another chunk that maps
        // to that instance.  Therefore, the schema for a parallel load must be unbounded in the first
        // (slowest-changing) dimension (or, for dataframes, the DF_SEQ_DIM dimension).
        //
        // The exception is the 'opaque' format, which contains embedded chunk positions and does
        // not rely on ChunkLoader::nextImplicitChunkPosition() to navigate the array dimensions.
        // For opaque parallel loads we can skip the check, since an optimizer-inserted SG will put
        // things right.
        //
        bool isOpaque = !compareStringsIgnoreCase(_settings.getFormat(), "opaque");
        if (!isOpaque) {
            const ArrayDesc& arrayDesc = ((std::shared_ptr<OperatorParamSchema>&)_parameters[0])->getSchema();
            Dimensions const& dims = arrayDesc.getDimensions();
            size_t unboundedDim = _settings.dataframeMode() ? DF_SEQ_DIM : 0;
            if(!dims[unboundedDim].isMaxStar()) {
                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_INPUT_PLOAD_BOUNDED,
                _parameters[0]->getParsingContext());
            }
        }
    }
    else if (instanceID == COORDINATOR_INSTANCE_MASK)
    {
        //This is loading from local instance. Throw error if file not found.
        string const& path = _settings.getPath();
        if (not Resources::getInstance()->fileExists(path, query->getInstanceID(), query))
        {
            throw USER_QUERY_EXCEPTION(
                SCIDB_SE_INFER_SCHEMA, SCIDB_LE_FILE_NOT_FOUND,
                _parameters[1]->getParsingContext()) << bfs::absolute(path);
        }
    }
    else
    {
        // From here on, instanceID is logical not physical.
        instanceID = query->mapPhysicalToLogical(instanceID);

        // This is loading from single instance.
        string const& path = _settings.getPath();
        if (!Resources::getInstance()->fileExists(path, instanceID, query))
        {
            throw USER_QUERY_EXCEPTION(
                SCIDB_SE_INFER_SCHEMA, SCIDB_LE_FILE_NOT_FOUND,
                _parameters[1]->getParsingContext()) << path;
        }
    }

    ArrayDesc arrayDesc = ((std::shared_ptr<OperatorParamSchema>&)_parameters[0])->getSchema();
    if (hasOverlap(arrayDesc.getDimensions()) && !iofmt::canReadOverlaps(_settings.getFormat())) {
        throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA,
                                   SCIDB_LE_OP_INPUT_NO_OVERLAP,
                                   _parameters[0]->getParsingContext())
            << _settings.getFormat();
    }
    if (compareStringsIgnoreCase(_settings.getFormat(), "opaque")) {
        // Only estimate for non-opaque input, since opaque input
        // encodes the dimension parameters.
        Dense1MChunkEstimator::estimate(arrayDesc.getDimensions());
    } else if (arrayDesc.isAutochunked()) {
        if (_settings.getChunkHint().empty()) {
            // For opaque input, autochunked schemas are resolved by
            // peeking at the input file.
            OpaqueChunkLoader::reconcileSchema(arrayDesc, _settings.getPath());
        } else {
            // The backup/restore script has given us a reliable hint
            // about the chunk sizes!
            Coordinates const& hint = _settings.getChunkHint();
            Dimensions& dims = arrayDesc.getDimensions();
            LOG4CXX_DEBUG(logger, cls << __func__ << ": Resolving "
                          << dims << " using hint " << CoordsToStr(hint));
            ASSERT_EXCEPTION(hint.size() == dims.size(), "Chunk hint size is wrong?!");
            for (size_t i = 0; i < dims.size(); ++i) {
                if (dims[i].isAutochunked()) {
                    ASSERT_EXCEPTION(hint[i] > 0, "Chunk hint is bogus!");
                    dims[i].setChunkInterval(hint[i]);
                }
            }
        }
    }

    DistType distType = dtUninitialized;
    if (arrayDesc.getDistribution()) {
        distType = arrayDesc.getDistribution()->getDistType();
    }

    ArrayDistPtr dist;
    if (instanceID != ALL_INSTANCE_MASK) {
        // loading from a single file/instance as in
        // load(ARRAY, 'file.x', -2) or input(<XXX>[YYY], 'file.x', 0)
        if (_settings.dataframeMode()) {
            distType = dtDataframe;
            dist = std::make_shared<DataframeDistribution>(DEFAULT_REDUNDANCY, "");
        } else {
            distType = dtLocalInstance;
            dist = std::make_shared<LocalArrayDistribution>((instanceID == COORDINATOR_INSTANCE_MASK) ?
                                                            query->getInstanceID() :
                                                            instanceID);
        }
        LOG4CXX_TRACE(logger, cls << __func__ << ": !ALL_INSTANCE_MASK, ps="<< dist->getDistType());

    } else if (isUninitialized(distType)) {
        // in-line schema currently does not provide a distribution, i.e.
        // input(<XXX>[YYY], 'file.x')
        dist = createDistribution(dtUndefined);
        LOG4CXX_TRACE(logger, cls << __func__ << ": psUninitialized, ps=" << dist->getDistType());
    } else {
        // the user-specified schema will be used for generating the implicit coordinates.
        // NOTICE that the optimizer will still be told dtUndefined for any parallel ingest
        // (e.g.  load(ARRAY, 'file.x', -1, ...)
        // by PhysicalInput::getOutputDistribution() because some input formats (e.g. opaque, text)
        // may specify the data coordinates (in any distribution).
        dist = arrayDesc.getDistribution();
        LOG4CXX_TRACE(logger, cls << __func__ << ": other, ps=" << dist->getDistType());
    }
    SCIDB_ASSERT(dist);

    //Use array name from catalog if possible or generate temporary name
    string inputArrayName = arrayDesc.getName();
    if (inputArrayName.empty())
    {
        inputArrayName = "tmp_input_array";
    }

    return ArrayDesc(inputArrayName,
                     arrayDesc.getAttributes(),
                     arrayDesc.getDimensions(),
                     dist,
                     query->getDefaultArrayResidency(),
                     arrayDesc.getFlags());
}

void LogicalInput::inferAccess(const std::shared_ptr<Query>& query)
{
    LogicalOperator::inferAccess(query);
    _settings.parse(_parameters, _kwParameters, *query);
}

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalInput, "input")

} //namespace
