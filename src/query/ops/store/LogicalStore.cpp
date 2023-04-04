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

/*
 * LogicalStore.cpp
 *
 *  Created on: Apr 17, 2010
 *      Author: Knizhnik
 */

#include "UniqueNameAssigner.h"

#include <array/ArrayName.h>
#include <query/LogicalExpression.h>
#include <query/LogicalOperator.h>
#include <system/SystemCatalog.h>
#include <system/Exceptions.h>
#include <query/Query.h>
#include <rbac/Rights.h>

#include <log4cxx/logger.h>

using namespace std;

namespace scidb {
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.ops.logical_store"));

/**
 * @brief The operator: store().
 *
 * @par Synopsis:
 *   store( srcArray, outputArray )
 *
 * @par Summary:
 *   Stores an array to the database. Each execution of store() causes a new version of the array to be created.
 *
 * @par Input:
 *   - srcArray: the source array with srcAttrs and srcDim.
 *   - outputArray: an existing array in the database, with the same schema as srcArray.
 *
 * @par Output array:
 *        <
 *   <br>   srcAttrs
 *   <br> >
 *   <br> [
 *   <br>   srcDims
 *   <br> ]
 *
 * @par Examples:
 *   n/a
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   n/a
 *
 */
class LogicalStore : public LogicalOperator
{
public:
    LogicalStore(const string& logicalName, const std::string& alias)
        : LogicalOperator(logicalName, alias)
    {
        _properties.tile = true;
        _properties.updater = true;
    }

    static PlistSpec const* makePlistSpec()
    {
        static PlistSpec argSpec {
            { "", // positionals
              RE(RE::LIST, {
                 RE(PP(PLACEHOLDER_INPUT)),
                 RE(PP(PLACEHOLDER_ARRAY_NAME).setMustExist(false))
              })
            },
            // keyworded
            { "etcomp", RE(PP(PLACEHOLDER_CONSTANT, TID_STRING)) },
            { "distribution", RE(PP(PLACEHOLDER_DISTRIBUTION)) },
            { "_fetch", RE(PP(PLACEHOLDER_CONSTANT, TID_BOOL)) },
        };
        return &argSpec;
    }

    void inferAccess(const std::shared_ptr<Query>& query) override
    {
        LogicalOperator::inferAccess(query);
        SCIDB_ASSERT(_parameters.size() > 0);
        SCIDB_ASSERT(_parameters[0]->getParamType() == PARAM_ARRAY_REF);
        const string& objName = ((std::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();

        SCIDB_ASSERT(isNameUnversioned(objName));

        ArrayDesc srcDesc;
        SCIDB_ASSERT(!srcDesc.isTransient());

        std::string arrayName;
        std::string namespaceName;
        query->getNamespaceArrayNames(objName, namespaceName, arrayName);

        // Throw an exception if the namespace does not exist.
        SystemCatalog& sysCat = *SystemCatalog::getInstance();
        NamespaceDesc nsDesc(namespaceName);
        if (!sysCat.findNamespaceByName(nsDesc)) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR,
                                   SCIDB_LE_CANNOT_RESOLVE_NAMESPACE)
                << namespaceName;
        }
        SCIDB_ASSERT(nsDesc.isIdValid());

        SystemCatalog::GetArrayDescArgs args;
        args.result = &srcDesc;
        args.arrayName = arrayName;
        args.nsName = namespaceName;
        args.throwIfNotFound = false;
        args.catalogVersion = SystemCatalog::ANY_VERSION;
        bool found = SystemCatalog::getInstance()->getArrayDesc(args);

        const LockDesc::LockMode lockMode =
            srcDesc.isTransient() ? LockDesc::XCL : LockDesc::WR;

        std::shared_ptr<LockDesc>  lock(
            make_shared<LockDesc>(
                namespaceName,
                arrayName,
                query->getQueryID(),
                Cluster::getInstance()->getLocalInstanceId(),
                LockDesc::COORD,
                lockMode));
        std::shared_ptr<LockDesc> resLock = query->requestLock(lock);
        SCIDB_ASSERT(resLock);
        SCIDB_ASSERT(resLock->getLockMode() >= LockDesc::WR);

        // And ask for needed namespace privs...
        query->getRights()->upsert(rbac::ET_NAMESPACE, namespaceName,
                                   (found ? rbac::P_NS_UPDATE : rbac::P_NS_CREATE));
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, std::shared_ptr< Query> query)
    {
        //
        // src desc: schemas[0]
        //
        SCIDB_ASSERT(schemas.size() == 1);
        ArrayDesc const& srcDesc = schemas[0];

        //
        // dst desc: _parameters[0]
        //
        SCIDB_ASSERT(_parameters.size() <= 2);
        ArrayDesc dstDesc;

        const string& objName = ((std::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();
        SCIDB_ASSERT(isNameUnversioned(objName));

        std::string arrayName;
        std::string namespaceName;
        query->getNamespaceArrayNames(objName, namespaceName, arrayName);

        ArrayID arrayId = query->getCatalogVersion(namespaceName, arrayName);

        SystemCatalog::GetArrayDescArgs args;
        args.result = &dstDesc;
        args.arrayName = arrayName;
        args.nsName = namespaceName;
        args.throwIfNotFound = false;
        args.catalogVersion = arrayId;
        bool dstFound = SystemCatalog::getInstance()->getArrayDesc(args);

        //
        // optional distribution: parameter
        //
        ArrayDistPtr argDistrib = nullptr;
        if(Parameter distParam= findKeyword("distribution")) {
            auto opParamDist = dynamic_pointer_cast<OperatorParamDistribution>(distParam);
            ASSERT_EXCEPTION(opParamDist, "cannot resolve supplied distribution parameter");
            argDistrib = opParamDist->getDistribution();
            ASSERT_EXCEPTION(argDistrib, "cannot getDistribution()");
            LOG4CXX_TRACE(logger, "LogicalStore::inferSchema(), distType: " << argDistrib->getDistType());
        }

        //
        // optional empty bitmap compression parameter
        //
        CompressorType emptyTagCompression = CompressorType::NONE;
        if (auto etcParam = findKeyword("etcomp")) {
            // If the user provided the etcomp parameter, then
            // it overrides whatever was in the input array, even if the input
            // and output arrays are the same.  This allows different versions
            // of the same array to have different empty tag compression schemes.
            auto targetCompression = paramToString(etcParam);
            emptyTagCompression = stringToCompressorType(targetCompression);
        }
        else if (auto srcEbm = srcDesc.getEmptyBitmapAttribute()) {
            // Otherwise, if there's an empty tag in the input schema, then use its
            // compression scheme.
            emptyTagCompression = srcEbm->getDefaultCompressionMethod();
        }
        // else the default, CompressorType::NONE (uncompressed), remains.

        if (dstFound) {
            // The array exists in the catalog, ensure that we have the latest
            // version of its schema as it can change from version-to-version.
            ArrayDesc latestVersionDesc =
                SystemCatalog::getInstance()->getLatestVersion(namespaceName,
                                                               arrayName,
                                                               arrayId);
            cloneAttributes(latestVersionDesc, dstDesc);

            auto dstDistType = dstDesc.getDistribution()->getDistType();
            if(argDistrib && argDistrib->getDistType() != dstDistType) {
                stringstream msg;
                msg << "store cannot change distribution of existing array"
                    << " with distribution: " << distTypeToStr(dstDistType)
                    << " to distribution: " << distTypeToStr(argDistrib->getDistType());
                LOG4CXX_ERROR(logger, "LogicalStore::inferSchema(): " << msg.str());
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_INVALID_OPERATOR_ARGUMENT)
                          << getLogicalName() << " cannot change distribution of existing target";
            }
        } else {  // !dstFound
            // Ensure attribute and dimension name uniqueness.
            UniqueNameAssigner assigner;
            for (const auto& attr : srcDesc.getAttributes()) {
                assigner.insertName(attr.getName());
            }
            for (auto const& dim : srcDesc.getDimensions()) {
                assigner.insertName(dim.getBaseName());
            }

            Attributes outAttrs;
            for (const auto& attr : srcDesc.getAttributes()) {
                outAttrs.push_back(AttributeDesc(
                    assigner.assignUniqueName(attr.getName()),
                    attr.getType(),
                    attr.getFlags(),
                    attr.getDefaultCompressionMethod(),
                    attr.getAliases(),
                    &attr.getDefaultValue(),
                    attr.getDefaultValueExpr()));
            }
            Dimensions outDims;
            outDims.reserve(srcDesc.getDimensions().size());
            for (auto const& dim : srcDesc.getDimensions()) {
                outDims.push_back(DimensionDesc(
                    assigner.assignUniqueName(dim.getBaseName()),
                    dim.getStartMin(),
                    dim.getCurrStart(),
                    dim.getCurrEnd(),
                    dim.getEndMax(),
                    dim.getRawChunkInterval(),
                    dim.getChunkOverlap()));
            }

            //XXX TODO: We take the distribution of the input (except when overridden by parameter)
            //XXX TODO: Another complication is that SGs are inserted into the physical plan only,
            //XXX TODO: During the logical phase, we dont yet know the final distribution
            //XXX TODO: coming into the store().

            DistType dstDistType = dtUninitialized ;
            // determine dstDistType, giving errors and warnings as appropriate...

            // Some 'distribution: x' args not allowed when source is dataframe (SDB-6465, SDB-6626).
            if (srcDesc.isDataframe() && argDistrib && not isDataframeCompatible(argDistrib->getDistType())) {
                stringstream ss;
                ss << "Cannot use 'distribution:" << distTypeToStr(argDistrib->getDistType())
                   << "' for storing dataframes";
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_BAD_DATAFRAME_USAGE)
                    << getLogicalName() << ss.str();
            }

            if(argDistrib) { // operator argument takes precedence over srcDesc
                dstDistType = argDistrib->getDistType();
                LOG4CXX_INFO(logger, "LogicalStore::inferSchema(), using distribution: arg " << dstDistType);
                if (!isStorable(dstDistType)) {
                    LOG4CXX_ERROR(logger, "LogicalStore::inferSchema(), distribution: arg not storable "
                                          << dstDistType);
                    LOG4CXX_ERROR(logger, "LogicalStore::inferSchema(), but source array had distribution "
                                          << srcDesc.getDistribution()->getDistType());
                    throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_INVALID_OPERATOR_ARGUMENT)
                        << getLogicalName() << " distribution: argument is not storable";
                }
            } else { // use srcDesc distribution or default
                dstDistType = srcDesc.getDistribution()->getDistType();
                LOG4CXX_INFO(logger, "LogicalStore::inferSchema(), input's distribution" << dstDistType);
                if (!isStorable(dstDistType)) {
                    LOG4CXX_WARN(logger, "LogicalStore::inferSchema(), subquery dist not storable " << dstDistType);
                    dstDistType = defaultDistType();   // force it to be storable
                    LOG4CXX_INFO(logger, "LogicalStore::inferSchema(), substituted defaultDistType()"
                                         << dstDistType);
                }
            }
            ASSERT_EXCEPTION(isStorable(dstDistType), "stored distribution must be storable");

            const size_t redundancy = Config::getInstance()->getOption<size_t>(CONFIG_REDUNDANCY);
            ArrayDistPtr arrDist = createDistribution(dstDistType, redundancy);
            //XXX TODO: but the residency is not propagated
            //XXX TODO: through the pipeline correctly, so we are forcing it.
            ArrayResPtr arrRes = query->getDefaultArrayResidencyForWrite();

            /* Notice that when storing to a non-existant array, we do not propagate the
               transience of the source array to the target ...*/
            ArrayDesc schema(
                namespaceName, arrayName,
                outAttrs, outDims,
                arrDist, arrRes,
                srcDesc.getFlags() & (~ArrayDesc::TRANSIENT));
            schema.setEmptyTagCompression(emptyTagCompression);
            LOG4CXX_TRACE(logger, "LogicalStore::inferSchema() @@@@ !FOUND, arrDist="<< arrDist->getDistType());
            return schema;
        }

        // The array is known to SciDB either because of a previous 'create array' or
        // 'store(build())' statement and its distribution was fixed at that time.
        // Check schemas to ensure that the source array can be stored in the destination.  We
        // can ignore overlaps and chunk intervals because our physical operator implements
        // requiresRedimensionOrRepartition() to get automatic repartitioning.

        ArrayDesc::checkConformity(srcDesc, dstDesc,
                                   ArrayDesc::IGNORE_PSCHEME |
                                   ArrayDesc::IGNORE_OVERLAP |
                                   ArrayDesc::IGNORE_INTERVAL |
                                   ArrayDesc::SHORT_OK_IF_EBM);
        LOG4CXX_TRACE(logger, "LogicalStore::inferSchema() 2 dstDesc.gPS()="<< dstDesc.getDistribution()->getDistType());

        Dimensions const& dstDims = dstDesc.getDimensions();
        Dimensions newDims(dstDims.size()); //XXX need this ?
        for (size_t i = 0; i < dstDims.size(); i++) {
            DimensionDesc const& dim = dstDims[i];
            newDims[i] = DimensionDesc(dim.getBaseName(),
                                       dim.getNamesAndAliases(),
                                       dim.getStartMin(), dim.getCurrStart(),
                                       dim.getCurrEnd(), dim.getEndMax(),
                                       dim.getRawChunkInterval(), dim.getChunkOverlap());
        }

        dstDesc.setEmptyTagCompression(emptyTagCompression);

        dstDesc.setDimensions(newDims);
        LOG4CXX_TRACE(logger, "LogicalStore::inferSchema() 3 dstDesc.gPS()="<< dstDesc.getDistribution()->getDistType());

        SCIDB_ASSERT(dstDesc.getId() == dstDesc.getUAId() && dstDesc.getName() == arrayName);
        SCIDB_ASSERT(dstDesc.getUAId() > 0);
        LOG4CXX_TRACE(logger, "LogicalStore::inferSchema() 4 dstDesc.gPS()="<< dstDesc.getDistribution()->getDistType());
        return dstDesc;
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalStore, "store")

}  // namespace scidb
