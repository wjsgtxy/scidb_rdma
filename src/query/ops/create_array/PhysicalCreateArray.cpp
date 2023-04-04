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

#include <array/ArrayName.h>
#include <array/MemArray.h>
#include <array/TransientCache.h>
#include <query/Expression.h>
#include <query/PhysicalOperator.h>
#include <query/Query.h>
#include <rbac/Session.h>
#include <system/SystemCatalog.h>

#include <log4cxx/logger.h>

using namespace std;

/****************************************************************************/
namespace scidb {
/****************************************************************************/
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.ops.physcial_create_array"));

struct PhysicalCreateArray : PhysicalOperator
{
    PhysicalCreateArray(const string& logicalName,
                        const string& physicalName,
                        const Parameters& parameters,
                        const ArrayDesc& schema)
     : PhysicalOperator(logicalName,physicalName,parameters,schema)
    {}

    virtual std::shared_ptr<Array>
    execute(vector<std::shared_ptr<Array> >& in,std::shared_ptr<Query> query)
    {
        bool const isTempArray =
            param<OperatorParamPhysicalExpression>(2)->getExpression()->evaluate().getBool();

        if (query->isCoordinator())
        {
            string const& objName = param<OperatorParamArrayReference>(0)->getObjectName();

            std::string arrayName;
            std::string namespaceName;
            query->getNamespaceArrayNames(objName, namespaceName, arrayName);
            SCIDB_ASSERT(isNameUnversioned(arrayName));

            ArrayDesc arrSchema(param<OperatorParamSchema>(1)->getSchema());

            arrSchema.setName(arrayName);
            arrSchema.setNamespaceName(namespaceName);
            arrSchema.setTransient(isTempArray); // Transient 短暂的

            // TODO JHM: provide a config variable to set what the default
            // distribution should be at any given installation
            DistType dist = arrSchema.isDataframe() ? dtDataframe : defaultDistType();

            if (_parameters.size() > 3) {
                // If it exists, parse the optional distribution param.
                auto paramDist = param<OperatorParamDistribution>(3);
                if (paramDist) {
                    dist = paramDist->getDistribution()->getDistType();
                }
            }

            if (_parameters.size() > 4) {
                // If it exists, parse the optional 'emptytag compression' param.
                auto targetCompression = paramToString(_parameters[4]);
                auto emptyTagCompression = stringToCompressorType(targetCompression);
                arrSchema.setEmptyTagCompression(emptyTagCompression);
            }

         /* Give our subclass a chance to compute missing dimension details
            such as a wild-carded chunk interval, for example...*/

            const size_t redundancy = Config::getInstance()->getOption<size_t> (CONFIG_REDUNDANCY); // 这个备份策略是针对所有的Array！
            arrSchema.setDistribution(createDistribution(dist, redundancy));
            arrSchema.setResidency(query->getDefaultArrayResidencyForWrite());
            ArrayID uAId = SystemCatalog::getInstance()->getNextArrayId();
            arrSchema.setIds(uAId, uAId, VersionID(0));
            if (!isTempArray) {
                query->setAutoCommit(); // db array默认自动提交
            }

            SystemCatalog::getInstance()->addArray(arrSchema);
        }

        if (isTempArray)
        {
            syncBarrier(0,query);                        // Workers wait here

            string const& arrayName = param<OperatorParamArrayReference>(0)->getObjectName();

            // XXX TODO: this needs to change to eliminate worker catalog access
            ArrayDesc arrSchema;
            SystemCatalog::GetArrayDescArgs args;
            args.result = &arrSchema;
            splitQualifiedArrayName(arrayName, args.nsName, args.arrayName);
            if (args.nsName.empty()) {
                args.nsName = query->getNamespaceName();
            }
            args.throwIfNotFound = true;
            SystemCatalog::getInstance()->getArrayDesc(args);

            transient::record(std::make_shared<MemArray>(arrSchema,query));
        }

        return std::shared_ptr<Array>();
    }

    template<class t>
    std::shared_ptr<t> param(size_t i) const
    {
        assert(i < _parameters.size());

        return std::dynamic_pointer_cast<t>(_parameters[i]);
    }
};

/****************************************************************************/

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalCreateArray,     "create_array",      "impl_create_array")

/****************************************************************************/
}
/****************************************************************************/
