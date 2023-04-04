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
 * PhysicalRename.cpp
 *
 *  Created on: Apr 16, 2010
 *      Author: Knizhnik
 */

#include <query/PhysicalOperator.h>
#include <query/Query.h>
#include <system/SystemCatalog.h>

#include <log4cxx/logger.h>

using namespace std;

namespace scidb
{
    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.physical_rename"));

class PhysicalRename: public PhysicalOperator
{
public:
    PhysicalRename(
        const string& logicalName, const string& physicalName,
        const Parameters& parameters, const ArrayDesc& schema)
        : PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }
    std::shared_ptr<Array> execute(vector< std::shared_ptr<Array> >& inputArrays, std::shared_ptr<Query> query)
    {
        getInjectedErrorListener().throwif(__LINE__, __FILE__);
        return std::shared_ptr<Array>();
    }

    void preSingleExecute(std::shared_ptr<Query> query)
    {
        const string& oldArrayNameOrg =
            ((std::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();
        ASSERT_EXCEPTION(!oldArrayNameOrg.empty(), "Array name cannot be empty");

        std::string oldArrayName;
        std::string oldNamespaceName;
        query->getNamespaceArrayNames(oldArrayNameOrg, oldNamespaceName, oldArrayName);
        _oldArrayName = oldArrayName;
        ASSERT_EXCEPTION(!_oldArrayName.empty(), "Array name cannot be empty");

        // From this point on _schema is used to describe the array to be removed rather than the
        // output array somewhat hacky ... but getOutputDistribution() and other optimizer
        // manipulations should be done by now
        ArrayID arrayId = query->getCatalogVersion(oldNamespaceName, oldArrayName);
        SystemCatalog::GetArrayDescArgs args;
        args.result = &_schema;
        args.nsName = oldNamespaceName;
        args.arrayName = oldArrayName;
        args.catalogVersion = arrayId;
        args.throwIfNotFound = true;
        SystemCatalog::getInstance()->getArrayDesc(args);
        assert(_schema.getName() == _oldArrayName);

        //XXX TODO: for now just check that all the instances in the residency are alive
        //XXX TODO: once we allow writes in a degraded mode, this call might have more semantics
        query->isDistributionDegradedForWrite(_schema);
    }

    void postSingleExecute(std::shared_ptr<Query> query)
    {
        assert(!_oldArrayName.empty());
        const string& newArrayNameOrg =
            ((std::shared_ptr<OperatorParamReference>&)_parameters[1])->getObjectName();
        ASSERT_EXCEPTION(!newArrayNameOrg.empty(), "Array name cannot be empty");

        string nsName;
        string newArrayName;
        query->getNamespaceArrayNames(newArrayNameOrg, nsName, newArrayName);

        query->setAutoCommit();
        SystemCatalog::getInstance()->renameArray(nsName, _oldArrayName, newArrayName);
    }

    private:
    std::string _oldArrayName;
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalRename, "rename", "physicalRename")

}  // namespace scidb
