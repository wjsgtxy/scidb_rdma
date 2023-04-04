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
 * @file Aggregate.cpp
 *
 * @author roman.simakov@gmail.com
 */

#include <query/Aggregate.h>
#include <query/FunctionLibrary.h>
#include <log4cxx/logger.h>

using std::shared_ptr;
using namespace std;

namespace scidb
{

void AggregateLibrary::addAggregate(
        AggregatePtr const& aggregate,
        string const & libraryName /* = "scidb" */ )
{
    if (!aggregate) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_CANT_ADD_NULL_FACTORY);
    }
    //Try to find functions with 1 argument which can match new aggregate
    std::vector<TypeId> inputTypes(1, aggregate->getInputType().typeId());
    FunctionDescription functDescription;
    std::vector<FunctionPointer> converters;
    bool foundScalar = false;

    foundScalar |= FunctionLibrary::getInstance()->findFunction(
            aggregate->getName(),
            inputTypes,
            functDescription,
            converters,
            true);

    foundScalar |= FunctionLibrary::getInstance()->findFunction(
            aggregate->getName(),
            inputTypes,
            functDescription,
            converters,
            false);

    if (foundScalar)
    {
        throw USER_EXCEPTION(SCIDB_SE_UDO, SCIDB_LE_CANNOT_ADD_AGGREGATE) << aggregate->getName();
    }

    const FactoriesMap::const_iterator i = _registeredFactories.find(aggregate->getName());
    if (i != _registeredFactories.end()) {
        auto i2 = i->second.find(aggregate->getInputType().typeId());
        if (i2 != i->second.end())
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_DUPLICATE_AGGREGATE_FACTORY);
    }

    AggregateElement element(aggregate, libraryName);
    _registeredFactories[aggregate->getName()][aggregate->getInputType().typeId()] = element;
}

size_t AggregateLibrary::getNumAggregates() const
{
    size_t size = 0;
    for (   FactoriesMap::const_iterator it = _registeredFactories.begin();
            it != _registeredFactories.end();
            ++it)
    {
        const AggregateTypeIdToElementMap &aggregateTypeIdToElementMap = (*it).second;
        size += aggregateTypeIdToElementMap.size();
    }

    return size;
}

void AggregateLibrary::visitPlugins(const Visitor& visit) const
{
    ScopedMutexLock cs (_mutex, PTW_SML_AGGREGATE_LIBRARY);

    for (FactoriesMap::value_type const& i : _registeredFactories)
    {
        // Visit each element in the TypeIdToElementMap
        for (auto const& j : i.second)
        {
            visit(i.first,j.first,j.second.getLibraryName());
        }
    }
}

AggregatePtr AggregateLibrary::createAggregate(std::string const& aggregateName, Type const& aggregateType) const
{
    const FactoriesMap::const_iterator i = _registeredFactories.find(aggregateName);
    if (i == _registeredFactories.end())
        throw USER_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_AGGREGATE_NOT_FOUND) << aggregateName;

    FactoriesMap::value_type::second_type::const_iterator i2 = i->second.find(aggregateType.typeId());
    if (i2 == i->second.end())
    {
        if (aggregateType.typeId() != TID_VOID) {
            i2 = i->second.find(TID_VOID);
        } else {
            throw USER_EXCEPTION(SCIDB_SE_TYPE, SCIDB_LE_AGGREGATE_DOESNT_SUPPORT_ASTERISK) << aggregateName;
        }
        if (i2 == i->second.end()) {
            throw USER_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_AGGREGATE_DOESNT_SUPPORT_TYPE) << aggregateName << aggregateType.typeId();
        }
    }

    const AggregateElement &aggregateElement = (*i2).second;
    if (aggregateType.typeId() == TID_VOID && !aggregateElement.getPtr()->supportAsterisk()) {
        throw USER_EXCEPTION(SCIDB_SE_TYPE, SCIDB_LE_AGGREGATE_DOESNT_SUPPORT_ASTERISK) << aggregateName;
    }

    return aggregateElement.getPtr()->clone(aggregateType);
}

} // namespace scidb
