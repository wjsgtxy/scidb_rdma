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
 * @file IndexLookupSettings.cpp
 * The settings structure for the index_lookup operator.
 * @author apoliakov@paradigm4.com
 */

#include "IndexLookupSettings.h"

#include <query/Expression.h>
#include <system/Config.h>

namespace scidb
{

const char* const IndexLookupSettings::KW_MEM_LIMIT = "memory_limit";
const char* const IndexLookupSettings::KW_IDX_SORTED = "index_sorted";

void IndexLookupSettings::setOutputAttributeName(std::shared_ptr<OperatorParam>const& param)
{
    if(_outputAttributeNameSet)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_CANNOT_BE_SET_MORE_THAN_ONCE) << "output attribute name";
    }
    //extracting the identifier that's been parsed and prepared by scidb
    _outputAttributeName = ((std::shared_ptr<OperatorParamReference> const&)param)->getObjectName();
    _outputAttributeNameSet = true;
}

void IndexLookupSettings::checkInputSchemas()
{
    //throw an error if the schemas don't satisfy us
    std::ostringstream err;
    if (_indexSchema.getDimensions().size() > 1 ||
        _indexSchema.getAttributes(true).size() > 1)
    {
        //note: index does NOT have to start at 0.
        throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_IMPROPER_INDEX_SHAPE);
    }
    AttributeDesc const& inputAttribute = _inputSchema.getAttributes().findattr(_inputAttributeId);
    AttributeDesc const& indexAttribute = _indexSchema.getAttributes().firstDataAttribute();
    if (inputAttribute.getType() != indexAttribute.getType())
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_ATTRIBUTES_DO_NOT_MATCH_TYPES)
                << _inputAttributeName
                << inputAttribute.getType()
                << indexAttribute.getName()
                << indexAttribute.getType();
    }
}

IndexLookupSettings::IndexLookupSettings(ArrayDesc const& inputSchema,
                                         ArrayDesc const& indexSchema,
                                         Parameters const& operatorParameters,
                                         KeywordParameters const& kwParameters,
                                         bool logical,
                                         std::shared_ptr<Query>& query):
    _inputSchema            (inputSchema),
    _indexSchema            (indexSchema),
    _inputAttributeId       (std::dynamic_pointer_cast<OperatorParamReference> (operatorParameters[0])->getObjectNo()),
    _inputAttributeName     (std::dynamic_pointer_cast<OperatorParamReference> (operatorParameters[0])->getObjectName()),
    _outputAttributeName    (_inputSchema.getAttributes().findattr(_inputAttributeId).getName() + "_index"),
    _outputAttributeNameSet (false),
    _memoryLimit            (Config::getInstance()->getOption<size_t>(CONFIG_MEM_ARRAY_THRESHOLD) * MiB),
    _memoryLimitSet         (false),
    _indexSorted            (false),
    _indexSortedSet         (false)

{
    SCIDB_ASSERT(operatorParameters.size() == 1 ||
                 operatorParameters.size() == 2);

    checkInputSchemas();

    if (std::dynamic_pointer_cast<OperatorParamReference> (operatorParameters[0])->getInputNo() != 0)
    {   //can happen if user specifies the index attribute!
        throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_NOT_AN_ATTRIBUTE_IN_INPUT) << _inputAttributeName;
    }

    if (operatorParameters.size() == 2) {
        std::shared_ptr<OperatorParam>const& param = operatorParameters[1];
        SCIDB_ASSERT(param->getParamType() == PARAM_ATTRIBUTE_REF);
        setOutputAttributeName(param);
    }

    auto memIter = kwParameters.find("memory_limit");
    if (memIter != kwParameters.end()) {
        int64_t limit;
        if (logical) {
            limit =
                evaluate(((std::shared_ptr<OperatorParamLogicalExpression>&)
                          memIter->second)->getExpression(), TID_INT64).getInt64();
        } else {
            limit = ((std::shared_ptr<OperatorParamPhysicalExpression>&)
                     memIter->second)->getExpression()->evaluate().getInt64();
        }
        if (limit <= 0) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_PARAMETER_NOT_POSITIVE_INTEGER)
                << KW_MEM_LIMIT;
        }
        _memoryLimit = limit * MiB;
        _memoryLimitSet = true;
    }

    auto idxIter = kwParameters.find("index_sorted");
    if (idxIter != kwParameters.end()) {
        if (logical) {
            _indexSorted =
                evaluate(((std::shared_ptr<OperatorParamLogicalExpression>&)
                          idxIter->second)->getExpression(), TID_BOOL).getBool();
        } else {
            _indexSorted = ((std::shared_ptr<OperatorParamPhysicalExpression>&)
                            idxIter->second)->getExpression()->evaluate().getBool();
        }
        _indexSortedSet = true;
    }
}

}
