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
 * @file IndexLookupSettings.h
 * The settings structure for the index_lookup operator.
 * @see InstanceStatsSettings.h
 * @author apoliakov@paradigm4.com
 */

#ifndef INDEX_LOOKUP_SETTINGS_H
#define INDEX_LOOKUP_SETTINGS_H

#include <query/OperatorParam.h>

namespace scidb
{

/**
 * @brief Settings for the IndexLookup operator.
 */
class IndexLookupSettings
{
public:
    static const char* const KW_MEM_LIMIT;
    static const char* const KW_IDX_SORTED;

    IndexLookupSettings(ArrayDesc const& inputSchema,
                        ArrayDesc const& indexSchema,
                        Parameters const& operatorParameters,
                        KeywordParameters const& kwParameters,
                        bool logical,
                        std::shared_ptr<Query>& query);

    /**
     * @return the memory limit (converted to bytes)
     */
    size_t getMemoryLimit() const
    {
        return _memoryLimit;
    }

    /**
     * @return the name of the output attribute
     */
    std::string const& getOutputAttributeName() const
    {
        return _outputAttributeName;
    }

    /**
     * @return the id of the input attribute
     */
    AttributeID getInputAttributeId() const
    {
        return _inputAttributeId;
    }

    /**
     * @return true if the user claims the index array is already dense and sorted,
     * false otherwise (default).
     */
    bool isIndexPreSorted() const
    {
        return _indexSorted;
    }

private:
    ArrayDesc const& _inputSchema;
    ArrayDesc const& _indexSchema;
    AttributeID const _inputAttributeId;
    std::string const _inputAttributeName;
    std::string _outputAttributeName;
    bool _outputAttributeNameSet;
    size_t _memoryLimit;
    bool _memoryLimitSet;
    bool _indexSorted;
    bool _indexSortedSet;

    void setOutputAttributeName(std::shared_ptr<OperatorParam>const& param);
    void checkInputSchemas();
};

}

#endif // ! INDEX_LOOKUP_SETTINGS_H
