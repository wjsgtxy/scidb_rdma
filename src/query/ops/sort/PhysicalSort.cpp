/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2014-2019 SciDB, Inc.
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
 * PhysicalSort.cpp
 *
 *  Created on: Oct 23, 2014
 *      Author: Donghui Zhang
 */

#include "DistributedSort.h"
#include "PrettySortArray.h"

#include <array/ProjectArray.h>
#include <array/SortArray.h>
#include <array/TupleArray.h>
#include <query/PhysicalOperator.h>
#include <query/Query.h>
#include <util/Timing.h>

using namespace std;

namespace scidb {

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.query.ops.sort"));

class PhysicalSort: public PhysicalOperator
{
public:
    PhysicalSort(const string& logicalName,
                 const string& physicalName,
                 const Parameters& parameters,
                 const ArrayDesc& schema)
        : PhysicalOperator(logicalName, physicalName, parameters, schema)
    { }

    virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                   const std::vector< ArrayDesc> & inputSchemas) const
    {
        uint64_t numCells = inputBoundaries[0].getNumCells();
        if (numCells == 0)
        {
            return PhysicalBoundaries::createEmpty(1);
        }

        Coordinates start(1);
        start[0] = _schema.getDimensions()[0].getStartMin();
        Coordinates end(1);
        end[0] = _schema.getDimensions()[0].getStartMin() + numCells -1 ;
        return PhysicalBoundaries(start,end);
    }

    /**
     * From the user-provided parameters to the sort() operator, generate SortingAttributeInfos.
     *
     * @param[in]  exSchema  the "expanded" schema used for internal sorting
     * @param[out] infos     an initially empty vector to receive the SortingAttributeInfos.
     *
     * @note I had exSchema on my hidden dimension once and it took months to go away.
     */
    void generateSortingAttributeInfos(ArrayDesc const& exSchema, SortingAttributeInfos& infos)
    {
        assert(infos.empty());

        Attributes const& attrs = exSchema.getAttributes(/*exclude:*/true);
        assert(attrs.size() >= 3);  // at least one genuine and two "expanded"
        size_t const N_TRUE_ATTRS = attrs.size() - 2; // # of genuine

        for (auto& param : _parameters)
        {
            if (param->getParamType() != PARAM_ATTRIBUTE_REF)
            {
                continue;
            }
            auto sortColumn = dynamic_pointer_cast<OperatorParamAttributeReference>(param);
            SortingAttributeInfo k;
            k.columnNo = sortColumn->getObjectNo();
            k.ascent = sortColumn->getSortAscent();
            if ((size_t)k.columnNo >= N_TRUE_ATTRS) {
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_SORT_ERROR2);
            }
            infos.push_back(k);
        }

        // If the users did not provide any sorting attribute, use the first attribute.
        if (infos.empty())
        {
            //No attribute is specified... so let's sort by first attribute ascending
            SortingAttributeInfo k;
            k.columnNo =0;
            k.ascent=true;
            infos.push_back(k);
        }

        // Add the chunk & cell positions.
        SortingAttributeInfo k;
        k.columnNo = static_cast<int>(N_TRUE_ATTRS);  // 1st expanded attr is $chunk_pos.
        k.ascent = true;
        infos.push_back(k);

        k.columnNo ++; // the next one is $cell_pos
        infos.push_back(k);
    }

    DistType inferSynthesizedDistType(std::vector<DistType> const& /*inDistrib*/, size_t /*depth*/) const override
    {
        SCIDB_ASSERT(isUndefined(_schema.getDistribution()->getDistType()));
        return _schema.getDistribution()->getDistType();
    }

    /**
     * @see PhysicalOperator::getOutputDistribution
     */
    virtual RedistributeContext getOutputDistribution(std::vector<RedistributeContext> const&,
                                                      std::vector<ArrayDesc> const&) const
    {
        std::shared_ptr<Query> query(Query::getValidQueryPtr(_query));
        SCIDB_ASSERT(query);
        _schema.setResidency(query->getDefaultArrayResidency());
        SCIDB_ASSERT(isUndefined(_schema.getDistribution()->getDistType()));
        return RedistributeContext(_schema.getDistribution(),
                                   _schema.getResidency());
    }

    bool isSingleThreaded() const override
    {
        return false;
    }

    /***
     * Sort operates by using the generic array sort utility provided by SortArray
     */
    std::shared_ptr< Array> execute(vector< std::shared_ptr< Array> >& inputArrays,
                                      std::shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 1);

        bool wantCoords = SORT_DEFAULT_WANT_COORDS;
        Parameter coords = findKeyword("_coords");
        if (coords) {
            auto pExp = dynamic_pointer_cast<OperatorParamPhysicalExpression>(coords);
            wantCoords = pExp->getExpression()->evaluate().getBool();
        }

        ElapsedMilliSeconds timing;

        //
        // LocalSorting.
        //
        SortArray sorter(inputArrays[0]->getArrayDesc(), _arena);
        sorter.setPreservePositions(true)
            .setDimName(_schema.getDimensions()[0].getBaseName())
            .setChunkSize(_schema.getDimensions()[0].getChunkInterval());
        ArrayDesc const& expandedSchema = sorter.getExpandedOutputSchema();
        SortingAttributeInfos saInfos;
        generateSortingAttributeInfos(expandedSchema, saInfos);
        auto tcomp = std::make_shared<TupleComparator>(saInfos, expandedSchema);
        auto sortedLocalData = sorter.getSortedArray(inputArrays[0], query, shared_from_this(), tcomp);

        timing.logTiming(logger, "[sort] Sorting local data");

        // Unless there is a single instance, do a distributed sort.
        // Note that sortedLocalData and expandedSchema have additional fields for the chunk/cell positions.
        // Also note that sortedLocalData->getArrayDesc() differs from expandedSchema, in that:
        //   - expandedSchema._dimensions[0]._endMax = INT_MAX, but
        //   - the schema in sortedLocalData has _endMax which may be the actual number of local records minus 1.
        std::shared_ptr<MemArray> distributedSortResult = sortedLocalData;
        if (query->getInstancesCount() > 1) {
            DistributedSort ds(query, shared_from_this(), sortedLocalData,
                               expandedSchema, _arena, saInfos, timing);
            distributedSortResult = ds.sort();
        }

        if (wantCoords) {
            // Transform trailing ($chunk_pos, $cell_pos) suffix into
            // preceding (inDim0, ... inDimN) input coordinates.
            return std::make_shared<PrettySortArray>(_schema,
                                                distributedSortResult,
                                                inputArrays[0]->getArrayDesc().getDimensions(),
                                                query);
        } else {
            // Project off the chunk_pos and cell_pos attributes.
            const bool excludeEmptyBitmap = true;
            AttributeID nAttrs = safe_static_cast<AttributeID>(_schema.getAttributes(excludeEmptyBitmap).size());
            vector<AttributeID> projection(nAttrs+1);
            for (AttributeID i=0; i<nAttrs; ++i) {
                projection[i] = i;
            }
            projection[nAttrs] = nAttrs+2; // this is the empty bitmap attribute.
            return std::make_shared<ProjectArray>(_schema, distributedSortResult, projection);
        }
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalSort, "sort", "physicalSort")

}  // namespace scidb
