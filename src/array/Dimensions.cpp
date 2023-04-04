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

#include <array/Dimensions.h>
#include <array/DimensionDesc.h>
#include <query/Query.h>
#include <system/Config.h>

namespace scidb {

std::ostream& operator<<(std::ostream& stream,const Dimensions& dims)
{
    return insertRange(stream,dims, "; ");
}

void printDimNames(std::ostream& os, Dimensions const& dims)
{
    const size_t N = dims.size();
    for (size_t i = 0; i < N; ++i) {
        if (i) {
            // Alias separator used by printNames is a comma, so use semi-colon.
            os << ';';
        }
        printNames(os, dims[i].getNamesAndAliases());
    }
}

bool hasOverlap(Dimensions const& dims)
{
    for (auto const& d : dims) {
        if (d.getChunkOverlap() != 0) {
            return true;
        }
    }
    return false;
}

bool isAutochunked(Dimensions const& dims)
{
    for (DimensionDesc const& d : dims) {
        if (d.getRawChunkInterval() == DimensionDesc::AUTOCHUNKED) {
            return true;
        }
    }
    return false;
}

bool isDataframe(Dimensions const& dims)
{
    return dims.size() == DF_NUM_DIMS
        && dims[DF_INST_DIM].getBaseName() == DFD_INST
        && dims[DF_INST_DIM].getStartMin() == 0
        && dims[DF_INST_DIM].getChunkOverlap() == 0
        && dims[DF_INST_DIM].getRawChunkInterval() == 1
        && dims[DF_SEQ_DIM].getBaseName() == DFD_SEQ
        && dims[DF_SEQ_DIM].getStartMin() == 0
        && dims[DF_SEQ_DIM].getChunkOverlap() == 0;
}

bool dataframePhysicalIdMode()
{
    Config& cfg = *Config::getInstance();
    return cfg.getOption<int>(CONFIG_X_DATAFRAME_PHYS_IID);
}

Dimensions makeDataframeDimensions(int64_t seqLen)
{
    static_assert(DF_INST_DIM == 0, "Bad dataframe dimension initialization");
    static_assert(DF_SEQ_DIM == 1, "Dataframe dimension initialization is bad");
    return Dimensions {
        DimensionDesc(DFD_INST, 0, CoordinateBounds::getMax(), 1, 0),
        DimensionDesc(DFD_SEQ, 0, CoordinateBounds::getMax(),
                      (seqLen > 0
                       ? seqLen
                       : int64_t(DimensionDesc::AUTOCHUNKED)),
                      0)
    };
}

}  // namespace scidb
