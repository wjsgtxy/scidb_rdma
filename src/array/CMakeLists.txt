########################################
# BEGIN_COPYRIGHT
#
# Copyright (C) 2008-2019 SciDB, Inc.
# All Rights Reserved.
#
# SciDB is free software: you can redistribute it and/or modify
# it under the terms of the AFFERO GNU General Public License as published by
# the Free Software Foundation.
#
# SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
# INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
# NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
# the AFFERO GNU General Public License for the complete license terms.
#
# You should have received a copy of the AFFERO GNU General Public License
# along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
#
# END_COPYRIGHT
########################################

set (array_src
     AccumulatorArray.cpp
     AllocationBuffer.cpp
     Array.cpp
     ArrayDescSerialize.cpp
     ArrayDistribution.cpp
     ArrayDesc.cpp
     ArrayIterator.cpp
     ArrayName.cpp
     ArrayWriter.cpp
     AttributeDesc.cpp
     Attributes.cpp
     CachedDBChunk.cpp
     CachedDBChunk_debug.cpp
     CachedTmpChunk.cpp
     Chunk.cpp
     CompressedBuffer.cpp
     ConstChunkIterator.cpp
     ChunkBuffer.cpp
     ChunkMaterializer.cpp
     ConstArrayIterator.cpp
     ConstItemIterator.cpp
     ConstIterator.cpp
     ConstChunk.cpp
     Coordinate.cpp
     DBArray.cpp
     DeepChunkMerger.cpp
     DelegateArray.cpp
     Dense1MChunkEstimator.cpp
     DimensionDesc.cpp
     Dimensions.cpp
     ListArrayBuilder.cpp
     MaterializedChunkCache.cpp
     MemArray.cpp
     MemChunk.cpp
     MergeSortArray.cpp
     Metadata.cpp
     MultiStreamArray.cpp
     ObjectNames.cpp
     ParallelAccumulatorArray.cpp
     ReplicationMgr.cpp
     RLE.cpp
     SharedBuffer.cpp
     SinglePassArray.cpp
     SinglePassAdaptor.cpp
     SortArray.cpp
     SpatialRangesChunkPosIterator.cpp
     StreamArray.cpp
     SyntheticArray.cpp
     TemplateParser.cpp
     Tile.cpp
     TransientCache.cpp
     TupleArray.cpp
)

add_library(array_lib OBJECT ${array_src})
target_link_libraries(array_lib
                      PUBLIC Boost::boost
                             scidb_msg_lib
  )
