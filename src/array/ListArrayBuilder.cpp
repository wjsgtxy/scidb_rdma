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

/****************************************************************************/

#include "array/ListArrayBuilder.h"

using namespace std;

/****************************************************************************/
namespace scidb {
/****************************************************************************/

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("listArrayBuilder"));

/****************************************************************************/

ListArrayBuilder::ListArrayBuilder(const std::string &listName)
                : _nAttrs(0),
                  _dimIdOff(0),
                  _listName(listName)
{}

ArrayDesc ListArrayBuilder::getSchema(std::shared_ptr<Query> const& query) const
{
    ArrayDesc desc(_listName,
                   getAttributes(),
                   getDimensions(query),
                   createDistribution(dtUndefined),     // SDB-5616, list('instances') for example
                                                        // produces a non-standard distribution
                                                        // so this is the only legitimate answer covering
                                                        // all current clients.
                                                        // Some other use cases might be able to use a standard
                                                        // distribution and forgo the sg.
                                                        // We can do those optimizations when the ROI exists.
                   query->getDefaultArrayResidency());
    return desc;
}

Dimensions ListArrayBuilder::getDimensions(std::shared_ptr<Query> const& query) const
{
    size_t n = query->getCoordinatorLiveness()->getNumInstances();

    return {DimensionDesc("inst", 0, 0, n-1, n-1, 1, 0),
            DimensionDesc("n", 0, 0, CoordinateBounds::getMax(), CoordinateBounds::getMax(), LIST_CHUNK_SIZE, 0)};
}

void ListArrayBuilder::initialize(const ArrayDesc& schema, std::shared_ptr<Query> const& query)
{
    _dimIdOff               = 0;
    const auto& attrs       = schema.getAttributes(true);
    _nAttrs                 = attrs.size();
    _array                  = std::make_shared<MemArray>(schema, query);

    _currPos.resize(schema.getDimensions().size(),0);

    if (_currPos.size() > 1)
    {
        // adding the instance coordinate
        _currPos[0] = query->getInstanceID();
        _dimIdOff = 1;
    }

    _outAIters.resize(_nAttrs);

    for (const auto& attr : attrs)
    {
        _outAIters[attr.getId()] = _array->getIterator(attr);
    }

    _outCIters.resize(_nAttrs);

    for (const auto& attr : attrs)
    {
        Chunk& ch = _outAIters[attr.getId()]->newChunk(_currPos);
        auto isFirst = attr.getId() == attrs.firstDataAttribute().getId();
        _outCIters[attr.getId()] = ch.getIterator(query, isFirst ? ChunkIterator::SEQUENTIAL_WRITE :
                                                  ChunkIterator::SEQUENTIAL_WRITE | ChunkIterator::NO_EMPTY_CHECK);
    }

    _nextChunkPos = _currPos;
    _nextChunkPos[_dimIdOff] += LIST_CHUNK_SIZE;
    _query = query;
}

std::shared_ptr<MemArray> ListArrayBuilder::getArray()
{
    assert(_query);

    for (const auto &i : _outCIters)
    {
        if (i) {
            i->flush();
        }
    }

    LOG4CXX_DEBUG(logger, "[ListArrayBuilder::getArray()] output PS: "
                          << _array->getArrayDesc().getDistribution()->getDistType());
    return _array;
}

void ListArrayBuilder::beginElement()
{
    assert(_query);
    assert(_array);

    const auto& attrs = _array->getArrayDesc().getAttributes(true);
    if (_currPos[_dimIdOff] == _nextChunkPos[_dimIdOff])
    {
        for (const auto& attr : attrs)
        {
            _outCIters[attr.getId()]->flush();
            Chunk& ch = _outAIters[attr.getId()]->newChunk(_currPos);
            _outCIters[attr.getId()] =
                ch.getIterator(_query, attr.getId() == attrs.firstDataAttribute().getId() ? ChunkIterator::SEQUENTIAL_WRITE :
                                                        ChunkIterator::SEQUENTIAL_WRITE | ChunkIterator::NO_EMPTY_CHECK);
        }

        _nextChunkPos[_dimIdOff] += LIST_CHUNK_SIZE;
    }

    for (const auto& attr : attrs)
    {
        _outCIters[attr.getId()]->setPosition(_currPos);
    }
}

void ListArrayBuilder::endElement()
{
    assert(_query);

    ++_currPos[_dimIdOff];
}

/****************************************************************************/
}
/****************************************************************************/
