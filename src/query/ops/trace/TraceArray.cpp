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

#include "TraceArray.h"

namespace scidb {

std::ostream& operator<<(std::ostream& os, const Coordinates& coords)
{
    os << "[";
    auto coordIter = coords.cbegin();
    while (coordIter != coords.cend() - 1) {
        os << *coordIter << ",";
        ++coordIter;
    }
    if (coordIter != coords.cend()) {
        os << *coordIter;
    }
    os << "]";
    return os;
}

Trace::Trace(const std::string& outputFilePath,
             InstanceID instanceID)
        : _outputFileStream()
        , _methodTraceMutex()
{
    SCIDB_ASSERT(!outputFilePath.empty());
    auto disambiguatedFilePath =
        outputFilePath +
        "-" + std::to_string(instanceID);
    _outputFileStream.open(disambiguatedFilePath);
}

Trace::~Trace()
{
    _outputFileStream.close();
}

void Trace::end(const void* pthis,
                int line,
                const char* classname,
                const char* methodName,
                const std::string& operatorName)
{
    std::lock_guard<std::mutex> lock(_methodTraceMutex);
    preamble(pthis, line, classname, methodName)
        << " END TRACE above " << operatorName
        << std::endl;
}

void Trace::start(const void* pthis,
                  int line,
                  const char* classname,
                  const char* methodName,
                  const std::string& operatorName)
{
    std::lock_guard<std::mutex> lock(_methodTraceMutex);
    _outputFileStream << "ThreadID\tObjectAddress\tLocation\tMethod" << std::endl;
    preamble(pthis, line, classname, methodName)
        << " BEGIN TRACE above " << operatorName
        << std::endl;
}

void Trace::waypoint(const void* pthis,
                     int line,
                     const char* classname,
                     const char* methodName,
                     const Exception& e)
{
    std::lock_guard<std::mutex> lock(_methodTraceMutex);
    preamble(pthis, line, classname, methodName)
        << " EXCEPTION: " << e.what()
        << std::endl;
}

void Trace::waypoint(const void* pthis,
                     int line,
                     const char* classname,
                     const char* methodName)
{
    std::lock_guard<std::mutex> lock(_methodTraceMutex);
    preamble(pthis, line, classname, methodName)
        << std::endl;
}

std::ostream& Trace::preamble(const void* pthis,
                              int line,
                              const char* classname,
                              const char* methodName)
{
    _outputFileStream
        << "0x" << std::hex << std::this_thread::get_id() << '\t'
        << pthis << std::dec << '\t'
        << "TraceArray.cpp" << ":" << line << '\t'
        << classname << "::" << methodName;
    return _outputFileStream;
}

TraceClient::TraceClient(std::shared_ptr<Trace> trace)
        : _trace(trace)
{
}

TraceClient::~TraceClient()
{
}

void TraceClient::end(int line,
                      const char* methodName,
                      const std::string& operatorName)
{
    _trace->end(static_cast<const void*>(this),
                line,
                getClassName(),
                methodName,
                operatorName);
}

void TraceClient::start(int line,
                        const char* methodName,
                        const std::string& operatorName)
{
    _trace->start(static_cast<const void*>(this),
                  line,
                  getClassName(),
                  methodName,
                  operatorName);
}

void TraceClient::waypoint(int line,
                           const char* methodName,
                           const Exception& e) const
{
    _trace->waypoint(static_cast<const void*>(this),
                           line,
                           getClassName(),
                           methodName,
                           e);
}

void TraceClient::waypoint(int line,
                           const char* methodName) const
{
    _trace->waypoint(static_cast<const void*>(this),
                     line,
                     getClassName(),
                     methodName);
}

TraceArrayConstChunkIterator::TraceArrayConstChunkIterator(
    std::shared_ptr<ConstChunkIterator> inputChunkIter,
    const ConstTraceArrayChunk& traceArrayChunk,
    std::shared_ptr<Trace> trace)
        : TraceClient(trace)
        , _inputChunkIter(inputChunkIter)
        , _traceArrayChunk(traceArrayChunk)
{
    SCIDB_ASSERT(_inputChunkIter);

    // Show the name from the abstraction rather than the trace iterator method name
    waypoint(__LINE__, "ConstChunkIterator");
}

TraceArrayConstChunkIterator::~TraceArrayConstChunkIterator()
{
    // Show the name from the abstraction rather than the trace iterator method name
    waypoint(__LINE__, "~ConstChunkIterator");
}

#define CATCH_EXCEPTION(line, function)         \
    catch(const Exception& e) {                 \
        waypoint(line, function, e);            \
        e.raise();                              \
        SCIDB_UNREACHABLE();                    \
    }

bool TraceArrayConstChunkIterator::end()
{
    waypoint(__LINE__, __FUNCTION__);
    try {
        bool atEnd = _inputChunkIter->end();
        waypoint(__LINE__, __FUNCTION__, atEnd);
        return atEnd;
    }
    CATCH_EXCEPTION(__LINE__, __FUNCTION__)
}

void TraceArrayConstChunkIterator::operator++()
{
    waypoint(__LINE__, __FUNCTION__);
    try {
        ++(*_inputChunkIter);
    }
    CATCH_EXCEPTION(__LINE__, __FUNCTION__)
}

Coordinates const& TraceArrayConstChunkIterator::getPosition()
{
    waypoint(__LINE__, __FUNCTION__);
    try {
        const auto& pos = _inputChunkIter->getPosition();
        waypoint(__LINE__, __FUNCTION__, pos);
        return pos;
    }
    CATCH_EXCEPTION(__LINE__, __FUNCTION__)
}

void TraceArrayConstChunkIterator::restart()
{
    waypoint(__LINE__, __FUNCTION__);
    try {
        _inputChunkIter->restart();
    }
    CATCH_EXCEPTION(__LINE__, __FUNCTION__)
}

int TraceArrayConstChunkIterator::getMode() const
{
    waypoint(__LINE__, __FUNCTION__);
    try {
        auto mode = _inputChunkIter->getMode();
        waypoint(__LINE__, __FUNCTION__, mode);
        return mode;
    }
    CATCH_EXCEPTION(__LINE__, __FUNCTION__)
}

Value const& TraceArrayConstChunkIterator::getItem()
{
    waypoint(__LINE__, __FUNCTION__);
    try {
        return _inputChunkIter->getItem();
    }
    CATCH_EXCEPTION(__LINE__, __FUNCTION__)
}

bool TraceArrayConstChunkIterator::isEmpty() const
{
    waypoint(__LINE__, __FUNCTION__);
    try {
        auto ie = _inputChunkIter->isEmpty();
        waypoint(__LINE__, __FUNCTION__, ie);
        return ie;
    }
    CATCH_EXCEPTION(__LINE__, __FUNCTION__)
}

ConstChunk const& TraceArrayConstChunkIterator::getChunk()
{
    waypoint(__LINE__, __FUNCTION__);
    try {
        return _traceArrayChunk;
    }
    CATCH_EXCEPTION(__LINE__, __FUNCTION__)
}

const Coordinates&
TraceArrayConstChunkIterator::getData(scidb::Coordinates& offset,
                                      size_t maxValues,
                                      std::shared_ptr<BaseTile>& tileData,
                                      std::shared_ptr<BaseTile>& tileCoords)
{
    waypoint(__LINE__, __FUNCTION__);
    try {
        const auto& pos = _inputChunkIter->getData(offset, maxValues, tileData, tileCoords);
        waypoint(__LINE__, __FUNCTION__, pos);
        return pos;
    }
    CATCH_EXCEPTION(__LINE__, __FUNCTION__)
}

position_t
TraceArrayConstChunkIterator::getData(position_t logicalOffset,
                                      size_t maxValues,
                                      std::shared_ptr<BaseTile>& tileData,
                                      std::shared_ptr<BaseTile>& tileCoords)
{
    waypoint(__LINE__, __FUNCTION__);
    try {
        auto pos = _inputChunkIter->getData(logicalOffset, maxValues, tileData, tileCoords);
        waypoint(__LINE__, __FUNCTION__, pos);
        return pos;
    }
    CATCH_EXCEPTION(__LINE__, __FUNCTION__)
}

const Coordinates&
TraceArrayConstChunkIterator::getData(scidb::Coordinates& offset,
                                      size_t maxValues,
                                      std::shared_ptr<BaseTile>& tileData)
{
    waypoint(__LINE__, __FUNCTION__);
    try {
        const auto& pos = _inputChunkIter->getData(offset, maxValues, tileData);
        waypoint(__LINE__, __FUNCTION__, pos);
        return pos;
    }
    CATCH_EXCEPTION(__LINE__, __FUNCTION__)
}

position_t
TraceArrayConstChunkIterator::getData(position_t logicalOffset,
                                      size_t maxValues,
                                      std::shared_ptr<BaseTile>& tileData)
{
    waypoint(__LINE__, __FUNCTION__);
    try {
        auto pos = _inputChunkIter->getData(logicalOffset, maxValues, tileData);
        waypoint(__LINE__, __FUNCTION__, pos);
        return pos;
    }
    CATCH_EXCEPTION(__LINE__, __FUNCTION__)
}

position_t TraceArrayConstChunkIterator::getLogicalPosition()
{
    waypoint(__LINE__, __FUNCTION__);
    try {
        auto pos = _inputChunkIter->getLogicalPosition();
        waypoint(__LINE__, __FUNCTION__, pos);
        return pos;
    }
    CATCH_EXCEPTION(__LINE__, __FUNCTION__)
}

bool TraceArrayConstChunkIterator::setPosition(const Coordinates& pos)
{
    waypoint(__LINE__, __FUNCTION__);
    try {
        auto success = _inputChunkIter->setPosition(pos);
        waypoint(__LINE__, __FUNCTION__, pos);
        return success;
    }
    CATCH_EXCEPTION(__LINE__, __FUNCTION__)
}

const char* TraceArrayConstChunkIterator::getClassName() const
{
    return "ConstChunkIterator";
}

ConstTraceArrayChunk::ConstTraceArrayChunk(std::shared_ptr<Trace> trace)
        : TraceClient(trace)
{
    // Show the name from the abstraction rather than the trace iterator method name
    waypoint(__LINE__, "ConstChunk");
}

ConstTraceArrayChunk::~ConstTraceArrayChunk()
{
    // Show the name from the abstraction rather than the trace iterator method name
    waypoint(__LINE__, "~ConstChunk");
}

const ArrayDesc& ConstTraceArrayChunk::getArrayDesc() const
{
    waypoint(__LINE__, __FUNCTION__);
    try {
        return _inputArrayChunk->getArrayDesc();
    }
    CATCH_EXCEPTION(__LINE__, __FUNCTION__)
}

const AttributeDesc& ConstTraceArrayChunk::getAttributeDesc() const
{
    waypoint(__LINE__, __FUNCTION__);
    try {
        const auto& attrDesc = _inputArrayChunk->getAttributeDesc();
        waypoint(__LINE__, __FUNCTION__, attrDesc.getId());
        return attrDesc;
    }
    CATCH_EXCEPTION(__LINE__, __FUNCTION__)
}

Coordinates const& ConstTraceArrayChunk::getFirstPosition(bool withOverlap) const
{
    waypoint(__LINE__, __FUNCTION__);
    try {
        const auto& pos = _inputArrayChunk->getFirstPosition(withOverlap);
        waypoint(__LINE__, __FUNCTION__, pos);
        return pos;
    }
    CATCH_EXCEPTION(__LINE__, __FUNCTION__)
}

Coordinates const& ConstTraceArrayChunk::getLastPosition(bool withOverlap) const
{
    waypoint(__LINE__, __FUNCTION__);
    try {
        const auto& pos = _inputArrayChunk->getLastPosition(withOverlap);
        waypoint(__LINE__, __FUNCTION__, pos);
        return pos;
    }
    CATCH_EXCEPTION(__LINE__, __FUNCTION__)
}

std::shared_ptr<ConstChunkIterator> ConstTraceArrayChunk::getConstIterator(
    int iterationMode) const
{
    waypoint(__LINE__, __FUNCTION__);
    auto inputChunkIter = _inputArrayChunk->getConstIterator(iterationMode);
    auto traceChunkIter =
        std::make_shared<TraceArrayConstChunkIterator>(inputChunkIter,
                                                       *this,
                                                       _trace);
    return traceChunkIter;
}

CompressorType ConstTraceArrayChunk::getCompressionMethod() const
{
    waypoint(__LINE__, __FUNCTION__);
    try {
        auto m = _inputArrayChunk->getCompressionMethod();
        waypoint(__LINE__, __FUNCTION__, m);
        return m;
    }
    CATCH_EXCEPTION(__LINE__, __FUNCTION__)
}

Array const& ConstTraceArrayChunk::getArray() const
{
    waypoint(__LINE__, __FUNCTION__);
    try {
        return _inputArrayChunk->getArray();
    }
    CATCH_EXCEPTION(__LINE__, __FUNCTION__)
}

void ConstTraceArrayChunk::setInputChunk(const ConstChunk* inputArrayChunk)
{
    _inputArrayChunk = inputArrayChunk;
    SCIDB_ASSERT(_inputArrayChunk);
}

const char* ConstTraceArrayChunk::getClassName() const
{
    return "ConstChunk";
}

ConstTraceArrayIterator::ConstTraceArrayIterator(std::shared_ptr<ConstArrayIterator> inputArrayIter,
                                                 TraceArray const& traceArray,
                                                 const AttributeDesc& inputAttr,
                                                 std::shared_ptr<Trace> trace)
        : ConstArrayIterator(traceArray)
        , TraceClient(trace)
        , _inputArrayIter(inputArrayIter)
        , _traceChunk(trace)
        , _inputAttr(inputAttr)
{
    SCIDB_ASSERT(_inputArrayIter);

    // Show the name from the abstraction rather than the trace iterator method name
    waypoint(__LINE__, "ConstArrayIterator attrId", _inputAttr.getId());
}

ConstTraceArrayIterator::~ConstTraceArrayIterator()
{
    // Show the name from the abstraction rather than the trace iterator method name
    waypoint(__LINE__, "~ConstArrayIterator attrId", _inputAttr.getId());
}

bool ConstTraceArrayIterator::end()
{
    waypoint(__LINE__, __FUNCTION__);
    try {
        auto atEnd = _inputArrayIter->end();
        waypoint(__LINE__, __FUNCTION__, atEnd);
        return atEnd;
    }
    CATCH_EXCEPTION(__LINE__, __FUNCTION__)
}

void ConstTraceArrayIterator::operator ++()
{
    waypoint(__LINE__, __FUNCTION__);
    try {
        ++(*_inputArrayIter);
    }
    CATCH_EXCEPTION(__LINE__, __FUNCTION__)
}

Coordinates const& ConstTraceArrayIterator::getPosition()
{
    waypoint(__LINE__, __FUNCTION__);
    try {
        const auto& pos = _inputArrayIter->getPosition();
        waypoint(__LINE__, __FUNCTION__, pos);
        return pos;
    }
    CATCH_EXCEPTION(__LINE__, __FUNCTION__)
}

bool ConstTraceArrayIterator::setPosition(Coordinates const& pos)
{
    waypoint(__LINE__, __FUNCTION__);
    try {
        auto success = _inputArrayIter->setPosition(pos);
        waypoint(__LINE__, __FUNCTION__, pos);
        return success;
    }
    CATCH_EXCEPTION(__LINE__, __FUNCTION__)
}

void ConstTraceArrayIterator::restart()
{
    waypoint(__LINE__, __FUNCTION__);
    try {
        _inputArrayIter->restart();
    }
    CATCH_EXCEPTION(__LINE__, __FUNCTION__)
}

ConstChunk const& ConstTraceArrayIterator::getChunk()
{
    waypoint(__LINE__, __FUNCTION__);
    const auto* inputArrayChunk = &_inputArrayIter->getChunk();
    _traceChunk.setInputChunk(inputArrayChunk);
    return _traceChunk;
}

const char* ConstTraceArrayIterator::getClassName() const
{
    return "ConstArrayIterator";
}

TraceArray::TraceArray(std::shared_ptr<Array> inputArray,
                       const std::string& outputFilePath,
                       InstanceID instanceID)
        : Array(inputArray)
        , TraceClient(nullptr)
        , _trace(std::make_shared<Trace>(outputFilePath, instanceID))
{
    auto pipe = getPipe(0);
    SCIDB_ASSERT(pipe && pipe == inputArray);

    // Show the name from the abstraction rather than the trace iterator method name
    TraceClient::_trace = _trace;
    start(__LINE__, "Array", pipe->getName());
}

TraceArray::~TraceArray()
{
    // Show the name from the abstraction rather than the trace iterator method name
    end(__LINE__, "~Array", getPipe(0)->getName());
}

ArrayDesc const& TraceArray::getArrayDesc() const
{
    waypoint(__LINE__, __FUNCTION__);
    try {
        return getPipe(0)->getArrayDesc();
    }
    CATCH_EXCEPTION(__LINE__, __FUNCTION__)
}

std::shared_ptr<ConstArrayIterator> TraceArray::getConstIteratorImpl(const AttributeDesc& attr) const
{
    waypoint(__LINE__, __FUNCTION__, attr.getId());
    auto inputArrayIter = getPipe(0)->getConstIteratorImpl(attr);
    auto traceArrayIter = std::make_shared<ConstTraceArrayIterator>(inputArrayIter,
                                                                    *this,
                                                                    attr,
                                                                    _trace);
    return traceArrayIter;
}

std::shared_ptr<ArrayIterator> TraceArray::getIteratorImpl(const AttributeDesc& attr)
{
    waypoint(__LINE__, __FUNCTION__, attr.getId());
    try {
        return getPipe(0)->getIterator(attr);
    }
    CATCH_EXCEPTION(__LINE__, __FUNCTION__)
}

const char* TraceArray::getClassName() const
{
    return "Array";
}

}  // namespace scidb
