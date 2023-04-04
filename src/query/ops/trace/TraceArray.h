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

#ifndef __TRACE_ARRAY_H__
#define __TRACE_ARRAY_H__

#include <array/Array.h>
#include <array/ConstArrayIterator.h>
#include <array/ConstChunk.h>

#include <fstream>
#include <mutex>
#include <thread>

namespace scidb {

class Exception;
class TraceArray;

class Trace
{
public:
    /**
     * Constructor.
     * @param outputFilePath the file into which to log the trace
     * @param instanceID this SciDB instance's logical ID.
     */
    Trace(const std::string& outputFilePath,
          InstanceID instanceID);

    /**
     * Destructor.
     */
    ~Trace();

    /**
     * Mark the end of a trace.
     * @param pthis The pointer to the object under trace.
     * @param line The line number to include in the trace.
     * @param classname The name of the class under trace.
     * @param methodName The name of the method under trace.
     * @param operatorName The name of the SciDB operator called
     * by the _trace operator.  This isn't the operator under trace
     * because that would be the operator calling into the
     * TraceArray itself.  However, a point of reference is handy
     * and we can't see the operator calling us, so at least show
     * who we're calling into.
     */
    void end(const void* pthis,
             int line,
             const char* classname,
             const char* methodName,
             const std::string& operatorName);

    /**
     * Mark the start of a trace.
     * @param pthis The pointer to the object under trace.
     * @param line The line number to include in the trace.
     * @param classname The name of the class under trace.
     * @param methodName The name of the method under trace.
     * @param operatorName The name of the SciDB operator called
     * by the _trace operator.  This isn't the operator under trace
     * because that would be the operator calling into the
     * TraceArray itself.  However, a point of reference is handy
     * and we can't see the operator calling us, so at least show
     * who we're calling into.
     */
    void start(const void* pthis,
               int line,
               const char* classname,
               const char* methodName,
               const std::string& operatorName);

    /**
     * Mark a waypoint that caused an exception, usually used from
     * a catch(...) block.
     * @param pthis The pointer to the object under trace.
     * @param line The line number to include in the trace.
     * @param classname The name of the class under trace.
     * @param methodName The name of the method under trace.
     * @param e The exception to log in the trace.
     */
    void waypoint(const void* pthis,
                  int line,
                  const char* classname,
                  const char* methodName,
                  const Exception& e);

    /**
     * Mark a waypoint from any method.
     * @param pthis The pointer to the object under trace.
     * @param line The line number to include in the trace.
     * @param classname The name of the class under trace.
     * @param methodName The name of the method under trace.
     */
    void waypoint(const void* pthis,
                  int line,
                  const char* classname,
                  const char* methodName);

    /**
     * Template to provide an argument along with a waypoint.  There
     * are many enhancement opportunities with this both here in this
     * method and using templates as a whole.
     * @param pthis The pointer to the object under trace.
     * @param line The line number to include in the trace.
     * @param classname The name of the class under trace.
     * @param methodName The name of the method under trace.
     * @param arg An instance of some type to log in the trace
     * output.  Requires that it support stream output operation.
     */
    template <typename ArgumentT>
    void waypoint(const void* pthis,
                  int line,
                  const char* classname,
                  const char* methodName,
                  const ArgumentT& arg)
    {
        std::lock_guard<std::mutex> lock(_methodTraceMutex);
        preamble(pthis, line, classname, methodName)
            << " " << arg
            << std::endl;
    }

private:
    std::ostream& preamble(const void* pthis,
                           int line,
                           const char* classname,
                           const char* methodName);

    std::ofstream _outputFileStream;
    std::mutex _methodTraceMutex;
};

/**
 * Each of the TraceArray family classes should extend privately
 * from TraceClient and implement getClassName.
 */
class TraceClient
{
public:
    /**
     * Constructor.
     * @param trace An instance of the trace object which will be
     * used to generate trace log entries (waypoints).
     */
    TraceClient(std::shared_ptr<Trace> trace);

    /**
     * Destructor.
     */
    virtual ~TraceClient();

    /**
     * Mark the end of a trace, invoked from ~TraceArray.
     * @param line The line number to include in the trace.
     * @param methodName The name of the method under trace.
     * @param operatorName The name of the SciDB operator called
     * by the _trace operator.  This isn't the operator under trace
     * because that would be the operator calling into the
     * TraceArray itself.  However, a point of reference is handy
     * and we can't see the operator calling us, so at least show
     * who we're calling into.
     */
    void end(int line,
             const char* methodName,
             const std::string& operatorName);

    /**
     * Mark the start of a trace, invoked from TraceArray::TraceArray.
     * @param line The line number to include in the trace.
     * @param methodName The name of the method under trace.
     * @param operatorName The name of the SciDB operator called
     * by the _trace operator.  This isn't the operator under trace
     * because that would be the operator calling into the
     * TraceArray itself.  However, a point of reference is handy
     * and we can't see the operator calling us, so at least show
     * who we're calling into.
     */
    void start(int line,
               const char* methodName,
               const std::string& operatorName);

    /**
     * Mark a waypoint that caused an exception, usually used from
     * a catch(...) block.
     * @param line The line number to include in the trace.
     * @param methodName The name of the method under trace.
     * @param e The exception to log in the trace.
     */
    void waypoint(int line,
                  const char* methodName,
                  const Exception& e) const;

    /**
     * Mark a waypoint from any method.
     * @param line The line number to include in the trace.
     * @param methodName The name of the method under trace.
     */
    void waypoint(int line,
                  const char* methodName) const;

    /**
     * Template to provide an argument along with a waypoint.  There
     * are many enhancement opportunities with this both here in this
     * method and using templates as a whole.
     * @param line The line number to include in the trace.
     * @param methodName The name of the method under trace.
     * @param arg An instance of some type to log in the trace
     * output.  Requires that it support stream output operation.
     */
    template <typename ArgumentT>
    void waypoint(int line,
                  const char* methodName,
                  const ArgumentT& arg) const
    {
        _trace->waypoint(static_cast<const void*>(this),
                         line,
                         getClassName(),
                         methodName,
                         arg);
    }

protected:
    virtual const char* getClassName() const = 0;

    std::shared_ptr<Trace> _trace;
};

class ConstTraceArrayChunk;

// TODO:  Not all array API base class objects are overloaded here (e.g., ConstArrayIterator
// is but ArrayIterator is not).  I didn't need them for the initial debugging work that
// motivated this change, so please be aware if tracing such arrays; you'll need to add
// those parts!

class TraceArrayConstChunkIterator : public ConstChunkIterator
                                   , TraceClient
{
 public:
    TraceArrayConstChunkIterator(std::shared_ptr<ConstChunkIterator> inputChunkIter,
                                 const ConstTraceArrayChunk& traceArrayChunk,
                                 std::shared_ptr<Trace> trace);
    virtual ~TraceArrayConstChunkIterator();

    bool end() override;
    void operator ++() override;
    Coordinates const& getPosition() override;
    void restart() override;
    int getMode() const override;
    Value const& getItem() override;
    bool isEmpty() const override;
    ConstChunk const& getChunk() override;
    const Coordinates& getData(scidb::Coordinates& offset,
                               size_t maxValues,
                               std::shared_ptr<BaseTile>& tileData,
                               std::shared_ptr<BaseTile>& tileCoords) override;
    position_t getData(position_t logicalOffset,
                       size_t maxValues,
                       std::shared_ptr<BaseTile>& tileData,
                       std::shared_ptr<BaseTile>& tileCoords) override;
    const Coordinates& getData(scidb::Coordinates& offset,
                               size_t maxValues,
                               std::shared_ptr<BaseTile>& tileData) override;
    virtual position_t getData(position_t logicalOffset,
                               size_t maxValues,
                               std::shared_ptr<BaseTile>& tileData) override;
    position_t getLogicalPosition() override;
    bool setPosition(const Coordinates& pos) override;

 private:
    const char* getClassName() const override;

    std::shared_ptr<ConstChunkIterator> _inputChunkIter;
    const ConstTraceArrayChunk& _traceArrayChunk;
};

class ConstTraceArrayChunk : public ConstChunk
                           , TraceClient
{
 public:
    ConstTraceArrayChunk(std::shared_ptr<Trace> trace);
    virtual ~ConstTraceArrayChunk();

    const ArrayDesc& getArrayDesc() const override;
    const AttributeDesc& getAttributeDesc() const override;
    Coordinates const& getFirstPosition(bool withOverlap) const override;
    Coordinates const& getLastPosition(bool withOverlap) const override;
    std::shared_ptr<ConstChunkIterator> getConstIterator(
        int iterationMode = ConstChunkIterator::IGNORE_OVERLAPS) const override;
    CompressorType getCompressionMethod() const override;
    Array const& getArray() const override;

    /**
     * Set the chunk to trace from the input, called by ConstTraceArrayIterator.
     * @param inputArrayChunk The chunk to trace from the input array.
     */
    void setInputChunk(const ConstChunk* inputArrayChunk);

 private:
    const char* getClassName() const override;

    const ConstChunk* _inputArrayChunk;
};

class ConstTraceArrayIterator : public ConstArrayIterator
                              , TraceClient
{
 public:
    ConstTraceArrayIterator(std::shared_ptr<ConstArrayIterator> inputArrayIterator,
                            TraceArray const& traceArray,
                            const AttributeDesc& inputAttr,
                            std::shared_ptr<Trace> trace);
    virtual ~ConstTraceArrayIterator();

    bool end() override;
    void operator ++() override;
    Coordinates const& getPosition() override;
    bool setPosition(Coordinates const& pos) override;
    void restart() override;
    ConstChunk const& getChunk() override;

 private:
    const char* getClassName() const override;

    std::shared_ptr<ConstArrayIterator> _inputArrayIter;
    ConstTraceArrayChunk _traceChunk;
    AttributeDesc _inputAttr;
};

class TraceArray : public Array
                 , TraceClient
{
 public:
    TraceArray(std::shared_ptr<Array> inputArray,
               const std::string& outputFilePath,
               InstanceID instanceID);
    virtual ~TraceArray();

    ArrayDesc const& getArrayDesc() const override;
    std::shared_ptr<ConstArrayIterator> getConstIteratorImpl(const AttributeDesc& attr) const override;
    std::shared_ptr<ArrayIterator> getIteratorImpl(const AttributeDesc& attr) override;

 private:
    const char* getClassName() const override;

    std::shared_ptr<Trace> _trace;  // Arrays can go out-of-scope before chunk and iterator instances
};

}  // namespace scidb

#endif  //  __TRACE_ARRAY_H__
