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
 * @file ChunkLoader.h
 * @brief Format-specific helper classes for loading chunks.
 */

#ifndef CHUNK_LOADER_H
#define CHUNK_LOADER_H

#include "TextScanner.h"
#include <array/TemplateParser.h>
#include <array/ArrayDistributionInterface.h>
#include <array/ArrayDesc.h>
#include <util/CsvParser.h>
#include <util/RegionCoordinatesIterator.h>
#include <array/MemChunk.h>

#include <memory>
#include <string>
#include <vector>

namespace scidb {

    class ArrayDesc;
    class InputArray;
    class Query;

    /**
     * Abstract base class for objects that load chunks from different file formats.
     */
    class ChunkLoader
    {
    public:

        /*
         * The value of LOOK_AHEAD needs to be at least 3 because RemoteMergedArray,
         * which streams data to the client, prefetches upto 2 chunks ahead.
         * There maybe other reasons as well ...
         */
        enum { LOOK_AHEAD = 3 };

        // 'structors
        static ChunkLoader* create(std::string const& format);
        virtual ~ChunkLoader();

        /// Set parent backpointer and initialize based on parent and query.
        void    bindArray(InputArray* parent, ArrayDistPtr const& distributionForNextChunk,
                          std::shared_ptr<Query>& query);
        bool    isBound() const { return _inArray != 0; }

        /// Open the file, return the resulting errno.
        int     openFile(std::string const& fileName);

        /// Open the string, return the resulting errno... probably zero!
        int     openString(std::string const& dataString);

        /// Return the path used to open this InputArray.
        std::string const& filePath() const { return _path; }

        virtual bool    isBinary() const     { return false; }

        /// Accessors used for error reporting.  Virtual because
        /// "text" format tracks these differently.
        /// @{
        virtual off_t       getFileOffset() const   { return _fileOffset; }
        virtual unsigned    getLine() const         { return _line; }
        virtual unsigned    getColumn() const       { return _column; }
        std::string         getBadField() const     { return _badField; }
        Coordinates const&  getChunkPos() const     { return _chunkPos; }
        /// @}

        enum WhoseChunk { MY_CHUNK, ANY_CHUNK };
        void            nextImplicitChunkPosition(WhoseChunk whoseChunk);

        MemChunk&       getLookaheadChunk(AttributeID attr, size_t chunkIndex);

        virtual bool    loadChunk(std::shared_ptr<Query>& query,
                                  size_t chunkIndex) = 0;

        /**
         * Examine a field to see if it is a database null.
         *
         * @param s the field contents
         * @return -1 if not a database null
         * @return 0 <= n < 128 if field is a null.  'n' is the "missing reason" code.
         */
        static int8_t   parseNullField(const char*s);

    protected:
        ChunkLoader();

        /**
         * Called by the #InputArray constructor when the array is bound to this ChunkLoader.
         *
         * @description ChunkLoader subclasses can be constructed without an active query or a load
         * schema (see InputArray::isSupportedFormat()).  This hook is called when a load schema and
         * query are finally available.  ChunkLoader subclasses can legitimately call the query(),
         * array(), and schema() const methods once this hook has been entered.  In short,
         * subclasses should put constructor code that depends on the array, schema, or query into
         * their bindHook() methods.
         */
        virtual void            bindHook() {}

        /// Called to inform subclasses that an input file is open and calls to fp() are now OK.
        virtual void            openHook() {}

        /// Log (and maybe throw) on out-of-sequence chunks.
        void enforceChunkOrder(const char* caller);

        InputArray*             array() { return _inArray; }
        ArrayDesc const&        schema() const;
        FILE*                   fp() { return _fp; }
        std::shared_ptr<Query>  query();
        std::shared_ptr<PhysicalOperator>  physicalOperator();
        size_t                  numInstances() const { return _numInstances; }
        InstanceID              myInstance() const { return _myInstance; }
        InstanceID              myPhysInstance() const { return _myPhysInstance; }
        AttributeID             emptyTagAttrId() const {return _emptyTagAttrId;}
        bool                    isParallelLoad() const;
        bool                    canSeek() const { return _isRegularFile; }
        Value&                  attrVal(AttributeID id) {return _attrVals[id];}
        TypeId const&           typeIdOfAttr(AttributeID id) const { return _attrTids[id]; }
        FunctionPointer         converter(AttributeID id) const { return _converters[id]; }
        bool                    hasOption(char opt) const { return _options.find(opt) != std::string::npos; }

        // Not necessarily up to date at all times.  Subclasses should
        // set these before signalling an error.
        off_t           _fileOffset;
        unsigned        _line;          // for non-line-oriented input, record number
        unsigned        _column;
        std::string     _badField;
        Coordinates     _chunkPos;      // also used to enforce chunk order

    private:
        InputArray*             _inArray; // not owned, do not delete
        FILE*                   _fp;
        std::string             _path;
        size_t                  _numInstances;
        InstanceID              _myInstance;
        InstanceID              _myPhysInstance;
        AttributeID             _emptyTagAttrId;
        bool                    _enforceDataIntegrity;
        bool                    _isRegularFile;
        std::vector<Value>      _attrVals;
        std::vector<TypeId>     _attrTids;
        std::vector<FunctionPointer> _converters;
        Coordinates             _lastChunkPos;
        std::string             _options;

    protected:
        // A call to nextImplicitChunkPosition() will increase chunkPos, except the first call
        // so as not to skip the first chunk.
        // This flag differentiates the first call from subsequent calls.
        bool _isInitialChunkPos;

    private:
        ArrayDistPtr _distributionForNextChunk;
        ArrayDistPtr _distSequential;

        struct LookAheadChunks {
            MemChunk chunks[LOOK_AHEAD];
        };
        std::vector<LookAheadChunks> _lookahead;
        /// true if a data integrity issue has been found
        bool _hasDataIntegrityIssue;
    };

    inline MemChunk&
    ChunkLoader::getLookaheadChunk(AttributeID attr, size_t chunkIndex)
    {
        return _lookahead[attr].chunks[chunkIndex % LOOK_AHEAD];
    }

    class TextChunkLoader : public ChunkLoader
    {
    public:
        TextChunkLoader()
            : _where(W_Start), _coordVal(TypeLibrary::getType(TID_INT64)) {}

        virtual bool loadChunk(std::shared_ptr<Query>& query,
                               size_t chunkIndex);

        virtual off_t       getFileOffset() const   { return _scanner.getPosition(); }
        virtual unsigned    getLine() const         { return _scanner.getLine(); }
        virtual unsigned    getColumn() const       { return _scanner.getColumn(); }

    protected:
        virtual void            openHook();
    private:
        enum Where {
            W_Start,
            W_InsideArray,
            W_EndOfChunk,
            W_EndOfStream
        };
        Where                   _where;
        Value                   _coordVal;
        Scanner                 _scanner;
    };

    class OpaqueMetadataLoaderCompat
    {
    public:
        OpaqueMetadataLoaderCompat(uint32_t version)
        {
            if (version != 1) {
                std::stringstream ss;
                ss << "Unable to parse opaque chunk metadata: incompatible version "
                   << version;
                ASSERT_EXCEPTION_FALSE(ss.str());
            }
        }

        template<class Archive>
        void serialize(Archive& ar,unsigned version)
        {
            ArrayID arrId=INVALID_ARRAY_ID;
            ArrayID uAId=INVALID_ARRAY_ID;
            VersionID versionId=NO_VERSION;
            std::string name;
            Attributes attributes;
            Dimensions dimensions;
            int32_t flags;
            DistType dt;
            if (Archive::is_loading::value)
            {
                ar & arrId;
                ar & uAId;
                ar & versionId;
                ar & name;
                attributes.serializer(ar);
                ar & dimensions;
                ar & flags;
                ar & dt;

            } else {
                ASSERT_EXCEPTION_FALSE("OpaqueMetadataLoaderCompat cannot be used for serialization");
            }
            _arrayDesc.setName(name);
            _arrayDesc.setDimensions(dimensions);
            _arrayDesc.setAttributes(attributes);
        }

        const ArrayDesc& getArrayDesc() { return _arrayDesc; }

    private:
        ArrayDesc  _arrayDesc;
    };

    class OpaqueChunkLoader : public ChunkLoader
    {
    public:
        virtual bool isBinary() { return true; }
        virtual bool loadChunk(std::shared_ptr<Query>& query,
                               size_t chunkIndex
                               /* inout params */);

        // Peek at array metadata from fileName and gleen chunk intervals.
        static void reconcileSchema(ArrayDesc& schema,
                                    std::string const& fileName);
    protected:
        virtual void            bindHook();
    private:
        uint32_t                _signature;

        static void validateHeader(OpaqueChunkHeader const& hdr);
        static ArrayDesc parseArrayDesc(OpaqueChunkHeader const& hdr,
                                        std::string const& arrayDescStr);
    };

    class BinaryChunkLoader : public ChunkLoader
    {
    public:
        BinaryChunkLoader(std::string const& format);
        virtual bool isBinary() { return true; }
        virtual bool loadChunk(std::shared_ptr<Query>& query,
                               size_t chunkIndex
                               /* inout params */);
    protected:
        virtual void            bindHook();
    private:
        std::string             _format;
        ExchangeTemplate        _templ;
        std::vector<Value>      _binVal;
    };

    class TsvChunkLoader : public ChunkLoader
    {
    public:
        TsvChunkLoader();
        virtual ~TsvChunkLoader();
        virtual bool loadChunk(std::shared_ptr<Query>& query,
                               size_t chunkIndex);
        virtual off_t getFileOffset() const { return _errorOffset; }
    protected:
        virtual void            bindHook();
    private:
        char*   _lineBuf;
        size_t  _lineLen;
        off_t   _errorOffset;
        bool    _tooManyWarning;
        bool    _skipLabelLine;
    };

    class CsvChunkLoader : public ChunkLoader
    {
    public:
        CsvChunkLoader();
        virtual ~CsvChunkLoader();
        virtual bool loadChunk(std::shared_ptr<Query>& query,
                               size_t chunkIndex);
    protected:
        virtual void            openHook();
        virtual void            bindHook();
    private:
        CsvParser   _csvParser;
        bool        _tooManyWarning;
        void        skipPastEol();
    };
}

#endif
