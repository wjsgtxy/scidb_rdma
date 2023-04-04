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
* @file ChunkLoader.cpp
* @brief Format-specific helper classes for loading chunks.
* @author Mike Leibensperger <mjl@paradigm4.com>
*/

#include "ChunkLoader.h"
#include "InputArray.h"

#include <array/UnpinAllChunks.h>
#include <query/Query.h>
#include <system/Warnings.h>
#include <util/OnScopeExit.h>
#include <util/FileIO.h>
#include <util/IoFormats.h>
#include <util/StringUtil.h>    // for debugEncode
#include <util/TsvParser.h>

#include <boost/archive/text_iarchive.hpp>

#include <sys/stat.h>

using namespace std;

namespace scidb {

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.qproc.ops.input.chunkloader"));

ChunkLoader* ChunkLoader::create(string const& format)
{
    string baseFmt, fmtOptions;
    if (format.empty()) {
        baseFmt = iofmt::DEFAULT_IO_FORMAT;
    } else {
        iofmt::split(format, baseFmt, fmtOptions);
    }

    ChunkLoader *ret = 0;
    if (baseFmt[0] == '(') {
        ret = new BinaryChunkLoader(baseFmt);
    }
    else if (baseFmt == "opaque") {
        ret = new OpaqueChunkLoader();
    }
    else if (baseFmt == "text" || baseFmt == "store") {
        ret = new TextChunkLoader();
    }
    else if (baseFmt == "tsv") {
        ret = new TsvChunkLoader();
    }
    else if (baseFmt == "csv") {
        ret = new CsvChunkLoader();
    }

    if (ret) {
        ASSERT_EXCEPTION(iofmt::isInputFormat(baseFmt),
                         "Found ChunkLoader for " << format <<
                         " but iofmt::isInputFormat() says no");
        ret->_options = fmtOptions;
    } else {
        ASSERT_EXCEPTION(not iofmt::isInputFormat(baseFmt),
                         "No ChunkLoader for " << format <<
                         " but iofmt::isInputFormat() says yes");
    }

    return ret;
}

ChunkLoader::ChunkLoader()
    : _fileOffset(0)
    , _line(0)                  // for non-line-oriented input, record number
    , _column(0)
    , _inArray(0)
    , _fp(0)
    , _numInstances(0)
    , _myInstance(INVALID_INSTANCE)
    , _myPhysInstance(INVALID_INSTANCE)
    , _emptyTagAttrId(INVALID_ATTRIBUTE_ID)
    , _enforceDataIntegrity(false)
    , _isRegularFile(false)
    , _isInitialChunkPos(true)
    , _hasDataIntegrityIssue(false)
{ }

ChunkLoader::~ChunkLoader()
{
    if (_fp) {
        ::fclose(_fp);
    }
}

int8_t ChunkLoader::parseNullField(const char* s)
{
    // Note we're not allowing leading or trailing whitespace here.

    SCIDB_ASSERT(s);
    if (*s == '\\' && *(s + 1) == 'N' && *(s + 2) == '\0') {
        // Per http://dataprotocols.org/linear-tsv/
        return 0;
    }
    if (*s == '?') {
        const char* cp = s + 1;
        if (!*cp) {
            return -1;          // lone ? does not cut it
        }
        int sum = 0;
        for (; *cp; ++cp) {
            if (!::isdigit(*cp)) {
                return -1;
            }
            sum = (sum * 10) + *cp - '0';
        }
        if (sum > INT8_MAX) {
            throw USER_EXCEPTION(SCIDB_SE_IMPORT_ERROR, SCIDB_LE_BAD_MISSING_REASON) << s;
        }
        return static_cast<int8_t>(sum);
    }
    else if (*s == 'n'
             && *++s == 'u'
             && *++s == 'l'
             && *++s == 'l'
             && *++s == '\0')
    {
        return 0;
    }
    return -1;
}

int ChunkLoader::openFile(string const& fileName)
{
    SCIDB_ASSERT(_fp == NULL);
    _path = fileName;
    // On POSIX "rb" and "r" are equivalent, the 'b' is strictly for C89 compat.
    const char* mode = isBinary() ? "rb" : "r";
    _fp = ::fopen(fileName.c_str(), mode);
    if (_fp) {
        struct stat stbuf;
        int rc = ::fstat(fileno(_fp), &stbuf);
        _isRegularFile = ((rc == 0) && S_ISREG(stbuf.st_mode));
        this->openHook();
        return 0;
    }
    return errno;
}

int ChunkLoader::openString(string const& dataString)
{
    _path = "<string>";
    _fp = openMemoryStream(dataString.c_str(), dataString.size());
    SCIDB_ASSERT(_fp);
    this->openHook();
    return 0;
}

/// @return the schema we are loading into
ArrayDesc const& ChunkLoader::schema() const
{
    SCIDB_ASSERT(_inArray);
    return _inArray->getArrayDesc();
}

/// Validate and return the query pointer.
/// @return shared_ptr to valid Query object
std::shared_ptr<Query> ChunkLoader::query()
{
    SCIDB_ASSERT(_inArray);
    return Query::getValidQueryPtr(_inArray->_query);
}

/// Validate and return the physical operator pointer.
/// @return shared_ptr to valid PhysicalOperator object
std::shared_ptr<PhysicalOperator> ChunkLoader::physicalOperator()
{
    SCIDB_ASSERT(_inArray);
    return _inArray->_phyOp.lock();
}

bool ChunkLoader::isParallelLoad() const
{
    SCIDB_ASSERT(_inArray);
    return _inArray->parallelLoad;
}

// This sort of activity would ordinarily happen in the constructor,
// but I prefer to delay it so that an attempt to construct a
// ChunkLoader for format "foo" can be used to determine that "foo" is
// a supported format, even when no @c InputArray* or @c std::shared_ptr<Query> is
// present.  Otherwise the check for is-supported has to be coded in
// two places, which grosses me out.
//
void ChunkLoader::bindArray(InputArray* parent,
                            ArrayDistPtr const& distribForNextChunk,
                            std::shared_ptr<Query>& query)
{
    _inArray = parent;
    _distributionForNextChunk = distribForNextChunk;
    _enforceDataIntegrity = parent->_enforceDataIntegrity;

    _numInstances = query->getInstancesCount();
    _myInstance = query->getInstanceID();
    _myPhysInstance = query->getPhysicalInstanceID();
    AttributeDesc const* aDesc = schema().getEmptyBitmapAttribute();
    if (aDesc) {
        _emptyTagAttrId = aDesc->getId();
    }

    Dimensions const& dims = schema().getDimensions();
    size_t nDims = dims.size();

    _chunkPos.resize(nDims);
    for (size_t i = 0; i < nDims; i++) {
        _chunkPos[i] = dims[i].getStartMin();
    }

    // _distSequential can be eliminated if/when ANY_CHUNK option to getNextChunkCoord()
    // is eliminated. (_distribForNextChunk will have to be changed for !isParallel() cases.)
    stringstream ss;
    ss << query->getInstanceID();
    _distSequential = ArrayDistributionFactory::getInstance()->construct(dtLocalInstance,
                                                                         DEFAULT_REDUNDANCY,
                                                                         ss.str());

    _isInitialChunkPos = true;

    // It's places like this, where the system touches the disk (or other box-edge I/O)
    // that we need the empty bitmap because it's populated here on chunk load (line ~261).
    // New API like getAttributesWithEmptyBitmap()?
    Attributes const& attrs = schema().getAttributes();
    size_t nAttrs = attrs.size();

    _lookahead.resize(nAttrs);
    _converters.resize(nAttrs);
    _attrTids.resize(nAttrs);
    for (size_t i = 0; i < nAttrs; ++i) {
        _attrTids[i] = attrs.findattr(i).getType();
        if (!isBuiltinType(_attrTids[i])) {
            _converters[i] = FunctionLibrary::getInstance()->findConverter(TID_STRING, _attrTids[i]);
        }
    }

    // For several subclasses, it's convenient to have a cell's worth
    // of Value objects pre-constructed with appropriate output type
    // and size.  For example, the TextChunkLoader's TKN_MULTIPLY
    // feature means the same parsed cell Values get written many
    // times.  And loaders that need to call conversion functions need
    // appropriately-sized Value objects as conversion targets.  That
    // said, there is no requirement that a subclass make use of this
    // Value vector, it's here in ChunkLoader as a convenience.
    //
    _attrVals.resize(nAttrs);
    for (size_t i = 0; i < nAttrs; i++) {
        _attrVals[i] = Value(TypeLibrary::getType(typeIdOfAttr(safe_static_cast<AttributeID>(i))));
        if (attrs.findattr(i).isEmptyIndicator()) {
            _attrVals[i].setBool(true);
        }
    }

    LOG4CXX_TRACE(logger, "Chunkloader::bind() schema/_inArray DistType: "
                  << _distributionForNextChunk->getDistType());
    if (isUndefined(_distributionForNextChunk->getDistType())) {
        LOG4CXX_WARN(logger, "Chunkloader::bind() distribution was dtUndefined");
        // TBD JHM: the caller probaly shouldn't be passing a dtUndefined that way.
        // put an abort() here and find out the case and fix it, or comment why its legal
        // the array descriptor does not specify a distribution,
        // dtUndefined will not work here
        SCIDB_ASSERT(isParallelLoad());
        // TODO: convert to synthesizedDistType()
        _distributionForNextChunk = createDistribution(defaultDistTypeInput());
        LOG4CXX_TRACE(logger, "Chunkloader::bind() _distributionForNextChunk->DT changed to "
                      << _distributionForNextChunk->getDistType());
    }
    LOG4CXX_TRACE(logger, "Chunkloader::bind() isParallelLoad: " << isParallelLoad());

    // Tell derived classes they can look at the schema() now.
    this->bindHook();
}

void ChunkLoader::nextImplicitChunkPosition(WhoseChunk whose)
{
    // Easy case first: dataframes!
    if (isDataframe(_distributionForNextChunk->getDistType())) {
        bool physMode = dataframePhysicalIdMode();
        _chunkPos[DF_INST_DIM] = physMode ? myPhysInstance() : myInstance();
        _chunkPos[DF_SEQ_DIM] += schema().getDimensions()[DF_SEQ_DIM].getChunkInterval();
        LOG4CXX_TRACE(logger, "ChunkLoader::nextImplicitChunkPosition(): Dataframe, " << CoordsToStr(_chunkPos)
                      << ", physMode=" << physMode);
        return;
    }

    LOG4CXX_TRACE(logger, "ChunkLoader::nextImplicitChunkPosition(): whose " << whose
                          << " (myInstance:" << myInstance() << ")");

    // TBD JHM: consideration of !isPara...() might be removed if _distForNext is dtLocalInstance when !isPara...()
    bool sequential = (whose != MY_CHUNK || !isParallelLoad());
    ArrayDistPtr distToAdvance = sequential ? _distSequential : _distributionForNextChunk;

    bool found;
    if (_isInitialChunkPos) {
        _isInitialChunkPos = false;
        found = distToAdvance->getFirstChunkCoord(_chunkPos, schema().getDimensions(),
                                                  _chunkPos, numInstances(), myInstance());
        SCIDB_ASSERT(found);
    } else {
        found = distToAdvance->getNextChunkCoord(_chunkPos, schema().getDimensions(),
                                                 _chunkPos, numInstances(), myInstance());
    }

    if (!found) {
        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_NEXT_CHUNK_OOB);
    }
    LOG4CXX_TRACE(logger, "ChunkLoader::nextImplicitChunkPosition(): new _chunkPos " << CoordsToStr(_chunkPos));
}

void ChunkLoader::enforceChunkOrder(const char *caller)
{
    if (_lastChunkPos.size() == 0) {
        // First time, no previous chunk.
        _lastChunkPos = _chunkPos;
        return;
    }

    CoordinatesLess comp;

    // Check that this explicit chunkPos isn't inconsistent
    // (ie. out of order). We should always grow chunk
    // addresses.
    if (!comp(_lastChunkPos, _chunkPos)) {
        if (!_hasDataIntegrityIssue) {
            LOG4CXX_WARN(logger, "Given that the last chunk processed was " << CoordsToStr(_lastChunkPos)
                         << " this chunk " << CoordsToStr(_chunkPos) << " is out of sequence ("
                         << caller << ")"
                         << ". Add scidb.qproc.ops.input.chunkloader=TRACE to the log4cxx config file for more");
            _hasDataIntegrityIssue = true;
        } else {
            LOG4CXX_WARN(logger, "Given that the last chunk processed was " << CoordsToStr(_lastChunkPos)
                          << " this chunk " << CoordsToStr(_chunkPos) << " is out of sequence ("
                          << caller << ")");
        }
        if (_enforceDataIntegrity) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_DUPLICATE_CHUNK_ADDR)
                << CoordsToStr(_chunkPos);
        }
    }

    _lastChunkPos = _chunkPos;
}

/**********************************************************************/

void OpaqueChunkLoader::bindHook()
{
    _signature = OpaqueChunkHeader::calculateSignature(schema());
}

static void reconcileArrayMetadata(ArrayDesc& a1,       // target array schema
                                   ArrayDesc const& a2, // opaque file schema
                                   bool strict = false)
{
    // IMPORTANT: We are only allowed to modify 'a1' if *NOT*
    // 'strict'!  Otherwise our contract with compareArrayMetadata()
    // is broken.

    Dimensions& dims1 = a1.getDimensions();
    Attributes const& attrs1 = a1.getAttributes();
    Dimensions const& dims2 = a2.getDimensions();
    Attributes const& attrs2 = a2.getAttributes();
    size_t nDims = dims1.size();
    size_t nAttrs = attrs1.size();

    if (nDims != dims2.size()) {
        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_ARRAYS_NOT_CONFORMANT)
        << "Dimensions do not match";
    }
    if (nAttrs != attrs2.size()) {
        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_ARRAYS_NOT_CONFORMANT)
        << "Attributes do not match";
    }

    for (size_t i = 0; i < nDims; i++) {
        if (dims1[i].isAutochunked()) {
            if (strict) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ARRAYS_NOT_CONFORMANT)
                    << "Autochunked intervals should be resolved by now";
            } else {
                // Reconcile!
                dims1[i].setChunkInterval(dims2[i].getChunkInterval());
            }
        } else if (dims1[i].getChunkInterval() != dims2[i].getChunkInterval()) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_ARRAYS_NOT_CONFORMANT)
                << "Dimension intervals do not match";
        }
        if (dims1[i].getChunkOverlap() != dims2[i].getChunkOverlap()) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_ARRAYS_NOT_CONFORMANT)
                << "Dimension overlaps do not match";
        }
        if (dims1[i].getStartMin() > dims2[i].getStartMin()) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_ARRAYS_NOT_CONFORMANT)
                << "Low dimension boundary violated";
        }
        if (dims1[i].getEndMax() < dims2[i].getEndMax()) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_ARRAYS_NOT_CONFORMANT)
                << "High dimension boundary violated";
        }
    }

    for (size_t i = 0; i < nAttrs; i++) {
        if (attrs1.findattr(i).getType() != attrs2.findattr(i).getType()) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_ARRAYS_NOT_CONFORMANT)
            << "Attribute types do not match";
        }
        if (attrs1.findattr(i).getFlags() != attrs2.findattr(i).getFlags()) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_ARRAYS_NOT_CONFORMANT)
            << "Attribute flags do not match";
        }
    }
}

static void compareArrayMetadata(ArrayDesc const& a1, ArrayDesc const& a2)
{
    reconcileArrayMetadata(const_cast<ArrayDesc&>(a1), a2, true);
}

void OpaqueChunkLoader::validateHeader(OpaqueChunkHeader const& hdr)
{
    if (hdr.magic != OPAQUE_CHUNK_MAGIC) {
        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR10);
    }
    if (hdr.version != SCIDB_OPAQUE_FORMAT_VERSION &&
        hdr.version != SCIDB_OPAQUE_FORMAT_VERSION-1) {
        // We must be able to load opaque data from the previous release of SciDB
        // (and obviously from the current one).
        // SCIDB_OPAQUE_FORMAT_VERSION does not necessarily change from one SciDB release to the next.
        // Technically, supporting the previous opaque format version is a stronger guarantee,
        // but it does not appear more burdensome.
        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_MISMATCHED_OPAQUE_FORMAT_VERSION)
            << hdr.version << SCIDB_OPAQUE_FORMAT_VERSION;
    }
}

ArrayDesc OpaqueChunkLoader::parseArrayDesc(OpaqueChunkHeader const& hdr,
                                            string const& arrayDescStr)
{
    stringstream ss;
    ss << arrayDescStr;
    boost::archive::text_iarchive ia(ss);
    if (hdr.version == SCIDB_OPAQUE_FORMAT_VERSION) {
        ArrayDesc result;
        ia & result;
        return result;
    }
    OpaqueMetadataLoaderCompat metaLoader(hdr.version);
    ia & metaLoader;
    return metaLoader.getArrayDesc();
}

// For good or ill, the opaque loader doesn't bother to upcall to
// handleError(), it just throws.  Assumption is that this data was
// saved by SciDB, so elaborate error reporting (via the shadow array)
// is never needed (and was never supported).  (Since then, the
// shadow array was eliminated, but this comment remains should similar
// error reporting schemes be re-instituted.)
//
bool OpaqueChunkLoader::loadChunk(std::shared_ptr<Query>& query, size_t chunkIndex)
{
    Dimensions const& dims = schema().getDimensions();
    Attributes const& attrs = schema().getAttributes();
    size_t nAttrs = attrs.size();
    size_t nDims = dims.size();

    // Can't call ftell/fseek/etc on a pipe, oh well.
    SCIDB_ASSERT(!canSeek() || _fileOffset == ::ftell(fp()));

    OpaqueChunkHeader hdr;
    for (size_t i = 0; i < nAttrs; i++) {
        if (scidb::fread_unlocked(&hdr, sizeof hdr, 1, fp()) != 1) {
            if (i == 0) {
                return false;
            }
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_FILE_READ_ERROR) << ferror(fp());
        }
        _fileOffset += sizeof(hdr);
        validateHeader(hdr);
        if (hdr.flags & OpaqueChunkHeader::ARRAY_METADATA)  {
            string arrayDescStr;
            arrayDescStr.resize(hdr.size);
            if (scidb::fread_unlocked(&arrayDescStr[0], 1, hdr.size, fp()) != hdr.size) {
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_FILE_READ_ERROR) << ferror(fp());
            }
            _fileOffset += hdr.size;
            ArrayDesc opaqueDesc(parseArrayDesc(hdr, arrayDescStr));
            compareArrayMetadata(schema(), opaqueDesc);
            i -= 1; // compensate increment in for: repeat loop and try to load more mapping arrays
            continue;
        }
        if (hdr.signature != _signature) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_ARRAYS_NOT_CONFORMANT)
            << "Chunk array metadata mismatch";
        }
        if (hdr.nDims != nDims) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_WRONG_NUMBER_OF_DIMENSIONS);
        }
        if (scidb::fread_unlocked(&_chunkPos[0], sizeof(Coordinate), hdr.nDims, fp()) != hdr.nDims) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_FILE_READ_ERROR) << ferror(fp());
        }
        _fileOffset += sizeof(Coordinate) * hdr.nDims;
        if (hdr.attrId != i) {
             throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_UNEXPECTED_DESTINATION_ATTRIBUTE)
                 << attrs.findattr(i).getName();
        }
        if (i==0) {
            enforceChunkOrder("opaque loader");
        }
        Address addr(safe_static_cast<AttributeID>(i), _chunkPos);
        MemChunk& chunk =
            getLookaheadChunk(safe_static_cast<AttributeID>(i), chunkIndex);
        chunk.initialize(array(), &schema(), addr, static_cast<CompressorType>(hdr.compressionMethod));
        chunk.allocate(hdr.size);
        if (scidb::fread_unlocked(chunk.getWriteData(), 1, hdr.size, fp()) != hdr.size) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_FILE_READ_ERROR) << ferror(fp());
        }
        _fileOffset += hdr.size;
        // SDB-5220: DON'T do this safe_static_cast, chunk elements can exceed 32-bits!!!
        // (Wasn't useful anyway: not the line number, and  doesn't easily locate the problem.)
        // _line += safe_static_cast<unsigned>(chunk.getNumberOfElements(false /*no overlap*/));
        chunk.write(query);
    }

    SCIDB_ASSERT(!canSeek() || _fileOffset == ::ftell(fp()));

    return true;
}

/**
 * @brief Peek at array metadata from data file and gleen chunk intervals.
 *
 * @param[in,out] schema data file must agree with this schema, except for
 *                       AUTOCHUNKED chunk sizes, which are set here according
 *                       to their values from the data file
 * @param[in] path filename of instance-local opaque data file
 *
 * @description We want to permit the schema for a load() or input()
 * operator to have unspecified chunk sizes.  For formats other than
 * 'opaque', that is handled by doing static chunk estimation.  But
 * loading from opaque data files presents a chicken-and-egg problem:
 *
 * - The required chunk sizes are encoded in the data file,
 * - PhysicalInput::execute() must return an Array with known chunk sizes,
 * - The chunk sizes cannot be known until the data is read,
 * - The data cannot be read prior to PhysicalInput::execute(),
 * - "Your logic was impeccable, Captain. We are in grave danger." -- Mr. Spock
 *
 * So here we break the cycle: LogicalInput::inferSchema() can call
 * this static method to peak at the encoded array metadata, check it
 * for consistency, and absorb any previously unspecified chunk sizes.
 *
 * @note For parallel opaque loads, we can expect that every instance
 * (including the query coordinator instance that is calling this
 * method) has at least a vestigial input file with an ARRAY_METADATA
 * section.  If that ceases to be so, we're scrod and must throw.
 *
 * @note Similarly, if 'path' names a pipe or socket, we cannot rewind
 * to let InputArray read it from the beginning, so we must throw.
 */
void OpaqueChunkLoader::reconcileSchema(ArrayDesc& schema, string const& path)
{
    SCIDB_ASSERT(schema.isAutochunked()); // ...else why are we here?

    // Throw if not a regular file, since we can't be assured we can
    // rewind it.  Intended to prevent failed reads from pipes and
    // sockets, but other file types are sketchy as well.
    struct stat stbuf;
    int rc = ::stat(path.c_str(), &stbuf);
    if (rc < 0) {
        throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_CANT_STAT_FILE)
            << path << ::strerror(errno)
            << "Needed to resolve unspecified chunk sizes";
    }
    if (!S_ISREG(stbuf.st_mode)) {
        throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_CANT_REWIND_FILE)
            << path << "Needed to resolve unspecified chunk sizes";
    }

    // Open the file, taking care to avoid file descriptor leaks.
    FILE* fp = scidb::fopen(path.c_str(), "r");
    if (!fp) {
        throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_CANT_OPEN_FILE)
            << path << ::strerror(errno) << errno;
    }
    OnScopeExit closeFile([fp]() { scidb::fclose(fp); });

    // Read and validate the array metadata header.
    OpaqueChunkHeader hdr;
    if (scidb::fread_unlocked(&hdr, sizeof hdr, 1, fp) != 1) {
        LOG4CXX_ERROR(logger, "Failed to read an opaque chunk header from "
                  << path << "while trying to resolve unspecified chunk sizes");
        throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_FILE_READ_ERROR)
            << ::ferror(fp);
    }
    validateHeader(hdr);
    ASSERT_EXCEPTION((hdr.flags & OpaqueChunkHeader::ARRAY_METADATA),
                 "Opaque data file should begin with ARRAY_METADATA header");

    // Read and reconcile the array metadata itself.
    string arrayDescStr;
    arrayDescStr.resize(hdr.size);
    if (scidb::fread_unlocked(&arrayDescStr[0], 1, hdr.size, fp) != hdr.size) {
        LOG4CXX_ERROR(logger, "Failed to read array metadata from " << path <<
                      "while trying to resolve unspecified chunk sizes");
        throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_FILE_READ_ERROR)
            << ::ferror(fp);
    }
    ArrayDesc opaqueDesc(parseArrayDesc(hdr, arrayDescStr));
    reconcileArrayMetadata(schema, opaqueDesc);

    // Done, closeFile lambda closes the data file for us.
    SCIDB_ASSERT(!schema.isAutochunked());
}

/**********************************************************************/

BinaryChunkLoader::BinaryChunkLoader(std::string const& format)
    : _format(format)
{ }

void BinaryChunkLoader::bindHook()
{
    _templ = TemplateParser::parse(schema(), _format, true);

    // We use this _binVal vector to minimize code churn in the
    // loadChunk method, but it would be so much better to have a
    // Value constructor that could point at pre-allocated external
    // memory, i.e. the buf vector in loadChunk.  That would avoid a
    // *lot* of realloc(3) calls on string attributes.
    //
    Attributes const& attrs = schema().getAttributes();
    _binVal.resize(attrs.size());
}

//
// the following 4 function/methods have useful trace statements included
// however having them in the code as LOG4CXX_TRACE slows down the query
// load(array_name, '/public/data/graph500/g500s19.dat', -2, '(int64,int64)') by a factor of 1.35
// therefore these traces are left in the code as a LOCALXX_TRACE macro, which can be enabled and
// disabled by a developer
#define WITH_LOCALXX_TRACING 0
#if WITH_LOCALXX_TRACING
# define LOCALXX_TRACE(logger, expression) LOG4CXX(logger, (expression))
#else
# define LOCALXX_TRACE(logger, expression) // as nothing
#endif

///
/// helper for readField() and readByteSequence()
/// @param dbgName[in] -- string printed in LOG4 messages within
/// @param rowReads[in] -- (one-based) 1st, 2nd, 3rd, etc read of the cell
/// @param fp[in] -- FILE* used by the fread_unlocked()
/// @param bytesReq[in] -- number of bytes requested on the fread_unlocked()
/// @param bytesRead[in] -- return value of the fread_unlocked()
/// @return true if data was read correctly, false if 1st read && EOF detected
/// all other cases throw FILE_READ_ERROR
///
bool expectedEOF(const char* dbgName, size_t rowReads, FILE* fp, size_t bytesReq, size_t bytesRead) {
    assert(bytesReq>0);

    LOCALXX_TRACE(logger, "expectedEOF: @ " << dbgName << " rowReads " << rowReads);
    LOCALXX_TRACE(logger, "expectedEOF: bytesReq " << bytesReq << " vs bytesRead " << bytesRead);

    if(bytesRead == bytesReq) {  // was it the "good" case? [typical]
        LOCALXX_TRACE(logger, "expectedEOF: returns true");
        return true;    // yes, the data was read correctly
    }

    auto error = ferror(fp);
    auto eof   = feof(fp);
    if(rowReads<2 && !error && eof && !bytesRead) { // EOF on 1st fread_unlocked() of cell?
        LOCALXX_TRACE(logger, "expectedEOF: acceptable EOF");
        return false; // no data, EOF OK
    }

    // all other cases are errors, log it in detail NOTE: not LOCALXX
    LOG4CXX_ERROR(logger, "binary loader: ERROR, rowReads " << rowReads
                          << " errorCode " << error
                          << " isEOF " << eof
                          << " bytesRead " << bytesRead
                          << " bytesReq " << bytesReq);

    // NOTE: in the future, we need to distinguish, where possible, whether
    //       it was bad formatting of the file by the user (e.g. premature EOF)
    //       or something worse (bug in code, filesystem problem, etc)
    //       The code historically only raised FILE_READ_ERROR, so we continue
    //       to do that, until the code could make use of multiple
    //       types of exception.
    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_FILE_READ_ERROR) << error;
}


///
/// helper for loadChunk()
/// this is used to read fixed-size fields
/// @return true if data was read correctly, false if 1st read && EOF detected
///
template<typename T>
bool readField(const char* dbgName, T& value, FILE* fp, size_t& rowReads)
{
    rowReads++;
    auto bytesWanted = sizeof(value);
    auto bytesRead=scidb::fread_unlocked(&value, 1, bytesWanted, fp);
    bool isValueValid = expectedEOF(dbgName, rowReads, fp, bytesWanted, bytesRead);
    if(WITH_LOCALXX_TRACING) {
        if(isValueValid) {
            LOCALXX_TRACE(logger, "  "<<dbgName<<"=" << value <<")");
        }
    }
    return  isValueValid;       // false -> acceptable EOF
}

///
/// helper for loadChunk()
/// this is used to read sequences of char from the input
/// this routine uses length, not null-termination
/// @return true if data was read correctly, false if 1st read && EOF detected
///
bool readByteSequence(void* buf, size_t bytesWanted, FILE* fp, size_t& rowReads)
{
    rowReads++;

    if (bytesWanted == 0) { // 0-length strings allowed
        return true;        // no actual fread, but does count as a "rowRead" (cellRead)
                            // because no longer on cell boundary
    } else {
        auto bytesRead=scidb::fread_unlocked(buf, 1, bytesWanted, fp);
        bool isValueValid = expectedEOF("readByteSequence", rowReads, fp, bytesWanted, bytesRead);

        if(WITH_LOCALXX_TRACING) {
            if(isValueValid && logger->isTraceEnabled()) {
                const char* bufAsChars = reinterpret_cast<char*>(buf);
                std::string asString(bufAsChars, bytesRead);
                LOCALXX_TRACE(logger, "  readByteSequence: '" << asString << "')");
            }
        }
        return isValueValid;       // the field was read completely
    }
}

/**-
 * Read binary data based on a template.
 * @see ArrayWriter
 * @see saveUsingTemplate()
 * @note this method is > 200 lines long when it has adequate tracing in it to find bugs
 *       it is due for refactoring.
 */
bool BinaryChunkLoader::loadChunk(std::shared_ptr<Query>& query, size_t chunkIndex)
{
    // It would be nice to SCIDB_ASSERT(_fileOffset == ::ftell(fp())) in a
    // few places, but use of FIFOs for input makes that infeasible.

    Attributes const& attrs = schema().getAttributes();
    size_t nAttrs = attrs.size();

    vector< std::shared_ptr<ChunkIterator> > chunkIterators(nAttrs);
    OnScopeExit onExit([this, &chunkIterators] () {
            unpinAllChunks(chunkIterators, logger);
        });

    Value emptyTagVal;
    emptyTagVal.setBool(true);

    // NOTE: we'd like the code to function correctly without any use of the following
    // getc()/ungetc() "trick" (since there are no unlocked_stdio equivalents)
    // however, the code fails without this block
    // further refactoring should look at what modifications to object state
    // happen between here and the top of the while loop that are perhaps being
    // done prematurely
    {
        int ch = getc(fp());
        if (ch == EOF) {
            LOG4CXX_DEBUG(logger, "loadChunk early return");    // NOTE: not LOCALXX_TRACE
            return false;       // when first read on empty chunk fails, must return false, not throw error
        }
        ungetc(ch, fp());
    }


    LOG4CXX_TRACE(logger, "BinaryChunkLoader::loadChunk(): nextImplicitChunkPosition(MY_CHUNK)");
    nextImplicitChunkPosition(MY_CHUNK);
    enforceChunkOrder("binary loader");

    // Initialize a chunk for each attribute.  This initializes half
    // of the lookahead chunks, and obtains iterators for them.  (We
    // don't seem to be doing any actual lookahead in this code path.)
    for (size_t i = 0; i < nAttrs; i++) {
        Address addr(safe_static_cast<AttributeID>(i), _chunkPos);
        MemChunk& chunk = getLookaheadChunk(safe_static_cast<AttributeID>(i), chunkIndex);
        chunk.initialize(array(), &schema(), addr, attrs.findattr(i).getDefaultCompressionMethod());
        chunkIterators[i] = chunk.getIterator(query,
                                              ChunkIterator::NO_EMPTY_CHECK |
                                              ConstChunkIterator::SEQUENTIAL_WRITE);
    }

    // NOTE: potential optimization
    // the format is "row oriented", requiring up to 3 reads and decisions per cell to "parse" the binary input
    // a "column oriented" format could require up to 3 reads per *chunk*.
    // now that the stdio locking is avoided, cpu-profiling will show the cost
    size_t nCols = _templ.columns.size();
    vector<uint8_t> buf(8);
    uint32_t size = 0;
    bool conversionError = false;

    LOCALXX_TRACE(logger, "loadChunk: PRE OUTER WHILE");
    bool initialEOF=false;  // true upon detecting EOF exactly at a cell boundary
    while (!chunkIterators[0]->end() && !initialEOF) {  // for each cell
        size_t rowReads=0;    // actualy counting in-cell reads
        _line += 1;             // really record count
        _column = 0;
        array()->countCell();   // clear lastBadAttr to -1, increase nLoadedCells (prematurely!)
        LOCALXX_TRACE(logger, "loadChunk: WHILE TOP _line "<< _line);
        for (size_t i = 0, j = 0; i < nAttrs; i++, j++) {
            LOCALXX_TRACE(logger, "loadChunk: FOR TOP i "<< i << " j " << j);
            // "skip over" colum data for skipped columns
            while (j < nCols && _templ.columns[j].skip) {
                // NOTE: these reads not in a try block, but later reads are. Why?
                LOCALXX_TRACE(logger, "loadChunk: WHILE SKIPs j "<< j );
                ExchangeTemplate::Column const& column = _templ.columns[j++];
                if (column.nullable) {
                    // potential optimization:
                    // add a skipField() function to fseek() instead of fread_unlocked() + ignore data
                    int8_t missingReason;
                    if(!readField("skip missing", missingReason, fp(), rowReads)) { initialEOF=true; break; }
                    _fileOffset += sizeof(missingReason);
                    LOCALXX_TRACE(logger, "loadChunk: 'skip missing' _fileOffset: "<< _fileOffset);
                }
                size = static_cast<uint32_t>(column.fixedSize);
                LOCALXX_TRACE(logger, "loadChunk: (skip fixedSize: "<< size <<")");
                if (size == 0) {
                    if(!readField("skip size", size, fp(), rowReads)) { initialEOF=true; break;}
                    _fileOffset += sizeof(size);
                    LOCALXX_TRACE(logger, "loadChunk: 'skip size' _fileOffset: "<< _fileOffset);
                }

                if (buf.size() < size) {
                    buf.resize(size * 2);
                }

                if (size) {
                    // potential optimization:
                    // add and use a skipSequence() function, like skipField() mentioned above
                    if(!readByteSequence(&(buf[0]), size, fp(), rowReads)) { initialEOF=true; break;}
                    _fileOffset += size;
                    LOCALXX_TRACE(logger, "loadChunk: skip sequence _fileOffset: "<< _fileOffset);
                }
            }  // end inner while

            if(initialEOF) {
                LOCALXX_TRACE(logger, "loadChunk: break due to initialEOF #1");
                break;
            }

            try { // read (vs skip) a cell
                if (j < nCols) {
                    ExchangeTemplate::Column const& column = _templ.columns[j];
                    LOCALXX_TRACE(logger, "loadChunk: (try column[j: "<<j<<")]");
                    int8_t missingReason = -1;
                    if (column.nullable) {
                        if(!readField("missing", missingReason, fp(), rowReads)) { initialEOF=true; goto endTry;}
                        _fileOffset += sizeof(missingReason);
                        LOCALXX_TRACE(logger, "loadChunk missingReason _fileOffset: "<< _fileOffset);
                    }
                    size = static_cast<uint32_t>(column.fixedSize);
                    LOCALXX_TRACE(logger, "loadChunk: (size=column.fixedSize B: "<<size<<")");
                    if (size == 0) {
                        if(!readField("size", size, fp(), rowReads)) { initialEOF=true; goto endTry;}
                        _fileOffset += sizeof(size);
                        LOCALXX_TRACE(logger, "loadChunk size _fileOffset: "<< _fileOffset);
                    }
                    if (missingReason >= 0) {
                        if (buf.size() < size) {
                            buf.resize(size * 2);
                        }
                        if (size) {
                            // skip over the space reserved for the value (unfortunate design)
                            if(!readByteSequence(&(buf[0]), size, fp(), rowReads)) { initialEOF=true; goto endTry;}
                            _fileOffset += size;
                            LOCALXX_TRACE(logger, "loadChunk 'missing sequence' _fileOffset: "<< _fileOffset);
                        }
                        attrVal(safe_static_cast<AttributeID>(i)).setNull(missingReason);
                        chunkIterators[i]->writeItem(attrVal(safe_static_cast<AttributeID>(i)));
                    } else {
                        // Potential optimization: could there be a version of .setSize() or way of
                        // coding that would avoid a realloc() on each pass through here?
                        // (as is done with buf)
                        _binVal[i].setSize<Value::IGNORE_DATA>(size);
                        if(!readByteSequence(_binVal[i].data(), size, fp(), rowReads)) { initialEOF=true; goto endTry;}
                        _fileOffset += size;
                        LOCALXX_TRACE(logger, "loadChunk 'sequence' _fileOffset: "<< _fileOffset);

                        if (column.converter) {
                            conversionError = false;
                            try {
                                Value const* v = &_binVal[i];
                                column.converter(&v, &attrVal(safe_static_cast<AttributeID>(i)), NULL);
                                chunkIterators[i]->writeItem(attrVal(safe_static_cast<AttributeID>(i)));
                            } catch (...) {
                                conversionError = true; // TODO: latent bug?
                                                        // resets to false only on next non-missing,
                                                        // so maybe a 2nd error after another missing
                                                        // would be incorrectly labelled a conversionError
                                throw;
                            }
                        } else {
                            chunkIterators[i]->writeItem(_binVal[i]);
                        }
                    }
                } else {
                    // empty tag
                    // TODO this puts the empty value at the 'end' of the chunk, sort-of
                    chunkIterators[i]->writeItem(emptyTagVal);
                }
            }
            catch(Exception const& x) {
                if (conversionError) {
                    LOCALXX_TRACE(logger, "loadChunk: catching conversion error");
                    // We don't know _binVal[i]'s type, but this will
                    // at least show us the initial bytes of data.
                    char* s = static_cast<char*>(_binVal[i].data());
                    string badBinVal(s, _binVal[i].size());
                    _badField = badBinVal;
                } else {
                    LOCALXX_TRACE(logger, "loadChunk: catching read error");
                    _badField = "(unreadable)";
                }
                LOCALXX_TRACE(logger, "loadChunk: doing handleError");
                array()->handleError(x, chunkIterators[i], safe_static_cast<AttributeID>(i));
            }

endTry:     if(initialEOF) {
                _line-- ;       // undo premature increment
                LOCALXX_TRACE(logger, "loadChunk: break due to initialEOF #2");
                break;
            }

            _column += 1;
            ++(*chunkIterators[i]);
            LOCALXX_TRACE(logger, "loadChunk: end for at _line " << _line << " _column" << _column);
        }   // end for
        LOCALXX_TRACE(logger, "loadChunk: exited FOR at _line " << _line << " _column" << _column);

        if(initialEOF) {
            LOCALXX_TRACE(logger, "loadChunk: break due to initialEOF #3");
            break;
        }
    } // end for each cell

    LOCALXX_TRACE(logger, "loadChunk: about to flush chunkIterators at _line " << _line << " _column " << _column);
    for (size_t i = 0; i < nAttrs; i++) {
        if (chunkIterators[i]) {
            chunkIterators[i]->flush();
        }
    }

    return true;
}


/**********************************************************************/

void TextChunkLoader::openHook()
{
    _scanner.open(fp(), query(), physicalOperator());
}

bool TextChunkLoader::loadChunk(std::shared_ptr<Query>& query, size_t chunkIndex)
{
    SCIDB_ASSERT(_where != W_EndOfStream);

    Dimensions const& dims = schema().getDimensions();
    Attributes const& attrs = schema().getAttributes();
    AttributeID nAttrs = safe_static_cast<AttributeID>(attrs.size());
    size_t nDims = dims.size();
    vector< std::shared_ptr<ChunkIterator> > chunkIterators(nAttrs);
    OnScopeExit onExit([this, &chunkIterators] () {
            unpinAllChunks(chunkIterators, logger);
        });

    Value tmpVal;

    bool isSparse = false;
BeginScanChunk:
    {
        Token tkn = _scanner.get();
        if (tkn == TKN_SEMICOLON) {
            tkn = _scanner.get();
        }
        if (tkn == TKN_EOF) {
            _where = W_EndOfStream;
            return false;
        }
        bool explicitChunkPosition = false;
        if (_where != W_InsideArray) {
            if (tkn == TKN_COORD_BEGIN) {
                explicitChunkPosition = true;
                for (size_t i = 0; i < nDims; i++)
                {
                    if (i != 0 && _scanner.get() != TKN_COMMA)
                        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR2) << ",";
                    if (_scanner.get() != TKN_LITERAL)
                        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR3);
                    StringToValue( TID_INT64, _scanner.getValue(), _coordVal);
                    _chunkPos[i] = _coordVal.getInt64();
                    if ((_chunkPos[i] - dims[i].getStartMin()) % dims[i].getChunkInterval() != 0)
                        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR4);
                }

                if (_scanner.get() != TKN_COORD_END)
                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR2) << "}";
                tkn = _scanner.get();
                LOG4CXX_TRACE(logger, "Explicit chunk coords are { " << CoordsToStr(_chunkPos) << " }");
            }
            if (tkn != TKN_ARRAY_BEGIN)
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR2) << "[";
            tkn = _scanner.get();
        }
        for (size_t i = 1; i < nDims; i++) {
            if (tkn != TKN_ARRAY_BEGIN)
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR2) << "[";
            tkn = _scanner.get();
        }

        if (tkn == TKN_ARRAY_BEGIN)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR6);

        if (!explicitChunkPosition) {
            LOG4CXX_TRACE(logger, "TextChunkLoader::loadChunk(): nextImplicitChunkPosition(ANY_CHUNK)");
            nextImplicitChunkPosition(ANY_CHUNK);
        } else {
            _isInitialChunkPos = false;
        }
        Coordinates const* first = NULL;
        Coordinates const* last = NULL;
        Coordinates pos = _chunkPos;

        while (true) {
            if (tkn == TKN_COORD_BEGIN) {
                isSparse = true;
                for (size_t i = 0; i < nDims; i++) {
                    if (i != 0 && _scanner.get() != TKN_COMMA)
                        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR2) << ",";
                    if (_scanner.get() != TKN_LITERAL)
                        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR3);
                    StringToValue( TID_INT64, _scanner.getValue(), _coordVal);
                    pos[i] = _coordVal.getInt64();
                }
                if (_scanner.get() != TKN_COORD_END)
                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR2) << "}";
                tkn = _scanner.get();
            }
            bool inParen = false;
            if (tkn == TKN_TUPLE_BEGIN) {
                inParen = true;
                tkn = _scanner.get();
            }
            array()->countCell();
            if (tkn == TKN_LITERAL || (inParen && tkn == TKN_COMMA)) {
                for (AttributeID i = 0; i < nAttrs; i++) {
                    if (!chunkIterators[i]) {
                        if (isSparse && !explicitChunkPosition) {
                            _chunkPos = pos;
                            schema().getChunkPositionFor(_chunkPos);
                            LOG4CXX_TRACE(logger, "New chunk coords { " << CoordsToStr(_chunkPos) << " }");
                        }
                        if (i==0) {
                            enforceChunkOrder("text loader 2");
                        }
                        Address addr(i, _chunkPos);
                        MemChunk& chunk = getLookaheadChunk(i, chunkIndex);
                        chunk.initialize(array(), &schema(), addr,
                                         attrs.findattr(i).getDefaultCompressionMethod());
                        if (first == NULL) {
                            first = &chunk.getFirstPosition(true);
                            if (!isSparse) {
                                pos = *first;
                            }
                            last = &chunk.getLastPosition(true);
                        }
                        chunkIterators[i] = chunk.getIterator(query, ChunkIterator::NO_EMPTY_CHECK
                                                              | (!isSparse ? ConstChunkIterator::SEQUENTIAL_WRITE : 0));
                    }
                    if (!(chunkIterators[i]->setPosition(pos))) {
                        // Load from sparse/dense file {f} at coord {pos} is out of chunk bounds: {chunkPos}
                        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_OOB)
                            << (isSparse ? "sparse" : "dense")
                            << _scanner.getFilePath()
                            << CoordsToStr(pos)
                            << CoordsToStr(_chunkPos);
                    }
                    _fileOffset = _scanner.getPosition();
                    if ((inParen && (tkn == TKN_COMMA || tkn == TKN_TUPLE_END)) || (!inParen && i != 0)) {
                        if (i == emptyTagAttrId()) {
                            attrVal(i).setBool(true);
                            chunkIterators[i]->writeItem(attrVal(i));
                        } else {
                            chunkIterators[i]->writeItem(attrs.findattr(i).getDefaultValue());
                        }
                        if (inParen && tkn == TKN_COMMA) {
                            tkn = _scanner.get();
                        }
                    } else {
                        if (tkn != TKN_LITERAL) {
                            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR8);
                        }
                        try {
                            if (schema().getAttributes().findattr(i).isNullable() && _scanner.isNull()) {
                                if (Value::isValidMissingReason(_scanner.getMissingReason())) {
                                    attrVal(i).setNull(safe_static_cast<Value::reason>(
                                                           _scanner.getMissingReason()));
                                } else {
                                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION,
                                                         SCIDB_LE_BAD_MISSING_REASON)
                                        << _scanner.getMissingReason();
                                }
                            } else if (converter(i)) {
                                tmpVal.setString(_scanner.getValue().c_str());
                                const Value* v = &tmpVal;
                                (*converter(i))(&v, &attrVal(i), NULL);
                            } else {
                                StringToValue(typeIdOfAttr(i),
                                              _scanner.getValue(), attrVal(i));
                            }
                            if (i == emptyTagAttrId()) {
                                if (!attrVal(i).getBool())
                                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR9);
                            }
                            chunkIterators[i]->writeItem(attrVal(i));
                        } catch(Exception const& x) {
                            try
                            {
                                // Scanner tracks position w/in file, load position
                                // info into *this so handleError() can get it.
                                _fileOffset = _scanner.getPosition();
                                _line = _scanner.getLine();
                                _column = _scanner.getColumn();
                                _badField = _scanner.getValue();
                                array()->handleError(x, chunkIterators[i], i);
                            }
                            catch (Exception const& x)
                            {
                                if (x.getShortErrorCode() == SCIDB_SE_TYPE_CONVERSION && i == emptyTagAttrId())
                                {
                                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR15);
                                }
                                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR16);
                            }
                        }
                        tkn = _scanner.get();
                        if (inParen && i+1 < nAttrs && tkn == TKN_COMMA) {
                            tkn = _scanner.get();
                        }
                    }
                    if (!isSparse) {
                        ++(*chunkIterators[i]);
                    }
                }
            } else if (inParen && tkn == TKN_TUPLE_END && !isSparse) {
                for (AttributeID i = 0; i < nAttrs; i++) {
                    if (!chunkIterators[i]) {
                        if (i==0) {
                            enforceChunkOrder("text loader 3");
                        }
                        Address addr(i, _chunkPos);
                        MemChunk& chunk = getLookaheadChunk(i, chunkIndex);
                        chunk.initialize(array(), &schema(), addr,
                                         schema().getAttributes().findattr(i).getDefaultCompressionMethod());
                        if (first == NULL) {
                            first = &chunk.getFirstPosition(true);
                            last = &chunk.getLastPosition(true);
                            pos = *first;
                        }
                        chunkIterators[i] = chunk.getIterator(query,
                                                              ChunkIterator::NO_EMPTY_CHECK|
                                                              ConstChunkIterator::SEQUENTIAL_WRITE);
                    }
                    if (emptyTagAttrId() == INVALID_ATTRIBUTE_ID) {
                        chunkIterators[i]->writeItem(attrs.findattr(i).getDefaultValue());
                    }
                    ++(*chunkIterators[i]);
                }
            }
            if (inParen) {
                if (tkn != TKN_TUPLE_END)
                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR2) << ")";
                tkn = _scanner.get();
                if (!isSparse && tkn == TKN_MULTIPLY) {
                    // Here's why text loader needs entire _attrVals[] vector.
                    tkn = _scanner.get();
                    if (tkn != TKN_LITERAL)
                        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR2) << "multiplier";
                    Value countVal;
                    StringToValue(TID_INT64, _scanner.getValue(), countVal);
                    int64_t count = countVal.getInt64();
                    while (--count != 0) {
                        for (AttributeID i = 0; i < nAttrs; i++) {
                            chunkIterators[i]->writeItem(attrVal(i));
                            ++(*chunkIterators[i]);
                        }
                    }
                    tkn = _scanner.get();
                    pos = chunkIterators[0]->getPosition();
                    pos[nDims-1] -= 1;
                }
            }
            size_t nBrackets = 0;
            if (isSparse) {
                while (tkn == TKN_ARRAY_END) {
                    if (++nBrackets == nDims) {
                        if (first == NULL) { // empty chunk
                            goto BeginScanChunk;
                        }
                        _where = W_EndOfChunk;
                        goto EndScanChunk;
                    }
                    tkn = _scanner.get();
                }
            } else {
                if (NULL == last ) {
                    _where = W_EndOfStream;
                    return false;
                }
                for (size_t i = nDims-1; ++pos[i] > (*last)[i]; i--) {
                    if (i == 0) {
                        if (tkn == TKN_ARRAY_END) {
                            _where = W_EndOfChunk;
                        } else if (tkn == TKN_COMMA) {
                            _where = W_InsideArray;
                        } else {
                            throw USER_EXCEPTION(SCIDB_SE_IMPORT_ERROR, SCIDB_LE_OP_INPUT_ERROR2) << "]";
                        }
                        goto EndScanChunk;
                    }
                    if (tkn != TKN_ARRAY_END)
                        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR2) << "]";
                    nBrackets += 1;
                    pos[i] = (*first)[i];
                    tkn = _scanner.get();
                }
            }
            if (tkn == TKN_COMMA) {
                tkn = _scanner.get();
            }
            while (nBrackets != 0 ) {
                if (tkn != TKN_ARRAY_BEGIN)
                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR2) << "[";
                nBrackets -= 1;
                tkn = _scanner.get();
            }
        }
    }
EndScanChunk:
    if (!isSparse && emptyTagAttrId() == INVALID_ATTRIBUTE_ID) {
        for (size_t i = 0; i < nAttrs; i++) {
            if (chunkIterators[i]) {
                while (!chunkIterators[i]->end()) {
                    chunkIterators[i]->writeItem(attrs.findattr(i).getDefaultValue());
                    ++(*chunkIterators[i]);
                }
            }
        }
    }
    for (size_t i = 0; i < nAttrs; i++) {
        if (chunkIterators[i]) {
            chunkIterators[i]->flush();
        }
    }
    return true;
}

/**********************************************************************/

TsvChunkLoader::TsvChunkLoader()
    : _lineBuf(0)
    , _lineLen(0)
    , _errorOffset(0)
    , _tooManyWarning(false) // warnings squelch
    , _skipLabelLine(false)
{ }

TsvChunkLoader::~TsvChunkLoader()
{
    if (_lineBuf) {
        ::free(_lineBuf);
    }
}

void TsvChunkLoader::bindHook()
{
    // For now at least, flat arrays only.
    Dimensions const& dims = schema().getDimensions();
    if (dims.size() != 1 && !isDataframe(dims)) {
        throw USER_EXCEPTION(SCIDB_SE_IMPORT_ERROR,
                             SCIDB_LE_MULTIDIMENSIONAL_ARRAY_NOT_ALLOWED);
    }

    _skipLabelLine = hasOption('l');
}

bool TsvChunkLoader::loadChunk(std::shared_ptr<Query>& query, size_t chunkIndex)
{
    // Must do EOF check *before* nextImplicitChunkPosition() call, or
    // we risk stepping out of bounds.
    int ch = ::getc(fp());
    if (ch == EOF) {
        return false;
    }
    ::ungetc(ch, fp());

    // Reposition and make sure all is cool.
    LOG4CXX_TRACE(logger, "TsvChunkLoader::loadChunk nextImplicitChunkPosition(MY_CHUNK)");
    nextImplicitChunkPosition(MY_CHUNK);
    enforceChunkOrder("tsv loader");

    // Initialize a chunk and chunk iterator for each attribute.
    Attributes const& attrs = schema().getAttributes();
    AttributeID nAttrs = safe_static_cast<AttributeID>(attrs.size());
    vector< std::shared_ptr<ChunkIterator> > chunkIterators(nAttrs);
    OnScopeExit onExit([this, &chunkIterators] () {
            unpinAllChunks(chunkIterators, logger);
        });

    for (AttributeID i = 0; i < nAttrs; i++) {
        Address addr(i, _chunkPos);
        MemChunk& chunk = getLookaheadChunk(i, chunkIndex);
        chunk.initialize(array(), &schema(), addr, attrs.findattr(i).getDefaultCompressionMethod());
        chunkIterators[i] = chunk.getIterator(query,
                                              ChunkIterator::NO_EMPTY_CHECK |
                                              ConstChunkIterator::SEQUENTIAL_WRITE);
    }

    TsvParser parser;
    if (hasOption('p')) {
        parser.setDelim('|');
    } else if (hasOption('c')) {
        // Seems sick and wrong---should use 'csv' format instead---whatever.
        parser.setDelim(',');
    }

    char const *field = 0;
    unsigned rc = 0;
    bool sawData = false;

    while (!chunkIterators[0]->end()) {

        ssize_t nread = ::getline(&_lineBuf, &_lineLen, fp());
        if (nread == EOF) {
            break;
        }
        if (_skipLabelLine) {
            _skipLabelLine = false;
            continue;
        }

        sawData = true;
        _column = 0;
        _fileOffset += nread;
        _line += 1;
        parser.reset(_lineBuf);
        array()->countCell();

        // Parse and write out a line's worth of fields.
        // NOTE WELL: if you have to 'continue;' after a writeItem()
        // call, make sure the iterator (and possibly the _column)
        // gets incremented.
        //
        for (AttributeID i = 0; i < nAttrs; ++i) {
            try {
                // Handle empty tag...
                if (i == emptyTagAttrId()) {
                    attrVal(i).setBool(true);
                    chunkIterators[i]->writeItem(attrVal(i));
                    ++(*chunkIterators[i]); // ...but don't increment _column.
                    continue;
                }

                // Parse out next input record field.
                rc = parser.getField(field);
                if (rc == TsvParser::EOL) {
                    // Previous getField() set end-of-line, but we have more attributes!
                    throw USER_EXCEPTION(SCIDB_SE_IMPORT_ERROR, SCIDB_LE_OP_INPUT_TOO_FEW_FIELDS)
                        << _fileOffset << _line << _column;
                }
                if (rc == TsvParser::ERR) {
                    throw USER_EXCEPTION(SCIDB_SE_IMPORT_ERROR, SCIDB_LE_TSV_PARSE_ERROR);
                }
                SCIDB_ASSERT(field);

                if (attrs.findattr(i).isNullable()) {
                    int8_t missingReason = parseNullField(field);
                    if (missingReason >= 0) {
                        attrVal(i).setNull(missingReason);
                        chunkIterators[i]->writeItem(attrVal(i));
                        ++(*chunkIterators[i]);
                        _column += 1;
                        continue;
                    }
                } // ...else "null", "?0", "?6", etc. are just strings.

                if (converter(i)) {
                    Value v;
                    v.setString(field);
                    const Value* vp = &v;
                    (*converter(i))(&vp, &attrVal(i), NULL);
                    chunkIterators[i]->writeItem(attrVal(i));
                }
                else {
                    StringToValue(typeIdOfAttr(i), field, attrVal(i));
                    chunkIterators[i]->writeItem(attrVal(i));
                }
            }
            catch (Exception& ex) {
                _badField = field;
                _errorOffset = (_fileOffset - nread) + (field - _lineBuf);
                array()->handleError(ex, chunkIterators[i], i);
            }

            _column += 1;
            ++(*chunkIterators[i]);
        }

        // We should be at EOL now, otherwise there are too many fields on this line.  Post a
        // warning: it seems useful not to complain too loudly about this or to abort the load, but
        // we do want to mention it.
        //
        rc = parser.getField(field);
        if (!_tooManyWarning && (rc != TsvParser::EOL)) {
            _tooManyWarning = true;
            query->postWarning(SCIDB_WARNING(SCIDB_LE_OP_INPUT_TOO_MANY_FIELDS)
                               << _fileOffset << _line << _column);
        }
    }

    for (size_t i = 0; i < nAttrs; i++) {
        if (chunkIterators[i]) {
            chunkIterators[i]->flush();
        }
    }

    return sawData;
}
} // namespace
