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
 * @file CsvChunkLoader.cpp
 * @author Mike Leibensperger <mjl@paradigm4.com>
 */

#include "ChunkLoader.h"
#include "InputArray.h"

#include <array/UnpinAllChunks.h>
#include <query/Query.h>
#include <util/OnScopeExit.h>
#include <util/StringUtil.h>    // for debugEncode
#include <util/CsvParser.h>
#include <system/Warnings.h>


using namespace std;

namespace scidb {

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.qproc.ops.input.csvchunkloader"));

CsvChunkLoader::CsvChunkLoader()
    : _tooManyWarning(false)
{ }

CsvChunkLoader::~CsvChunkLoader()
{ }

void CsvChunkLoader::openHook()
{
    if (hasOption('l')) {
        // Skip label line.
        char *buf = nullptr;
        size_t len = 0;
        (void) ::getline(&buf, &len, fp());
        if (buf) {
            ::free(buf);
        }
    }

    _csvParser
        .setFilePtr(fp())
        .setLogger(logger);

    if (hasOption('p')) {
        _csvParser.setDelim('|');
    } else if (hasOption('t')) {
        _csvParser.setDelim('\t');
    }

    if (hasOption('d')) {
        _csvParser.setQuote('\"');
    } else if (hasOption('s')) {
        _csvParser.setQuote('\'');
    }
}

void CsvChunkLoader::bindHook()
{
    // For now at least, flat arrays only.
    Dimensions const& dims = schema().getDimensions();
    if (dims.size() != 1 && !isDataframe(dims)) {
        throw USER_EXCEPTION(SCIDB_SE_IMPORT_ERROR,
                             SCIDB_LE_MULTIDIMENSIONAL_ARRAY_NOT_ALLOWED);
    }
}

bool CsvChunkLoader::loadChunk(std::shared_ptr<Query>& query, size_t chunkIndex)
{
    // Must do EOF check *before* nextImplicitChunkPosition() call, or
    // we risk stepping out of bounds.
    if (_csvParser.empty()) {
        int ch = ::getc(fp());
        if (ch == EOF) {
            return false;
        }
        ::ungetc(ch, fp());
    }

    // Reposition and make sure all is cool.
    nextImplicitChunkPosition(MY_CHUNK);
    enforceChunkOrder("csv loader");

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

    char const *field = 0;
    int rc = 0;
    bool sawData = false;
    bool sawEof = false;

    while (!sawEof && !chunkIterators[0]->end()) {

        _line += 1;
        _column = 0;
        array()->countCell();
        bool sawEol = false;

        // Parse and write out a line's worth of fields.  NB if you
        // have to 'continue;' after a writeItem() call, make sure the
        // iterator (and possibly the _column) gets incremented.
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

                // Make end-of-line "sticky" for the rest of the cell (as the TsvChunkLoader
                // does automatically).
                if (sawEol) {
                    throw USER_EXCEPTION(SCIDB_SE_IMPORT_ERROR, SCIDB_LE_OP_INPUT_TOO_FEW_FIELDS)
                        << _csvParser.getFileOffset() << _csvParser.getRecordNumber() << _column;
                }

                // Parse out next input field.
                rc = _csvParser.getField(field);
                if (rc == CsvParser::END_OF_FILE) {
                    sawEof = true;
                    break;
                }
                if (rc == CsvParser::END_OF_RECORD) {
                    // Got record terminator, but we have more attributes!
                    sawEol = true;
                    throw USER_EXCEPTION(SCIDB_SE_IMPORT_ERROR, SCIDB_LE_OP_INPUT_TOO_FEW_FIELDS)
                        << _csvParser.getFileOffset() << _csvParser.getRecordNumber() << _column;
                }
                SCIDB_ASSERT(rc == CsvParser::OK);
                SCIDB_ASSERT(field);
                sawData = true;

                // Process input field.

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
                    TypeId const &tid = typeIdOfAttr(i);
                    if (attrs.findattr(i).isNullable() &&
                        (*field == '\0' || (iswhitespace(field) && IS_NUMERIC(tid))))
                    {
                        // [csv2scidb compat] With csv2scidb, empty strings (or for numeric
                        // fields, whitespace) became nulls if the target attribute was
                        // nullable.  We keep the same behavior.  (We should *not* do this for
                        // TSV, that format requires explicit nulls!)
                        attrVal(i).setNull();
                    } else {
                        StringToValue(tid, field, attrVal(i));
                    }
                    chunkIterators[i]->writeItem(attrVal(i));
                }
            }
            catch (Exception& ex) {
                _badField = field;
                _fileOffset = _csvParser.getFileOffset();
                array()->handleError(ex, chunkIterators[i], i);
            }

            _column += 1;
            ++(*chunkIterators[i]);
        }

        if (!sawEof && !sawEol) {
            skipPastEol();
        }
    }

    for (size_t i = 0; i < nAttrs; i++) {
        if (chunkIterators[i]) {
            chunkIterators[i]->flush();
        }
    }

    return sawData;
}


/**
 * We should see EOL now, otherwise there are too many fields on this line.
 *
 * @description Read past the END_OF_RECORD mark.  If this means reading past data fields, post
 * a warning.  (It seems useful not to complain too loudly about this or to abort the load, but
 * we do want to mention it.)
 */
void CsvChunkLoader::skipPastEol()
{
    // Loop needed in case of parser reset in one of these getField() calls.  A reset should
    // always bring us to EOL or EOF, which is what we expect as the precondition of the code
    // below.  (In loadChunk() above we didn't need to worry about calling willReset(), because
    // the @c sawEol flag will make sure that the iterators get resynchronized.)
    char const* field = 0;
    do {
        try {
            int rc = _csvParser.getField(field);
            if (rc == CsvParser::OK) {
                if (!_tooManyWarning) {
                    _tooManyWarning = true;
                    query()->postWarning(SCIDB_WARNING(SCIDB_LE_OP_INPUT_TOO_MANY_FIELDS)
                                         << _csvParser.getFileOffset()
                                         << _csvParser.getRecordNumber()
                                         << _column);
                }

                // Must discard until we find EOL or EOF, otherwise the parser will be out of sync
                // with our position in the array.
                do {
                    rc = _csvParser.getField(field);
                } while (rc == CsvParser::OK);
            }
        }
        catch (Exception& ex) {
            LOG4CXX_WARN(logger, "Error parsing excess fields at record " <<
                         _csvParser.getRecordNumber() << " in CSV input: "
                         << ex.what() << " (ignored)");
        }
    } while (_csvParser.willReset());
}

} // namespace
