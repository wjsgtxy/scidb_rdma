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

/**
 * @file CsvParser.cpp
 * @author Mike Leibensperger <mjl@paradigm4.com>
 */

// Module header always comes first.
#include <util/CsvParser.h>

#include <system/ErrorCodes.h>
#include <system/UserException.h>
#include <system/Utils.h>
#include <util/FileIO.h>
#include <util/Platform.h>
#include <util/Utility.h>

#include <iostream>
#include <stdint.h>
#include <string.h>
#include <sys/stat.h>

#define LOG_INFO(_x)                                    \
do {                                                    \
    if (_logger) {                                      \
        LOG4CXX_INFO(_logger, _x);                      \
    } else {                                            \
        std::cerr << "INFO: " << _x << std::endl;       \
    }                                                   \
} while (0)

#define LOG_WARNING(_x)                                 \
do {                                                    \
    if (_logger) {                                      \
        LOG4CXX_WARN(_logger, _x);                      \
    } else {                                            \
        std::cerr << "WARN: " << _x << std::endl;       \
    }                                                   \
} while (0)

#define CKRET(_expr, _cond)                                     \
do {                                                            \
    int _rc = _expr ;                                           \
    if (!(_rc _cond)) {                                         \
        LOG_WARNING("Call to " << #_expr << " returned " << _rc \
                    << " (" << ::strerror(errno) << ") at "     \
                    << __FILE__ << ":" << __LINE__);            \
        SCIDB_ASSERT(_rc _cond); /* we know it will be false */ \
    }                                                           \
} while (0)

using namespace std;

namespace scidb {

CsvParser::CsvParser(FILE *fp)
    : _fp(fp)
    , _csverr(0)
    , _delim(',')
    , _quote('\0')
    , _lastField(START_OF_FILE, 0, 0, 0)
    , _inbuf(BUF_SIZE)
    , _bufOffset(0)
    , _data(BUF_SIZE + ::getpagesize())
    , _datalen(0)
    , _numRecords(0)
    , _numFields(0)
    , _prevFields(SIZE_MAX)
    , _warnings(0)
    , _nread(0)
    , _wantReset(false)
    , _reparse(NULL)
{
    CKRET(csv_init(&_parser, 0), == 0);
    csv_set_space_func(&_parser, spaceFunc);
    SCIDB_ASSERT(_fields.empty()); // Empty _fields queue says: read more data!
}

CsvParser::~CsvParser()
{
    csv_free(&_parser);
}

CsvParser& CsvParser::setFilePtr(FILE* fp)
{
    SCIDB_ASSERT(_fp == 0);
    SCIDB_ASSERT(fp != 0);
    _fp = fp;
    return *this;
}

CsvParser& CsvParser::setDelim(char delim)
{
    _delim = delim;
    csv_set_delim(&_parser, delim);
    return *this;
}

CsvParser& CsvParser::setQuote(char quote)
{
    _quote = quote;
    csv_set_quote(&_parser, quote);
    return *this;
}

CsvParser& CsvParser::setStrict(bool enable)
{
    // The problem with setting CSV_STRICT mode is that it's unclear
    // how to recover from an error and continue parsing.  This method
    // is here only so that we can experiment with it in the future.
    // For now, be mellow, dude!

    int opts = csv_get_opts(&_parser);
    if (enable) {
        opts |= CSV_STRICT;
    } else {
        opts &= ~CSV_STRICT;
    }
    csv_set_opts(&_parser, static_cast<unsigned char>(opts));
    return *this;
}

CsvParser& CsvParser::setLogger(log4cxx::LoggerPtr logger)
{
    _logger = logger;
    return *this;
}

void CsvParser::reset()
{
    LOG_INFO("Re-initializing CSV parser...");

    // Re-initialize parser state.
    int opts = csv_get_opts(&_parser);
    if (!_quote) {
        _quote = csv_get_quote(&_parser);
    }
    if (!_delim) {
        _delim = csv_get_delim(&_parser);
    }
    csv_free(&_parser);
    CKRET(csv_init(&_parser, 0), == 0);
    csv_set_space_func(&_parser, spaceFunc);
    csv_set_quote(&_parser, _quote);
    csv_set_delim(&_parser, _delim);
    csv_set_opts(&_parser, static_cast<unsigned char>(opts));

    // Very simple-minded error recovery: scan forward, starting with
    // last successfully parsed position in the _inbuf, to get beyond
    // a newline.  Start re-parsing from there.  If we don't find a
    // newline in the current buffer, keep reading.

    if (_reparse == NULL) {
        // _lastField.filepos is the *start* of the last parsed field.
        off_t start = std::max(_bufOffset, _lastField.filepos);
        _reparse = &_inbuf[std::min(size_t(start - _bufOffset), _nread)];
    } else {
        SCIDB_ASSERT(size_t(_reparse - &_inbuf[0]) <= _nread);
    }

    bool found = false;
    do {
        // Search (what's left of) current _inbuf for newline.
        size_t len = _nread - (_reparse - &_inbuf[0]);
        for (size_t i = 0; i < len; ++i) {
            if (*_reparse++ == '\n') {
                // Same EOR logic as in putRecord().
                _numRecords += 1;
                _fields.push_back(Field(END_OF_RECORD, _numRecords, _numFields,
                                        _bufOffset + (_reparse - &_inbuf[0])));
                _numFields = 0;
                found = true;
                break;
            }
        }
        if (_reparse == &_inbuf[_nread]) {
            // Found or not, if we're out of data, read more.
            readNextBuffer();
            _reparse = &_inbuf[0];
        }
    } while (_nread && !found);

    if (!_nread && !found) {
        _fields.push_back(Field(END_OF_FILE, _numRecords, _numFields, _bufOffset));
        LOG_INFO("Hit EOF while restarting CSV parse at file offset " << _bufOffset);
    } else {
        LOG_INFO("Restarting CSV parse at file offset " << (_bufOffset + (_reparse - &_inbuf[0])));
    }
}

int CsvParser::getField(char const*& field)
{
    SCIDB_ASSERT(_fp != 0);
    field = "";                  // Never return junk or NULL.

    if (_csverr) {
        // Errors are forever, at least until I figure out how to
        // recover from CSV_EPARSE when in strict mode.
        return _csverr;
    }

    // Parse next batch if we don't have anything.
    if (_fields.empty()) {
        _csverr = more();
        if (_csverr) {
            return _csverr;
        }
    }
    SCIDB_ASSERT(!_fields.empty());

    // Process head of queue.
    _lastField = _fields.front();
    _fields.pop_front();
    int offset = _lastField.offset;

    switch (offset) {
    case END_OF_RECORD:
        return END_OF_RECORD;
    case END_OF_FILE:
        SCIDB_ASSERT(_fields.empty());
        _fields.push_back(_lastField); // Once done, stay done.
        return END_OF_FILE;
    default:
        field = &_data[offset];
        return OK;
    }

    SCIDB_UNREACHABLE();
}

void CsvParser::readNextBuffer()
{
    // Read next input buffer.
    _bufOffset += _nread;
    _nread = scidb::fread_unlocked(&_inbuf[0], 1, BUF_SIZE, _fp);
    if (_nread == 0) {
        if (::ferror(_fp)) {
            int err = errno ? errno : EIO;
            throw USER_EXCEPTION(SCIDB_SE_IO, SCIDB_LE_FILE_READ_ERROR)
                << ::strerror(err);
        }
    }
}

int CsvParser::more()
{
    SCIDB_ASSERT(_fields.empty()); // else why are we being called?

    if (_wantReset) {
        reset();
        _wantReset = false;
    }

    _datalen = 0;           // Start reusing the _data buffer.

    // Remember our progress so far.
    size_t savedRecords = _numRecords;
    size_t savedFields = _numFields;

    size_t nparse = 0;
    bool error = false;
    if (_reparse) {
        // Using the buffer last seen by reset().
        if (_nread == 0) {
            // EOF encountered during reset.  Call to csv_fini() is
            // probably useless since we just reset the parser, but we
            // do it for form's sake.
            csv_fini(&_parser, fieldCbk, recordCbk, this);
            _fields.push_back(Field(END_OF_FILE, _numRecords, _numFields, _bufOffset + _nread));
            return 0;
        }

        size_t len = _nread - (_reparse - &_inbuf[0]);
        nparse = csv_parse(&_parser, _reparse, len, fieldCbk, recordCbk, this);
        error = (nparse != len);

    } else {
        readNextBuffer();
        if (_nread == 0) {
            // End of file, finish up.
            csv_fini(&_parser, fieldCbk, recordCbk, this);
            _fields.push_back(Field(END_OF_FILE, _numRecords, _numFields, _bufOffset));
            return 0;
        }

        // Guess at the quote character if none has been set.
        if (!_quote) {
            const char *cp = &_inbuf[0];
            for (size_t i = 0; i < _nread; ++i, ++cp) {
                if (*cp == '"' || *cp == '\'') {
                    setQuote(*cp);
                    break;
                }
            }
            if (!_quote) {
                // The save() operator emits single-quotes for backward
                // compatibility, so for input we'll assume the same.
                setQuote('\'');
            }
        }
        SCIDB_ASSERT(_quote);

        // ...and parse the buffer.
        nparse = csv_parse(&_parser, &_inbuf[0], _nread, fieldCbk, recordCbk, this);
        error = (nparse != _nread);
    }

    // Check for parse errors.  In theory this should only happen in
    // "strict" mode.  Note we haven't cleared _reparse yet, so
    // reset() knows that we're still chewing on the same buffer.
    //
    if (error) {
        _wantReset = true;
        int err = csv_error(&_parser);
        throw USER_EXCEPTION(SCIDB_SE_IO, SCIDB_LE_CSV_PARSE_ERROR)
            << (_bufOffset + nparse) << _numRecords << _numFields << csv_strerror(err);
    }

    // It's possible we're now at end-of-file.
    if (::feof(_fp)) {
        csv_fini(&_parser, fieldCbk, recordCbk, this);
        _fields.push_back(Field(END_OF_FILE, _numRecords, _numFields, _bufOffset + _nread));
        return 0;
    }

    // We just read and parsed a fairly large buffer.  If we didn't
    // make any progress, something is wrong---probably an unbalanced
    // quote.  We need to report the error, rebuild the parser state,
    // and move to a file offset that is likely beyond where the
    // problem occurred.
    //
    if (_numRecords == savedRecords && _numFields == savedFields) {
        LOG_WARNING("No progress after parsing " << nparse
                    << " bytes, unbalanced quotes in CSV input?");
        SCIDB_ASSERT(_fields.empty());
        SCIDB_ASSERT(_lastField.filepos > -1);
        _wantReset = true;
        throw USER_EXCEPTION(SCIDB_SE_IO, SCIDB_LE_CSV_UNBALANCED_QUOTE)
            << _numRecords;
    }

    // Not at EOF, so if we had a good parse and made progress, we
    // should have some fields here.
    SCIDB_ASSERT(!_fields.empty());

    // If we had to reparse a buffer after a reset, we can now safely
    // declare that we are finished with that buffer load.
    _reparse = NULL;

    return 0;
}

void CsvParser::fieldCbk(void* s, size_t n, void* state)
{
    assert(s);
    CsvParser* self = static_cast<CsvParser*>(state);
    self->putField(s, n);
}

void CsvParser::putField(void* s, size_t n)
{
    _numFields++;
    if ((_datalen + n + 1) >= _data.size()) {
        _data.resize(_data.size() + ::getpagesize());
    }
    _fields.push_back(Field(safe_static_cast<int>(_datalen),
                            _numRecords, _numFields, _bufOffset + _datalen));

    // Un-escape quotes and backslashes on the way in, or we'll never get rid of
    // \\\\\'em.  Libcsv turns "foo""bar" into foo"bar but does not
    // assist with foo\"bar.  @see Value::Formatter::quoteCstr
    const char* src = reinterpret_cast<const char*>(s);
    const char* end = src + n;
    char *dst = &_data[_datalen];
    size_t dlen = 0;
    for (size_t i = 0; i < n; ++i) {
        assert(*src);
        char ch = *src++;
        if (ch == '\\' && (src < end)) {
            if (*src == _quote) {
                *dst++ = _quote;
                ++src, ++i;
            } else if (*src == '\\') {
                *dst++ = '\\';
                ++src, ++i;
            } else {
                // Some backslash not injected by quoteCstr.
                *dst++ = '\\';
            }
        } else {
            *dst++ = ch;
        }
        ++dlen;
    }

    _datalen += dlen;
    _data[_datalen++] = '\0';
}

void CsvParser::recordCbk(int endChar, void* state)
{
    CsvParser* self = static_cast<CsvParser*>(state);
    self->putRecord(endChar);
}

void CsvParser::putRecord(int endChar)
{
    _numRecords++;

    if (endChar == -1) {
        // We are being called from csv_fini().
        LOG_WARNING("Last record (number " << _numRecords
                    << ") is missing a newline.");
    }

    _fields.push_back(Field(END_OF_RECORD, _numRecords, _numFields, _bufOffset + _datalen));
    size_t fieldCount = _numFields;
    _numFields = 0;

    // Changing field count is worth a warning... but not too many.
    if (_prevFields == SIZE_MAX) {
        _prevFields = fieldCount;
    } else if (_warnings < MAX_WARNINGS) {
        if (fieldCount != _prevFields) {
            LOG_WARNING("Field count changed from " << _prevFields << " to "
                        << fieldCount << " at input record " << _numRecords
                        << (++_warnings == MAX_WARNINGS ? " (Done complaining about this!)" : ""));
            _prevFields = fieldCount;
        }
    }
}

/**
 * Tell the libcsv parser what constitutes a space character.
 *
 * @description The parser ordinarily removes spaces and tabs from the
 * beginning and end of unquoted fields.  Apparently this is
 * undesirable, so we supply this callback to indicate that unquoted
 * fields should be left as is.
 *
 * @see Ticket #4353.
 */
int CsvParser::spaceFunc(unsigned char ch)
{
    return 0;
}

off_t CsvParser::getFileOffset() const
{
    // Sum of stuff prior to current parse plus latest field offset.
    // Because the fields are returned with trailing nulls, this will
    // include the approximate number of field delimiters.

    return _lastField.filepos;
}

string CsvParser::getLastField() const
{
    switch (_lastField.offset) {
    case START_OF_FILE:
        return "(unknown)";
    case END_OF_RECORD:
        return "(end-of-record)";
    case END_OF_FILE:
        return "(end-of-file)";
    default:
        SCIDB_ASSERT(_lastField.offset >= 0);
        return &_data[_lastField.offset];
    }
    SCIDB_UNREACHABLE();
}

} // namespace
