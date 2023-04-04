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
 * @file IoFormats.cpp
 * @author Mike Leibensperger <mjl@paradigm4.com>
 *
 * @brief All about supported I/O formats and their properties.
 */

#include <util/IoFormats.h>

#include <system/UserException.h>

#include <algorithm>

using namespace std;

namespace {

enum FormatFlags {
    COORDS  = 0x01,             // Has embedded coordinates
    RDOVLP  = 0x02,             // Can read overlap regions
    WROVLP  = 0x04,             // Can write overlap regions
    OUTPUT  = 0x08,             // Valid format for output
    INPUT   = 0x10,             // Valid format for input
    IN_OUT  = 0x18,             // Valid for both input and output
};

// Although formats csv+ and tsv+ do contain coordinates, they are not
// marked COORDS.  The reason is that no input format recognizes those
// coordinates: they look like ordinary csv and tsv on the way in, and
// you need to *manually* redimension() to turn coordinate attributes
// back into dimension coordinates.  The coordinates in csv+ and tsv+
// can't be *automatically* recovered on input().  The COORDS flag
// drives an SG decision in input(), so this matters.
//
// Formats marked "deprecated" are no longer fully supported, although
// we use them in many tests so they will probably linger.  The only
// fully supported COORDS format is "opaque".

struct FormatInfo {
    const char* cname;      // Canonical base name
    unsigned flags;         // Mask of FormatFlags
} formatTable[] = {
    {"(binary)",    IN_OUT},    // Dummy entry for '(...)' binary template
    {"csv",         IN_OUT},
    {"csv+",        OUTPUT},
    {"dcsv",        OUTPUT},
    {"dense",       IN_OUT | COORDS},                   // deprecated
    {"lsparse",     IN_OUT | COORDS},                   // deprecated
    {"opaque",      IN_OUT | COORDS | RDOVLP | WROVLP},
    {"sparse",      IN_OUT | COORDS},                   // deprecated
    {"store",       IN_OUT | COORDS | RDOVLP | WROVLP}, // deprecated
    {"text",        IN_OUT | COORDS | RDOVLP},          // deprecated
    {"tsv",         IN_OUT},
    {"tsv+",        OUTPUT},
};
const unsigned NUM_FORMATS = sizeof(formatTable) / sizeof(formatTable[0]);

FormatInfo const* findFormatInfo(std::string const& fmt)
{
    string format(fmt);
    if (format.empty()) {
        format = scidb::iofmt::DEFAULT_IO_FORMAT;
    }

    string baseFormat, options;
    scidb::iofmt::split(format, baseFormat, options);

    if (baseFormat[0] == '(') {
        // Binary template format, search for the dummy entry.
        baseFormat = "(binary)";
    }

    // Scan format info table for a match.
    for (unsigned i = 0; i < NUM_FORMATS; ++i) {
        if (0 == ::strcasecmp(baseFormat.c_str(), formatTable[i].cname)) {
            return &formatTable[i];
        }
    }
    return nullptr;
}

bool hasFlags(std::string const& format, unsigned flags)
{
    // Need these in anonymous namespace to avoid cpp macro expansion ugliness.
    using scidb::SCIDB_SE_IO;
    using scidb::SCIDB_LE_UNSUPPORTED_FORMAT;

    FormatInfo const* fi = findFormatInfo(format);
    if (fi == nullptr) {
        throw USER_EXCEPTION(SCIDB_SE_IO, SCIDB_LE_UNSUPPORTED_FORMAT)
            << format;
    }
    return (fi->flags & flags) == flags;
}

const char* isSupported(const string& format, unsigned flags)
{
    FormatInfo const* fi = findFormatInfo(format);
    if (fi) {
        if (flags && (fi->flags & flags) != flags) {
            return nullptr;
        }
        if (format[0] == '(') {
            // Binary template format is its own canonical name.
            return format.c_str();
        } else {
            return fi->cname;
        }
    } else {
        return nullptr;
    }
}

} // end anonymous namespace


namespace scidb { namespace iofmt {

void split(string const& format, string& base, string& options)
{
    string::size_type colon = format.find(':');
    base = format.substr(0, colon);
    if (colon == string::npos) {
        options.clear();
    } else {
        options = format.substr(colon + 1);
    }
    if (!base.empty() && base[0] != '(') {
        // Canonicalize the base.  No UTF-8 worries, these strings are
        // ASCII-only, tolower(3) is OK.  Hands off the binary
        // template formats (that begin with '(').
        std::transform(base.begin(), base.end(), base.begin(), ::tolower);
    }
}

const char* isOutputFormat(const string& format)
{
    return isSupported(format, OUTPUT);
}

const char* isInputFormat(const string& format)
{
    return isSupported(format, INPUT);
}

bool hasCoordinates(string const& format)
{
    return hasFlags(format, COORDS);
}

bool canWriteOverlaps(string const& format)
{
    return hasFlags(format, WROVLP);
}

bool canReadOverlaps(string const& format)
{
    return hasFlags(format, RDOVLP);
}

} } // end scidb::iofmt namespace
