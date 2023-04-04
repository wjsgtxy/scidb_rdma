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
 * @file IoFormats.h
 * @author Mike Leibensperger <mjl@paradigm4.com>
 *
 * @brief All about supported I/O formats and their properties.
 */

#ifndef IO_FORMATS_H
#define IO_FORMATS_H

#include <string>

namespace scidb { namespace iofmt {

/// Sad but true: this poorly specified, homegrown format is the default.
static constexpr char const * const DEFAULT_IO_FORMAT = "text";

/**
 * @brief Split a format name "foo:bar" into base "foo" and options "bar".
 *
 * @param[in]  format   the format name
 * @param[out] base     the canonical base format name (that is, lowercase)
 * @param[out] options  the string of single-character format options if any
 *
 * @details There may be no options suffix, in which case @c options
 * is cleared.
 */
void split(std::string const& format, std::string& base, std::string& options);

/**
 * @brief Is the named I/O format supported for output?
 *
 * @description If the format is supported, return a pointer
 * to its canonical name (i.e. its lowercase name), otherwise
 * NULL.  This routine only knows about concrete formats, not
 * about things like "auto" or empty string picking a default
 * format.
 *
 * @par
 * Template formats are recognized but have no canonical name,
 * so fmt.c_str() is returned.  They beginning with '(' and
 * can be associated with a custom plugin, e.g. "(myformat)".
 *
 * @see scidb::TemplateParser
 *
 * @param fmt the name of the format
 * @retval nullptr this format is not supported
 * @retval !nullptr format is supported, and this is its canonical name
 */
const char* isOutputFormat(const std::string& fmt);

/**
 * @brief Is the named I/O format supported for input?
 *
 * @description Just like #isOutputFormat but for input formats.
 * @see isOutputFormat
 */
const char* isInputFormat(const std::string& fmt);

/** Return true iff the format contains embedded cell coordinates. */
bool hasCoordinates(std::string const& fmt);

/** Return true iff this format encodes overlap regions. */
bool canWriteOverlaps(std::string const& fmt);

/** Return true iff this format decodes overlap regions. */
bool canReadOverlaps(std::string const& fmt);

} } // namespaces

#endif
