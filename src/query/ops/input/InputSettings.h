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
 * @file InputSettings.h
 * @author Mike Leibensperger <mjl@paradigm4.com>
 *
 * @brief Encapsulated parameter parsing for the input() operator.
 *
 * @description Take a page from Alex's playbook and isolate all parameter parsing in one place.
 * For input(), the parsing is especially painful.  We certainly don't want to have to re-parse all
 * over again once we get to PhysicalInput!
 */

#ifndef INPUT_SETTINGS_H
#define INPUT_SETTINGS_H

#include <ostream>

#include <util/IoFormats.h>
#include <query/OperatorParam.h>

namespace scidb {

class ArrayDesc;

/**
 * @brief Encapsulated parameter parsing for the input operator.
 *
 * @description
 * Look at the positional and keyword parameters, and figure it all out.  Then use simple accessors
 * to get the desired values.  Allow these objects to be stringified and unstringified, so they can
 * be passed as an Inspectable to PhysicalInput.
 *
 * @note We don't bother with positional parameter 0 (the schema) since that has no keyword
 *       equivalents and dealing with it those has always been straightforward.  We do handle
 *       positional parameter 1 (the file path) because it may have to be "expanded".
 */
class InputSettings
{
public:
    /// One True Definition of keyword parameter names, to guard against typos.
    /// @{
    static const char* const KW_PATH;
    static const char* const KW_INSTANCE;
    static const char* const KW_FORMAT;
    static const char* const KW_MAX_ERRORS;
    static const char* const KW_STRICT;
    static const char* const KW_CHUNK_HINT;
    /// @}

    /// Constructor
    InputSettings() = default;

    /// Parse the parameters into easily accessible values.
    void parse(Parameters const& parms, KeywordParameters const& kwParams, Query const& query);

    /// Marshalling and unmarshalling allows parsed values to be transmitted to PhysicalInput.
    /// @{
    void fromString(std::string const& s);
    std::string toString() const;
    /// @}

    /// True iff we have successfully parsed the parameters.
    bool isValid() const { return _valid; }

    /// True iff parallel load.
    bool isParallel() const { assert(_valid); return _instanceId == ALL_INSTANCE_MASK; }

    /// Accessors for parsed parameters.
    /// @{
    std::string const&  getPath() const         { assert(_valid); return _path; }
    InstanceID          getInstanceId() const   { assert(_valid); return _instanceId; }
    std::string const&  getFormat() const       { assert(_valid); return _format; }
    int64_t             getMaxErrors() const    { assert(_valid); return _maxErrors; }
    bool                getIsStrict() const     { assert(_valid); return _isStrict; }
    bool                dataframeMode() const   { assert(_valid); return _isDfMode; }
    Coordinates const&  getChunkHint() const    { assert(_valid); return _chunkHint; }
    /// @}

private:
    bool _valid { false };
    InstanceID _instanceId { COORDINATOR_INSTANCE_MASK };
    bool _isStrict { true };
    bool _isDfMode { false };
    int64_t _maxErrors { 0 };
    std::string _format { iofmt::DEFAULT_IO_FORMAT };
    std::string _path;
    Coordinates _chunkHint;
};

std::ostream& operator<< (std::ostream&, InputSettings const&);

} // namespace scidb

#endif // ! INPUT_SETTINGS_H
