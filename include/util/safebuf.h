/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2017-2019 SciDB, Inc.
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
 * @file safebuf.h
 * @brief Buffer class for holding sensitive information.
 *
 * @note This is probably a fruitless effort, because when you go to
 *       transmit the contents of a safebuf using protobufs, the
 *       protobufs runtime is going to create an std::string anyway,
 *       which means your sensitive information will be released into
 *       the heap anyway.  SAD!!!
 */

#ifndef SAFEBUF_H
#define SAFEBUF_H

#include <system/Utils.h>
#include <string>

namespace scidb {

/**
 * @brief Heap buffer class for holding sensitive information.
 *
 * @description Sensitive information (passwords, keys, crypto hashes,
 * etc.) should be overwritten as soon as possible after use.  This
 * class can assist with that if properly used.
 *
 * @note Be aware that many safebuf methods do not just copy their
 *       input, they destroy it.  This is by design.
 */
class safebuf
{
public:
    /// @brief Default constructor.
    safebuf();

    /// @brief Create safebuf of length n and fill with 'fill'.
    safebuf(size_t n, char fill);

    /// @brief Create n-byte safebuf, fill with data from p, AND WIPE INPUT.
    safebuf(void* p, size_t n, char fill = '\0');

    /// @brief Make copy of s in safebuf AND WIPE CONTENTS OF s !!!
    explicit safebuf(std::string& s);

    /// @brief Copy constructor.
    safebuf(safebuf const& s);

    /// @brief Copy assignment.
    safebuf& operator=(safebuf const& s);

    /// @brief Wipe and destroy safebuf.
    ~safebuf();

    /// @brief Fill with n bytes of data from p, AND WIPE INPUT.
    void grab(void* p, size_t n, char fill = '\0');

    /// @brief Fill with string data AND WIPE INPUT.
    /// @note Best not to use '\0' for fill, it might confuse std::string.
    void grab(std::string& s, char fill = '%');

    /// @brief Fill with string data AND WIPE INPUT.
    /// @note Best not to use '\0' for fill, it might confuse std::string.
    /// @note Intended for use with stack temporaries such as those
    ///       produced when inspecting pqxx results.
    void grab(std::string const& s, char fill = '%')
    {
        grab(const_cast<std::string&>(s), fill);
    }

    /// @brief Wipe contents of safebuf.
    void wipe();

    /// @brief Access data in the safebuf.
    /// @{
    void const* data() const { return _data; }
    size_t size() const { return _size; }
    char const* c_str() const;
    bool wiped() const { return _wiped; }
    bool empty() const { return _wiped || _size == 0UL; }
    /// @}

private:
    bool _wiped;
    size_t _size;
    size_t _capacity;
    char *_data;
};

inline bool operator==(safebuf const& lhs, safebuf const& rhs)
    { return 0 == ::strcmp(lhs.c_str(), rhs.c_str()); }

inline bool operator!=(safebuf const& lhs, safebuf const& rhs)
    { return ::strcmp(lhs.c_str(), rhs.c_str()); }

inline bool operator==(safebuf const& lhs, std::string const& rhs)
    { return 0 == ::strcmp(lhs.c_str(), rhs.c_str()); }

inline bool operator!=(safebuf const& lhs, std::string const& rhs)
    { return ::strcmp(lhs.c_str(), rhs.c_str()); }

inline bool operator==(std::string const& lhs, safebuf const& rhs)
    { return 0 == ::strcmp(lhs.c_str(), rhs.c_str()); }

inline bool operator!=(std::string const& lhs, safebuf const& rhs)
    { return ::strcmp(lhs.c_str(), rhs.c_str()); }

} // namespace

#endif /* ! SAFEBUF_H */
