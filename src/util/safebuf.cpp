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
 * @file safebuf.cpp
 * @brief Buffer class for holding sensitive information.
 *
 * @description The idea is to ensure that sensitive data is
 * overwritten (and not just freed onto the heap) after use.  This may
 * be a quixotic goal since the protobuf runtime is going to copy it
 * back into an ordinary std::string anyway.  Hmmm.
 *
 * @note The std::string-clobbering behavior here is apparently OK to
 * do in C++11 (or at least more OK than it used to be).
 * @see https://stackoverflow.com/questions/38702943/how-to-cleanse-overwrite-with-random-bytes-stdstring-internal-buffer#38703454
 */

#include <util/safebuf.h>
#include <string.h>

namespace scidb {

safebuf::safebuf()
    : _wiped(false)
    , _size(0)
    , _capacity(0)
    , _data(nullptr)
{ }

safebuf::safebuf(size_t n, char fill)
    : _wiped(false)
    , _size(n)
    , _capacity(n)
    , _data(nullptr)
{
    if (n) {
        _data = new char[_capacity];
        ::memset(_data, fill, _capacity);
    }
}

// WARNING: Contents of n bytes of memory at p will be overwritten.
safebuf::safebuf(void *p, size_t n, char fill)
    : _wiped(false)
    , _size(n)
    , _capacity(n)
    , _data(nullptr)
{
    if (p && n) {
        _data = new char[_capacity];
        ::memcpy(_data, p, _size);
        ::memset(p, fill, _size);
    } else {
        _size = _capacity = 0;
    }
}

// WARNING: Contents of 's' will be overwritten.
safebuf::safebuf(std::string& s)
    : _wiped(false)
    , _size(s.size() + 1)
    , _capacity(_size)
    , _data(new char[_capacity])
{
    char *p = const_cast<char*>(s.c_str());
    ::memcpy(_data, p, _size);
    ::memset(p, '%', _size);
    // *Must* overwrite... should we *also* s.clear()?  Debatable.
}

safebuf::safebuf(safebuf const& s)
    : _wiped(s._wiped)
    , _size(s._size)
    , _capacity(s._size)
    , _data(nullptr)
{
    if (s._data) {
        _data = new char[_capacity];
        ::memcpy(_data, s._data, _size);
    }
}

safebuf& safebuf::operator= (safebuf const& other)
{
    if (this != &other) {
        if (other._wiped) {
            wipe();
        } else {
            _wiped = false;
            _size = other._size;
            if (other._size > _capacity) {
                delete [] _data;
                _capacity = other._size;
                _data = new char[_capacity];
            }
            ::memcpy(_data, other.data(), _size);
        }
    }
    return *this;
}

safebuf::~safebuf()
{
    wipe();
    delete [] _data;
}

// WARNING: Contents of n bytes of memory at p will be overwritten.
void safebuf::grab(void* p, size_t n, char fill)
{
    wipe();
    if (!p || !n) {
        return;
    }
    if (n > _capacity) {
        delete [] _data;
        _data = new char[n];
        _capacity = n;
        _size = n;
    } else {
        _size = n;
    }
    _wiped = false;
    ::memcpy(_data, p, _size);
    ::memset(p, fill, _size);
}

// WARNING: Contents of 's' will be overwritten.  Out of paranoia we
// preserve the length of 's', but fill it with non-NUL characters.
void safebuf::grab(std::string& s, char fill)
{
    size_t n = s.size() + 1;   // +1 for NUL
    char *p = const_cast<char*>(s.c_str());
    grab(p, n, fill);
    p[n-1] = '\0';               // replace overwritten end-of-string
}

void safebuf::wipe()
{
    if (!_wiped) {
        if (_data) {
            ::memset(_data, 0, _capacity);
        }
        _wiped = true;
    }
}

namespace {
    char const* the_one_true_empty_string = "";
}

char const* safebuf::c_str() const
{
    if (_wiped || _size == 0) {
        // Like std::string::c_str(), never return nullptr.
        return the_one_true_empty_string;
    }

    SCIDB_ASSERT(_data[_size-1] == '\0'); // must really be a c_str
    return _data;
}

} // namespace
