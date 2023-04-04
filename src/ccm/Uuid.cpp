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

// The header file for the implementation details in this file
#include "Uuid.h"
// SciDB modules
#include <system/Utils.h>  // For SCIDB_ASSERT
// boost libraries, not yet implemented as a c++ standard
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
// c++ standard libraries
#include <chrono>
#include <iostream>

namespace {
auto seed = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count();
boost::mt19937 mt_rand(static_cast<unsigned int>(seed));
//boost::uuids::random_generator _rng(mt_rand); // dz 1.72报错，替换一下
boost::uuids::random_generator_pure _rng;

boost::uuids::nil_generator _nilgen;
}  // namespace

namespace scidb { namespace ccm {

Uuid Uuid::create()
{
    Uuid u;
    u._id = _rng();
    return u;
}

Uuid::Uuid()
    : _id(_nilgen())
{}

Uuid::Uuid(const uint8_t* mem)
{
    SCIDB_ASSERT(mem);
    std::memcpy(&_id, mem, 16);
}

Uuid::operator std::string() const
{
    return boost::uuids::to_string(_id);
}

std::size_t Uuid::hash() const
{
    return boost::uuids::hash_value(_id);
}

std::ostream& operator<<(std::ostream& os, const Uuid& id)
{
    os << id._id;
    return os;
}

}}  // namespace scidb::ccm
