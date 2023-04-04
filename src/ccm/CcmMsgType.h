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
#ifndef CCM_MSG_TYPE_H_
#define CCM_MSG_TYPE_H_

// c++ standard libraries
#include <cstdint>
#include <iosfwd>

namespace scidb { namespace ccm {

/**
 *
 * @ingroup Ccm
 */
enum class CcmMsgType
{
// clang-format off
#   define X(_name, _code, _desc)    _name = _code ,
#   include "CcmMsgTypes.inc"
#   undef X
    // clang-format on
};

std::ostream& operator<<(std::ostream& os, const scidb::ccm::CcmMsgType& mtype);

}}  // namespace scidb::ccm
#endif
