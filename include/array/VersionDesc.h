#ifndef VERSIONDESC_H_
#define VERSIONDESC_H_
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
#include <array/ArrayID.h>
#include <array/VersionID.h>

namespace scidb {

class VersionDesc
{
  public:
    VersionDesc(ArrayID a = 0,VersionID v = 0,time_t t = 0)
        : _arrayId(a),
          _versionId(v),
          _timestamp(t)
    {}

    ArrayID getArrayID() const
    {
        return _arrayId;
    }

    VersionID getVersionID() const
    {
        return _versionId;
    }

    time_t getTimeStamp() const
    {
        return _timestamp;
    }

  private:
    ArrayID   _arrayId;
    VersionID _versionId;
    time_t    _timestamp;
};

}  // namespace scidb
#endif
