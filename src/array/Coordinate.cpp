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
/****************************************************************************/

#include <array/Coordinate.h>                            // For Coordinate

#include <ostream>                                       // For ostream
#include <system/Utils.h>                                // For SCIDB_ASSERT

/****************************************************************************/
namespace scidb {
/****************************************************************************/

std::ostream& operator<<(std::ostream& os,CoordinateCRange point)
{
    os << '{';                                           // Emit the header
    insertRange(os,point,", ");                          // Emit the point
    return os << '}';                                    // Emit the trailer
}

void setCoordsMin ( Coordinates& coord ) {
  for ( size_t i = 0, len = coord.size(); i < len; ++i ) {
    coord[i]=CoordinateBounds::getMin();
  }
}

void setCoordsMax ( Coordinates& coord ) {
  for ( size_t i = 0, len = coord.size(); i < len; ++i ) {
    coord[i]=CoordinateBounds::getMax();
  }
}

void resetCoordMin ( Coordinates& coord, const Coordinates& other ) {
  SCIDB_ASSERT( coord.size() == other.size() );
  for (size_t i = 0, n = coord.size(); i < n; ++i) {
    if ( coord[i] > other[i] ) {
      coord[i] = other[i];
    }
  }
}

void resetCoordMax ( Coordinates& coord, const Coordinates& other ) {
  SCIDB_ASSERT( coord.size() == other.size() );
  for (size_t i = 0, n = coord.size(); i < n; ++i) {
    if ( coord[i] < other[i] ) {
      coord[i] = other[i];
    }
  }
}

}
/****************************************************************************/
