/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2015-2019 SciDB, Inc.
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

/*
 * GetArrays.cpp
 *
 *  Created on: Jun 14, 2016
 *      Author: mcorbett@paradigm4.com
 */
#include "FilterArrays.h"

#include <array/ArrayDesc.h>

#include <boost/regex.hpp>

namespace scidb
{
    void filterArrays(
        std::vector<scidb::ArrayDesc> & arrays,
        const std::string &             regExNamespace, /*  = "" */
        const std::string &             regExArray /*  = "" */)
    {
        // get rid of any arrayDesc whose name does not conform to the regular expression
        arrays.erase(
            std::remove_if(
                arrays.begin(), arrays.end(),
                [&regExNamespace, &regExArray](const ArrayDesc&arrayDesc)
                {
                    int matches = 0;

                    if(regExNamespace.empty())
                    {
                        matches++;
                    } else {
                        static boost::regex  reNamespace(regExNamespace);
                        boost::cmatch what;

                        const std::string &arrayName = arrayDesc.getNamespaceName();
                        if(boost::regex_match(arrayName.c_str(), what, reNamespace))
                        {
                            matches++;
                        }
                    }

                    if(regExArray.empty())
                    {
                        matches++;
                    } else {
                        static boost::regex  reArray(regExArray);
                        boost::cmatch what;

                        const std::string &arrayName = arrayDesc.getName();
                        if(boost::regex_match(arrayName.c_str(), what, reArray))
                        {
                            matches++;
                        }
                    }

                    return !(matches == 2);
                }),
                arrays.end());
    }
} //namespace scidb
