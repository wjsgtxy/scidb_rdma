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
 * @file FilterArrays.h
 *
 *  Created on: June 14, 2016
 *      Author: mcorbett@paradigm4.com
 */

#ifndef FILTER_ARRAYS_H_
#define FILTER_ARRAYS_H_


#include <string>
#include <vector>


namespace scidb
{
class ArrayDesc;
    /**
     * Filters array schemas that conform to the given regex criteria.
     *
     * @param[out] arrayDescs vector of ArrayDesc objects
     * @param[in] regExNamespace Regular expression to be used to filter namespaces. Use an empty string to choose all.
     * @param[in] regExArray  Regular expression to be used to filter arrays. Use an empty string to choose all.
     * @throws scidb::SystemException on error
     *
     * The regular expressions are allowed to conform to "A Proposal to add Regular Expressions to
     * the Standard Library" located at the following location:
     *     http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2003/n1429.htm
     *
     * The follows are examples based on the regular expression settings:
     *   regExNamespace='', regExArray=''
     *     {i} namespace,array
     *     {0} 'ns1','a1'
     *     {1} 'ns1','mrc3'
     *     {2} 'ns1','mrc4'
     *     {3} 'ns2','a1'
     *     {4} 'ns2','mrc1'
     *     {5} 'ns2','mrc4'
     *     {6} 'public','a1'
     *     {7} 'public','mrc1'
     *     {8} 'public','mrc2'
     *
     *   regExNamespace='', regExArray='^mrc.*'
     *     {i} namespace,array
     *     {0} 'ns1','mrc3'
     *     {1} 'ns1','mrc4'
     *     {2} 'ns2','mrc1'
     *     {3} 'ns2','mrc4'
     *     {4} 'public','mrc1'
     *     {5} 'public','mrc2'
     *
     *   regExNamespace='^ns1.*', regExArray='^mrc.*'
     *     {i} namespace,array
     *     {0} 'ns1','mrc3'
     *     {1} 'ns1','mrc4'
     *
     *   regExNamespace='^ns1.*', regExArray=''
     *     {i} namespace,array
     *     {0} 'ns1','a1'
     *     {1} 'ns1','mrc3'
     *     {2} 'ns1','mrc4'
     */
    void filterArrays(
        std::vector<scidb::ArrayDesc> &     arrays,
        const std::string &                 regExNamespace = "",
        const std::string &                 regExArray = "");
} //namespace scidb
#endif // FILTER_ARRAYS_H_
