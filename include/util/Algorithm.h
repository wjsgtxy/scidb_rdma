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

#ifndef UTIL_ALGORITHM_H_
#define UTIL_ALGORITHM_H_

/****************************************************************************/
namespace scidb {
/****************************************************************************/
/**
 * A 3-sequence version of the std::transform() algorithm.
 */
template<class I,class J,class K,class out,class function>
out transform(I i,I e,J j,K k,out o,function f)
{
    for (; i!=e; ++i,++j,++k,++o)
    {
        *o = f(*i,*j,*k);
    }

    return o;
}

/**
 * A 4-sequence version of the std::transform() algorithm.
 */
template<class I,class J,class K,class L,class out,class function>
out transform(I i,I e,J j,K k,L l,out o,function f)
{
    for ( ; i!=e; ++i,++j,++k,++l,++o)
    {
        *o = f(*i,*j,*k,*l);
    }

    return o;
}

/****************************************************************************/
}
/****************************************************************************/
#endif
/****************************************************************************/
