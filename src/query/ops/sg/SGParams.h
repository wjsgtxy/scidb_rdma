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
 * @file SGParam.h
 */

#ifndef SG_PARAM_H_
#define SG_PARAM_H_

namespace scidb
{

enum SGParamIndex {SGPARAM_DISTRIBUTION=0, SGPARAM_INSTANCE_ID, SGPARAM_IS_STRICT, SGPARAM_IS_PULL, NUM_SGPARAM};

} // namespace

#endif /* SG_PARAM_H_ */
