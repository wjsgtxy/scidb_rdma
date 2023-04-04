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
 * @file InstanceAuthenticator.cpp
 * @author Mike Leibensperger <mjl@paradigm4.com>
 */

#include "InstanceAuthenticator.h"
#include <system/Cluster.h>

namespace scidb {

Authenticator::Challenge
InstanceAuthenticator::getChallenge()
{
    Challenge ch;
    ch.code = 0;
    ch.text = "Something meaningful coming here soon!";
    return ch;
}

Authenticator::Status
InstanceAuthenticator::putResponse(std::string const& response)
{
    // Is their cluster UUID same as ours?
    return response == Cluster::getInstance()->getUuid()
        ? ALLOW : DENY;
}

std::string
InstanceAuthenticator::getResponse(std::string const& challenge)
{
    // For now we disregard the text of the challenge and trivially
    // reply with the cluster UUID.

    return Cluster::getInstance()->getUuid();
}

} // namespace
