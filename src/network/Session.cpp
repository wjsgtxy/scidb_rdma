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

/**
 * @file Session.cpp
 */

#include <rbac/Session.h>

#include <system/SystemCatalog.h>

#include <log4cxx/logger.h>
#include <lib_json/json.h>
#include <sstream>

using namespace std;

namespace scidb {

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.services.network.auth"));

Session::Session(UserDesc const& userDesc, int sessPri, bool peer)
    : _currentNs(rbac::PUBLIC_NS_NAME)
    , _userDesc(userDesc)
    , _priority(sessPri)
    , _isPeer(peer)
{ }


Session::~Session()
{
    try {
        if (_onClose) {
            LOG4CXX_DEBUG(logger, "Running session cleanup for "
                          << _userDesc.getName() << " from destructor");
            _onClose();
        }
    }
    catch (std::exception const &ex) {
        LOG4CXX_WARN(logger, "Exception on closing Session for "
                     << _userDesc.getName() << ": " << ex.what());
    }
}


namespace {
// JSON object keys, for typo prevention and a wee bit of space savings.
char const * const K_USER = "u";
char const * const K_UID = "ui";
char const * const K_NS = "n";
char const * const K_NSID = "ni";
char const * const K_PRI = "pr";
char const * const K_PEER = "p";
char const * const K_RIGHTS = "r";
char const * const K_ENTITY = "et";
char const * const K_NAME = "nm";
char const * const K_PERMS = "pm";
}


std::string Session::toJson() const
{
    Json::Value root;
    root[K_USER] = _userDesc.getName();
    root[K_UID] = Json::Value::UInt(_userDesc.getId());
    root[K_NS] = _currentNs.getName();
    root[K_NSID] = Json::Value::UInt(_currentNs.getId());
    root[K_PRI] = Json::Value::Int(_priority);
    root[K_PEER] = Json::Value::UInt(_isPeer);

    // There's never an _onClose callback sent to workers.  Those are
    // for things like PAM cleanup, which only needs to happen on the
    // instance terminating the client connection (that is, the query
    // coordinator).

    if (_rights) {
        int index = 0;
        Json::Value rights;
        for (auto const& r : *_rights) {
            Json::Value right;
            right[K_ENTITY] = r.first.type;
            right[K_NAME] = r.first.name;
            right[K_PERMS] = r.second.str();
            rights[index] = right;
            ++index;
        }
        root[K_RIGHTS] = rights;
    } else {
        // An empty array.
        root[K_RIGHTS] = Json::Value(Json::arrayValue);
    }

    Json::FastWriter writer;
    if (logger->isTraceEnabled()) {
        string json = writer.write(root);
        return json;
    }
    return writer.write(root);
}


std::shared_ptr<Session> Session::fromJson(string const& json)
{
    Json::Value root;
    Json::Reader reader;
    bool ok = reader.parse(json, root);
    ASSERT_EXCEPTION(ok, "Cannot parse Session JSON '" << json << "': "
                     << reader.getFormatedErrorMessages());

    // No default Session ctor, no problem... grab ctor parameters first.
    UserDesc ud(root[K_USER].asString(), root[K_UID].asUInt());
    int pri = static_cast<int>(root[K_PRI].asInt());
    bool peer = root[K_PEER].asBool();

    std::shared_ptr<Session> sess = make_shared<Session>(ud, pri, peer);
    sess->setNamespace(NamespaceDesc(root[K_NS].asString(),
                                     root[K_NSID].asUInt()));

    rbac::RightsMap rights;
    for (int index = 0, n = root[K_RIGHTS].size(); index < n; ++index) {
        Json::Value& right = root[K_RIGHTS][index];
        rights.upsert(rbac::EntityType(right[K_ENTITY].asUInt()),
                      right[K_NAME].asString(),
                      rbac::Permissions(right[K_PERMS].asString()));
    }
    sess->setRights(rights);

    return sess;
}

}  // namespace scidb
