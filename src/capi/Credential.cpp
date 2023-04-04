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
 * @file Credential.cpp
 * @brief Username/password pair and related methods.
 */

#include <rbac/Credential.h>

#include <system/UserException.h>
#include <util/CryptoUtils.h>

#include <boost/filesystem.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/ini_parser.hpp>
#include <lib_json/json.h>
#include <log4cxx/logger.h>

#include <fstream>

using namespace std;
namespace bfs = boost::filesystem;
namespace bpt = boost::property_tree;

namespace {

log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.capi.Credential"));

}

namespace scidb
{

Credential::Credential(std::string const& u)
    : _username(u)
{ }


Credential::Credential(std::string const& u, std::string const& p)
    : _username(u)
    , _password(p)
{ }


void Credential::setUsername(std::string const& u)
{
    _username = u;
}


void Credential::setPassword(std::string const& p)
{
    _password = p;
}


// Overwrite password before its memory is released to the heap.
Credential::~Credential()
{
    for (auto& ch : _password) {
        ch = '\0';
    }
}


bool Credential::isValidUsername(string const& u)
{
    if (u.empty() || u.size() > LOGIN_NAME_MAX || !::isalpha(u[0])) {
        return false;
    }
    bool punctuation = false;
    static const std::string LEGAL_PUNCT("_-@.");
    for (unsigned char const& ch : u) {
        punctuation = ::ispunct(ch);
        if (ch > 0x7F || (!::isalnum(ch) && LEGAL_PUNCT.find(ch) == std::string::npos)) {
            return false;
        }
    }
    return !punctuation;
}


bool Credential::isValidPassword(string const& p)
{
    if (p.empty()) {
        // Presumably a real password will be supplied later.
        return true;
    }
    if (p.size() > LOGIN_NAME_MAX) {
        return false;
    }
    for (unsigned char const& ch : p) {
        if (::iscntrl(ch) || ch > 0x7F) {
            return false;
        }
    }
    return true;
}


namespace {

bool fromJson(string const& fileName, string& user, string& pass)
{
    ifstream input(fileName);
    if (!input.is_open()) {
        // Won't be able to open it in fromIni() either.
        throw USER_EXCEPTION(SCIDB_SE_AUTHENTICATION, SCIDB_LE_CANT_OPEN_FILE)
            << fileName << ::strerror(errno) << errno;
    }

    Json::Value root;
    Json::Reader reader;
    bool ok = reader.parse(input, root);
    if (!ok) {
        // I'd like to throw, but we have to continue on to try
        // parsing it in INI format.
        LOG4CXX_DEBUG(logger, "Cannot parse '" << fileName << "' as JSON: "
                      << reader.getFormatedErrorMessages());
        return false;
    }

    user = root["user-name"].asString();
    pass = root["user-password"].asString();

    // Parsed it, but a missing user-name is fatal.  Fail with the
    // same exception that fromIni() would fail with.
    if (user.empty()) {
        throw USER_EXCEPTION(SCIDB_SE_AUTHENTICATION, SCIDB_LE_CANT_OPEN_FILE)
            << fileName << "Bad or missing user-name" << user;
    }

    return true;
}


void fromIni(string const& fileName, string& user, string& pass)
{
    try {
        bpt::ptree pt;
        bpt::ini_parser::read_ini(fileName.c_str(), pt);
        user = pt.get<std::string>("security_password.user-name"); // throw if missing
        pass = pt.get<std::string>("security_password.user-password", ""); // ok if missing
    }
    catch (bpt::ptree_error const& e) {
        throw USER_EXCEPTION(SCIDB_SE_AUTHENTICATION, SCIDB_LE_CANT_OPEN_FILE)
            << fileName << "Error on read or INI parse" << e.what();
    }
}

} // anonymous namespace


void Credential::fromAuthFile(const std::string &fileName)
{
    // Mode check.  Since we don't allow group or world permissions,
    // there's no need to check the euid: if it ain't us, we won't be
    // able to read it below.
    //
    boost::system::error_code ec;
    bfs::path fname(fileName);
    bfs::file_status fs = bfs::status(fname, ec);
    if (ec) {
        throw USER_EXCEPTION(SCIDB_SE_AUTHENTICATION, SCIDB_LE_CANT_STAT_FILE)
            << fileName << ec.message() << ec.value();
    }
    if (fs.type() != bfs::regular_file
        || (fs.permissions() & 0077)
        || ((fs.permissions() & 0400) == 0)
        || bfs::is_symlink(fname))
    {
        throw USER_EXCEPTION(SCIDB_SE_AUTHENTICATION, SCIDB_LE_CONFIGURATION_FILE_MODE)
            << fileName;
    }

    // Parse it, preferring JSON.
    if (!fromJson(fileName, _username, _password)) {
        // This one always throws on error.
        fromIni(fileName, _username, _password);
    }

    // Successful parse, but is it any good?
    if (!isValid()) {
        throw USER_EXCEPTION(SCIDB_SE_AUTHENTICATION, SCIDB_LE_BAD_CREDENTIAL);
    }
}

} // scidb
