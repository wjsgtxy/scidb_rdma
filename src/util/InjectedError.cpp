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
 * @file InjectedError.cpp
 *
 * @brief Implementaton of the error injection mechanism
 */

#include <memory>

#include <util/InjectedError.h>
#include <util/InjectedErrorCodes.h>
#include <system/Exceptions.h>


namespace scidb
{
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.util.injecterror"));

InjectedError::InjectedError(InjectErrCode ID)
:
    _ID(ID)
{
}

InjectedError::~InjectedError() {}

InjectedErrorLibrary::~InjectedErrorLibrary()
{
}

InjectErrCode InjectedError::getID() const
{
    return _ID;
}

void InjectedError::inject() const
{
    std::shared_ptr<const InjectedError> err(shared_from_this());
    Notification<InjectedError> notification(err);
    notification.publish();
}



/*
 * NOTE: no lock, because it is called from a static initializer
 *       if ever called from ordinary code, then turn the library into a singleton
 */
bool InjectedErrorLibrary::registerError(InjectErrCode id, const std::shared_ptr<const InjectedError>& err)
{
    return _registeredErrors.insert(std::make_pair(id, err)).second;
    return false;
}

std::shared_ptr<const InjectedError> InjectedErrorLibrary::getError(InjectErrCode id)
{
    ScopedMutexLock lock(_mutex, PTW_SML_INJECTED_ERROR_LISTENER);
    IdToErrorMap::const_iterator iter = _registeredErrors.find(id);
    if (iter == _registeredErrors.end()) {
        return std::shared_ptr<InjectedError>();
    }
    return iter->second;
}

InjectedErrorLibrary::InjectedErrorLibrary()
{
    for(InjectErrCode code = InjectErrCode::FIRST_USEABLE;  code < InjectErrCode::NUM_CODES;
        code=static_cast<InjectErrCode>(static_cast<long>(code)+1)) {

        bool rc = registerError(code, std::make_shared<InjectedError>(code));
        SCIDB_ASSERT(rc);
    }
}

InjectedErrorLibrary InjectedErrorLibrary::_injectedErrorLib;


InjectedErrorListener::InjectedErrorListener(InjectErrCode errID)
:
    _subscriberID(0),
    _errID(errID)
{}

void InjectedErrorListener::start()
{
    ScopedMutexLock lock(_mutex, PTW_SML_INJECTED_ERROR_LISTENER);
    if (_subscriberID) {
        return;
    }
    typename Notification<InjectedError>::Subscriber listener =
        std::bind(&InjectedErrorListener::handle,
                  this,
                  std::placeholders::_1);
    _subscriberID = Notification<InjectedError>::subscribe(listener);
}

bool InjectedErrorListener::test(int long line, const char* file)
{
    ScopedMutexLock lock(_mutex, PTW_SML_INJECTED_ERROR_LISTENER);
    start();                // checking re-enters the same lock, so overhead of starting during test
                            // is acceptable if they were different, we'd want a pre-check on
                            // _subscriberID outside the callee's locked scope

    if (!_err) {
        return false;
    }

    typename Notification<InjectedError>::MessageTypePtr err = _err;
    _err.reset(); // so that an injected error will only test true once
    LOG4CXX_WARN(logger, "InjectedErrorListener::check: activating ID" << int64_t(err->getID())
                         << " at line " << line << " file " << file);
    return true;
}

void InjectedErrorListener::throwif(int long line, const char* file)
{
    if (test(line, file)) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INJECTED_ERROR, SCIDB_LE_INJECTED_ERROR);
    }
}

void InjectedErrorListener::stop()
{
    ScopedMutexLock lock(_mutex, PTW_SML_INJECTED_ERROR_LISTENER);
    Notification<InjectedError>::unsubscribe(_subscriberID);
}

void InjectedErrorListener::handle(typename Notification<InjectedError>::MessageTypePtr err)
{
    if (err && err->getID() == _errID) {
        ScopedMutexLock lock(_mutex, PTW_SML_INJECTED_ERROR_LISTENER);
        _err = err;
    }
}

InjectedErrorListener::~InjectedErrorListener()
{}

} //namespace scidb
