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
 * @file InjectedError.h
 *
 * @brief
 *
 */

#ifndef INJECTEDERROR_H_
#define INJECTEDERROR_H_

#include <map>
#include <memory>
#include <util/InjectedErrorCodes.h>
#include <util/Mutex.h>
#include <util/Notification.h>

namespace scidb
{

enum class InjectErrCode : long;   // opaque, see <InjectedErrorCodes.h> for definitions

/**
 * @class InjectedError - a base class for all injected errors
 *
 */

class InjectedError : public std::enable_shared_from_this<InjectedError>
{
 public:
    InjectedError(InjectErrCode ID);
    virtual ~InjectedError();

    virtual InjectErrCode getID() const;
    virtual void inject() const;

 private:
    InjectErrCode _ID;

    InjectedError(const InjectedError&) = delete;
    InjectedError& operator=(const InjectedError&) = delete;
};


/**
 * @class InjectedErrorLibrary - a library of all injected error identified by their IDs
 *
 */
class InjectedErrorLibrary
{
 public:
    InjectedErrorLibrary();
    virtual ~InjectedErrorLibrary();
    std::shared_ptr<const InjectedError> getError(InjectErrCode id);
    static InjectedErrorLibrary* getLibrary()
    {
        return &_injectedErrorLib;
    }

 private:
    bool registerError(InjectErrCode id, const std::shared_ptr<const InjectedError>& err);

    typedef std::map<InjectErrCode, std::shared_ptr<const InjectedError> > IdToErrorMap;
    IdToErrorMap _registeredErrors;
    Mutex _mutex;

    static InjectedErrorLibrary _injectedErrorLib;
 };

/**
 * @class InjectedErrorListener - a mixin class to receive and act on the injected errors
 *
 */
class InjectedErrorListener
{
 public:
    InjectedErrorListener(InjectErrCode errID);

    /**
     * Mixin destructor to prevent standalone instanciations
     */
    virtual ~InjectedErrorListener();
    /**
     * Start receiving error notifications
     */
    void start();

    /**
     * return true if the error has been injected
     */
    bool test(long line, const char* file);

    /**
     * test and throw if the error has been injected
     */
    void throwif(long line, const char* file);

    /**
     * Must be called before destructing the object
     */
    void stop();

 private:
    void handle(typename Notification<class InjectedError>::MessageTypePtr msg);

    typename Notification<class InjectedError>::SubscriberID _subscriberID;
    InjectErrCode _errID;

    typename Notification<class InjectedError>::MessageTypePtr _err;
    Mutex _mutex;
};

/**
 * Provides a template for creating a listener and testing for the injected error
 * @param template parameter errorCode allows the compiler to generate a unique
 * instance of this function for each error code value.
 * @param line the line number from where the test for the injected error occurred
 * @param file the file from where the test for the injected error occurred
 * @return true if the injected error occurred, false if not
 */
template<InjectErrCode errorCode>
bool injectedError(const long line, const char* file)
{
    static InjectedErrorListener injectedErrorListener(errorCode);
    return injectedErrorListener.test(line, file);
}

/**
 * Convenience wrapper macro for injectedError template function.
 *
 * Calling code may invoke this macro as
 *    if(hasInjectedError(NO_CHUNK_PRESENT, __LINE__, __FILE__)) ...
 * or use the equivalent
 *    if(injectedError<InjectErrCode::NO_CHUNK_PRESENT>(__LINE__, __FILE__)) ...
 * with the former being a more compact syntax than the latter.
 */
#define hasInjectedError(ErrorCode, line, file)         \
    injectedError<InjectErrCode::ErrorCode>(line, file)

} //namespace scidb

#endif
