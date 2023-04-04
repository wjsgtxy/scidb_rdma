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
 * @file Exceptions.h
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 *
 * @brief Exceptions which thrown inside SciDB
 *
 * @note IMPORTANT: To avoid object slicing, never use ``throw;'' to
 *       rethrow a scidb::Exception-derived object.  Instead, call
 *       ``exc.raise()''.  See COMMON_EXCEPTION_METHODS() macro below.
 *       Similarly, use ``exc.clone()'' rather than invoking a base
 *       class copy constructor.
 */

#ifndef EXCEPTIONS_H_
#define EXCEPTIONS_H_

#include <system/ErrorCodes.h>

#include <query/QueryID.h>
#include <system/PluginApi.h>
#include <util/Platform.h>    // for isDebug()
#include <util/StringUtil.h>  // for REL_FILE

#include <boost/any.hpp>
#include <boost/format.hpp>

#include <memory>

#define SYSTEM_EXCEPTION(_short_error, _long_error)             \
    scidb::SystemException(REL_FILE, __FUNCTION__, __LINE__,    \
                           CORE_ERROR_NAMESPACE,                \
                           _short_error, _long_error,           \
                           #_short_error, #_long_error)

#define SYSTEM_EXCEPTION_SPTR(_short_error, _long_error)        \
    std::make_shared<scidb::SystemException>(                   \
        REL_FILE, __FUNCTION__, __LINE__,                       \
        CORE_ERROR_NAMESPACE,                                   \
        int(_short_error), int(_long_error),                    \
        #_short_error, #_long_error)

#define PLUGIN_SYSTEM_EXCEPTION(_err_space, _short_error, _long_error)  \
    scidb::SystemException(REL_FILE, __FUNCTION__, __LINE__,            \
                           _err_space, _short_error, _long_error,       \
                           #_short_error, #_long_error)

/**
 * This macro is equivalent to an assertion in DEBUG build, and an
 * exception in RELEASE build.
 *
 * NOTE, if there are issues in the future with _msg_ not being referenced
 * for, e.g. ASSERT_EXCEPTION(false, msg), then use
 * #pragma GCC diagnostic push
 * #pragma GCC diagnostic ignored "-Wunused-variable"
 * #pragma GCC diagnostic pop
 * rather than doing the very expensive ostream << _msg_
 * that was once used here to make _msg_ used.
 * That causes the behavior of Debug/Assert and RelWithDebInfo builds
 * to be far too different in timing due to additional mallocs and
 * other system calls it induced.
 */
#   define ASSERT_EXCEPTION(_cond, _msg)                                \
    do {                                                                \
        bool cond = static_cast<bool>(_cond);                           \
        if (!cond) {                                                    \
            assert(cond);                                               \
            std::stringstream ss;                                       \
            ss << _msg ;                                                \
            throw SYSTEM_EXCEPTION(scidb::SCIDB_SE_INTERNAL,            \
                                   scidb::SCIDB_LE_ASSERTION_FAILED)    \
                << #_cond << __FILE__ << __LINE__ << ss.str();          \
        }                                                               \
    } while (false)

/**
 * Expect that _stmt will not throw.  In the event that it throws and
 * the product is built as DEBUG, trigger an assertion.  In the event
 * that it throws and the product is built as RELEASE, invoke the
 * callback provided via _on_exc_callback to provide sane handling in
 * release build environments.
 */
#   define SHOULD_NOT_THROW(_stmt, _on_exc_callback)             \
    do {                                                         \
        try {                                                    \
            _stmt ;                                              \
        }                                                        \
        catch (const std::exception& e) {                        \
            const std::string what = e.what();                   \
            assert(false);                                       \
            _on_exc_callback(what.c_str());                      \
        }                                                        \
        catch (...) {                                            \
            assert(false);                                       \
            _on_exc_callback("unknown non-std::exception");      \
        }                                                        \
    } while (false)

/**
 * The macro is equivalent to ASSERT_EXCEPTION( false, _msg );
 * but it is designed so that it will not generate compiler warning
 * messages if it is the only action is a non-void function.
 */
#define ASSERT_EXCEPTION_FALSE(_msg)                            \
do {                                                            \
    assert(false);                                              \
    std::stringstream ss;                                       \
    ss << _msg ;                                                \
    throw SYSTEM_EXCEPTION(scidb::SCIDB_SE_INTERNAL,            \
                           scidb::SCIDB_LE_UNREACHABLE_CODE)    \
        << ss.str();                                            \
} while (0)

/**
 * This macro provides the boilerplate code each Exception subclass must
 * have to support throwing exceptions polymorphically, that is, without
 * object slicing and the subtle bugs that slicing causes.  See "How do
 * I throw polymorphically?" in https://isocpp.org/wiki/faq/exceptions .
 */
#define COMMON_EXCEPTION_METHODS(_exc_type)                     \
    void raise() const override __attribute__ ((noreturn))      \
    {                                                           \
        throw *this;                                            \
    }                                                           \
                                                                \
    Exception::Pointer clone() const override                   \
    {                                                           \
        auto e = std::make_shared<_exc_type>(*this);            \
        return e;                                               \
    }                                                           \
                                                                \
    template <class T>                                          \
    _exc_type& operator <<(const T &param)                      \
    {                                                           \
        try                                                     \
        {                                                       \
            getMessageFormatter() % param;                      \
        }                                                       \
        catch (std::exception& e)                               \
        {                                                       \
            if (isDebug()) {                                    \
                const std::string what = e.what();              \
                /* Generate a core to analyze 'what' */         \
                assert(false);                                  \
            }                                                   \
        }                                                       \
        catch (...)                                             \
        {                                                       \
            /* Silently ignore errors during adding */          \
            /* parameters, but not in debug builds. */          \
            assert(false);                                      \
        }                                                       \
                                                                \
        return *this;                                           \
    }


/**
 * This macro provides the boilerplate code for free functions
 * usually wanted for Exception subclasses.
 */
#define COMMON_EXCEPTION_FREE_FUNCTIONS(_exc_type)              \
    template <class T>                                          \
    std::shared_ptr<_exc_type>                                  \
    operator <<(std::shared_ptr<_exc_type> e, const T &param)   \
    {                                                           \
        (*e) << param;                                          \
        return e;                                               \
    }


/**
 * The macro is used to declare a SystemException subclass that hard-codes
 * the short and long error codes.  The specific subclass can then be the
 * target of a catch statement.
 *
 * Some exceptions so declared are used by plugins, hence PLUGIN_EXPORT
 * is necessary.
 */
#define DECLARE_SYSTEM_EXCEPTION_SUBCLASS(_exc_type,                    \
                                          _short_error, _long_error)    \
                                                                        \
    class PLUGIN_EXPORT _exc_type : public scidb::SystemException       \
    {                                                                   \
    public:                                                             \
        _exc_type(const char* file,                                     \
                  const char* function,                                 \
                  int32_t line)                                         \
            : scidb::SystemException(file, function, line,              \
                                     CORE_ERROR_NAMESPACE,              \
                                     _short_error, _long_error,         \
                                     #_short_error, #_long_error)       \
       {}                                                               \
                                                                        \
       COMMON_EXCEPTION_METHODS(_exc_type)                              \
    }                                                                   \

/**
 * Same as DECLARE_SYSTEM_EXCEPTION_SUBCLASS(), but insert some
 * arguments at construction time using operator<< .
 */
#define DECLARE_SYSTEM_EXC_SUBCLASS_W_ARGS(_exc_type,                   \
                                           _short_error,                \
                                           _long_error,                 \
                                           _args)                       \
                                                                        \
    class PLUGIN_EXPORT _exc_type : public scidb::SystemException       \
    {                                                                   \
    public:                                                             \
        _exc_type(const char* file,                                     \
                  const char* function,                                 \
                  int32_t line)                                         \
            : scidb::SystemException(file, function, line,              \
                                     CORE_ERROR_NAMESPACE,              \
                                     _short_error, _long_error,         \
                                     #_short_error, #_long_error)       \
        {                                                               \
            *this << _args ;  /* Pre-insert the arguments! */           \
        }                                                               \
                                                                        \
       COMMON_EXCEPTION_METHODS(_exc_type)                              \
    }

/** Throw these. */
#define SYSTEM_EXCEPTION_SUBCLASS(_exc_type) \
    _exc_type(REL_FILE, __FUNCTION__, __LINE__)

namespace scidb
{

/**
 * Exception base class for SciDB (and all its plugins).
 *
 * @note IMPORTANT: To avoid object slicing, never use ``throw;'' to
 *       rethrow a scidb::Exception object.  Instead, call
 *       ``exc->raise()''.  See COMMON_EXCEPTION_METHODS() macro.
 */
class PLUGIN_EXPORT Exception : public virtual std::exception
{
    friend class UserQueryException;

public:
    typedef std::shared_ptr<Exception> Pointer;

    Exception() = default;

    Exception(const char* file, const char* function, int32_t line,
              const char* errors_namespace, int32_t short_error_code, int32_t long_error_code,
              const char* stringified_short_error_code, const char* stringified_long_error_code,
              const QueryID& query_id = INVALID_QUERY_ID);

    // We must have a copy constructor/operator (to be thrown),
    // and we are relying on the default ones.
    // Make sure to avoid XxxException object slicing by calling raise() in
    // places where you cannot use "throw;" with no argument,
    // see https://isocpp.org/wiki/faq/exceptions .

    virtual ~Exception() noexcept = default;

    const std::string& getErrorsNamespace() const;

    const char* what() const noexcept;

    const std::string& getFile() const;

    const std::string& getFunction() const;

    int32_t getLine() const;

    const std::string& getWhatStr() const;

    int32_t getShortErrorCode() const;

    int32_t getLongErrorCode() const;

    const std::string& getStringifiedShortErrorCode() const;

    const std::string& getStringifiedLongErrorCode() const;

    virtual Exception::Pointer clone() const = 0;

    virtual void raise() const = 0;

    const std::string getErrorId() const;

    const std::string getStringifiedErrorId() const;

    std::string getErrorMessage() const;

    const QueryID& getQueryId() const;

    void setQueryId(const QueryID& queryId);

    InstanceID getInstanceId() const;

    // Appears in formatted whatStr.  Override if RTTI overhead bothers you.
    virtual std::string getClassName() const;

    /// Associate an arbitrary value with the exception.
    /// @note These are not serializable, so local instance use ONLY!!
    void setMemo(boost::any const& m)
    {
        _memo = m;
    }

    /// Retrieve an arbitrary value associated with the exception.
    /// @note These are not serializable, so local instance use ONLY!!
    boost::any getMemo() const
    {
        return _memo;
    }

    // FOR INTERNAL USE ONLY.  Used during transmission of Exception
    // objects across instances and in a few other system-internal
    // scenarios.  Made public to avoid yet another include file
    // dependency.
    void setInternalState(std::string const& w,
                          std::string const& f,
                          InstanceID i);

protected:
    // Derived class overrides are expected to call their base class
    // format() method and then append any additional information.
    virtual std::string format();

    boost::format& getMessageFormatter() const;

    // These data members have no side-effect-free getters.  We need
    // to make them directly accessible to subclasses.
    std::string _what_str;
    mutable std::string _formatted_msg;
    mutable boost::format _formatter;

private:
    std::string _file;
    std::string _function;
    int32_t _line;
    std::string _errors_namespace;
    int32_t _short_error_code;
    int32_t _long_error_code;
    std::string _stringified_short_error_code;
    std::string _stringified_long_error_code;
    QueryID _query_id;
    InstanceID _inst_id { INVALID_INSTANCE };

    // WARNING: Not serialized in mtError messages!  Instance-local use only!
    boost::any _memo;
};


/**
 * Base class for Exceptions raised due to errors from SciDB core.
 */
class PLUGIN_EXPORT SystemException : public Exception
{
public:
    SystemException(const char* file, const char* function, int32_t line,
                    const char* errors_namespace, int32_t short_error_code, int32_t long_error_code,
                    const char* stringified_short_error_code, const char* stringified_long_error_code,
                    const QueryID& query_id = INVALID_QUERY_ID);

    COMMON_EXCEPTION_METHODS(SystemException)
};

COMMON_EXCEPTION_FREE_FUNCTIONS(SystemException)

} // namespace

#endif /* EXCEPTIONS_H_ */
