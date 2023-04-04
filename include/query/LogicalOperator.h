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
 * @file LogicalOperator.h
 *
 */

#ifndef LOGICAL_OPERATOR_H_
#define LOGICAL_OPERATOR_H_

#include <array/ArrayDesc.h>
#include <query/DFA.h>
#include <query/OperatorDist.h>
#include <query/OperatorParam.h>

namespace scidb
{

class Aggregate;
class Query;

// Convenience type aliases for operators that implement makePlistSpec().
using PlistRegex = dfa::RE<OperatorParamPlaceholder>;
using PlistSpec = std::map<std::string, PlistRegex>;
using PP = OperatorParamPlaceholder; // Shorthand to make writing these specs
using RE = PlistRegex;               // in logical ops less cumbersome.

std::ostream& operator<< (std::ostream&, PlistRegex const&);
std::ostream& operator<< (std::ostream&, PlistSpec const&);

/**
 * This is pure virtual class for all logical operators. It provides API of logical operator.
 * In order to add new logical operator we must inherit new class and implement all methods in it.
 * Derived implementations are located in ops folder. See example there to know how to write new operators.
 */
class LogicalOperator : public OperatorDist
{
public:
    struct Properties
    {
        bool ddl                 { false }; // Data definition language (DDL) operator
        bool exclusive           { false }; // Requires XCL exclusive locking
        bool tile                { false }; // Supports tile mode
        bool updater             { false }; // Can update datastore (phys op is-a PhysicalUpdate)
        bool dataframe           { true };  // Supports dataframe input(s)
        bool schemaReservedNames { false }; // Operator allows reserved names in schema inputs.
    };

    // NOTE:  There are other places where operators can be inserted, such
    // as the Desugarer and the optimizer.  In the case of the Desugarer, it
    // adds Node objects that are later used by the Translator to create operator
    // instances.  Those should be noted appropriately and that information carried
    // from the Node into the newly constructed LogicalOperator.
    // Add new insertion points to this enumeration as needed.
    enum class Inserter
    {
         USER,        // Operator inserted by the user in AFL or AQL
         TRANSLATOR,  // Operator inserted by the translator (e.g., cast during canonicalizeTypes)
    };

public:
    LogicalOperator(const std::string& logicalName, const std::string& aliasName = "");

    virtual ~LogicalOperator(){}

    const std::string& getLogicalName() const;

    /// @return physicalOperatorName() for a physicalOperator and logicalOperatorName for a logical Operator
    const std::string&  getOperatorName() const override;

    /// @see OperatorDist
    /// temporary scaffolding. we must override OperatorDist::getSynthesized type for a while longer
    ///
    /// many LogicalOperators return an ArrayDesc from inferSchema() that has a bogus
    /// DistType, obtained from defaultDistType(), which is being phased out in
    /// favor of getSynthesizedDistType().
    /// However, until the synthesizedDistType is actually available on LogicalOperators
    /// this override allows us to continue to supply something that does not break the
    /// ArrayDesc ctor, even though it is not strictly correct.
    /// Once the logical synthesis works as well as the physical synthesis, this override
    /// will then be dropped.
    virtual DistType getSynthesizedDistType() const override;

    const Parameters& getParameters() const
    {
        return _parameters;
    }

    Parameters& getParameters()
    {
        return _parameters;
    }

    // Shorthand to index into the top-level parameter list.
    template<class T>
    std::shared_ptr<T> const& param(size_t i) const
    {
        SCIDB_ASSERT(i < getParameters().size());

        return (std::shared_ptr<T> const &)(getParameters()[i]);
    }

    virtual void setParameters(const Parameters& parameters);

    virtual void addParameter(const Parameter& parameter);

    const KeywordParameters& getKeywordParameters() const;

    Parameter findKeyword(const std::string& kw) const
    {
        auto const kwPair = getKeywordParameters().find(kw);
        return kwPair == getKeywordParameters().end() ? Parameter() : kwPair->second;
    }

    void addParameter(const char* keyword, const Parameter& parameter)
    {
        assert(keyword);
        bool inserted = _kwParameters.insert(make_pair(keyword, parameter)).second;
        if (!inserted) {
            throw USER_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_DUPLICATE_KEYWORD_PARAM)
                << keyword << getLogicalName();
        }
    }

    void setSchema(const ArrayDesc& schema);
    bool hasSchema() const;
    const ArrayDesc& getSchema() const;

    const std::string& getAliasName() const
    {
        return _aliasName;
    }

    void setAliasName(const std::string &alias);

    const Properties& getProperties() const
    {
        return _properties;
    }

    /** @note Do NOT override this if your operator uses placeholder regexes. */
    virtual bool compileParamInTileMode(size_t paramNo)
    {
        return false;
    }

    /** @note Do NOT override this if your operator uses placeholder regexes. */
    virtual bool compileParamInTileMode(std::string const& keyword)
    {
        return false;
    }

    /**
     * Tile mode property for operators that use placeholder regexes.
     *
     * @details Specifying parameter types with regular expressions
     * means that the top-level parameter list may contain nested
     * parameter lists.  The @c where vector indicates which nested
     * parameter the optimizer is inquiring about.  For positional
     * parameters the @c keyword argument is empty.  For keyword
     * parameters it contains the keyword, and if the value is itself
     * a nested parameter list, the @c where vector indicates the
     * position in the nested list.
     *
     * @note Tile mode ops that use placeholder regexes MUST support
     *       this, and ignore the other overloads above.
     */
    virtual bool compileParamInTileMode(PlistWhere const& where,
                                        std::string const& keyword)
    {
        return false;
    }

    virtual ArrayDesc inferSchema(std::vector< ArrayDesc>, std::shared_ptr< Query> query) = 0;

    /**
     * @brief Determine required access rights and locks for arrays, namespaces, etc.
     * @param query the current query context
     *
     * @description
     * This is where the logical operator can request array level locks for
     * any of the arrays specified in the operator parameters (or others).
     * The default implementation requests LockDesc::RD locks
     * for all arrays mentioned in the query string.  The subclasses are
     * expected to override this method if stricter locks are needed, and
     * the overrides should call LogicalOperator::inferAccess() to get
     * the minimal read locks.
     *
     * @p For an array used as a schema parameter, we'll always need
     * to read the namespace of that array, so acquire that right.
     * But for other array parameters, sometimes the derived class
     * will upgrade an acquired catalog RD lock, so we @em don't want
     * to automatically grab namespace rights for those parameters:
     * instead let the subclass do it.
     *
     * @note Array locks are not acquired in this method, only requested.
     *
     * @see scidb::Query::requestLock()
     * @see scidb::rbac::RightsMap::upsert()
     */
    virtual void inferAccess(const std::shared_ptr<Query>& query);

    /**
     * @brief Does query have rights for all array and array-as-schema parameters?
     *
     * @description
     * The query processor calls this as an integrity check after each
     * call to inferAccess(), to make sure that operators don't forget
     * to specify needed rights for -all- namespaces they intend to
     * touch.
     */
    bool hasRightsForAllNamespaces(const std::shared_ptr<Query>& query);

    /** Visit logical parameters, positional and keyword, nested or not. */
    void visitParameters(PlistVisitor& f)
    {
        scidb::visitParameters(f, _parameters, _kwParameters);
    }

    /** @note Obsolete, implement LogicalFoo::makePlistSpec() instead. */
    void addParamPlaceholder(const PlaceholderPtr& paramPlaceholder)
    {
        if (_paramPlaceholders.size() > 0 &&
                _paramPlaceholders[_paramPlaceholders.size() - 1]->getPlaceholderType() != PLACEHOLDER_INPUT &&
                paramPlaceholder->getPlaceholderType() == PLACEHOLDER_INPUT)
        {
                throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_INPUTS_MUST_BE_BEFORE_PARAMS) << _logicalName;
        }

        if (_paramPlaceholders.size() > 0 &&
                _paramPlaceholders[_paramPlaceholders.size() - 1]->getPlaceholderType() == PLACEHOLDER_VARIES)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_VAR_MUST_BE_AFTER_PARAMS) << _logicalName;
        }

        _paramPlaceholders.push_back(paramPlaceholder);
    }

    /** @note Obsolete, implement LogicalFoo::makePlistSpec() instead. */
    const Placeholders& getParamPlaceholders() const
    {
        return _paramPlaceholders;
    }

    void addKeywordPlaceholder(std::string const& keyword, PlaceholderPtr kwPlaceholder)
    {
        _kwPlaceholders.insert(std::make_pair(keyword, kwPlaceholder));
    }

    const KeywordPlaceholders& getKeywordPlaceholders() const
    {
        return _kwPlaceholders;
    }

    const std::string &getUsage() const
    {
        return _usage;
    }

    void setInserter(Inserter inserter);

    bool isUserInserted() const;

    /**
     *  Get generic inspectable string.
     *
     *  @description PhysicalOperator::inspectLogicalOp() is allowed to examine the LogicalOperator,
     *  but the current physical code layout often prevents a particular PhysicalFoo from knowing
     *  the definition of LogicalFoo.  This virtual method allows such physical operators to gleen
     *  some information from their logical counterpart without requiring the particular physical
     *  operator class definition.
     */
    virtual std::string getInspectable() const { return std::string(); }

    /**
      * Retrieve a human-readable description.
      * Append a human-readable description of this onto str. Description takes up
      * one or more lines. Append indent spacer characters to the beginning of
      * each line. Call toString on interesting children. Terminate with newline.
      * @param[out] stream to write to
      * @param[in] indent number of spacer characters to start every line with.
      */
    virtual void toString(std::ostream &out, int indent = 0) const;

protected:
    /**
     * Create a default ArrayDesc instance that is effectively a non-array for operators
     * like cancel() or most DDL operators which don't return an array.
     * @return An instance of ArrayDesc appropriate for indicating that this operator
     *   doesn't return an array.
     */
    ArrayDesc ddlArrayDesc(const std::shared_ptr<Query>& query) const;

    Properties _properties;
    std::string _usage;
    Parameters _parameters;
    KeywordParameters _kwParameters;

private:
    void _requestLeastAccess(Parameter const& p, std::shared_ptr<Query> const& query);
    bool _hasNamespaceRights(Parameter const& p, std::shared_ptr<Query> const& query);

    std::string _logicalName;
    bool      _hasSchema;
    ArrayDesc _schema;
    std::string _aliasName;
    Placeholders _paramPlaceholders;        // For old-style param checking only
    KeywordPlaceholders _kwPlaceholders;    // For old-style param checking only
    Inserter _inserter;
};


/**
 * Does logical operator support parameter list specification via
 * placeholder regular expressions?
 *
 * @details Logical operators that specify their parameter types using
 * <query/DFA.h> regular expressions do so by implementing a static
 * method
 *
 * @code
 * static PlistSpec const* makePlistSpec();
 * @endcode
 *
 * This struct template determines at compile-time whether a
 * LogicalOperator-derived class provides makePlistSpec().
 *
 * @see https://en.wikipedia.org/wiki/Substitution_failure_is_not_an_error
 * @see https://stackoverflow.com/questions/10957924/is-there-any-way-to-\
 *      detect-whether-a-function-exists-and-can-be-used-at-compile
 */
template <typename U>
struct uses_plist_spec {
    typedef char yes;
    struct no { char _[2]; };
    template < typename T, PlistSpec const* (*)() = &T::makePlistSpec >
    static yes impl( T* );
    static no  impl(...);

    enum { value = sizeof( impl( static_cast<U*>(0) ) ) == sizeof(yes) };
};


/** Call static T::makePlistSpec() iff class T provides it. */
template <typename T>
typename std::enable_if<uses_plist_spec<T>::value, PlistSpec const*>::type
plist_spec_getter()
{
    return T::makePlistSpec();
}

/** Used when T does NOT have a static makePlistSpec() method. */
template <typename T>
typename std::enable_if<!uses_plist_spec<T>::value, PlistSpec const*>::type
plist_spec_getter()
{
    return nullptr;
}


/**
 * It's base class for constructing logical operators. It declares some virtual functions that
 * will have template implementation.
 */
class BaseLogicalOperatorFactory
{
public:
    BaseLogicalOperatorFactory(const std::string& logicalName)
        : _logicalName(logicalName)
    {}

    virtual ~BaseLogicalOperatorFactory() = default;

    const std::string& getLogicalName() const
    {
        return _logicalName;
    }

    virtual std::shared_ptr<LogicalOperator>
    createLogicalOperator(const std::string& alias) = 0;

    virtual PlistSpec const* makePlistSpec() = 0;

protected:
    void registerFactory();

private:
    std::string _logicalName;
};


/**
 * This is template implementation of logical operators factory. To declare operator factory for
 * new logical operator just declare variable
 * LogicalOperatorFactory<NewLogicalOperator> newLogicalOperatorFactory("logical_name");
 */
template<class T>
struct LogicalOperatorFactory : BaseLogicalOperatorFactory
{
    LogicalOperatorFactory(const std::string& logicalName)
        : BaseLogicalOperatorFactory(logicalName)
    {}

    virtual ~LogicalOperatorFactory() = default;

    std::shared_ptr<LogicalOperator>
    createLogicalOperator(const std::string& alias) override
    {
        return std::shared_ptr<LogicalOperator>(new T(getLogicalName(), alias));
    }

    PlistSpec const* makePlistSpec() override
    {
        return plist_spec_getter<T>();
    }
};


template<class T>
struct UserDefinedLogicalOperatorFactory : LogicalOperatorFactory<T>
{
    UserDefinedLogicalOperatorFactory(const std::string& logicalName)
        : LogicalOperatorFactory<T>(logicalName)
    {
        BaseLogicalOperatorFactory::registerFactory();
    }

    virtual ~UserDefinedLogicalOperatorFactory() = default;
};

#define DECLARE_LOGICAL_OPERATOR_FACTORY(name, uname)                   \
    static LogicalOperatorFactory<name> _logicalFactory##name(uname);   \
    BaseLogicalOperatorFactory* get_logicalFactory##name()              \
    {                                                                   \
        return &_logicalFactory##name;                                  \
    }

#define REGISTER_LOGICAL_OPERATOR_FACTORY(name, uname) \
    static UserDefinedLogicalOperatorFactory<name> _logicalFactory##name(uname)


/**
 * This function is called by a LogicalOperator's inferSchema, if the operator takes aggregated attributes.
 * @param aggregateCall   an aggregate call, as a parameter to the operator.
 * @param inputDesc       the schema of the input array.
 * @param outputDesc      the schema of the output array.
 * @param operatorDoesAggregateInOrder  whether the operator guarantees to make aggregate calls in
 *                                      some deterministic order of the values.
 */
void addAggregatedAttribute (std::shared_ptr <OperatorParamAggregateCall>const& aggregateCall,
                             ArrayDesc const& inputDesc,
                             ArrayDesc& outputDesc,
                             bool operatorDoesAggregationInOrder);

} // namespace

#endif /* LOGICAL_OPERATOR_H_ */
