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
 * @file OperatorParam.h
 *
 * This file contains class declarations for the OperatorParam* classes.
 */

#ifndef OPERATOR_PARAM_H_
#define OPERATOR_PARAM_H_

#include <array/ArrayDesc.h>
#include <array/ArrayDistributionInterface.h>
#include <query/TypeSystem.h>
#include <rbac/NamespaceDesc.h>

#include <boost/serialization/serialization.hpp>
#include <boost/serialization/string.hpp> // needed for serialization of string parameter
#include <boost/serialization/vector.hpp>

namespace scidb
{

class Aggregate;
class Expression;
class LogicalExpression;
class Query;

class OperatorParam;
typedef std::shared_ptr<OperatorParam> Parameter;
typedef std::vector<Parameter> Parameters;
typedef std::map<std::string, Parameter> KeywordParameters;
typedef std::pair<std::string, Parameter> KwPair;

/**
 * Functor type for visiting logical or physical parameters.
 *
 * @param[in,out] param  The current parameter being visited.
 * @param[in] where      For positional parameters, *or* for keyword parameters
 *                       whose values are nested parameter lists, indices for
 *                       getting to the current parameter from the root list.
 * @param[in] kw         For keyword parameters, the keyword.  Empty for
 *                       positional parameters.
 *
 * @details For example, if you have an operator with arguments
 * @code
 * op_foo(A, (alpha, 12), scale_by: (3.1415, 6.02e23, 2.718))
 * @endcode
 * then when visiting the 12, @c where is [0, 1] and @c kw is "".  When
 * visiting 6.02e23, @c where is likewise [0, 1] but @c kw is "scale_by".
 */
using PlistWhere = std::vector<size_t>;
using PlistVisitor = std::function<void(Parameter& param,
                                        PlistWhere const& where,
                                        std::string const& kw)>;

/** Shared code for depth-first visiting logical or physical parameters. */
void visitParameters(PlistVisitor&, Parameters&, KeywordParameters&);

enum OperatorParamPlaceholderType
{
#define X(_name, _bit, _tag, _desc)  PLACEHOLDER_ ## _name = (1 << _bit),
#include <query/Placeholder.inc>
#undef X
};

class OperatorParamPlaceholder;
typedef std::shared_ptr<OperatorParamPlaceholder> PlaceholderPtr;
typedef std::vector<PlaceholderPtr> Placeholders;
typedef std::multimap<std::string, PlaceholderPtr> KeywordPlaceholders;

class OperatorParamPlaceholder final
{
public:
    // Default ctor is needed by DFA machinery but serves no other purpose.
    OperatorParamPlaceholder()
        : _placeholderType(PLACEHOLDER_UNDEF)
    { }

    explicit OperatorParamPlaceholder(OperatorParamPlaceholderType placeholderType)
        : _placeholderType(placeholderType)
    { }

    OperatorParamPlaceholder(OperatorParamPlaceholderType placeholderType,
                             Type const& requiredType)
        : _placeholderType(placeholderType)
        , _requiredType(requiredType)
    { }

    OperatorParamPlaceholder(OperatorParamPlaceholderType placeholderType,
                             const char* typeId);

    /** Set false if parameter need not exist in some input array's schema. */
    OperatorParamPlaceholder& setMustExist(bool b)
    {
        _mustExist = b;
        return *this;
    }

    /** Set true if a versioned array name such as A@5 is allowable. */
    OperatorParamPlaceholder& setAllowVersions(bool b)
    {
        SCIDB_ASSERT(_placeholderType == PLACEHOLDER_ARRAY_NAME);
        _allowVersions = b;
        return *this;
    }

    /**
     * Retrieve a human-readable description.
     * Append a human-readable description of this onto str. Description takes up
     * one or more lines. Append indent spacer characters to the beginning of
     * each line. Terminate with newline.
     * @param[out] stream to write to
     * @param[in]  indent number of spacer characters to start every line with.
     */
    virtual void toString(std::ostream &out, int indent = 0) const;

    // Describe in a way suitable for embedding in error messages.
    std::string toDescription() const;

    OperatorParamPlaceholderType getPlaceholderType() const
    {
        return _placeholderType;
    }

    const Type& getRequiredType() const
    {
        return _requiredType;
    }

    bool isInputSchema() const
    {
        return _mustExist;
    }

    bool allowVersions() const
    {
        return _allowVersions;
    }

private:
    OperatorParamPlaceholderType _placeholderType; // Set by all ctors
    Type _requiredType;
    bool _mustExist { true };
    bool _allowVersions { false };
};

bool operator== (OperatorParamPlaceholder const& left, OperatorParamPlaceholder const& right);
bool operator< (OperatorParamPlaceholder const& left, OperatorParamPlaceholder const& right);
std::ostream& operator<< (std::ostream& os, OperatorParamPlaceholder const& ph);

enum OperatorParamType
{
    PARAM_UNKNOWN,
    PARAM_ARRAY_REF,
    PARAM_ATTRIBUTE_REF,
    PARAM_DIMENSION_REF,
    PARAM_LOGICAL_EXPRESSION,
    PARAM_PHYSICAL_EXPRESSION,
    PARAM_SCHEMA,
    PARAM_AGGREGATE_CALL,
    PARAM_NS_REF,
    PARAM_ASTERISK,
    PARAM_NESTED,
    PARAM_DISTRIBUTION,
    PARAM_OBJECT_NAME,
};

class ParsingContext;
/**
 * Base class for parameters to both logical and physical operators.
 *
 * @note If you add a new child class of OperatorParam, you must add the child
 * class to registerLeafDerivedOperatorParams(), near the end of this file.
 */
class OperatorParam
{
public:
    OperatorParam() :
        _paramType(PARAM_UNKNOWN)
    {
    }

    OperatorParam(OperatorParamType paramType,
                  const std::shared_ptr<ParsingContext>& parsingContext) :
        _paramType(paramType),
        _parsingContext(parsingContext)
    {
    }

    OperatorParamType getParamType() const
    {
        return _paramType;
    }

    std::shared_ptr<ParsingContext> getParsingContext() const
    {
        return _parsingContext;
    }

    virtual ~OperatorParam() = default;

protected:
    OperatorParamType _paramType;
    std::shared_ptr<ParsingContext> _parsingContext;

public:
    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        ar & _paramType;
    }

    /**
    * Retrieve a human-readable description.
    * Append a human-readable description of this onto str. Description takes up
    * one or more lines. Append indent spacer characters to the beginning of
    * each line. Terminate with newline.
    * @param[out] stream to write to
    * @param[in] indent number of spacer characters to start every line with.
    */
    virtual void toString(std::ostream &out, int indent = 0) const;
};

class OperatorParamDistribution : public OperatorParam
{
    ArrayDistPtr _distribution;

public:
    OperatorParamDistribution()
        : OperatorParam()
        , _distribution(nullptr)
    {}

    OperatorParamDistribution(const std::shared_ptr<ParsingContext>& parsingContext,
                              ArrayDistPtr distribution)
        : OperatorParam(PARAM_DISTRIBUTION,
                        parsingContext)
        , _distribution(distribution)
    {}

    ArrayDistPtr getDistribution() const
    {
        return _distribution;
    }

    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        ar & boost::serialization::base_object<OperatorParam>(*this);
        OperatorParam::serialize(ar, version);

        if (Archive::is_saving::value) {
            auto dt = static_cast<int>(_distribution->getDistType());
            auto redundancy = static_cast<uint64_t>(_distribution->getRedundancy());
            ar & dt;
            ar & redundancy;
        }
        else {
            DistType dt;
            size_t redundancy;
            ar & dt;
            ar & redundancy;
            _distribution = createDistribution(dt, redundancy);
        }
    }
};

/**
 * class OperatorParamObjectName
 *
 * Represent a named object where operators assign meaning
 * in a context-dependent way.
 */
class OperatorParamObjectName : public OperatorParam
{
public:
    /**
     * A container where each element is a part or component
     * of a qualified name.
     *
     * For example, the qualfied name X.Y.Z would be represented
     * as elements in this container in this way:
     *
     *   [0] = "X"
     *   [1] = "Y"
     *   [2] = "Z"
     *
     */
    using QualifiedName = std::vector<std::string>;

    /**
     * Default constructor.
     */
    OperatorParamObjectName()
            : OperatorParam()
    {}

    /**
     * Constructor.
     * @param parsingContext The ParsingContext instance, usually created
     * by the Translator.
     * @name The object name, be it a dimension, attribute, or other object
     * name; context-dependent.
     */
    OperatorParamObjectName(const std::shared_ptr<ParsingContext>& parsingContext,
                            const QualifiedName& name)
            : OperatorParam(PARAM_OBJECT_NAME,
                            parsingContext)
            , _name(name)
    {}

    /**
     * Serialization (and de-serialization) method.
     * @param ar The target serializer (or de-serializer)
     * @version Parameter required by serialization framework, see
     * https://www.boost.org/doc/libs/1_54_0/libs/serialization/doc/index.html
     * (at time of writing, SciDB uses boost 1.54.0).
     */
    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        ar & boost::serialization::base_object<OperatorParam>(*this);
        OperatorParam::serialize(ar, version);

        ar & _name;
    }

    /**
     * @return The name.
     */
    const QualifiedName& getName() const
    {
        return _name;
    }

    /**
     * @return A qualified name in the a.b.c.d format, for
     * however many components are present.
     *
     * Arguably this should be a member of the QualifiedName class,
     * yet at this point in time that is a typedef and this method
     * is called only in one place.  Should the need arise, the
     * QualifiedName typedef can be promoted to a class and this
     * method moved there.
     */
    std::string toString() const;

    /**
     * Retrieve a human-readable description.
     * Append a human-readable description of this onto str. Description takes up
     * one or more lines. Append indent spacer characters to the beginning of
     * each line. Terminate with newline.
     * @param[out] stream to write to
     * @param[in]  indent number of spacer characters to start every line with.
     */
    void toString(std::ostream &out, int indent = 0) const override;

private:
    QualifiedName _name;
};

class OperatorParamReference: public OperatorParam
{
public:
    OperatorParamReference() : OperatorParam(),
        _arrayName(""),
        _objectName(""),
        _inputNo(-1),
        _objectNo(-1),
        _inputScheme(false)
    {
    }

    OperatorParamReference(
            OperatorParamType paramType,
            const std::shared_ptr<ParsingContext>& parsingContext,
            const std::string& arrayName,
            const std::string& objectName, bool inputScheme):
        OperatorParam(paramType, parsingContext),
        _arrayName(arrayName),
        _objectName(objectName),
        _inputNo(-1),
        _objectNo(-1),
        _inputScheme(inputScheme)
    {}

    const std::string& getArrayName() const
    {
        return _arrayName;
    }

    const std::string& getObjectName() const
    {
        return _objectName;
    }

    int32_t getInputNo() const
    {
        return _inputNo;
    }

    int32_t getObjectNo() const
    {
        return _objectNo;
    }

    void setInputNo(int32_t inputNo)
    {
        _inputNo = inputNo;
    }

    void setObjectNo(int32_t objectNo)
    {
        _objectNo = objectNo;
    }

    bool isInputScheme() const
    {
        return _inputScheme;
    }

private:
    std::string _arrayName;
    std::string _objectName;

    int32_t _inputNo;
    int32_t _objectNo;

    bool _inputScheme;

public:
    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        ar & boost::serialization::base_object<OperatorParam>(*this);
        OperatorParam::serialize(ar, version);
        ar & _arrayName;
        ar & _objectName;
        ar & _inputNo;
        ar & _objectNo;
        ar & _inputScheme;
    }

    /**
    * Retrieve a human-readable description.
    * Append a human-readable description of this onto str. Description takes up
    * one or more lines. Append indent spacer characters to the beginning of
    * each line. Terminate with newline.
    * @param[out] stream to write to
    * @param[in] indent number of spacer characters to start every line with.
    */
    void toString(std::ostream &out, int indent = 0) const override;
};

class OperatorParamArrayReference: public OperatorParamReference
{
public:
    OperatorParamArrayReference() :
        OperatorParamReference(), _version(0)
    {
        _paramType = PARAM_ARRAY_REF;
    }

    OperatorParamArrayReference(
            const std::shared_ptr<ParsingContext>& parsingContext,
            const std::string& arrayName, const std::string& objectName, bool inputScheme,
            VersionID version = 0):
        OperatorParamReference(PARAM_ARRAY_REF, parsingContext, arrayName, objectName, inputScheme),
        _version(version)
    {
    }

    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        ar & boost::serialization::base_object<OperatorParamReference>(*this);
        ar & _version;
    }

    /**
     * Retrieve a human-readable description.
     * Append a human-readable description of this onto str. Description takes up
     * one or more lines. Append indent spacer characters to the beginning of
     * each line. Terminate with newline.
     * @param[out] stream to write to
     * @param[in] indent number of spacer characters to start every line with.
     */
    void toString(std::ostream &out, int indent = 0) const override;

    VersionID getVersion() const;

private:
    VersionID _version;
};

class OperatorParamNamespaceReference: public OperatorParamReference
{
public:
    OperatorParamNamespaceReference()
        : OperatorParamReference()
        , _nsId(rbac::INVALID_NS_ID)
    {
        _paramType = PARAM_NS_REF;
    }

    OperatorParamNamespaceReference(const std::shared_ptr<ParsingContext>& parseCtx,
                                    NamespaceDesc const& nsDesc)
        : OperatorParamReference(PARAM_NS_REF,
                                 parseCtx,
                                 nsDesc.getName(), // The "array name", counterintuitively.
                                 "",
                                 nsDesc.isIdValid()) // Invalid id implies an output parameter,
                                                     // but valid can be either input or output.
        , _nsId(nsDesc.getId())
    { }

    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        ar & boost::serialization::base_object<OperatorParamReference>(*this);
        ar & _nsId;
    }

    void toString(std::ostream &out, int indent = 0) const override;

    rbac::ID getNsId() const { return _nsId; }
    std::string getNsName() const { return getArrayName(); }
    NamespaceDesc getNamespace() const
    {
        return NamespaceDesc(getArrayName(), _nsId);
    }

private:
    rbac::ID _nsId;
};

class OperatorParamAttributeReference: public OperatorParamReference
{
public:
    OperatorParamAttributeReference() :
        OperatorParamReference(),
        _sortAscent(true)
    {
        _paramType = PARAM_ATTRIBUTE_REF;
    }

    OperatorParamAttributeReference(
            const std::shared_ptr<ParsingContext>& parsingContext,
            const std::string& arrayName,
            const std::string& objectName,
            bool inputScheme):
        OperatorParamReference(PARAM_ATTRIBUTE_REF,
                               parsingContext,
                               arrayName,
                               objectName,
                               inputScheme),
        _sortAscent(true)
    {}

    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        ar & boost::serialization::base_object<OperatorParamReference>(*this);
        ar & _sortAscent;
    }

    /**
    * Retrieve a human-readable description.
    * Append a human-readable description of this onto str. Description takes up
    * one or more lines. Append indent spacer characters to the beginning of
    * each line. Terminate with newline.
    * @param[out] stream to write to
    * @param[in] indent number of spacer characters to start every line with.
    */
    void toString(std::ostream &out, int indent = 0) const override;

    bool getSortAscent() const
    {
        return _sortAscent;
    }

    void setSortAscent(bool sortAscent)
    {
        _sortAscent = sortAscent;
    }

private:
    //Sort quirk
    bool _sortAscent;
};

class OperatorParamDimensionReference: public OperatorParamReference
{
public:
    OperatorParamDimensionReference() : OperatorParamReference()
    {
        _paramType = PARAM_DIMENSION_REF;
    }

    OperatorParamDimensionReference(
            const std::shared_ptr<ParsingContext>& parsingContext,
            const std::string& arrayName,
            const std::string& objectName,
            bool inputScheme):
        OperatorParamReference(PARAM_DIMENSION_REF, parsingContext, arrayName, objectName, inputScheme)
    {}

    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        ar & boost::serialization::base_object<OperatorParamReference>(*this);
    }

    /**
    * Retrieve a human-readable description.
    * Append a human-readable description of this onto str. Description takes up
    * one or more lines. Append indent spacer characters to the beginning of
    * each line. Terminate with newline.
    * @param[out] stream to write to
    * @param[in] indent number of spacer characters to start every line with.
    */
    void toString(std::ostream &out, int indent = 0) const override;
};

class OperatorParamLogicalExpression: public OperatorParam
{
public:
    OperatorParamLogicalExpression() : OperatorParam()
    {
        _paramType = PARAM_LOGICAL_EXPRESSION;
    }

    OperatorParamLogicalExpression(
        const std::shared_ptr<ParsingContext>& parsingContext,
        const std::shared_ptr<LogicalExpression>& expression,  Type expectedType,
        bool constant = false):
            OperatorParam(PARAM_LOGICAL_EXPRESSION, parsingContext),
            _expression(expression),
            _expectedType(expectedType),
            _constant(constant)
    {

    }

    std::shared_ptr<LogicalExpression> getExpression() const
    {
        return _expression;
    }

    const  Type& getExpectedType() const
    {
        return _expectedType;
    }

    bool isConstant() const
    {
        return _constant;
    }

private:
    std::shared_ptr<LogicalExpression> _expression;
    Type _expectedType;
    bool _constant;

public:
    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        assert(0);
    }

    /**
     * Retrieve a human-readable description.
     * Append a human-readable description of this onto str. Description takes up
     * one or more lines. Append indent spacer characters to the beginning of
     * each line. Terminate with newline.
     * @param[out] stream to write to
     * @param[in] indent number of spacer characters to start every line with.
     */
    void toString(std::ostream &out, int indent = 0) const override;
};

class OperatorParamPhysicalExpression : public OperatorParam
{
public:
    OperatorParamPhysicalExpression() : OperatorParam()
    {
        _paramType = PARAM_PHYSICAL_EXPRESSION;
    }

    OperatorParamPhysicalExpression(
            const std::shared_ptr<ParsingContext>& parsingContext,
            const std::shared_ptr<Expression>& expression,
            bool constant = false):
        OperatorParam(PARAM_PHYSICAL_EXPRESSION, parsingContext),
        _expression(expression),
        _constant(constant)
    {}

    std::shared_ptr<Expression> getExpression() const
    {
        return _expression;
    }

    bool isConstant() const
    {
        return _constant;
    }

private:
    std::shared_ptr<Expression> _expression;
    bool _constant;

public:
    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        ar & boost::serialization::base_object<OperatorParam>(*this);

        if (Archive::is_loading::value) {
            // TO-DO: If the de-serialized pointer may be equivalent to a previously-deserialized pointer,
            // the trick at PhysicalQueryPlanNode::serialize() may be used.
            Expression* e;
            ar & e;
            _expression = std::shared_ptr<Expression>(e);
        }
        else {
            Expression* e = _expression.get();
            ar & e;
        }

        ar & _constant;
    }

    /**
    * Retrieve a human-readable description.
    * Append a human-readable description of this onto str. Description takes up
    * one or more lines. Append indent spacer characters to the beginning of
    * each line. Terminate with newline.
    * @param[out] stream to write to
    * @param[in] indent number of spacer characters to start every line with.
    */
    void toString(std::ostream &out, int indent = 0) const override;
};


class OperatorParamSchema: public OperatorParam
{
public:
    OperatorParamSchema() : OperatorParam()
    {
        _paramType = PARAM_SCHEMA;
    }

    OperatorParamSchema(
            const std::shared_ptr<ParsingContext>& parsingContext,
            const ArrayDesc& schema):
        OperatorParam(PARAM_SCHEMA, parsingContext),
        _schema(schema)
    {}

    const ArrayDesc& getSchema() const
    {
        return _schema;
    }

private:
    ArrayDesc _schema;

public:
    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        ar & boost::serialization::base_object<OperatorParam>(*this);
        ar & _schema;
    }

    /**
    * Retrieve a human-readable description.
    * Append a human-readable description of this onto str. Description takes up
    * one or more lines. Append indent spacer characters to the beginning of
    * each line. Call toString on interesting children. Terminate with newline.
    * @param[out] stream to write to
    * @param[in] indent number of spacer characters to start every line with.
    */
    void toString(std::ostream &out, int indent = 0) const override;
};

class OperatorParamAggregateCall: public OperatorParam
{
public:
    OperatorParamAggregateCall() : OperatorParam()
    {
        _paramType = PARAM_AGGREGATE_CALL;
    }

    OperatorParamAggregateCall(
            const std::shared_ptr<ParsingContext>& parsingContext,
            const std::string& aggregateName,
            std::shared_ptr <OperatorParam> const& inputAttribute,
            const std::string& alias):
        OperatorParam(PARAM_AGGREGATE_CALL, parsingContext),
        _aggregateName(aggregateName),
        _inputAttribute(inputAttribute),
        _alias(alias)
    {}

    std::string const& getAggregateName() const
    {
        return _aggregateName;
    }

    std::shared_ptr<OperatorParam> const& getInputAttribute() const
    {
        return _inputAttribute;
    }

    void setAlias(const std::string& alias)
    {
        _alias = alias;
    }

    std::string const& getAlias() const
    {
        return _alias;
    }

private:
    std::string _aggregateName;
    std::shared_ptr <OperatorParam> _inputAttribute;
    std::string _alias;

public:
    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        ar & boost::serialization::base_object<OperatorParam>(*this);
        ar & _aggregateName;

        if (Archive::is_loading::value) {
            // TO-DO: If the de-serialized pointer may be equivalent to a previously-deserialized pointer,
            // the trick at PhysicalQueryPlanNode::serialize() may be used.
            OperatorParam* op;
            ar & op;
            _inputAttribute = std::shared_ptr<OperatorParam>(op);
        }
        else {
            OperatorParam* op = _inputAttribute.get();
            ar & op;
        }

        ar & _alias;
    }

    /**
    * Retrieve a human-readable description.
    * Append a human-readable description of this onto str. Description takes up
    * one or more lines. Append indent spacer characters to the beginning of
    * each line. Call toString on interesting children. Terminate with newline.
    * @param[out] stream to write to
    * @param[in] indent number of spacer characters to start every line with.
    */
    void toString(std::ostream &out, int indent = 0) const override;
};

/**
 * @brief Little addition to aggregate call parameter. Mostly for built-in COUNT(*).
 */
class OperatorParamAsterisk: public OperatorParam
{
public:
    OperatorParamAsterisk(): OperatorParam()
    {
        _paramType = PARAM_ASTERISK;
    }

    OperatorParamAsterisk(
            const std::shared_ptr<ParsingContext>& parsingContext
            ) :
        OperatorParam(PARAM_ASTERISK, parsingContext)
    {
    }

    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        ar & boost::serialization::base_object<OperatorParam>(*this);
    }

    /**
    * Retrieve a human-readable description.
    * Append a human-readable description of this onto str. Description takes up
    * one or more lines. Append indent spacer characters to the beginning of
    * each line. Call toString on interesting children. Terminate with newline.
    * @param[out] stream to write to
    * @param[in] indent number of spacer characters to start every line with.
    */
    void toString(std::ostream &out, int indent = 0) const override;
};

/**
 * Composed group of (sub)parameters to support nested parameter lists.
 */
class OperatorParamNested : public OperatorParam
{
public:
    OperatorParamNested()
        : OperatorParam()
    {
        _paramType = PARAM_NESTED;
    }

    OperatorParamNested(const std::shared_ptr<ParsingContext>& parsingContext)
        : OperatorParam(PARAM_NESTED, parsingContext)
    {}

    bool empty() const { return _subparams.empty(); }
    size_t size() const { return _subparams.size(); }
    void push_back(Parameter& p) { _subparams.push_back(p); }

    // Accessors for immediate subparameters.
    Parameters const& getParameters() const { return _subparams; }
    Parameters& getParameters() { return _subparams; }

    template <class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        ar & boost::serialization::base_object<OperatorParam>(*this);

        size_t sz = _subparams.size(); // 0 on load, N on save
        ar & sz;
        _subparams.resize(sz);         // no-op on save

        if (Archive::is_loading::value) {
            for (size_t i = 0; i < sz; ++i) {
                OperatorParam* op;
                ar & op;
                _subparams[i] = std::shared_ptr<OperatorParam>(op);
            }
        }
        else {
            for (size_t i = 0; i < sz; ++i) {
                OperatorParam* op = _subparams[i].get();
                ar & op;
            }
        }
    }

    void toString(std::ostream &out, int indent = 0) const override;

private:
    Parameters _subparams;
};

template<class Archive>
void registerLeafDerivedOperatorParams(Archive& ar)
{
    ar.register_type(static_cast<OperatorParamArrayReference*>(NULL));
    ar.register_type(static_cast<OperatorParamDistribution*>(NULL));
    ar.register_type(static_cast<OperatorParamAttributeReference*>(NULL));
    ar.register_type(static_cast<OperatorParamDimensionReference*>(NULL));
    ar.register_type(static_cast<OperatorParamObjectName*>(NULL));
    ar.register_type(static_cast<OperatorParamLogicalExpression*>(NULL));
    ar.register_type(static_cast<OperatorParamPhysicalExpression*>(NULL));
    ar.register_type(static_cast<OperatorParamSchema*>(NULL));
    ar.register_type(static_cast<OperatorParamAggregateCall*>(NULL));
    ar.register_type(static_cast<OperatorParamNamespaceReference*>(NULL));
    ar.register_type(static_cast<OperatorParamAsterisk*>(NULL));
    ar.register_type(static_cast<OperatorParamNested*>(NULL));
}

/**
 * Convert logical or physical parameter to string.
 *
 * @param p   parameter to convert, must be a constant logical or
 *            physical expression
 * @return string value extracted from parameter
 * @throw internal error unless p can be evaluated as a string.
 *
 * For good or ill the syntax for evaluating logical vs. physical
 * expressions is different.  We do this so often for strings that
 * it's convenient to have a function that can be called from both
 * PhysicalOperator and LogicalOperator subclasses.
 */
std::string paramToString(Parameter const& p);

/**
 * Convert logical or physical parameter to uint32_t.
 *
 * @param p   parameter to convert, must be a constant logical or
 *            physical expression
 * @return uint32_t value extracted from parameter
 * @throw internal error unless p can be evaluated as an uint32_t.
 */
uint32_t paramToUInt32(Parameter const& p);

/**
 * Convert logical or physical parameter to int64_t.
 *
 * @param p   parameter to convert, must be a constant logical or
 *            physical expression
 * @return int64_t value extracted from parameter
 * @throw internal error unless p can be evaluated as an int64_t.
 */
int64_t paramToInt64(Parameter const& p);

/**
 * Convert logical or physical parameter to uint64_t.
 *
 * @param p   parameter to convert, must be a constant logical or
 *            physical expression
 * @return uint64_t value extracted from parameter
 * @throw internal error unless p can be evaluated as an uint64_t.
 */
uint64_t paramToUInt64(Parameter const& p);

/**
 * Convert logical or physical parameter to boolean.
 *
 * @param p   parameter to convert, must be a constant logical or
 *            physical expression
 * @return boolean value extracted from parameter
 * @throw internal error unless p can be evaluated as a boolean.
 */
bool paramToBool(Parameter const& p);

} // namespace

#endif /* OPERATOR_PARAM_H_ */
