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

/*
 * @file Operator.cpp
 *
 * @author roman.simakov@gmail.com
 *
 * @brief Implementation of basic operator methods.
 */

#include <query/OperatorParam.h>
#include <query/Expression.h>
#include <query/LogicalExpression.h>
#include <util/Indent.h>

#include <boost/algorithm/string/join.hpp>
#include <log4cxx/logger.h>

using namespace std;

namespace scidb
{

// Logger for operator. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.qproc.operator"));

bool operator== (OperatorParamPlaceholder const& left, OperatorParamPlaceholder const& right)
{
    return left.getPlaceholderType() == right.getPlaceholderType()
        && left.getRequiredType()    == right.getRequiredType()
        && left.isInputSchema()      == right.isInputSchema()
        && left.allowVersions()      == right.allowVersions();
}

bool operator< (OperatorParamPlaceholder const& left, OperatorParamPlaceholder const& right)
{
    if (left.getPlaceholderType() < right.getPlaceholderType()) {
        return true;
    } else if (left.getPlaceholderType() > right.getPlaceholderType()) {
        return false;
    }
    if (left.getRequiredType() < right.getRequiredType()) {
        return true;
    } else if (left.getRequiredType() > right.getRequiredType()) {
        return false;
    }
    if (left.isInputSchema() < right.isInputSchema()) {
        return true;
    } else if (left.isInputSchema() > right.isInputSchema()) {
        return false;
    }
    return left.allowVersions() < right.allowVersions();
}

static const char *PlaceholderTypeNames[] =
{
#define X(_name, _bit, _tag, _desc)  #_name ,
#include <query/Placeholder.inc>
#undef X
};

// Used by help() operator, keep it lookin' good!!
ostream& operator<< (ostream& os, OperatorParamPlaceholder const& ph)
{
    string name(PlaceholderTypeNames[::ffs(ph.getPlaceholderType()) - 1]);
    transform(name.begin(), name.end(), name.begin(), ::tolower);
    os << '<' << name;
    if (!ph.isInputSchema()) {
        os << "_out";
    }
    if (ph.getRequiredType().typeId() != TID_VOID) {
        os << ':' << ph.getRequiredType(); // void is boring
    }
    if (ph.allowVersions()) {
        os << "(@n)?";       // vaguely regex-like
    }
    os << '>';
    return os;
}

OperatorParamPlaceholder::OperatorParamPlaceholder(OperatorParamPlaceholderType placeholderType,
                                                   const char* typeId)
    : _placeholderType(placeholderType)
    , _requiredType(TypeLibrary::getType(typeId))
{ }

void OperatorParamPlaceholder::toString(std::ostream &out, int indent) const
{
    Indent prefix(indent);
    out << prefix(' ', false);
    size_t index = ::ffs(_placeholderType) - 1;
    out << "[opParamPlaceholder] " << PlaceholderTypeNames[index];
    out << " requiredType " << _requiredType.name();
    out << " mustExist " << _mustExist << '\n';
}

static const char *PlaceholderDescriptions[] =
{
#define X(_name, _bit, _tag, _desc)  _desc ,
#include <query/Placeholder.inc>
#undef X
};

string OperatorParamPlaceholder::toDescription() const
{
    string result;
    size_t index = ::ffs(_placeholderType) - 1;
    if (index < SCIDB_SIZE(PlaceholderDescriptions)) {
        result = PlaceholderDescriptions[index];
    } else {
        stringstream ss;
        ss << "PLACEHOLDER_" << _placeholderType
           << "_0x" << hex <<  _placeholderType;
        result = ss.str();
    }
    TypeId tid = _requiredType.typeId();
    if (tid != TID_VOID) {
        result += " with type ";
        result += tid;
    }
    return result;
}

void OperatorParam::toString(std::ostream &out, int indent) const
{
    Indent prefix(indent);
    out << prefix(' ', false);
    out <<"[param] type "<<_paramType<<"\n";
}

std::string OperatorParamObjectName::toString() const
{
    return boost::algorithm::join(_name, ".");
}

void OperatorParamObjectName::toString(std::ostream &out, int indent) const
{
    Indent prefix(indent);
    out << prefix(' ', false);
    out << toString();
    OperatorParam::toString(out, indent);
}

void OperatorParamReference::toString(std::ostream &out, int /*indent*/) const
{
    out << "object "<<_objectName;
    out << " inputNo " <<_inputNo;
    out << " objectNo " <<_objectNo;
    out << " inputScheme "<<_inputScheme;
    out << "\n";
}

void OperatorParamArrayReference::toString(std::ostream &out, int indent) const
{
    Indent prefix(indent);
    out << prefix(' ', false);
    out << "[paramArrayReference] ";
    OperatorParamReference::toString(out, indent);
}

void OperatorParamAttributeReference::toString(std::ostream &out, int indent) const
{
    Indent prefix(indent);
    out << prefix(' ', false);
    out << "[paramAttributeReference] ";
    OperatorParamReference::toString(out,indent);
}

void OperatorParamDimensionReference::toString(std::ostream &out, int indent) const
{
    Indent prefix(indent);
    out << prefix(' ', false);
    out << "[paramDimensionReference] ";
    OperatorParamReference::toString(out,indent);
}

void OperatorParamLogicalExpression::toString(std::ostream &out, int indent) const
{
    Indent prefix(indent);
    out << prefix(' ', false);
    out << "[paramLogicalExpression] type "<<_expectedType.name();
    out << " const " << _constant << "\n";
    _expression->toString(out, indent+1);
}

void OperatorParamPhysicalExpression::toString(std::ostream &out, int indent) const
{
    Indent prefix(indent);
    out << prefix(' ', false);
    out << "[paramPhysicalExpression] const " << _constant;
    out << "\n";
    _expression->toString(out, indent+1);
}

void OperatorParamSchema::toString(std::ostream &out, int indent) const
{
    Indent prefix(indent);
    out << prefix(' ', false);
    out <<"[paramSchema] " << _schema <<"\n";
}

void OperatorParamAggregateCall::toString(std::ostream &out, int indent) const
{
    Indent prefix(indent);
    out << prefix(' ', false);
    out << "[paramAggregateCall] " << _aggregateName << "\n" ;

    out << prefix(' ', false);
    out <<"input: ";
    _inputAttribute->toString(out);

    if (_alias.size() )
    {
        out << prefix(' ');
        out << "alias " << _alias << "\n";
    }
}

void OperatorParamNamespaceReference::toString(std::ostream &out, int indent) const
{
    Indent prefix(indent);
    out << prefix(' ', false);
    out << "[paramNsName] " << getNsName() << "(id=" << getNsId() << ")\n";
}

void OperatorParamAsterisk::toString(std::ostream &out, int indent) const
{
    Indent prefix(indent);
    out << prefix(' ', false);
    out << "[paramAsterisk] *" << "\n";
}

void OperatorParamNested::toString(std::ostream &out, int indent) const
{
    Indent prefix(indent);
    out << prefix(' ', false);
    out << "[paramNested, size:" << _subparams.size() << "]\n";
    for (size_t i = 0; i < _subparams.size(); ++i) {
        out << prefix(' ', false) << '[' << i << "]: ";
        _subparams[i]->toString(out, indent + 1);
    }
}

VersionID OperatorParamArrayReference::getVersion() const
{
    return _version;
}

std::string paramToString(Parameter const& p)
{
    // Try logical first...
    auto const lexp = dynamic_cast<OperatorParamLogicalExpression const*>(p.get());
    if (lexp) {
        return evaluate(lexp->getExpression(), TID_STRING).getString();
    }
    auto const pexp = dynamic_cast<OperatorParamPhysicalExpression const *>(p.get());
    ASSERT_EXCEPTION(pexp, "Expression parameter required");
    return pexp->getExpression()->evaluate().getString();
}

uint32_t paramToUInt32(Parameter const& p)
{
    // Try logical first...
    auto const lexp = dynamic_cast<OperatorParamLogicalExpression const*>(p.get());
    if (lexp) {
        return evaluate(lexp->getExpression(), TID_UINT32).getUint32();
    }
    auto const pexp = dynamic_cast<OperatorParamPhysicalExpression const *>(p.get());
    ASSERT_EXCEPTION(pexp, "Expression parameter required");
    return pexp->getExpression()->evaluate().getUint32();
}

int64_t paramToInt64(Parameter const& p)
{
    // Try logical first...
    auto const lexp = dynamic_cast<OperatorParamLogicalExpression const*>(p.get());
    if (lexp) {
        return evaluate(lexp->getExpression(), TID_INT64).getInt64();
    }
    auto const pexp = dynamic_cast<OperatorParamPhysicalExpression const *>(p.get());
    ASSERT_EXCEPTION(pexp, "Expression parameter required");
    return pexp->getExpression()->evaluate().getInt64();
}

uint64_t paramToUInt64(Parameter const& p)
{
    // Try logical first...
    auto const lexp = dynamic_cast<OperatorParamLogicalExpression const*>(p.get());
    if (lexp) {
        return evaluate(lexp->getExpression(), TID_UINT64).getUint64();
    }
    auto const pexp = dynamic_cast<OperatorParamPhysicalExpression const *>(p.get());
    ASSERT_EXCEPTION(pexp, "Expression parameter required");
    return pexp->getExpression()->evaluate().getUint64();
}

bool paramToBool(Parameter const& p)
{
    // Try logical first...
    auto const lexp = dynamic_cast<OperatorParamLogicalExpression const*>(p.get());
    if (lexp) {
        return evaluate(lexp->getExpression(), TID_BOOL).getBool();
    }
    auto const pexp = dynamic_cast<OperatorParamPhysicalExpression const *>(p.get());
    ASSERT_EXCEPTION(pexp, "Expression parameter required");
    return pexp->getExpression()->evaluate().getBool();
}

} //namespace
