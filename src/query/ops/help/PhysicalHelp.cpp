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
 * @file
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 *
 * @brief This operator shows parameters of other operator
 */

#include <array/MemArray.h>
#include <query/PhysicalOperator.h>
#include <query/OperatorLibrary.h>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>

using namespace std;

namespace {

// Crude but usually effective.
string wrap(string const& s, string const& indent)
{
    using boost::is_space;
    using boost::token_compress_on;
    vector<string> parts;
    boost::split(parts, s, is_space(), token_compress_on);

    string sep;
    stringstream ss;
    size_t const MAX = 80;
    size_t line_len = indent.size(); // we start out indented
    for (auto const& p : parts) {
        line_len += p.size() + sep.size();
        if (line_len >= MAX) {
            ss << '\n' << indent << p;
            line_len = p.size() + indent.size();
        } else {
            ss << sep << p;
        }
        if (sep.empty()) {
            sep = ' ';
        }
    }
    return ss.str();
}

}

namespace scidb
{

class PhysicalHelp: public PhysicalOperator
{
public:
    PhysicalHelp(const std::string& logicalName,
        const std::string& physicalName, const Parameters& parameters,
        const ArrayDesc& schema) :
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    /// @see OperatorDist
    DistType inferSynthesizedDistType(std::vector<DistType> const& /*inDist*/, size_t /*depth*/) const override
    {
        return _schema.getDistribution()->getDistType();
    }

    /// @see PhysicalOperator
    RedistributeContext getOutputDistribution(const std::vector<RedistributeContext>&,
                                              const std::vector< ArrayDesc>&) const override
    {
        return RedistributeContext(_schema.getDistribution(), _schema.getResidency());
    }

    void preSingleExecute(std::shared_ptr<Query> query) override
    {
        stringstream ss;
        OperatorLibrary& opLib = *OperatorLibrary::getInstance();

        if (_parameters.size() == 1)
        {
            const string opName = paramToString(_parameters[0]);
            ss << "Operator: " << opName << '\n';

            auto op = opLib.createLogicalOperator(opName);
            auto pr = opLib.getPlistRecognizer(opName);

            if (!op->getUsage().empty()) {
                ss << "Usage: " << op->getUsage();
            }
            else if (pr) {
                // Positional and keyword parameters have their own
                // regular expressions, print them in the canonical
                // order.

                string const INDENT(::strlen("Parameters: "), ' ');
                ss << "Parameters: ";
                bool hasParams = false;

                // Any non-hidden keyword parameters?
                bool hasKeywords = false;
                for (auto const& kwInfo : *pr) {
                    if (!kwInfo.first.empty() && kwInfo.first[0] != '_') {
                        hasKeywords = true;
                        hasParams = true;
                        break;
                    }
                }

                // Positionals (keyword "") first.
                auto pos = pr->find("");
                if (pos != pr->end()) {
                    ss << wrap(pos->second.regex->asRegex(", "), INDENT);
                    hasParams = true;
                    if (hasKeywords) {
                        ss << '\n' << INDENT << "[, "; // keywords will follow
                    }
                } else {
                    // No positionals, so no comma prior to keywords.
                    if (hasKeywords) {
                        ss << "[ ";
                    }
                }

                if (hasKeywords) {
                    // Always-optional keyword parameters, in any order.
                    string sep;
                    for (auto const& kwInfo : *pr ) {
                        if (kwInfo.first.empty()) {
                            continue; // Skip the positionals!
                        }
                        if (kwInfo.first[0] == '_') {
                            continue; // Skip hidden!
                        }
                        ss << sep << kwInfo.first
                           << ": " << kwInfo.second.regex->asRegex(", ");
                        if (sep.empty()) {
                            sep = " ]\n" + INDENT + "[, ";
                        }
                    }
                    if (!sep.empty()) {
                        ss << " ]"; // Close the last keyword.
                    }
                }

                if (!hasParams) {
                    ss << "None";
                }
            }
            else {
                // Operator doesn't (yet) have a PlistRecognizer.

                ss << "Usage: " << opName << '(';
                bool first = true;

                for (auto const& ph : op->getParamPlaceholders())
                {
                    if (first) {
                        first = false;
                    } else {
                        ss << ", ";
                    }
                    ss << _placeholderTypeToString(ph->getPlaceholderType());
                }

                if (!op->getKeywordPlaceholders().empty())
                {
                    for (auto const& kw : op->getKeywordPlaceholders())
                    {
                        if (kw.first[0] == '_')
                            continue; // undocumented keyword
                        if (first) {
                            first = false;
                            ss << "[ ";
                        } else {
                            ss << " [, ";
                        }
                        ss << kw.first << ": "
                           << _placeholderTypeToString(kw.second->getPlaceholderType())
                           << ']';
                    }
                }

                ss << ')';
            }
        }
        else
        {
            ss << "Use existing operator name as argument for help operator. "
                "You can see all operators by executing list('operators').";
        }

        _result = std::shared_ptr<MemArray>(new MemArray(_schema,query));
        const auto& fda = _result->getArrayDesc().getAttributes().firstDataAttribute();
        std::shared_ptr<ArrayIterator> arrIt = _result->getIterator(fda);
        Coordinates coords;
        coords.push_back(0);
        Chunk& chunk = arrIt->newChunk(coords);
        std::shared_ptr<ChunkIterator> chunkIt = chunk.getIterator(query);
        Value v(TypeLibrary::getType(TID_STRING));
        v.setString(ss.str().c_str());
        chunkIt->writeItem(v);
        chunkIt->flush();
    }

    std::shared_ptr<Array> execute(
        std::vector<std::shared_ptr<Array> >& inputArrays,
        std::shared_ptr<Query> query) override
    {
        SCIDB_ASSERT(inputArrays.size() == 0);
        if (!_result) {
            _result = std::shared_ptr<MemArray>(new MemArray(_schema,query));
        }
        return _result;
    }

private:

    static const char* _placeholderTypeToString(OperatorParamPlaceholderType x)
    {
        switch (x) {
#       define X(_name, _bit, _tag, _desc) case PLACEHOLDER_ ## _name: return _tag ;
#       include <query/Placeholder.inc>
#       undef X
        default:
            SCIDB_ASSERT(false);
            return "<unknown placeholder>"; // avoid compiler warning
        }

        SCIDB_UNREACHABLE();
    }

    std::shared_ptr<Array> _result;
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalHelp, "help", "impl_help")

} //namespace
