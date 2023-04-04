/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2016-2019 SciDB, Inc.
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

#ifndef SUMMARIZE_SETTINGS
#define SUMMARIZE_SETTINGS

#include <limits>
#include <sstream>
#include <memory>
#include <string>
#include <vector>
#include <ctype.h>

#include <query/OperatorParam.h>
#include <query/AttributeComparator.h>
#include <util/Platform.h>

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>

namespace scidb
{

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.summarize"));

namespace summarize
{

using std::string;
using std::vector;
using std::shared_ptr;
using std::dynamic_pointer_cast;
using std::ostringstream;
using boost::algorithm::trim;
using boost::starts_with;
using boost::lexical_cast;
using boost::bad_lexical_cast;

/*
 * Settings for the summarize operator.
 */
class Settings
{
private:
    size_t _numInputAttributes;
    size_t _numInstances;
    bool _perAttributeSet;
    bool _perAttribute;
    bool _perInstanceSet;
    bool _perInstance;

public:
    static const size_t MAX_PARAMETERS = 2;
    Settings(ArrayDesc const& inputSchema,
             vector< shared_ptr<OperatorParam> > const& operatorParameters,
             bool logical,
             shared_ptr<Query>& query):
        _numInputAttributes(inputSchema.getAttributes().size()),
        _numInstances(query->getInstancesCount()),
        _perAttributeSet(false),
        _perAttribute(false),
        _perInstanceSet(false),
        _perInstance(false)
    {
        string const perAttributeParamHeader              = "per_attribute=";
        string const perInstanceParamHeader               = "per_instance=";
        size_t const nParams = operatorParameters.size();
         if (nParams > MAX_PARAMETERS)
         {   //assert-like exception. Caller should have taken care of this!
             throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION)
                   << "illegal number of parameters passed to Settings";
         }
        for(size_t i = 0; i<operatorParameters.size(); ++i)
    	{
            shared_ptr<OperatorParam>const& param = operatorParameters[i];
            {
                string parameterString;
                if (logical)
                {
                    parameterString = evaluate(((shared_ptr<OperatorParamLogicalExpression>&) param)->getExpression(), TID_STRING).getString();
                }
                else
                {
                    parameterString = ((shared_ptr<OperatorParamPhysicalExpression>&) param)->getExpression()->evaluate().getString();
                }
                parseStringParam(parameterString);
            }
    	}
    }
private:

    bool checkBoolParam(string const& param, string const& header, bool& target, bool& setFlag)
    {
        string headerWithEq = header + "=";
        if(starts_with(param, headerWithEq))
        {
            if(setFlag)
            {
                ostringstream error;
                error<<"illegal attempt to set "<<header<<" multiple times";
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << error.str().c_str();
            }
            string paramContent = param.substr(headerWithEq.size());
            trim(paramContent);
            try
            {
                target = lexical_cast<bool>(paramContent);
                setFlag = true;
                return true;
            }
            catch (bad_lexical_cast const& exn)
            {
                ostringstream error;
                error<<"could not parse "<<param.c_str();
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << error.str().c_str();
            }
        }
        return false;
    }
    void parseStringParam(string const& param)
    {
        if(checkBoolParam (param,   "per_attribute",       _perAttribute,          _perAttributeSet       ) ) { return; }
        if(checkBoolParam (param,   "per_instance",        _perInstance,           _perInstanceSet       ) ) { return; }
        ostringstream error;
        error<<"unrecognized parameter "<<param;
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << error.str().c_str();
    }
public:
    static const size_t NUM_OUTPUT_ATTRIBUTES = 10;

    size_t numInputAttributes() const
    {
        return _numInputAttributes;
    }

    bool perAttributeflag() const
    {
        return _perAttribute;
    }

    bool perInstanceflag() const
    {
        return _perInstance;
    }
};

} //namespaces

#endif //summarize_settings
