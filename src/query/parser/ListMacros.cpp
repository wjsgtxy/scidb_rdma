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

/****************************************************************************/

#include <array/TupleArray.h>                            // For TupleArray
#include "Table.h"                                       // For Table
#include "AST.h"                                         // For Node
#include <query/Query.h>

/****************************************************************************/
namespace scidb {
/****************************************************************************/

using namespace parser;                                  // ALl of parser
using scidb::arena::ArenaPtr;

/****************************************************************************/

namespace {

/** Given a binding node, extract its name. */
name getName(const Node* pn)
{
    return pn->get(bindingArgName)->getString();
}

} // anonymous namespace

/**
 *  Implements the 'inferSchema' method for the "list('macros')" operator.
 */
ArrayDesc logicalListMacros(const std::shared_ptr<Query>& query, bool showHidden)
{
    using std::max;
    using std::stringstream;

    size_t n = max(getTable()->size(), 1LU);             // Elements to emit

    // But the count 'n' includes macros that begin with '_', and if
    // those are to be hidden then we need to (painfully) count only
    // the ones without leading underscores.
    //
    if (!showHidden)
    {
        struct Counter : Visitor
        {
            Counter(const Table& t) : count(0) { t.accept(*this); }

            void onBinding(Node*& pn) override
            {
                name nm = getName(pn);
                if (nm[0] != '_')
                {
                    ++count;
                }
            }

            size_t count;
        };

        n = max(Counter(*getTable()).count, 1LU);
    }

    Attributes outAtts;
    outAtts.push_back(AttributeDesc("name",TID_STRING,0,CompressorType::NONE)); // ...name attribute
    outAtts.push_back(AttributeDesc("type",TID_STRING,0,CompressorType::NONE)); // ...type attribute

    stringstream ss;
    ss << query->getInstanceID();
    ArrayDistPtr localDistribution = ArrayDistributionFactory::getInstance()->construct(dtLocalInstance,
                                                                                        DEFAULT_REDUNDANCY,
                                                                                        ss.str());
    return ArrayDesc("macros",                           // The array name
                     outAtts,
                     {DimensionDesc("No", 0, n-1, n, 0)},          // Has one dimension
                     localDistribution,
                     query->getDefaultArrayResidency());
}

/**
 *  Implements the 'execute' method for the "list('macros')" operator.
 *
 *  We define a local visitor subclass that formats each binding it visits and
 *  pushes another tuple onto the end of the vector it carries along with it.
 */
std::shared_ptr<Array> physicalListMacros(const ArenaPtr& arena,
                                          const std::shared_ptr<Query>& query,
                                          bool showHidden)
{
    using std::string;
    using std::ostringstream;

    struct Lister : Visitor
    {
        Lister(const Table& t,const ArenaPtr& arena,
               const std::shared_ptr<Query>& query,
               bool showHidden)
            : tuples(std::make_shared<TupleArray>(logicalListMacros(query, showHidden), arena))
            , hide(!showHidden)
        {
            t.accept(*this);                             // Visit the bindings
        }

        void onBinding(Node*& pn) override
        {
            Value t[2];                                  // Local value pair
            name nm = getName(pn);                       // Macro name

            if (hide && nm[0] == '_')                    // Hide leading '_'?
            {
                return;
            }

            t[0].setString(getName(pn));                 // Set name component
            t[1].setString(getType(pn));                 // Set type component

            tuples->appendTuple(t);                      // Append the tuple
        }

     /* Format and return a type string of the form 'name(a1, .. ,aN)', where
        the idenitifiers 'a.i' name the formal parameters of the macro...*/

        string getType(const Node* pn)
        {
            ostringstream s;                             // Local out stream

            s << getName(pn);                            // Insert macro name

            pn = pn->get(bindingArgBody);                // Aim at macro body

            if (pn->is(abstraction))                     // Takes parameters?
            {
                pn = pn->get(abstractionArgBindings);    // ...aim at bindings
                s << '(';                                // ...opening paren
                insertRange(s,pn->getList(),',');        // ...join with ','
                s << ')';                                // ...closing paren
            }

            return s.str();                              // The type string
        }

        std::shared_ptr<TupleArray> tuples;              // The result array
        bool hide;                                       // Hide leading '_'
    };

    return Lister(*getTable(), arena, query, showHidden).tuples;
}

/****************************************************************************/
}
/****************************************************************************/
