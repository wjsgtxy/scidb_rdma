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

#ifndef QUERY_PARSER_PARSER_DETAILS_H_
#define QUERY_PARSER_PARSER_DETAILS_H_

/****************************************************************************/

#include <system/Exceptions.h>                           // For error messages
#include <util/PointerRange.h>                           // For PointerRange
#include <util/Arena.h>                                  // For arena library
#include <query/Parser.h>                                // Public interface

/****************************************************************************/
namespace scidb {
/****************************************************************************/

class UserException;                                     // Base of hierarchy
class ParsingContext;                                    // {location,text}

/****************************************************************************/
namespace parser {
/****************************************************************************/

using std::shared_ptr;                                 // A tracking pointer
using std::make_shared;                                // For allocating one

/****************************************************************************/

class Log;                                               // Disposes of errors
class Node;                                              // A syntax tree node
class Lexer;                                             // Tokenizes the code
class Table;                                             // Maintains bindings
class Parser;                                            // Parses the tokens
class Visitor;                                           // Visits tree nodes
class Factory;                                           // Creates the nodes
class location;                                          // A source location

/****************************************************************************/

enum syntax
{
    aqlStatement,                                        // An AQL statement
    aflStatement,                                        // An AFL statement
    aflExpression,                                       // An AFL expression
    aflModule                                            // An AFL module
};

enum lexicon
{
    AFL,                                                 // The AFL lexicon
    AQL                                                  // The AQL lexicon
};

enum zone
{
    type_zone  = 1,                                      // For types
    array_zone = 2,                                      // For arrays
    apply_zone = 4                                       // For callable names
};

typedef unsigned zones;                                  // A bitmask of zones

/****************************************************************************/

typedef double                   real;                   // A real constant
typedef const char*              chars;                  // A string constant
typedef bool                     boolean;                // A boolean constant
typedef int64_t                  integer;                // An integer constant

/****************************************************************************/

typedef const char*              name;                   // An entity name
typedef int32_t                  error;                  // An error code
typedef std::shared_ptr<Query>        QueryPtr;               // The original query
typedef std::shared_ptr<std::string>  StringPtr;              // Its source text

/****************************************************************************/
/**
 *  @brief      Represents an abstract compilation error sink.
 *
 *  @details    Class Log  represents an  abstract sink for compilation errors
 *              that are detected while scanning, parsing, and translating the
 *              source code into its executable form.
 *
 *              The current implementation simply packages up each error as an
 *              exception and then throws it - in other words, the compilation
 *              fails on the first error - but a future implementation may put
 *              the error on a list and return enabling compilation to proceed
 *              so the caller should be written to assume that fail() returns.
 *              This would also enable warning messages to be properly handled
 *              too.
 *
 *  @author     jbell@paradigm4.com.
 */
struct Log
{
    virtual void                 fail(const UserException&) const             = 0;
    virtual void                 fail(error,const Node&    ,chars = "") const = 0;
    virtual void                 fail(error,const location&,chars = "") const = 0;
};

/****************************************************************************/

Table*                           getTable ();

/****************************************************************************/

Node*&                           desugar  (Factory&,Log&,Node*&,const QueryPtr&);
Node*&                           inliner  (Factory&,Log&,Node*&);
std::shared_ptr<LogicalExpression>    translateExp(Factory&,Log&,const StringPtr&,Node*,const QueryPtr&);
std::shared_ptr<LogicalQueryPlanNode> translatePlan(Factory&,Log&,const StringPtr&,Node*,const QueryPtr&);

/****************************************************************************/

/****************************************************************************/
}}
/****************************************************************************/
#endif
/****************************************************************************/
