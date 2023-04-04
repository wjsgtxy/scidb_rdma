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

#include <query/UserQueryException.h>
#include <system/Config.h>
#include <util/arena/ScopedArena.h>
#include <query/ParsingContext.h>
#include <log4cxx/logger.h>
#include <fstream>                                       // For ifstream
#include "Module.h"
#include "Lexer.h"
#include "AST.h"                                         // For Node etc.
#include "AstWriter.h"

/****************************************************************************/
namespace scidb { namespace parser { namespace {
/****************************************************************************/

using scidb::arena::ScopedArena;
using std::ifstream;
using std::istreambuf_iterator;
using std::istringstream;
using std::string;

log4cxx::LoggerPtr log(log4cxx::Logger::getLogger("scidb.qproc.driver"));

typedef std::shared_ptr<LogicalExpression>    LEPtr;
typedef std::shared_ptr<LogicalQueryPlanNode> LQPtr;

/****************************************************************************/

class Driver : public Log, boost::noncopyable
{
 public:                   // Construction
                              Driver(const string&, const QueryPtr&);

 public:                   // Operations
            Node*             process(syntax);
            LEPtr             translateExp(Node*);
            LQPtr             translatePlan(Node*);

 private:                  // From class Log
    virtual void              fail(const UserException&) const;
    virtual void              fail(error,const Node&    ,const char*) const;
    virtual void              fail(error,const location&,const char*) const;

 private:                  // Representation
            StringPtr   const _text;
            ScopedArena       _arena;
            Factory           _fact;
            QueryPtr          _qry;
};

          Driver::Driver(const string& text, const QueryPtr& qry)
          : _text (make_shared<string>(text)),
            _arena(arena::Options("parser::Driver")),
            _fact (_arena),
            _qry(qry)
{}

Node* Driver::process(syntax syntax)
{
    Node*          tree  (0);
    istringstream  source(*_text);
    Lexer          lexer (_arena,*this,source,syntax);
    Parser         parser(_fact,*this,tree,lexer);

    bool traceLexerParser = false;
    if (traceLexerParser)
    {
        lexer.setTracing(true);
        parser.set_debug_level(true);
    }

    parser.parse();

    LOG4CXX_DEBUG(log,"Driver::process(1)\n" << tree);
    //LOG4CXX_DEBUG(log,"AstWriter(1)\n" << AstWriter(tree));

    desugar (_fact,*this,tree, _qry);

    LOG4CXX_DEBUG(log,"Driver::process(2)\n" << tree);
    //LOG4CXX_DEBUG(log,"AstWriter(2)\n" << AstWriter(tree));

    inliner (_fact,*this,tree);

    LOG4CXX_DEBUG(log,"Driver::process(3)\n" << tree);
    //LOG4CXX_DEBUG(log,"AstWriter(3)\n" << AstWriter(tree));

    return tree;
}

LEPtr Driver::translateExp(Node* n)
{
    return parser::translateExp(_fact,*this,_text,n,_qry);
}

LQPtr Driver::translatePlan(Node* n)
{
    return parser::translatePlan(_fact,*this,_text,n,_qry);
}

/****************************************************************************/

void Driver::fail(const UserException& what) const
{
    what.raise();
}

void Driver::fail(error e,const Node& n,const char* s) const
{
    const Node* p = &n;                                  // The location node

    if (p->is(variable))                                 // Is it a variable?
    {
        p = p->get(variableArgName);                     // ...then use name
    }

    if (p->is(cstring))                                  // Is a string node?
    {
        s = p->getString();                              // ...then use chars
    }

    this->fail(e,p->getWhere(),s);                       // Reformat and throw
}

void Driver::fail(error e,const location& w,const char* s) const
{
    std::shared_ptr<ParsingContext> c(make_shared<ParsingContext>(_text,w));

/* Translate error code 'e' into an exception object: unfortunately these can
   only be created via macros at the moment, hence the ugly 'switch'. hoping
   to change this soon...*/

    switch (e)
    {
        case SCIDB_LE_QUERY_PARSING_ERROR:  fail(USER_QUERY_EXCEPTION(SCIDB_SE_PARSER,SCIDB_LE_QUERY_PARSING_ERROR, c) << s);break;
        case SCIDB_LE_BAD_BLOCK_COMMENT:    fail(USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX,SCIDB_LE_BAD_BLOCK_COMMENT,   c));break;
        case SCIDB_LE_BAD_LITERAL_REAL:     fail(USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX,SCIDB_LE_BAD_LITERAL_REAL,    c) << s);break;
        case SCIDB_LE_BAD_LITERAL_INTEGER:  fail(USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX,SCIDB_LE_BAD_LITERAL_INTEGER, c) << s);break;
        case SCIDB_LE_NAME_REDEFINED:       fail(USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX,SCIDB_LE_NAME_REDEFINED,      c) << s);break;
        case SCIDB_LE_NAME_NOT_APPLICABLE:  fail(USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX,SCIDB_LE_NAME_NOT_APPLICABLE, c) << s);break;
        case SCIDB_LE_NAME_IS_RECURSIVE:    fail(USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX,SCIDB_LE_NAME_IS_RECURSIVE,   c) << s);break;
        case SCIDB_LE_NAME_ARITY_MISMATCH:  fail(USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX,SCIDB_LE_NAME_ARITY_MISMATCH, c) << s);break;
        default                          :  fail(USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX,e,                            c) << s);break;
    }
}

/****************************************************************************/

/**
 *  Return the path to the AFL 'prelude', a special module of macros that ship
 *  with, and that the user percevies as being built into, the SciDB system.
 */
string getPreludePath()
{
    return Config::getInstance()->getOption<string>(CONFIG_INSTALL_ROOT) + "/lib/scidb/modules/prelude.txt";
}

/**
 *  Read the contents of the given text file into a string and return it.
 */
string read(const string& path)
{
    ifstream f(path.c_str());                            // Open for reading
    string   s((istreambuf_iterator<char>(f)),           // Copy contents to
                istreambuf_iterator<char>());            //  the string 's'

    if (f.fail())                                        // Failed to read?
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_PLUGIN_MGR,SCIDB_LE_FILE_READ_ERROR) << path;
    }

    return s;                                            // The file contents
}

/**
 *  Parse and translate the module statement 'text', and install the resulting
 *  bindings in the currently loaded module, where other queries can then find
 *  them.
 */
void load(const string& text)
{
    Module m(Module::write);                             // Lock for writing
    QueryPtr emptyQuery;
    Driver d(text,emptyQuery);                           // Create the driver
    Node*  n(d.process(aflModule));                      // Parse and desugar

    m.load(d,n);                                         // Install the module
}

/****************************************************************************/
}}}
/****************************************************************************/

#include <query/Query.h>

/****************************************************************************/
namespace scidb {
/****************************************************************************/

using namespace parser;

/**
 *  Parse and translate the expression 'text'.
 */
LEPtr parseExpression(const string& text)
{
    Module m(Module::read);                              // Lock for reading
    QueryPtr emptyQuery;
    Driver d(text, emptyQuery);                          // Create the driver
    Node*  n(d.process(aflExpression));                  // Parse and desugar

    return d.translateExp(n);                            // Return translation
}

/**
 *  Parse and translate the given query, which is specified in either 'AFL' or
 *  'AQL' syntax.
 */
LQPtr parseStatement(const QueryPtr& query,bool afl)
{
    Module m(Module::read);                              // Lock for reading
    Driver d(query->queryString, query);                 // Create the driver
    Node*  n(d.process(afl ? aflStatement:aqlStatement));// Parse and desugar

    return d.translatePlan(n);                           // Return translation
}

/**
 *  Parse and translate the prelude module.
 */
void loadPrelude()
{
    load(read(getPreludePath()));                        // Load the prelude
}

/**
 *  Parse and translate the given user module, after concatenating it onto the
 *  prelude module.
 */
void loadModule(const string& module)
{
    string p(read(getPreludePath()));                    // Read the prelude
    string m(read(module));                              // Read user module

    try                                                  // May fail to load
    {
        load(p + m);                                     // ...concat and load
    }
    catch (UserException& e)
    {
        load(p);                                         // ...load prelude
        e.raise();                                       // ...rethrow error
    }
}

/****************************************************************************/
}
/****************************************************************************/
