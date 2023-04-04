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

#include <array/ArrayDesc.h>
#include <query/Query.h>
#include <system/SystemCatalog.h>
#include <util/arena/ScopedArena.h>
#include <util/arena/Vector.h>                           // For mgd::vector
#include "AST.h"                                         // For Node etc.

/****************************************************************************/
namespace scidb { namespace parser {

using scidb::arena::ScopedArena;
using std::vector;
/****************************************************************************/

/**
 *  @brief      Eliminates syntacic sugar by rewriting derived constructs into
 *              the kernel language.
 *
 *  @details    Currently handles:
 *
 *              - load()         => store(input())
 *              - discard(...)   => project(..., inverse: true)
 *              - append(...)    => insert(_append_helper())
 *
 *  @author     jbell@paradigm4.com.
 */
class Desugarer : public Visitor
{
 public:
                           // Construction
                              Desugarer(Factory&,Log&,const QueryPtr&);

 private:                  // From class Visitor
    virtual void              onApplication(Node*&);

 private:
            void              onLoad            (Node*&);
            void              onDiscard         (Node*&);
            void              onAppend          (Node*&);

 private:                  // Implementation
            bool              isApplicationOf   (Node*,name)            const;

 private:                  // Representation
            Factory&          _fac;                      // The node factory
            Log&              _log;                      // The error log
            SystemCatalog&    _cat;                      // The system catalog
            ScopedArena       _mem;                      // The local heap
            QueryPtr  const   _qry;                      // The query context
};

/**
 *
 */
Desugarer::Desugarer(Factory& f,Log& l,const QueryPtr& q)
             : _fac(f),
               _log(l),
               _cat(*SystemCatalog::getInstance()),
               _mem(arena::Options("parser::Desugarer")),
               _qry(q)
{}

/**
 *
 */
void Desugarer::onApplication(Node*& pn)
{
    assert(pn!=0 && pn->is(application));                // Validate arguments

    // One of the "operators" we're rewriting?

    if (isApplicationOf(pn,"Load"))
    {
        onLoad(pn);
    }
    else if  (isApplicationOf(pn,"discard"))
    {
        onDiscard(pn);
    }
    else if  (isApplicationOf(pn,"append"))
    {
        onAppend(pn);
    }

    Visitor::onApplication(pn);                          // Process as before
}

/****************************************************************************/

/**
 *  Translate:
 *
 *      LOAD    (array,<a>)
 *
 *  into:
 *
 *      STORE   (INPUT(array,<a>)),array)
 *
 *  where:
 *
 *      array = names the target array to be stored to
 *
 *      a     = whatever remaining arguments the 'input' operator may happen
 *              to accept.
 */
void Desugarer::onLoad(Node*& pn)
{
    assert(pn->is(application));                         // Validate arguments

    location w(pn->getWhere());                          // The source location
    cnodes   a(pn->getList(applicationArgOperands));     // Operands for input

 /* Is the mandatory target array name missing? If so,  the application will
    certainly fail to compile, but we'll leave it to the 'input' operator to
    report the error...*/

    if (a.empty())                                       // No target array?
    {
        pn = _fac.newApp(w,"Input");                     // ...yes, will fail
    }
    else
    {
        pn = _fac.newApp(w,"Store",                      // store(
             _fac.newApp(w,"Input",a),                   //   input(<a>),
             a.front());                                 //   a[0])
    }
}

/****************************************************************************/

/**
 *  Translate:
 *
 *      discard (array, attr0 [, attr1 ...] )
 *
 *  into:
 *
 *      project (array, attr0 [, attr1 ...] , inverse: true)
 */
void Desugarer::onDiscard(Node*& pn)
{
    assert(pn->is(application));                         // Validate arguments

    location w(pn->getWhere());                          // The source location
    cnodes   a(pn->getList(applicationArgOperands));     // Operands for project

 /* Is the mandatory target array name missing? If so,  the application will
    certainly fail to compile, but we'll leave it to the 'project' operator to
    report the error...*/

    if (a.empty())                                       // No target array?
    {
        pn = _fac.newApp(w,"project");                   // ...yes, will fail
    }
    else
    {
        vector<Node*> operands;
        for (Node* const node : a) {
            operands.push_back(node);
        }
        operands.push_back(_fac.newNode(kwarg, w,
                                        _fac.newString(w, "inverse"),
                                        _fac.newBoolean(w, true)));

        pn = _fac.newApp(w,"project", cnodes(operands));
    }
}

/****************************************************************************/

/**
 *  Translate:
 *
 *    append (src_array, dst_array [, dim_name] )
 *
 *  into:
 *
 *    insert( _append_helper(src_array, dst_array [, dim_name] ),
 *            dst_array, _append:true )
 *
 *  (The _append flag tells insert() it's OK to accept a dataframe.)
 */
void Desugarer::onAppend(Node*& pn)
{
    assert(pn->is(application));                         // Validate arguments

    location w(pn->getWhere());                          // The source location
    cnodes   a(pn->getList(applicationArgOperands));     // Operands for helper

    if (a.size() < 2)                                    // Not enuf args?
    {
        pn = _fac.newApp(w, "append");                   // ...yes, will fail
    }
    else
    {
        Node* helper = _fac.newApp(w, "_append_helper", a);
        Node* kw = _fac.newNode(kwarg, w,
                                _fac.newString(w, "_append"),
                                _fac.newBoolean(w, true));
        pn = _fac.newApp(w, "insert", helper, a[appendDstArrayName], kw);
    }
}

/****************************************************************************/

/**
 *  Return true if the application node 'pn' represents an application of the
 *  operator-macro-function named 'nm'.
 */
bool Desugarer::isApplicationOf(Node* pn,name nm) const
{
    assert(pn->is(application) && nm!=0);                // Validate arguments

    return strcasecmp(nm,pn->get(applicationArgOperator)->get(variableArgName)->getString())==0;
}

/****************************************************************************/

/**
 *  Traverse the abstract syntax tree in search of derived constructs that are
 *  to be rewritten into the kernel syntax.
 */
Node*& desugar(Factory& f,Log& l,Node*& n,const QueryPtr& q)
{
    return Desugarer(f,l,q)(n);                          // Run the desugarer
}

/****************************************************************************/
}}
/****************************************************************************/
