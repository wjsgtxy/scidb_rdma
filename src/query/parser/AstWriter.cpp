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
 * @file AstWriter.cpp
 * @author Mike Leibensperger <mjl@paramdigm4.com>
 */

#include "AstWriter.h"
#include <system/Utils.h>

using namespace std;

namespace scidb { namespace parser {

AstWriter::Indent::Indent(AstWriter& aw)
    : _aw(aw)
{
    SCIDB_ASSERT(_aw._io != nullptr);
    SCIDB_ASSERT(_aw._depth >= 0);

    *_aw._io << string(_aw._depth * 2, ' ');
    ++_aw._depth;
}

AstWriter::Indent::~Indent()
{
    --_aw._depth;
    SCIDB_ASSERT(_aw._depth >= 0);
}


AstWriter::AstWriter(Node const* pn)
    : _root(pn)
{ }


void AstWriter::write(ostream& os) const
{
    _io = &os;
    _write(_root);
}


void AstWriter::_write(Node const* pn) const
{
    Indent _(*const_cast<AstWriter*>(this));

    if (pn) {
        *_io << '(' << strtype(pn->getType());
        if (pn->is(creal)) {
            *_io << ' ' << pn->getReal();
        } else if (pn->is(cstring)) {
            *_io << " \"" << pn->getString() << '"';
        } else if (pn->is(cboolean)) {
            *_io << (pn->getBoolean() ? " true" : " false");
        } else if (pn->is(cinteger)) {
            *_io << ' ' << pn->getInteger();
        }
        if (pn->getSize()) {
            for (auto const& child : pn->getList()) {
                *_io << '\n';
                _write(child);
            }
        }
        *_io << ')';
    } else {
        *_io << "(nil)";        // Avoid confusion with "cnull"
    }
}

} } // namespaces
