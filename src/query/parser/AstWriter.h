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
 * @file AstWriter.h
 * @author Mike Leibensperger <mjl@paramdigm4.com>
 */

#include "AST.h"

namespace scidb { namespace parser {

/**
 * @brief Dump abstract syntax trees for debugging.
 *
 * @note Intentionally not derived from Visitor.  Visitor is
 *       (apparently) intended for selecting and tweaking particular
 *       kinds of nodes, whereas we want to treat all Nodes more or
 *       less the same (that is, dump them).
 */
class AstWriter
{
public:
    explicit AstWriter(Node const* pn);
    void write(std::ostream& io) const;

private:
    void _write(Node const* pn) const;

    Node const* _root { nullptr };
    mutable std::ostream* _io { nullptr };
    mutable int _depth { 0 };

    // RAII for indentation management.
    class Indent
    {
        AstWriter& _aw;
    public:
        Indent(AstWriter& aw);
        ~Indent();
    };

    friend class AstWriter::Indent;
};

inline std::ostream& operator<<(std::ostream& io, AstWriter const& aw)
{
    aw.write(io);
    return io;
}

} } // namespaces
