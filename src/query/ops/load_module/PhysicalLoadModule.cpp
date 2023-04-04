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

#include <query/Expression.h>
#include <query/Parser.h>                                // For loadModule
#include <query/PhysicalOperator.h>                              // For PhysicalOperator
#include <query/Query.h>
#include <system/Resources.h>                            // For fileExists


/****************************************************************************/
namespace scidb { namespace {
/****************************************************************************/
using namespace std;                                     // For all of std
/****************************************************************************/

struct PhysicalLoadModule : PhysicalOperator
{
    PhysicalLoadModule(const string& l,const string& p,const Parameters& a,const ArrayDesc& s)
       : PhysicalOperator(l,p,a,s)
    {}

    std::shared_ptr<Array>
    execute(vector<std::shared_ptr<Array> >&,std::shared_ptr<Query> query)
    {
        if (query->isCoordinator())                      // Are we a coordinator?
        {
            string path(paramToString(_parameters[0]));

            // XXX How do we know logical instance zero is this query coordinator?  Bug??
            if (!Resources::getInstance()->fileExists(path,0,query))
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_PLUGIN_MGR,SCIDB_LE_FILE_NOT_FOUND) << path;
            }

            loadModule(path);                            // ...load user module
        }

        return std::shared_ptr<Array>();                      // Nothing to return
    }
};

/****************************************************************************/
} DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalLoadModule,"load_module","impl_load_module")}
/****************************************************************************/
