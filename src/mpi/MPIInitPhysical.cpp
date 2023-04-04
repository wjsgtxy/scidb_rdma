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

///
/// @file MPIInitPhysical.cpp
///
///

#include <array/MemArray.h>
#include <mpi/MPIPhysical.hpp>
#include <mpi/MPISlaveProxy.h>
#include <query/PhysicalOperator.h>

// de-facto standards
#include <log4cxx/logger.h>

using namespace std;

namespace scidb {

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.query.ops.mpi"));

class MPIInitPhysical: public MPIPhysical
{
  public:

    MPIInitPhysical(const string& logicalName, const string& physicalName,
                    const Parameters& parameters, const ArrayDesc& schema)
    : MPIPhysical(logicalName, physicalName, parameters, schema)
    {
    }

    /**
     * Spawn mpi slaves, get their handshakes, tell the slaves to exit.
     * This is used to initialize (and test) the basic MPI functionality.
     * It also has the side-effect of cleaning any MPI-related state left
     * in the filesystem by the previous incarnation (i.e. process) of this instance.
     */
    std::shared_ptr<Array> execute(std::vector< std::shared_ptr<Array> >& inputArrays, std::shared_ptr<Query> query)
    {
        MpiManager::getInstance()->forceInitMpi();

        launchMPISlaves(query, query->getInstancesCount());
        std::shared_ptr<MpiSlaveProxy> slave = _ctx->getSlave(_launchId);
        mpi::Command cmd;
        cmd.setCmd(string("EXIT"));
        slave->sendCommand(cmd, _ctx);
        slave->waitForExit(_ctx); // wait for the slave to disconnect
        unlaunchMPISlaves();
        return std::shared_ptr<Array> (new MemArray(_schema,query));
    }
};
REGISTER_PHYSICAL_OPERATOR_FACTORY(MPIInitPhysical, "mpi_init", "MPIInitPhysical");
}
