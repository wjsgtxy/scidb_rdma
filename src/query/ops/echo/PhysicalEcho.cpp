/*
**
* @P4COPYRIGHT_SINCE:2008@
*/

#include <query/PhysicalOperator.h>

namespace {
  log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.ops.echo"));
}

namespace scidb {

struct PhysicalEcho : PhysicalOperator
{
    PhysicalEcho(std::string const& logicalName,
                 std::string const& physicalName,
                 Parameters const& parameters,
                 ArrayDesc const& schema);
    virtual ~PhysicalEcho();

    std::shared_ptr<Array> execute(std::vector<std::shared_ptr<Array>>& inputArrays,
                                   std::shared_ptr<Query> query) override;
};

PhysicalEcho::PhysicalEcho(std::string const& logicalName,
                           std::string const& physicalName,
                           Parameters const& parameters,
                           ArrayDesc const& schema)
    : PhysicalOperator(logicalName, physicalName, parameters, schema)
{
}

PhysicalEcho::~PhysicalEcho()
{
}

std::shared_ptr<Array> PhysicalEcho::execute(std::vector<std::shared_ptr<Array>>& inputArrays,
                                             std::shared_ptr<Query> query)
{
    SCIDB_ASSERT(inputArrays.empty());
    SCIDB_ASSERT(getParameters().size() == 1);
    SCIDB_ASSERT(query);
    auto message = paramToString(getParameters()[0]);
    LOG4CXX_INFO(logger, message);
    return nullptr;
}

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalEcho,
                                  "echo",
                                  "PhysicalEcho")

}  // namespace scidb
