/*
**
* @P4COPYRIGHT_SINCE:2008@
*/

#include <query/LogicalOperator.h>
#include <query/Query.h>

namespace scidb {

/**
 * @brief The operator: echo().
 *
 * @par Synopsis:
 *   echo( 'text' )
 *
 * @par Summary:
 *   Writes the message 'text' (or some user-supplied string) to
 *   the log file on every instance.
 *
 */
struct LogicalEcho : LogicalOperator
{
    LogicalEcho(const std::string& logicalName,
                const std::string& alias);
    virtual ~LogicalEcho();

    static PlistSpec const* makePlistSpec()
    {
        static PlistSpec argSpec {
            { "", // positionals
              RE(PP(PLACEHOLDER_CONSTANT, TID_STRING))
            }
        };
        return &argSpec;
    }

    ArrayDesc inferSchema(std::vector<ArrayDesc> schemas,
                          std::shared_ptr<Query> query) override;
};


LogicalEcho::LogicalEcho(const std::string& logicalName,
                         const std::string& alias)
    : LogicalOperator(logicalName, alias)
{
}

LogicalEcho::~LogicalEcho()
{
}

ArrayDesc LogicalEcho::inferSchema(std::vector<ArrayDesc> schemas,
                                   std::shared_ptr<Query> query)
{
    SCIDB_ASSERT(schemas.empty());
    SCIDB_ASSERT(_parameters.size() == 1);
    return ddlArrayDesc(query);  // Not DDL, but we don't return an array.
}

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalEcho, "echo")

}  // namespace scidb
