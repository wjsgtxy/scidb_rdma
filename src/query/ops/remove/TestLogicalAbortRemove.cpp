/*
**
* @P4COPYRIGHT_SINCE:2008@
*/

// ugh, sigh
#include "LogicalRemove.cpp"

namespace scidb {

class TestLogicalAbortRemove : public LogicalRemove
{
public:
    TestLogicalAbortRemove(const std::string& logicalName,
                           const std::string& alias)
        : LogicalRemove(logicalName, alias)
    {
    }
};

REGISTER_LOGICAL_OPERATOR_FACTORY(TestLogicalAbortRemove, "test_abort_remove");

}  // namespace scidb
