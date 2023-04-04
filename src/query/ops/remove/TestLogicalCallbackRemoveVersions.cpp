/*
**
* @P4COPYRIGHT_SINCE:2008@
*/

// ugh, sigh
#include "LogicalRemoveVersions.cpp"

namespace scidb {

/**
 * @brief The operator: test_callback_remove_versions().
 *
 * @par Synopsis:
 *   @see remove_versions()
 *
 * @par Summary:
 *   @see remove_versions()
 *   One caveat, this operator intentionally crashes all worker
 *   instances in the cluster.
 */
class TestLogicalCallbackRemoveVersions : public LogicalRemoveVersions
{
public:
    TestLogicalCallbackRemoveVersions(const std::string& logicalName,
                                      const std::string& alias)
        : LogicalRemoveVersions(logicalName, alias)
    { }
};

REGISTER_LOGICAL_OPERATOR_FACTORY(TestLogicalCallbackRemoveVersions,
                                  "test_callback_remove_versions");

}  // namespace scidb
