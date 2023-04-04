/*
**
* @P4COPYRIGHT_SINCE:2008@
*/

// ugh, sigh
#include "PhysicalRemove.cpp"

namespace scidb
{

class TestPhysicalAbortRemove : public PhysicalRemove
{
public:
    TestPhysicalAbortRemove(const std::string& logicalName,
                            const std::string& physicalName,
                            const Parameters& parameters,
                            const ArrayDesc& schema)
        : PhysicalRemove(logicalName, physicalName, parameters, schema)
    { }

    void postSingleExecute(std::shared_ptr<Query> query) override
    {
        // Prevent PhysicalRemove (that is, remove()) from removing the coordinator
        // lock by overriding postSingleExecute and aborting the coordinator.
        // When the coordinator restarts, it should clean up the lock via the
        // RemoveErrorHandler logic.
        abort();
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(TestPhysicalAbortRemove,
                                   "test_abort_remove",
                                   "TestPhysicalAbortRemove");

}  // namespace scidb
