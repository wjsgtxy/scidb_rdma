/*
**
* @P4COPYRIGHT_SINCE:2008@
*/

// ugh, sigh
#include "PhysicalRemoveVersions.cpp"

namespace scidb
{

/**
 * Overrides of DBArray to customize the onRemoveChunk behavior,
 * primarily to induce a crash.
 */
class DBArrayRemoveTest : public DBArray
{
public:
    DBArrayRemoveTest(ArrayDesc const& desc,
                      const std::shared_ptr<Query>& query)
        : DBArray(desc, query)
    { }

    /**
     * @see DBArray::onRemoveChunk()
     */
    void onRemoveChunk(const DbAddressMeta::Key& dbckey) override
    {
        if (++_count == 10) {
            // After ten chunks, abort, leaving a bunch behind in rocksdb
            // and on disk.
            abort();
        }
    }

private:
    // Count of chunks removed, used in deciding when to abort()
    // during onRemoveChunk.
    size_t _count{0};
};

class TestPhysicalCallbackRemoveVersions : public PhysicalRemoveVersions
{
public:
    TestPhysicalCallbackRemoveVersions(const std::string& logicalName,
                                       const std::string& physicalName,
                                       const Parameters& parameters,
                                       const ArrayDesc& schema)
        : PhysicalRemoveVersions(logicalName, physicalName, parameters, schema)
    { }

    std::shared_ptr<Array> execute(std::vector<std::shared_ptr<Array>>& inputArrays,
                                   std::shared_ptr<Query> query)
    {
        auto target = std::make_shared<DBArrayRemoveTest>(_schema, query);

        /* Remove target versions from storage
         */
        target->removeVersions(query, _schema.getId());
        return std::shared_ptr<Array>();
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(TestPhysicalCallbackRemoveVersions,
                                   "test_callback_remove_versions",
                                   "TestPhysicalCallbackRemoveVersions");

}  // namespace scidb
