/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2015-2019 SciDB, Inc.
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
 * @file UnitTestStorageEnginePhysical.cpp
 * @brief The logical operator interface for testing the StorageEngine.
 * @author Steve Fridella, Donghui Zhang
 */

#include <array/AddressMeta.h>
#include <array/MemArray.h>
#include <query/PhysicalOperator.h>
#include <query/Query.h>
#include <storage/IndexMgr.h>
#include <system/Config.h>
#include <system/Exceptions.h>

#include <log4cxx/logger.h>

using namespace std;

namespace scidb {
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.unittest"));

class UnitTestStorageEnginePhysical : public PhysicalOperator
{
    void testBufferMgr()
    {
        /* Simple test:  allocate a buffer, write it, unpin it, pin it, read it,
           discard it.
         */
        DataStore::NsId nsid = DataStores::getInstance()->openNamespace("testbasic");
        DataStore::DsId dsid = 1;
        DataStore::DataStoreKey dsk(nsid, dsid);
        BufferMgr::BufferHandle bh =
            BufferMgr::getInstance()->allocateBuffer(dsk,
                                                     1024,
                                                     BufferMgr::Deferred,
                                                     CompressorType::NONE);
        PointerRange<char> base = bh.getRawData();
        uint64_t* beginData = reinterpret_cast<uint64_t*>(base.begin());
        uint64_t* endData = reinterpret_cast<uint64_t*>(base.end());
        size_t sizeData = base.size();

        for (uint64_t* p = beginData; endData - p > (off_t)sizeof(uint64_t); p++) {
            *p = 0xdeadbeef;
        }

        bh.unpinBuffer();

        bh.pinBuffer();

        PointerRange<const char> constbase = bh.getRawConstData();
        uint64_t const* beginCData = reinterpret_cast<uint64_t const*>(constbase.begin());
        uint64_t const* endCData = reinterpret_cast<uint64_t const*>(constbase.end());
        size_t sizeCData = constbase.size();

        ASSERT_EXCEPTION(sizeData == sizeCData, "Data size mismatch in buffer test 1");

        for (uint64_t const* p = beginCData; endCData - p > (off_t)sizeof(uint64_t); p++) {
            ASSERT_EXCEPTION(*p == 0xdeadbeef, "Data corruption in buffer test 1");
        }

        bh.unpinBuffer();

        bh.discardBuffer();

        /* LRU/spilling test:
           1) find out the cache size X.
           2) allocate and write 10 buffers of size X/9 (unpinning as we go).
           3) read back each of the ten buffers.
           4) discard the ten buffers.
         */
        size_t memThresholdMiB =
            Config::getInstance()->getOption<size_t>(CONFIG_MEM_ARRAY_THRESHOLD);
        size_t bufferSize = (memThresholdMiB * MiB) / 9;
        BufferMgr::BufferHandle bhs[10];

        for (int i = 0; i < 10; i++) {
            bhs[i] = BufferMgr::getInstance()->allocateBuffer(dsk,
                                                              bufferSize,
                                                              BufferMgr::Deferred,
                                                              CompressorType::NONE);
            base = bhs[i].getRawData();
            uint64_t* p = (uint64_t*)(base.begin());
            *p = 0xdeadbeef;
            bhs[i].unpinBuffer();
        }

        for (int i = 0; i < 9; i++) {
            bhs[i].pinBuffer();
            constbase = bhs[i].getRawConstData();
            uint64_t* p = (uint64_t*)(constbase.begin());
            ASSERT_EXCEPTION(*p == 0xdeadbeef, "Data corruption in buffer test 2");
            bhs[i].unpinBuffer();
            bhs[i].discardBuffer();
        }

        BufferMgr::getInstance()->discardAllBuffers(dsk);
        // clang-format off
        LOG4CXX_DEBUG(logger, "Buffer manager test successful.");
        // clang-format on
    }

    template<class KeyMeta, size_t LastVersion>
    void testIndexMgr(const std::string& name)
    {
        using TestDiskIndex = DiskIndex<KeyMeta>;
        using TestIndexMgr = IndexMgr<KeyMeta>;

        const size_t numDims = 2;
        KeyMeta addressMeta;
        TestIndexMgr* indexMgr = TestIndexMgr::getInstance();
        uint64_t auxMetaDataInstanceId = 0xdeadbeef;
        ChunkAuxMeta entryAuxMeta;
        entryAuxMeta.primaryInstanceId = auxMetaDataInstanceId;
        DataStore::NsId nsid = DataStores::getInstance()->openNamespace(name);
        DataStore::DsId dsid = 0;
        DataStore::DataStoreKey dsk(nsid, dsid);

        std::shared_ptr<TestDiskIndex> index;
        indexMgr->getIndex(index, dsk, addressMeta);
        assert(index);
        std::shared_ptr<TestDiskIndex> index2;
        indexMgr->findIndex(index2, dsk);
        assert(index2 && index2->getDsk() == index->getDsk());

        // Some input data to test DiskIndex.
        // addr2 < addr3 < addr1
        const size_t sizeMax = 100;
        char addr1Buffer[sizeMax], addr2Buffer[sizeMax], addr3Buffer[sizeMax];
        typename KeyMeta::Key* addr1 = reinterpret_cast<typename KeyMeta::Key*>(addr1Buffer);
        typename KeyMeta::Key* addr2 = reinterpret_cast<typename KeyMeta::Key*>(addr2Buffer);
        typename KeyMeta::Key* addr3 = reinterpret_cast<typename KeyMeta::Key*>(addr3Buffer);
        Coordinates coord1(numDims), coord2(numDims), coord3(numDims);
        for (size_t i = 0; i < numDims; ++i) {
            coord1[i] = 0;
        }
        for (size_t i = 0; i < numDims; ++i) {
            coord2[i] = 10;
        }
        for (size_t i = 0; i < numDims; ++i) {
            coord3[i] = 20;
        }
        AttributeDesc attr;
        attr.setId(2);
        addressMeta.fillKey(addr1, dsk, attr, coord1);
        attr.setId(1);
        addressMeta.fillKey(addr2, dsk, attr, coord2);
        addressMeta.fillKey(addr3, dsk, attr, coord3);
        char const* const value1 = "11111111111111";
        char const* const value2 = "22222222222222222";
        char const* const value3 = "333333333333333333333333";

        // Placeholders.
        Address tmpAddress;

        // Insert all three records.
        typename TestDiskIndex::DiskIndexValue diValue;
        index->allocateMemory(strlen(value1), diValue, CompressorType::NONE);
        strncpy(diValue.memory().begin(), value1, strlen(value1));
        index->insertRecord(addr1,
                            diValue,
                            entryAuxMeta,
                            false,  // unpin
                            false   // don't replace existing record
        );

        index->allocateMemory(strlen(value2), diValue, CompressorType::NONE);
        strncpy(diValue.memory().begin(), value2, strlen(value2));
        index->insertRecord(addr2,
                            diValue,
                            entryAuxMeta,
                            false,  // unpin
                            false   // don't replace existing record
        );

        index->allocateMemory(strlen(value3), diValue, CompressorType::NONE);
        strncpy(diValue.memory().begin(), value3, strlen(value3));
        index->insertRecord(addr3,
                            diValue,
                            entryAuxMeta,
                            false,  // unpin
                            false   // don't replace existing record
        );

        // Verify iteration:
        // begin() = key2
        // ++(begin()) = key3
        // ++(++(begin()) = key1
        // ++(++(++(begin()))) = END.

        typename TestDiskIndex::Iterator iter = index->begin();
        assert(!iter.isEnd());
        assert(addressMeta.keyEqual(addr2, &iter.getKey()));

        ++iter;
        assert(!iter.isEnd());
        assert(addressMeta.keyEqual(addr3, &iter.getKey()));

        ++iter;
        assert(!iter.isEnd());
        assert(addressMeta.keyEqual(addr1, &iter.getKey()));

        ++iter;
        assert(iter.isEnd());

        // delete the second one.
        iter = index->find(addr2);
        assert(!iter.isEnd());
        index->deleteRecord(iter);

        // check indeed the first and third key exist while the second one is not.
        assert(!index->find(addr1).isEnd());
        assert(index->find(addr2).isEnd());
        assert(!index->find(addr3).isEnd());

        // Verify iteration again:
        // (leastUpper(key2)) = key3;
        // ++(leastUpper(key2))) = key1;
        // ++(++(leastUpper(key2)))) = END.

        iter = index->leastUpper(addr2);
        assert(!iter.isEnd());
        assert(addressMeta.keyEqual(addr3, &iter.getKey()));

        ++iter;
        assert(!iter.isEnd());
        assert(addressMeta.keyEqual(addr1, &iter.getKey()));

        ++iter;
        assert(iter.isEnd());

        // pinValue(key3) will get value3
        index->pinValue(addr3, diValue);
        assert(diValue.constMemory().size() == strlen(value3));
        bool retStrncmp = strncmp(diValue.constMemory().begin(), value3, strlen(value3));
        assert(retStrncmp == 0);
        retStrncmp = retStrncmp;  // suppress compiler warning
        diValue.unpin();

        // getAuxMeta
        iter = index->find(addr3);
        entryAuxMeta = iter.getAuxMeta();
        assert(entryAuxMeta.primaryInstanceId == 0xdeadbeef);

        // update the third one to use the second one's value.
        index->allocateMemory(strlen(value2), diValue, CompressorType::NONE);
        strncpy(diValue.memory().begin(), value2, strlen(value2));
        index->insertRecord(addr3,
                            diValue,
                            entryAuxMeta,
                            false,  // do not keep pinned
                            true);  // replace existing value
        index->pinValue(addr3, diValue);
        assert(diValue.constMemory().size() == strlen(value2));
        diValue.unpin();

        // rollbackVersion
        index->rollbackVersion(0, LastVersion);

        // now nothing should exist.
        assert(index->find(addr1).isEnd());
        assert(index->find(addr2).isEnd());
        assert(index->find(addr3).isEnd());

        // close the index.
        indexMgr->closeIndex(index);
        // clang-format off
        LOG4CXX_DEBUG(logger, "DiskIndex test successful.");
        // clang-format on
    }

public:
    UnitTestStorageEnginePhysical(const string& logicalName,
                                  const string& physicalName,
                                  const Parameters& parameters,
                                  const ArrayDesc& schema)
        : PhysicalOperator(logicalName, physicalName, parameters, schema)
    {}

    std::shared_ptr<Array> execute(vector<std::shared_ptr<Array>>& inputArrays,
                                   std::shared_ptr<Query> query)
    {
        testBufferMgr();
        testIndexMgr<MemAddressMeta, 0>("testbasic");
        testIndexMgr<DbAddressMeta,  0>("testrocksdb");
        testIndexMgr<MemAddressMeta, 1>("testbasic");
        testIndexMgr<DbAddressMeta,  1>("testrocksdb");
        return std::shared_ptr<Array>(new MemArray(_schema, query));
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(UnitTestStorageEnginePhysical,
                                   "test_storage_engine",
                                   "UnitTestStorageEnginePhysical");
}  // namespace scidb
