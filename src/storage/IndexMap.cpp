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

/*
 * IndexMap.cpp
 *
 * @author Steve Fridella
 */

#include <storage/IndexMap.h>
#include <array/AddressMeta.h>

namespace scidb {

template<class KeyMeta>
typename IndexMap<KeyMeta>::Iterator& BasicIndexMap<KeyMeta>::BasicIterator::operator++()
{
    ++_it;
    return *this;
}

template<class KeyMeta>
void BasicIndexMap<KeyMeta>::clear()
{
    _inMemoryMap.clear();
}

template<class KeyMeta>
std::shared_ptr<typename IndexMap<KeyMeta>::Iterator> BasicIndexMap<KeyMeta>::find(Key const* key)
{
    std::shared_ptr<BasicIterator> basicIt =
        std::make_shared<BasicIterator>(_inMemoryMap.find(key), _inMemoryMap);
    return basicIt;
}

template<class KeyMeta>
std::shared_ptr<typename IndexMap<KeyMeta>::Iterator> BasicIndexMap<KeyMeta>::begin()
{
    std::shared_ptr<BasicIterator> basicIt =
        std::make_shared<BasicIterator>(_inMemoryMap.begin(), _inMemoryMap);
    return basicIt;
}

template<class KeyMeta>
std::shared_ptr<typename IndexMap<KeyMeta>::Iterator>
BasicIndexMap<KeyMeta>::leastUpper(Key const* key)
{
    std::shared_ptr<BasicIterator> basicIt =
        std::make_shared<BasicIterator>(_inMemoryMap.lower_bound(key), _inMemoryMap);
    return basicIt;
}

template<class KeyMeta>
bool BasicIndexMap<KeyMeta>::insert(Key const* key,
                                    BufferMgr::BufferHandle& value,
                                    const ChunkAuxMeta& auxMeta)
{
    bool success = false;

    /* Create a new BasicEntry and try to insert it
     */
    std::shared_ptr<BasicEntry> entry;
    std::pair<Key const*, std::shared_ptr<BasicEntry>> mapentry;

    size_t sizeKey = _keyMeta.keySize(key);

    Key* entrykey = reinterpret_cast<Key*>(arena::newVector<char>(*_arena, sizeKey));

    ASSERT_EXCEPTION(entrykey || sizeKey == 0,
                     "In BasicIndexMap::insert(), memory allocation failed.");

    memcpy(reinterpret_cast<char*>(entrykey), reinterpret_cast<char const*>(key), sizeKey);

    entry = std::make_shared<BasicEntry>(entrykey, value, auxMeta, _arena);
    mapentry = std::make_pair(entry->getKey(), entry);

    std::pair<typename EntryMap::iterator, bool> insertResult;

    insertResult = _inMemoryMap.insert(mapentry);
    success = insertResult.second;

    return success;
}

template<class KeyMeta>
BufferMgr::BufferHandle BasicIndexMap<KeyMeta>::update(Key const* key,
                                                       BufferMgr::BufferHandle& value,
                                                       const ChunkAuxMeta& auxMeta)
{
    BufferMgr::BufferHandle retVal;

    /* Create a new BasicEntry and try to insert it
     */
    std::shared_ptr<BasicEntry> entry;
    std::pair<Key const*, std::shared_ptr<BasicEntry>> mapentry;

    size_t sizeKey = _keyMeta.keySize(key);

    Key* entrykey = reinterpret_cast<Key*>(arena::newVector<char>(*_arena, sizeKey));

    ASSERT_EXCEPTION(entrykey || sizeKey == 0,
                     "In BasicIndexMap::update(), memory allocation failed.");

    memcpy(reinterpret_cast<char*>(entrykey), reinterpret_cast<char const*>(key), sizeKey);

    entry = std::make_shared<BasicEntry>(entrykey, value, auxMeta, _arena);
    mapentry = std::make_pair(entry->getKey(), entry);

    std::pair<typename EntryMap::iterator, bool> insertResult;

    insertResult = _inMemoryMap.insert(mapentry);

    if (!insertResult.second) {
        /* Insert failed due to existing key so save the existing
           value, and update the entry with new value/auxiliary chunk metadata.
        */
        BufferMgr::BufferHandle nullhandle;
        ChunkAuxMeta empty;
        entry->update(nullhandle, empty);

        BasicEntry* target = insertResult.first->second.get();

        retVal = target->getBufHandle();
        target->update(value, auxMeta);
    }

    return retVal;
}

template<class KeyMeta>
void BasicIndexMap<KeyMeta>::erase(BaseIterator& it)
{
    BasicIterator* basicIt = dynamic_cast<BasicIterator*>(&it);
    SCIDB_ASSERT(basicIt);
    _inMemoryMap.erase(basicIt->_it);
}

template<class KeyMeta>
void BasicIndexMap<KeyMeta>::rollbackVersion(size_t version)
{
    typename EntryMap::iterator it = _inMemoryMap.begin();
    while (it != _inMemoryMap.end()) {
        typename EntryMap::iterator toRemove = it;
        ++it;
        if (_keyMeta.keyVersion(it->first) == version) {
            _inMemoryMap.erase(toRemove);
        }
    }
}

template class BasicIndexMap<MemAddressMeta>;
template class BasicIndexMap<DbAddressMeta>;
}  // namespace scidb
