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
 * IndexMap.h
 *
 * @author Steve Fridella
 *
 *
 */

#ifndef INDEXMAP_H_
#define INDEXMAP_H_

#include <array/ChunkAuxMeta.h>
#include <system/Utils.h>
#include <storage/BufferMgr.h>

namespace scidb {

/**
 * IndexMap is an abstract class that describes the interface used by
 * DiskIndex to track keys/chunk auxiliary metadata/buffers.
 *
 * Template parameters:
 *   - class KeyMeta: class which provides the type definition for the record
 *     key, as well as functions to compare, hash, and get the sizes and versions
 *     of keys.
 */
template<class KeyMeta>
class IndexMap
{
public:
    using Key = typename KeyMeta::Key;

    /**
     * Iterator for items in the chunkmap
     */
    class Iterator
    {
    public:
        virtual ~Iterator() {}
        virtual Key const* getKey() = 0;
        virtual BufferMgr::BufferHandle& getBufHandle() = 0;
        virtual const ChunkAuxMeta& getAuxMeta() = 0;
        virtual Iterator& operator++() = 0;
        virtual bool isEnd() const = 0;
        virtual size_t valueSize() const = 0;
    };

    /**
     * Erase in-memory map
     */
    virtual void clear() = 0;

    /**
     * Find an element in the map
     */
    virtual std::shared_ptr<Iterator> find(Key const* key) = 0;

    /**
     * Find least upper bound of an element in the map
     */
    virtual std::shared_ptr<Iterator> leastUpper(Key const* key) = 0;

    /**
     * Return first element in chunk map
     */
    virtual std::shared_ptr<Iterator> begin() = 0;

    /**
     * Insert the entry in the proper place in the map
     * If the key exists, return false and do nothing
     */
    virtual bool insert(Key const* key,
                        BufferMgr::BufferHandle& value,
                        const ChunkAuxMeta& auxMeta) = 0;

    /**
     * Update the entry in the proper place in the map
     *   and return the previous value.
     * If the key does not exist, insert the entry as new
     *   and return a null value.
     */
    virtual BufferMgr::BufferHandle update(Key const* key,
                                           BufferMgr::BufferHandle& value,
                                           const ChunkAuxMeta& auxMeta) = 0;

    /**
     * Erase the indicated entry
     */
    virtual void erase(Iterator& it) = 0;

    /**
     * Erase record with matching key
     */
    virtual void erase(Key const* key)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL,
                               SCIDB_LE_UNREACHABLE_CODE) << __PRETTY_FUNCTION__;
    }

    /**
     * Ensure all records are stable on disk (if applicable)
     */
    virtual void flush() = 0;

    /**
     * Remove all entries with the indicated version #
     * @pre version # must be the latest version
     */
    virtual void rollbackVersion(size_t version) = 0;

    /**
     * Virtual destructor for derived class use
     */
    virtual ~IndexMap() {}
};

/**
 * BasicIndexMap is an implementation of IndexMap which stores the entire
 * map in memory
 *
 * Template parameters:
 *   - class KeyMeta: class which provides the type definition for the record
 *     key, as well as functions to compare, hash, and get the sizes and versions
 *     of keys.
 */
template<class KeyMeta>
class BasicIndexMap : public IndexMap<KeyMeta>
{
protected:
    typedef typename KeyMeta::Key Key;

    /**
     * Entry in the chunkmap
     */
    class BasicEntry
    {
    public:
        Key* getKey() { return _key; }

        BufferMgr::BufferHandle& getBufHandle() { return _value; }

        const ChunkAuxMeta& getAuxMeta() { return _auxMeta; }

        void update(BufferMgr::BufferHandle& value, const ChunkAuxMeta& auxMeta)
        {
            _value = value;
            _auxMeta = auxMeta;
        }

        BasicEntry(Key* key,
                   BufferMgr::BufferHandle& value,
                   const ChunkAuxMeta& auxMeta,
                   arena::ArenaPtr arena)
            : _key(key)
            , _value(value)
            , _auxMeta(auxMeta)
            , _arena(arena)
        {}

        ~BasicEntry()
        {
            if (_key) {
                arena::destroy(*_arena, (char*)_key);
            }
        }

    protected:
        // clang-format off
        Key*                    _key;
        BufferMgr::BufferHandle _value;
        ChunkAuxMeta            _auxMeta;
        arena::ArenaPtr         _arena;
        // clang-format on
    };

    using EntryMap =
        std::map<Key const*, std::shared_ptr<BasicEntry>, typename KeyMeta::KeyLess>;
    using BaseIterator = typename IndexMap<KeyMeta>::Iterator;

    /**
     * Class used to iterate elements of the chunkmap
     */
    class BasicIterator : public BaseIterator
    {
    public:
        /**
         * Access elements
         */
        Key const* getKey() override { return _it->second->getKey(); }

        BufferMgr::BufferHandle& getBufHandle() override { return _it->second->getBufHandle(); }

        const ChunkAuxMeta& getAuxMeta() override { return _it->second->getAuxMeta(); }

        /**
         * Advance to next element
         */
        BaseIterator& operator++() override;

        /**
         * Compare elements
         */
        bool isEnd() const override { return _it == _map.end(); }
        size_t valueSize() const override { return _it->second->getBufHandle().size(); }

        /**
         * Constructor
         */
        BasicIterator(const typename EntryMap::iterator& it, EntryMap const& map)
            : _it(it)
            , _map(map)
        {}

    private:
        // clang-format off
        typename EntryMap::iterator _it;   // map iterator
        EntryMap const&             _map;  // map reference
        // clang-format on
        friend BasicIndexMap;
    };

public:
    /**
     * Constructor/Destructor
     */
    BasicIndexMap(KeyMeta const& keyMeta, arena::ArenaPtr const& arena)
        : _inMemoryMap(keyMeta.keyLess)
        , _keyMeta(keyMeta)
        , _arena(arena)
    {}

    ~BasicIndexMap() {}

    /**
     * Erase in-memory map
     */
    void clear() override;

    /**
     * Find an element in the map
     */
    std::shared_ptr<BaseIterator> find(Key const* key) override;

    /**
     * Find least upper bound of an element in the map
     */
    std::shared_ptr<BaseIterator> leastUpper(Key const* key) override;

    /**
     * Return first element in chunk map
     */
    std::shared_ptr<BaseIterator> begin() override;

    /**
     * Insert the entry in the proper place in the map
     * If the key exists, return false and do nothing
     */
    bool insert(Key const* key,
                BufferMgr::BufferHandle& value,
                const ChunkAuxMeta& auxMeta) override;

    /**
     * Update the entry in the proper place in the map
     *   and return the previous value.
     * If the key does not exist, insert the entry as new
     *   and return a null value.
     */
    BufferMgr::BufferHandle update(Key const* key,
                                   BufferMgr::BufferHandle& value,
                                   const ChunkAuxMeta& auxMeta) override;

    /**
     * Erase the indicated entry
     */
    void erase(BaseIterator& it) override;

    /**
     * No-op for basic map, since map is in-memory
     */
    void flush() override {}

    /**
     * Remove all entries with the indicated version #
     * @pre version # must be the latest version
     */
    void rollbackVersion(size_t version) override;

private:
    // clang-format off
    EntryMap        _inMemoryMap;
    KeyMeta         _keyMeta;
    arena::ArenaPtr _arena;
    // clang-format on
};
}  // namespace scidb
#endif /* INDEXMAP_H_ */
