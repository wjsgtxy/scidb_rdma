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

/*
 * Storage.h
 *
 *  Created on: 06.01.2010
 *      Author: knizhnik@garret.ru
 *      Description: Storage manager interface
 */

#ifndef STORAGE_H_
#define STORAGE_H_

#include <stdlib.h>
#include <exception>
#include <limits>
#include <string>
#include <map>
#include <system/Cluster.h>
#include <array/MemArray.h>
#include <query/Query.h>

namespace scidb
{
    /**
     * An extension of Address that specifies the chunk of a persistent array.
     *
     * Note: we do not use virtual methods here so there is no polymorphism.
     * We do not need virtual methods on these structures, so we opt for better performance.
     * Inheritance is only used to factor out similarity between two structures.
     *
     * Storage addresses have an interesting ordering scheme. They are ordered by
     * AttributeID, Coordinates, ArrayID (reverse).
     *
     * Internally the storage manager keeps all chunks for a given array name in the same subtree.
     * For a given array, you will see this kind of ordering:
     *
     * AttributeID = 0
     *   Coordinates = {0,0}
     *     ArrayID = 1 --> CHUNK (this chunk exists in all versions >= 1)
     *     ArrayID = 0 --> CHUNK (this chunk exists only in version 0)
     *   Coordinates = {0,10}
     *     ArrayID = 2 --> NULL (tombstone)
     *     ArrayID = 0 --> CHUNK (this chunk exists only in versions 0 and 1; there's a tombstone at 2)
     * AttributeID = 1
     *   ...
     *
     * The key methods that implement iteration over an array are findChunk and findNextChunk.
     * For those methods, the address with zero-sized-list coordinates is considered to be the start of
     * the array. In practice, to find the first chunk in the array - create an address with coordinates
     * {} and then find the first chunk greater than it. This is why our comparison function cares about
     * the size of the coordinate list.
     */
    struct StorageAddress: public Address
    {
        /**
         * Array Identifier for the Versioned Array ID wherein this chunk first appeared.
         */
        ArrayID arrId;

        /**
         * Default constructor
         */
        StorageAddress():
            Address(0, Coordinates()),
            arrId(0)
        {}

        /**
         * Constructor
         * @param arrId array identifier
         * @param attId attribute identifier
         * @param coords element coordinates
         */
        StorageAddress(ArrayID arrId, AttributeID attId, Coordinates const& coords):
            Address(attId, coords), arrId(arrId)
        {}

        /**
         * Copy constructor
         * @param addr the object to copy from
         */
        StorageAddress(StorageAddress const& addr):
            Address(addr.attId, addr.coords), arrId(addr.arrId)
        {}

        /**
         * Partial comparison function, used to implement std::map
         * @param other another aorgument of comparison
         * @return true if "this" preceeds "other" in partial order
         */
        inline bool operator < (StorageAddress const& other) const
        {
            if(attId != other.attId)
            {
                return attId < other.attId;
            }
            if (coords.size() != other.coords.size())
            {
                return coords.size() < other.coords.size();
            }
            for (size_t i = 0, n = coords.size(); i < n; i++)
            {
                if (coords[i] != other.coords[i])
                {
                    return coords[i] < other.coords[i];
                }
            }
            if (arrId != other.arrId)
            {
                //note: reverse ordering to keep most-recent versions at the front of the map
                return arrId > other.arrId;
            }
            return false;
        }

        /**
         * Equality comparison
         * @param other another aorgument of comparison
         * @return true if "this" equals to "other"
         */
        inline bool operator == (StorageAddress const& other) const
        {
            if (arrId != other.arrId)
            {
                return false;
            }

            return Address::operator ==( static_cast<Address const&>(other));
        }

        /**
         * Inequality comparison
         * @param other another argument of comparison
         * @return true if "this" not equals to "other"
         */
        inline bool operator != (StorageAddress const& other) const
        {
            return !(*this == other);
        }

        /**
         * Check for same base Addr
         * @param other another argument of comparison
         * @return true if "this" is equal to "other" notwithstanding
         *         different arrId (version)
         */
        inline bool sameBaseAddr(StorageAddress const& other) const
        {
            if(attId != other.attId)
            {
                return false;
            }
            return (coords == other.coords);
        }
    };

    class ListChunkDescriptorsArrayBuilder;
    class ListChunkMapArrayBuilder;
    class PersistentChunk;
    struct ChunkDescriptor;
    class DataStores;

    /**
     * Storage manager interface
     */
    class Storage
    {
      public:
        typedef std::function<void(const ArrayUAID&,
                                     const StorageAddress&,
                                     const PersistentChunk*,
                                     uint64_t,
                                     bool)>    ChunkMapVisitor;
        typedef std::function<void(const ChunkDescriptor&,bool)> ChunkDescriptorVisitor;
        typedef std::map< ArrayID, std::pair<ArrayID, VersionID> > RollbackMap;

      public:
        virtual ~Storage() {}
        /**
         * Open storage manager at specified URL.
         * Format and semantic of storage URL depends in particular implementation of storage manager.
         * For local storage URL specifies path to the description file.
         * Description file has the following format:
         * ----------------------
         * <storage-header-path>
         * <log size limit> <storage-log-path>
         * ...
         * ----------------------
         * @param url implementation dependent database url
         * @param cacheSize chunk cache size: amount of memory in bytes which can be used to cache most frequently used
         */
        virtual void open(const std::string& url, size_t cacheSize) = 0;

        /**
         * Flush all changes to the physical device(s) for the indicated array.  (optionally flush data
         * for all arrays if uaId == INVALID_ARRAY_ID). If power fault or system failure happens when there
         * is some unflushed data, then these changes can be lost
         */
        virtual void flush(ArrayUAID uaId = INVALID_ARRAY_ID) = 0;

        /**
         * Close storage manager
         */
        virtual void close() = 0;

        virtual void onCreateChunk(const ArrayDesc& adesc,
                                   const ChunkDescriptor& desc,
                                   const StorageAddress& addr) {}
    };

    /**
     * Storage factory.
     * By default it points to local storage manager implementation.
     * But it is possible to register any storae manager implementation using setInstance method
     */
    class StorageMan
    {
      public:
        /**
         * Set custom implementaiton of storage manager
         */
        static void setInstance(Storage& storage) {
            instance = &storage;
        }
        /**
         * Get instance of the storage (it is assumed that there can be only one storage in the application)
         */
        static Storage& getInstance() {
            return *instance;
        }
      private:
        static Storage* instance;
    };
}

#endif
