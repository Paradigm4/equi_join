/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2008-2016 SciDB, Inc.
* All Rights Reserved.
*
* rjoin is a plugin for SciDB, an Open Source Array DBMS maintained
* by Paradigm4. See http://www.paradigm4.com/
*
* rjoin is free software: you can redistribute it and/or modify
* it under the terms of the AFFERO GNU General Public License as published by
* the Free Software Foundation.
*
* rjoin is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the AFFERO GNU General Public License for the complete license terms.
*
* You should have received a copy of the AFFERO GNU General Public License
* along with rjoin.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
*
* END_COPYRIGHT
*/

#ifndef JOINHASHTABLE_H_
#define JOINHASHTABLE_H_

#include <query/Operator.h>
#include <query/AttributeComparator.h>
#include <util/arena/Set.h>
#include <util/arena/Map.h>
#include <util/arena/Vector.h>
#include <util/arena/List.h>
#include <util/Arena.h>
#include <array/SortArray.h>
#include <array/TupleArray.h>
#include "RJoinSettings.h"

namespace scidb
{
namespace rjoin
{

using scidb::arena::Options;
using scidb::arena::ArenaPtr;
using scidb::arena::newArena;
using scidb::SortArray;
using std::shared_ptr;
using std::dynamic_pointer_cast;
using std::vector;
using rjoin::Settings;

class JoinHashTable
{
private:
    //-----------------------------------------------------------------------------
    // MurmurHash3 was written by Austin Appleby, and is placed in the public
    // domain. The author hereby disclaims copyright to this source code.
#define ROT32(x, y) ((x << y) | (x >> (32 - y))) // avoid effort
    static uint32_t murmur3_32(const char *key, uint32_t len, uint32_t const seed = 0x5C1DB123)
    {
        static const uint32_t c1 = 0xcc9e2d51;
        static const uint32_t c2 = 0x1b873593;
        static const uint32_t r1 = 15;
        static const uint32_t r2 = 13;
        static const uint32_t m = 5;
        static const uint32_t n = 0xe6546b64;
        uint32_t hash = seed;
        const int nblocks = len / 4;
        const uint32_t *blocks = (const uint32_t *) key;
        int i;
        uint32_t k;
        for (i = 0; i < nblocks; i++)
        {
            k = blocks[i];
            k *= c1;
            k = ROT32(k, r1);
            k *= c2;
            hash ^= k;
            hash = ROT32(hash, r2) * m + n;
        }
        const uint8_t *tail = (const uint8_t *) (key + nblocks * 4);
        uint32_t k1 = 0;
        switch (len & 3)
        {
        case 3:
            k1 ^= tail[2] << 16;
        case 2:
            k1 ^= tail[1] << 8;
        case 1:
            k1 ^= tail[0];
            k1 *= c1;
            k1 = ROT32(k1, r1);
            k1 *= c2;
            hash ^= k1;
        }
        hash ^= len;
        hash ^= (hash >> 16);
        hash *= 0x85ebca6b;
        hash ^= (hash >> 13);
        hash *= 0xc2b2ae35;
        hash ^= (hash >> 16);
        return hash;
    }
    //End of MurmurHash3 Implementation
    //-----------------------------------------------------------------------------

    struct HashTableEntry
    {
        size_t idx;
        HashTableEntry* next;
        HashTableEntry(size_t const idx, HashTableEntry* next):
            idx(idx), next(next)
        {}
    };

    Settings const&                          _settings;
    ArenaPtr                                 _arena;
    size_t const                             _numAttributes;
    size_t const                             _numKeys;
    vector<AttributeComparator>              _keyComparators;
    size_t const                             _numHashBuckets;
    mgd::vector<HashTableEntry*>             _buckets;
    mgd::vector<Value>                       _values;
    ssize_t                                  _largeValueMemory;
    size_t                                   _numHashes;
    size_t                                   _numGroups;

public:
    JoinHashTable(Settings const& settings, ArenaPtr const& arena, size_t numAttributes):
            _settings(settings),
            _arena(arena),
            _numAttributes(numAttributes),
            _numKeys(_settings.getNumKeys()),
            _keyComparators(_settings.getKeyComparators()),
            _numHashBuckets(_settings.getNumHashBuckets()),
            _buckets(_arena, _numHashBuckets, NULL),
            _values(_arena, 0),
            _largeValueMemory(0),
            _numHashes(0),
            _numGroups(0)
    {}

public:
    static uint32_t hashKeys(vector<Value const*> const& keys, size_t const numKeys)
    {
        size_t totalSize = 0;
        for(size_t i =0; i<numKeys; ++i)
        {
            totalSize += keys[i]->size();
        }
        static std::vector<char> buf (64);
        if(buf.size() < totalSize)
        {
            buf.resize(totalSize);
        }
        char* ch = &buf[0];
        for(size_t i =0; i<numKeys; ++i)
        {
            memcpy(ch, keys[i]->data(), keys[i]->size());
            ch += keys[i]->size();
        }
        return murmur3_32(&buf[0], totalSize);
    }

    bool keysLess(Value const* keys1, vector<Value const*> const& keys2) const
    {
        for(size_t i =0; i<_numKeys; ++i)
        {
           Value const& v1 = keys1[i];
           Value const& v2 = *(keys2[i]);
           if(_keyComparators[i](v1, v2))
           {
               return true;
           }
           else if( v1 == v2 )
           {
               continue;
           }
           else
           {
               return false;
           }
        }
        return false;
    }

    bool keysEqual(Value const* keys1, vector<Value const*>const& keys2) const
    {
        for(size_t i =0; i<_numKeys; ++i)
        {
            Value const& v1 = keys1[i];
            Value const& v2 = *(keys2[i]);
            if(v1.size() == v2.size()  &&  memcmp(v1.data(), v2.data(), v1.size()) == 0)
            {
                continue;
            }
            return false;
        }
        return true;
    }

    bool tuplesEqual(Value const* tuple1, vector<Value const*>const& tuple2) const
    {
        for(size_t i =0; i<_numAttributes; ++i)
        {
            Value const& v1 = tuple1[i];
            Value const& v2 = *(tuple2[i]);
            if(v1.size() == v2.size()  &&  memcmp(v1.data(), v2.data(), v1.size()) == 0)
            {
                continue;
            }
            return false;
        }
        return true;
    }

private:
    size_t addTuple(vector<Value const*> const& tuple)
    {
        size_t idx = _values.size();
        for(size_t i=0; i<_numAttributes; ++i)
        {
            Value const& datum = *(tuple[i]);
            if(datum.isLarge())
            {
                _largeValueMemory += datum.size();
            }
            _values.push_back(datum);
        }
        return idx;
    }

    Value const* getTuple(size_t const idx) const
    {
        return &(_values[idx]);
    }

public:
    void insert(vector<Value const*> const& tuple)
    {
        uint32_t hash = hashKeys(tuple, _numKeys) % _numHashBuckets;
        int newGroup = 1;
        int newHash = 1;
        HashTableEntry** entry = &(_buckets[hash]);
        while( (*entry) != NULL)
        {
            newHash = 0;
            HashTableEntry** next = &((*entry)->next);
            Value const* storedTuple = getTuple( (*entry)->idx);
            if(keysEqual(storedTuple, tuple))
            {
                newGroup = 0; //let's insert equal data in the same order it came in (might come in handy later)
            }
            else if (!keysLess(storedTuple, tuple))
            {
                break;
            }
            entry = next;
        }
        _numGroups += newGroup;
        _numHashes += newHash;
        size_t idx = addTuple(tuple);
        HashTableEntry* newEntry = ((HashTableEntry*)_arena->allocate(sizeof(HashTableEntry)));
        *newEntry = HashTableEntry(idx,*entry);
        *entry = newEntry;
    }

    bool contains(std::vector<Value const*> const& keys, uint32_t& hash) const
    {
        hash = hashKeys(keys, _numKeys) % _numHashBuckets;
        HashTableEntry const* bucket = _buckets[hash];
        while(bucket != NULL)
        {
            if(keysEqual(getTuple(bucket->idx), keys))
            {
                return true;
            }
            else if(! keysLess(getTuple(bucket->idx), keys))
            {
                return false;
            }
            bucket = bucket->next;
        }
        return false;
    }

    /**
     * @return the total amount of bytes used by the structure
     */
    size_t usedBytes() const
    {
        if(_largeValueMemory < 0)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION)<<"inconsistent state size overflow";
        }
        return _arena->allocated() + _largeValueMemory;
    }

    class const_iterator
    {
    private:
        JoinHashTable const* _table;
        HashTableEntry const* _mark;
        uint32_t _markHash;
        uint32_t _currHash;
        HashTableEntry const* _entry;

    public:
        const_iterator(JoinHashTable const* table):
            _table(table),
            _mark(NULL)
        {
            restart();
        }

        void restart()
        {
            _currHash = 0;
            do
            {
                _entry = _table->_buckets[_currHash];
                if(_entry != NULL)
                {
                    break;
                }
                ++_currHash;
            } while(_currHash < _table->_numHashBuckets);
        }

        bool end() const
        {
            return _currHash >= _table->_numHashBuckets;
        }

        void nextAtHash()
        {
            if (end())
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "iterating past end";
            }
            _entry = _entry->next;
            if ( _entry == NULL )
            {
                _currHash = _table->_numHashBuckets; //invalidate
            }
        }

        void next()
        {
            if (end())
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "iterating past end";
            }
            _entry = _entry->next;
            while ( _entry == NULL )
            {
                ++(_currHash);
                if(end())
                {
                    return;
                }
                _entry = _table->_buckets[_currHash];
            }
        }

        uint32_t getCurrentHash() const
        {
            if (end())
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "access past end";
            }
            return _currHash;
        }

        Value const* getTuple() const
        {
            if (end())
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "access past end";
            }
            return &(_table->_values[_entry->idx]);
        }

        bool find(vector<Value const*> const& keys)
        {
            _currHash = _table->hashKeys(keys, _table->_numKeys) % _table->_numHashBuckets;
            _entry = _table->_buckets[_currHash];
            while(_entry != NULL && ! _table->keysEqual(getTuple(), keys))
            {
                if(!_table->keysLess(getTuple(), keys))
                {
                    _entry = NULL;
                    break;
                }
                _entry = _entry->next;
            }
            if(_entry == NULL)
            {
                _currHash = _table->_numHashBuckets; //invalidate
                return false;
            }
            return true;
        }

        bool atKeys(vector<Value const*> const& keys)
        {
            if(end())
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "access past end";
            }
            return _table->keysEqual(getTuple(), keys);
        }

        void mark()
        {
            if(!end())
            {
                _mark = _entry;
                _markHash = _currHash;
            }
            else
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "mark past end";
            }
        }

        void goToMark()
        {
            if(_mark == NULL)
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "visit to null mark";
            }
            _entry = _mark;
            _currHash = _markHash;
        }
    };

    const_iterator getIterator() const
    {
        return const_iterator(this);
    }

    void logStuff()
    {
        LOG4CXX_DEBUG(logger, "RJN hashes "<<_numHashes<<" groups "<<_numGroups<<" large_vals "<<_largeValueMemory<<" total "<<usedBytes());
    }
};

} } //namespace scidb::grouped_aggregate


#endif /* JOINHASHTABLE_H_ */
