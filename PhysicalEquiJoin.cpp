/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2008-2016 SciDB, Inc.
* All Rights Reserved.
*
* equi_join is a plugin for SciDB, an Open Source Array DBMS maintained
* by Paradigm4. See http://www.paradigm4.com/
*
* equi_join is free software: you can redistribute it and/or modify
* it under the terms of the AFFERO GNU General Public License as published by
* the Free Software Foundation.
*
* equi_join is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the AFFERO GNU General Public License for the complete license terms.
*
* You should have received a copy of the AFFERO GNU General Public License
* along with equi_join.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
*
* END_COPYRIGHT
*/

#include <query/Operator.h>
#include <array/SortArray.h>

#include "ArrayIO.h"
#include "JoinHashTable.h"

namespace scidb
{

using namespace std;
using namespace equi_join;

class PhysicalEquiJoin : public PhysicalOperator
{
public:
    PhysicalEquiJoin(string const& logicalName,
                             string const& physicalName,
                             Parameters const& parameters,
                             ArrayDesc const& schema):
         PhysicalOperator(logicalName, physicalName, parameters, schema)
    {}

    virtual bool changesDistribution(std::vector<ArrayDesc> const&) const
    {
        return true;
    }

    virtual RedistributeContext getOutputDistribution(
               std::vector<RedistributeContext> const& inputDistributions,
               std::vector< ArrayDesc> const& inputSchemas) const
    {
        return RedistributeContext(createDistribution(psUndefined), _schema.getResidency() );
    }

    size_t computeExactArraySize(shared_ptr<Array> &input, shared_ptr<Query>& query)
    {
        size_t result = 0;
        size_t const nAttrs = input->getArrayDesc().getAttributes().size();
        vector<shared_ptr<ConstArrayIterator> >aiters(nAttrs);
        for(size_t i =0; i<nAttrs; ++i)
        {
            aiters[i] = input->getConstIterator(i);
        }
        while(!aiters[0]->end())
        {
            for(size_t i =0; i<nAttrs; ++i)
            {
                result += aiters[i]->getChunk().getSize();
            }
            for(size_t i =0; i<nAttrs; ++i)
            {
                ++(*aiters[i]);
            }
        }
        return result;
    }

    size_t findSizeLowerBound(shared_ptr<Array> &input, shared_ptr<Query>& query, Settings const& settings, size_t const sizeLimit)
    {
        size_t result = 0;
        ArrayDesc const& inputDesc = input->getArrayDesc();
        size_t const nAttrs = inputDesc.getAttributes().size();
        if(input->isMaterialized())
        {
            result = computeExactArraySize(input,query);
            if(result > sizeLimit)
            {
                return sizeLimit;
            }
        }
        else
        {
            size_t cellSize = PhysicalBoundaries::getCellSizeBytes(inputDesc.getAttributes());
            shared_ptr<ConstArrayIterator> iter = input->getConstIterator(nAttrs-1);
            while(!iter->end())
            {
                result += iter->getChunk().count() * cellSize;
                if(result > sizeLimit)
                {
                    return sizeLimit;
                }
                ++(*iter);
            }
        }
        return result;
    }

    size_t globalFindSizeLowerBound(shared_ptr<Array> &input, shared_ptr<Query>& query, Settings const& settings, size_t const sizeLimit)
    {
       size_t localSize = findSizeLowerBound(input,query,settings,sizeLimit);
       size_t const nInstances = query->getInstancesCount();
       InstanceID myId = query->getInstanceID();
       std::shared_ptr<SharedBuffer> buf(new MemoryBuffer(NULL, sizeof(size_t)));
       *((size_t*) buf->getData()) = localSize;
       for(InstanceID i=0; i<nInstances; i++)
       {
           if(i != myId)
           {
               BufSend(i, buf, query);
           }
       }
       for(InstanceID i=0; i<nInstances; i++)
       {
           if(i != myId)
           {
               buf = BufReceive(i,query);
               size_t otherInstanceSize = *((size_t*) buf->getData());
               localSize += otherInstanceSize;
           }
       }
       return localSize;
    }

    Settings::algorithm pickAlgorithm(vector< shared_ptr< Array> >& inputArrays, shared_ptr<Query> query, Settings const& settings)
    {
        if(settings.algorithmSet()) //user override
        {
            return settings.getAlgorithm();
        }
        if(inputArrays[0]->getSupportedAccess() == Array::SINGLE_PASS)
        {
            LOG4CXX_DEBUG(logger, "EJ ensuring left random access");
            inputArrays[0] = ensureRandomAccess(inputArrays[0], query);
        }
        size_t leftSize = globalFindSizeLowerBound(inputArrays[0], query, settings, settings.getHashJoinThreshold());
        LOG4CXX_DEBUG(logger, "EJ left size "<<leftSize);
        if(inputArrays[1]->getSupportedAccess() == Array::SINGLE_PASS)
        {
            LOG4CXX_DEBUG(logger, "EJ ensuring right random access");
            inputArrays[1] = ensureRandomAccess(inputArrays[1], query); //TODO: well, after this nasty thing we can know the exact size
        }
        size_t rightSize = globalFindSizeLowerBound(inputArrays[1], query, settings, settings.getHashJoinThreshold());
        LOG4CXX_DEBUG(logger, "EJ right size "<<rightSize);
        if(leftSize < settings.getHashJoinThreshold())
        {
            return Settings::HASH_REPLICATE_LEFT;
        }
        else if(rightSize < settings.getHashJoinThreshold())
        {
            return Settings::HASH_REPLICATE_RIGHT;
        }
        else if(leftSize < rightSize)
        {
            return Settings::MERGE_LEFT_FIRST;
        }
        else
        {
            return Settings::MERGE_RIGHT_FIRST;
        }
    }

    template <Handedness which, ReadArrayType arrayType>
    void readIntoTable(shared_ptr<Array> & array, JoinHashTable& table, Settings const& settings, ChunkFilter<which>* chunkFilterToPopulate = NULL)
    {
        ArrayReader<which, arrayType> reader(array, settings);
        while(!reader.end())
        {
            vector<Value const*> const& tuple = reader.getTuple();
            if(chunkFilterToPopulate)
            {
                chunkFilterToPopulate->addTuple(tuple);
            }
            table.insert(tuple);
            reader.next();
        }
    }

    template <Handedness which, ReadArrayType arrayType>
    shared_ptr<Array> arrayToTableJoin(shared_ptr<Array>& array, JoinHashTable& table, shared_ptr<Query>& query,
                                       Settings const& settings, ChunkFilter<which> const* chunkFilter = NULL)
    {
        //handedness LEFT means the LEFT array is in table so this reads in reverse
        ArrayReader<which == LEFT ? RIGHT : LEFT, arrayType> reader(array, settings, chunkFilter, NULL);
        ArrayWriter<WRITE_OUTPUT> result(settings, query, _schema);
        JoinHashTable::const_iterator iter = table.getIterator();
        while(!reader.end())
        {
            vector<Value const*> const& tuple = reader.getTuple();
            iter.find(tuple);
            while(!iter.end() && iter.atKeys(tuple))
            {
                Value const* tablePiece = iter.getTuple();
                if(which == LEFT)
                {
                    result.writeTuple(tablePiece, tuple);
                }
                else
                {
                    result.writeTuple(tuple, tablePiece);
                }
                iter.nextAtHash();
            }
            reader.next();
        }
        return result.finalize();
    }

    template <Handedness which>
    shared_ptr<Array> replicationHashJoin(vector< shared_ptr< Array> >& inputArrays, shared_ptr<Query> query, Settings const& settings)
    {
        shared_ptr<Array> redistributed = (which == LEFT ? inputArrays[0] : inputArrays[1]);
        redistributed = redistributeToRandomAccess(redistributed, createDistribution(psReplication), ArrayResPtr(), query);
        ArenaPtr operatorArena = this->getArena();
        ArenaPtr hashArena(newArena(Options("").resetting(true).threading(false).pagesize(8 * 1024 * 1204).parent(operatorArena)));
        JoinHashTable table(settings, hashArena, which == LEFT ? settings.getLeftTupleSize() : settings.getRightTupleSize());
        ChunkFilter <which> filter(settings, inputArrays[0]->getArrayDesc(), inputArrays[1]->getArrayDesc());
        readIntoTable<which, READ_INPUT> (redistributed, table, settings, &filter);
        return arrayToTableJoin<which, READ_INPUT>( which == LEFT ? inputArrays[1]: inputArrays[0], table, query, settings, &filter);
    }

    template <Handedness which>
    shared_ptr<Array> readIntoPreSort(shared_ptr<Array> & inputArray, shared_ptr<Query>& query, Settings const& settings,
                                      ChunkFilter<which>* chunkFilterToGenerate, ChunkFilter<which == LEFT ? RIGHT : LEFT> const* chunkFilterToApply,
                                      BloomFilter* bloomFilterToGenerate,        BloomFilter const* bloomFilterToApply)
    {
        ArrayReader<which, READ_INPUT> reader(inputArray, settings, chunkFilterToApply, bloomFilterToApply);
        ArrayWriter<WRITE_TUPLED> writer(settings, query, makePreTupledSchema<which>(settings, query));
        size_t const hashMod = settings.getNumHashBuckets();
        vector<char> hashBuf(64);
        size_t const numKeys = settings.getNumKeys();
        Value hashVal;
        while(!reader.end())
        {
            vector<Value const*> const& tuple = reader.getTuple();
            if(chunkFilterToGenerate)
            {
                chunkFilterToGenerate->addTuple(tuple);
            }
            if(bloomFilterToGenerate)
            {
                bloomFilterToGenerate->addTuple(tuple, numKeys);
            }
            hashVal.setUint32( JoinHashTable::hashKeys(tuple, numKeys, hashBuf) % hashMod);
            writer.writeTupleWithHash(tuple, hashVal);
            reader.next();
        }
        return writer.finalize();
    }

    shared_ptr<Array> sortArray(shared_ptr<Array> & inputArray, shared_ptr<Query>& query, Settings const& settings)
    {
        SortingAttributeInfos sortingAttributeInfos(settings.getNumKeys() + 1); //plus hash
        sortingAttributeInfos[0].columnNo = inputArray->getArrayDesc().getAttributes(true).size()-1;
        sortingAttributeInfos[0].ascent = true;
        for(size_t k=0; k<settings.getNumKeys(); ++k)
        {
            sortingAttributeInfos[k+1].columnNo = k;
            sortingAttributeInfos[k+1].ascent = true;
        }
        SortArray sorter(inputArray->getArrayDesc(), _arena, false, settings.getChunkSize());
        shared_ptr<TupleComparator> tcomp(make_shared<TupleComparator>(sortingAttributeInfos, inputArray->getArrayDesc()));
        return sorter.getSortedArray(inputArray, query, tcomp);
    }

    template <Handedness which>
    shared_ptr<Array> sortedToPreSg(shared_ptr<Array> & inputArray, shared_ptr<Query>& query, Settings const& settings)
    {
        ArrayWriter<WRITE_SPLIT_ON_HASH> writer(settings, query, makePreTupledSchema<which>(settings, query));
        ArrayReader<which, READ_TUPLED> reader(inputArray, settings);
        while(!reader.end())
        {
            writer.writeTuple(reader.getTuple());
            reader.next();
        }
        return writer.finalize();
    }

    shared_ptr<Array> localSortedMergeJoin(shared_ptr<Array>& leftSorted, shared_ptr<Array>& rightSorted, shared_ptr<Query>& query, Settings const& settings)
    {
        ArrayWriter<WRITE_OUTPUT> output(settings, query, _schema);
        vector<AttributeComparator> const& comparators = settings.getKeyComparators();
        size_t const numKeys = settings.getNumKeys();
        ArrayReader<LEFT, READ_SORTED>  leftCursor (leftSorted,  settings);
        ArrayReader<RIGHT, READ_SORTED> rightCursor(rightSorted, settings);
        if(leftCursor.end() || rightCursor.end())
        {
            return output.finalize();
        }
        vector<Value> previousLeftTuple(numKeys);
        Coordinate previousRightIdx = -1;
        vector<Value const*> const* leftTuple = &(leftCursor.getTuple());
        vector<Value const*> const* rightTuple = &(rightCursor.getTuple());
        size_t leftTupleSize = settings.getLeftTupleSize();
        size_t rightTupleSize = settings.getRightTupleSize();
        while(!leftCursor.end() && !rightCursor.end())
        {
            uint32_t leftHash = ((*leftTuple)[leftTupleSize])->getUint32();
            uint32_t rightHash =((*rightTuple)[rightTupleSize])->getUint32();
            while(rightHash < leftHash && !rightCursor.end())
            {
                rightCursor.next();
                if(!rightCursor.end())
                {
                    rightTuple = &(rightCursor.getTuple());
                    rightHash =((*rightTuple)[rightTupleSize])->getUint32();
                }
            }
            if(rightHash > leftHash)
            {
                leftCursor.next();
                if(!leftCursor.end())
                {
                    leftTuple = &(leftCursor.getTuple());
                }
                continue;
            }
            if(rightCursor.end())
            {
                break;
            }
            while (!rightCursor.end() && rightHash == leftHash && JoinHashTable::keysLess(*rightTuple, *leftTuple, comparators, numKeys) )
            {
                rightCursor.next();
                if(!rightCursor.end())
                {
                    rightTuple = &(rightCursor.getTuple());
                    rightHash =((*rightTuple)[rightTupleSize])->getUint32();
                }
            }
            if(rightCursor.end())
            {
                break;
            }
            if(rightHash > leftHash)
            {
                leftCursor.next();
                if(!leftCursor.end())
                {
                    leftTuple = &(leftCursor.getTuple());
                }
                continue;
            }
            previousRightIdx = rightCursor.getIdx();
            bool first = true;
            while(!rightCursor.end() && rightHash == leftHash && JoinHashTable::keysEqual(*leftTuple, *rightTuple, numKeys))
            {
                if(first)
                {
                    for(size_t i=0; i<numKeys; ++i)
                    {
                        previousLeftTuple[i] = *((*leftTuple)[i]);
                    }
                    first = false;
                }
                output.writeTuple(*leftTuple, *rightTuple);
                rightCursor.next();
                if(!rightCursor.end())
                {
                    rightTuple = &rightCursor.getTuple();
                    rightHash =((*rightTuple)[rightTupleSize])->getUint32();
                }
            }
            leftCursor.next();
            if(!leftCursor.end())
            {
                leftTuple = &leftCursor.getTuple();
                if(JoinHashTable::keysEqual( &(previousLeftTuple[0]), *leftTuple, numKeys) && !first)
                {
                    rightCursor.setIdx(previousRightIdx);
                    rightTuple = &rightCursor.getTuple();
                }
            }
        }
        return output.finalize();
    }

    template <Handedness which>
    shared_ptr<Array> globalMergeJoin(vector< shared_ptr< Array> >& inputArrays, shared_ptr<Query> query, Settings const& settings)
    {
        shared_ptr<Array>& first = (which == LEFT ? inputArrays[0] : inputArrays[1]);
        ChunkFilter <which> chunkFilter(settings, inputArrays[0]->getArrayDesc(), inputArrays[1]->getArrayDesc());
        BloomFilter bloomFilter(settings.getBloomFilterSize());
        first = readIntoPreSort<which>(first, query, settings, &chunkFilter, NULL, &bloomFilter, NULL);
        first = sortArray(first, query, settings);
        first = sortedToPreSg<which>(first, query, settings);
        first = redistributeToRandomAccess(first,createDistribution(psByRow),query->getDefaultArrayResidency(), query, true);
        chunkFilter.globalExchange(query);
        bloomFilter.globalExchange(query);
        shared_ptr<Array>& second = (which == LEFT ? inputArrays[1] : inputArrays[0]);
        second = readIntoPreSort<(which == LEFT ? RIGHT : LEFT)>(second, query, settings, NULL, &chunkFilter, NULL, &bloomFilter);
        second = sortArray(second, query, settings);
        second = sortedToPreSg<(which == LEFT ? RIGHT : LEFT)>(second, query, settings);
        second = redistributeToRandomAccess(second,createDistribution(psByRow),query->getDefaultArrayResidency(), query, true);
        size_t const firstSize  = computeExactArraySize(first, query);
        size_t const secondSize = computeExactArraySize(first, query);
        LOG4CXX_DEBUG(logger, "EJ merge after SG first size "<<firstSize<<" second size "<<secondSize);
        if (firstSize < settings.getHashJoinThreshold())
        {
            LOG4CXX_DEBUG(logger, "EJ merge rehashing first");
            ArenaPtr operatorArena = this->getArena();
            ArenaPtr hashArena(newArena(Options("").resetting(true).threading(false).pagesize(8 * 1024 * 1204).parent(operatorArena)));
            JoinHashTable table(settings, hashArena, which == LEFT ? settings.getLeftTupleSize() : settings.getRightTupleSize());
            readIntoTable<which, READ_TUPLED> (first, table, settings);
            return arrayToTableJoin<which, READ_TUPLED>( second, table, query, settings);
        }
        else if(secondSize < settings.getHashJoinThreshold())
        {
            LOG4CXX_DEBUG(logger, "EJ merge rehashing second");
            ArenaPtr operatorArena = this->getArena();
            ArenaPtr hashArena(newArena(Options("").resetting(true).threading(false).pagesize(8 * 1024 * 1204).parent(operatorArena)));
            JoinHashTable table(settings, hashArena, which == LEFT ? settings.getRightTupleSize() : settings.getLeftTupleSize());
            readIntoTable<(which == LEFT ? RIGHT : LEFT), READ_TUPLED> (second, table, settings);
            return arrayToTableJoin<(which == LEFT ? RIGHT : LEFT), READ_TUPLED>( first, table, query, settings);
        }
        else
        {
            LOG4CXX_DEBUG(logger, "EJ merge sorted");
            first = sortArray(first, query, settings);
            second= sortArray(second, query, settings);
            return which == LEFT ? localSortedMergeJoin(first, second, query, settings) :  localSortedMergeJoin(second, first, query, settings);
        }
    }

    shared_ptr< Array> execute(vector< shared_ptr< Array> >& inputArrays, shared_ptr<Query> query)
    {
        vector<ArrayDesc const*> inputSchemas(2);
        inputSchemas[0] = &inputArrays[0]->getArrayDesc();
        inputSchemas[1] = &inputArrays[1]->getArrayDesc();
        Settings settings(inputSchemas, _parameters, false, query);
        Settings::algorithm algo = pickAlgorithm(inputArrays, query, settings);
        if(algo == Settings::HASH_REPLICATE_LEFT)
        {
            LOG4CXX_DEBUG(logger, "EJ running hash_replicate_left");
            return replicationHashJoin<LEFT>(inputArrays, query, settings);
        }
        else if (algo == Settings::HASH_REPLICATE_RIGHT)
        {
            LOG4CXX_DEBUG(logger, "EJ running hash_replicate_right");
            return replicationHashJoin<RIGHT>(inputArrays, query, settings);
        }
        else if (algo == Settings::MERGE_LEFT_FIRST)
        {
            LOG4CXX_DEBUG(logger, "EJ running merge_left_first");
            return globalMergeJoin<LEFT>(inputArrays, query, settings);
        }
        else
        {
            LOG4CXX_DEBUG(logger, "EJ running merge_right_first");
            return globalMergeJoin<RIGHT>(inputArrays, query, settings);
        }
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalEquiJoin, "equi_join", "physical_equi_join");
} //namespace scidb
