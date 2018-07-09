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

    template<Handedness WHICH>
    size_t computeArrayOverhead(shared_ptr<Array> &input, shared_ptr<Query>& query, Settings const& settings)
    {
        size_t tupleOverhead = JoinHashTable::computeTupleOverhead(makeTupledSchema<WHICH> (settings, query).getAttributes(true));
        size_t totalCount = 0;
        shared_ptr<ConstArrayIterator> aiter(input->getConstIterator(input->getArrayDesc().getAttributes().size()-1));
        while(!aiter->end())
        {
            totalCount += aiter->getChunk().count();
            ++(*aiter);
        }
        return totalCount * tupleOverhead;
    }

    template<Handedness WHICH>
    size_t globalComputeArrayOverhead(shared_ptr<Array> &input, shared_ptr<Query>& query, Settings const& settings)
    {
        size_t overhead =  computeArrayOverhead<WHICH>(input, query, settings);
        size_t const nInstances = query->getInstancesCount();
        InstanceID myId = query->getInstanceID();
        std::shared_ptr<SharedBuffer> buf(new MemoryBuffer(NULL, sizeof(size_t)));
        *((size_t*) buf->getWriteData()) = overhead;
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
               size_t otherInstanceSize = *((size_t*) buf->getWriteData());
               overhead += otherInstanceSize;
           }
        }
        return overhead;
    }

    /**
     * If all nodes call this with true - return true.
     * Otherwise, return false.
     */
    bool agreeOnBoolean(bool value, shared_ptr<Query>& query)
    {
        std::shared_ptr<SharedBuffer> buf(new MemoryBuffer(NULL, sizeof(bool)));
        InstanceID myId = query->getInstanceID();
        *((bool*) buf->getWriteData()) = value;
        for(InstanceID i=0; i<query->getInstancesCount(); i++)
        {
            if(i != myId)
            {
                BufSend(i, buf, query);
            }
        }
        for(InstanceID i=0; i<query->getInstancesCount(); i++)
        {
            if(i != myId)
            {
                buf = BufReceive(i,query);
                bool otherInstanceVal = *((bool*) buf->getWriteData());
                value = value && otherInstanceVal;
            }
        }
        return value;
    }

    struct PreScanResult
    {
        bool finishedLeft;
        bool finishedRight;
        size_t leftSizeEstimate;
        size_t rightSizeEstimate;
        PreScanResult():
            finishedLeft(false),
            finishedRight(false),
            leftSizeEstimate(0),
            rightSizeEstimate(0)
        {}
    };

    PreScanResult localPreScan(vector< shared_ptr< Array> >& inputArrays, shared_ptr<Query> const& query, Settings const& settings)
    {
        LOG4CXX_DEBUG(logger, "EJ starting local prescan");
        if(inputArrays[0]->getSupportedAccess() == Array::SINGLE_PASS)
        {
            LOG4CXX_DEBUG(logger, "EJ ensuring left random access");
            inputArrays[0] = ensureRandomAccess(inputArrays[0], query);
        }
        if(inputArrays[1]->getSupportedAccess() == Array::SINGLE_PASS)
        {
            LOG4CXX_DEBUG(logger, "EJ ensuring right random access");
            inputArrays[1] = ensureRandomAccess(inputArrays[1], query); //TODO: well, after this nasty thing we can know the exact size
        }
        ArrayDesc const& leftDesc  = inputArrays[0]->getArrayDesc();
        ArrayDesc const& rightDesc = inputArrays[1]->getArrayDesc();
        size_t leftCellSize  = JoinHashTable::computeTupleOverhead(makeTupledSchema<LEFT> (settings, query).getAttributes(true));
        size_t rightCellSize = JoinHashTable::computeTupleOverhead(makeTupledSchema<LEFT> (settings, query).getAttributes(true));
        shared_ptr<ConstArrayIterator> laiter = inputArrays[0]->getConstIterator(leftDesc.getAttributes().size()-1);
        shared_ptr<ConstArrayIterator> raiter = inputArrays[1]->getConstIterator(rightDesc.getAttributes().size()-1);
        size_t leftSize =0, rightSize =0;
        size_t const threshold = settings.getHashJoinThreshold();
        while(leftSize < threshold && rightSize < threshold && !laiter->end() && !raiter->end())
        {
            leftSize  += laiter->getChunk().count() * leftCellSize;
            rightSize += raiter->getChunk().count() * rightCellSize;
            ++(*laiter);
            ++(*raiter);
        }
        if(laiter->end()) //make sure we've scanned at least the same size from both (in case the chunks are differently sized)
        {
            while(!raiter->end() && rightSize<leftSize)
            {
                rightSize += raiter->getChunk().count() * rightCellSize;
                ++(*raiter);
            }
        }
        else if(raiter->end())
        {
            while(!laiter->end() && leftSize<rightSize)
            {
                leftSize += laiter->getChunk().count() * leftCellSize;
                ++(*laiter);
            }
        }
        PreScanResult result;
        if(laiter->end())
        {
            result.finishedLeft=true;
        }
        if(raiter->end())
        {
            result.finishedRight=true;
        }
        result.leftSizeEstimate =leftSize;
        result.rightSizeEstimate=rightSize;
        LOG4CXX_DEBUG(logger, "EJ prescan complete left cell overhead "<<leftCellSize<<" right cell overhead "<<rightCellSize
                              <<" leftFinished "<<result.finishedLeft<<" rightFinished "<< result.finishedRight
                              <<" leftSize "<<result.leftSizeEstimate<<" rightSize "<<result.rightSizeEstimate);
        return result;
    }

    void globalPreScan(vector< shared_ptr< Array> >& inputArrays, shared_ptr<Query>& query, Settings const& settings,
                       size_t& leftFinished, size_t& rightFinished, size_t& leftSizeEst, size_t& rightSizeEst)
    {
        leftFinished = 0;
        rightFinished = 0;
        leftSizeEst = 0;
        rightSizeEst = 0;
        PreScanResult localResult = localPreScan(inputArrays, query, settings);
        if(localResult.finishedLeft)
        {
            leftFinished++;
        }
        if(localResult.finishedRight)
        {
            rightFinished++;
        }
        leftSizeEst+=localResult.leftSizeEstimate;
        rightSizeEst+=localResult.rightSizeEstimate;
        shared_ptr<SharedBuffer> buf(new MemoryBuffer(NULL, sizeof(PreScanResult)));
        InstanceID myId = query->getInstanceID();
        *((PreScanResult*) buf->getWriteData()) = localResult;
        for(InstanceID i=0; i<query->getInstancesCount(); i++)
        {
            if(i != myId)
            {
                BufSend(i, buf, query);
            }
        }
        for(InstanceID i=0; i<query->getInstancesCount(); i++)
        {
            if(i != myId)
            {
                buf = BufReceive(i,query);
                PreScanResult otherInstanceResult = *((PreScanResult*) buf->getWriteData());
                if(otherInstanceResult.finishedLeft)
                {
                    leftFinished++;
                }
                if(otherInstanceResult.finishedRight)
                {
                    rightFinished++;
                }
                leftSizeEst+=otherInstanceResult.leftSizeEstimate;
                rightSizeEst+=otherInstanceResult.rightSizeEstimate;
            }
        }
    }

    Settings::algorithm pickAlgorithm(vector< shared_ptr< Array> >& inputArrays, shared_ptr<Query>& query, Settings const& settings)
    {
        if(settings.algorithmSet()) //user override
        {
            return settings.getAlgorithm();
        }
        size_t const nInstances = query->getInstancesCount();
        size_t const hashJoinThreshold = settings.getHashJoinThreshold();
        bool leftMaterialized = agreeOnBoolean(inputArrays[0]->isMaterialized(), query);
        size_t leftOverhead  = leftMaterialized ? globalComputeArrayOverhead<LEFT>(inputArrays[0], query, settings) : -1;
        LOG4CXX_DEBUG(logger, "EJ left materialized "<<leftMaterialized<< " overhead "<<leftOverhead);
        if(leftMaterialized && leftOverhead < hashJoinThreshold && settings.isLeftOuter() == false)
        {
            return Settings::HASH_REPLICATE_LEFT;
        }
        bool rightMaterialized = agreeOnBoolean(inputArrays[1]->isMaterialized(), query);
        size_t rightOverhead = rightMaterialized ? globalComputeArrayOverhead<RIGHT>(inputArrays[1], query, settings) : -1;
        LOG4CXX_DEBUG(logger, "EJ right materialized "<<rightMaterialized<< " overhead "<<rightOverhead);
        if(rightMaterialized && rightOverhead < hashJoinThreshold && settings.isRightOuter() == false)
        {
            return Settings::HASH_REPLICATE_RIGHT;
        }
        if(leftMaterialized && rightMaterialized)
        {
            return leftOverhead < rightOverhead ? Settings::MERGE_LEFT_FIRST : Settings::MERGE_RIGHT_FIRST;
        }
        size_t leftArraysFinished =0;
        size_t rightArraysFinished=0;
        size_t leftOverheadEst = 0;
        size_t rightOverheadEst =0;
        globalPreScan(inputArrays, query, settings, leftArraysFinished, rightArraysFinished, leftOverheadEst, rightOverheadEst);
        LOG4CXX_DEBUG(logger, "EJ global prescan complete leftFinished "<<leftArraysFinished<<" rightFinished "<< rightArraysFinished<<" leftOverhead "<<leftOverheadEst<<
                      " rightOverhead "<<rightOverheadEst);
        if(leftArraysFinished == nInstances && leftOverheadEst < hashJoinThreshold && settings.isLeftOuter() == false)
        {
            return Settings::HASH_REPLICATE_LEFT;
        }
        if(rightArraysFinished == nInstances && rightOverheadEst < hashJoinThreshold && settings.isRightOuter() == false)
        {
            return Settings::HASH_REPLICATE_RIGHT;
        }
        //~~~ I dunno, Richard Parker, what do you think? Try to start with the thing that was smaller on most instances
        return leftArraysFinished < rightArraysFinished ? Settings::MERGE_RIGHT_FIRST : Settings::MERGE_LEFT_FIRST;
    }

    template <Handedness WHICH, ReadArrayType ARRAY_TYPE>
    void readIntoHashTable(shared_ptr<Array> & array, JoinHashTable& table, Settings const& settings, ChunkFilter<WHICH>* chunkFilterToPopulate = NULL)
    {
        if ((WHICH == LEFT && settings.isLeftOuter()) || (WHICH == RIGHT && settings.isRightOuter()))
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION)<<"internal inconsistency";
        }
        ArrayReader<WHICH, ARRAY_TYPE> reader(array, settings);
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
        reader.logStats();
    }

    template <Handedness WHICH_IS_IN_TABLE, ReadArrayType ARRAY_TYPE, bool ARRAY_OUTER_JOIN>
    shared_ptr<Array> arrayToTableJoin(shared_ptr<Array>& array, JoinHashTable& table, shared_ptr<Query>& query,
                                       Settings const& settings, ChunkFilter<WHICH_IS_IN_TABLE> const* chunkFilter = NULL)
    {
        //handedness LEFT means the LEFT array is in table so this reads in reverse
        //ARRAY_OUTER_JOIN means the join is outer on the side of the array. The table doesn't support outer joins, so if we were outer
        //on the other side, we wouldn't be in this loop.
        ArrayReader<WHICH_IS_IN_TABLE == LEFT ? RIGHT : LEFT, ARRAY_TYPE, ARRAY_OUTER_JOIN> reader(array, settings, chunkFilter, NULL);
        ArrayWriter<WRITE_OUTPUT> result(settings, query, _schema);
        JoinHashTable::const_iterator iter = table.getIterator();
        size_t const numKeys = settings.getNumKeys();
        while(!reader.end())
        {
            vector<Value const*> const& tuple = reader.getTuple();
            if(ARRAY_OUTER_JOIN && isNullTuple(tuple, numKeys))
            {
                result.writeOuterTuple<WHICH_IS_IN_TABLE == LEFT ? RIGHT : LEFT> (tuple);
                reader.next();
                continue;
            }
            iter.find(tuple);
            if (ARRAY_OUTER_JOIN && iter.end())
            {
                result.writeOuterTuple<WHICH_IS_IN_TABLE == LEFT ? RIGHT : LEFT> (tuple);
            }
            else
            {
                while(!iter.end() && iter.atKeys(tuple))
                {
                    Value const* tablePiece = iter.getTuple();
                    if(WHICH_IS_IN_TABLE == LEFT)
                    {
                        result.writeTuple(tablePiece, tuple);
                    }
                    else
                    {
                        result.writeTuple(tuple, tablePiece);
                    }
                    iter.nextAtHash();
                }
            }
            reader.next();
        }
        reader.logStats();
        return result.finalize();
    }

    template <Handedness WHICH_REPLICATED>
    shared_ptr<Array> replicationHashJoin(vector< shared_ptr< Array> >& inputArrays, shared_ptr<Query> query, Settings const& settings)
    {
        if((WHICH_REPLICATED == LEFT && settings.isLeftOuter()) || (WHICH_REPLICATED == RIGHT && settings.isRightOuter()))
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Internal inconsistency";
        }
        shared_ptr<Array> redistributed = (WHICH_REPLICATED == LEFT ? inputArrays[0] : inputArrays[1]);
        redistributed = redistributeToRandomAccess(redistributed, createDistribution(psReplication), ArrayResPtr(), query, getShared());
        ArenaPtr operatorArena = this->getArena();
        ArenaPtr hashArena(newArena(Options("").resetting(true).threading(false).pagesize(8 * 1024 * 1204).parent(operatorArena)));
        JoinHashTable table(settings, hashArena, WHICH_REPLICATED == LEFT ? settings.getLeftTupleSize() : settings.getRightTupleSize());
        shared_ptr<ChunkFilter<WHICH_REPLICATED> >filter;
        if ((WHICH_REPLICATED == LEFT && !settings.isRightOuter()) || (WHICH_REPLICATED == RIGHT && !settings.isLeftOuter()))
        {
            filter.reset(new ChunkFilter<WHICH_REPLICATED>(settings, inputArrays[0]->getArrayDesc(), inputArrays[1]->getArrayDesc()));
        }
        readIntoHashTable<WHICH_REPLICATED, READ_INPUT> (redistributed, table, settings, filter.get());
        if(settings.isLeftOuter() || settings.isRightOuter())
        {
            return arrayToTableJoin<WHICH_REPLICATED, READ_INPUT, true>( WHICH_REPLICATED == LEFT ? inputArrays[1]: inputArrays[0], table, query, settings, filter.get());
        }
        return arrayToTableJoin<WHICH_REPLICATED, READ_INPUT, false>( WHICH_REPLICATED == LEFT ? inputArrays[1]: inputArrays[0], table, query, settings, filter.get());
    }

    template <Handedness WHICH, bool INCLUDE_NULL_TUPLES = false, bool HASH_NULLS = false>
    shared_ptr<Array> readIntoPreSort(shared_ptr<Array> & inputArray, shared_ptr<Query>& query, Settings const& settings,
                                      ChunkFilter<WHICH>* chunkFilterToGenerate, ChunkFilter<WHICH == LEFT ? RIGHT : LEFT> const* chunkFilterToApply,
                                      BloomFilter* bloomFilterToGenerate,        BloomFilter const* bloomFilterToApply)
    {
        ArrayReader<WHICH, READ_INPUT, INCLUDE_NULL_TUPLES> reader(inputArray, settings, chunkFilterToApply, bloomFilterToApply);
        ArrayWriter<WRITE_TUPLED> writer(settings, query, makeTupledSchema<WHICH>(settings, query));
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
            hashVal.setUint32( JoinHashTable::hashKeys<HASH_NULLS>(tuple, numKeys, hashBuf) % hashMod);
            writer.writeTupleWithHash(tuple, hashVal);
            reader.next();
        }
        reader.logStats();
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
        return sorter.getSortedArray(inputArray, query, getShared(), tcomp);
    }

    template <Handedness WHICH>
    shared_ptr<Array> sortedToPreSg(shared_ptr<Array> & inputArray, shared_ptr<Query>& query, Settings const& settings)
    {
        ArrayWriter<WRITE_SPLIT_ON_HASH> writer(settings, query, makeTupledSchema<WHICH>(settings, query));
        ArrayReader<WHICH, READ_TUPLED> reader(inputArray, settings);
        while(!reader.end())
        {
            writer.writeTuple(reader.getTuple());
            reader.next();
        }
        return writer.finalize();
    }

    template <bool LEFT_OUTER = false, bool RIGHT_OUTER = false>
    shared_ptr<Array> localSortedMergeJoin(shared_ptr<Array>& leftSorted, shared_ptr<Array>& rightSorted, shared_ptr<Query>& query, Settings const& settings)
    {
        ArrayWriter<WRITE_OUTPUT> output(settings, query, _schema);
        vector<AttributeComparator> const& comparators = settings.getKeyComparators();
        size_t const numKeys = settings.getNumKeys();
        ArrayReader<LEFT, READ_SORTED>  leftReader (leftSorted,  settings);
        ArrayReader<RIGHT, READ_SORTED> rightReader(rightSorted, settings);
        vector<Value> previousLeftKeys(numKeys);
        Coordinate previousRightIdx = -1;
        uint32_t previousLeftHash;
        size_t const leftTupleSize = settings.getLeftTupleSize();
        size_t const rightTupleSize = settings.getRightTupleSize();
        while(!leftReader.end() && !rightReader.end())
        {
            vector<Value const*> const* leftTuple  = &(leftReader.getTuple());
            vector<Value const*> const* rightTuple = &(rightReader.getTuple());
            if(LEFT_OUTER && isNullTuple(*leftTuple, numKeys))
            {
                output.writeOuterTuple<LEFT>(*leftTuple);
                leftReader.next();
                continue;
            }
            else if(RIGHT_OUTER && isNullTuple(*rightTuple, numKeys))
            {
                output.writeOuterTuple<RIGHT>(*rightTuple);
                rightReader.next();
                continue;
            }
            uint32_t leftHash = ((*leftTuple)[leftTupleSize])->getUint32();
            uint32_t rightHash =((*rightTuple)[rightTupleSize])->getUint32();
            if(leftHash < rightHash)
            {
                if(LEFT_OUTER)
                {
                    output.writeOuterTuple<LEFT>(*leftTuple);
                }
                leftReader.next();
                continue;
            }
            else if(rightHash < leftHash)
            {
                if(RIGHT_OUTER)
                {
                    output.writeOuterTuple<RIGHT>(*rightTuple);
                }
                rightReader.next();
                continue;
            }
            else if(JoinHashTable::keysLess(*leftTuple, *rightTuple, comparators, numKeys))
            {
                if(LEFT_OUTER)
                {
                    output.writeOuterTuple<LEFT>(*leftTuple);
                }
                leftReader.next();
                continue;
            }
            else if(JoinHashTable::keysLess(*rightTuple, *leftTuple, comparators, numKeys))
            {
                if(RIGHT_OUTER)
                {
                    output.writeOuterTuple<RIGHT>(*rightTuple);
                }
                rightReader.next();
                continue;
            }
            //JOIN TIME!
            bool first = true;
            while(!rightReader.end() && rightHash == leftHash && JoinHashTable::keysEqual(*leftTuple, *rightTuple, numKeys))
            {
                if(first)
                {
                    for(size_t i=0; i<numKeys; ++i)
                    {
                        previousLeftKeys[i] = *((*leftTuple)[i]); //remember the keys from the left tuple
                    }
                    previousRightIdx = rightReader.getIdx(); //remember where the rightReader was in case we need to rewind later
                    first = false;
                }
                output.writeTuple(*leftTuple, *rightTuple);
                rightReader.next();
                if(!rightReader.end())
                {
                    rightTuple = &(rightReader.getTuple());
                    rightHash =((*rightTuple)[rightTupleSize])->getUint32();
                    if(RIGHT_OUTER && isNullTuple(*rightTuple, numKeys))
                    {
                        break; //will be caught up top
                    }
                }
            }
            leftReader.next();
            if(!leftReader.end())  //if the keys in the left reader are repeated, rewind the right reader to where it was
            {
                leftTuple = &(leftReader.getTuple());
                uint32_t nextLeftHash = ((*leftTuple)[leftTupleSize])->getUint32();
                if(leftHash == nextLeftHash && (!LEFT_OUTER || !isNullTuple(*leftTuple, numKeys)) && JoinHashTable::keysEqual( &(previousLeftKeys[0]), *leftTuple, numKeys) && !first)
                {
                    rightReader.setIdx(previousRightIdx);
                    rightTuple = &rightReader.getTuple();
                }
            }
        }
        while(LEFT_OUTER && !leftReader.end())
        {
            output.writeOuterTuple<LEFT> (leftReader.getTuple());
            leftReader.next();
        }
        while(RIGHT_OUTER && !rightReader.end())
        {
            output.writeOuterTuple<RIGHT> (rightReader.getTuple());
            rightReader.next();
        }
        return output.finalize();
    }

    template <Handedness WHICH_FIRST, bool LEFT_OUTER, bool RIGHT_OUTER>
    shared_ptr<Array> globalMergeJoin(vector< shared_ptr< Array> >& inputArrays, shared_ptr<Query> query, Settings const& settings)
    {
        shared_ptr<Array>& first = (WHICH_FIRST == LEFT ? inputArrays[0] : inputArrays[1]);
        shared_ptr<ChunkFilter <WHICH_FIRST> > chunkFilter;
        shared_ptr<BloomFilter> bloomFilter;
        if ((WHICH_FIRST == LEFT && !RIGHT_OUTER) || (WHICH_FIRST == RIGHT && !LEFT_OUTER)) //if second array is not outer, then use first array to filter it!
        {
            chunkFilter.reset(new ChunkFilter<WHICH_FIRST>(settings, inputArrays[0]->getArrayDesc(), inputArrays[1]->getArrayDesc()));
            bloomFilter.reset(new BloomFilter(settings.getBloomFilterSize()));
        }
        bool const KEEP_FIRST_NULL_TUPLES = ((WHICH_FIRST == LEFT && LEFT_OUTER) || (WHICH_FIRST == RIGHT && RIGHT_OUTER));
        bool const HASH_NULLS = (LEFT_OUTER || RIGHT_OUTER); //hashes gotta match
        first = readIntoPreSort<WHICH_FIRST, KEEP_FIRST_NULL_TUPLES, HASH_NULLS>(first, query, settings, chunkFilter.get(), NULL, bloomFilter.get(), NULL);
        first = sortArray(first, query, settings);
        first = sortedToPreSg<WHICH_FIRST>(first, query, settings);
        first = redistributeToRandomAccess(first,createDistribution(psByRow),query->getDefaultArrayResidency(), query, getShared());
        if(chunkFilter.get())
        {
            chunkFilter->globalExchange(query);
            bloomFilter->globalExchange(query);
        }
        Handedness const WHICH_SECOND = (WHICH_FIRST == LEFT ? RIGHT : LEFT);
        bool const KEEP_SECOND_NULL_TUPLES = ((WHICH_SECOND == LEFT && LEFT_OUTER) || (WHICH_SECOND == RIGHT && RIGHT_OUTER));
        shared_ptr<Array>& second = (WHICH_SECOND == LEFT ? inputArrays[0] : inputArrays[1]);
        second = readIntoPreSort<WHICH_SECOND, KEEP_SECOND_NULL_TUPLES, HASH_NULLS>(second, query, settings, NULL, chunkFilter.get(), NULL, bloomFilter.get());
        second = sortArray(second, query, settings);
        second = sortedToPreSg<WHICH_SECOND>(second, query, settings);
        second = redistributeToRandomAccess(second,createDistribution(psByRow),query->getDefaultArrayResidency(), query, getShared());
        size_t const firstOverhead  = computeArrayOverhead<WHICH_FIRST>(first, query, settings);
        size_t const secondOverhead = computeArrayOverhead<WHICH_SECOND>(second, query, settings);
        LOG4CXX_DEBUG(logger, "EJ merge after SG first overhead "<<firstOverhead<<" second overhead "<<secondOverhead);
        //if one of the arrays is small enough, and it's not being outer-joined, we can read it into table! Note: this is a local decision
        if (firstOverhead < settings.getHashJoinThreshold() && ((WHICH_FIRST == LEFT && !LEFT_OUTER) || (WHICH_FIRST == RIGHT && !RIGHT_OUTER)))
        {
            LOG4CXX_DEBUG(logger, "EJ merge rehashing first");
            ArenaPtr operatorArena = this->getArena();
            ArenaPtr hashArena(newArena(Options("").resetting(true).threading(false).pagesize(8 * 1024 * 1204).parent(operatorArena)));
            JoinHashTable table(settings, hashArena, WHICH_FIRST == LEFT ? settings.getLeftTupleSize() : settings.getRightTupleSize());
            readIntoHashTable<WHICH_FIRST, READ_TUPLED> (first, table, settings);
            return arrayToTableJoin<WHICH_FIRST, READ_TUPLED, LEFT_OUTER || RIGHT_OUTER>( second, table, query, settings);
        }
        else if(secondOverhead < settings.getHashJoinThreshold() && ((WHICH_FIRST == RIGHT && !LEFT_OUTER) || (WHICH_FIRST == LEFT && !RIGHT_OUTER)))
        {
            LOG4CXX_DEBUG(logger, "EJ merge rehashing second");
            ArenaPtr operatorArena = this->getArena();
            ArenaPtr hashArena(newArena(Options("").resetting(true).threading(false).pagesize(8 * 1024 * 1204).parent(operatorArena)));
            JoinHashTable table(settings, hashArena, WHICH_FIRST == LEFT ? settings.getRightTupleSize() : settings.getLeftTupleSize());
            readIntoHashTable<WHICH_SECOND, READ_TUPLED> (second, table, settings);
            return arrayToTableJoin<WHICH_SECOND, READ_TUPLED, LEFT_OUTER || RIGHT_OUTER>( first, table, query, settings);
        }
        else
        {
            //Sort em both, sort em out
            LOG4CXX_DEBUG(logger, "EJ merge sorted");
            first = sortArray(first, query, settings);
            second= sortArray(second, query, settings);
            return WHICH_FIRST == LEFT ? localSortedMergeJoin<LEFT_OUTER, RIGHT_OUTER>(first, second, query, settings) :
                                         localSortedMergeJoin<LEFT_OUTER, RIGHT_OUTER>(second, first, query, settings);
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
            if(settings.isLeftOuter() && settings.isRightOuter())
            {
                return globalMergeJoin<LEFT, true, true>(inputArrays, query, settings);
            }
            if(settings.isLeftOuter())
            {
                return globalMergeJoin<LEFT, true, false>(inputArrays, query, settings);
            }
            if(settings.isRightOuter())
            {
                return globalMergeJoin<LEFT, false, true>(inputArrays, query, settings);
            }
            return globalMergeJoin<LEFT, false, false>(inputArrays, query, settings);
        }
        else
        {
            LOG4CXX_DEBUG(logger, "EJ running merge_right_first");
            if(settings.isLeftOuter() && settings.isRightOuter())
            {
                return globalMergeJoin<RIGHT, true, true>(inputArrays, query, settings);
            }
            if(settings.isLeftOuter())
            {
                return globalMergeJoin<RIGHT, true, false>(inputArrays, query, settings);
            }
            if(settings.isRightOuter())
            {
                return globalMergeJoin<RIGHT, false, true>(inputArrays, query, settings);
            }
            return globalMergeJoin<RIGHT, false, false>(inputArrays, query, settings);
        }
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalEquiJoin, "equi_join", "physical_equi_join");
} //namespace scidb
