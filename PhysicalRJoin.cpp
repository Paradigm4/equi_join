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

#include <query/Operator.h>
#include <util/Network.h>
#include <array/SortArray.h>

#include "JoinHashTable.h"


namespace scidb
{

using namespace std;
using namespace rjoin;

namespace rjoin
{

class MemArrayAppender : public boost::noncopyable
{
private:
    shared_ptr<Array> _output;
    InstanceID const _myInstanceId;
    size_t const _numAttributes;
    size_t const _leftTupleSize;
    size_t const _numKeys;
    size_t const _chunkSize;
    shared_ptr<Query> _query;
    Settings const& _settings;
    Coordinates _outputPosition;
    vector<shared_ptr<ArrayIterator> >_arrayIterators;
    vector<shared_ptr<ChunkIterator> >_chunkIterators;

public:
    MemArrayAppender(Settings const& settings, shared_ptr<Query> const& query, string const name = ""):
        _output(make_shared<MemArray>(settings.getOutputSchema(query, name), query)),
        _myInstanceId(query->getInstanceID()),
        _numAttributes(settings.getNumOutputAttrs()),
        _leftTupleSize(settings.getLeftTupleSize()),
        _numKeys(settings.getNumKeys()),
        _chunkSize(settings.getChunkSize()),
        _query(query),
        _settings(settings),
        _outputPosition(2 , 0),
        _arrayIterators(_numAttributes, NULL),
        _chunkIterators(_numAttributes, NULL)
    {
        _outputPosition[0] = _myInstanceId;
        _outputPosition[1] = 0;
        for(size_t i =0; i<_numAttributes; ++i)
        {
            _arrayIterators[i] = _output->getIterator(i);
        }
    }

public:
    void writeTuple(vector<Value const*> const& left, Value const* right)
    {
        if( _outputPosition[1] % _chunkSize == 0)
        {
            for(size_t i=0; i<_numAttributes; ++i)
            {
                if(_chunkIterators[i].get())
                {
                    _chunkIterators[i]->flush();
                }
                _chunkIterators[i] = _arrayIterators[i]->newChunk(_outputPosition).getIterator(_query, i == 0 ?
                                                                                ChunkIterator::SEQUENTIAL_WRITE :
                                                                                ChunkIterator::SEQUENTIAL_WRITE | ChunkIterator::NO_EMPTY_CHECK);
            }
        }
        for(size_t i=0; i<_numAttributes; ++i)
        {
            _chunkIterators[i]->setPosition(_outputPosition);
            if(i<_leftTupleSize)
            {
                _chunkIterators[i]->writeItem(*(left[i]));
            }
            else
            {
                Value const* v = right + i - _leftTupleSize + _numKeys;
                _chunkIterators[i]->writeItem(*v);
            }
        }
        ++_outputPosition[1];
    }

    void writeTuple(Value const* left, vector<Value const*> const& right)
    {
        if( _outputPosition[1] % _chunkSize == 0)
        {
            for(size_t i=0; i<_numAttributes; ++i)
            {
                if(_chunkIterators[i].get())
                {
                    _chunkIterators[i]->flush();
                }
                _chunkIterators[i] = _arrayIterators[i]->newChunk(_outputPosition).getIterator(_query, i == 0 ?
                                                                                ChunkIterator::SEQUENTIAL_WRITE :
                                                                                ChunkIterator::SEQUENTIAL_WRITE | ChunkIterator::NO_EMPTY_CHECK);
            }
        }
        for(size_t i=0; i<_numAttributes; ++i)
        {
            _chunkIterators[i]->setPosition(_outputPosition);
            if(i<_leftTupleSize)
            {
                _chunkIterators[i]->writeItem(left[i]);
            }
            else
            {
                _chunkIterators[i]->writeItem(*(right[i - _leftTupleSize + _numKeys ]));
            }
        }
        ++_outputPosition[1];
    }

    void writeTuple(vector<Value const*> const& left, vector<Value const*> const& right)
    {
        if( _outputPosition[1] % _chunkSize == 0)
        {
            for(size_t i=0; i<_numAttributes; ++i)
            {
                if(_chunkIterators[i].get())
                {
                    _chunkIterators[i]->flush();
                }
                _chunkIterators[i] = _arrayIterators[i]->newChunk(_outputPosition).getIterator(_query, i == 0 ?
                                                                                ChunkIterator::SEQUENTIAL_WRITE :
                                                                                ChunkIterator::SEQUENTIAL_WRITE | ChunkIterator::NO_EMPTY_CHECK);
            }
        }
        for(size_t i=0; i<_numAttributes; ++i)
        {
            _chunkIterators[i]->setPosition(_outputPosition);
            if(i<_leftTupleSize)
            {
                _chunkIterators[i]->writeItem(*(left[i]));
            }
            else
            {
                _chunkIterators[i]->writeItem(*(right[i - _leftTupleSize + _numKeys ]));
            }
        }
        ++_outputPosition[1];
    }

    shared_ptr<Array> finalize()
    {
        for(size_t i =0; i<_numAttributes; ++i)
        {
            if(_chunkIterators[i].get())
            {
                _chunkIterators[i]->flush();
            }
            _chunkIterators[i].reset();
            _arrayIterators[i].reset();
        }
        shared_ptr<Array> result = _output;
        _output.reset();
        return result;
    }
};

class PreSortWriter : public boost::noncopyable
{
private:
    shared_ptr<Array> _output;
    InstanceID const _myInstanceId;
    size_t const _numAttributes;
    size_t const _chunkSize;
    shared_ptr<Query> _query;
    Coordinates _outputPosition;
    vector<shared_ptr<ArrayIterator> >_arrayIterators;
    vector<shared_ptr<ChunkIterator> >_chunkIterators;

public:
    PreSortWriter(ArrayDesc const& schema, shared_ptr<Query> const& query, string const name = ""):
        _output(make_shared<MemArray>(schema, query)),
        _myInstanceId(query->getInstanceID()),
        _numAttributes(schema.getAttributes(true).size()),
        _chunkSize(schema.getDimensions()[2].getChunkInterval()),
        _query(query),
        _outputPosition(3 , 0),
        _arrayIterators(_numAttributes, NULL),
        _chunkIterators(_numAttributes, NULL)
    {
        _outputPosition[0] = 0;
        _outputPosition[1] = _myInstanceId;
        _outputPosition[2] = 0;
        for(size_t i =0; i<_numAttributes; ++i)
        {
            _arrayIterators[i] = _output->getIterator(i);
        }
    }

public:
    void writeTuple(vector<Value const*> const& tuple)
    {
        if( _outputPosition[2] % _chunkSize == 0)
        {
            for(size_t i=0; i<_numAttributes; ++i)
            {
                if(_chunkIterators[i].get())
                {
                    _chunkIterators[i]->flush();
                }
                _chunkIterators[i] = _arrayIterators[i]->newChunk(_outputPosition).getIterator(_query, i == 0 ?
                                                                                ChunkIterator::SEQUENTIAL_WRITE :
                                                                                ChunkIterator::SEQUENTIAL_WRITE | ChunkIterator::NO_EMPTY_CHECK);
            }
        }
        for(size_t i=0; i<_numAttributes; ++i)
        {
            _chunkIterators[i]->setPosition(_outputPosition);
            _chunkIterators[i]->writeItem(*(tuple[i]));
        }
        ++_outputPosition[2];
    }

    shared_ptr<Array> finalize()
    {
        for(size_t i =0; i<_numAttributes; ++i)
        {
            if(_chunkIterators[i].get())
            {
                _chunkIterators[i]->flush();
            }
            _chunkIterators[i].reset();
            _arrayIterators[i].reset();
        }
        shared_ptr<Array> result = _output;
        _output.reset();
        return result;
    }
};

class PreSgWriter : public boost::noncopyable
{
private:
    shared_ptr<Array> _output;
    size_t const _numInstances;
    InstanceID const _myInstanceId;
    size_t const _numAttributes;
    size_t const _chunkSize;
    shared_ptr<Query> _query;
    Coordinates _outputPosition;
    vector<shared_ptr<ArrayIterator> >_arrayIterators;
    vector<shared_ptr<ChunkIterator> >_chunkIterators;
    vector <uint32_t> _hashBreaks;
    int64_t _currentBreak;

public:
    PreSgWriter(ArrayDesc const& schema, shared_ptr<Query> const& query, Settings const& settings):
        _output(make_shared<MemArray>(schema, query)),
        _numInstances(query->getInstancesCount()),
        _myInstanceId(query->getInstanceID()),
        _numAttributes(schema.getAttributes(true).size()),
        _chunkSize(schema.getDimensions()[2].getChunkInterval()),
        _query(query),
        _outputPosition(3 , 0),
        _arrayIterators(_numAttributes, NULL),
        _chunkIterators(_numAttributes, NULL),
        _hashBreaks(_numInstances-1, 0),
        _currentBreak(0)
    {
        _outputPosition[0] = 0;
        _outputPosition[1] = _myInstanceId;
        _outputPosition[2] = 0;
        for(size_t i =0; i<_numAttributes; ++i)
        {
            _arrayIterators[i] = _output->getIterator(i);
        }
        uint32_t break_interval = settings.getNumHashBuckets() / _numInstances;
        for(size_t i=0; i<_numInstances-1; ++i)
        {
            _hashBreaks[i] = break_interval * (i+1);
        }
    }

public:
    void writeTuple(vector<Value const*> const& tuple)
    {
        uint32_t hash = tuple[ _numAttributes-1 ]->getUint32();
        while( static_cast<size_t>(_currentBreak) < _numInstances - 1 && hash > _hashBreaks[_currentBreak] )
        {
            ++_currentBreak;
        }
        bool newChunk = false;
        if( _currentBreak != _outputPosition[0] )
        {
            _outputPosition[0] = _currentBreak;
            _outputPosition[2] = 0;
            newChunk =true;
        }
        else if (_outputPosition[2] % _chunkSize == 0)
        {
            newChunk =true;
        }
        if( newChunk )
        {
            for(size_t i=0; i<_numAttributes; ++i)
            {
                if(_chunkIterators[i].get())
                {
                    _chunkIterators[i]->flush();
                }
                _chunkIterators[i] = _arrayIterators[i]->newChunk(_outputPosition).getIterator(_query, i == 0 ?
                                                                                ChunkIterator::SEQUENTIAL_WRITE :
                                                                                ChunkIterator::SEQUENTIAL_WRITE | ChunkIterator::NO_EMPTY_CHECK);
            }
        }
        for(size_t i=0; i<_numAttributes; ++i)
        {
            _chunkIterators[i]->setPosition(_outputPosition);
            _chunkIterators[i]->writeItem(*(tuple[i]));
        }
        ++_outputPosition[2];
    }

    shared_ptr<Array> finalize()
    {
        for(size_t i =0; i<_numAttributes; ++i)
        {
            if(_chunkIterators[i].get())
            {
                _chunkIterators[i]->flush();
            }
            _chunkIterators[i].reset();
            _arrayIterators[i].reset();
        }
        shared_ptr<Array> result = _output;
        _output.reset();
        return result;
    }
};

class SortedTupleCursor
{
private:
    shared_ptr<Array> _input;
    shared_ptr<Query> _query;
    size_t const _nAttrs;
    vector<Value const*> _tuple;
    Coordinate const _chunkSize;
    Coordinate _currChunkIdx;
    vector<shared_ptr<ConstArrayIterator> > _aiters;
    vector<shared_ptr<ConstChunkIterator> > _citers;

public:
    SortedTupleCursor(shared_ptr<Array>& input, shared_ptr<Query>& query):
        _input(input),
        _query(query),
        _nAttrs(input->getArrayDesc().getAttributes(true).size()),
        _tuple(_nAttrs, NULL),
        _chunkSize(input->getArrayDesc().getDimensions()[0].getChunkInterval()),
        _currChunkIdx(0),
        _aiters(_nAttrs, NULL),
        _citers(_nAttrs, NULL)
    {
        Dimensions const& dims = input->getArrayDesc().getDimensions();
        if(dims.size()!=1 || dims[0].getStartMin() != 0)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Internal inconsistency";
        }
        for(size_t i =0; i<_nAttrs; ++i)
        {
            _aiters[i] = _input->getConstIterator(i);
        }
        if(!end())
        {
            _currChunkIdx = _aiters[0]->getPosition()[0];
            for(size_t i=0; i<_nAttrs; ++i)
            {
                _citers[i] = _aiters[i]->getChunk().getConstIterator();
            }
            if(_citers[0]->end())
            {   //should not happen with sorted 1D array
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Internal inconsistency";
            }
        }
    }

    bool end()
    {
        return _aiters[0]->end();
    }

    vector<Value const*> const& getTuple()
    {
        if(end())
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Internal inconsistency";
        }
        for(size_t i =0; i<_nAttrs; ++i)
        {
            _tuple[i] = &(_citers[i]->getItem());
        }
        return _tuple;
    }

    void next()
    {
        for(size_t i =0; i<_nAttrs; ++i)
        {
            ++(*_citers[i]);
        }
        if(_citers[0]->end())
        {
            for(size_t i =0; i<_nAttrs; ++i)
            {
                ++(*_aiters[i]);
            }
            if(_aiters[0]->end())
            {
                return;
            }
            _currChunkIdx = _aiters[0]->getPosition()[0];
            for(size_t i=0; i<_nAttrs; ++i)
            {
                _citers[i] = _aiters[i]->getChunk().getConstIterator();
            }
            if(_citers[0]->end())
            {   //should not happen with sorted 1D array
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Internal inconsistency";
            }
        }
    }

    Coordinate getIdx()
    {
        return _citers[0]->getPosition()[0];
    }

    void setIdx(Coordinate idx)
    {
        if(idx<0)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Internal inconsistency";
        }
        Coordinates pos (1,idx);
        if(!end() && idx % _chunkSize == _currChunkIdx) //easy
        {
            for(size_t i=0; i<_nAttrs; ++i)
            {
                if(!_citers[i]->setPosition(pos))
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Internal inconsistency";
                }
            }
        }
        else
        {
            for(size_t i=0; i<_nAttrs; ++i)
            {
                _citers[i].reset();
                if(!_aiters[i]->setPosition(pos))
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Internal inconsistency";
                }
            }
            _currChunkIdx = _aiters[0]->getPosition()[0];
            for(size_t i=0; i<_nAttrs; ++i)
            {
                _citers[i] = _aiters[i]->getChunk().getConstIterator();
                if(!_citers[i]->setPosition(pos))
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Internal inconsistency";
                }
            }
        }
    }
};

} //namespace rjoin

class PhysicalRJoin : public PhysicalOperator
{
public:
    PhysicalRJoin(string const& logicalName,
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
            LOG4CXX_DEBUG(logger, "RJN ensuring left random access");
            inputArrays[0] = ensureRandomAccess(inputArrays[0], query);
        }
        size_t leftSize = globalFindSizeLowerBound(inputArrays[0], query, settings, settings.getHashJoinThreshold());
        LOG4CXX_DEBUG(logger, "RJN left size "<<leftSize);
        if(inputArrays[1]->getSupportedAccess() == Array::SINGLE_PASS)
        {
            LOG4CXX_DEBUG(logger, "RJN ensuring right random access");
            inputArrays[1] = ensureRandomAccess(inputArrays[1], query); //TODO: well, after this nasty thing we can know the exact size
        }
        size_t rightSize = globalFindSizeLowerBound(inputArrays[1], query, settings, settings.getHashJoinThreshold());
        LOG4CXX_DEBUG(logger, "RJN right size "<<rightSize);
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

    template <Handedness which>
    void readIntoTable(shared_ptr<Array> & array, JoinHashTable& table, Settings const& settings)
    {
        size_t const nAttrs = (which == LEFT ?  settings.getNumLeftAttrs() : settings.getNumRightAttrs());
        size_t const nDims  = (which == LEFT ?  settings.getNumLeftDims()  : settings.getNumRightDims());
        vector<Value const*> tuple ( which == LEFT ? settings.getLeftTupleSize() : settings.getRightTupleSize(), NULL);
        size_t const nKeys = settings.getNumKeys();
        vector<shared_ptr<ConstArrayIterator> > aiters(nAttrs);
        vector<shared_ptr<ConstChunkIterator> > citers(nAttrs);
        vector<Value> dimVal(nDims);
        for(size_t i=0; i<nAttrs; ++i)
        {
            aiters[i] = array->getConstIterator(i);
        }
        while(!aiters[0]->end())
        {
            for(size_t i=0; i<nAttrs; ++i)
            {
                citers[i] = aiters[i]->getChunk().getConstIterator();
            }
            while(!citers[0]->end())
            {
                for(size_t i=0; i<nAttrs; ++i)
                {
                    ssize_t idx = which == LEFT ? settings.mapLeftToTuple(i) : settings.mapRightToTuple(i);
                    if (idx >= 0)
                    {
                        Value const& v = citers[i]->getItem();
                        tuple[ idx ] = &v;
                    }
                }
                Coordinates const& pos = citers[0]->getPosition();
                for(size_t i = 0; i<nDims; ++i)
                {
                    ssize_t idx = which == LEFT ? settings.mapLeftToTuple(i + nAttrs) : settings.mapRightToTuple(i + nAttrs);
                    if(idx >= 0)
                    {
                        dimVal[i].setInt64(pos[i]);
                        tuple [ idx ] = &dimVal[i];
                    }
                }
                bool anyNull = false;
                for(size_t i=0; i<nKeys; ++i)
                {
                    if(tuple[i]->isNull())
                    {
                        anyNull = true;
                        break;
                    }
                }
                if(anyNull)
                {
                    for(size_t i=0; i<nAttrs; ++i)
                    {
                        ++(*citers[i]);
                    }
                    continue;
                }
                table.insert(tuple);
                for(size_t i=0; i<nAttrs; ++i)
                {
                    ++(*citers[i]);
                }
            }
            for(size_t i=0; i<nAttrs; ++i)
            {
                ++(*aiters[i]);
            }
        }
    }

    template <Handedness which>
    shared_ptr<Array> arrayToTableJoin(shared_ptr<Array>& array, JoinHashTable& table, shared_ptr<Query>& query, Settings const& settings)
    {
        //handedness LEFT means the LEFT array is in table so this reads in reverse
        size_t const nAttrs = (which == LEFT ?  settings.getNumRightAttrs() : settings.getNumLeftAttrs());
        size_t const nDims  = (which == LEFT ?  settings.getNumRightDims()  : settings.getNumLeftDims());
        vector<Value const*> tuple ( which == LEFT ? settings.getRightTupleSize() : settings.getLeftTupleSize(), NULL);
        vector<shared_ptr<ConstArrayIterator> > aiters(nAttrs, NULL);
        vector<shared_ptr<ConstChunkIterator> > citers(nAttrs, NULL);
        vector<Value> dimVal(nDims);
        size_t const nKeys = settings.getNumKeys();
        JoinHashTable::const_iterator iter = table.getIterator();
        MemArrayAppender result (settings, query);
        for(size_t i=0; i<nAttrs; ++i)
        {
            aiters[i] = array->getConstIterator(i);
        }
        while(!aiters[0]->end())
        {
            for(size_t i=0; i<nAttrs; ++i)
            {
                citers[i] = aiters[i]->getChunk().getConstIterator();
            }
            while(!citers[0]->end())
            {
                for(size_t i=0; i<nAttrs; ++i)
                {
                    Value const& v = citers[i]->getItem();
                    ssize_t idx = which == LEFT ? settings.mapRightToTuple(i) : settings.mapLeftToTuple(i);
                    if (idx >= 0)
                    {
                       Value const& v = citers[i]->getItem();
                       tuple[ idx ] = &v;
                    }
                }
                Coordinates const& pos = citers[0]->getPosition();
                for(size_t i =0; i<nDims; ++i)
                {
                    ssize_t idx = which == LEFT ? settings.mapRightToTuple(i + nAttrs) : settings.mapLeftToTuple(i + nAttrs);
                    if(idx >= 0)
                    {
                        dimVal[i].setInt64(pos[i]);
                        tuple [ idx ] = &dimVal[i];
                    }
                }
                bool anyNull = false;
                for(size_t i =0; i<nKeys; ++i)
                {
                    if(tuple[i]->isNull())
                    {
                        anyNull = true;
                        break;
                    }
                }
                if(anyNull)
                {
                    for(size_t i=0; i<nAttrs; ++i)
                    {
                        ++(*citers[i]);
                    }
                    continue;
                }
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
                    iter.next();
                }
                for(size_t i=0; i<nAttrs; ++i)
                {
                    ++(*citers[i]);
                }
            }
            for(size_t i=0; i<nAttrs; ++i)
            {
                ++(*aiters[i]);
            }
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
        readIntoTable<which> (redistributed, table, settings);
        return arrayToTableJoin<which>( which == LEFT ? inputArrays[1]: inputArrays[0], table, query, settings);
    }

    template <Handedness which>
    shared_ptr<Array> readIntoPreSort(shared_ptr<Array> & inputArray, shared_ptr<Query>& query, Settings const& settings)
    {
        ArrayDesc schema = settings.getPreSgSchema <which> (query);
        PreSortWriter writer(schema, query);
        size_t const nAttrs = (which == LEFT ?  settings.getNumLeftAttrs() : settings.getNumRightAttrs());
        size_t const nDims  = (which == LEFT ?  settings.getNumLeftDims()  : settings.getNumRightDims());
        size_t const tupleSize = (which == LEFT ? settings.getLeftTupleSize() : settings.getRightTupleSize());
        size_t const numKeys = settings.getNumKeys();
        vector<Value const*> tuple ( tupleSize+1, NULL);
        vector<shared_ptr<ConstArrayIterator> > aiters(nAttrs, NULL);
        vector<shared_ptr<ConstChunkIterator> > citers(nAttrs, NULL);
        vector<Value> dimVal(nDims);
        Value hashVal;
        tuple[tupleSize] = &hashVal;
        size_t const hashMod = settings.getNumHashBuckets();
        for(size_t i=0; i<nAttrs; ++i)
        {
            aiters[i] = inputArray->getConstIterator(i);
        }
        while(!aiters[0]->end())
        {
            for(size_t i=0; i<nAttrs; ++i)
            {
                citers[i] = aiters[i]->getChunk().getConstIterator();
            }
            while(!citers[0]->end())
            {
                for(size_t i=0; i<nAttrs; ++i)
                {
                    Value const& v = citers[i]->getItem();
                    ssize_t idx = which == LEFT ? settings.mapLeftToTuple(i) : settings.mapRightToTuple(i);
                    if (idx >= 0)
                    {
                        Value const& v = citers[i]->getItem();
                        tuple[ idx ] = &v;
                    }
                }
                Coordinates const& pos = citers[0]->getPosition();
                for(size_t i =0; i<nDims; ++i)
                {
                    ssize_t idx = which == LEFT ? settings.mapLeftToTuple(i + nAttrs) : settings.mapRightToTuple(i + nAttrs);
                    if(idx >= 0)
                    {
                        dimVal[i].setInt64(pos[i]);
                        tuple [ idx ] = &dimVal[i];
                    }
                }
                hashVal.setUint32(JoinHashTable::hashKeys(tuple, numKeys) % hashMod);
                writer.writeTuple(tuple);
                for(size_t i=0; i<nAttrs; ++i)
                {
                    ++(*citers[i]);
                }
            }
            for(size_t i=0; i<nAttrs; ++i)
            {
                ++(*aiters[i]);
            }
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
        ArrayDesc schema = settings.getPreSgSchema<which>(query);
        PreSgWriter writer(schema, query, settings);
        size_t const nAttrs = schema.getAttributes().size();
        vector<Value const*> tuple ( nAttrs, NULL);
        vector<shared_ptr<ConstArrayIterator> > aiters(nAttrs, NULL);
        vector<shared_ptr<ConstChunkIterator> > citers(nAttrs, NULL);
        for(size_t i=0; i<nAttrs; ++i)
        {
            aiters[i] = inputArray->getConstIterator(i);
        }
        while(!aiters[0]->end())
        {
            for(size_t i=0; i<nAttrs; ++i)
            {
                citers[i] = aiters[i]->getChunk().getConstIterator();
            }
            while(!citers[0]->end())
            {
                for(size_t i=0; i<nAttrs; ++i)
                {
                    Value const& v = citers[i]->getItem();
                    tuple[ i ] = &v;
                }
                writer.writeTuple(tuple);
                for(size_t i=0; i<nAttrs; ++i)
                {
                    ++(*citers[i]);
                }
            }
            for(size_t i=0; i<nAttrs; ++i)
            {
                ++(*aiters[i]);
            }
        }
        return writer.finalize();
    }

    bool keysLess(vector<Value const*> const& left, vector<Value const*> const& right, vector <AttributeComparator> const& keyComparators, size_t const numKeys)
    {
        for(size_t i =0; i<numKeys; ++i)
        {
           Value const& v1 = *(left[i]);
           Value const& v2 = *(right[i]);
           if(keyComparators[i](v1, v2))
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

    bool keysEqual(vector<Value const*> const& left, vector<Value const*> const& right, size_t const numKeys)
    {
        for(size_t i =0; i<numKeys; ++i)
        {
            Value const& v1 = *(left[i]);
            Value const& v2 = *(right[i]);
            if(v1.size() == v2.size()  &&  memcmp(v1.data(), v2.data(), v1.size()) == 0)
            {
                continue;
            }
            return false;
        }
        return true;
    }

    bool keysEqual(vector<Value const*> const& left, vector<Value> const& right, size_t const numKeys)
    {
        for(size_t i =0; i<numKeys; ++i)
        {
            Value const& v1 = *(left[i]);
            Value const& v2 = (right[i]);
            if(v1.size() == v2.size()  &&  memcmp(v1.data(), v2.data(), v1.size()) == 0)
            {
                continue;
            }
            return false;
        }
        return true;
    }

    shared_ptr<Array> localSortedMergeJoin(shared_ptr<Array>& leftSorted, shared_ptr<Array>& rightSorted, shared_ptr<Query>& query, Settings const& settings)
    {
        MemArrayAppender output(settings, query, _schema.getName());
        vector<AttributeComparator> const& comparators = settings.getKeyComparators();
        size_t const nKeys = settings.getNumKeys();
        SortedTupleCursor leftCursor (leftSorted, query);
        SortedTupleCursor rightCursor(rightSorted, query);
        if(leftCursor.end() || rightCursor.end())
        {
            return output.finalize();
        }
        vector<Value> previousLeftTuple(nKeys);
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
            while (!rightCursor.end() && rightHash == leftHash && keysLess(*rightTuple, *leftTuple, comparators, nKeys) )
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
            while(!rightCursor.end() && rightHash == leftHash && keysEqual(*leftTuple, *rightTuple, nKeys))
            {
                if(first)
                {
                    for(size_t i=0; i<nKeys; ++i)
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
                if(keysEqual(*leftTuple, previousLeftTuple, nKeys) && !first)
                {
                    rightCursor.setIdx(previousRightIdx);
                    rightTuple = &rightCursor.getTuple();
                }
            }
        }
        return output.finalize();
    }

    template <Handedness which>
    shared_ptr<Array> mergeJoin(vector< shared_ptr< Array> >& inputArrays, shared_ptr<Query> query, Settings const& settings)
    {
        shared_ptr<Array>& first = (which == LEFT ? inputArrays[0] : inputArrays[1]);
        first = readIntoPreSort<which>(first, query, settings);
        first = sortArray(first, query, settings);
        first = sortedToPreSg<which>(first, query, settings);
        first = redistributeToRandomAccess(first,createDistribution(psByRow),query->getDefaultArrayResidency(), query, true);
        //        size_t localFirstSize = computeExactArraySize(left, query);
        shared_ptr<Array>& second = (which == LEFT ? inputArrays[1] : inputArrays[0]);
        second = readIntoPreSort<(which == LEFT ? RIGHT : LEFT)>(second, query, settings);
        second = sortArray(second, query, settings);
        second = sortedToPreSg<(which == LEFT ? RIGHT : LEFT)>(second, query, settings);
        second = redistributeToRandomAccess(second,createDistribution(psByRow),query->getDefaultArrayResidency(), query, true);
        //        size_t localSecondSize = computeExactArraySize(second, query);
        //TODO: if first or second is small - read it into a table at this point
        first = sortArray(first, query, settings);
        second= sortArray(second, query, settings);
        return which == LEFT ? localSortedMergeJoin(first, second, query, settings) :  localSortedMergeJoin(second, first, query, settings);
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
            LOG4CXX_DEBUG(logger, "RJN running hash_replicate_left");
            return replicationHashJoin<LEFT>(inputArrays, query, settings);
        }
        else if (algo == Settings::HASH_REPLICATE_RIGHT)
        {
            LOG4CXX_DEBUG(logger, "RJN running hash_replicate_right");
            return replicationHashJoin<RIGHT>(inputArrays, query, settings);
        }
        else if (algo == Settings::MERGE_LEFT_FIRST)
        {
            LOG4CXX_DEBUG(logger, "RJN running merge_left_first");
            return mergeJoin<LEFT>(inputArrays, query, settings);
        }
        else
        {
            LOG4CXX_DEBUG(logger, "RJN running merge_right_first");
            return mergeJoin<RIGHT>(inputArrays, query, settings);
        }
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalRJoin, "rjoin", "physical_rjoin");
} //namespace scidb
