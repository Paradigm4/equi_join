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
#include <util/Network.h>
#include <array/SortArray.h>

#include "JoinHashTable.h"


namespace scidb
{

using namespace std;
using namespace equi_join;

namespace equi_join
{

enum ArrayWriterMode
{
    PRE_SORT,         //first phase:  convert input to tuples and add a hash attribute
    SPLIT_ON_HASH,    //second phase: split sorted tuples into different chunks based on hash - to send around the cluster
    OUTPUT            //final phase:  combine left and right tuples into
};

template<ArrayWriterMode mode>
class ArrayWriter : public boost::noncopyable
{
private:
    shared_ptr<Array>                   _output;
    InstanceID const                    _myInstanceId;
    size_t const                        _numInstances;
    size_t const                        _numAttributes;
    size_t const                        _leftTupleSize;
    size_t const                        _numKeys;
    size_t const                        _chunkSize;
    shared_ptr<Query>                   _query;
    Settings const&                     _settings;
    vector<Value const*>                _tuplePlaceholder;
    Coordinates                         _outputPosition;
    vector<shared_ptr<ArrayIterator> >  _arrayIterators;
    vector<shared_ptr<ChunkIterator> >  _chunkIterators;
    vector <uint32_t>                   _hashBreaks;
    int64_t                             _currentBreak;
    Value                               _boolTrue;
    shared_ptr<Expression>              _filterExpression;
    vector<BindInfo>                    _filterBindings;
    size_t                              _numBindings;
    shared_ptr<ExpressionContext>       _filterContext;

public:
    ArrayWriter(Settings const& settings, shared_ptr<Query> const& query, ArrayDesc const& schema):
        _output           (make_shared<MemArray>( schema, query)),
        _myInstanceId     (query->getInstanceID()),
        _numInstances     (query->getInstancesCount()),
        _numAttributes    (_output->getArrayDesc().getAttributes(true).size() ),
        _leftTupleSize    (settings.getLeftTupleSize()),
        _numKeys          (settings.getNumKeys()),
        _chunkSize        (settings.getChunkSize()),
        _query            (query),
        _settings         (settings),
        _tuplePlaceholder (_numAttributes,  NULL),
        _outputPosition   (mode == OUTPUT ? 2 : 3, 0),
        _arrayIterators   (_numAttributes+1, NULL),
        _chunkIterators   (_numAttributes+1, NULL),
        _hashBreaks       (_numInstances-1, 0),
        _currentBreak     (0),
        _filterExpression (mode == OUTPUT ? settings.getFilterExpression() : NULL)
    {
        _boolTrue.setBool(true);
        for(size_t i =0; i<_numAttributes+1; ++i)
        {
            _arrayIterators[i] = _output->getIterator(i);
        }
        if(mode == OUTPUT)
        {
            _outputPosition[0] = _myInstanceId;
            _outputPosition[1] = 0;
            if(_filterExpression.get())
            {
                _filterBindings = _filterExpression->getBindings();
                _numBindings = _filterBindings.size();
                _filterContext.reset(new ExpressionContext(*_filterExpression));
                for(size_t i =0; i<_filterBindings.size(); ++i)
                {
                    BindInfo const& binding = _filterBindings[i];
                    if(binding.kind == BindInfo::BI_VALUE)
                    {
                        (*_filterContext)[i] = binding.value;
                    }
                    else if(binding.kind == BindInfo::BI_COORDINATE)
                    {
                        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "filtering on dimensions not supported";
                    }
                }
            }
        }
        else
        {
            _outputPosition[0] = 0;
            _outputPosition[1] = _myInstanceId;
            _outputPosition[2] = 0;
            if(mode == SPLIT_ON_HASH)
            {
                uint32_t break_interval = settings.getNumHashBuckets() / _numInstances;
                for(size_t i=0; i<_numInstances-1; ++i)
                {
                    _hashBreaks[i] = break_interval * (i+1);
                }
            }
        }
    }

    bool tuplePassesFilter(vector<Value const*> const& tuple)
    {
        if(_filterExpression.get())
        {
            for(size_t i=0; i<_numBindings; ++i)
            {
                BindInfo const& binding = _filterBindings[i];
                if(binding.kind == BindInfo::BI_ATTRIBUTE)
                {
                    size_t index = _filterBindings[i].resolvedId;
                    (*_filterContext)[i] = *(tuple[index]);
                }
            }
            Value const& res = _filterExpression->evaluate(*_filterContext);
            if(res.isNull() || res.getBool() == false)
            {
                return false;
            }
        }
        return true;
    }

    void writeTuple(vector<Value const*> const& tuple)
    {
        if(mode == OUTPUT && !tuplePassesFilter(tuple))
        {
            return;
        }
        bool newChunk = false;
        if(mode == SPLIT_ON_HASH)
        {
            uint32_t hash = tuple[ _numAttributes-1 ]->getUint32();
            while( static_cast<size_t>(_currentBreak) < _numInstances - 1 && hash > _hashBreaks[_currentBreak] )
            {
                ++_currentBreak;
            }
            if( _currentBreak != _outputPosition[0] )
            {
                _outputPosition[0] = _currentBreak;
                _outputPosition[2] = 0;
                newChunk =true;
            }
        }
        if (_outputPosition[mode == OUTPUT ? 1 : 2] % _chunkSize == 0)
        {
            newChunk = true;
        }
        if( newChunk )
        {
            for(size_t i=0; i<_numAttributes+1; ++i)
            {
                if(_chunkIterators[i].get())
                {
                    _chunkIterators[i]->flush();
                }
                _chunkIterators[i] = _arrayIterators[i]->newChunk(_outputPosition).getIterator(_query, ChunkIterator::SEQUENTIAL_WRITE );
            }
        }
        for(size_t i=0; i<_numAttributes; ++i)
        {
            _chunkIterators[i]->setPosition(_outputPosition);
            _chunkIterators[i]->writeItem(*(tuple[i]));
        }
        _chunkIterators[_numAttributes]->setPosition(_outputPosition);
        _chunkIterators[_numAttributes]->writeItem(_boolTrue);
        ++_outputPosition[ mode == OUTPUT ? 1 : 2];
    }

    void writeTuple(vector<Value const*> const& left, Value const* right)
    {
        for(size_t i=0; i<_numAttributes; ++i)
        {
            if(i<_leftTupleSize)
            {
                _tuplePlaceholder[i] = left[i];
            }
            else
            {
                _tuplePlaceholder[i] = right + i - _leftTupleSize + _numKeys;
            }
        }
        writeTuple(_tuplePlaceholder);
    }

    void writeTuple(Value const* left, vector<Value const*> const& right)
    {
        for(size_t i=0; i<_numAttributes; ++i)
        {
            if(i<_leftTupleSize)
            {
                _tuplePlaceholder[i] = left + i;
            }
            else
            {
                _tuplePlaceholder[i] = right[i - _leftTupleSize + _numKeys];
            }
        }
        writeTuple(_tuplePlaceholder);
   }

   void writeTuple(vector<Value const*> const& left, vector<Value const*> const& right)
   {
       for(size_t i=0; i<_numAttributes; ++i)
       {
           if(i<_leftTupleSize)
           {
               _tuplePlaceholder[i] = left[i];
           }
           else
           {
               _tuplePlaceholder[i] = right[i - _leftTupleSize + _numKeys];
           }
       }
       writeTuple(_tuplePlaceholder);
   }

    shared_ptr<Array> finalize()
    {
        for(size_t i =0; i<_numAttributes+1; ++i)
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

class BitVector
{
private:
    size_t _size;
    vector<char> _data;

public:
    BitVector (size_t const bitSize):
        _size(bitSize),
        _data( (_size+7) / 8, 0)
    {}

    BitVector(size_t const bitSize, void const* data):
        _size(bitSize),
        _data( (_size+7) / 8, 0)
    {
        memcpy(&(_data[0]), data, _data.size());
    }

    void set(size_t const& idx)
    {
        if(idx >= _size)
        {
            throw 0;
        }
        size_t byteIdx = idx / 8;
        size_t bitIdx  = idx - byteIdx * 8;
        char& b = _data[ byteIdx ];
        b = b | (1 << bitIdx);
    }

    bool get(size_t const& idx) const
    {
        if(idx >= _size)
        {
            throw 0;
        }
        size_t byteIdx = idx / 8;
        size_t bitIdx  = idx - byteIdx * 8;
        char const& b = _data[ byteIdx ];
        return (b & (1 << bitIdx));
    }

    size_t getBitSize() const
    {
        return _size;
    }

    size_t getByteSize() const
    {
        return _data.size();
    }

    char const* getData() const
    {
        return &(_data[0]);
    }

    void orIn(BitVector const& other)
    {
        if(other._size != _size)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "OR-ing in unequal vit vectors";
        }
        for(size_t i =0; i<_data.size(); ++i)
        {
            _data[i] |= other._data[i];
        }
    }
};

class BloomFilter
{
public:
    static uint32_t const hashSeed1 = 0x5C1DB123;
    static uint32_t const hashSeed2 = 0xACEDBEEF;

private:
    BitVector _vec;
    mutable vector<char> _hashBuf;

public:
    BloomFilter(size_t const bitSize):
        _vec(bitSize),
        _hashBuf(64)
    {}

    void addData(void const* data, size_t const dataSize )
    {
        uint32_t hash1 = JoinHashTable::murmur3_32((char const*) data, dataSize, hashSeed1) % _vec.getBitSize();
        uint32_t hash2 = JoinHashTable::murmur3_32((char const*) data, dataSize, hashSeed2) % _vec.getBitSize();
        _vec.set(hash1);
        _vec.set(hash2);
    }

    bool hasData(void const* data, size_t const dataSize ) const
    {
        uint32_t hash1 = JoinHashTable::murmur3_32((char const*) data, dataSize, hashSeed1) % _vec.getBitSize();
        uint32_t hash2 = JoinHashTable::murmur3_32((char const*) data, dataSize, hashSeed2) % _vec.getBitSize();
        return _vec.get(hash1) && _vec.get(hash2);
    }

    void addTuple(vector<Value const*> data, size_t const numKeys)
    {
        size_t totalSize = 0;
        for(size_t i=0; i<numKeys; ++i)
        {
            totalSize+=data[i]->size();
        }
        if(_hashBuf.size() < totalSize)
        {
            _hashBuf.resize(totalSize);
        }
        char* ch = &_hashBuf[0];
        for(size_t i =0; i<numKeys; ++i)
        {
            memcpy(ch, data[i]->data(), data[i]->size());
            ch += data[i]->size();
        }
        addData(&_hashBuf[0], totalSize);
    }

    bool hasTuple(vector<Value const*> data, size_t const numKeys) const
    {
        size_t totalSize = 0;
        for(size_t i=0; i<numKeys; ++i)
        {
            totalSize+=data[i]->size();
        }
        if(_hashBuf.size() < totalSize)
        {
            _hashBuf.resize(totalSize);
        }
        char* ch = &_hashBuf[0];
        for(size_t i =0; i<numKeys; ++i)
        {
            memcpy(ch, data[i]->data(), data[i]->size());
            ch += data[i]->size();
        }
        return hasData(&_hashBuf[0], totalSize);
    }

    void globalExchange(shared_ptr<Query>& query)
    {
        /*
         * The bloom filters are sufficiently large (4MB each?), so we use two-phase messaging to save memory in case
         * there are a lot of instances (i.e. 256). This means two rounds of messaging, a little longer to execute but
         * we won't see a sudden memory spike (only on one instance, not on every instance).
         */
        size_t const nInstances = query->getInstancesCount();
        InstanceID myId = query->getInstanceID();
        if(!query->isCoordinator())
        {
           InstanceID coordinator = query->getCoordinatorID();
           shared_ptr<SharedBuffer> buf(new MemoryBuffer(NULL, _vec.getByteSize()));
           memcpy(buf->getData(), _vec.getData(), _vec.getByteSize());
           BufSend(coordinator, buf, query);
           buf = BufReceive(coordinator,query);
           BitVector incoming(_vec.getBitSize(), buf->getData());
           _vec = incoming;
        }
        else
        {
           for(InstanceID i=0; i<nInstances; ++i)
           {
              if(i != myId)
              {
                  shared_ptr<SharedBuffer> inBuf = BufReceive(i,query);
                  if(inBuf->getSize() != _vec.getByteSize())
                  {
                      throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "exchanging unequal bit vectors";
                  }
                  BitVector incoming(_vec.getBitSize(), inBuf->getData());
                  _vec.orIn(incoming);
              }
           }
           shared_ptr<SharedBuffer> buf(new MemoryBuffer(NULL, _vec.getByteSize()));
           memcpy(buf->getData(), _vec.getData(), _vec.getByteSize());
           for(InstanceID i=0; i<nInstances; ++i)
           {
              if(i != myId)
              {
                  BufSend(i, buf, query);
              }
           }
        }
    }
};

/**
 * First add in tuples from one of the arrays and then filter chunk positions from the other arrays.
 * The *which* template corresponds to the generator / training array
 */
template <Handedness which>
class ChunkFilter
{
private:
    size_t _numJoinedDimensions;
    vector<size_t>     _trainingArrayFields;    //index into the training array tuple
    vector<size_t>     _filterArrayDimensions;  //index into the filtered array dimensions
    vector<Coordinate> _filterArrayOrigins;
    vector<Coordinate> _filterChunkSizes;
    BloomFilter        _chunkHits;

public:
    ChunkFilter(Settings const& settings, ArrayDesc const& leftSchema, ArrayDesc const& rightSchema):
        _numJoinedDimensions(0),
        _chunkHits(0) //reallocated if actually needed below
    {
        size_t const numFilterAtts = which == LEFT ? settings.getNumRightAttrs() : settings.getNumLeftAttrs();
        size_t const numFilterDims = which == LEFT ? settings.getNumRightDims() : settings.getNumLeftDims();
        for(size_t i=numFilterAtts; i<numFilterAtts+numFilterDims; ++i)
        {
            if(which == LEFT ? settings.isRightKey(i) : settings.isLeftKey(i))
            {
                _numJoinedDimensions ++;
                _trainingArrayFields.push_back( which == LEFT ? settings.mapRightToTuple(i) : settings.mapLeftToTuple(i));
                size_t dimensionId = i - numFilterAtts;
                _filterArrayDimensions.push_back(dimensionId);
                DimensionDesc const& dimension = which == LEFT ? rightSchema.getDimensions()[dimensionId] : leftSchema.getDimensions()[dimensionId];
                _filterArrayOrigins.push_back(dimension.getStartMin());
                _filterChunkSizes.push_back(dimension.getChunkInterval());
            }
        }
        if(_numJoinedDimensions != 0)
        {
            _chunkHits = BloomFilter(settings.getBloomFilterSize());
        }
        ostringstream message;
        message<<"EJ chunk filter initialized dimensions "<<_numJoinedDimensions<<", training fields ";
        for(size_t i=0; i<_numJoinedDimensions; ++i)
        {
            message<<_trainingArrayFields[i]<<" ";
        }
        message<<", filter dimensions ";
        for(size_t i=0; i<_numJoinedDimensions; ++i)
        {
            message<<_filterArrayDimensions[i]<<" ";
        }
        message<<", filter origins ";
        for(size_t i=0; i<_numJoinedDimensions; ++i)
        {
            message<<_filterArrayOrigins[i]<<" ";
        }
        message<<", filter chunk sizes ";
        for(size_t i=0; i<_numJoinedDimensions; ++i)
        {
            message<<_filterChunkSizes[i]<<" ";
        }
        LOG4CXX_DEBUG(logger, message.str());
    }

    void addTuple(vector<Value const*> const& tuple)
    {
        if(_numJoinedDimensions==0)
        {
            return;
        }
        Coordinates input(_numJoinedDimensions);
        for(size_t i=0; i<_numJoinedDimensions; ++i)
        {
            input[i] = ((tuple[_trainingArrayFields[i]]->getInt64() - _filterArrayOrigins[i]) / _filterChunkSizes[i]) * _filterChunkSizes[i] + _filterArrayOrigins[i];
        }
        _chunkHits.addData(&(input[0]), _numJoinedDimensions*sizeof(Coordinate));
    }

    bool containsChunk(Coordinates const& inputChunkPos) const
    {
        if(_numJoinedDimensions==0)
        {
            return true;
        }
        Coordinates input(_numJoinedDimensions);
        for(size_t i=0; i<_numJoinedDimensions; ++i)
        {
            input[i] = inputChunkPos[_filterArrayDimensions[i]];
        }
        bool result = _chunkHits.hasData(&input[0], _numJoinedDimensions*sizeof(Coordinate));
        return result;
    }

    void globalExchange(shared_ptr<Query>& query)
    {
        if(_numJoinedDimensions!=0)
        {
            _chunkHits.globalExchange(query);
        }
    }
};

} //namespace equi_join

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

    template <Handedness which, bool populateChunkFilter, bool preTupled, typename ChunkFilterInstantiation>
    void readIntoTable(shared_ptr<Array> & array, JoinHashTable& table, Settings const& settings, ChunkFilterInstantiation& chunkFilter)
    {
        size_t const nAttrs = array->getArrayDesc().getAttributes(true).size();
        size_t const nDims  = array->getArrayDesc().getDimensions().size();
        vector<Value const*> tuple ( which == LEFT ? settings.getLeftTupleSize() : settings.getRightTupleSize(), NULL);
        size_t const numKeys = settings.getNumKeys();
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
                if(!preTupled)
                {
                    bool anyNull = false;
                    for(size_t i=0; i<nAttrs; ++i)
                    {
                        ssize_t idx = which == LEFT ? settings.mapLeftToTuple(i) : settings.mapRightToTuple(i);
                        Value const& v = citers[i]->getItem();
                        if(idx < (ssize_t) numKeys && v.isNull())
                        {
                            anyNull = true;
                            break;
                        }
                        tuple[ idx ] = &v;
                    }
                    if(anyNull)
                    {
                        for(size_t i=0; i<nAttrs; ++i)
                        {
                            ++(*citers[i]);
                        }
                        continue;
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
                }
                else
                {   //if pretupled; the last attribute is the hash
                    for(size_t i=0; i<nAttrs-1; ++i)
                    {
                        Value const& v = citers[i]->getItem();
                        tuple[ i ] = &v;
                    }
                }
                if(populateChunkFilter)
                {
                    chunkFilter.addTuple(tuple);
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

    template <Handedness which, bool useChunkFilter, bool arrayPreTupled, typename ChunkFilterInstantiation>
    shared_ptr<Array> arrayToTableJoin(shared_ptr<Array>& array, JoinHashTable& table, shared_ptr<Query>& query, Settings const& settings, ChunkFilterInstantiation const& chunkFilter)
    {
        //handedness LEFT means the LEFT array is in table so this reads in reverse
        size_t const nAttrs = array->getArrayDesc().getAttributes(true).size();
        size_t const nDims  = array->getArrayDesc().getDimensions().size();
        vector<Value const*> tuple ( which == LEFT ? settings.getRightTupleSize() : settings.getLeftTupleSize(), NULL);
        vector<shared_ptr<ConstArrayIterator> > aiters(nAttrs, NULL);
        vector<shared_ptr<ConstChunkIterator> > citers(nAttrs, NULL);
        vector<Value> dimVal(nDims);
        size_t const numKeys = settings.getNumKeys();
        JoinHashTable::const_iterator iter = table.getIterator();
        ArrayWriter<OUTPUT> result(settings, query, _schema);
        for(size_t i=0; i<nAttrs; ++i)
        {
            aiters[i] = array->getConstIterator(i);
        }
        while(!aiters[0]->end())
        {
            Coordinates const& chunkPos = aiters[0]->getPosition();
            if(useChunkFilter && !chunkFilter.containsChunk(chunkPos))
            {
                for(size_t i=0; i<nAttrs; ++i)
                {
                    ++(*aiters[i]);
                }
                continue;
            }
            for(size_t i=0; i<nAttrs; ++i)
            {
                citers[i] = aiters[i]->getChunk().getConstIterator();
            }
            while(!citers[0]->end())
            {
                if(!arrayPreTupled)
                {
                    bool anyNull = false;
                    for(size_t i=0; i<nAttrs; ++i)
                    {
                        ssize_t idx = which == LEFT ? settings.mapRightToTuple(i) : settings.mapLeftToTuple(i);
                        Value const& v = citers[i]->getItem();
                        if(idx<((ssize_t)numKeys) && v.isNull())
                        {
                            anyNull = true;
                            break;
                        }
                        tuple[ idx ] = &v;
                    }
                    if(anyNull)
                    {
                        for(size_t i=0; i<nAttrs; ++i)
                        {
                            ++(*citers[i]);
                        }
                        continue;
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
                }
                else
                {   //if pretupled; the last attribute is the hash
                    for(size_t i=0; i<nAttrs-1; ++i)
                    {
                        Value const& v = citers[i]->getItem();
                        tuple[i] = &v;
                    }
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
                    iter.nextAtHash();
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
        ChunkFilter <which> filter(settings, inputArrays[0]->getArrayDesc(), inputArrays[1]->getArrayDesc());
        readIntoTable<which, true, false> (redistributed, table, settings, filter);
        return arrayToTableJoin<which, true, false>( which == LEFT ? inputArrays[1]: inputArrays[0], table, query, settings, filter);
    }

    enum FilteringBehavior
    {
        GENERATE_FILTER,
        REDUCE_WITH_FILTER
    };

    template <Handedness which, FilteringBehavior filtering, typename ChunkFilterInstantiation>
    shared_ptr<Array> readIntoPreSort(shared_ptr<Array> & inputArray, shared_ptr<Query>& query, Settings const& settings,
                                      ChunkFilterInstantiation& chunkFilter, BloomFilter& bloomFilter)
    {
        ArrayDesc schema = settings.getPreSgSchema <which> (query);
        ArrayWriter<PRE_SORT> writer(settings, query, schema);
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
        vector<char> hashBuf(64);
        for(size_t i=0; i<nAttrs; ++i)
        {
            aiters[i] = inputArray->getConstIterator(i);
        }
        while(!aiters[0]->end())
        {
            if(filtering == REDUCE_WITH_FILTER)
            {
                Coordinates const& chunkPos = aiters[0]->getPosition();
                if(!chunkFilter.containsChunk(chunkPos))
                {
                    for(size_t i=0; i<nAttrs; ++i)
                    {
                        ++(*aiters[i]);
                    }
                    continue;
                }
            }
            for(size_t i=0; i<nAttrs; ++i)
            {
                citers[i] = aiters[i]->getChunk().getConstIterator();
            }
            while(!citers[0]->end())
            {
                bool anyNull = false;
                for(size_t i=0; i<nAttrs; ++i)
                {
                    Value const& v = citers[i]->getItem();
                    ssize_t idx = which == LEFT ? settings.mapLeftToTuple(i) : settings.mapRightToTuple(i);
                    if(idx<((ssize_t)numKeys) && v.isNull())
                    {
                        anyNull = true;
                        break;
                    }
                    tuple[ idx ] = &v;
                }
                if(anyNull)
                {
                    for(size_t i=0; i<nAttrs; ++i)
                    {
                        ++(*citers[i]);
                    }
                    continue;
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
                if(filtering == GENERATE_FILTER)
                {
                    chunkFilter.addTuple(tuple);
                    bloomFilter.addTuple(tuple, numKeys);
                }
                else if( !bloomFilter.hasTuple(tuple, numKeys))
                {
                    for(size_t i=0; i<nAttrs; ++i)
                    {
                        ++(*citers[i]);
                    }
                    continue;
                }
                hashVal.setUint32(JoinHashTable::hashKeys(tuple, numKeys, hashBuf) % hashMod);
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
        ArrayWriter<SPLIT_ON_HASH> writer(settings, query, schema);
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
        ArrayWriter<OUTPUT> output(settings, query, _schema);
        vector<AttributeComparator> const& comparators = settings.getKeyComparators();
        size_t const numKeys = settings.getNumKeys();
        SortedTupleCursor leftCursor (leftSorted, query);
        SortedTupleCursor rightCursor(rightSorted, query);
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
            while (!rightCursor.end() && rightHash == leftHash && keysLess(*rightTuple, *leftTuple, comparators, numKeys) )
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
            while(!rightCursor.end() && rightHash == leftHash && keysEqual(*leftTuple, *rightTuple, numKeys))
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
                if(keysEqual(*leftTuple, previousLeftTuple, numKeys) && !first)
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
        ChunkFilter <which> chunkFilter(settings, inputArrays[0]->getArrayDesc(), inputArrays[1]->getArrayDesc());
        BloomFilter bloomFilter(settings.getBloomFilterSize());
        first = readIntoPreSort<which, GENERATE_FILTER>(first, query, settings, chunkFilter, bloomFilter);
        first = sortArray(first, query, settings);
        first = sortedToPreSg<which>(first, query, settings);
        first = redistributeToRandomAccess(first,createDistribution(psByRow),query->getDefaultArrayResidency(), query, true);
        chunkFilter.globalExchange(query);
        bloomFilter.globalExchange(query);
        shared_ptr<Array>& second = (which == LEFT ? inputArrays[1] : inputArrays[0]);
        second = readIntoPreSort<(which == LEFT ? RIGHT : LEFT), REDUCE_WITH_FILTER>(second, query, settings, chunkFilter, bloomFilter);
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
            readIntoTable<which, false, true> (first, table, settings, chunkFilter);
            return arrayToTableJoin<which, false, true>( second, table, query, settings, chunkFilter);
        }
        else if(secondSize < settings.getHashJoinThreshold())
        {
            LOG4CXX_DEBUG(logger, "EJ merge rehashing second");
            ArenaPtr operatorArena = this->getArena();
            ArenaPtr hashArena(newArena(Options("").resetting(true).threading(false).pagesize(8 * 1024 * 1204).parent(operatorArena)));
            JoinHashTable table(settings, hashArena, which == LEFT ? settings.getRightTupleSize() : settings.getLeftTupleSize());
            readIntoTable<(which == LEFT ? RIGHT : LEFT), false, true> (second, table, settings, chunkFilter);
            return arrayToTableJoin<(which == LEFT ? RIGHT : LEFT), false, true>( first, table, query, settings, chunkFilter);
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
            return mergeJoin<LEFT>(inputArrays, query, settings);
        }
        else
        {
            LOG4CXX_DEBUG(logger, "EJ running merge_right_first");
            return mergeJoin<RIGHT>(inputArrays, query, settings);
        }
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalEquiJoin, "equi_join", "physical_equi_join");
} //namespace scidb
