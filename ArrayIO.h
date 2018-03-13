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

#ifndef ARRAY_WRITER_H
#define ARRAY_WRITER_H

#include "EquiJoinSettings.h"
#include "JoinHashTable.h"
#include <util/Network.h>

namespace scidb
{
namespace equi_join
{

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
           memcpy(buf->getWriteData(), _vec.getData(), _vec.getByteSize());
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
           memcpy(buf->getWriteData(), _vec.getData(), _vec.getByteSize());
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
 * The WHICH template corresponds to the generator / training array
 */
template <Handedness WHICH>
class ChunkFilter
{
private:
    size_t _numJoinedDimensions;
    vector<size_t>     _trainingArrayFields;    //index into the training array tuple
    vector<size_t>     _filterArrayDimensions;  //index into the filtered array dimensions
    vector<Coordinate> _filterArrayOrigins;
    vector<Coordinate> _filterChunkSizes;
    BloomFilter        _chunkHits;
    mutable vector<Coordinate> _coordBuf;
    mutable vector<Coordinate> _oldBuf;

public:
    ChunkFilter(Settings const& settings, ArrayDesc const& leftSchema, ArrayDesc const& rightSchema):
        _numJoinedDimensions(0),
        _chunkHits(0), //reallocated if actually needed below
        _coordBuf(0),
        _oldBuf(0)
    {
        size_t const numFilterAtts = WHICH == LEFT ? settings.getNumRightAttrs() : settings.getNumLeftAttrs();
        size_t const numFilterDims = WHICH == LEFT ? settings.getNumRightDims() : settings.getNumLeftDims();
        for(size_t i=numFilterAtts; i<numFilterAtts+numFilterDims; ++i)
        {
            if(WHICH == LEFT ? settings.isRightKey(i) : settings.isLeftKey(i))
            {
                _numJoinedDimensions ++;
                _trainingArrayFields.push_back( WHICH == LEFT ? settings.mapRightToTuple(i) : settings.mapLeftToTuple(i));
                size_t dimensionId = i - numFilterAtts;
                _filterArrayDimensions.push_back(dimensionId);
                DimensionDesc const& dimension = WHICH == LEFT ? rightSchema.getDimensions()[dimensionId] : leftSchema.getDimensions()[dimensionId];
                _filterArrayOrigins.push_back(dimension.getStartMin());
                _filterChunkSizes.push_back(dimension.getChunkInterval());
            }
        }
        if(_numJoinedDimensions != 0)
        {
            _chunkHits = BloomFilter(settings.getBloomFilterSize());
            _coordBuf.resize(_numJoinedDimensions);
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
        for(size_t i=0; i<_numJoinedDimensions; ++i)
        {
            _coordBuf[i] = ((tuple[_trainingArrayFields[i]]->getInt64() - _filterArrayOrigins[i]) / _filterChunkSizes[i]) * _filterChunkSizes[i] + _filterArrayOrigins[i];
        }
        if(_oldBuf.size() == 0 || _coordBuf != _oldBuf)
        {
            _chunkHits.addData(&(_coordBuf[0]), _numJoinedDimensions*sizeof(Coordinate));
            _oldBuf = _coordBuf;
        }
    }

    bool containsChunk(Coordinates const& inputChunkPos) const
    {
        if(_numJoinedDimensions==0)
        {
            return true;
        }
        for(size_t i=0; i<_numJoinedDimensions; ++i)
        {
            _coordBuf[i] = inputChunkPos[_filterArrayDimensions[i]];
        }
        bool result = _chunkHits.hasData(&_coordBuf[0], _numJoinedDimensions*sizeof(Coordinate));
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

template <Handedness WHICH>
ArrayDesc makeTupledSchema(Settings const& settings, shared_ptr< Query> const& query)
{
    size_t const numAttrs = ( WHICH == LEFT ? settings.getLeftTupleSize() : settings.getRightTupleSize()) + 1; //plus hash
    Attributes outputAttributes(numAttrs);
    outputAttributes[numAttrs-1] = AttributeDesc(numAttrs-1, "hash", TID_UINT32, 0, CompressorType::NONE);
    ArrayDesc const& inputSchema = ( WHICH == LEFT ? settings.getLeftSchema() : settings.getRightSchema());
    size_t const numInputAttrs = (WHICH == LEFT ? settings.getNumLeftAttrs() : settings.getNumRightAttrs());
    size_t const numInputDims = (WHICH == LEFT ? settings.getNumLeftDims() : settings.getNumRightDims());
    for(AttributeID i = 0; i < numInputAttrs; ++i)
    {
        AttributeDesc const& input = inputSchema.getAttributes(true)[i];
        AttributeID destinationId = (WHICH == LEFT ? settings.mapLeftToTuple(i) : settings.mapRightToTuple(i));
        uint16_t flags = input.getFlags();
        if( (WHICH == LEFT ? settings.isLeftKey(i) : settings.isRightKey(i)) && settings.isKeyNullable(destinationId) )
        {
            flags |= AttributeDesc::IS_NULLABLE;
        }
        outputAttributes[destinationId] = AttributeDesc(destinationId, input.getName(), input.getType(), flags, CompressorType::NONE);
    }
    for(size_t i = 0; i< numInputDims; ++i )
    {
        ssize_t destinationId = (WHICH == LEFT ? settings.mapLeftToTuple(i + numInputAttrs) : settings.mapRightToTuple(i + numInputAttrs));
        if(destinationId < 0)
        {
            continue;
        }
        DimensionDesc const& inputDim = inputSchema.getDimensions()[i];
        outputAttributes[destinationId] = AttributeDesc(destinationId, inputDim.getBaseName(), TID_INT64, 0, CompressorType::NONE);
    }
    outputAttributes = addEmptyTagAttribute(outputAttributes);
    Dimensions outputDimensions;
    outputDimensions.push_back(DimensionDesc("dst_instance_id", 0, query->getInstancesCount()-1,             1,         0));
    outputDimensions.push_back(DimensionDesc("src_instance_id", 0, query->getInstancesCount()-1,             1,         0));
    outputDimensions.push_back(DimensionDesc("value_no",        0, CoordinateBounds::getMax(),               settings.getChunkSize(), 0));
    return ArrayDesc("equi_join_state" , outputAttributes, outputDimensions, defaultPartitioning(), query->getDefaultArrayResidency());
}

enum WriteArrayType
{
    WRITE_TUPLED,           //we're writing a tupled array (schema as above), we don't really use the dst_instance_id dimension
    WRITE_SPLIT_ON_HASH,    //we're writing a tupled array (schema as above), we expect input to be sorted on hash and we assign dst_instance_id chunks based on hash
    WRITE_OUTPUT            //we're writing the output array (schema as generated in Settings). Here we merge left+right tuples and use the Filter Expression if any.
};

template<WriteArrayType MODE>
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
    Value                               _nullVal;
    shared_ptr<Expression>              _filterExpression;
    vector<BindInfo>                    _filterBindings;
    size_t                              _numBindings;
    shared_ptr<ExpressionContext>       _filterContext;

public:
    ArrayWriter(Settings const& settings, shared_ptr<Query> const& query, ArrayDesc const& schema):
        _output           (std::make_shared<MemArray>( schema, query)),
        _myInstanceId     (query->getInstanceID()),
        _numInstances     (query->getInstancesCount()),
        _numAttributes    (_output->getArrayDesc().getAttributes(true).size() ),
        _leftTupleSize    (settings.getLeftTupleSize()),
        _numKeys          (settings.getNumKeys()),
        _chunkSize        (settings.getChunkSize()),
        _query            (query),
        _settings         (settings),
        _tuplePlaceholder (_numAttributes,  NULL),
        _outputPosition   (MODE == WRITE_OUTPUT ? 2 : 3, 0),
        _arrayIterators   (_numAttributes+1, NULL),
        _chunkIterators   (_numAttributes+1, NULL),
        _hashBreaks       (_numInstances-1, 0),
        _currentBreak     (0),
        _filterExpression (MODE == WRITE_OUTPUT ? settings.getFilterExpression() : NULL)
    {
        _boolTrue.setBool(true);
        _nullVal.setNull();
        for(size_t i =0; i<_numAttributes+1; ++i)
        {
            _arrayIterators[i] = _output->getIterator(i);
        }
        if(MODE == WRITE_OUTPUT)
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
            if(MODE == WRITE_SPLIT_ON_HASH)
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
        if(MODE == WRITE_OUTPUT && !tuplePassesFilter(tuple))
        {
            return;
        }
        bool newChunk = false;
        if(MODE == WRITE_SPLIT_ON_HASH)
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
        if (_outputPosition[MODE == WRITE_OUTPUT ? 1 : 2] % _chunkSize == 0)
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
                _chunkIterators[i] = _arrayIterators[i]->newChunk(_outputPosition).getIterator(_query, ChunkIterator::SEQUENTIAL_WRITE | ChunkIterator::NO_EMPTY_CHECK );
            }
        }
        for(size_t i=0; i<_numAttributes; ++i)
        {
            _chunkIterators[i]->setPosition(_outputPosition);
            _chunkIterators[i]->writeItem(*(tuple[i]));
        }
        _chunkIterators[_numAttributes]->setPosition(_outputPosition);
        _chunkIterators[_numAttributes]->writeItem(_boolTrue);
        ++_outputPosition[ MODE == WRITE_OUTPUT ? 1 : 2];
    }

    void writeTupleWithHash(vector<Value const*> const& tuple, Value const& hash)
    {
        if(MODE != WRITE_TUPLED)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "internal inconsistency";
        }
        for(size_t i=0; i<_numAttributes-1; ++i)
        {
            _tuplePlaceholder[i] = tuple[i];
        }
        _tuplePlaceholder[_numAttributes-1] = &hash;
        writeTuple(_tuplePlaceholder);
    }

    //combine two tuples (i.e. join); see getValueFromTuple in JoinHashTable
    template <typename TUPLE_TYPE_1, typename TUPLE_TYPE_2>
    void writeTuple(TUPLE_TYPE_1 const& left, TUPLE_TYPE_2 const& right)
    {
        if(MODE != WRITE_OUTPUT)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "internal inconsistency";
        }
        for(size_t i=0; i<_numAttributes; ++i)
        {
            if(i<_leftTupleSize)
            {
                _tuplePlaceholder[i] = &(getValueFromTuple(left, i));
            }
            else
            {
                _tuplePlaceholder[i] = &(getValueFromTuple(right, i - _leftTupleSize + _numKeys));
            }
        }
        writeTuple(_tuplePlaceholder);
    }

    template <Handedness which, typename TUPLE_TYPE>
    void writeOuterTuple(TUPLE_TYPE const& tuple)
    {
        if(MODE != WRITE_OUTPUT)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "internal inconsistency";
        }
        for(size_t i=0; i<_numAttributes; ++i)
        {
            if(i<_numKeys)
            {
                _tuplePlaceholder[i] = &(getValueFromTuple(tuple, i));
            }
            else if(i<_leftTupleSize)
            {
                if(which == LEFT)
                {
                    _tuplePlaceholder[i] = &(getValueFromTuple(tuple, i));
                }
                else
                {
                    _tuplePlaceholder[i] = &_nullVal;
                }
            }
            else
            {
                if(which == RIGHT)
                {
                    _tuplePlaceholder[i] = &(getValueFromTuple(tuple, i - _leftTupleSize + _numKeys));
                }
                else
                {
                    _tuplePlaceholder[i] = &_nullVal;
                }
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

enum ReadArrayType
{
    READ_INPUT,          //we're reading the input array, however it may be; we reorder attributes and convert dimensions to tuple if needed;
                         //here we filter join keys for nulls and apply the chunk filter if requested.
    READ_TUPLED,         //we're reading an array that's been tupled (see above schema)
    READ_SORTED          //we're reading an array that's been tupled and sorted, so it is 1D, dense and client can seek to a Coordinate of their choice
};

template<Handedness WHICH, ReadArrayType MODE, bool INCLUDE_NULL_TUPLES = false>
class ArrayReader
{
private:
    shared_ptr<Array>                       _input;
    Settings const&                         _settings;
    size_t const                            _nAttrs;   //internal: corresponds to num actual attributes
    size_t const                            _nDims;
    vector<Value const*>                    _tuple;    //external: corresponds to the left or right tuple desired
    vector<Value>                           _dimVals;  //for reading dimensions from INPUT
    size_t const                            _numKeys;
    Coordinate const                        _chunkSize;
    ChunkFilter<WHICH == LEFT ? RIGHT : LEFT> const *const   _readChunkFilter;
    BloomFilter const * const               _readBloomFilter;
    Coordinate                              _currChunkIdx;
    vector<shared_ptr<ConstArrayIterator> > _aiters;
    vector<shared_ptr<ConstChunkIterator> > _citers;
    size_t                                  _chunksAvailable;
    size_t                                  _chunksExcluded;
    size_t                                  _tuplesAvailable;
    size_t                                  _tuplesExcludedNull;
    size_t                                  _tuplesExcludedBloom;

public:
    ArrayReader( shared_ptr<Array>& input, Settings const& settings,
                 ChunkFilter<WHICH == LEFT ? RIGHT : LEFT> const* readChunkFilter = NULL,
                 BloomFilter const* readBloomFilter = NULL):
        _input(input),
        _settings(settings),
        _nAttrs( input->getArrayDesc().getAttributes(true).size()),
        _nDims ( input->getArrayDesc().getDimensions().size()),
        _tuple( (WHICH == LEFT ? _settings.getLeftTupleSize() : _settings.getRightTupleSize()) + (MODE == READ_INPUT ? 0 : 1)),
        _dimVals (MODE == READ_INPUT ? _nDims : 0),
        _numKeys(_settings.getNumKeys()),
        _chunkSize( MODE == READ_SORTED ? _input->getArrayDesc().getDimensions()[0].getChunkInterval() : -1 ),
        _readChunkFilter(readChunkFilter),
        _readBloomFilter(readBloomFilter),
        _currChunkIdx( MODE == READ_SORTED ? 0 : -1),
        _aiters(_nAttrs),
        _citers(_nAttrs),
        _chunksAvailable(0),
        _chunksExcluded(0),
        _tuplesAvailable(0),
        _tuplesExcludedNull(0),
        _tuplesExcludedBloom(0)
    {
        Dimensions const& dims = _input->getArrayDesc().getDimensions();
        if(MODE == READ_SORTED && (dims.size()!=1 || dims[0].getStartMin() != 0))
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Internal inconsistency";
        }
        if(MODE != READ_INPUT && _nAttrs != _tuple.size())
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Internal inconsistency";
        }
        if(MODE != READ_INPUT && _readChunkFilter)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Internal inconsistency";
        }
        for(size_t i =0; i<_nAttrs; ++i)
        {
            _aiters[i] = _input->getConstIterator(i);
        }
        if(!end())
        {
            next<true>();
        }
    }

private:
    bool setAndCheckTuple()
    {
        ++_tuplesAvailable;
        if(MODE == READ_TUPLED || MODE == READ_SORTED)
        {
            for(size_t i =0; i<_nAttrs; ++i) //note: no null filtering in these modes
            {
                _tuple[i] = &(_citers[i]->getItem());
            }
        }
        else
        {
            for(size_t i =0; i<_nAttrs; ++i)
            {
                size_t idx = WHICH == LEFT ? _settings.mapLeftToTuple(i) : _settings.mapRightToTuple(i);
                _tuple[idx] = &(_citers[i]->getItem());
                if (INCLUDE_NULL_TUPLES == false && idx <_numKeys && _tuple[idx]->isNull())  //filter for NULLs
                {
                    ++_tuplesExcludedNull;
                    return false;
                }
            }
            Coordinates const& pos = _citers[0]->getPosition();
            for(size_t i =0; i<_nDims; ++i)
            {
                for(size_t i = 0; i<_nDims; ++i)
                {
                    ssize_t idx = (WHICH == LEFT ? _settings.mapLeftToTuple(i + _nAttrs) : _settings.mapRightToTuple(i + _nAttrs));
                    if(idx >= 0)
                    {
                        _dimVals[i].setInt64(pos[i]);
                        _tuple [ idx ] = &_dimVals[i];
                    }
                }
            }
        }
        if(_readBloomFilter && _readBloomFilter->hasTuple(_tuple, _numKeys) == false) //now run through the bloom filter, if any
        {
            ++_tuplesExcludedBloom;
            return false;
        }
        return true; //we got a valid tuple!
    }

    bool findNextTupleInChunk()
    {
        while(!_citers[0]->end())
        {
            if(setAndCheckTuple())
            {
                return true;
            }
            for(size_t i =0; i<_nAttrs; ++i)
            {
                ++(*_citers[i]);
            }
        }
        return false;
    }

public:
    template <bool FIRST_ITERATION = false>
    void next()
    {
        if(end())
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Internal inconsistency";
        }
        if(!FIRST_ITERATION)
        {
            for(size_t i =0; i<_nAttrs; ++i)
            {
                ++(*_citers[i]);
            }
            if(findNextTupleInChunk())
            {
                return;
            }
            for(size_t i =0; i<_nAttrs; ++i)
            {
                ++(*_aiters[i]);
            }
        }
        while(!_aiters[0]->end())
        {
            ++_chunksAvailable;
            if(MODE == READ_INPUT && _readChunkFilter)
            {
                Coordinates const& chunkPos = _aiters[0]->getPosition();
                if(! _readChunkFilter->containsChunk(chunkPos))
                {
                    for(size_t i =0; i<_nAttrs; ++i)
                    {
                        ++(*_aiters[i]);
                    }
                    ++_chunksExcluded;
                    continue;
                }
            }
            for(size_t i =0; i<_nAttrs; ++i)
            {
                _citers[i] = _aiters[i]->getChunk().getConstIterator();
            }
            if(MODE == READ_SORTED)
            {
                _currChunkIdx = _aiters[0]->getPosition()[0];
            }
            if(findNextTupleInChunk())
            {
                return;
            }
            for(size_t i =0; i<_nAttrs; ++i)
            {
                ++(*_aiters[i]);
            }
        }
    }

    bool end()
    {
        return _aiters[0]->end();
    }

    void logStats()
    {
        string const which = WHICH == LEFT ? "left" : "right";
        string const mode  = MODE == READ_INPUT ? "input" : MODE ==READ_TUPLED ? "tupled" : "sorted";
        LOG4CXX_DEBUG(logger, "EJ Array Read "<<which<<" "<< mode<< " total chunks "<<_chunksAvailable<<" chunks excluded "<<_chunksExcluded<<" tuples in included chunks "<<_tuplesAvailable<<
                " NULL tuples excluded "<<_tuplesExcludedNull<<" Bloom filter tuples excluded "<<_tuplesExcludedBloom);
    }

    vector<Value const*> const& getTuple()
    {
        if(end())
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Internal inconsistency";
        }
        return _tuple;
    }

    Coordinate getIdx()
    {
        if(MODE != READ_SORTED || end())
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Internal inconsistency";
        }
        return _citers[0]->getPosition()[0];
    }

    void setIdx(Coordinate idx)
    {
        if(MODE != READ_SORTED || idx<0)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Internal inconsistency";
        }
        Coordinates pos (1,idx);
        if(!end() && idx % _chunkSize == _currChunkIdx) //easy
        {
            for(size_t i=0; i<_nAttrs; ++i)
            {
                if(!_citers[i]->setPosition(pos) || !setAndCheckTuple())
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
            setAndCheckTuple();
        }
    }
};

} } //namespace scidb::equi_join

#endif //ARRAY_WRITER_H
