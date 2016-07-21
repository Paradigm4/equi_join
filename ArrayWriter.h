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

namespace scidb
{
namespace equi_join
{

enum ArrayWriterMode
{
    PRE_SORT,         //first phase:  convert input to tuples and add a hash attribute
    SPLIT_ON_HASH,    //second phase: split sorted tuples into different chunks based on hash - to send around the cluster
    OUTPUT            //final phase:  combine left and right tuples into the final result, optionally filtering with the supplied expression
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

} } //namespace scidb::equi_join

#endif //ARRAY_WRITER_H
