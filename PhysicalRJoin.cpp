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
    size_t const _numLeftAttributes;
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
        _numLeftAttributes(settings.getNumLeftAttrs()),
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
            if(i<_settings.getNumLeftAttrs())
            {
                _chunkIterators[i]->writeItem(*(left[i]));
            }
            else
            {
                Value const* v = right + _numKeys;
                _chunkIterators[i]->writeItem(*v);
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

    void readRightIntoTable(shared_ptr<Array> & rightArray, JoinHashTable& table, Settings const& settings)
    {
        size_t const nAttrs = settings.getNumRightAttrs();
        vector<Value const*> tuple(nAttrs, NULL);
        vector<shared_ptr<ConstArrayIterator> > aiters(nAttrs);
        vector<shared_ptr<ConstChunkIterator> > citers(nAttrs);
        for(size_t i=0; i<nAttrs; ++i)
        {
            aiters[i] = rightArray->getConstIterator(i);
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
                    tuple[settings.mapRightToTable(i)] = &v;
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

    shared_ptr<Array> leftToRightTableJoin(shared_ptr<Array>& leftArray, JoinHashTable& table, shared_ptr<Query>& query, Settings const& settings)
    {
        size_t const nLeftAttrs = settings.getNumLeftAttrs();
        vector<Value const*> tuple(nLeftAttrs, NULL);
        vector<shared_ptr<ConstArrayIterator> > leftAiters(nLeftAttrs, NULL);
        vector<shared_ptr<ConstChunkIterator> > leftCiters(nLeftAttrs, NULL);
        JoinHashTable::const_iterator iter = table.getIterator();
        MemArrayAppender result (settings, query);
        for(size_t i=0; i<nLeftAttrs; ++i)
        {
            leftAiters[i] = leftArray->getConstIterator(i);
        }
        while(!leftAiters[0]->end())
        {
            for(size_t i=0; i<nLeftAttrs; ++i)
            {
                leftCiters[i] = leftAiters[i]->getChunk().getConstIterator();
            }
            while(!leftCiters[0]->end())
            {
                for(size_t i=0; i<nLeftAttrs; ++i)
                {
                    Value const& v = leftCiters[i]->getItem();
                    tuple [ settings.mapLeftToOutput(i) ] = &v;
                }
                if(iter.find(tuple))
                {
                    Value const* right = iter.getTuple();
                    result.writeTuple(tuple, right);
                }
                for(size_t i=0; i<nLeftAttrs; ++i)
                {
                    ++(*leftCiters[i]);
                }
            }
            for(size_t i=0; i<nLeftAttrs; ++i)
            {
                ++(*leftAiters[i]);
            }
        }
        return result.finalize();
    }


    shared_ptr< Array> execute(vector< shared_ptr< Array> >& inputArrays, shared_ptr<Query> query)
    {
        vector<ArrayDesc const*> inputSchemas(2);
        inputSchemas[0] = &inputArrays[0]->getArrayDesc();
        inputSchemas[1] = &inputArrays[1]->getArrayDesc();
        Settings settings(inputSchemas, _parameters, false, query);
        ArenaPtr operatorArena = this->getArena();
        ArenaPtr hashArena(newArena(Options("").resetting(true).threading(false).pagesize(8 * 1024 * 1204).parent(operatorArena)));
        JoinHashTable jht(settings, hashArena, settings.getNumRightAttrs());
        readRightIntoTable(inputArrays[1], jht, settings);
        jht.logStuff();
        return leftToRightTableJoin(inputArrays[0], jht, query, settings);
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalRJoin, "rjoin", "physical_rjoin");
} //namespace scidb
