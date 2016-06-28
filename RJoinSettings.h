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

#ifndef RJOIN_SETTINGS
#define RJOIN_SETTINGS

#include <query/Operator.h>
#include <query/AttributeComparator.h>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>

namespace scidb
{
namespace rjoin
{

using std::string;
using std::vector;
using std::shared_ptr;
using std::dynamic_pointer_cast;
using std::ostringstream;
using std::stringstream;
using boost::algorithm::trim;
using boost::starts_with;
using boost::lexical_cast;
using boost::bad_lexical_cast;

// Logger for operator. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.operators.rjoin"));

/**
 * Table sizing considerations:
 *
 * We'd like to see a load factor of 4 or less. A group occupies at least 32 bytes in the structure,
 * usually more - depending on how many values and states there are and also whether they are variable sized.
 * An empty bucket is an 8-byte pointer. So the ratio of group data / bucket overhead is at least 16.
 * With that in mind we just pick a few primes for the most commonly used memory limits.
 * We start with that many buckets and, at the moment, we don't bother rehashing:
 *
 * memory_limit_MB     max_groups    desired_buckets   nearest_prime   buckets_overhead_MB
 *             128        4194304            1048576         1048573                     8
 *             256        8388608            2097152         2097143                    16
 *             512       16777216            4194304         4194301                    32
 *           1,024       33554432            8388608         8388617                    64
 *           2,048       67108864           16777216        16777213                   128
 *           4,096      134217728           33554432        33554467                   256
 *           8,192      268435456           67108864        67108859                   512
 *          16,384      536870912          134217728       134217757                 1,024
 *          32,768     1073741824          268435456       268435459                 2,048
 *          65,536     2147483648          536870912       536870909                 4,096
 *         131,072     4294967296         1073741824      1073741827                 8,192
 *            more                                        2147483647                16,384
 */

static const size_t NUM_SIZES = 12;
static const size_t memLimits[NUM_SIZES]  = {    128,     256,     512,    1024,     2048,     4096,     8192,     16384,     32768,     65536,     131072,  ((size_t)-1) };
static const size_t tableSizes[NUM_SIZES] = {1048573, 2097143, 4194301, 8388617, 16777213, 33554467, 67108859, 134217757, 268435459, 536870909, 1073741827,    2147483647 };

static size_t chooseNumBuckets(size_t maxTableSize)
{
   for(size_t i =0; i<NUM_SIZES; ++i)
   {
       if(maxTableSize <= memLimits[i])
       {
           return tableSizes[i];
       }
   }
   return tableSizes[NUM_SIZES-1];
}

/*
 * Settings for the grouped_aggregate operator.
 */
class Settings
{
private:
    ArrayDesc                     _leftSchema;
    ArrayDesc                     _rightSchema;
    size_t                        _numLeftAttrs;
    size_t                        _numRightAttrs;
    size_t                        _maxTableSize;
    size_t                        _numHashBuckets;
    size_t                        _chunkSize;
    size_t                        _numInstances;
    size_t                        _numKeys;
    vector<ssize_t>               _leftKeys;
    vector<ssize_t>               _rightKeys;
    vector<size_t>                _leftMap;
    vector<size_t>                _rightMap;
    vector<bool>                  _keyNullable;
    vector<AttributeComparator>   _keyComparators;

private:
    static string paramToString(shared_ptr <OperatorParam> const& parameter, shared_ptr<Query>& query, bool logical)
    {
        if(logical)
        {
            return evaluate(((shared_ptr<OperatorParamLogicalExpression>&) parameter)->getExpression(),query, TID_STRING).getString();
        }
        return ((shared_ptr<OperatorParamPhysicalExpression>&) parameter)->getExpression()->evaluate().getString();
    }

    void setParamKeys(string trimmedContent, vector<ssize_t> &keys)
    {
        stringstream ss(trimmedContent);
        string tok;
        while(getline(ss, tok, ','))
        {
            try
            {
                ssize_t key = lexical_cast<ssize_t>(tok);
                keys.push_back(key);
            }
            catch (bad_lexical_cast const& exn)
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "could not parse keys";
            }
        }
    }

    void setParamLeftKeys(string trimmedContent)
    {
        setParamKeys(trimmedContent, _leftKeys);
    }

    void setParamRightKeys(string trimmedContent)
    {
        setParamKeys(trimmedContent, _rightKeys);
    }

    void setParamMaxTableSize(string trimmedContent)
    {
        try
        {
            int64_t res = lexical_cast<int64_t>(trimmedContent);
            if(res <= 0)
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "max table size must be positive";
            }
            _maxTableSize = res;
            _numHashBuckets = chooseNumBuckets(_maxTableSize);
        }
        catch (bad_lexical_cast const& exn)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "could not parse max table size";
        }
    }

    void setParamChunkSize(string trimmedContent)
    {
        try
        {
            int64_t res = lexical_cast<int64_t>(trimmedContent);
            if(res <= 0)
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "chunk size must be positive";
            }
            _chunkSize = res;
        }
        catch (bad_lexical_cast const& exn)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "could not parse chunk size";
        }
    }

    void setParam (string const& parameterString, bool& alreadySet, string const& header, void (Settings::* innersetter)(string) )
    {
        string paramContent = parameterString.substr(header.size());
        if (alreadySet)
        {
            string header = parameterString.substr(0, header.size()-1);
            ostringstream error;
            error<<"illegal attempt to set "<<header<<" multiple times";
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << error.str().c_str();
        }
        trim(paramContent);
        (this->*innersetter)(paramContent); //TODO:.. tried for an hour with a template first. #thisIsWhyWeCantHaveNiceThings
        alreadySet = true;
    }

public:
    static size_t const MAX_PARAMETERS = 4;

    Settings(vector<ArrayDesc const*> inputSchemas,
             vector< shared_ptr<OperatorParam> > const& operatorParameters,
             bool logical,
             shared_ptr<Query>& query):
        _leftSchema(*(inputSchemas[0])),
        _rightSchema(*(inputSchemas[1])),
        _numLeftAttrs(_leftSchema.getAttributes(true).size()),
        _numRightAttrs(_rightSchema.getAttributes(true).size()),
        _maxTableSize(Config::getInstance()->getOption<int>(CONFIG_MERGE_SORT_BUFFER)),
        _numHashBuckets(chooseNumBuckets(_maxTableSize)),
        _chunkSize(1000000),
        _numInstances(query->getInstancesCount()),
        _leftMap(_numLeftAttrs, _numLeftAttrs),
        _rightMap(_numRightAttrs, _numRightAttrs)
    {
        string const leftKeysHeader                = "left_keys=";
        string const rightKeysHeader               = "right_keys=";
        string const maxTableSizeHeader            = "max_table_size=";
        string const chunkSizeHeader               = "chunk_size=";
        bool leftKeysSet      = false;
        bool rightKeysSet     = false;
        bool maxTableSizeSet  = false;
        bool chunkSizeSet     = false;
        size_t const nParams = operatorParameters.size();
        if (nParams > MAX_PARAMETERS)
        {   //assert-like exception. Caller should have taken care of this!
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal number of parameters passed to rjoin";
        }
        for (size_t i= 0; i<nParams; ++i)
        {
            string parameterString = paramToString(operatorParameters[i], query, logical);
            if (starts_with(parameterString, leftKeysHeader))
            {
                setParam(parameterString, leftKeysSet, leftKeysHeader, &Settings::setParamLeftKeys);
            }
            else if (starts_with(parameterString, rightKeysHeader))
            {
                setParam(parameterString, rightKeysSet, rightKeysHeader, &Settings::setParamRightKeys);
            }
            else if (starts_with(parameterString, maxTableSizeHeader))
            {
                setParam(parameterString, maxTableSizeSet, maxTableSizeHeader, &Settings::setParamMaxTableSize);
            }
            else if (starts_with(parameterString, chunkSizeHeader))
            {
                setParam(parameterString, chunkSizeSet, chunkSizeHeader, &Settings::setParamChunkSize);
            }
            else
            {
                ostringstream error;
                error << "Unrecognized token '"<<parameterString<<"'";
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << error.str().c_str();
            }
        }
        verifyInputs();
        mapAttributes();
        logSettings();
    }

private:
    void throwIf(bool const cond, char const* errorText)
    {
        if(cond)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << errorText;
        }
    }

    void verifyInputs()
    {
        throwIf(_leftKeys.size() == 0,                    "no left keys provided");
        throwIf(_rightKeys.size() == 0,                   "no right keys provided");
        throwIf(_leftKeys.size() != _rightKeys.size(),    "mismatched numbers of keys provided");
        for(size_t i =0; i<_leftKeys.size(); ++i)
        {
            ssize_t leftKey  = _leftKeys[i];
            ssize_t rightKey = _rightKeys[i];
            throwIf(leftKey < 0 || rightKey < 0,                     "negative keys not supported");
            throwIf(static_cast<size_t>(leftKey)  >= _numLeftAttrs,  "left key out of bounds");
            throwIf(static_cast<size_t>(rightKey) >= _numRightAttrs, "right key out of bounds");
            AttributeDesc const& leftAttr =  _leftSchema.getAttributes(true)[leftKey];
            AttributeDesc const& rightAttr = _rightSchema.getAttributes(true)[rightKey];
            throwIf(leftAttr.getType() != rightAttr.getType(), "key types do not match");
        }
    }

    void mapAttributes()
    {
        _numKeys = _leftKeys.size();
        for(size_t i =0; i<_numKeys; ++i)
        {
            _leftMap[_leftKeys[i]]   = i;
            _rightMap[_rightKeys[i]] = i;
            AttributeDesc const& leftAttr =  _leftSchema.getAttributes(true)[_leftKeys[i]];
            AttributeDesc const& rightAttr = _rightSchema.getAttributes(true)[_rightKeys[i]];
            _keyComparators.push_back(AttributeComparator(leftAttr.getType()));
            _keyNullable.push_back( leftAttr.isNullable() || rightAttr.isNullable());
        }
        size_t j=_numKeys;
        for(size_t i =0; i<_numLeftAttrs; ++i)
        {
            if(_leftMap[i] == _numLeftAttrs)
            {
                _leftMap[i] = j++;
            }
        }
        for(size_t i =0; i<_numRightAttrs; ++i)
        {
            if(_rightMap[i] == _numRightAttrs)
            {
                _rightMap[i] = j++;
            }
        }
    }

    void logSettings()
    {
        ostringstream output;
        for(size_t i=0; i<_numKeys; ++i)
        {
            output<<_leftKeys[i]<<"->"<<_rightKeys[i]<<" ";
        }
        output<<"buckets "<< _numHashBuckets;
        output<<" chunk "<<_chunkSize;
        LOG4CXX_DEBUG(logger, "RJN keys "<<output.str().c_str());
    }


public:
    size_t getNumKeys() const
    {
        return _numKeys;
    }

    size_t getNumLeftAttrs() const
    {
        return _numLeftAttrs;
    }

    size_t getNumRightAttrs() const
    {
        return _numRightAttrs;
    }

    size_t getNumOutputAttrs() const
    {
        return _numLeftAttrs + _numRightAttrs - _numKeys;
    }

    vector<ssize_t> const& getLeftKeys() const
    {
        return _leftKeys;
    }

    vector<ssize_t> const& getRightKeys() const
    {
        return _rightKeys;
    }

    size_t getNumHashBuckets() const
    {
        return _numHashBuckets;
    }

    size_t getChunkSize() const
    {
        return _chunkSize;
    }

    ArrayDesc const& getLeftSchema() const
    {
        return _leftSchema;
    }

    ArrayDesc const& getRightSchema() const
    {
        return _rightSchema;
    }

    bool isLeftKey(AttributeID const i) const
    {
        return _leftMap[i] < _numKeys;
    }

    bool isRightKey(AttributeID const i) const
    {
        return _rightMap[i] < _numKeys;
    }

    AttributeID mapLeftToOutput(AttributeID const leftAttr) const
    {
        return _leftMap[leftAttr];
    }

    AttributeID mapRightToOutput(AttributeID const rightAttr) const
    {
        return _rightMap[rightAttr];
    }

    AttributeID mapRightToTable(AttributeID const rightAttr) const
    {
        if(isRightKey(rightAttr))
        {
            return mapRightToOutput(rightAttr);
        }
        else
        {
            return mapRightToOutput(rightAttr) - _numLeftAttrs + _numKeys;
        }
    }

    ArrayDesc getOutputSchema(shared_ptr< Query> query, string const name = "") const
    {
        Attributes outputAttributes(getNumOutputAttrs());
        for(AttributeID i =0; i<_numLeftAttrs; ++i)
        {
            AttributeDesc const& input = _leftSchema.getAttributes(true)[i];
            AttributeID destinationId = mapLeftToOutput(i);
            uint16_t flags = input.getFlags();
            if(isLeftKey(i) && _keyNullable[destinationId] )
            {
                flags = AttributeDesc::IS_NULLABLE;
            }
            outputAttributes[destinationId] = AttributeDesc(destinationId, input.getName(), input.getType(), flags, 0);
        }
        for(AttributeID i =0; i<_numRightAttrs; ++i)
        {
            if(isRightKey(i))
            {
                continue;
            }
            AttributeDesc const& input = _rightSchema.getAttributes(true)[i];
            AttributeID destinationId = mapRightToOutput(i);
            outputAttributes[destinationId] = AttributeDesc(destinationId, input.getName(), input.getType(), input.getFlags(), 0);
        }
        outputAttributes = addEmptyTagAttribute(outputAttributes);
        Dimensions outputDimensions;
        outputDimensions.push_back(DimensionDesc("instance_id", 0, _numInstances-1,            1,          0));
        outputDimensions.push_back(DimensionDesc("value_no",    0, CoordinateBounds::getMax(), _chunkSize, 0));
        return ArrayDesc(name.size() == 0 ? "rjoin" : name, outputAttributes, outputDimensions, defaultPartitioning(), query->getDefaultArrayResidency());
    }

    vector <AttributeComparator> const& getKeyComparators() const
    {
        return _keyComparators;
    }
};

} } //namespaces

#endif //RJOIN_SETTINGS

