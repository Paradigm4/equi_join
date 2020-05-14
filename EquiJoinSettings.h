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

#ifndef EQUI_JOIN_SETTINGS
#define EQUI_JOIN_SETTINGS

#define LEGACY_API
#include <query/LogicalOperator.h>
#include <query/OperatorParam.h>
#include <query/Expression.h>
#include <query/Query.h>
#include <query/AttributeComparator.h>
#include <system/Config.h>
#include <boost/algorithm/string.hpp>

namespace scidb
{

std::shared_ptr<LogicalExpression>    parseExpression(const std::string&);
namespace equi_join
{

using std::string;
using std::vector;
using std::shared_ptr;
using std::dynamic_pointer_cast;
using std::ostringstream;
using std::stringstream;
using boost::algorithm::trim;
using boost::starts_with;

// Logger for operator. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.operators.equi_join"));

static const char* const KW_LEFT_IDS = "left_ids";
static const char* const KW_RIGHT_IDS = "right_ids";
static const char* const KW_LEFT_NAMES = "left_names";
static const char* const KW_RIGHT_NAMES = "right_names";
static const char* const KW_HASH_JOIN_THRES = "hash_join_threshold";
static const char* const KW_CHUNK_SIZE = "chunk_size";
static const char* const KW_ALGORITHM = "algorithm";
static const char* const KW_KEEP_DIMS = "keep_dimensions";
static const char* const KW_BLOOM_FILT_SZ = "bloom_filter_size";
static const char* const KW_FILTER = "filter";
static const char* const KW_LEFT_OUTER = "left_outer";
static const char* const KW_RIGHT_OUTER = "right_outer";
static const char* const KW_OUT_NAMES = "out_names";

typedef std::shared_ptr<OperatorParamLogicalExpression> ParamType_t ;

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

//For hash join purposes, the handedness refers to which array is copied into a hash table and redistributed
enum Handedness
{
    LEFT,
    RIGHT
};

class Settings
{
public:
    enum algorithm
    {
        HASH_REPLICATE_LEFT,
        HASH_REPLICATE_RIGHT,
        MERGE_LEFT_FIRST,
        MERGE_RIGHT_FIRST
    };

private:
    ArrayDesc                     _leftSchema;
    ArrayDesc                     _rightSchema;
    size_t                        _numLeftAttrs;
    size_t                        _numLeftDims;
    size_t                        _numRightAttrs;
    size_t                        _numRightDims;
    vector<ssize_t>               _leftMapToTuple;   //maps all attributes and dimensions from left to tuple, -1 if not used
    vector<ssize_t>               _rightMapToTuple;
    size_t                        _leftTupleSize;
    size_t                        _rightTupleSize;
    size_t                        _numKeys;
    vector<AttributeComparator>   _keyComparators;   //one per key
    vector<size_t>                _leftIds;          //key indeces in the left array:  attributes start at 0, dimensions start at numAttrs
    vector<size_t>                _rightIds;        //key indeces in the right array: attributes start at 0, dimensions start at numAttrs
    vector<bool>                  _keyNullable;      //one per key, in the output
    size_t                        _hashJoinThreshold;
    size_t                        _numHashBuckets;
    size_t                        _chunkSize;
    size_t                        _numInstances;
    algorithm                     _algorithm;
    bool                          _algorithmSet;
    bool                          _keepDimensions;
    size_t                        _bloomFilterSize;
    size_t                        _readAheadLimit;
    size_t                        _varSize;
    string                        _filterExpressionString;
    shared_ptr<Expression>        _filterExpression;
    vector<string>                _leftNames;
    vector<string>                _rightNames;
    bool                          _leftOuter;
    bool                          _rightOuter;
    vector<string>                _outNames;

    void setParamIds(vector<int64_t> content, vector<size_t> &keys, size_t shift)
    /*
     * Attributes are zero based and count up.  Dimensions are -1 based and count down.
     */
    {
        for (size_t i = 0; i < content.size(); ++i) {
            size_t val;
            if (content[i] < 0)  // It's a dimension
                val = shift + abs(content[i] + 1);
            else
                val = content[i]; // It's an attribute
            keys.push_back(val);
        }
    }

    void setParamLeftIds(vector<int64_t> content)
    {
        setParamIds(content, _leftIds, _numLeftAttrs);
    }

    void setParamRightIds(vector<int64_t> content)
    {
        setParamIds(content, _rightIds, _numRightAttrs);
    }

    void setParamNames(vector<string> content, vector<string> &names)
    {
        for (size_t i = 0; i < content.size(); ++i) {
            names.push_back(content[i]);
            LOG4CXX_DEBUG(logger, "EJ out name " << i << " is " << content[i]);
        }
    }

    void setParamLeftNames(vector<string> content)
    {
        setParamNames(content, _leftNames);
    }

    void setParamRightNames(vector <string> content)
    {
        setParamNames(content, _rightNames);
    }

    void setParamOutNames(vector<string> content)
    {
        setParamNames(content, _outNames);
    }

    void setParamHashJoinThreshold(vector<int64_t> keys)
    {
        int64_t res = keys[0];
        if(res < 0)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "hash join threshold must be non negative";
        }
        _hashJoinThreshold = res * 1024 * 1204;
        _numHashBuckets = chooseNumBuckets(_hashJoinThreshold / (1024*1024));
    }

    void setParamChunkSize(vector<int64_t> keys)
    {
        int64_t res = keys[0];
        if(res <= 0)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "chunk size must be positive";
        }
        _chunkSize = res;
    }

    void setParamFilterExpression(vector <string> content)
    {
        string exp = content[0];
        trim(exp);
        _filterExpressionString = exp;
    }

    void setParamAlgorithm(vector <string> content)
    {
        string trimmedContent = content[0];
        if(trimmedContent == "hash_replicate_left")
        {
            _algorithm = HASH_REPLICATE_LEFT;
        }
        else if (trimmedContent == "hash_replicate_right")
        {
            _algorithm = HASH_REPLICATE_RIGHT;
        }
        else if (trimmedContent == "merge_left_first")
        {
            _algorithm = MERGE_LEFT_FIRST;
        }
        else if (trimmedContent == "merge_right_first")
        {
            _algorithm = MERGE_RIGHT_FIRST;
        }
        else
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "could not parse algorithm";
        }
    }

    bool setParamBool(string trimmedContent, bool& value)
    {
        if(trimmedContent == "1" || trimmedContent == "t" || trimmedContent == "T" || trimmedContent == "true" || trimmedContent == "TRUE")
        {
            value = true;
            return true;
        }
        else if (trimmedContent == "0" || trimmedContent == "f" || trimmedContent == "F" || trimmedContent == "false" || trimmedContent == "FALSE")
        {
            value = false;
            return true;
        }
        return false;
    }

    void setParamKeepDimensions(string trimmedContent)
    {
        if(!setParamBool(trimmedContent, _keepDimensions))
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "could not parse keep_dimensions";
        }
    }

    void setParamBloomFilterSize(vector<int64_t> content)
    {
        int64_t res = content[0];
        if(res <= 0)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "bloom filter size size must be positive";
        }
        _bloomFilterSize = res;
    }

    void setParamLeftOuter(string trimmedContent)
    {
        if(!setParamBool(trimmedContent, _leftOuter))
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "could not parse left_outer";
        }
    }

    void setParamRightOuter(string trimmedContent)
    {
        if(!setParamBool(trimmedContent, _rightOuter))
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "could not parse right_outer";
        }
    }

    string getParamContentString(Parameter& param)
    {
        string paramContent;

        if(param->getParamType() == PARAM_LOGICAL_EXPRESSION) {
            ParamType_t& paramExpr = reinterpret_cast<ParamType_t&>(param);
            paramContent = evaluate(paramExpr->getExpression(), TID_STRING).getString();
        } else {
            OperatorParamPhysicalExpression* exp =
                dynamic_cast<OperatorParamPhysicalExpression*>(param.get());
            SCIDB_ASSERT(exp != nullptr);
            paramContent = exp->getExpression()->evaluate().getString();
        }
        return paramContent;
    }

    void setKeywordParamString(KeywordParameters const& kwParams,
                               const char* const kw,
                               void (Settings::* innersetter)(vector<string>) )
    {
        vector <string> paramContent;

        Parameter kwParam = getKeywordParam(kwParams, kw);
        if (kwParam) {
            if (kwParam->getParamType() == PARAM_NESTED) {
                auto group = dynamic_cast<OperatorParamNested*>(kwParam.get());
                Parameters& gParams = group->getParameters();
                for (size_t i = 0; i < gParams.size(); ++i) {
                    paramContent.push_back(getParamContentString(gParams[i]));
                }
            } else {
                paramContent.push_back(getParamContentString(kwParam));
            }
            (this->*innersetter)(paramContent);
        } else {
            LOG4CXX_DEBUG(logger, "EJ findKeyword null: " << kw);
        }
    }

    string getParamContentJoinField(Parameter& param)
    {
        string paramContent;

        if(param->getParamType() == PARAM_DIMENSION_REF) {
            const OperatorParamDimensionReference* dimRef =
                safe_dynamic_cast<OperatorParamDimensionReference*>(param.get());
            paramContent = dimRef->getObjectName();
        }

        if(param->getParamType() == PARAM_ATTRIBUTE_REF) {
            const OperatorParamAttributeReference* attRef =
                safe_dynamic_cast<OperatorParamAttributeReference*>(param.get());
            paramContent = attRef->getObjectName();
        }

        return paramContent;
    }

    void setKeywordParamJoinField(KeywordParameters const& kwParams,
                                  const char* const kw,
                                  void (Settings::* innersetter)(vector<string>) )
    /**
     * This function works for both Dimension names and Attribute names.
     */
    {
        vector<string> paramContent;
        Parameter kwParam = getKeywordParam(kwParams, kw);
        if (kwParam) {
            if (kwParam->getParamType() == PARAM_NESTED) {
                auto group = dynamic_cast<OperatorParamNested*>(kwParam.get());
                Parameters& gParams = group->getParameters();
                for (size_t i = 0; i < gParams.size(); ++i) {
                    paramContent.push_back(getParamContentJoinField(gParams[i]));
                }
            } else {
                paramContent.push_back(getParamContentJoinField(kwParam));
            }
            (this->*innersetter)(paramContent);
        } else {
            LOG4CXX_DEBUG(logger, "EJ findKeyword null: " << kw);
        }
    }

    int64_t getParamContentInt64(Parameter& param)
    {
        size_t paramContent;

        if(param->getParamType() == PARAM_LOGICAL_EXPRESSION) {
            ParamType_t& paramExpr = reinterpret_cast<ParamType_t&>(param);
            paramContent = evaluate(paramExpr->getExpression(), TID_INT64).getInt64();
        } else {
            OperatorParamPhysicalExpression* exp =
                dynamic_cast<OperatorParamPhysicalExpression*>(param.get());
            SCIDB_ASSERT(exp != nullptr);
            paramContent = exp->getExpression()->evaluate().getInt64();
            LOG4CXX_DEBUG(logger, "EJ integer param is " << paramContent)

        }
        return paramContent;
    }

    void setKeywordParamInt64(KeywordParameters const& kwParams, const char* const kw, void (Settings::* innersetter)(vector<int64_t>) )
    {
        vector<int64_t> paramContent;
        size_t numParams;

        Parameter kwParam = getKeywordParam(kwParams, kw);
        if (kwParam) {
            if (kwParam->getParamType() == PARAM_NESTED) {
                auto group = dynamic_cast<OperatorParamNested*>(kwParam.get());
                Parameters& gParams = group->getParameters();
                numParams = gParams.size();
                for (size_t i = 0; i < numParams; ++i) {
                    paramContent.push_back(getParamContentInt64(gParams[i]));
                }
            } else {
                paramContent.push_back(getParamContentInt64(kwParam));
            }
            (this->*innersetter)(paramContent);
        } else {
            LOG4CXX_DEBUG(logger, "EJ findKeyword null: " << kw);
        }
    }

    bool getParamContentBool(Parameter& param)
    {
        bool paramContent;

        if(param->getParamType() == PARAM_LOGICAL_EXPRESSION) {
            ParamType_t& paramExpr = reinterpret_cast<ParamType_t&>(param);
            paramContent = evaluate(paramExpr->getExpression(), TID_BOOL).getBool();
        } else {
            OperatorParamPhysicalExpression* exp =
                dynamic_cast<OperatorParamPhysicalExpression*>(param.get());
            SCIDB_ASSERT(exp != nullptr);
            paramContent = exp->getExpression()->evaluate().getBool();
        }
        return paramContent;
    }

    void setKeywordParamBool(KeywordParameters const& kwParams, const char* const kw, bool& value)
    {
        Parameter kwParam = getKeywordParam(kwParams, kw);
        if (kwParam) {
            bool paramContent = getParamContentBool(kwParam);
            LOG4CXX_DEBUG(logger, "EJ setting " << kw << " to " << paramContent);
            value = paramContent;
        } else {
            LOG4CXX_DEBUG(logger, "EJ findKeyword null: " << kw);
        }
    }

    Parameter getKeywordParam(KeywordParameters const& kwp, const std::string& kw) const
    {
        auto const& kwPair = kwp.find(kw);
        return kwPair == kwp.end() ? Parameter() : kwPair->second;
    }

public:
    static size_t const MAX_PARAMETERS = 11;

    Settings(vector<ArrayDesc const*> inputSchemas,
             vector< shared_ptr<OperatorParam> > const& operatorParameters,
             KeywordParameters const& kwParams,
             shared_ptr<Query>& query):
        _leftSchema(*(inputSchemas[0])),
        _rightSchema(*(inputSchemas[1])),
        _numLeftAttrs(_leftSchema.getAttributes(true).size()),
        _numLeftDims(_leftSchema.getDimensions().size()),
        _numRightAttrs(_rightSchema.getAttributes(true).size()),
        _numRightDims(_rightSchema.getDimensions().size()),
        _hashJoinThreshold(Config::getInstance()->getOption<int>(CONFIG_MERGE_SORT_BUFFER) * 1024 * 1024 ),
        _numHashBuckets(chooseNumBuckets(_hashJoinThreshold / (1024*1024))),
        _chunkSize(1000000),
        _numInstances(query->getInstancesCount()),
        _algorithm(HASH_REPLICATE_RIGHT),
        _algorithmSet(kwParams.find(KW_ALGORITHM) != kwParams.end()),
        _keepDimensions(false),
        _bloomFilterSize(33554467), //about 4MB, why not?
        _filterExpressionString(""),
        _filterExpression(NULL),
        _leftOuter(false),
        _rightOuter(false),
        _outNames(0)
    {
        string const outNamesHeader                = "out_names=";
        size_t const nParams = operatorParameters.size();

        setKeywordParamInt64(kwParams, KW_LEFT_IDS, &Settings::setParamLeftIds);
        setKeywordParamInt64(kwParams, KW_RIGHT_IDS, &Settings::setParamRightIds);
        setKeywordParamJoinField(kwParams, KW_LEFT_NAMES, &Settings::setParamLeftNames);
        setKeywordParamJoinField(kwParams, KW_RIGHT_NAMES, &Settings::setParamRightNames);
        setKeywordParamInt64(kwParams, KW_HASH_JOIN_THRES, &Settings::setParamHashJoinThreshold);
        setKeywordParamInt64(kwParams, KW_CHUNK_SIZE, &Settings::setParamChunkSize);
        setKeywordParamString(kwParams, KW_ALGORITHM, &Settings::setParamAlgorithm);
        setKeywordParamBool(kwParams, KW_KEEP_DIMS, _keepDimensions);
        setKeywordParamInt64(kwParams, KW_BLOOM_FILT_SZ, &Settings::setParamBloomFilterSize);
        setKeywordParamBool(kwParams, KW_LEFT_OUTER, _leftOuter);
        setKeywordParamBool(kwParams, KW_RIGHT_OUTER, _rightOuter);
        setKeywordParamJoinField(kwParams, KW_OUT_NAMES, &Settings::setParamOutNames);
        setKeywordParamString(kwParams, KW_FILTER, &Settings::setParamFilterExpression);

        verifyInputs();
        mapAttributes();
        checkOutputNames();
        compileExpression(query, kwParams);
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
        LOG4CXX_DEBUG(logger, "Verifying inputs.");
        throwIf(_leftIds.size() && _leftNames.size(),     "both left_ids and left_names are set; use one or the other");
        throwIf(_rightIds.size() && _rightNames.size(),   "both right_ids and right_names are set; use one or the other");
        LOG4CXX_DEBUG(logger, "Left id size: " << _leftIds.size());
        LOG4CXX_DEBUG(logger, "Left names size: " << _leftNames.size());
        LOG4CXX_DEBUG(logger, "Right id size: " << _rightIds.size());
        LOG4CXX_DEBUG(logger, "Right names size: " << _rightNames.size());
        throwIf(_leftIds.size() == 0 && _leftNames.size() == 0,   "no left join-on fields provided");
        throwIf(_rightIds.size() == 0 && _rightNames.size() == 0, "no right join-on fields provided");
        if(_leftNames.size())
        {
            for(size_t i=0; i<_leftNames.size(); ++i)
            {
                string const& name = _leftNames[i];
                bool found = false;
                for(const auto& attr : _leftSchema.getAttributes())
                {
                    LOG4CXX_DEBUG(logger, "Testing left size for: " << attr.getName());
                    if(attr.getName() == name)
                    {
                        if(!found)
                        {
                            _leftIds.push_back(attr.getId());
                            found = true;
                        }
                        else
                        {
                            ostringstream err;
                            err<<"Left join field '"<<name<<"' is ambiguous; use ids or cast";
                            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << err.str().c_str();
                        }
                    }
                }
                for(size_t j = 0; j<_numLeftDims; ++j)
                {
                    DimensionDesc const& dim = _leftSchema.getDimensions()[j];
                    LOG4CXX_DEBUG(logger, "EJ checking " << name << " against " << dim.getBaseName());
                    if(dim.getBaseName() == name)
                    {
                        if(!found)
                        {
                            _leftIds.push_back(j+_numLeftAttrs);
                            found = true;
                        }
                        else
                        {
                            ostringstream err;
                            err<<"Left join field '"<<name<<"' is ambiguous; use ids or cast";
                            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << err.str().c_str();
                        }
                    }
                }
                if(!found)
                {
                    ostringstream err;
                    err<<"Left join field '"<<name<<"' not found in the left array";
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << err.str().c_str();
                }
            }
        }
        if(_rightNames.size())
        {
            for(size_t i=0; i<_rightNames.size(); ++i)
            {
                string const& name = _rightNames[i];
                bool found = false;
                for(const auto& attr : _rightSchema.getAttributes())
                {
                    if(attr.getName() == name)
                    {
                        if(!found)
                        {
                            _rightIds.push_back(attr.getId());
                            found = true;
                        }
                        else
                        {
                            ostringstream err;
                            err<<"Right join field '"<<name<<"' is ambiguous; use ids or cast";
                            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << err.str().c_str();
                        }
                    }
                }
                for(size_t j = 0; j<_numRightDims; ++j)
                {
                    DimensionDesc const& dim = _rightSchema.getDimensions()[j];
                    if(dim.getBaseName() == name)
                    {
                        if(!found)
                        {
                            _rightIds.push_back(j+_numRightAttrs);
                            found = true;
                        }
                        else
                        {
                            ostringstream err;
                            err<<"Right join field '"<<name<<"' is ambiguous; use ids or cast";
                            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << err.str().c_str();
                        }
                    }
                }
                if(!found)
                {
                    ostringstream err;
                    err<<"Right join field '"<<name<<"' not found in the right array";
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << err.str().c_str();
                }
            }
        }
        throwIf(_leftIds.size() != _rightIds.size(),    "mismatched numbers of join-on fields provided");
        for(size_t i =0; i<_leftIds.size(); ++i)
        {
            size_t leftKey  = _leftIds[i];
            size_t rightKey = _rightIds[i];
            LOG4CXX_DEBUG(logger, "EJ leftKey is " << leftKey);
            throwIf(leftKey  >= _numLeftAttrs + _numLeftDims,  "left id out of bounds");
            throwIf(rightKey >= _numRightAttrs + _numRightDims, "right id out of bounds");
            TypeId leftType   = leftKey  < _numLeftAttrs  ? _leftSchema.getAttributes(true).findattr(leftKey).getType()   : TID_INT64;
            TypeId rightType  = rightKey < _numRightAttrs ? _rightSchema.getAttributes(true).findattr(rightKey).getType() : TID_INT64;
            throwIf(leftType != rightType, "key types do not match");
        }
        throwIf( _algorithmSet && _algorithm == HASH_REPLICATE_LEFT  && isLeftOuter(),  "left replicate algorithm cannot be used for left  outer join");
        throwIf( _algorithmSet && _algorithm == HASH_REPLICATE_RIGHT && isRightOuter(), "right replicate algorithm cannot be used for right outer join");
    }

    void mapAttributes()
    {
        _numKeys = _leftIds.size();
        _leftMapToTuple.resize(_numLeftAttrs + _numLeftDims, -1);
        _rightMapToTuple.resize(_numRightAttrs + _numRightDims, -1);
        for(size_t i =0; i<_numKeys; ++i)
        {
            size_t leftKey  = _leftIds[i];
            size_t rightKey = _rightIds[i];
            throwIf(_leftMapToTuple[leftKey] != -1, "left keys not unique");
            throwIf(_rightMapToTuple[rightKey] != -1, "right keys not unique");
            _leftMapToTuple[leftKey]   = i;
            _rightMapToTuple[rightKey] = i;
            TypeId leftType   = leftKey  < _numLeftAttrs  ? _leftSchema.getAttributes(true).findattr(leftKey).getType()   : TID_INT64;
            bool leftNullable  = leftKey  < _numLeftAttrs  ?  _leftSchema.getAttributes(true).findattr(leftKey).isNullable()   : false;
            bool rightNullable = rightKey < _numRightAttrs  ? _rightSchema.getAttributes(true).findattr(rightKey).isNullable() : false;
            _keyComparators.push_back(AttributeComparator(leftType));
            _keyNullable.push_back( leftNullable || rightNullable );
        }
        size_t j=_numKeys;
        for(size_t i =0; i<_numLeftAttrs + _numLeftDims; ++i)
        {
            if(_leftMapToTuple[i] == -1 && (i<_numLeftAttrs || _keepDimensions))
            {
                _leftMapToTuple[i] = j++;
            }
        }
        _leftTupleSize = j;
        j = _numKeys;
        for(size_t i =0; i<_numRightAttrs + _numRightDims; ++i)
        {
            if(_rightMapToTuple[i] == -1 && (i<_numRightAttrs || _keepDimensions))
            {
                _rightMapToTuple[i] = j++;
            }
        }
        _rightTupleSize = j;
    }

    void checkOutputNames()
    {
        if(_outNames.size())
        {
            if(_outNames.size() != getNumOutputAttrs())
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Incorrect number of output names provided";
            }
            for(size_t i =0; i<_outNames.size(); ++i)
            {
                string const& t = _outNames[i];
                if(t.size()==0)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Improper output names provided";
                }
                for(size_t j=0; j<t.size(); ++j)
                {
                    char ch = t[j];
                    if( !( (j == 0 && ((ch>='a' && ch<='z') || (ch>='A' && ch<='Z') || ch == '_')) ||
                           (j > 0  && ((ch>='a' && ch<='z') || (ch>='A' && ch<='Z') || (ch>='0' && ch <= '9') || ch == '_' ))))
                    {
                        ostringstream error;
                        error<<"invalid name '"<<t<<"'";
                        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << error.str();;
                    }
                }
            }
        }
    }

    void compileExpression(shared_ptr<Query>& query, KeywordParameters const& kwParams)
    {
        Parameter kwParam = getKeywordParam(kwParams, KW_FILTER);

        if (kwParam) {
            ArrayDesc inputDesc = getOutputSchema(query);
            vector<ArrayDesc> inputDescs;
            inputDescs.push_back(inputDesc);
            ArrayDesc outputDesc =inputDesc;

            shared_ptr<LogicalExpression> lExpr = parseExpression(_filterExpressionString);

            _filterExpression.reset(new Expression());
            _filterExpression->compile(lExpr, false, TID_BOOL, inputDescs, outputDesc);

//            } else if(kwParam->getParamType() == PARAM_PHYSICAL_EXPRESSION) {
//                string filter = ((std::shared_ptr<OperatorParamPhysicalExpression>&)kwParam)->getExpression()->evaluate().getString();
//                LOG4CXX_DEBUG(logger, "EJ physical filter is: " << filter);
//                shared_ptr<LogicalExpression> lExp = parseExpression("a=c");

//                auto param = dynamic_pointer_cast<OperatorParamPhysicalExpression>(kwParam);
//                auto lExp = dynamic_pointer_cast<LogicalExpression>(param->getExpression());
//                _filterExpression=lExp;

//                _filterExpression.reset(new Expression());
//                _filterExpression->compile(lExp, false, TID_BOOL, inputDescs, outputDesc);
//            }
        }
    }

    void logSettings()
    {
        ostringstream output;
        for(size_t i=0; i<_numKeys; ++i)
        {
            output<<_leftIds[i]<<"->"<<_rightIds[i]<<" ";
        }
        output<<"buckets "<< _numHashBuckets;
        output<<" chunk "<<_chunkSize;
        output<<" keep_dimensions "<<_keepDimensions;
        output<<" bloom filter size "<<_bloomFilterSize;
        output<<" left outer "<<_leftOuter;
        output<<" right outer "<<_rightOuter;
        LOG4CXX_DEBUG(logger, "EJ keys "<<output.str().c_str());
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

    size_t getNumLeftDims() const
    {
        return _numLeftDims;
    }

    size_t getNumRightAttrs() const
    {
        return _numRightAttrs;
    }

    size_t getNumRightDims() const
    {
        return _numRightDims;
    }

    size_t getNumOutputAttrs() const
    {
        return _leftTupleSize + _rightTupleSize - _numKeys;
    }

    size_t getLeftTupleSize() const
    {
        return _leftTupleSize;
    }

    size_t getRightTupleSize() const
    {
        return _rightTupleSize;
    }

    size_t getNumHashBuckets() const
    {
        return _numHashBuckets;
    }

    size_t getChunkSize() const
    {
        return _chunkSize;
    }

    bool isLeftKey(size_t const i) const
    {
        if(_leftMapToTuple[i] < 0)
        {
            return false;
        }
        return static_cast<size_t>(_leftMapToTuple[i]) < _numKeys;
    }

    bool isRightKey(size_t const i) const
    {
        if(_rightMapToTuple[i] < 0)
        {
            return false;
        }
        return static_cast<size_t>(_rightMapToTuple[i]) < _numKeys;
    }

    bool isKeyNullable(size_t const keyIdx) const
    {
        return _keyNullable[keyIdx];
    }

    ssize_t mapLeftToTuple(size_t const leftField) const
    {
        return _leftMapToTuple[leftField];
    }

    ssize_t mapRightToTuple(size_t const rightField) const
    {
        return _rightMapToTuple[rightField];
    }

    ssize_t mapLeftToOutput(size_t const leftField) const
    {
        return _leftMapToTuple[leftField];
    }

    ssize_t mapRightToOutput(size_t const rightField) const
    {
        if(_rightMapToTuple[rightField] == -1)
        {
            return -1;
        }
        return  isRightKey(rightField) ? _rightMapToTuple[rightField] : _rightMapToTuple[rightField] + _leftTupleSize - _numKeys;
    }

    bool keepDimensions() const
    {
        return _keepDimensions;
    }

    vector <AttributeComparator> const& getKeyComparators() const
    {
        return _keyComparators;
    }

    algorithm getAlgorithm() const
    {
        return _algorithm;
    }

    bool algorithmSet() const
    {
        return _algorithmSet;
    }

    size_t getHashJoinThreshold() const
    {
        return _hashJoinThreshold;
    }

    size_t getBloomFilterSize() const
    {
        return _bloomFilterSize;
    }

    shared_ptr<Expression> const& getFilterExpression() const
    {
        return _filterExpression;
    }

    bool isLeftOuter() const
    {
        return _leftOuter;
    }

    bool isRightOuter() const
    {
        return _rightOuter;
    }

    ArrayDesc const& getLeftSchema() const
    {
        return _leftSchema;
    }

    ArrayDesc const& getRightSchema() const
    {
        return _rightSchema;
    }

    ArrayDesc getOutputSchema(shared_ptr< Query> const& query) const
    {
        Attributes outputAttributes;
        std::vector<AttributeDesc> tmpOutput(getNumOutputAttrs());
        ArrayDesc const& leftSchema = getLeftSchema();
        size_t const numLeftAttrs = getNumLeftAttrs();
        size_t const numLeftDims  = getNumLeftDims();
        ArrayDesc const& rightSchema = getRightSchema();
        size_t const numRightAttrs = getNumRightAttrs();
        size_t const numRightDims  = getNumRightDims();
        size_t i = 0;
        for(const auto& input : _leftSchema.getAttributes(true))
        {
            AttributeID destinationId = mapLeftToOutput(i);
            uint16_t flags = input.getFlags();
            if( isRightOuter() || (isLeftKey(i) && isKeyNullable(destinationId)))
            {
                flags |= AttributeDesc::IS_NULLABLE;
            }
            string const& name = _outNames.size() ? _outNames[destinationId] : input.getName();
            tmpOutput[destinationId] = AttributeDesc(name, input.getType(), flags, CompressorType::NONE, input.getAliases());
            i++;
        }
        for(size_t i =0; i<numLeftDims; ++i)
        {
            ssize_t destinationId = mapLeftToOutput(i + numLeftAttrs);
            if(destinationId < 0)
            {
                continue;
            }
            DimensionDesc const& inputDim = leftSchema.getDimensions()[i];
            uint16_t flags = 0;
            if( isRightOuter() || (isLeftKey(i + _numLeftAttrs) && isKeyNullable(destinationId))) //is it joined with a nullable attribute?
            {
                flags = AttributeDesc::IS_NULLABLE;
            }
            string const& name = _outNames.size() ? _outNames[destinationId] : inputDim.getBaseName();
            tmpOutput[destinationId] = AttributeDesc(name, TID_INT64, flags, CompressorType::NONE);
        }

        i = 0;
        for(const auto& input : _rightSchema.getAttributes(true))
        {
            if(isRightKey(i)) //already in the schema
            {
                i++;
                continue;
            }
            AttributeID destinationId = mapRightToOutput(i);
            uint16_t flags = input.getFlags();
            if(isLeftOuter())
            {
                flags |= AttributeDesc::IS_NULLABLE;
            }
            string const& name = _outNames.size() ? _outNames[destinationId] : input.getName();
            tmpOutput[destinationId] = AttributeDesc(name, input.getType(), flags, CompressorType::NONE, input.getAliases());
            i++;
        }
        for(size_t i =0; i<numRightDims; ++i)
        {
            ssize_t destinationId = mapRightToOutput(i + _numRightAttrs);
            if(destinationId < 0 || isRightKey(i + _numRightAttrs))
            {
                continue;
            }
            DimensionDesc const& inputDim = rightSchema.getDimensions()[i];
            string const& name = _outNames.size() ? _outNames[destinationId] : inputDim.getBaseName();
            tmpOutput[destinationId] = AttributeDesc(name, TID_INT64, isLeftOuter() ? AttributeDesc::IS_NULLABLE : 0, CompressorType::NONE);
        }
        for (size_t i = 0; i < getNumOutputAttrs(); ++i) {
            const AttributeDesc pushable(tmpOutput[i]);
            outputAttributes.push_back(pushable);
        }
        outputAttributes.addEmptyTagAttribute();

        Dimensions outputDimensions;
        outputDimensions.push_back(DimensionDesc("instance_id", 0, _numInstances-1,            1,          0));
        outputDimensions.push_back(DimensionDesc("value_no",    0, CoordinateBounds::getMax(), _chunkSize, 0));
        return ArrayDesc("equi_join", outputAttributes, outputDimensions, createDistribution(dtUndefined), query->getDefaultArrayResidency());
    }

};

}


} //namespaces

#endif //EQUI_JOIN_SETTINGS
