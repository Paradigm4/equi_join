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

#include "query/Operator.h"

#include "EquiJoinSettings.h"

namespace scidb
{

using namespace std;
using namespace equi_join;

class LogicalEquiJoin : public LogicalOperator
{
public:
    LogicalEquiJoin(const string& logicalName, const string& alias):
        LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_INPUT();
        ADD_PARAM_INPUT();

	addKeywordPlaceholder("left_ids", PARAM_CONSTANT(TID_STRING));
	addKeywordPlaceholder("right_ids", PARAM_CONSTANT(TID_STRING));
	addKeywordPlaceholder("left_names", PARAM_CONSTANT(TID_STRING));
	addKeywordPlaceholder("right_names", PARAM_CONSTANT(TID_STRING));
	addKeywordPlaceholder("hash_join_threshold", PARAM_CONSTANT(TID_UINT64));
	addKeywordPlaceholder("chunk_size", PARAM_CONSTANT(TID_UINT64));
	addKeywordPlaceholder("algorithm", PARAM_CONSTANT(TID_STRING));
	addKeywordPlaceholder("keep_dimensions", PARAM_CONSTANT(TID_BOOL));
	addKeywordPlaceholder("bloom_filter_size", PARAM_CONSTANT(TID_UINT64));
	//	addKeywordPlaceholder("filter", PARAM_EXPRESSION(TID_BOOL));
	addKeywordPlaceholder("filter", PARAM_CONSTANT(TID_STRING));
	addKeywordPlaceholder("left_outer", PARAM_CONSTANT(TID_BOOL));
	addKeywordPlaceholder("right_outer", PARAM_CONSTANT(TID_BOOL));
	addKeywordPlaceholder("out_names", PARAM_CONSTANT(TID_STRING));
    }

    std::vector<shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(const std::vector< ArrayDesc> &schemas)
    {
        std::vector<shared_ptr<OperatorParamPlaceholder> > res;
        res.push_back(END_OF_VARIES_PARAMS());
        if (_parameters.size() < Settings::MAX_PARAMETERS)
        {
            res.push_back(PARAM_CONSTANT("string"));
        }
        return res;
    }

    EquiJoinParams readParameters()
    {
        EquiJoinParams ejp;
	Parameter leftIdsParam = findKeyword("left_ids");
	if (leftIdsParam)
	{
	  ejp.leftIds = evaluate(((std::shared_ptr<OperatorParamLogicalExpression>&)leftIdsParam)->getExpression(), TID_STRING).getString();
	  ejp.leftIdsSet = true;
	}
	Parameter rightIdsParam = findKeyword("right_ids");
        if (rightIdsParam)
        {
	  ejp.rightIds = evaluate(((std::shared_ptr<OperatorParamLogicalExpression>&)rightIdsParam)->getExpression(), TID_STRING).getString();
	  ejp.rightIdsSet = true;
        }
	Parameter leftNamesParam = findKeyword("left_names");
	if (leftNamesParam)
	{
	  ejp.leftNames = evaluate(((std::shared_ptr<OperatorParamLogicalExpression>&)leftNamesParam)->getExpression(), TID_STRING).getString();
	  ejp.leftNamesSet = true;
	}
	Parameter rightNamesParam = findKeyword("right_names");
	if (rightNamesParam)
	{
	  ejp.rightNames = evaluate(((std::shared_ptr<OperatorParamLogicalExpression>&)rightNamesParam)->getExpression(), TID_STRING).getString();
	  ejp.rightNamesSet = true;
	}
	Parameter hashJoinThresholdParam = findKeyword("hash_join_threshold");
	if (hashJoinThresholdParam)
	{
	  ejp.hashJoinThreshold = evaluate(((std::shared_ptr<OperatorParamLogicalExpression>&)hashJoinThresholdParam)->getExpression(), TID_UINT64).getUint64();
	  ejp.hashJoinThresholdSet = true;
	}
	Parameter chunkSizeParam = findKeyword("chunk_size");
	if (chunkSizeParam)
	{
	  ejp.chunkSize = evaluate(((std::shared_ptr<OperatorParamLogicalExpression>&)chunkSizeParam)->getExpression(), TID_UINT64).getUint64();
	  ejp.chunkSizeSet = true;
        }
	Parameter algorithmParam = findKeyword("algorithm");
	if (algorithmParam)
	{
	  ejp.algorithm = evaluate(((std::shared_ptr<OperatorParamLogicalExpression>&)algorithmParam)->getExpression(), TID_STRING).getString();
	  ejp.algorithmSet = true;
	}
	Parameter keepDimensionsParam = findKeyword("keep_dimensions");
	if (keepDimensionsParam)
	{
	  ejp.keepDimensions = evaluate(((std::shared_ptr<OperatorParamLogicalExpression>&)keepDimensionsParam)->getExpression(), TID_BOOL).getBool();
	  ejp.keepDimensionsSet = true;
        }
	Parameter bloomFilterSizeParam = findKeyword("bloom_filter_size");
	if (bloomFilterSizeParam)
        {
	  ejp.bloomFilterSize = evaluate(((std::shared_ptr<OperatorParamLogicalExpression>&)bloomFilterSizeParam)->getExpression(), TID_UINT64).getUint64();
	  ejp.bloomFilterSizeSet = true;
        }
	Parameter filterParam = findKeyword("filter");
	if (filterParam)
	{
	  ejp.filter = evaluate(((std::shared_ptr<OperatorParamLogicalExpression>&)filterParam)->getExpression(), TID_STRING).getString();
	  ejp.filterSet = true;
        }
	Parameter leftOuterParam = findKeyword("left_outer");
	if (leftOuterParam)
        {
	  ejp.leftOuter = evaluate(((std::shared_ptr<OperatorParamLogicalExpression>&)leftOuterParam)->getExpression(), TID_BOOL).getBool();
	  ejp.leftOuterSet = true;
        }
	Parameter rightOuterParam = findKeyword("right_outer");
	if (rightOuterParam)
	{
	  ejp.rightOuter = evaluate(((std::shared_ptr<OperatorParamLogicalExpression>&)rightOuterParam)->getExpression(), TID_BOOL).getBool();
	  ejp.rightOuterSet = true;
        }
	Parameter outNamesParam = findKeyword("out_names");
	if (outNamesParam) 
	{
	  ejp.outNames = evaluate(((std::shared_ptr<OperatorParamLogicalExpression>&)outNamesParam)->getExpression(), TID_STRING).getString();
	  ejp.outNamesSet = true;
        }
	return ejp;
    }

    ArrayDesc inferSchema(vector< ArrayDesc> schemas, shared_ptr< Query> query)
    {
        vector<ArrayDesc const*> inputSchemas;
        inputSchemas.push_back(&(schemas[0]));
        inputSchemas.push_back(&(schemas[1]));

	EquiJoinParams EJParams = readParameters();
        Settings settings(inputSchemas, _parameters, true, query, EJParams);
	
        return settings.getOutputSchema(query);
    }
};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalEquiJoin, "equi_join");

}
