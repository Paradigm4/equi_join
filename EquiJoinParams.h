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

using std::string;
using std::shared_ptr;
struct EquiJoinParams
{
  string leftIds;
  string rightIds;
  string leftNames;
  string rightNames;
  uint64_t hashJoinThreshold;
  uint64_t chunkSize;
  string algorithm;
  bool keepDimensions;
  uint64_t bloomFilterSize ;
  string filter;
  bool filterResult;
  shared_ptr<LogicalExpression>        logFilterExpression;
  shared_ptr<Expression>              physFilterExpression;
  bool leftOuter;
  bool rightOuter;
  string outNames;
  bool leftIdsSet            = false;
  bool rightIdsSet           = false;
  bool leftNamesSet          = false;
  bool rightNamesSet         = false;
  bool hashJoinThresholdSet  = false;
  bool chunkSizeSet          = false;
  bool algorithmSet          = false;
  bool keepDimensionsSet     = false;
  bool bloomFilterSizeSet    = false;
  bool filterSet	     = false;
  bool leftOuterSet          = false;
  bool rightOuterSet         = false;
  bool outNamesSet           = false;
};

