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

#define LEGACY_API
#include "query/LogicalOperator.h"

#include "EquiJoinSettings.h"
#include "ArrayIO.h"

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
    }

    static PlistSpec const* makePlistSpec()
    {
        static PlistSpec argSpec {
            { "", // positionals
              RE(RE::LIST, {
                 RE(PP(PLACEHOLDER_INPUT)),
                 RE(PP(PLACEHOLDER_INPUT)),
                 RE(RE::STAR, {
                    RE(PP(PLACEHOLDER_CONSTANT, TID_STRING))
                 })
              })
            },
            { KW_LEFT_IDS, RE(RE::OR, {
                              RE(PP(PLACEHOLDER_EXPRESSION, TID_INT64)),
                              RE(RE::GROUP, {
                                     RE(PP(PLACEHOLDER_EXPRESSION, TID_INT64)),
                                     RE(RE::PLUS, {
                                        RE(PP(PLACEHOLDER_EXPRESSION, TID_INT64))
                                     })
                                 })
                              })
            },
            { KW_RIGHT_IDS, RE(RE::OR, {
                              RE(PP(PLACEHOLDER_EXPRESSION, TID_INT64)),
                              RE(RE::GROUP, {
                                     RE(PP(PLACEHOLDER_EXPRESSION, TID_INT64)),
                                     RE(RE::PLUS, {
                                        RE(PP(PLACEHOLDER_EXPRESSION, TID_INT64))
                                     })
                                  })
                             })
            },
            { KW_LEFT_NAMES, RE(RE::OR, {
                                RE(RE::OR, {
                                      RE(PP(PLACEHOLDER_DIMENSION_NAME)),
                                      RE(PP(PLACEHOLDER_ATTRIBUTE_NAME))
                                   }),
                                RE(RE::GROUP, {
                                     RE(RE::OR, {
                                        RE(PP(PLACEHOLDER_DIMENSION_NAME)),
                                        RE(PP(PLACEHOLDER_ATTRIBUTE_NAME))
                                     }),
                                     RE(RE::PLUS, {
                                        RE(RE::OR, {
                                           RE(PP(PLACEHOLDER_DIMENSION_NAME)),
                                           RE(PP(PLACEHOLDER_ATTRIBUTE_NAME))
                                        })
                                     })
                                  })
                             })
            },
            { KW_RIGHT_NAMES, RE(RE::OR, {
                              RE(RE::OR, {
                                 RE(PP(PLACEHOLDER_DIMENSION_NAME)),
                                 RE(PP(PLACEHOLDER_ATTRIBUTE_NAME))
                              }),
                              RE(RE::GROUP, {
                                     RE(RE::OR, {
                                         RE(PP(PLACEHOLDER_DIMENSION_NAME)),
                                         RE(PP(PLACEHOLDER_ATTRIBUTE_NAME))
                                     }),
                                     RE(RE::PLUS, {
                                        RE(RE::OR, {
                                           RE(PP(PLACEHOLDER_DIMENSION_NAME)),
                                           RE(PP(PLACEHOLDER_ATTRIBUTE_NAME))
                                        })
                                     })
                                  })
                             })
            },
            { KW_HASH_JOIN_THRES, RE(PP(PLACEHOLDER_CONSTANT, TID_INT64)) },
            { KW_CHUNK_SIZE, RE(PP(PLACEHOLDER_CONSTANT, TID_INT64)) },
            { KW_ALGORITHM, RE(PP(PLACEHOLDER_CONSTANT, TID_STRING)) },
            { KW_KEEP_DIMS, RE(PP(PLACEHOLDER_CONSTANT, TID_BOOL)) },
            { KW_BLOOM_FILT_SZ, RE(PP(PLACEHOLDER_CONSTANT, TID_INT64)) },
            { KW_FILTER, RE(PP(PLACEHOLDER_EXPRESSION, TID_BOOL)) },
            { KW_LEFT_OUTER, RE(PP(PLACEHOLDER_EXPRESSION, TID_BOOL)) },
            { KW_RIGHT_OUTER, RE(PP(PLACEHOLDER_EXPRESSION, TID_BOOL)) },
            { KW_OUT_NAMES, RE(RE::OR, {
                               RE(PP(PLACEHOLDER_ATTRIBUTE_NAME).setMustExist(false)),
                               RE(RE::GROUP, {
                                  RE(PP(PLACEHOLDER_ATTRIBUTE_NAME).setMustExist(false)),
                                  RE(RE::PLUS, {
                                     RE(PP(PLACEHOLDER_ATTRIBUTE_NAME).setMustExist(false))
                                  })
                               })
                            })
             }
        };
        return &argSpec;
    }

    ArrayDesc inferSchema(vector< ArrayDesc> schemas, shared_ptr< Query> query)
    {
        vector<ArrayDesc const*> inputSchemas;

        inputSchemas.push_back(&(schemas[0]));
        inputSchemas.push_back(&(schemas[1]));
        Settings settings(inputSchemas, _parameters, _kwParameters, query);
        return settings.getOutputSchema(query);
    }
};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalEquiJoin, "equi_join");

}
