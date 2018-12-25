#!/usr/bin/env python3
#-*- coding:utf-8 -*-
# author:liulh01@mingyuanyun.com
# datetime:18-12-25 下午2:04
# software: PyCharm

from datasource.sql_parser import SQLDataQueryParser
#from datasource.base_models import LimitOffset,Condition,QueryObject
from datasource.querystructure import QueryStructre


if __name__ == "__main__":
    data = {
    "analysis_model":"pageview",
    "prodcut_code":"xxx",
    "app_code":"xxx",
    "tenant_code":"xxx",
    "from_date": "2018-12-20",
    "to_date": "2018-12-20",
    "date_unit":"hour,day,week,month,year",
    "dim_fields": ["pageview.title", "pageview.country"],
    "num_fields":["pageview.count"],
    "filter": {
         "relation": "and",
         "conditions": [{
              "field": "pageview.device_screen_width",
              "function": "<=",
              "params": ["1900"]

         }, {
             "field": "pageview.title",
             "function": "=",
             "params": ["明源云-首页"]
         }]
	},
     "order_by": [{
       "field": "pageview.country",
       "method": "AES"
     },{
       "field": "pageview.pv",
       "method": "DESC"
     }],

     "page": 1,
     "limit": 10
    }
    select = [{
        "query_name":"user",
        "prop_name":"id"
    },{"query_name":"user",
       "prop_name":"name"},
        {"query_name": "user",
         "prop_name": "email"},
    ]
    limit = {"offset":(data.get("page",1) - 1) * data.get("limit",10),
             "row":data.get("limit",10)}
    #Limit_object = LimitOffset(**limit)
    data_filter = data.get("filter",{})
    relation = data_filter.get("relation")
    conds = data_filter.get("conditions",[])
    conditions = [{"left":{"query_name":"xxx",
                           "prop_name":cond.get("field","")},
                   "right":{"value":cond.get("params",["0"])[0]},
                   "operator":cond.get("function"),
                   "logical_relation":relation}  for cond in conds]
    if conditions:
        conditions[0]["logical_relation"] = None

    #c = Condition(**conditions[0])

    table = [{"name":"user_table","alias":"user","join_type":None},
             {"name": "order_table", "alias": "order", "join_type": "INDER",
              "join_conditions":[{"left":{"query_name":"user",
                           "prop_name":"id"},
                   "right":{"query_name":"order","prop_name":"user_id"},
                   "operator":"=",
                   "logical_relation":None}]}
             ]
    #t = QueryObject(**table[-1])
    query_struct = QueryStructre(select=select,query_object=table,where=conditions,limit=limit)
    sql = SQLDataQueryParser(query_struct,data)
    #print(query_struct.query_object[1])
    print(sql.parser())