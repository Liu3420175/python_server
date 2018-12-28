#!/usr/bin/env python3
#-*- coding:utf-8 -*-
# author:liulh01@mingyuanyun.com
# datetime:18-12-25 下午2:04
# software: PyCharm

from datasource.sql_parser import SQLDataQueryParser
from datasource.base_models import LimitOffset,Property
from datasource.querystructure import QueryStructre
from datasource.parse_json import BaseStringToStruct,StringToStruct,JsonParse

import json

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
    select = [
        {
        "query_name":"user",
        "prop_name":"id"
    },{"query_name":"user",
       "prop_name":"name"},
        {"query_name": "user",
         "prop_name": "email",
         "operator":"+",
         "props":[{
             "query_name": "user",
             "prop_name": "id"
        }],
         },
        {"value":"10"},
        {"case_when":[
            {"condition":{
                "left":{"query_name":"xxx","prop_name":"pv"},
                "right":{"value":"1000"},
                "operator":"<="
            },"display":"好的"
            },{
                "condition": None, "display": "其它"
            }
        ]}
    ]
    limit = {"offset":(data.get("page",1) - 1) * data.get("limit",10),
             "row":data.get("limit",10)}
    Limit_object = LimitOffset(**limit)

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

    table = [{"name":"user_table","alias":"user",},

             ]
    #se = Property(**select[-1])
    #t = QueryObject(**table[-1])
    #query_struct = QueryStructre(select=select,query_object=table,where=conditions,limit=limit)
    #sql = SQLDataQueryParser(query_struct,data)
    #print(query_struct.query_object[1])
    #s = BaseStringToStruct("计算方式(可选)@字段名$in(1,2,3,4)","table")
    #s = BaseStringToStruct("字段名$lt(范围)","table")
    #s = BaseStringToStruct("字段名$desc", "table")
    #s = BaseStringToStruct("100", "table")
    #s = StringToStruct("别名(可选):计算方式(可选)@字段名$eq(100)#+#计算方式(可选)@字段名$gt(1000)","user")
    #s = StringToStruct("别名(可选):字段名", "user")
    #s = StringToStruct("字段名", "user")
    #print(sql.parser())

    json_data = """
    {
    "analysis_model":"page_view",
    "product_code":"xxxx",
    "app_code":"",
    "tenant_code":"",
    "from_date": "2018-12-20",
    "to_date": "2018-12-20",
    "date_unit":"hour",
    "dim_fields": ["product", "os_name"],
    "num_fields":["product",
        "app",
        "pv:COUNT@app",
        "div_10:response_time#/#10",
        "Over_1800_Rate:SUM@response_time$lt(1800)#/#COUNT@response_time"
        ],
    "filter": ["app$in(fast,yunlian)","browser_name$eq(chrome)","os_name$rlike(Windowns)","title$like(标题)"],
	"order_by": ["product$desc"],
     "page": 2,
	 "page_size":10
}
"""

    j = JsonParse(json_data)
    print(j.sql())