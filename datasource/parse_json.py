#!/usr/bin/env python3
#-*- coding:utf-8 -*-
# author:liulh01@mingyuanyun.com
# datetime:18-12-25 下午4:51
# software: PyCharm

"""
解析json数据，将其转换成SQL查询语句
"""

import json
from datetime import datetime

from datasource.sql_parser import SQLDataQueryParser
from datasource.querystructure import QueryStructre

Operators = {
    "eq" : "=",
    "lt" : "<",
    "lte": "<=",
    "gt" : ">",
    "gte": ">=",
    "in" : "IN",
    "rlike":"LIKE",
    "llike":"LIKE",
    "like":"LIKE"
}


class BaseStringToStruct(object):
    """
    将基本的string转化成结构
    如下四种类型：
        计算方式(可选)@字段名$lg|gt|eq(范围)
        字段名$desc|asc
        字段名$lg|gt|eq(范围,范围)
        常数
        字段名
    """
    __slots__ = ("query_name","prop_name","function","operator","value","origin_string",
                 "error_code","error_msg")

    def __init__(self,
                 origin_string,
                 query_name,
                ):
        """
        Args:
            origin_string:原始字符串
            query_name:表名
            prop_name:表字段名
            function:计算方式
            operator:比较操作符
            value:值，范围，null或者字符串
            error_code:错误代号
            error_msg:错误信息
        """
        self.origin_string = origin_string
        self.query_name = query_name
        self.prop_name = ""
        self.function = ""
        self.operator = ""
        self.value = ""
        self.error_code = 0
        self.error_msg = ""
        self.parse()

    def __str__(self):
        return "BaseStringToStruct(query_name=%s,prop_name=%s,function=%s,operator='%s',value=%s)"%(
            self.query_name,
            self.prop_name,
            self.function,
            self.operator,
            self.value
        )


    def parse(self):
        """
        解析
        """
        jList = self.origin_string.split("@")
        if len(jList) == 1:
            self.function = ""
        elif len(jList) == 2:
            self.function = jList[0].upper()
        else:
            self.error_code = 10001
            self.error_msg = "参数值格式错误"

        fList = jList[-1].split("$")
        if len(fList) == 1:
            #没有$，有可能是直接的字段或者常数
            if fList[0].isdigit():
                self.value = fList[0] #常数
                self.query_name = ""
                self.prop_name = ""
            else:
                self.prop_name = fList[0] #直接字段

        elif len(fList) == 2:
            self.prop_name = fList[0]
            operator,params = self.parse_comparison_string(fList[1])
            self.operator = operator
            self.value = params
        else:
            self.error_code = 10001
            self.error_msg = "参数值格式错误"


    def parse_comparison_string(self, comparison_string):
        """
        解析比较条件字符串，格式：lg|gt|eq(范围,范围) 或者 desc|asc
        Args:
            comparison_string:比较条件字符串
        """
        if comparison_string.endswith(")"):
            fList = comparison_string.split("(")
            if len(fList) != 2:
                return "", ""
            operator = Operators.get(fList[0].lower(), "")
            if operator == "":
                return "", ""
            params = fList[1].strip(")")
            if operator == "IN":
                paramsList = params.split(",")
                params = ",".join(["'%s'"%s for s in paramsList])
                params = "".join(["(",params,")"])
            else:
                params = "'%s'"%params
            if fList[0] == "rlike":
                params = "%%%s"%(params)
            elif fList[0] == "llike":
                params = "%s%%"%(params)
            elif fList[0] == "like":
                params = "%%%s%%"%(params)
            else:
                pass
        else:#解析 AES|DESC
            operator = comparison_string.upper()
            params = ""
        return operator, params



class StringToStruct(object):
    """
    解析如下格式
    别名(可选):计算方式(可选)@字段名$lg|gt|eq(范围)# 操作符 # 计算方式(可选)@字段名$lg|gt|eq(范围)
    别名(可选):计算方式(可选)@字段名
    """
    __slots__ = ("alias","query_name","left","right","operator","origin_string",
                 "error_code","error_msg")

    def __init__(self,origin_string,query_name):
        """

        """
        self.origin_string = origin_string
        self.query_name = query_name
        self.alias = ""
        self.left = None
        self.right = None
        self.operator = ""
        self.error_code = 0
        self.error_msg = ""
        self.parse()


    def __str__(self):
        return "StringToStruct(left=%s,rigit=%s,operator=%s,alias=%s)"%(self.left,self.right,self.operator,self.alias)

    def parse(self):
        aList = self.origin_string.split(":")
        if len(aList) == 1:
            self.alias = ""
        elif len(aList) == 2:
            self.alias = aList[0]
        else:
            self.error_code = 10001
            self.error_msg = "参数值格式错误"
        cList = aList[-1].split("#")
        if len(cList) == 1:
            self.left = BaseStringToStruct(cList[0],self.query_name)
            self.right = None
            self.operator = ""
        elif len(cList) == 3:
            self.left = BaseStringToStruct(cList[0],self.query_name)
            self.right = BaseStringToStruct(cList[-1],self.query_name)
            self.operator = cList[1]
        else:
            self.error_code = 10001
            self.error_msg = "参数值格式错误"



class JsonParse(object):
    """
    json数据解析
    """
    __slots__ = ("params",)

    def __init__(self,json_data):
        self.params = self.json_to_object(json_data)


    def __str__(self):
        return "JsonParse"


    def sql(self):
        """
        输出SQL
        """
        table_name = self.parseTableName()
        select = self.parseColumnFields(table_name)
        query_name = self.parseTable(table_name)
        where = self.parseFilterFields(table_name)
        partition = self.parseTablePartition(table_name)
        partition.extend(where)
        group_by = self.parseGroupByFields(table_name)
        order_by = self.parseOrderByFields(table_name)
        limit = self.parseLimitOffset()
        query_struct = QueryStructre(select=select,
                                     query_object=query_name,
                                     where=partition,
                                     limit=limit,
                                     group_by=group_by,
                                     order_by=order_by
                                    )
        s = SQLDataQueryParser(query_struct, self.params)
        return s.parser()[0]


    def json_to_object(self,json_data):
        """
        将json数据转换成Python对象
        """
        try:
            data = json.loads(json_data)
        except:
            data = {}
        return data

    def parseTableName(self):
        """
        解析表名
        """
        analysis_model = self.params.get("analysis_model","")
        product_code  = self.params.get("product_code","")
        app_code = self.params.get("app_code","")
        tenant_code = self.params.get("tenant_code","")
        date_unit = self.params.get("date_unit","day")

        product = "product" if product_code else ""
        apps = "app" if app_code else ""
        tenant = "tenant" if tenant_code else ""
        table_name = "_".join(filter(lambda one:len(one) > 0,[analysis_model,product,apps,tenant,date_unit]))
        return table_name


    def parseTable(self,table):
        query_name = [{"name": table}]
        return query_name


    def parseTablePartition(self,table):
        """
        解析分区条件
        分区条件应该放在where子句最前面
        """
        # 解析时间分区
        partition_cond = []
        today = datetime.today()
        today = today.strftime("%Y-%m-%d")
        from_date = self.params.get("from_date",today)
        to_date = self.params.get("to_date",today)
        from_date = from_date.strip()
        to_date = to_date.strip()
        if from_date == to_date:
            cond = self.parse_partition(table,"dt",from_date,"=","AND")
            partition_cond.append(cond)
        else:
            cond = [self.parse_partition(table,"dt",from_date,">=","AND"),
                    self.parse_partition(table,"dt",to_date,"<=","AND")]
            partition_cond.extend(cond)

        # 解析 产品
        product_code = self.params.get("product_code","")
        product_code = product_code.strip()
        if product_code:
            cond = self.parse_partition(table,"product",product_code,"=","AND")
            partition_cond.append(cond)

        app_code = self.params.get("app_code","")
        app_code = app_code.strip()
        if app_code:
            cond = self.parse_partition(table,"app",app_code,"=","AND")
            partition_cond.append(cond)

        tenant_code = self.params.get("tenant_code","")
        tenant_code = tenant_code.strip()
        if tenant_code:
            cond = self.parse_partition(table, "tenant", tenant_code, "=", "AND")
            partition_cond.append(cond)

        return partition_cond


    def parseGroupByFields(self,table):
        """
        解析group by 参数
        """
        dim_fields = self.params.get("dim_fields",[])
        dim_map_fields = [BaseStringToStruct(dim_field,table) for dim_field in dim_fields]
        group_bys = [{"query_name":dim.query_name,"prop_name":dim.prop_name} for dim in dim_map_fields]
        return group_bys


    def parseColumnFields(self,table):
        """
        解析展示列参数
        返回值有两个：一是展示的列；另一个是阈值条件
        """
        num_fields = self.params.get("num_fields",[])
        num_fields = [StringToStruct(s,table) for s in num_fields]
        column_list = []
        for obj in num_fields:
            left,right = obj.left , obj.right

            if left and right:
                #模型   别名(可选):计算方式(可选)@字段名$lg|gt|eq(范围)# 操作符 # 计算方式(可选)@字段名$lg|gt|eq(范围)
                d1 = self.parse_column_field(left,obj.alias)
                d2 = self.parse_column_field(right,None)
                if d1.get("operator",""):
                    #说明有阀值条件,用case-when
                    self.parse_case_when(d1, table, left)

                d1["operator"] = None
                d2["operator"] = obj.operator
                d1["value"] = None
                d1["props"] = [d2]
                column_list.append(d1)

            elif left and not right:
                #别名(可选):计算方式(可选)@字段名$lg|gt|eq(范围)
                d1 = self.parse_column_field(left, obj.alias)
                if d1.get("operator", ""):
                    # 说明有阀值条件
                    self.parse_case_when(d1, table, left)

                d1["operator"] = None
                d1["value"] = None
                column_list.append(d1)
            else:
                raise Exception("参数值解析错误")
        return column_list


    def parseFilterFields(self,table):
        """
        解析过滤条件
        """
        filters = self.params.get("filter",[])
        conds = [self.parse_filter_field(table,cond) for cond in filters]
        conds = filter(lambda x:len(x) > 0,conds)
        return list(conds)


    def parseOrderByFields(self,table):
        """
        解析排序参数
        """
        order_by = self.params.get("order_by",[])
        order_by = [BaseStringToStruct(order,table) for order in order_by]
        order_bys = [{"query_name": order.query_name, "prop_name": order.prop_name, "method": order.operator}
                     for order in order_by]
        return order_bys


    def parseLimitOffset(self):
        """
        解析limit
        """
        limit = {
            "offset": (self.params.get("page", 1) - 1) * self.params.get("page_size", 10),
            "row": self.params.get("page_size", 10)
        }
        return limit



    def parse_column_field(self,obj,alias):
        if not isinstance(obj,BaseStringToStruct):
            return {}
        column = dict(query_name=obj.query_name,
                 prop_name=obj.prop_name,
                 alias=alias,
                 func=obj.function,
                 specifier=None,
                 value=obj.value,
                 operator=obj.operator,
                      )
        column = {k:v for k,v in column.items() if v}
        return column



    def parse_filter_field(self,table,s):
        """
        解析一个过滤条件参数
        Args:
            table: 表名
            s:字符串，格式如下"字段名$lg|gt|eq(范围,范围)
        """
        if not isinstance(s,str):
            return {}
        bs = BaseStringToStruct(s,table)
        condition = {
            "left":{
                "query_name":bs.query_name,
                "prop_name":bs.prop_name
            },
            "right":{
                "value":bs.value
            },
            "operator":bs.operator,
            "logical_relation":"AND"
        }
        return condition

    def parse_case_when(self,d,table,obj):
        """
        设置case-when语句
        """
        if isinstance(d,dict) and isinstance(obj,BaseStringToStruct):
            cond1 = {
                "left": {
                    "query_name": table,
                    "prop_name": obj.prop_name
                },
                "right": {
                    "value": obj.value
                },
                "operator": obj.operator,

            }
            d.setdefault("case_when", [{"condition": cond1, "display": "1"}, {"condition": None, "display": "0"}])


    def parse_partition(self,table,prop_name,value,operator,logical):
        """
        分区条件
        """
        cond = {
            "left": {"query_name": table, "prop_name": prop_name},
            "right": {"value": "'%s'"%value},
            "operator": operator,
            "logical_relation": logical
        }
        return cond