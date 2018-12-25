#!/usr/bin/env python3
#-*- coding:utf-8 -*-
# author:liulh01@mingyuanyun.com
# datetime:18-12-24 下午4:12
# software: PyCharm

from collections import Iterable

from datasource.base_models import Property,CaseObject,Condition,QueryObject,OrderBy,LimitOffset
from datasource.querystructure import QueryStructre

class SQLDataQueryParser(object):
    def __init__(self,querystructure,params):
        if not isinstance(querystructure,QueryStructre):
            raise Exception("初始化变量必须为QueryStructre对象")
        self.querystucture = querystructure
        self.params = params


    def parser(self):
        """

        :return:
        """
        select = self.parserSelect()
        if not select:
            raise Exception("缺少Select部分")
        table = self.parserTable()
        if not table:
            raise Exception("缺少table部分")
        sql_list = ["SELECT {0} FROM {1}".format(select,table)]

        where = self.parserWhere()
        if where:
            sql_list.append("WHERE {0}".format(where))

        groupby = self.parserGroupBy()
        if groupby:
            sql_list.append("GROUP BY {0}".format(groupby))

        having = self.parserHaving()
        if having:
            sql_list.append("HAVING {0}".format(having))

        order_by = self.parserOrderBy()
        if order_by:
            sql_list.append("ORDER BY {0}".format(order_by))

        limit = self.parserLimit()
        sql_list.append(limit)
        return " ".join(sql_list),self.params


    def parserSelect(self):
        """
        解析Select部分
        """
        selects = self.querystucture.select
        if not selects:
            # TODO 是报异常好呢还是用*代替
            raise Exception("Select 部分缺失")
        res = [self.parserColumnContent(prop) for prop in selects]
        return ",".join(res)



    def parserTable(self):
        """
        解析表名
        """
        tables = self.querystucture.query_object
        if not tables:
            raise Exception("表名缺失")
        res = [self.parserQueryObjectContent(query) for query in tables]
        return "".join(res)


    def parserWhere(self):
        """
        解析where子句

        """
        wheres = self.querystucture.where
        res = ["1"]
        if wheres:
            res = [self.parserConditionContent(cond) for cond in wheres]
        return "".join(res)


    def parserGroupBy(self):
        """
        解析group by

        """
        group_bys = self.querystucture.group_by
        res = []
        if group_bys:
            res = [self.parserColumnContent(prop) for prop in group_bys]
        return ",".join(res)


    def parserOrderBy(self):
        """
        解析order by
        """
        order_bys = self.querystucture.order_by
        res = []
        if order_bys:
            res = [self.parserOrderByContent(order) for order in order_bys]
        return ",".join(res)



    def parserLimit(self):
        """
        解析limit
        """
        limit = self.querystucture.limit
        return "LIMIT {0}".format(self.parseLimitOffsetContent(limit)) if limit else ""


    def parserHaving(self):
        """
        解析having
        """
        havings = self.querystucture.having
        res = []
        if havings:
            res = [self.parserConditionContent(cond) for cond in havings]
        return "".join(res)



    def parserColumnContent(self,prop):
        """
        解析Property
        """
        if not isinstance(prop,Property):
            raise Exception("必须是Property类型") # TODO 有待改进，需要考虑None的情况
        str_list = []
        if prop.func:
            str_list.append("{0}".format(prop.func))

        if prop.specifier:
            str_list.append("{0} ".format(prop.specifier))

        if prop.props:
            str_list.append("(")
            for p in prop.props:
                str_list.append(self.parserColumnContent(p))
            str_list.append(")")


        if prop.case_when and isinstance(prop.case_when,list):
            l = len(prop.case_when)
            str_list.append(" CASE {0} {1} END".format(
                " ".join([self.parserCaseWhenContent(case) for case in prop.case_when[l - 1]]),
                   self.parserCaseWhenContent(prop.case_when[-1]))
            )


        if prop.prop_name:
            if prop.query_name:
                str_list.append("{0}.{1}".format(prop.query_name,prop.prop_name))

            if prop.operator and prop.value:
                str_list.append(" {0} {1}".format(prop.operator,prop.value))

        elif  not prop.func and not prop.props:
            # 将Property当作值，没有表表名和字段名
            if prop.value is None:
                str_list.append(" NULL")
            elif isinstance(prop.value,Iterable):
                str_list.append("({0})".format(",".join(prop.value)))

        if prop.alias:
            str_list.append(" AS {0} ".format(prop.alias))

        return "".join(str_list)


    def parserCaseWhenContent(self,case):
        """
        解析CaseObject

        """
        str_list = []
        if not isinstance(case,CaseObject):
            raise Exception("必须是CaseObject对象")
        if case.condition is None:
            expression = "ELSE"
            str_list.append("{0} {1}".format(expression, case.display))
        else:
            expression = self.parserConditionContent(case.condition)
            str_list.append(" WHEN {0} THEN {1}".format(expression,case.display))
        return "".join(str_list)


    def parserConditionContent(self,condition):
        """
         解析Condition
        """
        str_list = []

        if not isinstance(condition,Condition):
            raise Exception("必须是Condition对象")
        if condition.logical_relation:
            str_list.append(" {0}".format(condition.logical_relation))

        if condition.conditions and isinstance(condition.conditions,Iterable):
            str_list.append(" ({0})".format([self.parserColumnContent(cond) for cond in condition.conditions if cond]))
        else:
            if not condition.right or not condition.left or not condition.operator:
                raise Exception("比较条件部分缺少")
            else:
                str_list.append(" {0} {1} {2}".format(
                    self.parserColumnContent(condition.left),
                    condition.operator,
                    self.parserColumnContent(condition.right)
                ))

        return "".join(str_list)


    def parserQueryObjectContent(self,query):
        """
        解析QueryObject
        """
        if not isinstance(query,QueryObject):
            raise Exception("必须是QueryObject对象")
        str_list = []
        if query.join_type:
            str_list.append("{0} JOIN {1} {2}".format(
                query.join_type,
                query.name,
                "AS %s"%query.alias if query.alias else ""
            ))

            if not query.join_condition:
                raise Exception("连接条件缺失")

            str_list.append(" ON {0}".format("".join([self.parserConditionContent(cond) for cond in query.join_condition if cond])))
        else:
            #没有外连接关系
            str_list.append("{0} {1}".format(query.name,"AS %s"%query.alias if query.alias else ""))
        return "".join(str_list)


    def parserOrderByContent(self,order_by):
        """
        解析OrderBy
        """
        if not isinstance(order_by,OrderBy):
            raise Exception("必须是OrderBy对象")
        str_list = []
        if order_by.query_name and order_by.prop_name:
            str_list.append("`{0}`.`{1}` {2}".format(order_by.query_name,order_by.prop_name,order_by.method))
        return "".join(str_list)


    def parseLimitOffsetContent(self,limit):
        """
        解析LimitOffset对象
        """
        if not isinstance(limit,LimitOffset):
            raise Exception("必须是LimitOffset对象")
        return "{0},{1}".format(limit.offset,limit.row)