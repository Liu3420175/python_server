#!/usr/bin/env python3
#-*- coding:utf-8 -*-
# author:liulh01@mingyuanyun.com
# datetime:18-12-24 下午2:58
# software: PyCharm
from datasource.base_models import Property,Condition,LimitOffset,QueryObject,OrderBy,QueryBaseModel

class QueryStructre(QueryBaseModel):
    """
    查询结构体
    """
    __slots__ = ("select","query_object","where","group_by","order_by","limit","having",)

    def __init__(self,
                 select,
                 query_object,
                 where,
                 limit,
                 group_by=None,
                 order_by=None,
                 having=None):
        """
        所有的参数都是可迭代对象，除了limit
        Args:
            kwargs:原始数据
        """
        self.select = select
        self.query_object = query_object
        self.where = where
        self.group_by = group_by
        self.order_by = order_by
        self.limit = limit
        self.having = having
        self._init()
        super(QueryStructre,self).__init__()


    def _init(self):
        """
        初始化属性
        """
        self.TransformList2Object("select",Property,True)
        self.TransformList2Object("query_object",QueryObject,True)
        self.TransformList2Object("where",Condition,True)
        self.TransformList2Object("group_by",Property,True)
        self.TransformList2Object("order_by",OrderBy,True)
        self.TransformList2Object("limit",LimitOffset,False)
        self.TransformList2Object("having",Condition,True)
