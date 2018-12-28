#!/usr/bin/env python3
#-*- coding:utf-8 -*-
# author:liulh01@mingyuanyun.com
# datetime:18-12-24 上午9:32
# software: PyCharm


class QueryBaseModel(object):
    __slots__ = ()

    def __init__(self):
       pass

    def _check_parameter(self,*args):
        """
        检查初始化的参数是否合法，只适合是None 或者字符串情况
        """

        for one in args:
            if one is  None  or (isinstance(one,str) and len(one) > 0):
                pass
            else:
                #pass
                raise  Exception("必须为非空字符串或者None")

    def TransformList2Object(self, propName, classType, istolist=False):
        """
            讲原始数据转换成基础对象
        """
        properties = getattr(self, propName, None)
        if not (isinstance(properties, list) or isinstance(properties, set) or isinstance(properties, dict)):
            return None

        if istolist:
            tmpList = [classType(**p) for p in properties if isinstance(p, dict)]
            setattr(self, propName, tmpList)
        else:
            setattr(self, propName, classType(**properties))


class Property(QueryBaseModel):
    """
    属性，查询对象的属性，不考虑case_when和嵌套情况
    """
    __slots__ = ("query_name","prop_name","alias","func","specifier","operator","value","props","case_when")

    def __init__(self,
                 query_name=None,
                 prop_name=None,
                 alias=None,
                 func=None,
                 specifier=None,
                 value=None,
                 operator=None,
                 props=None,
                 case_when=None
                 ):
        """
        Args:
            query_name: 查询对象名(表名)
            prop_name: 表字段名
            alias: 字段别名
            func: 函数
            specifier: 修饰符
            value: 具体的值,属性可能不是表的属性，只是表示一个值而已
            operator: 算术操作符
            props:组合属性
        """

        self._check_parameter(query_name,prop_name, alias, func, specifier)
        self.query_name = query_name
        self.prop_name = prop_name
        self.alias = alias
        self.func = func
        self.value = value
        self.specifier = specifier
        self.operator = operator
        self.props = [] if props is None else props
        self.case_when = case_when
        self._init()
        super(Property, self).__init__()


    def __str__(self):
        return "Property(query_name=%s,propname=%s,value=%s)"%(
            self.query_name if self.query_name else "",
            self.prop_name if self.prop_name else "",
            self.value)

    def _init(self):
        self.TransformList2Object( "props", Property, True)
        self.TransformList2Object( "case_when", CaseObject, True)


class QueryObject(QueryBaseModel):
    """
    查询对象,不考虑表连接情况
    """
    __slots__ = ("name","alias")

    def __init__(self,
                 name,
                 alias=None):
        """
         Args:
             name:查询对象名字
             alias:查询对象别名
        """
        #self._check_parameter(alias)
        #assert isinstance(name, str) and len(name) > 0, "查询对象名必须是字符串且长度大于0"
        self.name = name
        self.alias = alias
        super(QueryObject,self).__init__()


    def __str__(self):
        return "QueryObject(name=%s,alias=%s)"%(self.name,self.alias)


class Condition(QueryBaseModel):
    """
    条件对象
    """
    __slots__ = ("logical_relation","left","right","operator","conditions")

    def __init__(self,
                 left,
                 right,
                 operator=None,
                 logical_relation=None,
                 conditions=None):
        """
        Args:
            logical_relation:逻辑关系，AND等
            left:左边表达式，Property对象
            right:右边表达式，Property对象
            operator:操作符
            conditions:嵌套条件
        """
        self.logical_relation = logical_relation
        self.right = right
        self.left = left
        self.operator = operator
        self.conditions = conditions
        self._init()
        super(Condition,self).__init__()


    def _init(self):

        self.TransformList2Object("left",Property,False)
        self.TransformList2Object( "right", Property, False)
        self.TransformList2Object("conditions",Condition,True)

    def __str__(self):
        return "Condition(left=%s,right=%s,operator='%s')"%(self.left,self.right,self.operator)




class CaseObject(QueryBaseModel):
    """
    case-when-then,预留
    """
    __slots__ = ("condition","display")
    def __init__(self,condition=None,display=""):
        """
        Args:
            codition:case-when条件
            display:then展示信息
        """
        self.condition = condition
        self.display = display
        self._init()
        super(CaseObject,self).__init__()

    def _init(self):
        #TransformList2Object(self,"condition",Condition,False)
        self.TransformList2Object( "condition", Condition, False)

    def __str__(self):
        return "CaseObject(condition=%s,display=%s)"%(self.condition,self.display)



class OrderBy(QueryBaseModel):
    """
    排序方式
    """
    __slots__ = ("query_name","prop_name","method")

    def __init__(self,
                 query_name,
                 prop_name,
                 method="AES"):
        """
        Args:
            query_name:查询对象名称
            prop_name:属性名称
            method:排序方式
        """
        self._check_parameter(query_name,prop_name)
        self.query_name = query_name
        self.prop_name = prop_name
        if isinstance(method,str):
            method = method.upper()
            if method in ("AES","DESC"):
                self.method = method
            else:
                raise Exception("SQL排序关键字不合法")
        else:
            raise Exception("OrderBy.method必须是非空字符串")

        super(OrderBy,self).__init__()


    def __str__(self):
        return "OrderBy(query_name=%s,prop_name=%s,method=%s)"%(self.query_name,self.prop_name,self.method)



class LimitOffset(QueryBaseModel):
    """
    分页
    """
    __slots__ = ("offset","row")
    def __init__(self,offset=0,row=10):
        """
        Args:
            offset:偏移行数
            row:返回行数
        """
        self.offset = offset
        self.row = row
        super(LimitOffset,self).__init__()

    def __str__(self):
        return "LimitOffset(offset=%s,row=%s)"%(self.offset,self.row)
