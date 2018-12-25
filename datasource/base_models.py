#!/usr/bin/env python3
#-*- coding:utf-8 -*-
# author:liulh01@mingyuanyun.com
# datetime:18-12-24 上午9:32
# software: PyCharm

def TransformList2Object(obj, propName,classType,istolist=False):
    """
        讲原始数据转换成基础对象
    """
    properties = getattr(obj,propName,None)
    if not (isinstance(properties, list) or isinstance(properties, set) or isinstance(properties, dict)):
        return None

    if istolist:
        tmpList = [classType(**p) for p in properties if isinstance(p,dict)]
        setattr(obj,propName,tmpList)
    else:
        setattr(obj,propName,classType(**properties))


class QueryBaseModel(object):

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
                raise  Exception() # TODO 异常


class Property(QueryBaseModel):
    """
    属性，查询对象的熟悉
    """
    __slots__ = ("query_name","prop_name","alias","func","specifier","operator",
                 "value","case_when","props")

    def __init__(self,
                 query_name=None,
                 prop_name=None,
                 alias=None,
                 func=None,
                 specifier=None,
                 value=None,
                 operator=None,
                 case_when=None,
                 props=None
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
            case_when:case_when条件,列表
            props:组合属性
        """

        self._check_parameter(query_name,prop_name, alias, func, specifier, operator)
        self.query_name = query_name
        self.prop_name = prop_name
        self.alias = alias
        self.func = func
        self.value = value
        self.specifier = specifier
        self.operator = operator
        self.case_when = case_when
        self.props = [] if props is None else props
        self._init()
        super(Property, self).__init__()


    def __chech_func_args(self,query_name,prop_name,func_args):
        """
        检验初始化的func_args参数的合法性
        """
        if func_args is None or func_args == "":
            _func_args = [".".join([query_name,prop_name]),]
        else:
            if isinstance(func_args,list) or isinstance(func_args,set) or isinstance(func_args,tuple):
                _func_args = func_args
            else:
                raise Exception() # TODO 异常
        return _func_args

    def _init(self):
        TransformList2Object(self,"props",Property,True)
        TransformList2Object(self,"case_when",CaseObject,True)

    def __str__(self):
        return "Property Object(query_name=%s,propname=%s)"%(self.query_name if self.query_name else "",
                                                             self.prop_name if self.prop_name else "")


class QueryObject(QueryBaseModel):
    """
    查询对象
    """
    __slots__ = ("name","alias","join_type","join_condition")

    def __init__(self,
                 name,
                 alias,
                 join_type=None,
                 join_conditions=None):
        """
         Args:
             name:查询对象名字
             alias:查询对象别名
             join_type:连接方式
             join_condition:连接条件,Condition对象
        """
        self._check_parameter(alias,join_type)
        assert isinstance(name, str) and len(name) > 0, "查询对象名必须是字符串且长度大于0"
        self.name = name
        self.alias = alias
        self.join_type = join_type
        self.join_condition = join_conditions
        self._init()
        super(QueryObject,self).__init__()

    def _init(self):
        TransformList2Object(self,"join_condition",Condition,True)

    def __str__(self):
        return "QueryObject Object(name=%s,alias=%s,join_type=%s,join_condition=%s)"%(self.name,
                                                                                      self.alias,
                                                                                      self.join_type if self.join_type else "",
                                                                                      self.join_condition)


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
        TransformList2Object(self,"left",Property,False)
        TransformList2Object(self,"right",Property,False)
        TransformList2Object(self,"conditions",Condition,True)

    def __str__(self):
        return "Condition Object(left=%s,right=%s,operator='%s')"%(self.left,self.right,self.operator)


class CaseObject(QueryBaseModel):
    """
    case-when-then
    """
    __slots__ = ["condition","display"]
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
        TransformList2Object(self,"condition",Condition,False)



class OrderBy(QueryBaseModel):
    """
    排序方式
    """
    __slots__ = ["query_name","prop_name","method"]

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
                raise Exception()
        else:
            raise Exception()

        super(OrderBy,self).__init__()



class LimitOffset(QueryBaseModel):
    """
    分页
    """
    __slots__ = ["offset","row"]
    def __init__(self,offset=0,row=10):
        """
        Args:
            offset:偏移行数
            row:返回行数
        """
        #assert isinstance(offset,int) and isinstance(row,int) and offset >= 0 and row >= 0
        self.offset = offset
        self.row = row
        super(LimitOffset,self).__init__()

    def __str__(self):
        return "LimitOffset Object:(offset=%s,row=%s)"%(self.offset,self.row)
