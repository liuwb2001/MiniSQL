from abc import abstractproperty
from os import sep
from CatalogManager import *
from index import Index
import re
from FileOp import *
import globalValue
from prettytable import PrettyTable

# --------------------------------数据库操作---------
"""
@ 创建新数据库
@param: 形如 create database 库名;\n
        设定传进来的已经trim并去除了多余的空格
"""


def create_db(query):
    match = re.match(
        r'^create\s+database\s+([a-z](\w)*)$', query, re.S)
    if match:
        tableName = match.group(1)
        createDB(tableName)
    else:
        raise MiniSQLSyntaxError('Syntax Error in: ' + query)


"""
@ 删除数据库
@param: 形如 drop database 库名;\n
        设定传进来的已经trim并去除了多余的空格
"""


def drop_db(query):
    match = re.match(
        r'^drop\s+database\s+([a-z][0-9a-z_]*)$', query, re.S)
    if match:
        DBName = match.group(1)
        if DBName == globalValue.currentDB:
            log('[drop db]\t无法删除正在使用的库 '+DBName)
        else:
            dropDB(DBName)
    else:
        raise MiniSQLSyntaxError('Syntax Error in: ' + query)


def use_db(query):
    match = re.match(
        r'^use\s+([a-z][0-9a-z_]*)$', query, re.S)
    if match:
        DBName__ = match.group(1)
        SwitchToDB(DBName__)

    else:
        raise MiniSQLSyntaxError('Syntax Error in: ' + query)


"""
@ 查看当前数据库
@param: 形如 select database();\n
        设定传进来的已经trim并去除了多余的空格
"""


def select_db(query):
    match = re.match(r'^select\s+database\s*\(\s*\)$', query, re.S)
    if match:
        # log('[select database]\t当前数据库为 '+globalValue.currentDB)
        return globalValue.currentDB
    else:
        raise MiniSQLSyntaxError('Syntax Error in: ' + query)


def show_dbs(query):
    match = re.match(r'^show\s+databases$', query, re.S)
    if match:
        return printDB()
    else:
        raise MiniSQLSyntaxError('Syntax Error in: ' + query)


def show_tables(query):
    match = re.match(r'^show\s+tables$', query, re.S)
    if match:
        return showTables()
    else:
        raise MiniSQLSyntaxError('Syntax Error in: ' + query)
# -------------------------- 表操作 -----------------------


"""
@param: 形如 CREATE TABLE 表名(
                字段名 字段类型 PRIMARY KEY, #主键
                字段名 字段类型 UNIQUE, #唯一
            );
"""


def create_table(query):
    match = re.match(
        r'^create\s+table\s+([a-z]\w*)\s*\((.+)\)$', query, re.S)
    if match:
        tableName, cols = match.groups()
        attris, types, ifUniques = [], [], []
        ExistsPri = 0
        key = None
        for a in cols.split(','):
            a = a.strip()
            priKey = re.match(r'^\s*primary\s+key\s*\((.+)\)\s*$', a, re.S)
            if priKey:
                key = priKey.group(1)
                ExistsPri += 1
            else:
                a = a.split(' ')
                attri, type__ = a[0].strip(), a[1].strip()
                type__ = convert(type__)

                if len(a) == 3 and a[2].strip() == 'unique':
                    ifUniques.append(True)
                elif len(a) != 2:
                    raise MiniSQLSyntaxError('Syntax Error in: ' + query)
                else:
                    ifUniques.append(False)
                attris.append(attri)
                types.append(type__)
        if ExistsPri > 1:
            raise MiniSQLError('Multiple primary keys')
        else:
            if key == None:
                key = attris[0]
            for i, pair in enumerate(attris):
                if pair == key:
                    ifUniques[i] = True
                    break
            createTable(tableName.strip(), attris, types, key, ifUniques)
    else:
        raise MiniSQLSyntaxError('Syntax Error in: ' + query)


def drop_table(query):
    match = re.match(
        r'^drop\s+table\s+([a-z][0-9a-z_]*)$', query, re.S)
    if match:
        tableName = match.group(1)
        dropTable(tableName)

    else:
        raise MiniSQLSyntaxError('Syntax Error in: ' + query)


def create_index(query):
    match = re.match(
        r'^create\s+index\s+(.+)+on\s+(.+)\s*\((.+)\)$', query, re.S)

    if match:
        indexName = match.group(1).strip()
        tableName = match.group(2).strip()
        attri = match.group(3).strip()
        createIndex(indexName, tableName, attri, False)
    else:
        raise MiniSQLSyntaxError('Syntax Error in: '+query)


def drop_index(query):
    match = re.match(
        r'^drop\s+index\s+([a-z][a-z0-9_]*)$', query, re.S)
    if match:
        indexName = match.group(1).strip()
        dropIndex(indexName, False)
    else:
        raise MiniSQLSyntaxError('Syntax Error in: ' + query)
# ---------------------------------------


def insert(query):
    match = re.match(
        r'^insert\s+into\s+([a-z][a-z0-9_]*)\s+values\s*\((.+)\)$', query, re.S)
    if match:
        tableName = match.group(1).strip()
        Values = match.group(2).strip()
        values = []
        uniques = []
        if existsTable(tableName):
            schema = getTable(tableName)
            attrs = schema['attrs']
            types = schema['types']
            indexs = []

            for attr in attrs:
                index = IndexOfAttr(tableName, attr)
                indexs.append(index)

            num = 0
            for v in Values.split(','):
                vv = v.strip()
                vv = vv.strip('"')
                vv.strip()
                if types[num] == -1:
                    vv = int(vv)
                elif types[num] == 0:
                    vv = float(vv)
                num += 1
                values.append(vv)
            globalValue.currentIndex.Insert_into_table(
                tableName, attrs, types, values, indexs)
        else:
            raise MiniSQLError('[insert]\t不存在该表'+tableName)
    else:
        raise MiniSQLSyntaxError('Syntax Error in: ' + query)


def seperateCondition(query, condition):
    condition = condition.strip()
    schema = {}
    attrs = []
    ops = []
    keys = []
    for subCond in condition.split('and'):
        subCond = subCond.strip()
        match = re.match(
            r'^([a-z0-9][a-z0-9_]*)\s*=\s*(.+)$', subCond, re.S)
        
        # 情况1：等于
        if match:
            attrs.append(match.group(1).strip())
            ops.append(0)
            key = match.group(2).strip()
            key = key.strip('"')
            key = key.strip()
            keys.append(key)
        else:
            match = re.match(
                r'^([a-z0-9][a-z0-9_]*)\s*<>\s*\"*(.+)\"*$', subCond, re.S)
            # 情况2：不等于
            if match:
                attrs.append(match.group(1).strip())
                ops.append(1)
                key = match.group(2).strip()
                key = key.strip('"')
                key = key.strip()
                keys.append(key)
            else:
                match = re.match(
                    r'^([a-z0-9][a-z0-9_]*)\s*<=\s*\"*(.+)\"*$', subCond, re.S)
                # 情况3：小于等于
                if match:
                    attrs.append(match.group(1).strip())
                    ops.append(4)
                    key = match.group(2).strip()
                    key = key.strip('"')
                    key = key.strip()
                    keys.append(key)
                else:
                    match = re.match(
                        r'^([a-z0-9][a-z0-9_]*)\s*>=\s*\"*(.+)\"*$', subCond, re.S)
                    # 情况4：大于等于
                    if match:
                        attrs.append(match.group(1).strip())
                        ops.append(5)
                        key = match.group(2).strip()
                        key = key.strip('"')
                        key = key.strip()
                        keys.append(key)
                    else:
                        match = re.match(
                            r'^([a-z0-9][a-z0-9_]*)\s*<\s*\"*(.+)\"*$', subCond, re.S)
                        # 情况5：小于
                        if match:
                            attrs.append(match.group(1).strip())
                            ops.append(2)
                            key = match.group(2).strip()
                            key = key.strip('"')
                            key = key.strip()
                            keys.append(key)
                        else:
                            match = re.match(
                                r'^([a-z0-9][a-z0-9_]*)\s*>\s*\"*(.+)\"*$', subCond, re.S)
                            # 情况6：大于
                            if match:
                                attrs.append(match.group(1).strip())
                                ops.append(3)
                                key = match.group(2).strip()
                                key = key.strip('"')
                                key = key.strip()
                                keys.append(key)
                            else:
                                raise MiniSQLSyntaxError(
                                    'Syntax Error in: ' + query)
    schema['keys'] = keys
    schema['attrs'] = attrs
    schema['ops'] = ops
    return schema


def delete(query):
    match = re.match(
        r'^delete\s+from\s+([a-z][a-z0-9_]*)\s+where\s+(.+)$', query, re.S)
    # 情况1：有 where
    if match:
        tableName = match.group(1).strip()
        condition = match.group(2).strip()
        if existsTable(tableName):
            schema = getTable(tableName)
            isPri = schema['primary_key']
            Types = schema['types']
            Attrs = schema['attrs']
            index = []

            for a in Attrs:
                index.append(IndexOfAttr(tableName,a))
            unique = []
            isindex = []
            types = []

            subCond = seperateCondition(query, condition.strip())
            attrs = subCond['attrs']
            keys = subCond['keys']
            ops = subCond['ops']
            for i in range(0, len(attrs)):
                if not existsAttr(tableName, attrs[i]):
                    raise MiniSQLError(
                        '[delete]\t表 '+tableName+' 中不存在该属性 '+attrs[i])
                else:
                    unique.append(UniqueOfAttr(tableName, attrs[i]))
                    isindex.append(IndexOfAttr(tableName, attrs[i]))
                    Type = TypeOfAttr(tableName, attrs[i])
                    types.append(Type)
                    keys[i] = TypeChange(keys[i], Type)

            return globalValue.currentIndex.Delete_and_join(tableName, Attrs, Types, index, attrs, keys, isindex, ops)

        else:
            raise MiniSQLError('[delete]\t不存在该表'+tableName)
    else:
        match = re.match(
            r'^delete\s+from\s+([a-z][a-z0-9_]*)$', query, re.S)
        # 情况2：无where
        if match:
            tableName = match.group(1).strip()
            condition = None
            if existsTable(tableName):
                schema = getTable(tableName)
                isPri = schema['primary_key']
                Types = schema['types']
                Attrs = schema['attrs']
                index = schema['index']
                IIndex = TypeChange('1', Types[0])
                ifIndex = IndexOfAttr(tableName, Attrs[0])
                flag1 = globalValue.currentIndex.Delete_and_join(
                    tableName, Attrs, Types, index, [Attrs[0]], [IIndex], [ifIndex], [0])
                flag2 = globalValue.currentIndex.Delete_and_join(
                    tableName, Attrs, Types, index, [Attrs[0]], [IIndex], [ifIndex], [1])
                return flag1 or flag2
            else:
                raise MiniSQLError('[delete]\t不存在该表 '+tableName)
        else:
            raise MiniSQLSyntaxError('Syntax Error in: ' + query)


def TypeChange(key, Type):
    if Type == -1:
        try:
            key = int(key)
            return key
        except:
            return False
    elif Type == 0:
        try:
            key = float(key)
            return key
        except:
            return False
    elif Type <= 255:
        return key


def select(query):
    match = re.match(
        r'^select\s+(.+)\s+from\s+([a-z][a-z0-9_]*)+\s+where\s*(.+)$', query, re.S)
    # 情况1：有where
    if match:
        cols = match.group(1).strip()
        tableName = match.group(2).strip()
        condition = match.group(3).strip()

        if existsTable(tableName):

            uniques = []
            ifindexs = []
            types = []
            subcon = seperateCondition(query, condition)
            attrs = subcon['attrs']
            keys = subcon['keys']
            ops = subcon['ops']
            for i in range(0, len(attrs)):
                if not existsAttr(tableName, attrs[i]):
                    raise MiniSQLError(
                        '[select]\t表 '+tableName+' 中不存在该属性 '+attrs[i])
                else:
                    uniques.append(UniqueOfAttr(tableName, attrs[i]))
                    ifindexs.append(IndexOfAttr(tableName, attrs[i]))
                    Type = TypeOfAttr(tableName, attrs[i])
                    types.append(Type)
                    x = TypeChange(keys[i], Type)
                    if x == False:
                        # 类型不匹配
                        return False
                    keys[i] = x

            schema = getTable(tableName)
            isPri = schema['primary_key']
            Types = schema['types']

        else:
            raise MiniSQLError('[select]\t不存在该表 '+tableName)
    else:

        match = re.match(
            r'^select\s+(.+)\s+from\s+([a-z](\w)*)$', query, re.S)
        # 情况2：没有 where
        if match:
            cols = match.group(1).strip()
            tableName = match.group(2).strip()
            schema = getTable(tableName)
            isPri = schema['primary_key']
            attrs = schema['attrs']
            Types = schema['types']
            ops = -1

        else:
            raise MiniSQLSyntaxError('Syntax Error in: ' + query)
    if cols != '*':
        raise MiniSQLSyntaxError('Syntax Error in: ' + query)
    else:
        if ops == -1:
            select_res = globalValue.currentIndex.Select_all_data(
                tableName, isPri, Types)
        else:
            select_res = globalValue.currentIndex.Select_and_join(
                tableName, Types, attrs, keys, ifindexs, ops)

        if select_res:
            output = {}
            output['attrs'] = schema['attrs']
            output['select_res'] = select_res
            return output
        else:
            return False
 
def test():
    clear_all()
    
if __name__ == '__main__':
    test()
