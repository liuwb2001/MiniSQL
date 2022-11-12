from genericpath import exists
import os
import shutil
import re
from BPlusTree import *
from typing import Tuple
from index import *
from FileOp import *
import globalValue


def clear_all():
    shutil.rmtree('Index')
    shutil.rmtree('IIndex')
    shutil.rmtree('DBFiles')
# --------------------
# 数据库操作
# ---------------------


def createDB(DBName__):
    path = DBFiles.format(DBName__)
    if os.path.exists(path):
        log('[create DB]\t创建数据库失败，当前已有该数据库名 ' + DBName__)
    else:
        if not os.path.exists("DBFiles"):
            os.mkdir("DBFiles")
        if not os.path.exists("IIndex"):
            os.mkdir("IIndex")
        if not os.path.exists("Index"):
            os.mkdir("Index")
        fileName = DBFiles.format(DBName__)
        fp = open(fileName, "w")
        fp.close()
        fileName = index_File.format(DBName__)
        fp = open(fileName, "w")
        fp.close()
        fileName = index_filepath.format(DBName__)
        fp = open(fileName, "w")
        fp.close()
        fileName = list_filepath.format(DBName__)
        fp = open(fileName, "w")
        fp.close()
        fileName = data_filepath.format(DBName__)
        fp = open(fileName, "w")
        fp.close()

        index = Index()
        index.Load_file(index_filepath.format(DBName__), list_filepath.format(
            DBName__), data_filepath.format(DBName__))
        log('[create DB]\t创建数据库 '+DBName__+' 成功')


def dropDB(DBName__):
    path1 = DBFiles.format(DBName__)
    path2 = index_File.format(DBName__)
    path3 = index_filepath.format(DBName__)
    path4 = list_filepath.format(DBName__)
    path5 = data_filepath.format(DBName__)

    if os.path.exists(path1) or os.path.exists(path2) or os.path.exists(path3) or os.path.exists(path4) or os.path.exists(path5):
        if os.path.exists(path1):
            os.remove(path1)
        if os.path.exists(path2):
            os.remove(path2)
        if os.path.exists(path3):
            os.remove(path3)
        if os.path.exists(path4):
            os.remove(path4)
        if os.path.exists(path5):
            os.remove(path5)

        log('[drop DB]\t\t删除数据库 ' + DBName__ + ' 成功')
    else:
        log('[drop DB]\t\t删除数据库失败，当前不存在数据库名为 ' + DBName__)


def SwitchToDB(DBName__):

    path = DBFiles.format(DBName__)
    if os.path.exists(path):
        if globalValue.currentDB == None:
            globalValue.currentDB = DBName__
            globalValue.currentIndex.Load_file(index_filepath.format(DBName__), list_filepath.format(DBName__), data_filepath.format(DBName__))
        else:
            globalValue.currentIndex.Save_file()
            globalValue.currentDB = DBName__
            globalValue.currentIndex.Load_file(index_filepath.format(DBName__), list_filepath.format(DBName__), data_filepath.format(DBName__))
        print('#当前切换到', DBName__, '库')
        return globalValue.currentDB
        
    else:
        log('#error: 切换数据库失败，当前不存在该数据库名: '+DBName__)   


def printDB():
    # print('当前本机拥有数据库如下：')
    file = []
    for root, dirs, files in os.walk("DBFiles"):
        for f in files:
            file.append(f.strip('.json'))
    return file


def showTables():
    DBName__ = globalValue.currentDB
    print('当前本数据库', DBName__, '拥有表如下：')
    try:
        path = DBFiles.format(DBName__)
        # "DBFiles\\"+currentDB+".json"
        schemas = load(path)
        names = []
        for name in schemas:
            names.append(name)
            # print(name)
        return names
    except FileNotFoundError:
        schemas = None
        return False

# ---------------------------------
# 表操作
# --------------------------------


def createTable(tableName__, attributes,  types, priKey, ifUniques):
    try:
        path = DBFiles.format(globalValue.currentDB)
        schemas = load(path)
    except FileNotFoundError:
        schemas = {}
    if tableName__ not in schemas:
        digit_types = []
        indexs = []
        for type in types:
            digit_types.append(type)
            indexs.append(False)
        schema = {'attrs': attributes, 'types': digit_types,
                  'primary_key': priKey, 'uniques': ifUniques, 'index': []}
        schemas[tableName__] = schema
        store(schemas, path)

        globalValue.currentIndex.Create_table(tableName__, priKey, attributes)
        createIndex('priKey_'+tableName__+'_'+priKey, tableName__, priKey,True)
        log('[Create Table]\t创建表 ' + tableName__ + ' 成功')
    else:
        log('[Create Table]\t已存在该表名 ' + tableName__)


def dropTable(tableName__):
    try:
        path = DBFiles.format(globalValue.currentDB)

        schemas = load(path)
    except FileNotFoundError:
        schemas = {}
    if tableName__ in schemas:
        index = schemas[tableName__]['index']
        Prikey = schemas[tableName__]['primary_key']
        for IndexName, col in index:
            if col != Prikey:
                dropIndex(IndexName, True)
                store(schemas, path)
                
            else:
                dropIndex(IndexName, True)
        globalValue.currentIndex.Drop_table(tableName__, Prikey)
        schemas.pop(tableName__)
        store(schemas, path)
        log('[Drop Table]\t删除表 '+tableName__+' 成功')
        # log('[drop table]\t不能删去主键索引')

    else:
        log('[Drop Table]\t删除失败，不存在该表名 ' + tableName__)


def existsTable(tableName__):
    try:
        path = DBFiles.format(globalValue.currentDB)
        schemas = load(path)
    except FileNotFoundError:
        schemas = {}
    if tableName__ in schemas:
        return True
    else:
        return False


def getTable(tableName__):
    DBName__ = globalValue.currentDB
    try:
        path = DBFiles.format(DBName__)
        schemas = load(path)
    except FileNotFoundError:
        schema = {}
    if tableName__ in schemas:
        schema = schemas[tableName__]
    else:
        schema = {}
    return schema


def existsAttr(tableName, attri):
    try:
        path = DBFiles.format(globalValue.currentDB)
        schemas = load(path)
    except FileNotFoundError:
        schemas = {}
    table = schemas[tableName]
    attris = table['attrs']
    if attris:
        for i, pair in enumerate(attris):
            if pair == attri:
                return True
    return False

# ------------------------
# 转换操作
# ------------------------


def UniqueOfAttr(tableName, attr):
    try:
        path = DBFiles.format(globalValue.currentDB)
        schemas = load(path)
    except FileNotFoundError:
        schemas = {}
    table = schemas[tableName]
    attris = table['attrs']
    unique = table['uniques']
    if attris:
        for i, pair in enumerate(attris):
            if pair == attr:
                break
        return unique[i]


def TypeOfAttr(tableName, attr):
    try:
        path = DBFiles.format(globalValue.currentDB)
        schemas = load(path)
    except FileNotFoundError:
        schemas = {}
    table = schemas[tableName]
    attris = table['attrs']
    types = table['types']
    if attris:
        for i, pair in enumerate(attris):
            if pair == attr:
                break
        return types[i]


def IndexOfAttr(tableName, attr):
    try:
        path = DBFiles.format(globalValue.currentDB)
        schemas = load(path)
    except FileNotFoundError:
        schemas = {}
    table = schemas[tableName]
    index = table['index']
    for i, pair in enumerate(index):
        if pair[1] == attr:
            return True
            break
    return False

def convert(type):
    if type == 'int':
        return -1
    elif type == 'float':
        return 0
    else:
        match = re.match(r'^char\((\d+)\)$', type, re.S)
        if match:
            n = int(match.group(1))
            if n >= 1 and n <= 255:
                return n
            else:
                return MiniSQLError('char(n): n is out of range [1, 255]')
        else:
            raise MiniSQLSyntaxError('Syntax Error in type {} '.format(type))

# ------------------------
# 索引操作
# ------------------------


def createIndex(indexName, tableName, attri, ifDrop):
    indexFile = index_File.format(globalValue.currentDB)
    path = DBFiles.format(globalValue.currentDB)
    schemas = load(path)
    Indexs = load(indexFile)

    if indexName in Indexs:
        log('[create Index]\t索引创建失败，已创建该索引名 '+indexName)
        return False
    else:

        if tableName in schemas:
            if existsAttr(tableName, attri):
                if IndexOfAttr(tableName, attri):
                    log('[create Index]\t索引 '+indexName +
                        '创建失败，本程序暂不支持且不建议在同一列上建立多个索引')
                    return False
                Indexs[indexName] = {'table': tableName, 'attri': attri}

                # 更新表结构
                schemas[tableName]['index'].append([indexName, attri])

                if ifDrop == False:
                # --------------------需要Index的接口
                    BT = BPlusTree()
                    BT.BuildNewBPTree()
                    table = globalValue.currentIndex.normal_list[tableName]
                    if attri in table:
                        keys = table[attri]['keys']
                        values = table[attri]['values']
                    else:
                        keys = []
                        values = []

                    for i in range(len(keys)):
                        BT.Insert_node(keys[i], values[i])
                    globalValue.currentIndex.index_trees[tableName][attri] = BT.Trees
                    if attri in table:
                        globalValue.currentIndex.normal_list[tableName].pop(attri)

                store(schemas, path)
                store(Indexs, indexFile)
                log('[create Index]\t索引 '+indexName+' 创建成功')

            else:
                log('[create Index]\t索引创建失败，该表不存在属性 '+attri)

        else:
            log('[create Index]\t索引创建失败，当前数据库不存在表 ' + tableName)


def dropIndex(indexName__, ifDrop):
    indexFile = index_File.format(globalValue.currentDB)
    path = DBFiles.format(globalValue.currentDB)
    schemas = load(path)
    Indexs = load(indexFile)

    if indexName__ not in Indexs:
        log('[drop Index]\t删除索引失败，不存在该索引名 ' + indexName__,)
    else:

        tableName = Indexs[indexName__]['table']
        attri = Indexs[indexName__]['attri']
        table = schemas[tableName]

        # 删除表
        if ifDrop == True:
            Indexs.pop(indexName__)
            log('[drop Index]\t删除索引 ' + indexName__ + ' 成功')
            store(schemas, path)
            store(Indexs, indexFile)
            return True
        else:
            if table['primary_key'] == attri:
                log('[drop Index]\t删除索引失败，无法删除主键索引 ' + indexName__)
                return False
            else:
                Indexs.pop(indexName__)

                index = table['index']
                attri = []
                if index:
                    for i, pair in enumerate(index):
                        if pair[0] == indexName__:
                            attri = pair[1]
                            index.pop(i)
                            BT = BPlusTree()
                            BT.Trees = globalValue.currentIndex.index_trees[tableName][attri]
                            values = list(BT.Fetch_all_nodes())
                            keys = list(BT.Fetch_all_nodes_key())

                        
                            NL = NormalList()
                            for i in range(0,len(keys)):
                                NL.Insert_node(keys[i],values[i])
                            globalValue.currentIndex.normal_list[tableName][attri] = {'keys':NL.keys,'values':NL.values}
                            if attri in globalValue.currentIndex.index_trees[tableName]:
                                globalValue.currentIndex.index_trees[tableName].pop(attri)

                            norm = {'keys':keys,'values':values}
                            globalValue.currentIndex.normal_list[tableName][attri] = norm
                            break

            store(schemas, path)
            store(Indexs, indexFile)
            log('[drop Index]\t删除索引 ' + indexName__ + ' 成功')


def getIndexInfo():
    print('当前本数据库', globalValue.currentDB, '拥有索引如下：')
    indexFile = index_File.format(globalValue.currentDB)
    Indexs = load(indexFile)
    for index in Indexs:
        print('\t'+index)
