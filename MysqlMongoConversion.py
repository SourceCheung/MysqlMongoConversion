# -*- coding: utf-8 -*-

import sys, os
import multiprocessing
import logging
import random
import time, datetime
import MySQLdb
from MySQLdb import cursors
from pymongo import MongoClient


class Config:
    tables = ['bloglist', 'blogStatistics', 'connectStatistics']


class MysqlMongoConversion(object):
    mysql_host = '127.0.0.1'
    mysql_port = 3306
    mysql_user = "root"
    mysql_pass = "12345678"
    mysql_db = "datacenter"

    mongo_host = '127.0.0.1'
    mongo_port = 27017
    mongo_dbname = 'blog'

    conn = None
    cursor = None
    mongo = None
    mongodb = None

    def __init__(self, logger):
        self.logger = logger

        self.conn = self.getMysqlConn()
        self.cursor = self.conn.cursor()

        self.mongo = MongoClient(host=self.mongo_host, port=self.mongo_port)
        self.mongodb = self.mongo[self.mongo_dbname]

    def getMysqlConn(self):
        return MySQLdb.connect(host=self.mysql_host, port=self.mysql_port, user=self.mysql_user, \
                               charset='utf8', use_unicode=0, \
                               passwd=self.mysql_pass, db=self.mysql_db, cursorclass=MySQLdb.cursors.SSCursor)

    def setMongoCollectionDocument(self, table, data):
        if (isinstance(data, dict) == False):
            return False
        else:
            self.mongodb[table].insert(data)

    def getMysqlTableDesc(self, table):
        sql = """desc %s""" % (table.lower())
        n = self.cursor.execute(sql)
        data = self.cursor.fetchall()
        keys = []
        types = []
        for row in data:
            key = str(row[0])
            if (row[1].find('int') >= 0):
                type = 1
            elif (row[1].find('char') >= 0):
                type = 2
            elif (row[1].find('text') >= 0):
                type = 2
            elif (row[1].find('decimal') >= 0):
                type = 3
            else:
                type = 2
            keys.append(key)
            types.append(type)
        return keys, types

    # 生成mongodb数据,id
    def mysql2Mongo(self, table):
        self.mongodb[table].drop()
        keys, types = self.getMysqlTableDesc(table)
        self.cursor.close()
        self.cursor = self.conn.cursor()
        sql = """select * from  %s order by id asc""" % (table)
        n = self.cursor.execute(sql)
        data = self.cursor.fetchall()

        for row in data:
            ret = {}
            for k, key in enumerate(keys):
                if key == 'id':
                    key = '_id'
                if (types[k] == 1):
                    if row[k] == None:
                        ret[key] = 0
                        continue
                    ret[key] = int(row[k])
                elif (types[k] == 2):
                    if row[k] == None:
                        ret[key] = ''
                        continue
                    ret[key] = str(row[k])
                elif (types[k] == 3):
                    if row[k] == None:
                        ret[key] = ''
                        continue
                    ret[key] = float(row[k])
                else:
                    if row[k] == None:
                        ret[key] = ''
                        continue
                    ret[key] = str(row[k])
                    # if(table== 'hs_card') or (table== 'hs_hero'):
                    # ret['rand'] = random.random()
            print
            ret
            self.setMongoCollectionDocument(table, ret)

    # 预先在mysql建好名字对应的表，检测mysql字段插入对应数据
    def mongo2Mysql(self, table):
        keys, _ = self.getMysqlTableDesc(table)

        datas = self.mongodb[table].find()
        delKeys = []  # 存在差异的key，去除掉
        for data in datas:
            if datas:
                for mKey in data.keys():
                    if mKey not in keys:
                        delKeys.append(mKey)

            for delK in delKeys:
                if delK in data.keys():
                    del data[delK]
            self.insertMysql(table, data)

        self.conn.commit()

    def insertMysql(self, table, dict):
        qmarks = ', '.join(['%s'] * len(dict))  # 用于替换记录值
        cols = ', '.join(dict.keys())  # 字段名
        sql = "INSERT INTO %s (%s) VALUES (%s)" % (table.lower(), cols, qmarks)
        try:
            print
            'insertResult:', self.cursor.execute(sql, dict.values())
        except Exception, e:
            print
            e

    def __del__(self):
        self.mongo.close()
        self.cursor.close()
        self.conn.close()


if __name__ == '__main__':
    multiprocessing.log_to_stderr()
    logger = multiprocessing.get_logger()
    logger.setLevel(logging.INFO)

    t1 = time.time()
    cls = MysqlMongoConversion(logger)
    for tb in Config.tables:
        # cls.mysql2Mongo(tb) #mysql数据库转mongo数据
        cls.mongo2Mysql(tb)  # mongodb数据转mysql数据

    print
    time.time() - t1
    logger.info("done")