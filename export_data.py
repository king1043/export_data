# encoding=utf-8
import pymysql

import pymongo
from pymongo.collection import Collection

import logging

MONGO_HOST = 'localhost'
MONGO_PORT = 27017

MYSQL_DB   = 'new'
MYSQL_TAB  = 'tab_prog_auto'
MYSQL_HOST = '192.168.106.21'
MSYQL_PORT = 3306
MYSQL_USER = 'root'
MYSQL_PWD  = 'root'

class MySQL():
    def __init__(self):
        try:
            self.conn = pymysql.connect(host=MYSQL_HOST, port=MSYQL_PORT, user = MYSQL_USER, passwd=MYSQL_PWD, db=MYSQL_DB, charset='utf8')
            self.cursor = self.conn.cursor()
        except:
            print('connect mysql error.')

    def insert(self, sql):
        try:
            self.cursor.execute(sql)
            self.conn.commit()
        except:
            print('insert error')

    def update(self, sql):
        try:
            self.cursor.execute(sql)
            self.conn.commit()
        except:
            print('update error')

    def select(self, sql):
        count = self.cursor.execute(sql)
        result=self.cursor.fetchall()
        return count, result

    def close(self):
        self.cursor.close()
        self.conn.close()

class MongoDb():
    def __init__(self, db = '', host = MONGO_HOST, port = MONGO_PORT):
        self.host = host
        self.port = port
        self.db = db

    def getMongoDB(self):
        try:
            self.client = pymongo.MongoClient(self.host, self.port)
            db = self.db
            self.db = self.client.riqiyi_struts  #这需要手动填mongodb数据库名
            return self.db
        except:
            print('connect mongodb error.')

    def close(self):
        self.client.close()

if __name__ == '__main__':
    mysql = MySQL()
    mongoDB = MongoDb()
    db = mongoDB.getMongoDB()

    mysqlKeys = '(prog_uuid, site_id,pub_channel, prog_url, prog_title, release_time, prog_type, play_count, comment_count, read_status)'

    flag = True
    while flag:
        flag = False
        mongoDatas = db.recent_iqiyi_info.find({'read_status':0}).limit(100)  # 这需要手动填写mongodb表名

        mysqlDatas = []
        for data in mongoDatas:
            mysqlData = (data['_id'], 1, data['pub_channel'],data['url'], data['title'], data['issueTime'], data['type'], int(data['playcount'] == [] and '0' or data['playcount']), int(data['cmts_count'] == [] and '0' or data['cmts_count']), 0)
            print(data['_id'])

            #先查找mysql中是否有即将插入的url 若已存在 则不插入
            sql = 'select * from tab_prog_auto where prog_url = "' + data['url'] + '"'
            result = mysql.select(sql)
            if result[0]:
                print('mysql 已存在 url ：%s'%data['url'])
            else:
                sql = 'insert into %s '%MYSQL_TAB + mysqlKeys + ' values("%s",%d,"%s","%s","%s","%s","%s",%d,%d,%d)'%mysqlData
                mysql.insert(sql)

                db.recent_iqiyi_info.update(data, {'$set':{'read_status':1}})

            flag = True

        sql = '''
                UPDATE tab_prog_auto
                SET prog_type_id = (
                SELECT
                   p.prog_property_id
                FROM
                   tab_prog_property p
                WHERE
                   p.prog_property_name = prog_type
                )
                WHERE prog_type_id IS NULL
        '''
        msyql.update(sql)


    mongoDB.close()
    mysql.close()
    pause