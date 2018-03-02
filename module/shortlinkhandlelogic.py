#-*-coding:utf-8-*-
from handlelogic import HandleLogic
import time
from lib.mysql import mysql_operation
import logging
log = logging.getLogger('test_kafka')

class ShortlinkHandlelogic(HandleLogic):

    def __init__(self, tup):
        self.tup_value = eval(tup[0].values()[0])
        self.timefield = self.tup_value['createtime']
        super(ShortlinkHandlelogic, self).__init__()

    def logWriteMysql(self):
        ret = self.tup_value
        timestamp = self.timefield
        time_struct = time.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
        timestamp = time.strftime("%Y%m%d%H%M%S", time_struct)
        sql = """INSERT INTO shortlink(ip,url,code,createTime,os,useragent) VALUES ("%s", "%s", "%s", "%s","%s","%s")""" % (ret["ip"], ret["url"],ret["code"],timestamp,ret["os"],ret["useragent"])
        log.debug("用户行为执行的sql语句为:%s" % (sql))
        mysql_operation_obj = mysql_operation('logserver')
        mysql_operation_obj.mysql_insert(sql)
