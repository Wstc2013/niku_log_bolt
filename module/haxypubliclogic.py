#-*-coding:utf-8-*-
from handlelogic_haxy import HandleLogic
import time
from lib.mysql import mysql_operation
import logging
import configparser
from config.mysqlinfo import switch
import json
log = logging.getLogger('test_kafka')


def insertpublic(table_name,values,column):
    newvalues = []
    for value in values:
      newvalues.append('"%s"' %(value))
    newvalue = ','.join(newvalues)
    insert_rbd = "INSERT INTO %s(%s) VALUES (%s)" % (table_name,column,newvalue)
    log.debug(insert_rbd)
    mysql_operation_rdb_obj = mysql_operation('loginput')
    mysql_operation_rdb_obj.mysql_insert(insert_rbd)



class HaxypublicLogic(HandleLogic):
    #config = configparser.ConfigParser()
    #config.read("config/mysqlinfo.ini",encoding='utf-8')

    def __init__(self, tup):
        self.tup_value = self.tup_value_solt(tup)
        self.timefield = self.tup_value.split("|")[0]
        super(HaxypublicLogic, self).__init__()

    def tup_value_solt(self, tup):
        try:
           new_value = tup[0]
        except Exception as e:
           log.debug(u"%s" % (e))
        return new_value


    def insertMysql(self):
        value = self.tup_value.split("|")
        table_name = value.pop().lower().strip()
        #log.debug(self.config.get(table_name, "columnname"))
        insertpublic(table_name,value,switch[table_name])

    def deviceonline(self):
        value = self.tup_value.split("|")
        userid = value[2]
        equipmentid = value[6]
        onlinesecond = value[8]
        key = self.handle_key_obj.all_time_key("DEVICEONLINE")
        log.debug(u"对应的key为:%s,filed为:%s" % (key,equipmentid))
        value = self.redis_obj.hget(key, equipmentid)
        log.debug(u"当前value值为:%s" % (value))
        newvalue = {}
        jsonvalue = []
        uuid = []
        if value == None:
          newvalue['uuid'] = userid
          newvalue['onlineTime'] = int(onlinesecond)
          jsonvalue.append(newvalue)
        else:
          value_list = json.loads(value)
          for value in value_list:
              uuid.append(value['uuid'])
          if userid in uuid:
              for value in value_list:
                 if value['uuid'] == userid:
                     value["onlineTime"] = value["onlineTime"] + int(onlinesecond)
                 jsonvalue.append(value)
          else:
              jsonvalue = value_list
              newvalue['uuid'] = userid
              newvalue['onlineTime'] = int(onlinesecond)
              jsonvalue.append(newvalue)
        jsonvalue = json.dumps(jsonvalue)
        log.debug(u"新的value值为:%s" % (jsonvalue))
        self.redis_obj.hset(key, equipmentid, jsonvalue)

    def uuidonline(self):
        value = self.tup_value.split("|")
        userid = [value[2]]
        onlinesecond = value[8]
        key = self.handle_key_obj.base_key_list("UUIDONLINE")
        self.newsum(key,userid,onlinesecond)
     
    def uuiddevice(self):
        value = self.tup_value.split("|")
        userid = value[2]
        equipmentid = value[6]
        key = self.handle_key_obj.all_time_key("UUIDDEVICE")
        log.debug(u"对应的key为:%s,filed为:%s" % (key,userid))
        value = self.redis_obj.hget(key, userid)
        log.debug(u"当前value值为:%s" % (value))
        newvalue = {}
        newvalue = {}
        jsonvalue = []
        deviceid = []
        if value == None:
          newvalue['deviceId'] = equipmentid
          newvalue['deviceCount'] = 1
          jsonvalue.append(newvalue)
        else:
          value_list = json.loads(value)
          for value in value_list:
              deviceid.append(value['deviceId'])
          if equipmentid in deviceid:
              for value in value_list:
                 if value['deviceId'] == equipmentid:
                     value["deviceCount"] = value["deviceCount"] + 1
                 jsonvalue.append(value)
          else:
              jsonvalue = value_list
              newvalue['deviceId'] = equipmentid
              newvalue['deviceCount'] = 1
              jsonvalue.append(newvalue)
        jsonvalue = json.dumps(jsonvalue)
        log.debug(u"新的value值为:%s" % (jsonvalue))
        self.redis_obj.hset(key, userid, jsonvalue)
