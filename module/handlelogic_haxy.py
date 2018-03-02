#-*-coding:utf8-*-
import time
import redis
import configparser
from field.handlefield_haxy import HandleField
from key.handlekey_haxy import HandleKey
import logging
log = logging.getLogger('test_kafka')

class HandleLogic(object):
    config = configparser.ConfigParser()
    config.read("config/config.ini",encoding='utf-8')
    redis_obj = redis.Redis(host=config.get("redis", "hostname"), port=int(config.get("redis", "port")), db=0)

    def __init__(self):
        self.handle_key_obj = HandleKey(self.tup_value, self.timefield)
        self.handle_field_obj = HandleField(self.tup_value)

    def newaccount(self, account_key_list, fields):
        log.debug(u"对应的key为:%s,filed为:%s" % (account_key_list, fields))
        for account_key in account_key_list:
            for field in fields:
                self.redis_obj.hincrby(account_key, field)

    def newcount(self, count_key_list, fields):
        log.debug(u"对应的key为:%s,filed为:%s" % (count_key_list,fields))
        for count_key in count_key_list:
            for field in fields:
                self.redis_obj.hincrby(count_key, field)

    def newnum(self, num_type_key_list,fields,type_value):
        log.debug(u"存放uuid或IMEI的集合key为:%s" % (num_type_key_list))
        if type_value == 'UUID':
            s_value = self.tup_value['UUID']
        elif type_value == 'IMEI':
            s_value = self.tup_value['IMEI']
        for num_type_key in num_type_key_list:
            log.debug(u"判断%s是否为活跃用户或活跃设备" % (s_value))
            if not  self.redis_obj.sismember(num_type_key,s_value):
                log.debug(u"为活跃用户或活跃设备进行处理")
                num_type_key_split = num_type_key.split(':')
                num_type_key_split.pop()
                num_type_key_split.pop()
                num_key = ':'.join(num_type_key_split)
                log.debug("num的key为:%s,field为:%s" % (num_key,fields))
                for field in fields:
                   self.redis_obj.hincrby(num_key, field)
                self.redis_obj.sadd(num_type_key, s_value)

    def newsum(self, sum_key_list, fields, value):
        for sum_key in sum_key_list:
            for field in fields:
                self.redis_obj.hincrbyfloat(sum_key, field, value)

