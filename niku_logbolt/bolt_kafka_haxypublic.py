#-*-coding:utf8-*-
#!/usr/bin/env python
from __future__ import absolute_import
from pyleus.storm import SimpleBolt
import logging
from module.haxypubliclogic import HaxypublicLogic
import time

log = logging.getLogger('test_kafka')
class HaxypublicBolt(SimpleBolt):
    
    
    def process_tuple(self,tup):
         value = tup.values
         log.debug("kafka获取到的数据为:%s" % (value))
         try:
           haxypublic_handle_logic_obj = HaxypublicLogic(value)
         except:
           return
         table_name = haxypublic_handle_logic_obj.tup_value.split("|").pop().lower().strip()
         #haxypublic_handle_logic_obj.insertMysql()
         if table_name == 'logout':
             log.debug("开始uuidonline处理!!!!")
             haxypublic_handle_logic_obj.uuidonline()
         if table_name == 'devicelogoutlog':
             log.debug("开始deviceonline处理!!!!")
             haxypublic_handle_logic_obj.deviceonline()
             log.debug("开始uuiddevice处理!!!!")
             haxypublic_handle_logic_obj.uuiddevice()



if __name__ == '__main__':
    log_time = time.strftime('%Y%m%d', time.localtime(time.time()))
    log_filename = '/data/niku_logbolt/logs/test_haxypublic_%s.log' % (log_time)
    logging.basicConfig(
                level=logging.DEBUG,
                filename=log_filename,
                format="%(asctime)s[%(levelname)s][%(process)d][%(thread)d][%(lineno)d]%(message)s",
                filemode='a',
    )
    HaxypublicBolt().run()
