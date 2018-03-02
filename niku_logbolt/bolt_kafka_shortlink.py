#-*-coding:utf8-*-
#!/usr/bin/env python
from __future__ import absolute_import
from pyleus.storm import SimpleBolt
import logging
from module.shortlinkhandlelogic import ShortlinkHandlelogic


log = logging.getLogger('test_kafka')
class LoginBolt(SimpleBolt):
    
    
    def process_tuple(self,tup):
         value = tup.values
         log.debug("kafka获取到的数据为:%s" % (value))
         shortlink_handle_logic_obj = ShortlinkHandlelogic(value)
         log.debug("开始os数登陆处理!!!!")
         shortlink_handle_logic_obj.account('SHORTLINKOS', 'os')
         log.debug("开始落入数据库处理!!!!")
         shortlink_handle_logic_obj.logWriteMysql()




if __name__ == '__main__':
    logging.basicConfig(
                level=logging.DEBUG,
                filename='/tmp/niku_login.log',
                format="%(asctime)s[%(levelname)s][%(process)d][%(thread)d][%(lineno)d]%(message)s",
                filemode='a',
    )
    LoginBolt().run()
