import time
class HandleKey(object):

    def __init__(self, tup_value, timefieldname):
        self.tup_value = tup_value
        self.timestamp_list = self.timestamp_list_solt(timefieldname)
        self.game_id = 'NIK-AHXY'

    def timestamp_list_format(self, timestamp_format_list, timefieldname):
        timestamp_list = ['all']
        timestamp = timefieldname
        time_struct = time.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
        for timestamp_format in timestamp_format_list:
            timestamp_list.append(time.strftime(timestamp_format, time_struct))
        return timestamp_list
    
    def timestamp_format(self, timestamp_format, timefieldname):
        time_struct = time.strptime(timefieldname, '%Y-%m-%d %H:%M:%S')
        timestamp = time.strftime(timestamp_format, time_struct)
        return timestamp

    def timestamp_list_solt(self, timefieldname):
        timestamp_format_list = ["%Y", "%Y%m", "%Y%m%d", "%Y%m%d%H"]
        timestamp_list = self.timestamp_list_format(timestamp_format_list, timefieldname)
        return timestamp_list

    def timestamp_solt(self, timefieldname):
        timestamp = timefieldname
        time_struct = time.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
        timestamp = time.strftime("%Y%m%d", time_struct)
        return timestamp

    def base_key_list(self, datatype):
        redis_key_list = []
        for timestamp in self.timestamp_list:
            redis_key = 'ROYAL:%s:%s:%s' % (datatype, self.game_id, timestamp)
            redis_key_list.append(redis_key)
        return redis_key_list

    def base_key(self, datatype, timefieldname):
        timestamp = self.timestamp_solt(timefieldname)
        redis_key = 'ROYAL:%s:%s:%s' % (datatype, self.game_id, timestamp)
        return redis_key

    def all_time_key(self,datatype):
        all_time_key = 'ROYAL:%s:%s:all' % (datatype,self.game_id)
        return all_time_key
