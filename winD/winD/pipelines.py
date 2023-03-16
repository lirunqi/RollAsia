from queue import Queue
import pymysql


class MySQLDb:
    def __init__(self, host, port, user, password, database, table, columns):
        self.conn = pymysql.connect(
            host=host, port=port, user=user, password=password, database=database)
        self.cursor = self.conn.cursor()
        self.table = table
        self.columns = columns
        self.sql_insert_queue = Queue()

    def close_spider(self, spider):
        while not self.sql_insert_queue.empty():
            self.cursor.executemany(self.get_insert_query(),
                                    self.sql_insert_queue.get())
        self.conn.commit()
        self.conn.close()

    def get_insert_query(self):
        placeholders = ', '.join(['%s'] * len(self.columns))
        return f"insert into `{self.table}` ({','.join(self.columns)}) values ({placeholders})"

    def process_item(self, item, spider):
        values = [item.get(col, '') for col in self.columns]
        self.sql_insert_queue.put(values)
        if self.sql_insert_queue.qsize() >= 100:
            self.cursor.executemany(self.get_insert_query(),
                                    self.sql_insert_queue.get())
        return item


class MySQLPipeline(MySQLDb):
    def __init__(self, host, port, user, password, database, table, columns):
        super().__init__(host, port, user, password, database, table, columns)


class WindPipeline(MySQLDb):
    def __init__(self, host, port, user, password, database):
        super().__init__(host, port, user, password, database,
                         'beforeMatch', ['date_time', 'series', 'matchid', 'compid', 'home_odd', 'guest_odd', 'odd_term'])


class HgGq(MySQLDb):
    def __init__(self, host, port, user, password, database):
        super().__init__(host, port, user, password, database,
                         'realTimeMatch', ['date_time', 'realtime', 'matchid', 'compid', 'realtimeresult', 'home_odd', 'guest_odd', 'odd_term'])


class Results(MySQLDb):
    def __init__(self, host, port, user, password, database):
        super().__init__(host, port, user, password, database,
                         'matchResult', ['matchid', 'result', 'home_team', 'gust_team', 'hf_result', 'home_conner_reslut', 'hf_home_conner_reslut', 'home_attact', 'hf_home_attact', 'home_dgattact', 'hf_home_dgattact', 'home_shoot', 'hf_home_shoot', 'home_dshoot', 'hf_home_dshoot', 'guest_conner_reslut', 'hf_guest_conner_reslut', 'guest_attact', 'hf_guest_attact', 'guest_dgattact', 'hf_guest_dgattact', 'guest_shoot', 'hf_guest_shoot', 'guest_dshoot', 'hf_guest_dshoot'])
