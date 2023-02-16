# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import pymysql


class MysqlDb:
    def __init__(self):
        self.conn = pymysql.connect(host='localhost', port=3306, user='root', password='wobuhuiwan',
                                    database='GO_API')
        self.cursor = self.conn.cursor()  # 游标
        self.sql_insert = []
        return self.cursor

    def close_spider(self, spider):
        if len(self.sql_insert) > 0:
            self.conn.commit()
        self.conn.close()

    # todo批量100条插入sql，插入后清空待插list
    def pl_insert(self):
        self.conn.commit()
        self.sql_insert.clear()

# 初盘beforeMatch表
class WindPipeline(MysqlDb):
    # date_time = scrapy.Field()
    # matchid= scrapy.Field()
    # series= scrapy.Field()
    # compid= scrapy.Field()
    # home_odd= scrapy.Field()
    # guest_odd= scrapy.Field()
    # odd_term= scrapy.Field()
    def __init__(self):
        self.sql_insert = []

    def process_item(self, item, spider):
        # pass
        date_time = item.get('date_time', 0)
        series = item.get('series', '')
        matchid = item.get('matchid', -1)
        compid = item.get('compid', -1)
        home_odd = item.get('home_odd', -1)
        guest_odd = item.get('guest_odd', -1)
        odd_term = item.get('odd_term', '')
        self.sql_insert.append((date_time, series, matchid, compid, home_odd, guest_odd, odd_term))
        if len(self.sql_insert) == 100:
            self.cursor.executemany(
                "insert into `beforeMatch` (date_time,series,matchid,compid,home_odd,guest_odd,odd_term) values (FROM_UNIXTIME(%s),%s,%s,%s,%s,%s,%s)",
                self.sql_insert
            )
            self.pl_insert()
        return item

# 滚盘 realTimeMatch
class HgGq(MysqlDb):
    # date_time = scrapy.Field()
    # matchid = scrapy.Field()
    # compid = scrapy.Field()
    # realtimeresult = scrapy.Field()
    # home_odd = scrapy.Field()
    # guest_odd = scrapy.Field()
    # odd_term = scrapy.Field()
    def process_item(self, item, spider):
        # pass
        date_time = item.get('date_time', 0)
        realtime = item.get('realtime', '')
        matchid = item.get('matchid', -1)
        compid = item.get('compid', -1)
        realtimeresult = item.get('realtimeresult', '')
        home_odd = item.get('home_odd', -1)
        guest_odd = item.get('guest_odd', -1)
        odd_term = item.get('odd_term', '')
        self.sql_insert.append((date_time, realtime, matchid, compid, realtimeresult, home_odd, guest_odd, odd_term))
        if len(self.sql_insert) == 100:
            self.cursor.executemany(
                "insert into `realTimeMatch` (date_time,realtime,matchid,compid,realtimeresult,home_odd,guest_odd,odd_term) values (FROM_UNIXTIME(%s),%s,%s,%s,%s,%s,%s,%s)",
                self.sql_insert
            )
            self.pl_insert()
        return item

# 赛后 matchResult
class Results(MysqlDb):
    def process_item(self,item,spider):
        matchid = item.get('matchid','')
        result = item.get('result','')
        home_team = item.get('home_team','')
        gust_team = item.get()
        hf_result
        home_conner_reslut
        hf_home_conner_reslut
        home_attact
        hf_home_attact
        home_dgattact
        hf_home_dgattact
        home_shoot
        hf_home_shoot
        home_dshoot
        hf_home_dshoot
        guest_conner_reslut
        hf_guest_conner_reslut
        guest_attact
        hf_guest_attact
        guest_dgattact
        hf_guest_dgattact
        guest_shoot
        hf_guest_shoot
        guest_dshoot
        hf_guest_dshoot
