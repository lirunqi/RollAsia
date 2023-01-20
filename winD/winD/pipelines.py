# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import pymysql


class WindPipeline:
    def __init__(self):
        self.conn = pymysql.connect(host='localhost', port=3306, user='root', password='wobuhuiwan',
                                    database='GO_API')
        self.cursor = self.conn.cursor()   # 游标

    def close_spider(self,spider):
        self.conn.commit()
        self.conn.close()
    # date_time = scrapy.Field()
    # matchid= scrapy.Field()
    # series= scrapy.Field()
    # compid= scrapy.Field()
    # home_odd= scrapy.Field()
    # guest_odd= scrapy.Field()
    # odd_term= scrapy.Field()
    def process_item(self, item, spider):
        # pass
        date_time = item.get('date_time',0)
        series = item.get('series','')
        matchid = item.get('matchid',-1)
        compid = item.get('compid',-1)
        home_odd = item.get('home_odd',-1)
        guest_odd = item.get('guest_odd',-1)
        odd_term = item.get('odd_term','')
        self.cursor.execute(
            "insert into `beforeMatch` (date_time,matchid,series,compid,home_odd,guest_odd,odd_term) values (FROM_UNIXTIME(%s),%s,%s,%s,%s,%s,%s)",
            (date_time,matchid,series,compid,home_odd,guest_odd,odd_term)
        )
        return item

class HgGq:
    def __init__(self):
        self.conn = pymysql.connect(host='localhost', port=3306, user='root', password='wobuhuiwan',
                                    database='GO_API')
        self.cursor = self.conn.cursor()   # 游标

    def close_spider(self,spider):
        self.conn.commit()
        self.conn.close()
    # date_time = scrapy.Field()
    # matchid = scrapy.Field()
    # compid = scrapy.Field()
    # realtimeresult = scrapy.Field()
    # home_odd = scrapy.Field()
    # guest_odd = scrapy.Field()
    # odd_term = scrapy.Field()
    def process_item(self, item, spider):
        # pass
        date_time = item.get('date_time',0)
        realtime = item.get('realtime','')
        matchid = item.get('matchid',-1)
        compid = item.get('compid',-1)
        realtimeresult = item.get('realtimeresult','')
        home_odd = item.get('home_odd',-1)
        guest_odd = item.get('guest_odd',-1)
        odd_term = item.get('odd_term','')
        self.cursor.execute(
            "insert into `realTimeMatch` (date_time,realtime,matchid,compid,realtimeresult,home_odd,guest_odd,odd_term) values (FROM_UNIXTIME(%s),%s,%s,%s,%s,%s,%s,%s)",
            (date_time,realtime,matchid,compid,realtimeresult,home_odd,guest_odd,odd_term)
        )
        return item