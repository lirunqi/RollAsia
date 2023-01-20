import scrapy
import json
import jsonpath
from winD.items import RealTime


# r是滚球
class HgGqSpider(scrapy.Spider):
    name = 'hg_gq'
    allowed_domains = ['odds.gooooal.com/match']

    def start_requests(self):
        base_url = 'http://odds.gooooal.com/match/1777/r_1777'
        for i in range(429, 433):
            url = base_url + str(i) + '.json'
            res = scrapy.Request(url, self.parse)
            yield res

    def parse(self, response):

        item = RealTime()
        tmp_item = jsonpath.jsonpath(json.loads(response.text), '$.al[?(@.c==1001)][*]')
        matchid = jsonpath.jsonpath(json.loads(response.text), '$.m.id')
        # series = jsonpath.jsonpath(json.loads(response.text), '$.m.l.id')

        for i in tmp_item[1][0]:
            item['date_time'] = i['t']
            item['realtime'] = i['rt']
            item['matchid'] = matchid
            item['compid'] = '1001'  # 皇冠
            item['realtimeresult'] = str(i['hg']) + '-' + str(i['ag'])
            item['home_odd'] = i['h']
            item['guest_odd'] = i['a']
            item['odd_term'] = i['p']
            yield item
