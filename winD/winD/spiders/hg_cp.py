import scrapy
import json
import jsonpath
from winD.items import WindItem

# r网址是滚球 h是初盘
class HgCpSpider(scrapy.Spider):
    name = 'hg_cp'
    allowed_domains = ['odds.gooooal.com/match']
    start_urls = ['http://odds.gooooal.com/match/1777/h_1777429.json']

    def parse(self, response):
        base_url = 'http://odds.gooooal.com/match/1777/h_1777'
        item = WindItem()
        for i in range(429,433):
            url = base_url + str(i) + '.json'
            res = scrapy.Request(url,self.parse)
            yield res

            tmp_item = jsonpath.jsonpath(json.loads(response.text),'$.al[?(@.c==1001)][*]')
            matchid = jsonpath.jsonpath(json.loads(response.text), '$.m.id')
            series = jsonpath.jsonpath(json.loads(response.text), '$.m.l.id')

            for i in tmp_item[1][0]:
                item['date_time'] = i['t']
                item['matchid'] = matchid
                item['series'] = series
                item['compid'] = '1001'   # 皇冠
                item['home_odd'] = i['h']
                item['guest_odd'] = i['a']
                item['odd_term'] = i['p']
                yield item
