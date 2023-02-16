import scrapy
from winD.items import Results,WindItem,RealTime
import re
import json
import jsonpath

class ResultsSpider(scrapy.Spider):
    name = 'results'
    allowed_domains = ['odds.gooooal.com']

    def __init__(self, name=None, **kwargs):
        super().__init__(name, kwargs)
        self.matchid = None

    def _re_response(self,re_obj):
        res_re = []
        res_hf_re = []
        for i in re_obj:
            res_re.append(re.findall("\((.*?)\)", i)[0])
            res_hf_re.append(i[:i.index("(")])
        return res_re,res_hf_re

    def start_requests(self):
        base_url = 'http://app.gooooal.com/sports/football/analysis/event.do?matchId=1777'
        for i in range(429, 433):
            self.matchid = '1777' + str(i)
            url = base_url + str(i) + '&lang=cn'
            res = scrapy.Request(url, self.parse)
            yield res


    def parse(self, response):
        sel = scrapy.Selector(response)
        item = Results()
        print('----------------------start-----------------------')
        a = sel.xpath('/html/body/div[12]/div/div[1]/div/div[1]/div[*]/p[2]/span[@class="span01"]/text()').extract()
        b = sel.xpath('/html/body/div[12]/div/div[1]/div/div[1]/div[*]/p[2]/span[@class="span01"]/text()').extract()
        home_hole_data,home_hf_data = self._re_response(a)
        guest_hole_data,guest_hf_data = self._re_response(b)
        item['matchid'] = self.matchid
        item['result'] = sel.xpath('/html/body/div[4]/div[2]/p[1]/text()').extract()
        item['home_team'] =sel.xpath('/html/body/div[4]/div[1]/p[1]/span/text()').extract()
        item['gust_team']=sel.xpath('/html/body/div[4]/div[3]/p[1]/span/text()').extract()
        item['hf_result'] = sel.xpath('/html/body/div[4]/div[2]/p[2]/text()').extract()
        item['home_conner_reslut'] = home_hole_data[6]
        item['hf_home_conner_reslut'] = home_hf_data[6]
        item['home_attact'] =home_hole_data[16]
        item['hf_home_attact']=home_hf_data[16]
        item['home_dgattact']=home_hole_data[17]
        item['hf_home_dgattact']=home_hf_data[17]
        item['guest_conner_reslut'] = guest_hole_data[6]
        item['hf_guest_conner_reslut']=guest_hf_data[6]
        item['guest_attact']=guest_hole_data[16]
        item['hf_guest_attact']=guest_hf_data[16]
        item['guest_dgattact']=guest_hole_data[17]
        item['hf_guest_dgattact']=guest_hf_data[17]
        item['home_shoot']=home_hole_data[0]
        item['hf_home_shoot']=home_hf_data[0]
        item['home_dshoot']=home_hole_data[1]
        item['hf_home_dshoot']=home_hf_data[1]
        item['guest_shoot']=guest_hole_data[0]
        item['hf_guest_shoot']=guest_hf_data[0]
        item['guest_dshoot']=guest_hole_data[1]
        item['hf_guest_dshoot']=guest_hf_data[1]
        yield item

        base_url = 'http://odds.gooooal.com/match/1777/r_1777'
        item = WindItem()
        for i in range(429, 433):
            url = base_url + str(i) + '.json'
            res = scrapy.Request(url, self.parse_realtime())
            yield res

    # 解析实时盘
    def parse_realtime(self,response):
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

        base_url = 'http://odds.gooooal.com/match/1777/h_1777'
        item = WindItem()
        for i in range(429, 433):
            url = base_url + str(i) + '.json'
            res = scrapy.Request(url, self.parse_cp)
            yield res

    def parse_cp(self,response):
            tmp_item = jsonpath.jsonpath(json.loads(response.text), '$.al[?(@.c==1001)][*]')
            matchid = jsonpath.jsonpath(json.loads(response.text), '$.m.id')
            series = jsonpath.jsonpath(json.loads(response.text), '$.m.l.id')
            item = WindItem()
            for i in tmp_item[1][0]:
                item['date_time'] = i['t']
                item['matchid'] = matchid
                item['series'] = series
                item['compid'] = '1001'  # 皇冠
                item['home_odd'] = i['h']
                item['guest_odd'] = i['a']
                item['odd_term'] = i['p']
                yield item
