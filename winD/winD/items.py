# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


# 初盘result
# date_time	日期时间
# matchid	比赛编号
# series	联赛类型
# compid	公司编号
# home_odd	主队赔
# guest_odd	客队赔
# odd_term	让球数
class WindItem(scrapy.Item):
    # define the fields for your item here like:
    date_time = scrapy.Field()
    matchid = scrapy.Field()
    series = scrapy.Field()
    compid = scrapy.Field()
    home_odd = scrapy.Field()
    guest_odd = scrapy.Field()
    odd_term = scrapy.Field()


# date_time	日期时间
# matchid	比赛编号
# compid	公司编号
# realtimeresult	实时结果
# home_odd	主队赔
# guest_odd	客队赔
# odd_term	让球数
class RealTime(scrapy.Item):
    date_time = scrapy.Field()
    realtime = scrapy.Field()
    matchid = scrapy.Field()
    compid = scrapy.Field()
    realtimeresult = scrapy.Field()
    home_odd = scrapy.Field()
    guest_odd = scrapy.Field()
    odd_term = scrapy.Field()


# matchid	比赛编号
# result	赛果
# home_team	主队
# gust_team	客队
# hf_result	半场赛果
# home_conner_reslut	主队角球数
# hf_home_conner_reslut	半场主队角球数
# home_attact	主队进攻
# hf_home_attact	半场主队进攻
# home_dgattact	主队危险进攻
# hf_home_dgattact	半场主队危险进攻
# guest_conner_reslut	客队角球
# hf_guest_conner_reslut	半场客队角球
# guest_attact	客队进攻
# hf_guest_attact	半场客队进攻
# guest_dgattact	客队危险进攻
# hf_guest_dgattact	半场客队危险进攻

class Results(scrapy.Item):
    matchid = scrapy.Field()
    result = scrapy.Field()
    home_team = scrapy.Field()
    gust_team = scrapy.Field()
    hf_result = scrapy.Field()
    home_conner_reslut = scrapy.Field()
    hf_home_conner_reslut = scrapy.Field()
    home_attact = scrapy.Field()
    hf_home_attact = scrapy.Field()
    home_dgattact = scrapy.Field()
    hf_home_dgattact = scrapy.Field()
    home_shoot = scrapy.Field()
    hf_home_shoot = scrapy.Field()
    home_dshoot = scrapy.Field()
    hf_home_dshoot = scrapy.Field()
    guest_conner_reslut = scrapy.Field()
    hf_guest_conner_reslut = scrapy.Field()
    guest_attact = scrapy.Field()
    hf_guest_attact = scrapy.Field()
    guest_dgattact = scrapy.Field()
    hf_guest_dgattact = scrapy.Field()
    guest_shoot = scrapy.Field()
    hf_guest_shoot = scrapy.Field()
    guest_dshoot = scrapy.Field()
    hf_guest_dshoot = scrapy.Field()