import json
import jsonpath
import pymysql
import re
# js = open('a.txt','r').read()
# a = json.loads(js)
# print(a.get('al'))

# a = '<span class="span01">15(4)</span>'
a = ['item1','item2']


print(a)
# a = ['15(4)','2(3)','44(33)']
# a_re = []
# b_re = []
# for i in a:
#     a_re.append(re.findall("\((.*?)\)",i)[0])
#     b_re.append(i[:i.index("(")])
# print(a_re)
# print(b_re)

# print(re.findall("\((.*?)\)",a))
# print(a[:a.index("(")])

# 打印测试 ?(c=="1001") 滚球
# item = jsonpath.jsonpath(a,'$.al[?(@.c==1001)][*]')
# # print(item)
# for i in item[1][0]:
#     print(i)

# 打印测试 ?(c=="1001") 初盘
# item = jsonpath.jsonpath(a,'$.al[?(@.c==1001)][*]')
# # print(item)
# for i in item[1][0]:
#     print(i)

# 打印测试 ?(c=="1001") id和联赛id
# item = jsonpath.jsonpath(a,'$.m.id')
# print(item)
# item = jsonpath.jsonpath(a,'$.m.l.id')
# print(item)


# 写数据库

# cnn = pymysql.connect(host='localhost',port=3306,user='root',password='wobuhuiwan',database='GO_API')
# curses = cnn.cursor()
# # print(jsonpath.jsonpath(a,'$.al[*].l[*][*]'))
# tmp = jsonpath.jsonpath(a,'$.al[*].l[*][*]')
# for i in tmp[:5]:
#     a = i['t']
#     b = str(i['h'])
#     curses.execute("insert into `result` (date_time,series) values(FROM_UNIXTIME(%s),%s)",
#             (a,b))
# cnn.commit()


# m = {'a':1,'b':2}
# print(m['a'])