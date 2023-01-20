from scrapy import cmdline

if __name__ == '__main__':
    print('爬虫进程开始运行...')
    cmdline.execute('scrapy crawl results'.split())
