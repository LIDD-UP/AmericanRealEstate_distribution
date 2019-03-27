# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/spider-middleware.html

import random
import requests
import time
import datetime
import re
from scrapy import signals
import os
# from AmericanRealEstate.settings import spider_close_process_shell_path
from AmericanRealEstate.settings import realtor_list_spider_close_process_url
from AmericanRealEstate.settings import realtor_detail_spider_close_process_url


class RealtorListPageMiddleware(object):
    def __init__(self):
        super(RealtorListPageMiddleware,self).__init__()
        self.stop_signal = 1

    def process_request(self, request, spider):
        # # 随机停顿
        random_seed = [0, 1]
        from random import choice
        a = choice(random_seed)
        print('a:-----------------', a)
        if a == 1:
            time.sleep(3)

    def process_response(self, request, response, spider):
        print(response.status)
        if response.status in [x for x in range(300,500)]:
            print('当前的status code：', response.status)
            # 设置暂停时间
            import time
            time.sleep(1)
            self.stop_signal += 1
            print(self.stop_signal)

            if self.stop_signal > 100:
                spider.crawler.engine.close_spider(spider, '爬虫已经被发现了')

        return response


class RealtorListFinishSpiderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_closed, signal=signals.spider_closed)

        return s

    def spider_closed(self, spider):
        requests.get(url=realtor_list_spider_close_process_url)
        print('list spider 整个过程完毕')


class RealtorDetailFinishSpiderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_closed, signal=signals.spider_closed)

        return s

    def spider_closed(self, spider):
        requests.get(url=realtor_detail_spider_close_process_url)
        print('detail spider 整个过程完毕')



class RealtorDetailPageAMiddleware(object):
    def __init__(self):
        super(RealtorDetailPageAMiddleware,self).__init__()
        self.stop_signal = 1

    def process_request(self, request, spider):

        # 爬虫爬取3个小时后停止31分钟
        spider_scrapy_start_time = spider.scrapy_start_time
        if spider_scrapy_start_time is not None:
            scrapy_time_now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            true_scrapy_time_now = datetime.datetime.strptime(scrapy_time_now, '%Y-%m-%d %H:%M:%S')
            time_seconds_subtract = true_scrapy_time_now - spider_scrapy_start_time
            print(time_seconds_subtract.seconds)
            time_seconds_subtract = int(time_seconds_subtract.seconds)

            print('时间间隔：', time_seconds_subtract)
            if time_seconds_subtract % 3600 == 0 and time_seconds_subtract !=0:
                print('sleep中')
                time.sleep(900)

    def process_response(self, request, response, spider):
        print(response.status)
        if response.status in [x for x in range(300,500)]:
            print('当前的status code：', response.status)
            # 设置暂停时间
            import time
            time.sleep(300)
            self.stop_signal += 1
            print(self.stop_signal)

            if self.stop_signal > 1000:
                spider.crawler.engine.close_spider(spider, '爬虫已经被发现了')

        return response


class RealtorDetailPageAProcessUrlMiddleware(object):
    def __init__(self):
        super(RealtorDetailPageAProcessUrlMiddleware, self).__init__()

    def process_request(self, request, spider):
        print(request.url)
        property_id = re.search(r'\d+',request.url).group()
        request._url='https://mapi-ng.rdc.moveaws.com/api/v1/properties/{}?client_id=rdc_mobile_native%2C9.3.7%2Candroid'.format(property_id)
        print(request.url)

    def process_response(self, request, response, spider):

        return response















