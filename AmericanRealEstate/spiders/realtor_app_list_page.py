# -*- coding: utf-8 -*-
import json
import re

import scrapy
from scrapy_redis.spiders import RedisSpider


from AmericanRealEstate.items import RealtorListPageJsonItem


class RealtorAppListPageSpider(RedisSpider):
    name = 'realtor_app_list_page'
    allowed_domains = ['mapi-ng.rdc.moveaws.com']
    redis_key = "realtor:list_url"

    custom_settings = {
        "ITEM_PIPELINES": {
            'AmericanRealEstate.pipelines.RealtorListPageMysqlsqlPipeline': 301,

        },
        "DOWNLOADER_MIDDLEWARES": {
            # 'AmericanRealEstate.middlewares.RealtorListPageMiddleware': 545,

        },
        "SPIDER_MIDDLEWARES": {
            'AmericanRealEstate.middlewares.RealtorListPageMysqlSpiderMiddleware':544,
            'AmericanRealEstate.middlewares.RealtorCloseSpiderWhenRedisNullSpiderMiddleware': 545
        },
        "DEFAULT_REQUEST_HEADERS": {
            "Cache-Control": "public",
            "Mapi-Bucket": "for_sale_v2:on,for_rent_ldp_v2:on,for_rent_srp_v2:on,recently_sold_ldp_v2:on,recently_sold_srp_v2:on,not_for_sale_ldp_v2:on,not_for_sale_srp_v2:on,search_reranking_srch_rerank1:variant1",
            "Host": "mapi-ng.rdc.moveaws.com",
            "Connection": "Keep-Alive",
            "Accept-Encoding": "gzip",
            "User-Agent": "okhttp/3.10.0",
        },
        "COOKIES_ENABLED": False,
        "REDIRECT_ENABLED": False,
        "REFERER_ENABLED": False,
        "RETRY_ENABLED": False,
        "CONCURRENT_REQUESTS":  15,
        "REDIS_HOST": '127.0.0.1',
        'REDIS_PORT': 6379,
        "REACTOR_THREADPOOL_MAXSIZE": 100,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 10,
        # "LOG_FILE": "realtor_log.txt",
        "LOG_LEVEL": 'ERROR',

        # 指定 redis链接密码
        # 'REDIS_PARAMS': {
        #     'password': '123456',
        # },
        # redis 设置：
        # Enables scheduling storing requests queue in redis.
        "SCHEDULER": "scrapy_redis.scheduler.Scheduler",

        # Ensure all spiders share same duplicates filter through redis.
        "DUPEFILTER_CLASS": "scrapy_redis.dupefilter.RFPDupeFilter",
    }

    def parse(self, response):
        res_text = response.text
        res_url = response.url
        realtor_list_page_item = RealtorListPageJsonItem()
        realtor_list_page_item['jsonData'] = res_text

        json_res_listings = json.loads(res_text)['listings']

        offset = int(re.findall(r'offset=(.*)&limit', res_url)[0])
        county_name = re.findall(r'county=(.*)&state_code', res_url)[0]
        state_code = re.findall(r'state_code=(.*)&sort=relevance', res_url)[0]
        if len(json_res_listings) != 0:
            yield realtor_list_page_item
            next_url = 'https://mapi-ng.rdc.moveaws.com/api/v1/properties?offset={}&limit=200&county={}&state_code={}&sort=relevance&schema=mapsearch&client_id=rdc_mobile_native%2C9.4.2%2Candroid'.format(offset+200,county_name,state_code)
            yield scrapy.Request(url=next_url,callback=self.parse)









