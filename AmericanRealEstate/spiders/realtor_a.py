# -*- coding: utf-8 -*-
import re
import datetime
import time
import scrapy
from urllib.parse import urljoin
import pandas as pd
import ast

from AmericanRealEstate.items import RealtorDetailPageJsonItem
from AmericanRealEstate.settings import realtor_detial_search_criteria


class RealtorASpider(scrapy.Spider):
    name = 'realtor_a'
    allowed_domains = ['mapi-ng.rdc.moveaws.com']
    start_urls = [x for x in realtor_detial_search_criteria]

    def __init__(self,
                 *args, **kwargs):
        super(RealtorASpider, self).__init__(*args, **kwargs)
        true_scrapy_start_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        true_scrapy_start_time = datetime.datetime.strptime(true_scrapy_start_time, '%Y-%m-%d %H:%M:%S')
        self.scrapy_start_time = true_scrapy_start_time


    custom_settings = {
        "ITEM_PIPELINES": {
            # 'AmericanRealEstate.pipelines.RealtordetailPageMysqlPipeline': 301,
            'AmericanRealEstate.pipelines.RealtorDetailStoredByServerPipeline': 302,


        },
        "DOWNLOADER_MIDDLEWARES": {
            'AmericanRealEstate.middlewares.RealtorDetailPageAMiddleware': 545,
        },
        "SPIDER_MIDDLEWARES": {
            'AmericanRealEstate.middlewares.RealtorDetailFinishSpiderMiddleware': 544,
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
        "CONCURRENT_REQUESTS": 15,
        "REFERER_ENABLED": False,
        "RETRY_ENABLED": False,
        "REACTOR_THREADPOOL_MAXSIZE": 100,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 10,
        # "CONCURRENT_REQUESTS_PER_IP" : 100,

        # "RETRY_HTTP_CODES": [500, 502, 503, 504, 400, 408]

        # "LOG_FILE": "realtor_log.txt",
        "LOG_LEVEL": 'ERROR',
    }

    def parse(self,response):
        # 接口的parse
        realtor_detail_pageJson_item = RealtorDetailPageJsonItem()
        realtor_detail_pageJson_item['detailJson'] = response.text
        realtor_detail_pageJson_item['propertyId'] = re.findall(r'api/v1/properties/(\d.*)\?client_id=',response.url)[0]
        yield realtor_detail_pageJson_item
