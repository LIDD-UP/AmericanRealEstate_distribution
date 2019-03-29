# -*- coding: utf-8 -*-
import json
import re

import scrapy


from AmericanRealEstate.items import RealtorListPageJsonItem
from AmericanRealEstate.settings import realtor_list_search_criteria
# from AmericanRealEstate.settings import realtor_list_search_criteria_test as realtor_list_search_criteria


class RealtorAppListPageSpider(scrapy.Spider):
    name = 'realtor_app_list_page'
    allowed_domains = ['mapi-ng.rdc.moveaws.com']
    start_urls = [x for x in realtor_list_search_criteria]
    last_item = False
    last_url = start_urls[-1]
    last_url_flag = re.findall('offset=0&limit=200&(.*)&sort=relevance&schema=mapsearch&client_id=', last_url)[0]
    custom_settings = {
        "ITEM_PIPELINES": {
            # 'AmericanRealEstate.pipelines.RealtorListPageMysqlsqlPipeline': 301,
            'AmericanRealEstate.pipelines.RealtorListStoredByServerPipeline': 302
        },
        "DOWNLOADER_MIDDLEWARES": {
            # 'AmericanRealEstate.middlewares.RealtorListPageMiddleware': 545,

        },
        "SPIDER_MIDDLEWARES": {
            'AmericanRealEstate.middlewares.RealtorListFinishSpiderMiddleware':544,
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


        "REACTOR_THREADPOOL_MAXSIZE": 100,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 10,
        # "LOG_FILE": "realtor_log.txt",
        "LOG_LEVEL": 'ERROR',

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

        present_url_flag = re.findall('limit=200&(.*)&sort=relevance&schema=mapsearch&client_id=', response.url)
        # print(present_url_flag)
        # print(RealtorAppListPageSpider.last_url_flag)
        if len(present_url_flag) != 0:
            present_url_flag = present_url_flag[0]
        if len(present_url_flag) == 0:
            present_url_flag = ''

        if present_url_flag == RealtorAppListPageSpider.last_url_flag and len(json_res_listings) == 0:
            print('已经是最后一个item了,提交item')

            RealtorAppListPageSpider.last_item = True
            yield realtor_list_page_item

        if len(json_res_listings) != 0:
            yield realtor_list_page_item
            next_url = 'https://mapi-ng.rdc.moveaws.com/api/v1/properties?offset={}&limit=200&county={}&state_code={}&sort=relevance&schema=mapsearch&client_id=rdc_mobile_native%2C9.4.2%2Candroid'.format(offset+200,county_name,state_code)
            yield scrapy.Request(url=next_url,callback=self.parse)














