# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import json
import os
import requests
import threading
import time

from AmericanRealEstate.items import RealtorListPageJsonItem, RealtorDetailPageJsonItem
from crawl_tools.get_sql_con import get_sql_con
from crawl_tools.test_file import post_url
from AmericanRealEstate.settings import realtor_list_post_interface_url, realtor_detail_post_interface_url
from AmericanRealEstate.settings import realtor_get_list_search_criteria_url,realtor_get_detail_search_criteria_url
from AmericanRealEstate.settings import realtor_list_spider_close_process_url,realtor_detial_spider_start_url


class RealtordetailPageMysqlPipeline(object):
    def __init__(self):
        self.conn = get_sql_con()

    def process_item(self, item, spider):
        if isinstance(item,RealtorDetailPageJsonItem):
            cursor = self.conn.cursor()
            cursor.execute(
                '''
                  UPDATE tb_realtor_detail_json set detail_json=%s ,is_dirty='0',last_operation_date=now(),data_interface='1'
                  WHERE property_id =%s
                ''', [item['detailJson'], item['propertyId']
                      ]
            )
            self.conn.commit()

        return item


class RealtorListPageMysqlsqlPipeline(object):
    houses = []

    def __init__(self):
        self.conn = get_sql_con()
        self.cursor = self.conn.cursor()
        self.sql = '''
            insert into tb_realtor_list_page_json(json_data,last_operation_date) values(%s,now())
        '''

    def json_process(self, item_data):
        json_dict = json.loads(item_data)
        json_dict_houses = json_dict['listings']
        self.houses = [json.dumps(house) for house in json_dict_houses]

    def bulk_insert_to_mysql(self, bulkdata):
        print("插入长度", len(bulkdata))
        self.cursor.executemany(self.sql, bulkdata)
        print("执行插入完毕")
        self.conn.commit()
        del self.houses[:]

    def process_item(self, item, spider):
        if isinstance(item, RealtorListPageJsonItem):
            self.json_process(item['jsonData'])
            self.bulk_insert_to_mysql(self.houses)
        return item


class RealtorListStoredByServerPipeline(object):
    house_list = []
    # list_session = requests.session()

    def post_data_to_server(self, data):
        post_data = {
            "data": data
        }
        result = requests.post(url=realtor_list_post_interface_url, json=json.dumps(post_data))

    def process_item(self, item, spider):
        if isinstance(item, RealtorListPageJsonItem):

            # 爬虫主动请求的方式;可能会导致数据没有抓取完全就导致爬虫退出
            # if not spider.server.exists(spider.redis_key):
            #     req = requests.get(url=realtor_get_list_search_criteria_url)
            #     print(req.text)
            #     if req.text == 'list页搜索条件已经空了':
            #         # list搜索条件空，启动list结束的数据处理操作；
            #         requests.get(url=realtor_list_spider_close_process_url)
            #         # 数据处理操作完成之后进行开启详情页爬虫
            #         requests.get(url=realtor_detial_spider_start_url)

            self.house_list.append(json.loads(item['jsonData']))
            if len(self.house_list) >= 5 or not spider.server.exists(spider.redis_key):
                print('list 数据列表已经达到要求，开始发送数据到服务器')
                time_now = time.time()
                self.post_data_to_server(self.house_list)
                print("发送数据消耗时间：{}".format(time.time() - time_now))
                print("list 数据发送到服务器成功")
                del self.house_list[:]
        return item


class RealtorDetailStoredByServerPipeline(object):
    house_list = []
    # detail_session = requests.session()

    def post_data_to_server(self,data):

        post_data = {
            "data": data
        }
        result = requests.post(url=realtor_detail_post_interface_url, json=json.dumps(post_data))

    def process_item(self, item, spider):
        if isinstance(item, RealtorDetailPageJsonItem):

            # # 爬虫端主动请求需要爬取的url
            # if not spider.server.exists(spider.redis_key):
            #     req = requests.get(url=realtor_get_detail_search_criteria_url)
            #     print(req.text)
            #     if req.text=='detail搜索条件已经空了':
            #         print('time to close detail spider')
            #         # 发送信号关闭爬虫

            detial_format_data = {
                "detailJson": json.loads(item['detailJson']),
                "propertyId": int(item['propertyId'])
            }
            self.house_list.append(detial_format_data)
            if len(self.house_list) >= 50 or not spider.server.exists(spider.redis_key):
                print('详情数据列表已经满足要求开始发送数据到服务器')
                time_now = time.time()
                self.post_data_to_server(self.house_list)
                print("发送数据消耗时间：{}".format(time.time()-time_now))
                print("detail 数据发送到服务器成功")
                del self.house_list[:]
        return item

