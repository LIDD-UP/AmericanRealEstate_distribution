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
from AmericanRealEstate.settings import realtor_list_post_interface_url, realtor_detail_post_interface_url


class RealtorListStoredByServerPipeline(object):
    house_list = []
    list_session = requests.session()

    def post_data_to_server(self, data):
        post_data = {
            "data": data
        }
        list_json_data = json.dumps(post_data)
        print("list json data 没有去除空格的大小", len(list_json_data))
        list_json_data_strip = list_json_data.strip()
        print("list json data 去除空格的大小", len(list_json_data_strip))
        result = RealtorListStoredByServerPipeline.list_session.post(url=realtor_list_post_interface_url, json=list_json_data_strip)

    def process_item(self, item, spider):
        if isinstance(item, RealtorListPageJsonItem):
            self.house_list.append(json.loads(item['jsonData']))
            if len(self.house_list) >= 20 or len(spider.start_urls) == 0:
                print('list 数据列表已经达到要求，开始发送数据到服务器')
                time_now = time.time()
                self.post_data_to_server(self.house_list)
                print("发送数据消耗时间：{}".format(time.time() - time_now))
                print("list 数据发送到服务器成功")
                del self.house_list[:]

        return item


class RealtorDetailStoredByServerPipeline(object):
    house_list = []
    detail_session = requests.session()

    def post_data_to_server(self,data):

        post_data = {
            "data": data
        }
        detail_json_data = json.dumps(post_data)
        print("list json data 没有去除空格的大小", len(detail_json_data))
        detail_json_data_strip = detail_json_data.strip()
        print("list json data 去除空格的大小", len(detail_json_data_strip))
        result = RealtorDetailStoredByServerPipeline.detail_session.post(url=realtor_detail_post_interface_url, json=detail_json_data_strip)

    def process_item(self, item, spider):
        if isinstance(item, RealtorDetailPageJsonItem):
            detial_format_data = {
                "detailJson": json.loads(item['detailJson']),
                "propertyId": int(item['propertyId'])
            }

            self.house_list.append(detial_format_data)
            if len(self.house_list) >= 50 or len(spider.start_urls) == 0:
                print('详情数据列表已经满足要求开始发送数据到服务器')
                time_now = time.time()
                self.post_data_to_server(self.house_list)
                print("发送数据消耗时间：{}".format(time.time()-time_now))
                print("detail 数据发送到服务器成功")
                del self.house_list[:]
        return item

