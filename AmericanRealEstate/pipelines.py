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
import zlib

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
        list_json_data_strip = list_json_data.replace(' ', '')
        print("list json data 去除空格的大小", len(list_json_data_strip))

        # # 进行数据压缩
        json_data_encode = list_json_data_strip.encode()
        list_json_data_strip = zlib.compress(json_data_encode)
        print('压缩后的大小',len(list_json_data_strip))

        result = RealtorListStoredByServerPipeline.list_session.post(url=realtor_list_post_interface_url,
                                                                     data = list_json_data_strip,
                                                                     # json=list_json_data_strip
                                                                     )

    def process_item(self, item, spider):
        if isinstance(item, RealtorListPageJsonItem):

            print("----------------是不是最后一个item：",spider.last_item)
            RealtorListStoredByServerPipeline.house_list.append(json.loads(item['jsonData']))
            if spider.last_item:
                print("已经是最后一批数据了，发送最后一批数据")
                time_now = time.time()
                self.post_data_to_server(RealtorListStoredByServerPipeline.house_list)
                print("发送数据消耗时间：{}".format(time.time() - time_now))
                print("list 数据发送到服务器成功")
                del RealtorListStoredByServerPipeline.house_list[:]

            if len(RealtorListStoredByServerPipeline.house_list) == 20:
                print('list 数据列表已经达到要求，开始发送数据到服务器')
                time_now = time.time()
                self.post_data_to_server(RealtorListStoredByServerPipeline.house_list)
                print("发送数据消耗时间：{}".format(time.time() - time_now))
                print("list 数据发送到服务器成功")
                del RealtorListStoredByServerPipeline.house_list[:]
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
        detail_json_data_strip = detail_json_data.replace(' ', '')
        print("list json data 去除空格的大小", len(detail_json_data_strip))

        json_data_encode = detail_json_data_strip.encode()
        detail_json_data_strip = zlib.compress(json_data_encode)
        print('压缩后的大小',len(detail_json_data_strip))

        result = RealtorDetailStoredByServerPipeline.detail_session.post(url=realtor_detail_post_interface_url,
                                                                         data = detail_json_data_strip,
                                                                         # json=detail_json_data_strip
                                                                         )

    def process_item(self, item, spider):
        if isinstance(item, RealtorDetailPageJsonItem):
            detial_format_data = {
                "detailJson": json.loads(item['detailJson']),
                "propertyId": int(item['propertyId'])
            }

            RealtorDetailStoredByServerPipeline.house_list.append(detial_format_data)

            print("--------------------最后一个item的状态", spider.last_item)

            if spider.last_item:
                print('发送最后一批数据')
                time_now = time.time()
                self.post_data_to_server(RealtorDetailStoredByServerPipeline.house_list)
                print("发送数据消耗时间：{}".format(time.time() - time_now))
                print("最后一批detail 数据发送到服务器成功")
                del RealtorDetailStoredByServerPipeline.house_list[:]

            if len(RealtorDetailStoredByServerPipeline.house_list) == 100:
                print('详情数据列表已经满足要求开始发送数据到服务器')
                time_now = time.time()
                self.post_data_to_server(RealtorDetailStoredByServerPipeline.house_list)
                print("发送数据消耗时间：{}".format(time.time()-time_now))
                print("detail 数据发送到服务器成功")
                del RealtorDetailStoredByServerPipeline.house_list[:]

        return item

