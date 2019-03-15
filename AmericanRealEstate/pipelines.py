# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import json
import os
import requests

from AmericanRealEstate.items import RealtorListPageJsonItem, RealtorDetailPageJsonItem
from crawl_tools.get_sql_con import get_sql_con
from crawl_tools.test_file import post_url
from AmericanRealEstate.settings import realtor_list_post_interface_url, realtor_detail_post_interface_url
from AmericanRealEstate.settings import realtor_list_pipeline_process_path, realtor_detial_pipeline_process_path, spider_close_process_shell_path


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

    def process_item(self, item, spider):
        if isinstance(item, RealtorListPageJsonItem):
            self.house_list.append(json.loads(item['jsonData']))
            if len(self.house_list) >= 3:
                print('数据显示',self.house_list)
                post_data = {
                    "data": self.house_list
                }
                result = requests.post(url=realtor_list_post_interface_url, json=json.dumps(post_data))
                print('发送数据结果{}'.format(result))

                del self.house_list[:]
        return item


class RealtorDetailStoredByServerPipeline(object):
    house_list = []

    def process_item(self, item, spider):
        if isinstance(item,RealtorDetailPageJsonItem):
            detial_format_data = {
                "detailJson": json.loads(item['detailJson']),
                "propertyId": int(item['propertyId'])
            }
            self.house_list.append(detial_format_data)
            if len(self.house_list) >= 3:
                print('数据显示',self.house_list)
                post_data = {
                    "data": self.house_list
                }
                result = requests.post(url=realtor_detail_post_interface_url, json=json.dumps(post_data))
                print('发送数据结果{}'.format(result))

                del self.house_list[:]
        return item

