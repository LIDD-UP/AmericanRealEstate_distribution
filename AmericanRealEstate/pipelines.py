# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import json
from twisted.enterprise import adbapi
import pymysql

from AmericanRealEstate.items import RealtorListPageJsonItem, RealtorDetailPageJsonItem, RealtorDetailPageJsonWebItem
from crawl_tools.get_sql_con import get_sql_con
from crawl_tools.test_file import post_url
from AmericanRealEstate.settings import post_interface_url



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
        if isinstance(item, RealtorDetailPageJsonWebItem):
            cursor = self.conn.cursor()

            cursor.execute(
                '''
                  UPDATE tb_realtor_detail_json set detail_json=%s ,is_dirty='0',last_operation_date=now(),data_interface='2'
                  WHERE "property_id" =%s
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
        self.sql2 = '''
            INSERT INTO tb_realtor_list_page_json_splite (property_id, last_update, address, last_operation_date)
            values(%s,%s,%s,now())
        '''

    def json_process(self,item_data):
        json_dict = json.loads(item_data)
        json_dict_houses = json_dict['listings']
        # self.houses = [[json.dumps(house['property_id']),json.dumps(house['last_update']),json.dumps(house['address'])] for house in json_dict_houses]
        self.houses = [json.dumps(house) for house in json_dict_houses]

    def bulk_insert_to_mysql(self, bulkdata):
        print("插入长度", len(bulkdata))
        # sql = "insert into realtor_list_page_json(json_data,last_operation_date) values(%s,now())"
        self.cursor.executemany(self.sql, bulkdata)
        print("执行插入完毕")
        self.conn.commit()
        del self.houses[:]


    def process_item(self, item, spider):
        if isinstance(item, RealtorListPageJsonItem):
            self.json_process(item['jsonData'])
            self.bulk_insert_to_mysql(self.houses)

        return item




