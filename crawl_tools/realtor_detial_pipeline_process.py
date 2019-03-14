# -*- coding:utf-8 _*-  
""" 
@author:Administrator
@file: realtor_detial_pipeline_process.py
@time: 2019/3/14
"""
from crawl_tools.get_sql_con import get_sql_con


class RealtordetailPageMysqlPipeline(object):
    def __init__(self):
        self.conn = get_sql_con()

    def process_item(self, item):
        cursor = self.conn.cursor()
        cursor.execute(
            '''
              UPDATE tb_realtor_detail_json set detail_json=%s ,is_dirty='0',last_operation_date=now(),data_interface='1'
              WHERE property_id =%s
            ''', [item['detailJson'], item['propertyId']
                  ]
        )
        print("插入数据成功")
        self.conn.commit()

if __name__ == "__main__":
    realtor_detail_test = RealtordetailPageMysqlPipeline()

    item_data = {"detailJson": '{"11":"123"}', "propertyId": 123445678}

    realtor_detail_test.process_item(item_data)