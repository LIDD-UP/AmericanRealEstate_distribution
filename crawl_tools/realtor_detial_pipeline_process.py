# -*- coding:utf-8 _*-  
""" 
@author:Administrator
@file: realtor_detial_pipeline_process.py
@time: 2019/3/14
"""
import json
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
            ''', [json.dumps(item['detailJson']), json.dumps(item['propertyId'])
                  ]
        )
        print("插入数据成功")
        self.conn.commit()

    def traversal_json_data(self,json_data):
        json_data = json.loads(json_data)
        for format_data in json_data['data']:
            self.process_item(format_data)


if __name__ == "__main__":
    realtor_detail_test = RealtordetailPageMysqlPipeline()
    item_data = json.dumps({"data":[{"detailJson": '{"11":"123"}', "propertyId": 123445678},{"detailJson": '{"11":"123"}', "propertyId": 123445678}]})
    realtor_detail_test.traversal_json_data(item_data)