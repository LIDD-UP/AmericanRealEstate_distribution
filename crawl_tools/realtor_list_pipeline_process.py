# -*- coding:utf-8 _*-  
""" 
@author:Administrator
@file: realtor_list_pipeline_process.py
@time: 2019/3/14
"""
import sys
import json
from crawl_tools.get_sql_con import get_sql_con


class RealtorListPageMysqlsqlPipeline(object):
    houses = []

    def __init__(self):
        self.conn = get_sql_con()
        self.cursor = self.conn.cursor()
        self.sql = '''
            insert into tb_realtor_list_page_json(json_data,last_operation_date) values(%s,now())
        '''

    def json_process(self, item_data):
        json_dict_houses = item_data['listings']
        self.houses = [json.dumps(house) for house in json_dict_houses]

    def bulk_insert_to_mysql(self, bulkdata):
        print("插入长度", len(bulkdata))
        # sql = "insert into realtor_list_page_json(json_data,last_operation_date) values(%s,now())"
        self.cursor.executemany(self.sql, bulkdata)
        print("执行插入完毕")
        self.conn.commit()
        del self.houses[:]

    def process_item(self, item):
        json_item = json.loads(item)
        for item_data in json_item["data"]:
            self.json_process(item_data)
            self.bulk_insert_to_mysql(self.houses)


if __name__ == "__main__":

    dict_data = '''{"data":[{"listings":[{"property_id":"5528101208","listing_id":"652373674","prop_type":"condo","last_update":"2019-03-14T11:32:02Z","rdc_web_url":"https://www.realtor.com/realestateandhomes-detail/8253-Highway-98-D200_Navarre_FL_32566_M55281-01208","prop_sub_type":"condo","is_turbo":false,"address":"8253 Highway 98 Unit D200, Navarre, 32566","prop_status":"for_sale","price_raw":220000,"sqft_raw":1146,"list_date":"2019-03-14T11:40:37Z","office_name":"Re/Max Gulf Properties","products":["co_broke","basic_opt_in","co_broke"],"is_showcase":false,"price":"$220,000","beds":2,"baths":3,"sqft":"1,146 sq ft","lot_size":"1,307 sq ft lot","photo":"https://ap.rdcpix.com/1756448895/05fb1701ebdb475d2fa6e46f4bec2305l-m0x.jpg","is_cobroker":true,"short_price":"$220K","baths_half":1,"baths_full":2,"photo_count":25,"lat":30.4272976,"lon":-86.88539,"is_new_listing":true,"has_leadform":true,"page_no":1,"rank":1,"list_tracking":"type|property|data|prop_id|5528101208|list_id|652373674|page|rank|list_branding|listing_agent|listing_office|property_status|product_code|advantage_code^1|1|0|1|35T|9HU|0^^$0|1|2|$3|4|5|6|7|F|8|G|9|$A|H|B|I]|C|J|D|K|E|L]]"},{"property_id":"6049007396","listing_id":"652370298","prop_type":"single_family","last_update":"2019-03-13T22:28:13Z","rdc_web_url":"https://www.realtor.com/realestateandhomes-detail/2027-Heritage-Park-Way_Navarre_FL_32566_M60490-07396","is_turbo":false,"address":"2027 Heritage Park Way in Heritage Park, Navarre, 32566","prop_status":"for_sale","price_raw":415000,"sqft_raw":2676,"list_date":"2019-03-14T03:35:52Z","office_name":"Keller Williams Realty Navarre","products":["co_broke","basic_opt_in","co_broke"],"is_showcase":false,"price":"$415,000","beds":4,"baths":3,"sqft":"2,676 sq ft","lot_size":"0.26 acres","photo":"https://ap.rdcpix.com/694472650/cf1c33ba89af42adfd4bdb401dc8ca24l-m0x.jpg","is_cobroker":true,"short_price":"$415K","baths_full":3,"photo_count":63,"lat":30.407241,"lon":-86.840354,"is_new_listing":true,"has_leadform":true,"page_no":1,"rank":2,"list_tracking":"type|property|data|prop_id|6049007396|list_id|652370298|page|rank|list_branding|listing_agent|listing_office|property_status|product_code|advantage_code^1|2|0|1|35T|9HU|0^^$0|1|2|$3|4|5|6|7|F|8|G|9|$A|H|B|I]|C|J|D|K|E|L]]"}]},{"listings":[{"property_id":"5528101208","listing_id":"652373674","prop_type":"condo","last_update":"2019-03-14T11:32:02Z","rdc_web_url":"https://www.realtor.com/realestateandhomes-detail/8253-Highway-98-D200_Navarre_FL_32566_M55281-01208","prop_sub_type":"condo","is_turbo":false,"address":"8253 Highway 98 Unit D200, Navarre, 32566","prop_status":"for_sale","price_raw":220000,"sqft_raw":1146,"list_date":"2019-03-14T11:40:37Z","office_name":"Re/Max Gulf Properties","products":["co_broke","basic_opt_in","co_broke"],"is_showcase":false,"price":"$220,000","beds":2,"baths":3,"sqft":"1,146 sq ft","lot_size":"1,307 sq ft lot","photo":"https://ap.rdcpix.com/1756448895/05fb1701ebdb475d2fa6e46f4bec2305l-m0x.jpg","is_cobroker":true,"short_price":"$220K","baths_half":1,"baths_full":2,"photo_count":25,"lat":30.4272976,"lon":-86.88539,"is_new_listing":true,"has_leadform":true,"page_no":1,"rank":1,"list_tracking":"type|property|data|prop_id|5528101208|list_id|652373674|page|rank|list_branding|listing_agent|listing_office|property_status|product_code|advantage_code^1|1|0|1|35T|9HU|0^^$0|1|2|$3|4|5|6|7|F|8|G|9|$A|H|B|I]|C|J|D|K|E|L]]"},{"property_id":"6049007396","listing_id":"652370298","prop_type":"single_family","last_update":"2019-03-13T22:28:13Z","rdc_web_url":"https://www.realtor.com/realestateandhomes-detail/2027-Heritage-Park-Way_Navarre_FL_32566_M60490-07396","is_turbo":false,"address":"2027 Heritage Park Way in Heritage Park, Navarre, 32566","prop_status":"for_sale","price_raw":415000,"sqft_raw":2676,"list_date":"2019-03-14T03:35:52Z","office_name":"Keller Williams Realty Navarre","products":["co_broke","basic_opt_in","co_broke"],"is_showcase":false,"price":"$415,000","beds":4,"baths":3,"sqft":"2,676 sq ft","lot_size":"0.26 acres","photo":"https://ap.rdcpix.com/694472650/cf1c33ba89af42adfd4bdb401dc8ca24l-m0x.jpg","is_cobroker":true,"short_price":"$415K","baths_full":3,"photo_count":63,"lat":30.407241,"lon":-86.840354,"is_new_listing":true,"has_leadform":true,"page_no":1,"rank":2,"list_tracking":"type|property|data|prop_id|6049007396|list_id|652370298|page|rank|list_branding|listing_agent|listing_office|property_status|product_code|advantage_code^1|2|0|1|35T|9HU|0^^$0|1|2|$3|4|5|6|7|F|8|G|9|$A|H|B|I]|C|J|D|K|E|L]]"}]}]}'''
    realtor_test_dict = RealtorListPageMysqlsqlPipeline()
    realtor_test_dict.process_item(dict_data)
