# -*- coding:utf-8 _*-  
""" 
@author:Administrator
@file: realtor_list_process.py
@time: 2019/3/14
"""
import os
from AmericanRealEstate.settings import realtor_list_search_criteria, realtor_list_page_main_path


class RealtorListProcess(object):

    @staticmethod
    def get_list_url():
        import redis
        pool = redis.ConnectionPool(host='127.0.0.1',
                                    # password='123456'
                                    )
        redis_pool = redis.Redis(connection_pool=pool)
        redis_pool.flushdb()
        for url in realtor_list_search_criteria:
            print(url)
            redis_pool.lpush('realtor:list_url', url)

    @staticmethod
    def truncate_list_json_and_split_table():
        """
        清空realtor_list_page_json 表和realtor_list_page_json_split 表
        :return:
        """
        from crawl_tools.get_sql_con import get_sql_con

        truncate_realtor_list_str = '''
            TRUNCATE tb_realtor_list_page_json;
        '''

        truncate_realtor_list_splite_str = '''
            TRUNCATE tb_realtor_list_page_json_splite
        '''
        conn = get_sql_con()
        cursor = conn.cursor()
        cursor.execute(truncate_realtor_list_str)
        conn.commit()
        cursor.execute(truncate_realtor_list_splite_str)
        conn.commit()
        conn.close()
        print('清空realtor_list_page_json 表和清空清空realtor_list_page_json_splite 表成功')

    @staticmethod
    def execute_list_spider():
        # 本机list spider启动
        os.system("python {}".format(realtor_list_page_main_path))
        # 其他机器list spider启动
        # pass


if __name__ == "__main__":
    realtor_process = RealtorListProcess()
    realtor_process.get_list_url()
    realtor_process.truncate_list_json_and_split_table()
    # realtor_process.execute_list_spider()





