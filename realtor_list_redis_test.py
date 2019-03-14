# -*- coding:utf-8 _*-  
""" 
@author:Administrator
@file: realtor_list_redis_test.py
@time: 2019/3/14
"""
from AmericanRealEstate.settings import realtor_list_search_criteria


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


get_list_url()