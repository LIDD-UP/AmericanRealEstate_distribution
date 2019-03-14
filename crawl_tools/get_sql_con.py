# -*- coding:utf-8 _*-  
""" 
@author:Administrator
@file: get_sql_con.py
@time: 2019/1/9
"""
import pymysql

from AmericanRealEstate import settings


def get_sql_con():
    mysql_con = pymysql.connect(
        host=settings.MYSQL_HOST,
        port=settings.MYSQL_PORT,
        user=settings.MYSQL_USER,
        passwd=settings.MYSQL_PASSWORD,
        db=settings.MYSQL_DBNAME,
        charset='utf8',
    )
    return mysql_con


if __name__ == '__main__':
    test_conn = get_sql_con()
    print(test_conn)

