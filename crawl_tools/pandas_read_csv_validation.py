# -*- coding:utf-8 _*-  
""" 
@author:Administrator
@file: pandas_read_csv_validation.py
@time: 2019/3/29
"""
import pandas as pd

# data = list(pd.read_csv(r'F:\PycharmProject\AmericanRealEstate\crawl_tools\realtor_app_list_page_search_criteria.csv')['list_criteria'])
# print(len(data))
# print(len(set(data)))

# print(data[-1])


# data1 = list(pd.read_csv(r'F:\PycharmProject\AmericanRealEstate\crawl_tools\realtor_app_list_page_search_criteria_one.csv')['list_criteria'])
# print(len(data1))
# print(len(set(data1)))
#
#
# data2 = list(pd.read_csv(r'F:\PycharmProject\AmericanRealEstate\crawl_tools\realtor_app_list_page_search_criteria_two.csv')['list_criteria'])
# print(len(data2))
# print(len(set(data2)))
#
#
# print(list(set(data1).intersection(set(data2))))


# list_a = [0,1,2,3,4,5,6,7,8,9]
# #
# # print(list_a[0:2])
#
# list_a.pop()
# print(list_a)

# if 'aaa' == 'aaa':
#     print('yes')

# a = False
# if a:
#     print('aa')
import re
last_url = 'https://mapi-ng.rdc.moveaws.com/api/v1/properties?offset=0&limit=200&county=Woodford&state_code=KY&sort=relevance&schema=mapsearch&client_id=rdc_mobile_native%2C9.4.2%2Candroid'
last_url_flag = re.findall('offset=0&limit=200&(.*)&sort=relevance&schema=mapsearch&client_id=',last_url)[0]


print(last_url_flag)


