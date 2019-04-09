# -*- coding:utf-8 _*-  
""" 
@author:Administrator
@file: split_list_search_criteria.py
@time: 2019/3/27
"""
import pandas as pd
from AmericanRealEstate.settings import list_search_criteria_stored_path


def split_list_search_citeria(split_count, file_path1, file_path_2,stored_root_path):
    data = pd.read_csv(file_path1)
    data2 = pd.read_csv(file_path_2)

    data_list = list(set((list(set(list(data['list_criteria']))) + list(set(list(data2['list_criteria']))))))
    print(data_list[1027])
    print(len(data_list))
    data_split_index = int(len(data_list)/split_count)
    print(data_split_index)
    data_part_one = data_list[:data_split_index]
    print(len(data_part_one))
    data_part_two = data_list[data_split_index:]
    print(len(data_part_two))

    data_part_one_df = pd.DataFrame(data_part_one,columns=['list_criteria'])
    data_part_two_df = pd.DataFrame(data_part_two, columns=['list_criteria'])

    data_part_one_df.to_csv(stored_root_path + '/realtor_app_list_page_search_criteria_one.csv',index=False)
    data_part_two_df.to_csv(stored_root_path + '/realtor_app_list_page_search_criteria_two.csv', index=False)
    # data_merge = data_part_one +data_part_two
    # print(len(set(data_merge)))


if __name__ == '__main__':
    splite_data_path = r'G:\PycharmProject\AmericanRealEstate\crawl_tools\realtor_app_list_page_search_criteria.csv'
    splite_data_path2 = r'G:\PycharmProject\AmericanRealEstate\crawl_tools\realtor_app_list_page_search_criteria_for_rent.csv'
    split_list_search_citeria(2,splite_data_path,splite_data_path2,list_search_criteria_stored_path)








