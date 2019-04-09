import pandas as pd


data = pd.read_csv(r'G:\PycharmProject\AmericanRealEstate\crawl_tools\realtor_app_list_page_search_criteria.csv')

data_new = list(data['list_criteria'])

data_new = [x.replace('mapsearch','mapsearch&prop_status=for_rent') for x in data_new]

data_for_rent = pd.DataFrame()

data_for_rent['list_criteria'] = data_new
data_for_rent.to_csv('./realtor_app_list_page_search_criteria_for_rent.csv',index=False)



























