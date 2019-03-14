import datetime
from scrapy.cmdline import execute
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


execute(['scrapy', 'crawl',
         'realtor_app_list_page',
         ])


















