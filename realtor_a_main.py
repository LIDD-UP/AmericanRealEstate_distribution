import os
import sys
# print(os.path.abspath(__file__))
# sys.path.append(os.path.abspath(__file__))

import datetime
from scrapy.cmdline import execute
# from crawl_tools.get_psql_con import get_psql_con

execute(['scrapy', 'crawl',
         # 'realtor',
         # 'realtor_app',
         # 'realtor_property_web',
         'realtor_a',
         # "-a",
         # "start_urls={}".format(get_detail_url()),
         # "-a",
         # "user_agent_list={}".format(user_agent_list),
         # "-s",
         # "JOBDIR=crawls/realtor{}".format(num),
         ])