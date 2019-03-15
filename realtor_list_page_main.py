from scrapy.cmdline import execute
import os
import sys
sys.path.append("F:\PycharmProject\AmericanRealEstate")
print(os.path.abspath(__file__))


execute(['scrapy', 'crawl',
         'realtor_app_list_page',
         ])


















