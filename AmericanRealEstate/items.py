# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy

class RealtorListPageJsonItem(scrapy.Item):
    jsonData = scrapy.Field()


class RealtorDetailPageJsonItem(scrapy.Item):
    detailJson =scrapy.Field()
    propertyId = scrapy.Field()


class RealtorDetailPageJsonWebItem(scrapy.Item):
    detailJson =scrapy.Field()
    propertyId = scrapy.Field()


