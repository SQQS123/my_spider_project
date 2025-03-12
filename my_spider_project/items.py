# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class ZbItem(scrapy.Item):
    title = scrapy.Field()
    ann_type = scrapy.Field()
    main_content = scrapy.Field()
    res_urls = scrapy.Field()
    fujian_urls = scrapy.Field()
    publish_date = scrapy.Field()