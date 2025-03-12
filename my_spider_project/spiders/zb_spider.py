from bs4 import BeautifulSoup
import scrapy
from urllib.parse import urljoin, parse_qs, urlparse, urlencode
import sys
import os
from my_spider_project.items import ZbItem # type: ignore
import pymysql

class ZbSpider(scrapy.Spider):
    name = "zb_spider"
    allowed_domains = ["ccgp-henan.gov.cn"]
    base_url = "http://www.ccgp-henan.gov.cn"
    
    def start_requests(self):
        initial_url = "http://www.ccgp-henan.gov.cn/henan/list2?pageNo=1&pageSize=16&bz=0&gglx=0"
        yield scrapy.Request(url=initial_url, callback=self.parse_nav_links)

    def parse_nav_links(self, response):
        nav_links = response.css('div.list a:not([class])::attr(href)').getall()
        for link in nav_links:
            yield response.follow(link, self.parse_nav)

    def parse_nav(self, response):
        # 获取总页数
        page_info = response.css('li.pageInfo::text').get()
        all_page_num = 1
        if page_info:
            all_page_num = int(''.join(filter(str.isdigit, page_info)))
        
        # 解析原始URL参数
        parsed_url = urlparse(response.url)
        base_query = parse_qs(parsed_url.query)
        
        # 构造每页的URL
        for page in range(1, all_page_num + 1):
            base_query['pageNo'] = [str(page)]
            new_query = '&'.join([f'{k}={v[0]}' for k,v in base_query.items()])
            nav_url = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}?{new_query}"
            yield scrapy.Request(url=nav_url, callback=self.parse_page)

    def parse_page(self, response):
        # 获取公告类型
        ann_type = response.css('h2.Title02::text').get()
        
        # 获取右侧栏内容
        right_sidebar = response.css('div.List2')
        a_tags = right_sidebar.css('a')
        date_spans = right_sidebar.css('span.Gray.Right::text').getall()
        
        for i in range(len(a_tags)):
            title = a_tags[i].css('::text').get()
            href = a_tags[i].attrib['href']
            publish_date = date_spans[i]
            
            # 请求详情页
            detail_url = urljoin(self.base_url, href)
            yield scrapy.Request(url=detail_url, callback=self.parse_detail, meta={
                'title': title,
                'ann_type': ann_type,
                'publish_date': publish_date
            })

    def parse_detail(self, response):
        item = ZbItem()
        item['title'] = response.meta['title']
        item['ann_type'] = response.meta['ann_type']
        item['publish_date'] = response.meta['publish_date']
        
        # 提取附件链接
        fujian_div = response.css('div.List1')
        fujian_urls = [a.attrib['href'] for a in fujian_div.css('a') if 'href' in a.attrib]
        item['fujian_urls'] = fujian_urls
        
        # 解析URL参数
        parsed_url = urlparse(response.url)
        query_params = parse_qs(parsed_url.query)
        
        # 获取infoId和channelCode
        infoId = query_params.get('infoId', [''])[0]
        channelCode = query_params.get('channelCode', [''])[0]
        
        missing = []
        if not infoId:
            missing.append('infoId')
        if not channelCode:
            missing.append('channelCode')
        if missing:
            self.logger.error(f"缺少必要参数 {', '.join(missing)} URL: {response.url}")
            return
        
        content_url = f"{self.base_url}/henan/content?{urlencode({'infoId': infoId, 'channelCode': channelCode})}"
        
        yield scrapy.Request(url=content_url, callback=self.parse_content, meta={'item': item})

    def parse_content(self, response):
        item = response.meta['item']
        
        # 提取相关公告链接
        res_div = response.css('div.List2')
        res_urls = [a.attrib['href'] for a in res_div.css('a') if 'href' in a.attrib]
        item['res_urls'] = res_urls
        
        # 提取正文内容
        script_div = response.css('script').getall()[-1]
        cms_url = script_div.split('"')[1]
        get_url = urljoin(self.base_url, cms_url)
        
        yield scrapy.Request(url=get_url, callback=self.parse_main_content, meta={'item': item})

    def parse_main_content(self, response):
        item = response.meta['item']
        
        # 提取正文内容
        data_soup = BeautifulSoup(response.text, 'html.parser')
        tb_content = data_soup.find('table', class_='Content')
        if tb_content:
            main_content = str(tb_content)
            item['main_content'] = main_content
        else:
            item['main_content'] = "无法获取正文内容"
        
        yield item