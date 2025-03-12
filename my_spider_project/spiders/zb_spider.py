from bs4 import BeautifulSoup
import requests
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
        print("开始爬取……")
        initial_url = "http://www.ccgp-henan.gov.cn/henan/list2?pageNo=1&pageSize=16&bz=0&gglx=0"
        yield scrapy.Request(url=initial_url, callback=self.parse_nav_links)

    def parse_nav_links(self, response):
        nav_links = response.css('div.list a:not([class])::attr(href)').getall()
        print("获取导航链接……")
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
        
        # 构造每页的URL，这里因为页数比较多，所以会浪费时间，将在这里实现爬取并保存到数据库的功能
        for page in range(1, all_page_num + 1):
            item = ZbItem()
            base_query['pageNo'] = [str(page)]
            new_query = '&'.join([f'{k}={v[0]}' for k,v in base_query.items()])
            nav_url = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}?{new_query}"
            res = requests.get(nav_url)
            soup = BeautifulSoup(res.text,'html.parser')
            ann_type = soup.find('h2',class_='Title02').text
            # 获取右侧栏内容
            right_sidebar = soup.find('div', class_='List2')
            if right_sidebar:
                a_tags = right_sidebar.find_all('a',href = True)
                date_spans = right_sidebar.find_all('span',class_='Gray Right')
                for i in range(len(a_tags)):
                    title = a_tags[i].text
                    href = a_tags[i]['href']
                    publish_date = date_spans[i]
                    # 请求详情页
                    detail_url = urljoin(self.base_url, href)
                    page_res =  requests.get(detail_url)
                    detail_soup = BeautifulSoup(page_res.text,'html.parser')
                    
                    item['title'] = title
                    item['ann_type'] = ann_type
                    item['publish_date'] = publish_date

                    # 提取附件链接
                    fujian_urls = []
                    try:
                        fujian_div = detail_soup.find('div',class_='List1')
                        fujian_urls = [a['href'] for a in fujian_div.find_all('a',href=True)]
                    except:
                        print("该公告无附件")

                    item['fujian_urls'] = fujian_urls

                    # 解析URL参数
                    parsed_url = urlparse(page_res.url)
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
                        self.logger.error(f"缺少必要参数 {', '.join(missing)} URL: {page_res.url}")
                        return
                    
                    content_url = f"{self.base_url}/henan/content?{urlencode({'infoId': infoId, 'channelCode': channelCode})}"
                    detail_res = requests.get(content_url)
                    
                    re_soup = BeautifulSoup(detail_res.text,'html.parser')
                    # item = detail_res.meta['item']

                    # 提取相关公告链接
                    res_urls = []
                    try:
                        re_div = re_soup.find('div',class_='List2')
                        for a_tag in re_div.find_all('a',href=True):
                            res_urls.append(a_tag['href'])
                    except:
                        print("该公告无相关公告")
                    item['res_urls'] = res_urls

                    # 提取正文内容
                    try:
                        script_div = re_soup.find_all('script')[-1]
                        cms_url = script_div.text.split('"')[1]
                        get_url = self.base_url + cms_url
                    except:
                        print("错误：无法获取正文链接")
                        continue
                    # script_div = detail_res.css('script').getall()[-1]
                    # cms_url = script_div.split('"')[1]
                    # get_url = urljoin(self.base_url, cms_url)

                    # main_content_res = scrapy.Request(url=get_url, callback=self.parse_main_content, meta={'item': item})
                    main_content_res = requests.get(get_url)
                    main_content_res.encoding = 'gbk2312'
                    # 提取正文内容
                    data_soup = BeautifulSoup(main_content_res.text, 'html.parser')
                    tb_content = data_soup.find('body')
                    if tb_content:
                        main_content = str(tb_content)
                        item['main_content'] = main_content
                    else:
                        item['main_content'] = "无法获取正文内容"
                    # 插入数据到数据库
                    print("正在插入数据库……")
                    self.insert_into_db(item)
                    yield item
            # yield scrapy.Request(url=nav_url, callback=self.parse_page)

    # def parse_page(self, response):
    #     # 获取公告类型
    #     ann_type = response.css('h2.Title02::text').get()
        
    #     # 获取右侧栏内容
    #     right_sidebar = response.css('div.List2')
    #     a_tags = right_sidebar.css('a')
    #     date_spans = right_sidebar.css('span.Gray.Right::text').getall()
        
    #     for i in range(len(a_tags)):
    #         title = a_tags[i].css('::text').get()
    #         href = a_tags[i].attrib['href']
    #         publish_date = date_spans[i]
            
    #         # 请求详情页
    #         detail_url = urljoin(self.base_url, href)
    #         yield scrapy.Request(url=detail_url, callback=self.parse_detail, meta={
    #             'title': title,
    #             'ann_type': ann_type,
    #             'publish_date': publish_date
    #         })

    # # def parse_detail(self, response):
    #     item = ZbItem()
    #     item['title'] = response.meta['title']
    #     item['ann_type'] = response.meta['ann_type']
    #     item['publish_date'] = response.meta['publish_date']
        
    #     # 提取附件链接
    #     fujian_div = response.css('div.List1')
    #     fujian_urls = [a.attrib['href'] for a in fujian_div.css('a') if 'href' in a.attrib]
    #     item['fujian_urls'] = fujian_urls
        
    #     # 解析URL参数
    #     parsed_url = urlparse(response.url)
    #     query_params = parse_qs(parsed_url.query)
        
    #     # 获取infoId和channelCode
    #     infoId = query_params.get('infoId', [''])[0]
    #     channelCode = query_params.get('channelCode', [''])[0]
        
    #     missing = []
    #     if not infoId:
    #         missing.append('infoId')
    #     if not channelCode:
    #         missing.append('channelCode')
    #     if missing:
    #         self.logger.error(f"缺少必要参数 {', '.join(missing)} URL: {response.url}")
    #         return
        
    #     content_url = f"{self.base_url}/henan/content?{urlencode({'infoId': infoId, 'channelCode': channelCode})}"
        
    #     yield scrapy.Request(url=content_url, callback=self.parse_content, meta={'item': item})

    # # def parse_content(self, response):
    #     item = response.meta['item']
        
    #     # 提取相关公告链接
    #     res_div = response.css('div.List2')
    #     res_urls = [a.attrib['href'] for a in res_div.css('a') if 'href' in a.attrib]
    #     item['res_urls'] = res_urls
        
    #     # 提取正文内容
    #     script_div = response.css('script').getall()[-1]
    #     cms_url = script_div.split('"')[1]
    #     get_url = urljoin(self.base_url, cms_url)
        
    #     yield scrapy.Request(url=get_url, callback=self.parse_main_content, meta={'item': item})

    # # def parse_main_content(self, response):
    #     item = response.meta['item']
        
    #     # 提取正文内容
    #     data_soup = BeautifulSoup(response.text, 'html.parser')
    #     tb_content = data_soup.find('table', class_='Content')
    #     if tb_content:
    #         main_content = str(tb_content)
    #         item['main_content'] = main_content
    #     else:
    #         item['main_content'] = "无法获取正文内容"
    #     # 插入数据到数据库
    #     print("正在插入数据库……")
    #     input()
    #     self.insert_into_db(item)
    #     yield item

    def insert_into_db(self, item):
        connection = pymysql.connect(
            host='localhost',
            user='root',
            password='123456',
            database='spider_db',
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        
        try:
            with connection.cursor() as cursor:
                sql = """
                INSERT INTO zb_table (title, ann_type, publish_date, fujian_urls, res_urls, main_content)
                VALUES (%s, %s, %s, %s, %s, %s)
                """
                cursor.execute(sql, (
                    item['title'],
                    item['ann_type'],
                    item['publish_date'],
                    ','.join(item['fujian_urls']),
                    ','.join(item['res_urls']),
                    item['main_content']
                ))
            connection.commit()
        except pymysql.Error as err:
            print(f"数据库插入错误: {err}") 
        finally:
            connection.close()