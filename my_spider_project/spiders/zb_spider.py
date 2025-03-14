from bs4 import BeautifulSoup
import scrapy
from urllib.parse import urljoin, parse_qs, urlparse, urlencode
from my_spider_project.items import ZbItem
import pymysql
from twisted.enterprise import adbapi
import asyncio
from twisted.internet import asyncioreactor

# 设置 Reactor
asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
asyncioreactor.install(asyncio.get_event_loop())

class ZbSpider(scrapy.Spider):
    name = "zb_spider"
    allowed_domains = ["ccgp-henan.gov.cn"]
    base_url = "http://www.ccgp-henan.gov.cn"
    custom_settings = {
        'CONCURRENT_REQUESTS': 16,
        'DOWNLOAD_DELAY': 0.5,
        'COOKIES_ENABLED': False
    }

    def __init__(self):
        self.dbpool = adbapi.ConnectionPool(
            'pymysql',
            host='localhost',
            user='root',
            password='123456',
            database='spider_db',
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )

    def start_requests(self):
        initial_url = "http://www.ccgp-henan.gov.cn/henan/list2?pageNo=1&pageSize=16&bz=0&gglx=0"
        yield scrapy.Request(url=initial_url, callback=self.parse_nav_links)

    def parse_nav_links(self, response):
        nav_links = response.css('div.list a:not([class])::attr(href)').getall()
        for link in nav_links:
            yield response.follow(link, self.parse_nav)

    def parse_nav(self, response):
        page_info = response.css('li.pageInfo::text').get()
        all_page_num = 1
        if page_info:
            all_page_num = int(''.join(filter(str.isdigit, page_info)))
        
        parsed_url = urlparse(response.url)
        base_query = parse_qs(parsed_url.query)
        
        for page in range(1, all_page_num + 1):
            base_query['pageNo'] = [str(page)]
            new_query = '&'.join([f'{k}={v[0]}' for k,v in base_query.items()])
            nav_url = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}?{new_query}"
            yield scrapy.Request(nav_url, callback=self.parse_page)

    def parse_page(self, response):
        soup = BeautifulSoup(response.text, 'html.parser')
        ann_type = soup.find('h2', class_='Title02').text

        right_sidebar = soup.find('div', class_='List2')
        if right_sidebar:
            a_tags = right_sidebar.find_all('a', href=True)
            date_spans = right_sidebar.find_all('span', class_='Gray Right')
            for i in range(len(a_tags)):
                item = ZbItem()
                title = a_tags[i].text
                href = a_tags[i]['href']
                publish_date = date_spans[i].text
                item['title'] = title
                item['ann_type'] = ann_type
                item['publish_date'] = publish_date

                detail_url = urljoin(self.base_url, href)
                yield scrapy.Request(detail_url, callback=self.parse_detail, meta={'item': item})

    def parse_detail(self, response):
        item = response.meta['item']
        detail_soup = BeautifulSoup(response.text, 'html.parser')

        fujian_urls = []
        try:
            fujian_div = detail_soup.find('div', class_='List1')
            fujian_urls = [a['href'] for a in fujian_div.find_all('a', href=True)]
        except:
            pass
        item['fujian_urls'] = fujian_urls

        parsed_url = urlparse(response.url)
        query_params = parse_qs(parsed_url.query)
        infoId = query_params.get('infoId', [''])[0]
        channelCode = query_params.get('channelCode', [''])[0]

        if not infoId or not channelCode:
            self.logger.error(f"缺少必要参数 URL: {response.url}")
            return

        content_url = f"{self.base_url}/henan/content?{urlencode({'infoId': infoId, 'channelCode': channelCode})}"
        yield scrapy.Request(content_url, callback=self.parse_content, meta={'item': item})

    def parse_content(self, response):
        item = response.meta['item']
        re_soup = BeautifulSoup(response.text, 'html.parser')

        res_urls = []
        try:
            re_div = re_soup.find('div', class_='List2')
            for a_tag in re_div.find_all('a', href=True):
                res_urls.append(a_tag['href'])
        except:
            pass
        item['res_urls'] = res_urls

        try:
            script_div = re_soup.find_all('script')[-1]
            cms_url = script_div.text.split('"')[1]
            get_url = self.base_url + cms_url
        except:
            self.logger.error("错误：无法获取正文链接")
            return

        yield scrapy.Request(get_url, callback=self.parse_main_content, meta={'item': item})

    def parse_main_content(self, response):
        item = response.meta['item']
        response.encoding = 'gbk2312'
        data_soup = BeautifulSoup(response.text, 'html.parser')
        tb_content = data_soup.find('body')
        item['main_content'] = str(tb_content) if tb_content else "无法获取正文内容"
        self.insert_into_db(item)
        # print("插入成功")
        yield item

    def insert_into_db(self, item):
        query = self.dbpool.runInteraction(self.do_insert, item)
        query.addErrback(self.handle_error, item)

    def do_insert(self, cursor, item):
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

    def handle_error(self, failure, item):
        self.logger.error(f"数据库插入错误: {failure}")