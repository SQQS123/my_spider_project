import pymysql
from itemadapter import ItemAdapter
from .settings import MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB, MYSQL_TABLE

import asyncio
import aiomysql
from scrapy.exceptions import NotConfigured

class MariaDBAsyncPipeline:
    def __init__(self, db_config):
        self.db_config = db_config
        self.loop = asyncio.get_event_loop()
        self.conn = None

    @classmethod
    def from_crawler(cls, crawler):
        db_config = crawler.settings.get("MARIADB_CONFIG")
        if not db_config:
            raise NotConfigured("MariaDB configuration is missing")
        return cls(db_config)

    async def _create_connection(self):
        """异步创建数据库连接"""
        self.conn = await aiomysql.connect(**self.db_config)
        self.cursor = await self.conn.cursor()

    async def _close_connection(self):
        """异步关闭数据库连接"""
        if self.cursor:
            await self.cursor.close()
        if self.conn:
            self.conn.close()
            await self.conn.wait_closed()

    async def _process_item(self, item):
        """异步处理单个数据项"""
        try:
            insert_query = "INSERT INTO your_table_name (column1, column2) VALUES (%s, %s)"
            data = (item["field1"], item["field2"])
            await self.cursor.execute(insert_query, data)
            await self.conn.commit()
        except aiomysql.Error as err:
            print(f"Failed to insert item: {err}")
            await self.conn.rollback()

    def open_spider(self, spider):
        """在爬虫启动时创建数据库连接"""
        self.loop.run_until_complete(self._create_connection())

    def close_spider(self, spider):
        """在爬虫关闭时关闭数据库连接"""
        self.loop.run_until_complete(self._close_connection())

    def process_item(self, item, spider):
        """在 Pipeline 中处理数据项"""
        self.loop.run_until_complete(self._process_item(item))
        return item


# class ZbPipeline:
#     def __init__(self):
#         self.conn = None

#     def open_spider(self, spider):
#         try:
#             self.conn = pymysql.connect(
#                 host="localhost",
#                 user="root",
#                 password="123456",
#                 database="spider_db",
#                 charset='utf8mb4'
#             )
#             self.cursor = self.conn.cursor()
#         except pymysql.Error as err:
#             print(f"数据库连接错误: {err}")

#     def process_item(self, item, spider):
#         try:
#             res_urls_str = ','.join(item['res_urls'])
#             fujian_urls_str = ','.join(item['fujian_urls'])
            
#             sql = '''
#             INSERT INTO zb_table (title, ann_type, main_content, res_urls, fujian_urls_str, publish_date)
#             VALUES (%s, %s, %s, %s, %s, %s)
#             ON DUPLICATE KEY UPDATE
#             title = VALUES(title),
#             ann_type = VALUES(ann_type),
#             main_content = VALUES(main_content),
#             res_urls = VALUES(res_urls),
#             fujian_urls_str = VALUES(fujian_urls_str),
#             publish_date = VALUES(publish_date)
#             '''
#             self.cursor.execute(sql, (
#                 item['title'],
#                 item['ann_type'],
#                 item['main_content'],
#                 res_urls_str,
#                 fujian_urls_str,
#                 item['publish_date']
#             ))
#             self.conn.commit()
#         except pymysql.Error as err:
#             print(f"保存数据时出错: {err}")
#             self.conn.rollback()
#         return item

#     def close_spider(self, spider):
#         if self.conn:
#             self.conn.close()



# class MySQLPipeline:
#     def __init__(self, db_settings):
#         self.db_settings = db_settings
#         self.conn = None

#     @classmethod
#     def from_crawler(cls, crawler):
#         return cls(
#             db_settings=crawler.settings.get('DB_SETTINGS')
#         )

#     def open_spider(self, spider):
#         self.conn = pymysql.connect(**self.db_settings)
#         self.cursor = self.conn.cursor()
#         self._create_table()

#     def _create_table(self):
#         self.cursor.execute('''
#             CREATE TABLE IF NOT EXISTS zb_table (
#                 id INT AUTO_INCREMENT PRIMARY KEY,
#                 title VARCHAR(500) NOT NULL,
#                 ann_type VARCHAR(50),
#                 main_content TEXT,
#                 res_urls TEXT,
#                 fujian_urls_str TEXT,
#                 publish_date VARCHAR(50),
#                 created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
#             ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
#         ''')
#         self.conn.commit()

#     def process_item(self, item, spider):
#         if isinstance(item, ZbItem):
#             try:
#                 sql = '''
#                     INSERT INTO zb_table (
#                         title, ann_type, main_content, 
#                         res_urls, fujian_urls_str, publish_date
#                     )
#                     VALUES (%s,%s,%s,%s,%s,%s)
#                     ON DUPLICATE KEY UPDATE
#                         title=VALUES(title),
#                         ann_type=VALUES(ann_type),
#                         main_content=VALUES(main_content),
#                         res_urls=VALUES(res_urls),
#                         fujian_urls_str=VALUES(fujian_urls_str),
#                         publish_date=VALUES(publish_date)
#                 '''
#                 self.cursor.execute(sql, (
#                     item['title'],
#                     item['ann_type'],
#                     item['main_content'],
#                     ','.join(item['res_urls']),
#                     ','.join(item['fujian_urls']),
#                     item['publish_date']
#                 ))
#                 self.conn.commit()
#             except pymysql.Error as e:
#                 spider.logger.error(f"数据库错误 信息ID:{item['title']} 错误详情:{e}")
#                 self.conn.rollback()
#         return item

#     def close_spider(self, spider):
#         if self.conn:
#             self.conn.close()
