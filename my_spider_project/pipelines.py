import pymysql
from itemadapter import ItemAdapter
from .settings import MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB, MYSQL_TABLE

# class MysqlPipeline:
#     def open_spider(self, spider):
#         self.conn = pymysql.connect(
#             host=MYSQL_HOST,
#             port=MYSQL_PORT,
#             user=MYSQL_USER,
#             password=MYSQL_PASSWORD,
#             db=MYSQL_DB,
#             charset='utf8mb4',
#             cursorclass=pymysql.cursors.DictCursor
#         )
#         self.cursor = self.conn.cursor()

#     def close_spider(self, spider):
#         self.conn.close()

#     def process_item(self, item, spider):
#         sql = """
#         INSERT INTO {} (title, ann_type, publish_date, main_content, res_urls, fujian_urls)
#         VALUES (%s,%s,%s,%s,%s,%s)
#         """.format(MYSQL_TABLE)
        
#         self.cursor.execute(sql, (
#             item['title'],
#             item['ann_type'],
#             item['publish_date'],
#             item['main_content'],
#             str(item['res_urls']),
#             str(item['fujian_urls'])
#         ))
#         self.conn.commit()
#         return item


class ZbPipeline:
    def __init__(self):
        self.conn = None

    def open_spider(self, spider):
        try:
            self.conn = pymysql.connect(
                host="localhost",
                user="root",
                password="123456",
                database="spider_db",
                charset='utf8mb4'
            )
            self.cursor = self.conn.cursor()
        except pymysql.Error as err:
            print(f"数据库连接错误: {err}")

    def process_item(self, item, spider):
        try:
            res_urls_str = ','.join(item['res_urls'])
            fujian_urls_str = ','.join(item['fujian_urls'])
            
            sql = '''
            INSERT INTO zb_table (title, ann_type, main_content, res_urls, fujian_urls_str, publish_date)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            title = VALUES(title),
            ann_type = VALUES(ann_type),
            main_content = VALUES(main_content),
            res_urls = VALUES(res_urls),
            fujian_urls_str = VALUES(fujian_urls_str),
            publish_date = VALUES(publish_date)
            '''
            self.cursor.execute(sql, (
                item['title'],
                item['ann_type'],
                item['main_content'],
                res_urls_str,
                fujian_urls_str,
                item['publish_date']
            ))
            self.conn.commit()
        except pymysql.Error as err:
            print(f"保存数据时出错: {err}")
            self.conn.rollback()
        return item

    def close_spider(self, spider):
        if self.conn:
            self.conn.close()



class MySQLPipeline:
    def __init__(self, db_settings):
        self.db_settings = db_settings
        self.conn = None

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            db_settings=crawler.settings.get('DB_SETTINGS')
        )

    def open_spider(self, spider):
        self.conn = pymysql.connect(**self.db_settings)
        self.cursor = self.conn.cursor()
        self._create_table()

    def _create_table(self):
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS zb_table (
                id INT AUTO_INCREMENT PRIMARY KEY,
                title VARCHAR(500) NOT NULL,
                ann_type VARCHAR(50),
                main_content TEXT,
                res_urls TEXT,
                fujian_urls_str TEXT,
                publish_date VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
        ''')
        self.conn.commit()

    def process_item(self, item, spider):
        if isinstance(item, ZbItem):
            try:
                sql = '''
                    INSERT INTO zb_table (
                        title, ann_type, main_content, 
                        res_urls, fujian_urls_str, publish_date
                    )
                    VALUES (%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                        title=VALUES(title),
                        ann_type=VALUES(ann_type),
                        main_content=VALUES(main_content),
                        res_urls=VALUES(res_urls),
                        fujian_urls_str=VALUES(fujian_urls_str),
                        publish_date=VALUES(publish_date)
                '''
                self.cursor.execute(sql, (
                    item['title'],
                    item['ann_type'],
                    item['main_content'],
                    ','.join(item['res_urls']),
                    ','.join(item['fujian_urls']),
                    item['publish_date']
                ))
                self.conn.commit()
            except pymysql.Error as e:
                spider.logger.error(f"数据库错误 信息ID:{item['title']} 错误详情:{e}")
                self.conn.rollback()
        return item

    def close_spider(self, spider):
        if self.conn:
            self.conn.close()

class MySpiderProjectPipeline:
    def process_item(self, item, spider):
        return item
