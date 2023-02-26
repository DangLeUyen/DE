# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import sqlite3

class BookPipeline:
    def __init__(self):
        self.con = sqlite3.connect('book_2.db')
        self.cur = self.con.cursor()
        self.create_table()

    def create_table(self):
        ## Create quotes table if none exists
        self.cur.execute("""CREATE TABLE IF NOT EXISTS products(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category VARCHAR(20),
            asin VARCHAR(20),
            book_name TEXT,
            author VARCHAR(50),
            price FLOAT,
            publication_date date,
            publisher VARCHAR(50),
            customer_review FLOAT,
            ratings FLOAT,
            isbn_13 VARCHAR(15),
            availability VARCHAR(15)
            )""")

    def process_item(self, item, spider):
        self.cur.execute("""INSERT INTO products (category, asin, book_name, author, price, publication_date, publisher, 
                            customer_review, ratings, isbn_13, availability) values (?,?,?,?,?,?,?,?,?,?,?)""",
                         (item['category'], item["asin"], item["book_name"], item["author"], item["price"], item["publication_date"],
                          item['publisher'], item["customer_review"], item['ratings'], item['isbn_13'], item['availability'])
                         )
        self.con.commit()
        return item

    def close_spider(self, spider):
        ## Close cursor & connection to database
        self.cur.close()
        self.con.close()
