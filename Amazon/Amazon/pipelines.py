# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import sqlite3

class BookPipeline:
    def __init__(self):
        self.con = sqlite3.connect('book.db')
        self.cur = self.con.cursor()
        self.create_table()

    def create_table(self):
        ## Create books table if none exists
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS author(
            author_id serial PRIMARY KEY,
            author_name VARCHAR(150)
        )
        """)

        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS publisher(
            publisher_id serial PRIMARY KEY,
            publisher_name VARCHAR(150)
        )
        """)

        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS parent_category(
            parent_category_id serial PRIMARY KEY,
            parent_category_name VARCHAR(100)
        )
        """)

        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS category(
            category_id serial PRIMARY KEY,
            category_name VARCHAR(100),
            parent_category_id INT REFERENCES parent_category(parent_category_id)
        )
        """)

        self.cur.execute("""CREATE TABLE IF NOT EXISTS book(
            book_id serial,
	        book_asin VARCHAR(20) unique not null PRIMARY KEY,
	        title text not null,
	        isbn_13 VARCHAR(15) unique not null,
	        price FLOAT,
	        rating FLOAT,
	        review_count INT,
	        publication_date DATE,
	        author_id INT not null REFERENCES author(author_id),
	        category_id INT not null REFERENCES category(category_id),
	        publisher_id INT REFERENCES publisher(publisher_id)
            )""")

    def process_item(self, item, spider):

        self.cur.execute("""INSERT INTO parent_category (parent_category_name) values (?)""", (item["parent_category"]))
        parent_category_id = self.cur.execute('SELECT last_insert_id() from parent_category')
        self.cur.execute("""INSERT INTO category (category_name,parent_category_id) values (?,?)""", (item["category_name"], parent_category_id))

        self.cur.execute("""INSERT INTO author (author_name) values (?)""", (item["author"]))

        self.cur.execute("""INSERT INTO publisher (publisher_name) values (?)""", (item["publisher"]))

        author_id = self.cur.execute('SELECT last_insert_id() from author')
        category_id = self.cur.execute('SELECT last_insert_id() from category')
        publisher_id = self.cur.execute('SELECT last_insert_id() from publisher')

        self.cur.execute("""INSERT INTO book (book_asin, title, isbn_13, price, rating, review_count, publication_date, author_id,
                            category_id, publisher_id) 
                            values (?,?,?,?,?,?,?,?,?,?)""",
                         (item["book_asin"], item["title"], item["isbn_13"], item["price"], item["rating"],
                          item["review_count"], item["publication_date"], author_id, category_id, publisher_id)
                         )
        self.con.commit()

        return item

    def close_spider(self, spider):
        ## Close cursor & connection to database
        self.cur.close()
        self.con.close()