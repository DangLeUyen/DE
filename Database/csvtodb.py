import psycopg2
import csv

conn = psycopg2.connect(database="amazonbooks",
                        host="localhost",
                        user="postgres",
                        password="123456",
                        port="5432")

cur = conn.cursor()

# Drop tables if they existed
cur.execute("""
     DROP TABLE IF EXISTS author, parent_category, category, publisher, book
 """)

# Create an immediate table that gets all data from csv and
# then we use this table to import data into other tables
cur.execute("""
    CREATE TABLE immediate_table (
	book_asin VARCHAR(20),
	parent_category VARCHAR(100),
	category VARCHAR(100),
	isbn_13 VARCHAR(15),
	title text,
	author VARCHAR(150),
	price FLOAT,
	publication_date DATE,
	publisher VARCHAR(150),
	rating FLOAT,
	review_count INT
)
""")

with open('cleaned_books.csv', 'r') as f:
    reader = csv.reader(f)
    next(reader) # Skip the header row.
    for row in reader:
        cur.execute(
        "INSERT INTO immediate_table VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        row
    )

############# end importing from csv #############

############ Start creating our database #########

# Create table author
cur.execute("""
    CREATE TABLE author(
    author_id serial PRIMARY KEY,
    author_name VARCHAR(150)
)
""")

cur.execute("""
    CREATE TABLE publisher(
    publisher_id serial PRIMARY KEY,
    publisher_name VARCHAR(150)
)
""")

cur.execute("""
    CREATE TABLE parent_category(
    parent_category_id serial PRIMARY KEY,
    parent_category_name VARCHAR(100)
)
""")

cur.execute("""
    CREATE TABLE category(
    category_id serial PRIMARY KEY,
    category_name VARCHAR(100),
    parent_category_id INT REFERENCES parent_category(parent_category_id)
)
""")

cur.execute("""
    CREATE TABLE book (
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
	)
""")

# Insert data into each table
cur.execute("""
    INSERT INTO author (author_name)
    SELECT DISTINCT author
    FROM immediate_table
""")

cur.execute("""
    INSERT INTO parent_category (parent_category_name)
    SELECT DISTINCT parent_category
    FROM immediate_table
""")

cur.execute("""
    INSERT INTO publisher (publisher_name)
    SELECT DISTINCT publisher
    FROM immediate_table
""")

cur.execute("""
    INSERT INTO category (category_name, parent_category_id)
    SELECT DISTINCT l.category, p.parent_category_id
    FROM immediate_table l
    JOIN parent_category p ON l.parent_category = p.parent_category_name
""")

cur.execute("""
    INSERT INTO book (book_asin, title, isbn_13, price, rating, review_count, publication_date, author_id,
				publisher_id, category_id, parent_category_id)
    SELECT DISTINCT l.book_asin, l.title, l.isbn_13, l.price, l.rating, l.review_count, l.publication_date, a.author_id, pu.publisher_id,
				c.category_id
    FROM immediate_table l
        JOIN author a ON l.author = a.author_name
        JOIN category c ON l.category = c.category_name
        JOIN publisher pu ON l.publisher = pu.publisher_name
""")

cur.execute(""" DROP TABLE immediate_table """)

conn.commit()