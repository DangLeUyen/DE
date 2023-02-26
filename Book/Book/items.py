# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from itemloaders.processors import TakeFirst, MapCompose
from w3lib.html import remove_tags
from pandas import to_datetime
import string

def remove_punc(value):
    # python replace method to replace '$,.' with a blank
    punctuation = ['$', ',']
    for c in punctuation:
        value = value.replace(c, '')
    return float(value.strip())

def convert_to_date(value):
    return to_datetime(value).strftime("%Y-%m-%d")

def get_first_element(value):
    return value.split(" ")[0]

def remove_strip(value):
    return value.strip()

def remove_dot(value):
    return value.replace('.', '').strip()

class BookItem(scrapy.Item):
    # define the fields for your item here like:
    category = scrapy.Field(
        input_processor=MapCompose(remove_tags),
        output_processor=TakeFirst()
    )

    asin = scrapy.Field(
        input_processor=MapCompose(remove_tags),
        output_processor=TakeFirst()
    )

    book_name = scrapy.Field(
        input_processor=MapCompose(remove_tags, remove_strip),
        output_processor=TakeFirst()
    )

    author = scrapy.Field(
        input_processor=MapCompose(remove_tags, remove_strip),
        output_processor=TakeFirst()
    )

    price = scrapy.Field(
        input_processor=MapCompose(remove_tags, remove_punc),
        output_processor=TakeFirst()
    )

    publication_date = scrapy.Field(
        input_processor=MapCompose(remove_tags, convert_to_date),
        output_processor=TakeFirst()
    )

    publisher = scrapy.Field(
        input_processor=MapCompose(remove_tags,remove_strip),
        output_processor=TakeFirst()
    )

    customer_review = scrapy.Field(
        input_processor=MapCompose(remove_tags, get_first_element),
        output_processor=TakeFirst()
    )

    ratings = scrapy.Field(
        input_processor=MapCompose(remove_tags, get_first_element, remove_punc),
        output_processor=TakeFirst()
    )

    isbn_13 = scrapy.Field(
        input_processor=MapCompose(remove_tags),
        output_processor=TakeFirst()
    )

    availability = scrapy.Field(
        input_processor=MapCompose(remove_tags, remove_dot),
        output_processor=TakeFirst()
    )
