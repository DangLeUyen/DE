# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from itemloaders.processors import TakeFirst, MapCompose
from w3lib.html import remove_tags
from pandas import to_datetime
import string

def remove_dollar_sign(value):
    # python replace method to replace '$' with a blank
    return float(value.replace('$', '').strip())

def remove_hashtag_sign(value):
    # python replace method to replace '$' with a blank
    return int(value.replace('#', '').strip())

def remove_comma_ratingnum(value):
    # python replace method to replace ',' with a blank
    return int(value.replace(',', '').strip())

def convert_to_float(value):
    # python replace method to replace ',' with a blank
    return float(value)

def convert_to_date(value):
    return to_datetime(value).strftime("%Y-%m-%d")

def get_first_element(value):
    return value.split(" ")[0]

def remove_strip(value):
    return value.strip()

class BookItem(scrapy.Item):
    # define the fields for your item here like:
    parent_category = scrapy.Field(
        input_processor=MapCompose(remove_tags),
        output_processor=TakeFirst()
    )

    category = scrapy.Field(
        input_processor=MapCompose(remove_tags),
        output_processor=TakeFirst()
    )

    book_asin = scrapy.Field(
        input_processor=MapCompose(remove_tags),
        output_processor=TakeFirst()
    )

    title = scrapy.Field(
        input_processor=MapCompose(remove_tags, remove_strip),
        output_processor=TakeFirst()
    )

    author = scrapy.Field(
        input_processor=MapCompose(remove_tags, remove_strip),
        output_processor=TakeFirst()
    )

    price = scrapy.Field(
        input_processor=MapCompose(remove_tags, remove_dollar_sign),
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

    rating = scrapy.Field(
        input_processor=MapCompose(remove_tags, get_first_element, convert_to_float),
        output_processor=TakeFirst()
    )

    review_count = scrapy.Field(
        input_processor=MapCompose(remove_tags, get_first_element, remove_comma_ratingnum),
        output_processor=TakeFirst()
    )

    isbn_13 = scrapy.Field(
        input_processor=MapCompose(remove_tags),
        output_processor=TakeFirst()
    )