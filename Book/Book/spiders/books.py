import scrapy
import pandas as pd
from urllib.parse import urljoin
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy.loader import ItemLoader
from ..items import BookItem
import random

user_agent_list = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 14_4_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Mobile/15E148 Safari/604.1',
    'Mozilla/4.0 (compatible; MSIE 9.0; Windows NT 6.1)',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36 Edg/87.0.664.75',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36 Edge/18.18363',
]

class BooksSpider(scrapy.Spider):
    name = "books"
    allowed_domains = ["amazon.com"]
    #start_urls = ["http://amazon.com/"]

    rules = (
        Rule(LinkExtractor(allow='dp'), callback='parse_item')
    )

    def start_requests(self):
        df = pd.read_csv('./Book/spiders/df1_asin.csv')
        asin = df['asin'].tolist()[8]
        category = 'Algorithms'
        # for asin in asins:
        product_url = f"https://www.amazon.com/dp/{asin}"
        yield scrapy.Request(product_url, callback=self.parse_item, meta={'asin': asin, 'category': category},
                             headers={"User-Agent": user_agent_list[random.randint(0, len(user_agent_list)-1)]})

    def start_requests_jshdjas(self):
        keyword_list = ['Algorithms', 'Artificial Intelligence', 'Database Storage & Design', 'Graphics & Visualization',
                        'Networking', 'Object-Oriented Software Design', 'Operating Systems', 'Programming Languages',
                        'Software Design & Engineering']
        for keyword in keyword_list:
            amazon_search_url = f'https://www.amazon.com/s?k={keyword}&page=1'
            yield scrapy.Request(url=amazon_search_url, callback=self.discover_product_urls,
                                 meta={'category': keyword, 'page': 1},
                                 headers={"User-Agent": user_agent_list[random.randint(0, len(user_agent_list)-1)]})


    def discover_product_urls(self, response):
        page = response.meta['page']
        keyword = response.meta['category']

        ## Discover asins URLs
        asins = response.xpath("//div['data-asin']/@data-asin").extract()
        for asin in asins:
            if asin != '':
                product_url = urljoin('https://www.amazon.com/dp/', asin)
                yield scrapy.Request(url=product_url, callback=self.parse_item,
                                    meta={'category': keyword,'page': page, 'asin': asin},
                                    headers={"User-Agent": user_agent_list[random.randint(0, len(user_agent_list)-1)]})

        ## Get next page
        next_page = response.xpath('//a[has-class("s-pagination-next")]/@href').extract_first()
        if next_page:
            page_num = next_page.split('&')[1].split('=')[1]
            yield response.follow(url=next_page, callback=self.discover_product_urls, meta={'keyword': keyword, 'page': page_num})

    def parse_item(self, response):
        loader = ItemLoader(item=BookItem(), response=response)
        loader.add_value('category', response.meta['category'])
        loader.add_value('asin', response.meta['asin'])
        loader.add_css('book_name', "span#productTitle ::text")
        loader.add_css('author', "a.contributorNameID::text")
        loader.add_css('price', "span.a-price span.a-offscreen ::text")
        loader.add_css('publication_date', "div#rpi-attribute-book_details-publication_date div.rpi-attribute-value span ::text")
        loader.add_css('publisher', "div#rpi-attribute-book_details-publisher div.rpi-attribute-value span::text")
        loader.add_xpath('customer_review', "//span['acrPopover']/@title")
        loader.add_xpath('ratings', "//span[@id='acrCustomerReviewText']/text()")
        loader.add_css('isbn_13', "div#rpi-attribute-book_details-isbn13 div.rpi-attribute-value span ::text")
        loader.add_css('availability', "div#rentBoxMerchantDetails span ::text")

        yield loader.load_item()
