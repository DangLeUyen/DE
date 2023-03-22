import scrapy
from urllib.parse import urljoin
from scrapy.loader import ItemLoader
from ..items import BookItem
from scrapy_selenium import SeleniumRequest
from selenium.common.exceptions import NoSuchElementException
from fake_useragent import UserAgent

ua = UserAgent()
fake_user_agent = ua.random

class BooksSpider(scrapy.Spider):
    name = "books"
    allowed_domains = ["amazon.com"]

    custom_settings = {
        "USER_AGENT": fake_user_agent,
        'DOWNLOAD_DELAY': 10,  # 10 seconds of delay
        'RANDOMIZE_DOWNLOAD_DELAY': False,
    }

    def start_requests(self):
        link = 'https://www.amazon.com/s?i=stripbooks&bbn=5&rh=n%3A283155%2Cn%3A5&dc&fs=true'

        yield SeleniumRequest(url=link, callback=self.get_link,
                              headers={'User-Agent': self.settings['USER_AGENT']})

    def get_link(self, response):
        li_selector = ".s-navigation-indent-2"
        name_selector = " span a span::text"
        link_selector = "a::attr('href')"

        for li in response.css(li_selector):
            parent_category = li.css(name_selector).extract_first()
            link = li.css(link_selector).extract_first()
            yield SeleniumRequest(url='https://www.amazon.com' + link, callback=self.get_link_sub,
                                  meta={'parent_category':parent_category},
                                  headers={'User-Agent': self.settings['USER_AGENT']})

    def get_link_sub(self, response):
        parent_category = response.meta['parent_category']
        li_selector = ".s-navigation-indent-2"
        name_selector = " span a span::text"
        link_selector = "a::attr('href')"

        for li in response.css(li_selector):
            category = li.css(name_selector).extract_first()
            link = li.css(link_selector).extract_first()

            yield SeleniumRequest(url='https://www.amazon.com' + link, callback=self.discover_product_urls,
                                  meta={'parent_category':parent_category, 'category': category},
                                  headers={'User-Agent': self.settings['USER_AGENT']})

    def discover_product_urls(self, response):
        parent_category = response.meta['parent_category']
        category = response.meta['category']
        ## Discover asins URLs
        asins = response.xpath('//div[contains(@class, "s-main-slot")]/div["data-asin"]/@data-asin').extract()

        for asin in asins:
            if asin != '':
                product_url = urljoin('https://www.amazon.com/dp/', asin)
                yield SeleniumRequest(url=product_url, callback=self.parse_product,
                                      meta={'parent_category':parent_category, 'category': category,'asin': asin})

        ## Get next page
        next_page = response.xpath('//a[has-class("s-pagination-next")]/@href').extract_first()
        if next_page:
            yield response.follow(url=next_page, callback=self.discover_product_urls,
                                  meta={'parent_category':parent_category, 'category': category})

    def get_name(self, response):
        try:
            name = response.xpath('//*[@id="productTitle"]/text()').extract()
        except NoSuchElementException:
            name = 'Na'
        return name

    def get_author(self, response):
        try:
            author = response.xpath('//*[@id="bylineInfo"]/span/a/text()')
        except NoSuchElementException:
            try:
                author = response.xpath('//*[@id="bylineInfo"]/span/span[1]/a[1]/text()')
            except NoSuchElementException:
                author = 'Na'
            else:
                author = author.extract_first()
        else:
            author = author.extract_first()

        return author

    def get_price(self, response):
        try:
            price = response.xpath('//*[@id="tp_price_block_total_price_ww"]/span/text()').extract_first()
        except NoSuchElementException:
            price = 0
        return price

    # get rating_value
    def get_rating(self, response):
        try:
            rating = (response.xpath(
                '//*[@id="reviewsMedley"]/div/div[1]/span[1]/span/div[2]/div/div[2]/div/span/span/text()')
                      .extract_first())
        except NoSuchElementException:
            rating = 0
        return rating

    # get number of ratings
    def get_rating_num(self, response):
        try:
            rat_num = response.xpath('//*[@id="acrCustomerReviewText"]/text()').extract_first()
        except NoSuchElementException:
            rat_num = 0
        return rat_num

    def get_isbn_13(self, response):
        try:
            isbn_13 = (response.xpath(
                '//*[@id="rpi-attribute-book_details-isbn13"]/div[contains(@class, "rpi-attribute-value")]/span/text()')
                       .extract_first())
        except NoSuchElementException:
            isbn_13 = 'Na'

        return isbn_13

    def get_publisher(self, response):
        try:
            publisher = (response.xpath(
                '//*[@id="rpi-attribute-book_details-publisher"]/div[contains(@class, "rpi-attribute-value")]/span/text()')
                         .extract_first())
        except NoSuchElementException:
            publisher = 'Na'
        return publisher

    def get_publication_date(self, response):
        try:
            publisher = (response.xpath(
                '//*[@id="rpi-attribute-book_details-publication_date"]/div[contains(@class, "rpi-attribute-value")]/span/text()')
                         .extract_first())
        except NoSuchElementException:
            publisher = 'Na'
        return publisher

    def parse_product(self, response):
        loader = ItemLoader(item=BookItem(), response=response)
        loader.add_value('parent_category', response.meta['parent_category'])
        loader.add_value('category', response.meta['category'])
        loader.add_value('book_asin', response.meta['asin'])
        loader.add_value('title', self.get_name(response))
        loader.add_value('author', self.get_author(response))
        loader.add_value('price', self.get_price(response))
        loader.add_value('rating', self.get_rating(response))
        loader.add_value('review_count', self.get_rating_num(response))
        loader.add_value('isbn_13', self.get_isbn_13(response))
        loader.add_value('publisher', self.get_publisher(response))
        loader.add_value('publication_date', self.get_publication_date(response))
        yield loader.load_item()
