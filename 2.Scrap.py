import os 
import pandas as pd 
import logging
import boto3
import scrapy
from scrapy.crawler import CrawlerProcess

class QuotesSpider(scrapy.Spider):

    # Name of your spider
    name = "booking_test"

    # Url to start your spider from 
    start_urls = [
       # "https://www.booking.com/searchresults.fr.html?aid=7927915&sid=21e54365e48f242f53efe3c8336a9845&sb=1&sb_lp=1&src=index&src_elem=sb&error_url=https%3A%2F%2Fwww.booking.com%2Findex.fr.html%3Faid%3D7927915%26sid%3D21e54365e48f242f53efe3c8336a9845%26sb_price_type%3Dtotal%3Bsrpvid%3Db8786af712e005ee%26%26&ss=nimes&is_ski_area=0&checkin_year=&checkin_month=&checkout_year=&checkout_month=&group_adults=2&group_children=0&no_rooms=1&b_h4u_keep_filters=&from_sf=1&dest_id=&dest_type=&search_pageview_id=a7bb7034928a0006&search_selected=false",
        # "https://www.booking.com/hotel/fr/antichambre.fr.html?aid=7927915&sid=21e54365e48f242f53efe3c8336a9845&dist=0;group_adults=2;group_children=0;hapos=1;hpos=1;map=1;no_rooms=1;req_adults=2;req_children=0;room1=A%2CA;sb_price_type=total;sr_order=bayesian_review_score;srepoch=1666967121;srpvid=4ebd656818d1017d;type=total;ucfs=1&#map_opened-hotel_header"
      "https://www.booking.com/searchresults.fr.html?ss=nimes&ssne=Paris&ssne_untouched=Paris&efdco=1&label=gen173nr-1FCAEoggI46AdIM1gEaEaIAQGYAQ24AQfIAQzYAQHoAQH4AQuIAgGoAgO4AperqZ8GwAIB0gIkNTVlM2Q0MzgtM2E5Ny00OTI3LTliZTUtOWI2ZGU2Y2UzZDAw2AIG4AIB&sid=1f5200ba9085196426266e923706b5c3&aid=304142&lang=fr&sb=1&src_elem=sb&src=searchresults&group_adults=2&no_rooms=1&group_children=0&sb_travel_purpose=leisure",
    ]

    # Callback function that will be called when starting your spider
    # It will get text, author and tags of all the <div> with class="quote"
    def parse_city(self, response):
        i = 0
        for path in response.xpath('//*[@data-testid="property-card"]'):
            #url = path.xpath('div[1]/div[2]/div/div/div[1]/div/div[1]/div/h3/a').attrib['href']
            #url = path.xpath('div[1]/div[2]/div/div/div[1]/div/div[1]/div/h3/a/@href').extract()
            url = path.xpath('div[1]/div[1]/div/a').attrib['href']
            data = {
                #'hotel name': path.xpath('div[1]/div[2]/div/div/div[1]/div/div[1]/div/h3/a/div[1]/text()').get(),
                'hotel name': path.xpath('div[1]/div[2]/div/div/div/div[1]/div/div[1]/div/h3/a/div[1]/text()').get(),
                'Url to its booking.com page': url,
                #'Score': path.xpath('div[1]/div[2]/div/div/div[2]/div[1]/a/span/div/div[1]/text()').get(),
                'Score': path.xpath('div[1]/div[2]/div/div/div/div[2]/div/div[1]/div/a/span/div/div[1]/text()').get(),
                #'Text description': path.xpath('div[1]/div[2]/div/div/div[1]/div/div[last()]/text()').get(),
                'Text description': path.xpath('div[1]/div[2]/div/div/div/div[1]/div/div[4]/text()').get(),
            }
            #yield data
            yield response.follow(url, callback=self.parse_products, meta=data)
            i += 1
            if i == 20:
                break

 
    def parse_products(self, response):
        return {
            **response.meta,
            'latlng': response.xpath('//*[@id="hotel_sidebar_static_map"]').attrib['data-atlas-latlng'],
        }

    def parse(self, response):
        # next_city = response.xpath('//*[@name="ss"]')
        # top_5_cuty_name.read
        city_name = pd.read_csv('top_5_city_name.csv', names= ['city'])
        cities = [x.strip() for x in city_name['city']]
        #cities = ['nimes']
        for city in cities:
            yield scrapy.FormRequest.from_response(response, formdata={'ss': city}, callback=self.parse_city)


# Name of the file where the results will be saved
filename = "booking_test.json"

# If file already exists, delete it before crawling (because Scrapy will 
# concatenate the last and new results otherwise)
if filename in os.listdir('data/'):
        os.remove('data/' + filename)


# Declare a new CrawlerProcess with some settings
## USER_AGENT => Simulates a browser on an OS
## LOG_LEVEL => Minimal Level of Log 
## FEEDS => Where the file will be stored 
## More info on built-in settings => https://docs.scrapy.org/en/latest/topics/settings.html?highlight=settings#settings
process = CrawlerProcess(settings = {
    'USER_AGENT': 'Chrome/97.0',
    'LOG_LEVEL': logging.INFO,
    "FEEDS": {
        'data/' + filename : {"format": "json"},
    }
})

# Start the crawling using the spider you defined above
process.crawl(QuotesSpider)
process.start()


#Send the data to my S3 

session = boto3.Session(aws_access_key_id=os.getenv("AWS_ACCESS_LEY_ID"), 
                        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"))
s3 = session.resource("s3")

bucket = s3.create_bucket(Bucket="booking-scapping")

df = pd.read_json('data/booking_test.json')
print(df.head())
csv = df.to_csv()

put_object = bucket.put_object(Key="hotels_info.csv", Body=csv)