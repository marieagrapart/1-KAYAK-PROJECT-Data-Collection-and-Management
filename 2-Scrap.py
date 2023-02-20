import os
import pandas as pd
import logging
import boto3
import scrapy
from scrapy.crawler import CrawlerProcess


class QuotesSpider(scrapy.Spider):
    # Name of your spider
    name = "booking_scrap"

    # Url to start your spider from
    start_urls = [
        "https://www.booking.com/searchresults.fr.html?ss=nimes&ssne=Paris&ssne_untouched=Paris&efdco=1&label=gen173nr-1FCAEoggI46AdIM1gEaEaIAQGYAQ24AQfIAQzYAQHoAQH4AQuIAgGoAgO4AperqZ8GwAIB0gIkNTVlM2Q0MzgtM2E5Ny00OTI3LTliZTUtOWI2ZGU2Y2UzZDAw2AIG4AIB&sid=1f5200ba9085196426266e923706b5c3&aid=304142&lang=fr&sb=1&src_elem=sb&src=searchresults&group_adults=2&no_rooms=1&group_children=0&sb_travel_purpose=leisure",
    ]

    # we create a loop over our top 5 cities
    def parse(self, response):
        top_5_city = pd.read_csv("data/top_5_city_name.csv")
        cities = top_5_city["city"].values.tolist()
        for city in cities:
            yield scrapy.FormRequest.from_response(
                response, formdata={"ss": city}, callback=self.parse_city
            )

    # we take the information og the top 20 hotels per cities
    def parse_city(self, response):
        i = 0
        for path in response.xpath('//*[@data-testid="property-card"]'):
            url = path.xpath("div[1]/div[1]/div/a").attrib["href"]
            data = {
                "hotel name": path.xpath(
                    "div[1]/div[2]/div/div/div/div[1]/div/div[1]/div/h3/a/div[1]/text()"
                ).get(),
                "Url to its booking.com page": url,
                "Score": path.xpath(
                    "div[1]/div[2]/div/div/div/div[2]/div/div[1]/div/a/span/div/div[1]/text()"
                ).get(),
                "Text description": path.xpath(
                    "div/div[2]/div/div/div/div/div/div[3]/text()"
                ).get()
                or path.xpath("div/div[2]/div/div/div/div/div/div[4]/text()").get(),
            }

            yield response.follow(url, callback=self.parse_products, meta=data)
            i += 1
            if i == 20:
                break

    # we go into the url of each hotels to take their localisation
    def parse_products(self, response):
        return {
            **response.meta,
            "latlng": response.xpath('//*[@id="hotel_sidebar_static_map"]').attrib[
                "data-atlas-latlng"
            ],
        }


# Name of the file where the results will be saved
filename = "booking.json"

# If file already exists, delete it before crawling (because Scrapy will
# concatenate the last and new results otherwise)
if filename in os.listdir("data/"):
    os.remove("data/" + filename)


# Declare a new CrawlerProcess with some settings
## USER_AGENT => Simulates a browser on an OS
## LOG_LEVEL => Minimal Level of Log
## FEEDS => Where the file will be stored
process = CrawlerProcess(
    settings={
        "USER_AGENT": "Chrome/97.0",
        "LOG_LEVEL": logging.INFO,
        "FEEDS": {
            "data/" + filename: {"format": "json"},
        },
    }
)

# Start the crawling using the spider you defined above
process.crawl(QuotesSpider)
process.start()

# clean the data before send it to S3

hotel = pd.read_json("data/booking.json")

hotel[["lat", "lon"]] = hotel["latlng"].str.split(",", 1, expand=True)
hotel = hotel.drop(
    ["depth", "download_timeout", "download_slot", "latlng", "download_latency"], axis=1
)

hotel["Score"] = hotel["Score"].replace({",": "."}, regex=True)

hotel["Score"] = hotel["Score"].astype(float)
hotel["lat"] = hotel["lat"].astype(float)
hotel["lon"] = hotel["lon"].astype(float)


# Send the data to my S3

session = boto3.Session(
    aws_access_key_id=os.getenv("AWS_ACCESS_LEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)
s3 = session.resource("s3")

bucket = s3.create_bucket(Bucket="booking-scapping")

csv = hotel.to_csv()

bucket.put_object(Key="hotels_info.csv", Body=csv)

print("We got the top hotels and their infos in our data folder !")
