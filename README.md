# 1-KAYAK-PROJECT-Data-Collection-and-Management

Video:
Certification:

It'a a student project with the following context and gloas : 

<sup> *Kayak Marketing Team would like to create an application that will recommend where people should plan their next holidays. The application should be based on real data about:*
<sup> - Weather
<sup> - Hotels in the area
<sup>*The application should then be able to recommend the best destinations and hotels based on the above variables at any given time*

#### Goals 
<sup>*As the project has just started, your team doesn't have any data that can be used to create this application. Therefore, your job will be to:*

<sup>- Scrape data from destinations
<sup>- Get weather data from each destination
<sup>- Get hotels' info about each destination
<sup>- Store all the information above in a data lake
<sup>- Extract, transform and load cleaned data from your datalake to a data warehouse 
</sup>

## Prerequisites

You will need : 
- all the libraries in requirement.txt 
- Credendials for AWS S3 and RDS 

## Installing

Once you have the prerequisites, 
You can run files in the order, in other words : 

First : 
"1.API_weather.ipynb"
Then :
"2.Scrap.py"
Finally: 
"3.visualization.ipynb"

You will have your raw data from API & scrapping in S3 and cleaned sql table in RDS. 
At the end for the third file, you will see two maps with the top 5 cities : one about the weather and one about best hotels

*The top cities was chosen by the best temperature and the less precipitation* 

## Authors

**Marie Agrapart** 

