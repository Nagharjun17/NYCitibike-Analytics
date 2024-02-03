# NYCitibike-Analytics

This project fetches real-time New York Citibike data and follows an ETL pipeline using Amazon EC2 and Apache Airflow. A DAG is created in airflow where the data is fetched through a Python script using the requests library. Data transformations are done using Pandas and finally, the data is stored in Amazon S3. To create visualizations, the data is transferred from Amazon S3 to Snowflake using event triggers in S3 and Snowpipe in Snowflake. Finally, Tableau is used to create map visualizations of New York City and the Citibike availability.

## Data Sources
Link1: https://gbfs.lyft.com/gbfs/2.3/bkn/en/station_status.json

Link2: https://api.citybik.es/citi-bike-nyc.json

## Airflow DAG

<img width="660" alt="image" src="https://github.com/Nagharjun17/NYCitibike-Analytics/assets/64778259/c0d5f882-165b-4a1e-8f84-eb186600da79">

## Tableau Visualization

<img width="1317" alt="image" src="https://github.com/Nagharjun17/NYCitibike-Analytics/assets/64778259/8cbf55d6-9018-4678-b721-70b5ee86908e">

## Replication

- Create an Amazon EC2 instance with t2.medium instance type and install pip and python 3.10 on it.
- Create a Python virtual environment and activate it.
- Pip install the requirements.txt file.
- Activate Amazon CLI using the access and secret keys.
- SSH the EC2 instance to an IDE.
- Create a folder called dags in the root directory and paste the nycitibike_analytics.py file in it.
- At the same time, run the snowflake SQL script to create the database and snowpipe.
- Create an Amazon S3 bucket with events triggering snowpipe.
- Subsequently, connect snowflake to Tableau or Power BI to create visualizations
