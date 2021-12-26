import configparser
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, regexp_replace, countDistinct, \
                                  year, month, col, mean, desc, udf, first
import os


def create_spark_session():
    """
    - build a spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.repositories",
                "https://repos.spark-packages.org/") \
        .config("spark.jars.packages",
                "saurfang:spark-sas7bdat:2.0.0-s_2.11") \
        .enableHiveSupport() \
        .getOrCreate()
    return spark


def process_temperature_data(spark, input_data, output_data):
    """
    - Process Temperature Data
    - Load and clean Temperature data
    - Filter "United States" temperature data and aggregate into
      dim_temperatures_us_cities_table
    - Write dim_temperatures_us_cities_table to parquet files partitioned
      by year and month
    - Filter out "United States" temperature data and aggregate into
      dim_temperatures_us_cities_table
    - Write dim_temperatures_countries_table to parquet files partitioned
      by year and month
    """

    # read temperatures data file
    df_temperatures = spark.read.format('csv') \
                                .option("header", True) \
                                .load(input_data)

    # drop rows for na value in columns
    df_temperatures = df_temperatures.na.drop(subset=["dt",
                                                      "AverageTemperature",
                                                      "City",
                                                      "Country"])

    # generate year and month columns from dt column
    df_temperatures = df_temperatures.withColumn("year", year(col("dt"))) \
                                     .withColumn("month", month(col("dt")))

    # process to generate temperatures_us_cities
    # filter 'United States' from Country Column
    df_temperatures_us_cities = df_temperatures.filter(
                                                "Country == 'United States'")

    # aggregate temperature dataframe by year, month and city
    df_temperatures_us_cities = df_temperatures_us_cities \
        .groupby("year", "month", "City") \
        .agg(mean(df_temperatures_us_cities.AverageTemperature
                  .cast('decimal')).alias('average_temperature')) \
        .orderBy(desc('year'), desc('month'), desc('City'))

    # generate dim_temperatures_us_cities_table from
    # df_temperatures_us_cities dataframe
    dim_temperatures_us_cities_table = df_temperatures_us_cities \
        .filter(df_temperatures_us_cities.year.isNotNull() &
                df_temperatures_us_cities.month.isNotNull() &
                df_temperatures_us_cities.City.isNotNull()) \
        .selectExpr([
            "cast(year as int)",
            "cast(month as int)",
            "cast(city as string)",
            "cast(average_temperature as decimal(4,2))"
            ]).dropDuplicates(["year", 'month', 'city'])

    # write dim_temperatures_us_cities_table to parquet files
    # partitioned by year and month
    dim_temperatures_us_cities_table.write.mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet(output_data+"dim_temperatures_us_cities_table.parquet")

    # process to generate temperatures_countries
    # filter out 'United States' from Country column
    df_temperatures_countries = df_temperatures \
        .filter("Country != 'United States'")

    # aggregate temperature dataframe by year, month and country
    df_temperatures_countries = df_temperatures_countries \
        .groupby('year', 'month', 'Country') \
        .agg(mean(df_temperatures_countries.AverageTemperature.cast('decimal'))
             .alias('average_temperature')) \
        .orderBy(desc('year'), desc('month'), desc('Country'))

    # generate dim_temperatures_countries_table from
    # df_temperatures_countries dataframe
    dim_temperatures_countries_table = df_temperatures_countries \
        .filter(df_temperatures_countries.year.isNotNull() &
                df_temperatures_countries.month.isNotNull() &
                df_temperatures_countries.Country.isNotNull()) \
        .selectExpr([
            "cast(year as int)",
            "cast(month as int)",
            "cast(country as string)",
            "cast(average_temperature as decimal(4,2))"
            ]).dropDuplicates(["year", 'month', 'country'])

    # write dim_temperatures_countries_table to parquet files
    # partitioned by year and month
    dim_temperatures_countries_table.write.mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet(output_data+"dim_temperatures_countries_table.parquet")


def process_cities_demographic_data(spark, input_data, output_data):
    """
    - Process Cities Demographic Data
    - Load and clean Cities Demographic data
    - Aggregate Cities Demographic dataframe by Race with Count
    - Write dim_cities_demographic_table to parquet files
    """

    # read cities demographic data file
    df_cities_demographic = spark.read.format('csv') \
        .options(header='True', delimiter=';').load(input_data)

    # drop rows for NA value in Columns
    df_cities_demographic = df_cities_demographic.na.drop()

    # aggregate cities demographic dataframe by Race with Count
    df_cities_demographic = df_cities_demographic \
        .groupby('State', 'City', 'Median Age', 'Male Population',
                 'Female Population', 'Total Population', 'Number of Veterans',
                 'Foreign-born', 'Average Household Size', 'State Code') \
        .pivot('Race').agg(first('Count')).orderBy(desc('State'), desc('City'))

    # generate dim_cities_demographic_table from cities_demographic dataframe
    dim_cities_demographic_table = df_cities_demographic \
        .filter(df_cities_demographic.State.isNotNull() &
                df_cities_demographic.City.isNotNull()) \
        .selectExpr([
            "cast(state as string)",
            "cast(`State Code` as string) state_code",
            "cast(city as string)",
            "cast(`Median Age` as decimal(4,1)) median_age",
            "cast(`Male Population` as int) male_population",
            "cast(`Female Population` as int) female_population",
            "cast(`Total Population` as int) total_population",
            "cast(`Number of Veterans` as int) number_of_veterans",
            "cast(`Foreign-born` as int) number_of_foregn_born",
            "cast(`Average Household Size` as decimal(4,2)) \
                avarage_household_size",
            "cast(`American Indian and Alaska Native` as int) \
                american_indian_alaska_native",
            "cast(`Asian` as int) asian",
            "cast(`Black or African-American` as int) \
                black_or_africanamerican",
            "cast(`Hispanic or Latino` as int) hispanic_or_latino",
            "cast(`White` as int) white"
            ]).dropDuplicates(["state", 'city'])

    # write dim_cities_demographic_table to parquet files
    dim_cities_demographic_table.write.mode("overwrite") \
        .parquet(output_data+"dim_cities_demographic_table.parquet")


def process_i94_sas_labels_data(spark, input_data, output_data):
    """
    - Process I94 SAS Labels Data
    - Load, clean and parse i94_sas_labels data
    - Generate dim_i94_country_table and write to parquet files
    - Generate dim_i94_port_table and write to parquet files
    - Generate dim_i94_travel_mode_table and write to parquet files
    """

    # read i94 sas labels descriptions file
    text_i94_sas_labels_descriptions = open(input_data, 'r').read()

    # i94 SAS LABELS DESCRIPTIONS exploration and cleaning
    dictionary_i94_sas_labels_descriptions = {}
    list_i94_sas_labels_descriptions = text_i94_sas_labels_descriptions \
        .replace('\t', '').replace('    ', '').replace('   ', '') \
        .replace('  ', '').replace(' \'', '\'').replace('\'', '') \
        .replace('$', '').replace('\n', '|').split('value ')

    for line in list_i94_sas_labels_descriptions:
        line = line.split(';')[0].split("|")
        dictionary_i94_sas_labels_descriptions[
            line[0]] = [x.split("=") for x in line[0:] if "=" in x]

    # create I94 COUNTRY dataframe with spark
    df_i94_country = spark \
        .createDataFrame(dictionary_i94_sas_labels_descriptions['i94cntyl'],
                         ('country_code', 'country'))

    # generate dim_i94_country_table from df_i94_country dataframe
    dim_i94_country_table = df_i94_country \
        .withColumn('country_code', regexp_replace('country_code', ' ', '')) \
        .filter(df_i94_country.country_code.isNotNull()) \
        .selectExpr([
            "cast(country_code as int)",
            "cast(country as string)"
        ]).dropDuplicates(["country_code"])

    # write dim_i94_country_table to parquet files
    dim_i94_country_table.write.mode("overwrite") \
        .parquet(output_data+"dim_i94_country_table.parquet")

    # create I94 STATE dataframe
    df_i94_state = spark \
        .createDataFrame(dictionary_i94_sas_labels_descriptions['i94addrl'],
                         ('state_code', 'state'))

    # create I94 PORT dataframe
    df_i94_port = spark \
        .createDataFrame(dictionary_i94_sas_labels_descriptions['i94prtl'],
                         ('port_code', 'port'))
    df_i94_port = df_i94_port \
        .withColumn('city', split(df_i94_port['port'], ',')[0]) \
        .withColumn('state_code',
                    regexp_replace(
                        split(df_i94_port['port'], ',')[1], ' ', '')[0:2])

    # generate dim_i94_port_table from df_i94_port dataframe and df_i94_state
    dim_i94_port_table = df_i94_port \
        .join(df_i94_state, 'state_code') \
        .filter(df_i94_port.port_code.isNotNull()) \
        .selectExpr([
            "cast(port_code as string)",
            "cast(city as string)",
            "cast(state_code as string) state_code",
            "cast(state as string)"
        ]).dropDuplicates(["port_code"])

    # write dim_i94_port_table to parquet files
    dim_i94_port_table.write.mode("overwrite") \
        .parquet(output_data + "dim_i94_port_table.parquet")

    # create I94 MODE dataframe
    df_i94_mode = spark \
        .createDataFrame(dictionary_i94_sas_labels_descriptions['i94model'],
                         ('travel_mode_code', 'travel_mode'))

    # generate dim_travel_mode_table from df_i94_mode dataframe
    dim_i94_travel_mode_table = df_i94_mode \
        .withColumn('travel_mode_code',
                    regexp_replace('travel_mode_code', ' ', '')) \
        .selectExpr([
            "cast(travel_mode_code as int)",
            "cast(travel_mode as string)"
        ]).dropDuplicates(["travel_mode_code"])

    # write dim_i94_travel_mode_table to parquet files
    dim_i94_travel_mode_table.write.mode("overwrite") \
        .parquet(output_data + "dim_i94_travel_mode_table.parquet")


def process_immigration_data(spark, input_data, output_data):
    """
    - Process Immigration I94 Data
    - Load and clean Immigration I94 data
    - Filter visitor data that visited for pleasure
    - Convert sasdate to datetime
    - Generate dim_date_table and write to parquet files partitioned
      by year and month
    - Generate fact_i94_us_visitor_arrivals_table and write to parquet files
      partitioned by arrival_year and arrival_month
    """

    # read immigration file
    df_immigrations = spark.read.format('com.github.saurfang.sas.spark') \
        .load(input_data)

    # filter i94visa by 2.0 for pleasure visiting
    # due to the fact that our target profile is tourist
    df_immigrations = df_immigrations.filter("I94VISA==2.0")

    # drop rows for na value in columns
    df_immigrations = df_immigrations.na \
        .drop(how='any',
              subset=["cicid", "i94res", "i94port", "arrdate", "i94mode"])

    # convert sasdate to datetime
    SASdate_to_datetime = udf(lambda z: (timedelta(days=z) +
                              datetime(1960, 1, 1)).strftime('%Y-%m-%d'))
    df_immigrations = df_immigrations \
        .withColumn("arrival_date", SASdate_to_datetime(col("arrdate")))

    # Generate dim_date_table from df_immigrations dataframe
    dim_date_table = df_immigrations \
        .filter(df_immigrations.arrival_date.isNotNull()) \
        .selectExpr([
            "cast(arrival_date as date) arrival_date",
            "cast(dayofmonth(arrival_date) as int) day",
            "cast(weekofyear(arrival_date) as int) week",
            "cast(month(arrival_date) as int) month",
            "cast(year(arrival_date) as int) year",
            "cast(quarter(arrival_date) as int) quarter"
        ]).dropDuplicates(["arrival_date"])

    # write dim_date_table to parquet files partitioned by year and month
    dim_date_table.write.mode("overwrite").partitionBy("year", "month") \
        .parquet(output_data+"dim_date_table.parquet")

    # Generate fact_i94_us_visitor_arrivals_table from df_immigrations
    fact_i94_us_visitor_arrivals_table = df_immigrations \
        .filter(df_immigrations.cicid.isNotNull() &
                df_immigrations.i94res.isNotNull() &
                df_immigrations.i94port.isNotNull() &
                df_immigrations.arrival_date.isNotNull() &
                df_immigrations.i94mode.isNotNull()) \
        .selectExpr([
            "cast(cicid as long)",
            "cast(i94yr as int) arrival_year",
            "cast(i94mon as int) arrival_month",
            "cast(arrival_date as date) arrival_date",
            "cast(i94port as string) arrival_port_code",
            "cast(i94cit as int) visitor_birth_country",
            "cast(i94res as int) visitor_residence_country",
            "cast(i94bir as int) visitor_age",
            "cast(gender as string) visitor_gender",
            "cast(i94mode as int) travel_mode_code",
            "cast(airline as string) travel_airline",
            "cast(visatype as string) travel_visa_type"
        ]).dropDuplicates(["cicid"])

    # write fact_i94_us_visitor_arrivals_table to parquet files
    # partitioned by arrival_year and arrival_month
    fact_i94_us_visitor_arrivals_table.write.mode("overwrite") \
        .partitionBy("arrival_year", "arrival_month") \
        .parquet(output_data+"fact_i94_us_visitor_arrivals_table.parquet")


def main():
    """
    # This section should be used for running data processing on AWS S3
    # AWS S3 access credential reading and setting
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    os.environ['AWS_ACCESS_KEY_ID'] = config \
        .get('default', 'AWS_ACCESS_KEY_ID')
    os.environ['AWS_SECRET_ACCESS_KEY'] = config \
        .get('default','AWS_SECRET_ACCESS_KEY')

    aws_s3_url = 's3a://udacity_capstone_us_tourism_data_lake/data'
    immigration_file = aws_s3_url + \
                       '/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat'
    temperature_file = aws_s3_url + '/GlobalLandTemperaturesByCity.csv'
    cities_demographic_file = aws_s3_url + '/us-cities-demographics.csv'
    i94_sas_labels_descriptions_file = aws_s3_url + \
                                       '/I94_SAS_Labels_Descriptions.SAS'
    output_data = aws_s3_url + '/tables/'
    """

    # Source Files and output folder
    immigration_file = \
        '../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat'
    temperature_file = '../../data2/GlobalLandTemperaturesByCity.csv'
    cities_demographic_file = './us-cities-demographics.csv'
    i94_sas_labels_descriptions_file = './I94_SAS_Labels_Descriptions.SAS'
    output_data = './tables/'

    # Build Spark Session and run data process functions
    spark = create_spark_session()
    process_immigration_data(spark, immigration_file, output_data)
    process_temperature_data(spark, temperature_file, output_data)
    process_cities_demographic_data(spark, cities_demographic_file,
                                    output_data)
    process_i94_sas_labels_data(spark, i94_sas_labels_descriptions_file,
                                output_data)


if __name__ == "__main__":
    main()
