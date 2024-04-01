import logging

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, MapType


def create_keyspace(session):
    session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """
    )

    print("Keyspace created successfully!")


def create_table(session):
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS spark_streams.movies (
            title TEXT,
            year TEXT,
            director TEXT,
            actors TEXT,
            plot TEXT,
            ratings MAP<TEXT, TEXT>,
            box_office TEXT,
            PRIMARY KEY (title, year)
        );
        """
    )

    print("Table created successfully!")


def insert_data(session, **kwargs):
    print("inserting data...")

    title = kwargs.get("Title")
    year = kwargs.get("Year")
    director = kwargs.get("Director")
    actors = kwargs.get("Actors")
    plot = kwargs.get("Plot")
    ratings = kwargs.get("Ratings")
    box_office = kwargs.get("BoxOffice")

    ratings_map = {rating["Source"]: rating["Value"] for rating in ratings}

    try:
        session.execute(
            """
            INSERT INTO movies(title, year, director, actors, plot, ratings, box_office)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                title,
                year,
                director,
                actors,
                plot,
                ratings_map,
                box_office,
            ),
        )
        logging.info(f"Data inserted for {title}")

    except Exception as e:
        logging.error(f"Could not insert data due to {e}")


def create_spark_connection():
    s_conn = None

    try:
        s_conn = (
            SparkSession.builder.appName("SparkDataStreaming")
            .config(
                "spark.jars.packages",
                "com.datastax.spark:spark-cassandra-connector_2.13:3.5.0,"
                "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1",
            )
            .config("spark.cassandra.connection.host", "localhost")
            .getOrCreate()
        )

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn


def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = (
            spark_conn.readStream.format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "movie_data")
            .option("startingOffsets", "earliest")
            .load()
        )
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df


def create_cassandra_connection():
    try:
        cluster = Cluster(["localhost"])

        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None


def create_movie_df_from_kafka(spark_df):
    schema = StructType(
        [
            StructField("Title", StringType(), False),
            StructField("Year", StringType(), False),
            StructField("Director", StringType(), False),
            StructField("Actors", StringType(), False),
            StructField("Plot", StringType(), False),
            StructField("Ratings", MapType(StringType(), StringType()), False),
            StructField("BoxOffice", StringType(), False),
        ]
    )

    sel = (
        spark_df.selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
    )
    print(sel)

    return sel


if __name__ == "__main__":
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_movie_df_from_kafka(spark_df)
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)

            logging.info("Streaming is being started...")

            streaming_query = (
                selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                .option("checkpointLocation", "/tmp/checkpoint")
                .option("keyspace", "spark_streams")
                .option("table", "movies")
                .start()
            )

            streaming_query.awaitTermination()
