from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, when

def load_data(spark):
    """
    Loads red and white wine data into DataFrames.
    """
    # Read the red wine data
    red_wine_df = spark.read.csv("data/winequality-red.csv", 
                                 header=True, sep=";", inferSchema=True)
    # Read the white wine data
    white_wine_df = spark.read.csv("data/winequality-white.csv", 
                                   header=True, sep=";", inferSchema=True)
    return red_wine_df, white_wine_df

def transform_data(red_wine_df, white_wine_df):
    """
    Adds 'wine_type' and 'quality_category' columns and combines the DataFrames.
    """
    # Add 'wine_type' column
    red_wine_df = red_wine_df.withColumn("wine_type", lit("red"))
    white_wine_df = white_wine_df.withColumn("wine_type", lit("white"))
    # Union the DataFrames
    wine_df = red_wine_df.union(white_wine_df)
    # Add 'quality_category' column
    wine_df = wine_df.withColumn("quality_category", 
                                 when(wine_df.quality >= 7, "High")
                                 .when(wine_df.quality <= 4, "Low")
                                 .otherwise("Medium"))
    return wine_df

def perform_sql_query(spark, wine_df):
    """
    Performs a Spark SQL query to find average alcohol content.
    """
    # Create a temporary view
    wine_df.createOrReplaceTempView("wine_table")
    # Execute SQL query
    avg_alcohol_df = spark.sql("""
        SELECT 
            wine_type, 
            quality_category, 
            ROUND(AVG(alcohol), 2) AS avg_alcohol
        FROM 
            wine_table
        GROUP BY 
            wine_type, 
            quality_category
        ORDER BY 
            wine_type, 
            quality_category
    """)
    return avg_alcohol_df

def save_output(avg_alcohol_df):
    """
    Saves the result DataFrame to a CSV file.
    """
    avg_alcohol_df.coalesce(1).write.csv("output/avg_alcohol_by_wine_type_and_quality", 
                                         header=True, mode="overwrite")

def main():
    spark = SparkSession.builder.appName("WineQualityAnalysis").getOrCreate()
    red_wine_df, white_wine_df = load_data(spark)
    wine_df = transform_data(red_wine_df, white_wine_df)
    avg_alcohol_df = perform_sql_query(spark, wine_df)
    avg_alcohol_df.show()
    save_output(avg_alcohol_df)
    spark.stop()

if __name__ == "__main__":
    main()


