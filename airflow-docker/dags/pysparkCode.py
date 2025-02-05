from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rank
from pyspark.sql.window import Window

def process_csv():
# Initialize Spark
    spark = SparkSession.builder.appName("Bus Travel Analysis").getOrCreate()

    # Load CSV data
    csv_filename = "bus_travel_data.csv"
    df = spark.read.csv(csv_filename, header=True, inferSchema=True)

    # Aggregate total passengers per route per month
    agg_df = df.groupBy("Route", "Month").sum("Passengers").withColumnRenamed("sum(Passengers)", "Total_Passengers")

    # Rank months based on total passengers per route
    windowSpec = Window.partitionBy("Route").orderBy(col("Total_Passengers").desc())
    ranked_df = agg_df.withColumn("rank", rank().over(windowSpec))

    # Extract top travel month per route
    top_months_per_route = ranked_df.filter(col("rank") == 1).drop("rank")

    # ---------------- Find top routes per month ----------------
    agg_route_df = df.groupBy("Month", "Route").sum("Passengers").withColumnRenamed("sum(Passengers)", "Total_Passengers")

    # Rank routes based on total passengers per month
    windowSpecRoute = Window.partitionBy("Month").orderBy(col("Total_Passengers").desc())
    ranked_route_df = agg_route_df.withColumn("rank", rank().over(windowSpecRoute))

    # Extract top route per month
    top_routes_per_month = ranked_route_df.filter(col("rank") == 1).drop("rank")

    # Save both insights as CSV
    output_top_months = "bus_travel_top_months.csv"
    output_top_routes = "bus_travel_top_routes.csv"

    top_months_per_route.toPandas().to_csv(output_top_months, index=False)
    top_routes_per_month.toPandas().to_csv(output_top_routes, index=False)

    print(f"Insights saved to '{output_top_months}' and '{output_top_routes}'.")

process_csv()
