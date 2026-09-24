from pyspark import pipelines as dlt
from pyspark.sql.functions import *



@dlt.table(
    comment = "Raw EIA electricity demand data from ADF ingestion"
)
def bronze_electricity_demand():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load("abfss://bronze@energyprojectdl.dfs.core.windows.net/API ingestion/electricity_daily")
    )

@dlt.table(
    comment = "Cleaned, dedup Silver Layer"
)
@dlt.expect_or_drop("valid_value", "value IS NOT NULL")
def silver_electricity_demand():
    return (
        dlt.read_stream("bronze_electricity_demand")
        .dropDuplicates(["period", "respondent", "type"])
    )
    
@dlt.table(
    comment = "Daily demand summary by region: total, average and peak megawatt-hours"
)

def gold_daily_demand_summary():
    df = dlt.read("silver_electricity_demand")
    return (
        df.groupBy("respondent", "respondent-name")
        .agg(
            count("*").alias("hours_recorded"),
            avg("value").alias("average_demand"),
            max("value").alias("peak_demand")
        )
    )

@dlt.table(
    comment = "Peak demand hour per region - which hour of day sees the highest load"
)

def gold_peak_demand_hour():
    from pyspark.sql.window import Window
    df = dlt.read("silver_electricity_demand")
    
    df_with_hour = df.withColumn("hour_of_the_day", hour(col("period")))

    window_spec = Window.partitionBy("respondent").orderBy(col("value").desc())
    return(
    df_with_hour
    .withColumn("rank", row_number().over(window_spec))
    .filter(col("rank")==1)
    .select("respondent", "respondent-name", "period", "hour_of_the_day" ,"value")
        )
    


