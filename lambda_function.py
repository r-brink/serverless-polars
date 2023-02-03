import json
from datetime import datetime

import boto3
import polars as pl

def lambda_handler(event, context):
    s3 = boto3.resource("s3")
    s3.meta.client.download_file("[BUCKETNAME]", "lineitem.parquet", "/tmp/lineitem.parquet")

    lineitem = pl.scan_parquet("/tmp/lineitem.parquet")

    VAR1 = datetime(1998, 9, 2)

    print(
        lineitem.filter(pl.col("l_shipdate") <= VAR1)
        .groupby(["l_returnflag", "l_linestatus"])
        .agg(
            [
                pl.sum("l_quantity").alias("sum_qty"),
                pl.sum("l_extendedprice").alias("sum_base_price"),
                (pl.col("l_extendedprice") * (1 - pl.col("l_discount")))
                .sum()
                .alias("sum_disc_price"),
                (
                    pl.col("l_extendedprice")
                    * (1.0 - pl.col("l_discount"))
                    * (1.0 + pl.col("l_tax"))
                )
                .sum()
                .alias("sum_charge"),
                pl.mean("l_quantity").alias("avg_qty"),
                pl.mean("l_extendedprice").alias("avg_price"),
                pl.mean("l_discount").alias("avg_disc"),
                pl.count().alias("count_order"),
            ],
        )
        .sort(["l_returnflag", "l_linestatus"])
        .collect(streaming=True)
    )

    return {"status": 200}