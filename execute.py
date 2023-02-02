import os
from dotenv import load_dotenv, find_dotenv
from datetime import datetime
import boto3
import polars as pl

load_dotenv(find_dotenv())

def download_data(file_name, bucket_name):
    file_name = file_name
    bucket_name = bucket_name
    s3_resource = boto3.resource('s3')
    return s3_resource.Object(bucket_name, file_name).download_file(f'{file_name}')

def polars_analysis(path, output_file):
    # Q1 from TPCH benchmark
    
    lineitem = pl.scan_parquet(f'{path}')
    VAR1 = datetime(1998, 9, 2)

    output = (
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
            .sort(["l_returnflag", "l_linestatus"]).collect(streaming=True)
        )
    return output.write_csv(f'{output_file}', sep='\t')

def upload_data(file_name, bucket_name):
    file_name = file_name
    bucket_name = bucket_name
    s3_resource = boto3.resource('s3')
    return s3_resource.Object(bucket_name, file_name).upload_file(f'{file_name}')

download_data(os.environ.get('FILENAME'), os.environ.get('BUCKETNAME')) # download data from S3 to instance
polars_analysis(os.environ.get('FILENAME'), 'output.txt')   # run your query
upload_data('output.txt', os.environ.get('BUCKETNAME')) # upload results back to S3 bucket