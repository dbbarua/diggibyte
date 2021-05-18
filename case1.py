#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Developed By Deepak Barua<dbbarua@icloud.com> on 18 May 2021 for Diggibyte
import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit


# Function to join the two dataframes and remove duplicate columns
def join(df1, df2, cond, how):
    df = df1.join(df2, cond, how=how)
    repeated_columns = [c for c in df1.columns if c in df2.columns]
    for col in repeated_columns:
        df = df.drop(df2[col])
    return df


if __name__ == '__main__':
    scSpark = SparkSession \
        .builder \
        .appName("process data") \
        .getOrCreate()

    # Get the CSV to dataframe
    data_file = 'started_streams.csv'
    sdfData = scSpark.read.csv(data_file, header=True, sep=",").cache()
    # print('Total Records = {}'.format(sdfData.count()))
    # sdfData.show()
    print(sdfData.head())

    # Get the CSV to Dataframe
    data_file2 = 'whatson.csv'
    sdfData2 = scSpark.read.csv(data_file, header=True, sep=",").cache()
    # print('Total Records = {}'.format(sdfData2.count()))
    # sdfData2.show()
    #Performing Inner Join as we need the common data from both Dataframes
    mergedSdf = join(sdfData, sdfData2, [sdfData["house_number"] == sdfData2["house_number"],
                                         sdfData["country_code"] == sdfData2["country_code"]], 'inner')

    # Filter records with product type as tvod or est
    mergedSdf = mergedSdf.filter(mergedSdf["product_type"].eqNullSafe(lit("tvod" or "est")))
    # Broadcast dates most recent on top
    print(mergedSdf.sort("time", order="dsc").show())

