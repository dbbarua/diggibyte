#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Developed By Deepak Barua<dbbarua@icloud.com> on 18 May 2021 for Diggibyte
# Get the uniques users and most watched genre
#Case 2,3

from pyspark.sql import SparkSession



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
#Groups the common genres and user_ids together to count the most hours watched
    print(str(sdfData.groupby("genre","user_id").count().distinct().show()))