# -*- coding: utf-8 -*-
"""Top-level package for Peloptimize."""

__author__ = 'Erik Ingwersen'
__email__ = 'erik.ingwersen@br.ey.com'
__version__ = '0.01'

if 'spark' not in globals().keys():
    from pyspark.sql import SparkSession

    global spark
    spark = SparkSession.builder.getOrCreate()
