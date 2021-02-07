import unittest

import sys
import time
import os

sys.path.append("..")

from deltarest import *

from pyspark.sql import SparkSession


class Test(unittest.TestCase):
    root_dir = f"/tmp/delta_rest_test_{int(time.time())}"

    spark: SparkSession = None

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession \
            .builder \
            .appName("unit_tests") \
            .master("local") \
            .config("spark.jars.packages", "io.delta:delta-core_2.12:0.8.0") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        # os.rmdir(cls.root_dir)

    def test_delta(self):
        self.spark \
            .range(1) \
            .write \
            .format("delta") \
            .save(f"{self.root_dir}/test_delta")

    def test_get(self):
        # get full table
        self.spark \
            .range(1, 2) \
            .selectExpr("id as a", "array(1, 2) as b") \
            .write \
            .format("delta") \
            .save(f"{self.root_dir}/test_get")

        self.assertEqual(
            '{"rows":[{"a":1,"b":[1,2]}]}',
            bytes.decode(
                DeltaRESTAdapter(self.root_dir).get(
                    "/tables/test_get").response[0],
                "utf-8"
            )
        )

        # get with query on a table

        self.assertEqual(
            '{"rows":[{"count":1}]}',
            bytes.decode(
                DeltaRESTAdapter(self.root_dir).get(
                    """/tables/test_get?sql=SELECT count(b) as count 
                    FROM test_get GROUP BY a"""
                ).response[0],
                "utf-8"
            )
        )

        # get with query on tables

        self.spark \
            .range(1, 2) \
            .selectExpr("id as a", "array(1, 2) as b") \
            .write \
            .format("delta") \
            .save(f"{self.root_dir}/test_get2")

        self.assertEqual(
            '{"rows":[{"u":[1,2,1,2]}]}',
            bytes.decode(
                DeltaRESTAdapter(self.root_dir).get(
                    """/tables?sql=SELECT 
                      concat(t1.b, t2.b) as u 
                    FROM test_get as t1 JOIN test_get2 as t2 ON t1.a = t2.a 
                    LIMIT 100"""
                ).response[0],
                "utf-8"
            )
        )

        # get tables names listing
        self.assertEqual(
            '{"table_names":["test_get2","test_get"]}',
            bytes.decode(
                DeltaRESTAdapter(self.root_dir).get("/tables").response[0],
                "utf-8"
            )
        )
