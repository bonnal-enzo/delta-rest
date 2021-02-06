import unittest

import sys
import time

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
            DeltaRESTAdapter(self.root_dir).get("/tables/test_get")
        )

        # get with query on a table

        self.assertEqual(
            '{"rows":[{"count":1}]}',
            DeltaRESTAdapter(self.root_dir).get(
                """/tables/test_get/query?sql=
                SELECT count(b) as count FROM test_get GROUP BY a"""
            )
        )
