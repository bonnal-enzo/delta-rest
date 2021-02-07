import unittest

import sys
import time

sys.path.append("..")

from deltarest import DeltaRESTAdapter, DeltaRESTService

from pyspark.sql import SparkSession


class Test(unittest.TestCase):
    root_dir: str = f"/tmp/delta_rest_test_{int(time.time())}"

    spark: SparkSession = None

    dra: DeltaRESTAdapter

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession \
            .builder \
            .appName("unit_tests") \
            .master("local") \
            .config("spark.jars.packages", "io.delta:delta-core_2.12:0.8.0") \
            .getOrCreate()
        cls.dra = DeltaRESTAdapter(cls.root_dir, 10)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        # os.rmdir(cls.root_dir)

    def test_scenario(self):
        # PUT

        self.assertEqual(
            '{"message":"Table foo created"}',
            bytes.decode(self.dra.put("/tables/foo").response[0], "utf-8")
        )

        self.assertEqual(
            '{"message":"Table foo already exists"}',
            bytes.decode(self.dra.put("/tables/foo").response[0], "utf-8")
        )

        self.assertEqual(
            '{"message":"Table bar created"}',
            bytes.decode(self.dra.put("/tables/bar").response[0], "utf-8")
        )

        # POST

        self.assertEqual(
            '{"message":"Rows created"}',
            bytes.decode(self.dra.post(
                "/tables/foo",
                {"rows": [
                    {"id": 1, "collection": [1, 2]},
                    {"id": 2, "collection": [3, 4]}
                ]}
            ).response[0], "utf-8")
        )

        # post rows in a table not created yet

        self.assertEqual(
            '{"message":"Table foo_ not found"}',
            bytes.decode(self.dra.post(
                "/tables/foo_",
                {"rows": [
                    {"id": 1, "collection": [1, 2]},
                    {"id": 2, "collection": [3, 4]}
                ]}
            ).response[0], "utf-8")
        )

        # GET

        # get full table

        self.assertEqual(
            '{"rows":[{"id":1,"collection":[1,2]},{"id":2,"collection":[3,4]}]}',
            bytes.decode(
                self.dra.get("/tables/foo").response[0],
                "utf-8"
            )
        )

        # get with query on a table

        self.assertEqual(
            '{"rows":[{"count":2,"collections_concat_size":4}]}',
            bytes.decode(
                self.dra.get(
                    """/tables/foo?sql=SELECT
                    count(*) as count,
                    sum(size(collection)) as collections_concat_size
                    FROM foo"""
                ).response[0],
                "utf-8"
            )
        )

        # get with query on tables

        self.assertEqual(
            '{"rows":[{"count":4}]}',
            bytes.decode(
                self.dra.get(
                    """/tables?sql=SELECT 
                      count(*) as count
                    FROM foo as t1 CROSS JOIN foo as t2
                    LIMIT 100"""
                ).response[0],
                "utf-8"
            )
        )

        # get tables names listing

        self.assertEqual(
            '{"tables":["bar","foo"]}',
            bytes.decode(
                self.dra.get("/tables").response[0],
                "utf-8"
            )
        )

    # def test_service(self):
    #     DeltaRESTService(self.root_dir).run("localhost", "4444")
