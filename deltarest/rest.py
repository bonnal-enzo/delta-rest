from pyspark.sql import SparkSession, DataFrame
from pyspark import RDD
import re
import json

class ParsedURI:
    def __init__(self, table_name: str, query_sql: str):
        self.table_name = table_name
        self.query_sql = query_sql


class URIParser:
    uri_pattern = r"/tables/([a-zA-Z_]+)?(/query\?sql=[\\n ]*(.*))?"

    @classmethod
    def parse(cls, uri):
        match = re.fullmatch(
            cls.uri_pattern,
            uri
        )
        if match is None:
            raise RuntimeError(
                f"unvalid URI '{uri}'. Must match pattern: '{cls.uri_pattern}'"
            )
        else:
            table_name = match.group(1)
            query_sql = match.group(3)
            if query_sql is not None and \
                    not query_sql.lower().startswith("select"):
                raise RuntimeError(
                    f"query SQL statement must be a SELECT, not: '{query_sql}'"
                )
            return ParsedURI(table_name, query_sql)

class ResponseFormatter:
    @staticmethod
    def to_json_payload(rdd: DataFrame) -> str:
        return f'{{"rows": [{",".join(rdd.toJSON().collect())}]}}'

class DeltaRESTAdapter:
    def __init__(self, delta_storage_root: str):
        self.delta_storage_root = delta_storage_root
        self.spark = SparkSession.builder.getOrCreate()

    def post(self, uri: str, payload: dict):
        pass

    def get(self, uri: str) -> str:
        parsed_uri = URIParser.parse(uri)
        if parsed_uri.table_name is not None:
            table = self.spark \
                .read \
                .format("delta") \
                .load(f"{self.delta_storage_root}/{parsed_uri.table_name}")
            if parsed_uri.query_sql is None:
                return ResponseFormatter.to_json_payload(table)
            else:
                table.createOrReplaceTempView(parsed_uri.table_name)
                self.spark.sql(parsed_uri.query_sql)

        else:
            raise RuntimeError("Currently unsupported")