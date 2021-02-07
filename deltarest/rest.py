import re
import json

from flask import Response
from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkContext

import sqlparse
from sqlparse.sql import Identifier, IdentifierList

class _HadoopFSUtil:
    @staticmethod
    def list_folder_names(sc: SparkContext, root):
        fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
            sc._jsc.hadoopConfiguration()
        )
        statuses = fs.listStatus(sc._jvm.org.apache.hadoop.fs.Path(root))
        return list(map(lambda status: status.getPath().getName(), statuses))

class _ParsedURI:
    def __init__(self, table_name: str, query_sql: str):
        self.table_name = table_name
        self.query_sql = query_sql


class _URIParser:
    uri_pattern = \
        r"/tables(/([a-zA-Z0-9_]+))?(\?sql=(SELECT(\n|.)*))?"

    @classmethod
    def parse(cls, uri):
        match = re.fullmatch(
            cls.uri_pattern,
            uri
        )
        if match is None:
            raise RuntimeError(
                f"Invalid URI '{uri}'. Must match pattern: '{cls.uri_pattern}'"
            )
        else:
            return _ParsedURI(
                table_name=match.group(2),
                query_sql=match.group(4)
            )


class _ResponseFormatter:
    @staticmethod
    def rows(table: DataFrame) -> Response:
        return Response(
            f'{{"rows":[{",".join(table.toJSON().collect())}]}}',
            status=200,
            mimetype="application/json"
        )

    @staticmethod
    def tables_list(tables_names: list) -> Response:
        as_dict = {
            "table_names": tables_names
        }
        return Response(
            json.dumps(as_dict, separators=(',', ':')),
            status=200,
            mimetype="application/json"
        )

class _SQLParser:
    @staticmethod
    def get_table_identifiers_inside_kw(statement, keyword):
        kw = keyword.lower()
        tokens = sqlparse.parse(statement)[0].tokens
        tables_real_names = []
        i = 0

        while not (tokens[i].is_keyword and tokens[i].value.lower() == kw):
            i += 1
        i += 1

        while i < len(tokens) and not (tokens[i].is_keyword):
            token = tokens[i]
            if isinstance(token, Identifier):
                tables_real_names.append(token.get_real_name())
            if isinstance(token, IdentifierList):
                for identifier in token.get_identifiers():
                    tables_real_names.append(identifier.get_real_name())
            i += 1

        return tables_real_names


class DeltaRESTAdapter:
    def __init__(self, delta_storage_root: str):
        self.delta_storage_root = delta_storage_root
        self.spark = SparkSession.builder.getOrCreate()

    def post(self, uri: str, payload: dict):
        pass

    def get(self, uri: str) -> Response:
        parsed_uri = _URIParser.parse(uri)
        if parsed_uri.table_name is not None:
            table = self.spark \
                .read \
                .format("delta") \
                .load(f"{self.delta_storage_root}/{parsed_uri.table_name}")
            if parsed_uri.query_sql is None:
                return _ResponseFormatter.rows(table)
            else:
                table.createOrReplaceTempView(parsed_uri.table_name)
                return _ResponseFormatter.rows(
                    self.spark.sql(parsed_uri.query_sql)
                )

        else:
            if parsed_uri.query_sql is None:
                return _ResponseFormatter.tables_list(
                    _HadoopFSUtil.list_folder_names(
                        self.spark.sparkContext,
                        self.delta_storage_root
                    )
                )
            else:
                table_identifiers = _SQLParser.get_table_identifiers_inside_kw(
                    parsed_uri.query_sql,
                    "FROM"
                ) + \
                                    _SQLParser.get_table_identifiers_inside_kw(
                                        parsed_uri.query_sql,
                                        "JOIN"
                                    )
                for table_identifier in table_identifiers:
                    self.spark \
                        .read \
                        .format("delta") \
                        .load(f"{self.delta_storage_root}/{table_identifier}") \
                        .createOrReplaceTempView(table_identifier)

                return _ResponseFormatter.rows(
                    self.spark.sql(parsed_uri.query_sql)
                )
