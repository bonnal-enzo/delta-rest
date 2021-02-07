import re
import json
from urllib.parse import unquote

from flask import Response
from pyspark.sql import SparkSession, DataFrame

import sqlparse
from sqlparse.sql import Identifier, IdentifierList


class _HadoopFSUtil:
    def __init__(self, jvm, conf):
        self.jvm = jvm
        self.fs = self.jvm.org.apache.hadoop.fs.FileSystem.get(conf)

    def list(self, path):
        """
        :param path: absolute path to scan
        :return: alphabetically sorted list of folder and files names in `path`
        """
        statuses = self.fs.listStatus(self.jvm.org.apache.hadoop.fs.Path(path))
        statuses_names_list = list(map(
            lambda status: status.getPath().getName(),
            statuses
        ))
        statuses_names_list.sort()
        return statuses_names_list

    def mkdir(self, path):
        self.fs.mkdirs(
            self.jvm.org.apache.hadoop.fs.Path(path)
        )

    def exists(self, path):
        self.fs.exists(
            self.jvm.org.apache.hadoop.fs.Path(path)
        )


class _ParsedURI:
    def __init__(self, table_identifier: str, query_sql: str):
        self.table_identifier = table_identifier
        self.query_sql = query_sql


class _URIParser:
    uri_pattern = \
        r"/tables(/([a-zA-Z0-9_]+))?(\?sql=(SELECT(\n|.)*))?"

    @classmethod
    def parse(cls, uri):
        match = re.fullmatch(
            cls.uri_pattern,
            unquote(uri)
        )
        if match is None:
            raise RuntimeError(
                f"Invalid URI '{uri}'. Must match pattern: '{cls.uri_pattern}'"
            )
        else:
            return _ParsedURI(
                table_identifier=match.group(2),
                query_sql=match.group(4)
            )


class _ResponseFormatter:
    post_done: Response = Response(
        f'{{"message":"Rows created"}}',
        status=201,
        mimetype="application/json"
    )

    @staticmethod
    def table_created(table_identifier: str) -> Response:
        return Response(
            f'{{"message":"Table {table_identifier} created"}}',
            status=201,
            mimetype="application/json"
        )

    @staticmethod
    def table_already_exists(table_identifier: str) -> Response:
        return Response(
            f'{{"message":"Table {table_identifier} already exists"}}',
            status=200,
            mimetype="application/json"
        )

    @staticmethod
    def table_not_found(table_identifier: str) -> Response:
        return Response(
            f'{{"message":"Table {table_identifier} not found"}}',
            status=404,
            mimetype="application/json"
        )

    @staticmethod
    def bad_request(msg):
        return Response(
            f'{{"message":"BAD REQUEST: {msg}"}}',
            status=400,
            mimetype="application/json"
        )

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
            "tables": tables_names
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

        while i < len(tokens) and not (
                tokens[i].is_keyword and tokens[i].value.lower() == kw):
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

    @classmethod
    def extract_identifiers(cls, query_sql):
        return _SQLParser.get_table_identifiers_inside_kw(query_sql, "FROM") + \
               _SQLParser.get_table_identifiers_inside_kw(query_sql, "JOIN")


class DeltaRESTAdapter:

    def __init__(self, delta_storage_root: str, max_response_n_rows: int):
        self.delta_storage_root = delta_storage_root
        self.max_response_n_rows = max_response_n_rows
        self.spark = SparkSession.builder.getOrCreate()
        self.fs_util = _HadoopFSUtil(
            self.spark.sparkContext._jvm,
            self.spark.sparkContext._jsc.hadoopConfiguration()
        )
        if not self.fs_util.exists(self.delta_storage_root):
            self.fs_util.mkdir(self.delta_storage_root)

    def put(self, uri):
        parsed_uri = _URIParser.parse(uri)
        if parsed_uri.table_identifier is None:
            raise RuntimeError(
                "PUT must be called on route like /tables/<table_identifier>"
            )
        if parsed_uri.query_sql is not None:
            raise RuntimeError(
                "PUT does not support sql query arg"
            )
        if parsed_uri.table_identifier in \
                self.fs_util.list(self.delta_storage_root):
            return _ResponseFormatter.table_already_exists(
                parsed_uri.table_identifier)
        else:
            self.fs_util.mkdir(
                f"{self.delta_storage_root}/{parsed_uri.table_identifier}"
            )
            return _ResponseFormatter.table_created(parsed_uri.table_identifier)

    def post(self, uri: str, payload: dict):
        parsed_uri = _URIParser.parse(uri)
        if parsed_uri.table_identifier is None:
            raise RuntimeError(
                "POST must be called on route like /tables/<table_identifier>"
            )
        if parsed_uri.query_sql is not None:
            raise RuntimeError(
                "POST does not support sql query arg"
            )

        if parsed_uri.table_identifier not in \
                self.fs_util.list(self.delta_storage_root):
            return _ResponseFormatter.table_not_found(
                parsed_uri.table_identifier
            )

        rows: list = payload["rows"]
        columns = rows[0].keys()

        self.spark.sparkContext \
            .parallelize([list(row.values()) for row in rows]) \
            .toDF() \
            .selectExpr([f"_{i + 1} as {k}" for i, k in enumerate(columns)]) \
            .write.option("mergeSchema", "true") \
            .format("delta") \
            .mode("append") \
            .save(f"{self.delta_storage_root}/{parsed_uri.table_identifier}")

        return _ResponseFormatter.post_done

    def get(self, uri: str) -> Response:
        try:
            parsed_uri = _URIParser.parse(uri)
            folder_names_list = self.fs_util.list(self.delta_storage_root)

            if parsed_uri.table_identifier is not None:
                if parsed_uri.table_identifier not in folder_names_list:
                    return _ResponseFormatter.table_not_found(
                        parsed_uri.table_identifier
                    )

                table = self.spark \
                    .read \
                    .format("delta") \
                    .load(
                    f"{self.delta_storage_root}/{parsed_uri.table_identifier}")

                if parsed_uri.query_sql is None:
                    return _ResponseFormatter.rows(
                        table.limit(self.max_response_n_rows)
                    )

                else:
                    table.createOrReplaceTempView(parsed_uri.table_identifier)
                    return _ResponseFormatter.rows(
                        self.spark.sql(parsed_uri.query_sql)
                            .limit(self.max_response_n_rows)
                    )

            else:
                if parsed_uri.query_sql is None:
                    return _ResponseFormatter.tables_list(
                        folder_names_list
                    )
                else:
                    for table_identifier in _SQLParser.extract_identifiers(
                            parsed_uri.query_sql
                    ):
                        self.spark \
                            .read \
                            .format("delta") \
                            .load(
                            f"{self.delta_storage_root}/{table_identifier}") \
                            .createOrReplaceTempView(table_identifier)

                    return _ResponseFormatter.rows(
                        self.spark.sql(parsed_uri.query_sql)
                            .limit(self.max_response_n_rows)
                    )

        except Exception as e:
            return _ResponseFormatter.bad_request(
                str(e).replace('"', "'").split("\n")[:self.max_response_n_rows]
            )
