# delta-rest
[![Actions Status](https://github.com/enzobnl/delta-rest/workflows/test/badge.svg)](https://github.com/enzobnl/delta-rest/actions) [![Actions Status](https://github.com/enzobnl/delta-rest/workflows/PyPI/badge.svg)](https://github.com/enzobnl/delta-rest/actions)


***Interact with [Delta Lake](https://github.com/delta-io/delta) through a RESTful API.***

*DeltaREST* is a Python library that allows you to easily launch inside your [Spark](https://github.com/apache/spark/) *driver* process a [Flask](https://github.com/pallets/flask) -based server exposing a RESTful API to interact with Delta tables.


*(Help and pull requests are very welcome !)*

# Example in local
## Install
```bash
pip install deltarest
```

## Run service
```python
from deltarest import DeltaRESTService
from pyspark.sql import SparkSession

# Create local SparkSession
SparkSession \
    .builder \
    .appName("local_deltarest_test") \
    .master("local") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:0.8.0") \
    .getOrCreate()

# Start service on port 4444
DeltaRESTService(delta_root_path="/tmp/lakehouse-root") \
    .run("0.0.0.0", "4444")
```

Notes:
When deployed on cluster: 
- `delta_root_path` could be a cloud storage path. 
- run `spark-submit` using `client` deployMode.

## PUT
### Create Delta table with a specific identifier (evolutive schema)
```bash
curl -X PUT http://127.0.0.1:4444/tables/foo
```
Response code `201`.
```json
{
    "message":"Table foo created"
}
```

On already existing table identifier:
```bash
curl -X PUT http://127.0.0.1:4444/tables/foo
```
Response code `200`.
```json
{
    "message":"Table foo already exists"
}
```

## POST
### Append json rows to a Delta table
```bash
curl -X POST http://127.0.0.1:4444/tables/foo --data '{"rows":[{"id":1,"collection":[1,2]},{"id":2,"collection":[3,4]}]}'
```
Response code `201`.
```json
{
    "message": "Rows created"
}
```

## GET

### List available Delta tables
```bash
curl -G http://127.0.0.1:4444/tables
```
Response code `200`.
```json
{
  "tables":["foo"]
}
```

### Get a particular Delta table content
```bash
curl -G http://127.0.0.1:4444/tables/foo
```
Response code `200`.
```json
{
    "rows":[
        {"id":1,"collection":[1,2]},
        {"id":2,"collection":[3,4]}
    ]
}
```
On unexisting Delta table
```bash
curl -G http://127.0.0.1:4444/tables/bar
```
Response code `404`.
```json
{
  "message":"Table bar not found"
}
```

### Get the result of an arbitrary SQL query on Delta tables
Must only involve listable delta tables.

```bash
curl -G http://127.0.0.1:4444/tables --data-urlencode "sql=SELECT count(*) as count FROM foo CROSS JOIN foo"
```
Response code `200`.
```json
{
    "rows":[
        {"count":4}
    ]
}
```