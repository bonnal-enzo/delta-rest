# delta-rest
[![Actions Status](https://github.com/enzobnl/delta-rest/workflows/test/badge.svg)](https://github.com/enzobnl/delta-rest/actions) [![Actions Status](https://github.com/enzobnl/delta-rest/workflows/PyPI/badge.svg)](https://github.com/enzobnl/delta-rest/actions)


RESTful API to interact with Delta Lake.

# Examples
## Run Flask service
This service is run in the *Spark driver*,
use `client` deployment mode to be able to access the service.

```python
from deltarest import DeltaRESTService

DeltaRESTService(delta_storage_root="gs://lakehouse/tables")\
    .run("localhost", "4444")
```

## PUT
### Create Delta table with a specific identifier (evolutive schema)
```bash
curl -X PUT http://localhost:4444/tables/foo
```
Response code `201`.
```json
{
    "message":"Table foo created"
}
```

On already existing table identifier:
```bash
curl -X PUT http://localhost:4444/tables/foo
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
curl -X POST http://localhost:4444/tables/foo --data '{"rows":[{"id":1,"collection":[1,2]},{"id":2,"collection":[3,4]}]}'
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
curl -G http://localhost:4444/tables
```
Response code `200`.
```json
{
  "tables":["foo"]
}
```

### Get a particular Delta table content
```bash
curl -G http://localhost:4444/tables/foo
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
curl -G http://localhost:4444/tables/bar
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
curl -G http://localhost:4444/tables --data-urlencode "sql=SELECT count(*) as count FROM foo CROSS JOIN foo"
```
Response code `200`.
```json
{
    "rows":[
        {"count":4}
    ]
}
```