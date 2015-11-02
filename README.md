# swutils

## Introduction

swutils is a [Python library](#library) for interacting with remote and local [CKAN](http://ckan.org/) instances. It uses [ckanapi](https://github.com/ckan/ckanapi) under the hood, and is essentially a high level wrapper for it. A command line interface built on top of this library is available at [ckanny](https://github.com/reubano/ckanny).

With swutils, you can

- Download a CKAN resource
- Upload CSV/XLS/XLSX files into a CKAN DataStore
- and much more...

## Requirements

swutils has been tested on the following configuration:

- MacOS X 10.9.5
- Python 2.7.9

swutils requires the following in order to run properly:

- [Python >= 2.7](http://www.python.org/download) (MacOS X comes with python preinstalled)

## Installation

(You are using a [virtualenv](http://www.virtualenv.org/en/latest/index.html), right?)

     sudo pip install swutils

## Usage

swutils is intended to be used directly from Python.

### Examples

*Fetch a remote resource*

```python
from swutils import CKAN

ckan = CKAN(remote='http://demo.ckan.org')
resource_id = '36f33846-cb43-438e-95fd-f518104a32ed'
r, filepath = ckan.fetch_resource(resource_id, filepath='path/to/file.csv')
print(r.encoding)
```

*Fetch a local resource*

```python
from swutils import CKAN

ckan = CKAN(api_key='mykey', remote=None)
resource_id = '36f33846-cb43-438e-95fd-f518104a32ed'
r, filepath = ckan.fetch_resource(resource_id, filepath='path/to/file.csv')
print r.encoding
```
*show data*

```python
from swutils import CKAN

ckan = CKAN(api_key='mykey', remote=None)
resource_id = '36f33846-cb43-438e-95fd-f518104a32ed'
r = ckan.datastore_search(resource_id)
print r.next()
```

## Configuration

swutils will use the following [Environment Variables](http://www.cyberciti.biz/faq/set-environment-variable-linux/) if set:

Environment Variable|Description
--------------------|-----------
CKAN_API_KEY|Your CKAN API Key
CKAN_REMOTE_URL|Your CKAN instance remote url
CKAN_USER_AGENT|Your user agent

## Hash Table

In order to support file hashing, swutils creates a hash table resource called `hash_table.csv` with the following schema:

field|type
------|----
datastore_id|text
hash|text

By default the hash table resource will be placed in the package `hash_table`. swutils will create this package if it doesn't exist. Optionally, you can set the hash table package in the command line with the `-H, --hash-table` option, or in a Python file as the `hash_table` keyword argument to `api.CKAN`.

Example:

```python
from swutils import api
ckan = api.CKAN(hash_table='custom_hash_table')
hash = ckan.get_hash('36f33846-cb43-438e-95fd-f518104a32ed')
```

## Scripts

swutils comes with a built in task manager `manage.py` and a `Makefile`.

### Setup

    pip install -r dev-requirements.txt

### Examples

*Run python linter and nose tests*

```bash
manage lint
manage test
```

Or if `make` is more your speed...

```bash
make lint
make test
```

## Contributing

View [CONTRIBUTING.rst](https://github.com/reubano/swutils/blob/master/CONTRIBUTING.rst)

## License

swutils is distributed under the [MIT License](http://opensource.org/licenses/MIT), the same as [ckanapi](https://github.com/ckan/ckanapi).
