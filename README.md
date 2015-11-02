# swutils

## Introduction

swutils is a Python library for interacting with [ScraperWiki](https://scraperwiki.com/products/data-science-platform) boxes.

With swutils, you can

- Schedule jobs to run on daily basis
- Get email notifications if a job fails
- and much more...

## Requirements

swutils has been tested on the following configuration:

- MacOS X 10.9.5
- Python 2.7.10

swutils requires the following in order to run properly:

- [Python >= 2.7](http://www.python.org/download) (MacOS X comes with python preinstalled)

## Installation

(You are using a [virtualenv](http://www.virtualenv.org/en/latest/index.html), right?)

    sudo pip install swutils

## Usage

```python
import swutils

job = lambda: 'Code to update SW database'
exc_handler = swutils.ExceptionHandler('reubano@gmail.com').handler
swutils.run_or_schedule(job, True, exc_handler)
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

swutils is distributed under the [MIT License](http://opensource.org/licenses/MIT).
