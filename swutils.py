# -*- coding: utf-8 -*-
# vim: sw=4:ts=4:expandtab

"""
swutils
~~~~~~~

Provides methods for interacting with ScraperWiki boxes

Examples:
    Schedule a job::

        job = lambda x: 'hello %s' % x
        exception_handler = ExceptionHandler('reubano@gmail.com').handler
        run_or_schedule(job, True, exception_handler)

Attributes:
    SCHEDULE_TIME (str): Time of the day to run the scheduled job.
"""

from __future__ import (
    absolute_import, division, print_function, with_statement,
    unicode_literals)

import time
import smtplib
import logging
import itertools as it

import schedule as sch
import scraperwiki

from os import environ, path as p
from email.mime.text import MIMEText
from pprint import pprint
from operator import itemgetter
from functools import partial

from tabutils import process as pr, fntools as ft, io
from sqlalchemy import Column, Integer
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import MetaData
from sqlalchemy.orm import sessionmaker

__version__ = '0.11.0'

__title__ = 'swutils'
__author__ = 'Reuben Cummings'
__description__ = 'ScraperWiki box utility library'
__email__ = 'reubano@gmail.com'
__license__ = 'MIT'
__copyright__ = 'Copyright 2015 Reuben Cummings'

SCHEDULE_TIME = '10:30'
meta = MetaData()
logger = logging.getLogger('populate')


class ExceptionHandler(object):
    """Creates a logging exception handler with email notifications

    Note: the following doctests assume you have a running Postfix server
    https://www.garron.me/en/mac/postfix-relay-gmail-mac-os-x-local-smtp.html
    `sudo postfix start`
    """
    def __init__(self, to, logfile='log.txt', logname=''):
        """ExceptionHandler constructor

        Args:
            to (str): The email recipient
            logfile (str): The logfile (default: log.txt)
            logname (str): The logger name (default: '')

        Examples:
            >>> ExceptionHandler('reubano@gmail.com')  # doctest: +ELLIPSIS
            <swutils.ExceptionHandler object at 0x...>
        """
        logging.basicConfig(filename=logfile, level=logging.DEBUG)
        self.to = to
        self.logfile = logfile
        self.logger = logging.getLogger(logname)
        open(logfile, 'w').close() if not p.exists(logfile) else None

    def email(self, subject=None, text=None, host='localhost'):
        """Sends the email notification

        Args:
            subject (str): The email subject (default: localhost).
            text (str): The email content (default: None).
            host (str): The email host server (default: localhost).

        Examples:
            >>> to = 'reubano@gmail.com'
            >>> ExceptionHandler(to).email('hello world')  # doctest: +ELLIPSIS
            <smtplib.SMTP instance at 0x...>
        """
        user = environ.get('USER')
        body = 'https://scraperwiki.com/dataset/%s\n\n%s' % (user, text)
        msg = MIMEText(body)

        msg['From'] = '%s@scraperwiki.com' % user
        msg['Subject'] = subject or 'scraperwiki box %s failed' % user
        msg['To'] = self.to

        s = smtplib.SMTP(host)
        s.sendmail(msg['From'], [msg['To']], msg.as_string())
        s.quit()
        return s

    def handler(self, func):
        """Creates the exception handler

        Args:
            func (func): The func to catch exceptions from

        Examples:
            >>> import os
            >>> from testfixtures import LogCapture
            >>> from tempfile import NamedTemporaryFile
            >>>
            >>> f = NamedTemporaryFile(delete=False)
            >>> to = 'reubano@gmail.com'
            >>> exc_handler = ExceptionHandler(to, f.name).handler
            >>> job = exc_handler(lambda x: x * 2)
            >>> print(job(2))
            4
            >>> with LogCapture() as l:
            ...     job(None)
            ...     print(l)
            root ERROR
              unsupported operand type(s) for *: 'NoneType' and 'int'
            >>> os.unlink(f.name)

        Returns:
            func: the exception handler
        """
        def wrapper(*args, **kwargs):
            try:
                res = func(*args, **kwargs)
            except Exception as e:
                self.logger.exception(str(e))
                scraperwiki.status('error', 'Error collecting data')

                with open(self.logfile, 'rb') as f:
                    self.email(text=f.read())
            else:
                scraperwiki.status('ok')
                return res

        return wrapper


def run_or_schedule(job, schedule=False, exception_handler=None):
    """Runs a job and optionally schedules it to run later

    Args:
        job (func): The func to run
        schedule (bool): Schedule `func` to run in the future (default: False)
        exception_handler (func): The exception handler to wrap the function in
            (default: None)

    Examples:
        >>> job = partial(pprint, 'hello world')
        >>> run_or_schedule(job)
        u'hello world'
        >>> exception_handler = ExceptionHandler('reubano@gmail.com').handler
        >>> run_or_schedule(job, False, exception_handler)
        u'hello world'
    """
    if exception_handler and schedule:
        job = exception_handler(job)

    job()

    if schedule:
        sch.every(1).day.at(SCHEDULE_TIME).do(job)

        while True:
            sch.run_pending()
            time.sleep(1)


def get_message(count, name, deleted=True):
    """Generates a message on the number of records inserted or deleted

    Args:
        count (int): The number of records
        name (str): The table name
        deleted (bool): The exception handler to wrap the function in
            (default: True)

    Examples:
        >>> print(get_message(5, 'table'))
        Deleted 5 records from table `table`.
        >>> print(get_message(5, 'table', False))
        Inserted 5 records into table `table`.

    Returns
        str: The message
    """
    verb, prep = ('Deleted', 'from') if deleted else ('Inserted', 'into')
    return '%s %s records %s table `%s`.' % (verb, count, prep, name)


def execute(records, engine, table, rid=None):
    in_count = len(records)

    if rid:
        # delete records if already in db
        ids = map(itemgetter(rid), records)
        q = table.query.filter(getattr(table, rid).in_(ids))
        del_count = q.delete(synchronize_session=False)
        engine.session.commit()
    else:
        del_count = 0

    engine.execute(table.__table__.insert(), records)
    return del_count, in_count


def get_dynamic_res(engine, get_name, t, **kwargs):
    name, data = t
    f = get_name or unicode.lower
    table_name = f(name)
    Base = declarative_base()

    # dynamically create sqlalchemy table
    attrs = {'__tablename__': table_name}
    table = type(str(name), (Base, kwargs['mixin']), attrs)
    return {'table': table, 'rid': None, 'data': data}


def res_from_models(models, t, data=None, **kwargs):
    table = getattr(models, t.get('name').title())
    return {'table': table, 'rid': t.get('rid'), 'data': data}


def res_from_meta(engine, t, data=None, **kwargs):
    meta.reflect(engine)
    table = meta.tables[t.get('name')]
    return {'table': table, 'rid': t.get('rid'), 'data': data}


def delete_records(table, rid, engine):
    if not rid:
        # delete all records since there is no way to identify them
        try:
            del_count = table.query.delete(synchronize_session=False)
        except OperationalError:
            table.__table__.create(engine)
            del_count = 0
        else:
            engine.session.commit()
    else:
        del_count = 0

    return del_count


def get_tables(data, key):
    keyfunc = itemgetter(key)
    return it.groupby(sorted(data, key=keyfunc), keyfunc)


def gen_data(fetch=None, **kwargs):
    """Generates data from records or file"""
    result = fetch(**kwargs)

    if result.get('f'):
        f = result.pop('f')
        ext = result.pop('ext', 'csv')
        reader = io.get_reader(ext)
        records = reader(f, sanitize=True, **result)
    elif result.get('records'):
        records = result['records']
    else:
        msg = '`fetch` must return a dict with either `records` or `f`.'
        raise TypeError(msg)

    if kwargs.get('normalize'):
        normalized = kwargs['normalize'](records, **kwargs)
    else:
        normalized = records

    if kwargs.get('filterer'):
        filtered = it.ifilter(partial(kwargs['filterer'], **kwargs), normalized)
    else:
        filtered = normalized

    if kwargs.get('parse'):
        parsed = it.imap(partial(kwargs['parse'], **kwargs), filtered)
    else:
        parsed = filtered

    return parsed


def populate(engine, models=None, get_name=None, **kwargs):
    """Populates a SQLAlchemy db with data. Supports both declarative
    SQLAlchemy and Flask-SQLAlchemy

    Note: Either `TABLES` or `KEY` must be defined.

    Args:
        gen_data (func): A function used to generate the data to be inserted
            into the db. It will receive keywords comprised of combining
            `kwargs` with a table defined in `TABLES`.

        engine (obj): A SQLAlchemy engine.
        models (module): A models module of SQLAlchemy table classes
            (default: None).
        get_name (func): A function used to generate the table name if
            `TABLES` is unset. It will receive the name of each
            each grouped obtained by grouping the data generated from
            `gen_data` (default: None).
        kwargs (dict): Keyword arguments passed to `gen_data`.

    Kwargs:
        mixin (class): Base table that dynamically create tables inherit.
            Required if `TABLES` is unset.
        TABLES (list[dicts]): The table options. Required if `KEY` is unset.
        KEY (str): The field used to group data generated from `gen_data`.
            Required if `TABLES` is unset.
        ROW_LIMIT (int): The max total number of rows to process
        CHUNK_SIZE (int): The max number of rows to process at one time
        DEBUG (bool): Run in debug mode
        TESTING (bool): Run in test mode

    Examples:
        >>> # Test dynamic tables
        >>> from sqlalchemy import create_engine
        >>> class BaseMixin(object):
        ...    id = Column(Integer, primary_key=True)
        ...    value = Column(Integer)
        ...
        >>> meta = MetaData()
        >>> kwargs = {'KEY': 'kind', 'ROW_LIMIT': 4, 'mixin': BaseMixin}
        >>> f = lambda x: {'kind': 'odd' if x % 2 else 'even', 'value': x}
        >>> gen_data = lambda **x: map(f, range(15))
        >>> engine = create_engine('sqlite:///:memory:')
        >>> populate(gen_data, engine, **kwargs)
        >>> session = sessionmaker(engine)()
        >>> meta.reflect(engine)
        >>> tables = meta.sorted_tables
        >>> dict(session.query(tables[0]).all()) == {1: 0, 2: 2, 3: 4, 4: 6}
        True
        >>> dict(session.query(tables[1]).all()) == {1: 1, 2: 3, 3: 5, 4: 7}
        True
        >>> meta.drop_all(engine)
        >>>
        >>> # Test tables without specifying the `rid`
        >>> Base = declarative_base()
        >>> class Single(Base):
        ...     __tablename__ = 'single'
        ...     id = Column(Integer, primary_key=True)
        ...     rid = Column(Integer)
        ...     value = Column(Integer)
        ...
        >>> class Triple(Base):
        ...     __tablename__ = 'triple'
        ...     id = Column(Integer, primary_key=True)
        ...     rid = Column(Integer)
        ...     value = Column(Integer)
        ...
        >>> options = [
        ...     {'mul': 1, 'name': 'single'}, {'mul': 3, 'name': 'triple'}]
        >>> kwargs = {'TABLES': options, 'ROW_LIMIT': 4}
        >>> def gen_data(**x):
        ...     return ({'value': n * x['mul'], 'rid': n} for n in it.count())
        >>> Base.metadata.create_all(engine)
        >>> populate(gen_data, engine, **kwargs)
        >>> Base.metadata.reflect(engine)
        >>> tables = Base.metadata.sorted_tables
        >>> session.query(tables[0]).all()
        [(1, 0, 0), (2, 1, 1), (3, 2, 2), (4, 3, 3)]
        >>> session.query(tables[1]).all()
        [(1, 0, 0), (2, 1, 3), (3, 2, 6), (4, 3, 9)]
        >>>
        >>> # Test tables with a specified `rid`
        >>> populate(gen_data, engine, rid='rid', **kwargs)
        >>> Base.metadata.reflect(engine)
        >>> tables = Base.metadata.sorted_tables
        >>> session.query(tables[0]).all()
        [(1, 0, 0), (2, 1, 1), (3, 2, 2), (4, 3, 3)]
        >>> session.query(tables[1]).all()
        [(1, 0, 0), (2, 1, 3), (3, 2, 6), (4, 3, 9)]

    Returns
        str: The message
    """
    log_level = logging.DEBUG if kwargs.get('DEBUG') else logging.INFO
    logger.setLevel(log_level)
    console_handler = logging.StreamHandler()
    logger.addHandler(console_handler)
    test = kwargs.get('TESTING')
    row_limit = kwargs.get('ROW_LIMIT')
    tables = kwargs.get('TABLES')
    chunk_size = min(row_limit or 'inf', kwargs.get('CHUNK_SIZE', row_limit))
    engine.session = sessionmaker(engine)()
    dynamic = not tables

    if test:
        meta.create_all(engine)

    if dynamic:
        data = gen_data(**kwargs)
        tables = get_tables(data, kwargs['KEY'])
        result_func = partial(get_dynamic_res, engine, get_name, **kwargs)
    elif models:
        result_func = partial(res_from_models, models, **kwargs)
    else:
        result_func = partial(res_from_meta, engine, **kwargs)

    for t in tables:
        count = 0
        data = data if dynamic else gen_data(**pr.merge([kwargs, t]))
        result = result_func(t, data=data)
        table, rid, data = result['table'], result['rid'], result['data']
        table.name = table.__table__.name
        table.query = engine.session.query(table)
        del_count = delete_records(table, rid, engine)

        if del_count:
            logger.debug(get_message(del_count, table.name))

        for records in ft.chunk(data, chunk_size):
            del_count, in_count = execute(records, engine, table, rid)
            count += in_count

            if del_count:
                logger.debug(get_message(del_count, table.name))

            logger.debug(get_message(in_count, table.name, False))

            if test:
                pprint(records)

            if row_limit and count >= row_limit:
                break

        logger.debug('Success! %s' % get_message(count, table.name, False))
