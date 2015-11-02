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

import schedule as sch
import scraperwiki

from os import environ
from email.mime.text import MIMEText
from testfixtures import LogCapture

__version__ = '0.6.0'

__title__ = 'swutils'
__author__ = 'Reuben Cummings'
__description__ = 'ScraperWiki box utility library'
__email__ = 'reubano@gmail.com'
__license__ = 'MIT'
__copyright__ = 'Copyright 2015 Reuben Cummings'

SCHEDULE_TIME = '10:30'


class ExceptionHandler(object):
    """Creates a logging exception handler with email notifications

    Note: the following doctests assume you have a running Postfix server
    https://www.garron.me/en/mac/postfix-relay-gmail-mac-os-x-local-smtp.html
    """
    def __init__(self, to, logfile='log.txt', logname=''):
        """ ExceptionHandler constructor

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

    def email(self, subject=None, text=None, host='localhost'):
        """ Sends the email notification

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

        # Send the message via our own SMTP server, but don't include the
        # envelope header.
        s = smtplib.SMTP(host)
        s.sendmail(msg['From'], [msg['To']], msg.as_string())
        s.quit()
        return s

    def handler(self, func):
        """ Creates the exception handler

        Args:
            func (func): The func to catch exceptions from

        Examples:
            >>> import os
            >>> from tempfile import NamedTemporaryFile
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
    """ Runs a job and optionally schedules it to run later

    Args:
        job (func): The func to run
        schedule (bool): Schedule `func` to run in the future (default: False)
        exception_handler (func): The exception handler to wrap the function in
            (default: None)

    Examples:
        >>> from pprint import pprint
        >>> from functools import partial
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
