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

__version__ = '0.5.0'

__title__ = 'swutils'
__author__ = 'Reuben Cummings'
__description__ = 'ScraperWiki box utility library'
__email__ = 'reubano@gmail.com'
__license__ = 'MIT'
__copyright__ = 'Copyright 2015 Reuben Cummings'

SCHEDULE_TIME = '10:30'


class ExceptionHandler(object):
    def __init__(self, recipient, logfile='log.txt', project=''):
        logging.basicConfig(filename=logfile, level=logging.DEBUG)
        self.recipient = recipient
        self.logfile = logfile
        self.logger = logging.getLogger(project)

    def handler(self, func):
        def wrapper(*args, **kwargs):
            try:
                func(*args, **kwargs)
            except Exception as e:
                self.logger.exception(str(e))
                scraperwiki.status('error', 'Error collecting data')

                with open(self.logfile, 'rb') as f:
                    send_email(self.recipient, text=f.read())
            else:
                scraperwiki.status('ok')

        return wrapper


def send_email(to, subject=None, text=None, host='localhost'):
    user = environ.get('USER')
    from_ = '%s@scraperwiki.com' % user
    subject = subject or 'scraperwiki box %s failed' % user
    source = 'https://scraperwiki.com/dataset/%s\n\n' % user
    body = source + text
    msg = MIMEText(body)
    msg['Subject'], msg['From'], msg['To'] = subject, from_, to

    # Send the message via our own SMTP server, but don't include the envelope
    # header.
    s = smtplib.SMTP(host)
    s.sendmail(from_, [to], msg.as_string())
    s.quit()


def run_or_schedule(job, schedule=False, exception_handler=None):
    if exception_handler and schedule:
        job = exception_handler(job)

    job()

    if schedule:
        sch.every(1).day.at(SCHEDULE_TIME).do(job)

        while True:
            sch.run_pending()
            time.sleep(1)
