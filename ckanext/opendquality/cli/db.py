# encoding: utf-8

"""
Copyright (c) 2018 Keitaro AB

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
"""

import click
import logging

from ckanext.opendquality.cli import error_shout
from ckanext.opendquality.model.data_quality import setup as data_quality_setup

log = logging.getLogger(__name__)


@click.group()
def opendquality():
    pass


@opendquality.command(u'init', short_help=u'Initialize opendquality tables')
def init():
    init_db()


def init_db():
    u'''Initialising the Opendquality tables'''
    log.info(u"Initialize Opendquality tables")
    try:
        data_quality_setup()

    except Exception as e:
        error_shout(e)
    else:
        click.secho(
            u'Initialising Opendquality tables: SUCCESS',
            fg=u'green',
            bold=True)

def get_commands():
    return [opendquality]