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

import click, six

import ckan.plugins.toolkit as toolkit
from ckan.model import package_table, Session
import ckanext.opendquality.quality as quality_lib
# from ckanext.opendquality.quality import (
#     Completeness,
#     DataQualityMetrics
# )
import ckanext.opendquality.quality as quality_lib
# from ckanext.opendquality.lib.quality import (
#     # Accuracy,
#     # Uniqueness,
#     # Validity,
#     # Timeliness,
#     # Consistency,
#     Completeness,
#     DataQualityMetrics
# )
from logging import getLogger


log = getLogger(__name__)

@click.group('quality')
def quality():
    pass


@quality.command(u'calculate', help='Calculate data quality metrics')
@click.option('--dataset',
              default='all',
              help='Calculate quality metrics for dataset.')
@click.option('--dimension',
              default='all',
              help='Which metric to calculate.')
def calculate(dataset, dimension):
    if six.PY2:
        _register_mock_translator()
    # dimensions = ['completeness',
    #             #   'uniqueness',
    #             #   'timeliness',
    #             #   'validity',
    #             #   'accuracy',
    #             #   'consistency'
    #               ]
       # dimension_calculators = {
    #     'completeness': Completeness(),
    #     # 'uniqueness': Uniqueness(),
    #     # 'timeliness': Timeliness(),
    #     # 'validity': Validity(),
    #     # 'accuracy': Accuracy(),
    #     # 'consistency': Consistency(),
    # }
    dimensions =  ['completeness','uniqueness','validity','consistency','openness','downloadable','access_api','machine_readable','timeliness']
    dimension_calculators = {
        'completeness': quality_lib.Completeness(),
        'uniqueness'  : quality_lib.Uniqueness(),
        'validity'    : quality_lib.Validity(),
        'consistency' : quality_lib.Consistency(),
        'openness'    : quality_lib.Openness(),
        'downloadable' : quality_lib.Downloadable(),
        'access_api' : quality_lib.AccessAPI(),
        'machine_readable' : quality_lib.MachineReadable(),
        'timeliness': quality_lib.Timeliness()
    }
 

    if dimension == 'all':
        calculators = [dimension_calculators[dim] for dim in dimensions]
    elif dimension not in dimensions:
        raise Exception('Invalid dimension specified. Valid dimensions are: ' +
                        ', '.join(dimensions))
    else:
        calculators = [dimension_calculators[dimension]]

    metrics = quality_lib.DataQualityMetrics(metrics=calculators)

    if dataset == 'all':

        def _process_batch(packages):
            for pkg in packages:
                try:
                    metrics.calculate_metrics_for_dataset(pkg)
                except Exception as e:
                    log.error('Failed to calculate metrics for %s. Error: %s',
                              pkg, str(e))
                    log.exception(e)

        all_packages(_process_batch)

    else:
        metrics.calculate_metrics_for_dataset(dataset)


def _register_mock_translator():
    # Workaround until the core translation function defaults to the Flask one
    from paste.registry import Registry
    from ckan.lib.cli import MockTranslator
    registry = Registry()
    registry.prepare()
    from pylons import translator
    registry.register(translator, MockTranslator())


def all_packages(handler):
    offset = 0
    limit = 64
    while True:
        log.debug('Fetching dataset batch %d to %d', offset, offset+limit)
        query = Session.query(package_table.c.id)
        query = query.offset(offset).limit(limit)

        count = 0
        packages = []
        for result in query.all():
            packages.append(result[0])
            count += 1

        if not count:
            log.debug('No more packages to process.')
            break

        offset += limit

        try:
            log.debug('Processing %d packages in current batch.', count)
            handler(packages)
        except Exception as e:
            log.error('Failed to process package batch. Error: %s', str(e))
            log.exception(e)