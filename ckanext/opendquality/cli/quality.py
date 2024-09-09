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
from ckan.model import package_table, resource_table, Session
import ckanext.opendquality.quality as quality_lib
from logging import getLogger
import ckan.model as model
from ckanext.opendquality.model import DataQualityMetrics as qa_table

log = getLogger(__name__)

@click.group('quality')
def quality():
    pass

@quality.command(u'calculate', help='Calculate data quality metrics')
@click.option('--dataset',
              help='Calculate quality metrics for dataset by datasett name.')
@click.option('--organization',
              help='Calculate quality metrics for dataset by organization name.')
@click.option('--dimension',
              default='all',
              help='Which metric to calculate.')
# def calculate(dataset, dimension):
def calculate(organization=None, dataset=None,dimension='all'):
    if six.PY2:
        _register_mock_translator()
    # dimensions =  ['completeness','uniqueness','validity','consistency','openness','downloadable','access_api','machine_readable','timeliness']
    dimensions =  ['validity','consistency','openness','downloadable','access_api','machine_readable','timeliness']
    dimension_calculators = {
        # 'completeness': quality_lib.Completeness(),
        # 'uniqueness'  : quality_lib.Uniqueness(),
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
    
    if dataset:
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
            pkg = Session.query(package_table.c.id).filter(package_table.c.name == dataset, package_table.c.type == 'dataset', package_table.c.private == False).first()
            metrics.calculate_metrics_for_dataset(pkg[0])

    #------------------------------
    if organization:  
        log.debug('-------organization--------')  
        log.debug(organization)     
        if organization == 'all':
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
            def _process_batch(packages):
                for pkg in packages:
                    try:
                        metrics.calculate_metrics_for_dataset(pkg)
                    except Exception as e:
                        log.error('Failed to calculate metrics for %s. Error: %s',
                                pkg, str(e))
                        log.exception(e)
            
            org_packages(_process_batch, organization)

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
        query = Session.query(package_table.c.id).filter(package_table.c.type == 'dataset', package_table.c.private == False)
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

def org_packages(handler,org_name):
    offset = 0
    limit = 64
    while True:
        log.debug('Fetching dataset batch %d to %d', offset, offset+limit)
        group = model.Group.get(org_name)
        query = Session.query(package_table.c.id).filter(package_table.c.owner_org == group.id, package_table.c.type == 'dataset', package_table.c.private == False)
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


@quality.command(u'delete', help='Delete data quality metrics')
@click.option('--organization',
              default='all',
              help='Delete quality metrics by organization')
def del_metrict(organization):
    if organization == 'all':
        obj = Session.query(qa_table).delete()
        Session.commit()
    else:
        org = model.Group.get(organization)
        pkg = Session.query(package_table.c.id).filter(package_table.c.owner_org == org.id)
        
        list_ref = [ row[0] for row in pkg.all()]
        for result in pkg.all():
            res = Session.query(resource_table.c.id).filter(resource_table.c.package_id == result[0]).all()
            list_ref += [ rw[0] for rw in res]
        qa = Session.query(qa_table).filter(qa_table.ref_id.in_(list_ref)).delete(synchronize_session='fetch')
        Session.commit()
