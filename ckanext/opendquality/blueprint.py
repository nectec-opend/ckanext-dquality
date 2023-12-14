# encoding: utf-8

from flask import Blueprint
import ckan.plugins.toolkit as toolkit 
# from ckan.common import config
import ckanext.opendquality.quality as quality_lib
# from ckanext.myorg import helpers as myh
# from ckanext.opendquality.quality import (
#     Completeness,
#     DataQualityMetrics
# )
qa = Blueprint('quality', __name__)
dquality = quality_lib.OpendQuality()
# metrics  = quality_lib.DataQualityMetrics()#metrics=calculators

#---------------------Call calculate---------------------------------
#--------------------------------------------------------------------
def calculate(dataset, dimension):
    # _register_mock_translator()
    # dimensions = ['completeness','uniqueness','timeliness','validity','accuracy','consistency']
    dimensions = ['completeness','uniqueness','validity','consistency','openness','downloadable']
    dimension_calculators = {
        'completeness': quality_lib.Completeness(),
        'uniqueness'  : quality_lib.Uniqueness(),
        # 'timeliness'  : quality_lib.Timeliness(),
        'validity'    : quality_lib.Validity(),
        # 'accuracy'    : quality_lib.Accuracy(),
        'consistency' : quality_lib.Consistency(),
        'openness' : quality_lib.Openness(),
        'downloadable' : quality_lib.Downloadable()
    }

    if dimension == 'all':
        calculators = [dimension_calculators[dim] for dim in dimensions]
    elif dimension not in dimensions:
        raise Exception('Invalid dimension specified. Valid dimensions are: ' +
                        ', '.join(dimensions))
    else:
        calculators = [dimension_calculators[dimension]]
    # self.logger.debug('----Calculators---')
    # self.logger.debug(calculators)
    the_metrics = quality_lib.DataQualityMetrics(metrics=calculators)

    if dataset == 'all':

        def _process_batch(packages):
            for pkg in packages:
                try:
                    the_metrics.calculate_metrics_for_dataset(pkg)
                    # self.calculate_metrics_for_dataset(pkg)
                except Exception as e:
                    log.error('Failed to calculate metrics for %s. Error: %s',
                            pkg, str(e))
                    log.exception(e)

        all_packages(_process_batch)

    else:
        the_metrics.calculate_metrics_for_dataset(dataset)
        # self.calculate_metrics_for_dataset(dataset)


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
#-----------------------------------------------------
def home():
    return {'msg': 'hello world quality'}

def completeness():
    return {'msg': 'completeness score',
            'score': dquality.get_last_modified_datasets(),
            'metric': calculate('bird','all')
            # metrics.calculate('bird','completeness')
            #metrics.calculate_metrics_for_dataset('bird')  
    }

# def top_package_owners(limit=100, page=1):
#     return {
#         u'opendstats_data': stats.top_package_owners(),
#         u'opendstats_page': 'top_package_owners'
#     }

qa.add_url_rule('/quality', view_func=home)
qa.add_url_rule('/completeness', view_func=completeness)