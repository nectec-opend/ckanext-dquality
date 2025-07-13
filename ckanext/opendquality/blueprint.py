# encoding: utf-8

from flask import Blueprint, request
import ckan.plugins.toolkit as toolkit 
from logging import getLogger
# from ckan.common import config
from ckan.model import package_table, Session, Package, Group
from ckanext.opendquality.model import DataQualityMetrics as DQM
import ckanext.opendquality.quality as quality_lib
import ckan.lib.helpers as h
# from ckanext.myorg import helpers as myh
# from ckanext.opendquality.quality import (
#     Completeness,
#     DataQualityMetrics
# )
log = getLogger(__name__)
qa = Blueprint('opendquality', __name__, url_prefix="/qa")
dquality = quality_lib.OpendQuality()
# metrics  = quality_lib.DataQualityMetrics()#metrics=calculators
EXEMPT_ENDPOINTS = {
    'opendquality.index',
    'opendquality.admin_report',
    'opendquality.dashboard',
}


@qa.before_request
def request_before():
    if request.endpoint in EXEMPT_ENDPOINTS:
        return
    user = getattr(toolkit.c, 'userobj', None)
    if not user or not getattr(user, 'is_sysadmin', False):
        toolkit.abort(403, toolkit._('You do not have permission to access this page.'))

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
        'openness'    : quality_lib.Openness(),
        'downloadable' : quality_lib.Downloadable(),
        'machine_readable' : quality_lib.MachineReadable()
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
    the_metrics  = quality_lib.DataQualityMetrics(metrics=calculators)
    # the_metrics = resource.DataQualityMetrics(metrics=calculators)

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
    extra_vars = {
        'title': toolkit._('Open Data Quality index'),
        'home': True,
        'user': toolkit.c.user,
        'userobj': toolkit.c.userobj,
        'site_url': toolkit.request.host_url,
        'site_title': toolkit.config.get('ckan.site_title'),
        'ckan_version': toolkit.config.get('ckan.version'),
        'ckanext_opendquality_version': toolkit.config.get('ckanext-opendquality.version'),
        # 'external_dashboard': external_stats
    }
    return toolkit.render('ckanext/opendquality/index.html', extra_vars)
    # return {'msg': 'hello world quality'}

def admin_report():
    data_quality = Session.query(
        Package.title.label('package_title'),
        Package.id.label('package_id'),
        Group.title.label('org_title'),
        Group.id.label('org_id'),
        DQM.openness,
        DQM.timeliness,
        DQM.acc_latency,
        DQM.freshness,
        DQM.availability,
        DQM.downloadable,
        DQM.access_api,
        DQM.relevance,
        DQM.utf8,
        DQM.preview,
        DQM.completeness,
        DQM.uniqueness,
        DQM.validity,
        DQM.consistency,
        DQM.metrics
    ) \
    .join(Package, Package.id == DQM.ref_id)\
    .join(Group, Group.id == Package.owner_org)\
    .filter(
        DQM.type == 'package').order_by(DQM.modified_at.desc()).all()
    extra_vars = {
        'reports': data_quality,
        'title': toolkit._('รายงานคุณภาพชุดข้อมูลเปิดสำหรับ ผู้ดูแลระบบ'),
        'admin_report': True,
        'user': toolkit.c.user,
        'userobj': toolkit.c.userobj,
        'site_url': toolkit.request.host_url,
        'site_title': toolkit.config.get('ckan.site_title'),
        'ckan_version': toolkit.config.get('ckan.version'),
        'ckanext_opendquality_version': toolkit.config.get('ckanext-opendquality.version'),
        # 'external_dashboard': external_stats
    }
    return toolkit.render('ckanext/opendquality/admin_reports.html', extra_vars)

def dashboard():
    extra_vars = {
        'title': toolkit._('Open Data Quality Dashboard'),
        'dashboard': True,
        'user': toolkit.c.user,
        'userobj': toolkit.c.userobj,
        'site_url': toolkit.request.host_url,
        'site_title': toolkit.config.get('ckan.site_title'),
        'ckan_version': toolkit.config.get('ckan.version'),
        'ckanext_opendquality_version': toolkit.config.get('ckanext-opendquality.version'),
        # 'external_dashboard': external_stats
    }
    return toolkit.render('ckanext/opendquality/index.html', extra_vars)

def calculate_quality(): #completeness
    return {'msg': 'calculate quality score',
            'score': dquality.get_last_modified_datasets(),
            'metric': calculate('all','all')
            # 'metric': calculate('bird','all')
            # metrics.calculate('bird','completeness')
            #metrics.calculate_metrics_for_dataset('bird')  
    }

def quality_reports():
    return {'msg': 'quality reports'}
# def top_package_owners(limit=100, page=1):
#     return {
#         u'opendstats_data': stats.top_package_owners(),
#         u'opendstats_page': 'top_package_owners'
#     }

qa.add_url_rule('/', endpoint="index", view_func=home)
qa.add_url_rule('/calculate', endpoint="index", view_func=home)
# qa.add_url_rule('/calculate_quality', view_func=calculate_quality)
# qa.add_url_rule('/reports', endpoint="reports", view_func=quality_reports)
qa.add_url_rule('/admin_report', endpoint="admin_report", view_func=admin_report)
qa.add_url_rule('/dashboard', endpoint="dashboard", view_func=dashboard)