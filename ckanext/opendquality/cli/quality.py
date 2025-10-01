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
from ckanext.opendquality.model import JobDQ
import uuid
from datetime import datetime
import requests


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
    dimensions =  ['completeness','uniqueness','validity','consistency','openness','availability','downloadable','access_api','timeliness','acc_latency','freshness','relevance','utf8','preview']
    dimension_calculators = {
        'completeness': quality_lib.Completeness(),
        'uniqueness'  : quality_lib.Uniqueness(),
        'validity'    : quality_lib.Validity(),
        'consistency' : quality_lib.Consistency(),
        'openness'    : quality_lib.Openness(),
        'availability' : quality_lib.Availability(),
        'downloadable' : quality_lib.Downloadable(),
        'access_api' : quality_lib.AccessAPI(),
        # 'machine_readable' : quality_lib.MachineReadable(),
        'timeliness': quality_lib.Timeliness(),
        'acc_latency': quality_lib.AcceptableLatency(),
        'freshness': quality_lib.Freshness(),
        'relevance': quality_lib.Relevance(),
        'utf8': quality_lib.EncodingUTF8(),
        'preview': quality_lib.Preview(),
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
            pkg = Session.query(package_table.c.id).filter(package_table.c.name == dataset, package_table.c.type == 'dataset', package_table.c.private == False,package_table.c.state == 'active').first()
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
            parent_org_id, parent_org_name = get_parent_organization(organization)
            org_id = get_org_id_from_name(organization)

            if not org_id:
                log.error(f"Organization '{organization}' not found in CKAN.")
                raise ValueError(f"Organization '{organization}' not found in CKAN.")

            job_id = str(uuid.uuid4())
            job = JobDQ(
                job_id=job_id,
                org_parent_id=parent_org_id,
                org_parent_name=parent_org_name,
                org_id=org_id,
                org_name=organization,
                status="pending",
                requested_date=datetime.utcnow(),
                active=True
            )
            Session.add(job)
            Session.commit()
            try:
                # เปลี่ยนเป็น running
                job.status = "running"
                job.started_date = datetime.utcnow()
                Session.commit()
                def _process_batch(packages):
                    for pkg in packages:
                        try:
                            metrics.calculate_metrics_for_dataset(pkg,job_id=job_id)
                        except Exception as e:
                            log.error('Failed to calculate metrics for %s. Error: %s',
                                    pkg, str(e))
                            log.exception(e)
                
                org_packages(_process_batch, organization)

                # จบงาน → mark finish
                job.status = "finish"
                job.finish_date = datetime.utcnow()
                job.activate = True

                Session.commit()
            except Exception as e:
                job.status = "fail"
                job.finish_date = datetime.utcnow()
                Session.commit()
                log.error("Job %s failed. Error: %s", job_id, str(e))
                log.exception(e)
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
        query = Session.query(package_table.c.id).filter(package_table.c.type == 'dataset', package_table.c.private == False, package_table.c.state == 'active')
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
        query = Session.query(package_table.c.id).filter(package_table.c.owner_org == group.id, package_table.c.type == 'dataset', package_table.c.private == False, package_table.c.state == 'active')
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
@click.option('--dataset',
              help='Delete a dataset by name. Use "all" to delete all datasets.')
@click.option('--organization',
              default= None,
              help='Delete quality metrics by organization')
def del_metrict(organization=None, dataset=None):
    if dataset:
        if dataset == 'all':
            # ลบทุกแถวในตาราง metrics
            Session.query(qa_table).delete()
            Session.commit()
            log.info("Deleted all data quality metrics")
        else:
            # หา dataset ที่ตรงกับชื่อ
            pkg = Session.query(package_table.c.id).filter(
                package_table.c.name == dataset,
                package_table.c.type == 'dataset'
            ).first()

            if not pkg:
                log.error("Dataset %s not found", dataset)
                return

            list_ref = [pkg[0]]

            # เพิ่ม resource ที่อยู่ใน dataset นี้ด้วย
            res = Session.query(resource_table.c.id).filter(
                resource_table.c.package_id == pkg[0]
            ).all()
            list_ref += [rw[0] for rw in res]

            Session.query(qa_table).filter(
                qa_table.ref_id.in_(list_ref)
            ).delete(synchronize_session='fetch')
            Session.commit()
            log.info("Deleted data quality metrics for dataset %s", dataset)

    elif organization:
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
            log.info("Deleted data quality metrics for organization %s", organization)
    else:
        log.error("Please provide either --dataset or --organization")
def get_parent_organization(org_id):
    # url = f"https://data.go.th/api/3/action/group_tree_section?type=organization&id={org_id}"
    url = f"https://ckan-dev.opend.cloud/api/3/action/group_tree_section?type=organization&id={org_id}"
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        if not data.get("success"):
            return None, None

        results = data.get("result")

        # --- case 1: result เป็น dict (เช่นที่คุณเจอ) ---
        if isinstance(results, dict):
            parent_id = results.get("id")
            parent_name = results.get("name")
            return parent_id, parent_name

        # --- case 2: result เป็น list ---
        if isinstance(results, list) and results:
            org_node = results[0]
            parent = org_node.get("parent")
            if parent:
                return parent.get("id"), parent.get("name")
            else:
                return None, None

        # ถ้าไม่เข้า case ไหนเลย
        return None, None

    except requests.exceptions.HTTPError as e:
        print(f"HTTP error: {e}")
        return None, None
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None, None

def get_org_id_from_name(org_name):
    org = model.Session.query(model.Group) \
        .filter(model.Group.name == org_name) \
        .filter(model.Group.type == 'organization') \
        .first()
    if org:
        return org.id
    return None