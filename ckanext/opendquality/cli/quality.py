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
from ckanext.opendquality.model import JobDQ as job_table
import uuid
from datetime import datetime, date, timedelta
import requests
from ckan.plugins.toolkit import config


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
            # pkg = Session.query(package_table.c.id).filter(package_table.c.name == dataset, package_table.c.type == 'dataset', package_table.c.private == False,package_table.c.state == 'active').first()
            # metrics.calculate_metrics_for_dataset(pkg[0])
            #-------------------------------
            # หา dataset id
            pkg = Session.query(package_table.c.id, package_table.c.name).filter(
                package_table.c.name == dataset,
                package_table.c.type == 'dataset',
                package_table.c.private == False,
                package_table.c.state == 'active'
            ).first()

            if not pkg:
                log.error(f"Dataset '{dataset}' not found or inactive.")
                raise ValueError(f"Dataset '{dataset}' not found or inactive.")

            dataset_id, dataset_name = pkg

            # 1. ปิด job เก่า (active=False)
            old_jobs = Session.query(job_table).filter(
                job_table.org_id == None,     # แยกกรณี dataset job
                job_table.org_name == dataset_name,
                job_table.active == True
            ).all()

            for old_job in old_jobs:
                old_job.active = False
            Session.commit()
            if old_jobs:
                log.info("Deactivated %s previous active job(s) for dataset %s", len(old_jobs), dataset_name)

            # 2. ตรวจสอบ job ของวันนี้ → ถ้ามี → ลบ metrics + job
            # today = date.today()
            # today_jobs = Session.query(job_table).filter(
            #     job_table.org_name == dataset_name,
            #     job_table.requested_timestamp >= datetime(today.year, today.month, today.day)
            # ).all()
            
            # if today_jobs:
            #     for job in today_jobs:
            #         Session.query(qa_table).filter(
            #             qa_table.job_id == job.job_id
            #         ).delete(synchronize_session='fetch')
            #         Session.delete(job)
            #     Session.commit()
            #     log.info("Deleted %s old job(s) and metrics for dataset %s today",
            #              len(today_jobs), dataset_name)
            today = date.today()
            start_of_day = datetime(today.year, today.month, today.day)
            end_of_day = start_of_day + timedelta(days=1)

            today_jobs = Session.query(job_table).filter(
                job_table.org_name == dataset_name,
                job_table.requested_timestamp >= start_of_day,
                job_table.requested_timestamp < end_of_day
            ).all()
            log.debug('--today_jobs--')
            log.debug(today_jobs)
            if today_jobs:
                for job in today_jobs:
                    # ลบ metrics ทั้งหมดที่ผูกกับ job_id นี้
                    Session.query(qa_table).filter(
                        qa_table.job_id == job.job_id
                    ).delete(synchronize_session='fetch')

                    # แล้วค่อยลบ job ตัวนั้น
                    Session.delete(job)
                    Session.commit()
                    log.info("Deleted %s old job(s) and metrics for dataset %s today",
                            len(today_jobs), dataset_name)

            # 3. สร้าง job ใหม่
            job_id = str(uuid.uuid4())
            job = job_table(
                job_id=job_id,
                org_id=None,
                org_name=dataset_name,
                status="pending",
                requested_timestamp=date.today(),
                run_type='dataset',
                active=True
            )
            Session.add(job)
            Session.commit()

            try:
                # เปลี่ยนเป็น running
                job.status = "running"
                job.started_timestamp = date.today()
                Session.commit()

                # run metric calculation
                metrics.calculate_metrics_for_dataset(dataset_id, job_id=job_id)

                # mark finish
                job.status = "finish"
                job.finish_timestamp = date.today()
                job.active = True
                Session.commit()

            except Exception as e:
                log.error("Job %s for dataset %s failed. Error: %s", job_id, dataset_name, str(e))
                log.exception(e)
                job.status = "fail"
                job.finish_timestamp = date.today()
                Session.commit()

    #------------------------------
    if organization:  
        log.debug('-------organization--------')  
        log.debug(organization)     
        if organization == 'all':
            # def _process_batch(packages):
            #     for pkg in packages:
            #         try:
            #             metrics.calculate_metrics_for_dataset(pkg)
            #         except Exception as e:
            #             log.error('Failed to calculate metrics for %s. Error: %s',
            #                     pkg, str(e))
            #             log.exception(e)

            # all_packages(_process_batch)
            #---------------------------------------------
            all_orgs = get_all_organizations()  # ต้องมีฟังก์ชันคืนชื่อหรือ id ของทุก org
            for org_name in all_orgs:
                try:
                    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    log.info(f"Processing organization: {org_name}: date_time_start[{timestamp}]")

                    parent_org_id, parent_org_name = get_parent_organization(org_name)
                    org_id = get_org_id_from_name(org_name)

                    if not org_id:
                        log.error(f"Organization '{org_name}' not found in CKAN.")
                        continue

                    # 1. ปิด job เดิม
                    old_all_jobs = Session.query(job_table).filter(
                        job_table.org_id == org_id,
                        job_table.active == True
                    ).all()
                    for old_job in old_all_jobs:
                        old_job.active = False
                    Session.commit()

                    # 2. ลบ job เดิมของวันนี้
                    today = date.today()
                    today_jobs = Session.query(job_table).filter(
                        job_table.org_id == org_id,
                        job_table.requested_timestamp >= datetime(today.year, today.month, today.day)
                    ).all()

                    if today_jobs:
                        for job in today_jobs:
                            Session.query(qa_table).filter(
                                qa_table.job_id == job.job_id
                            ).delete(synchronize_session='fetch')
                            Session.delete(job)
                        Session.commit()

                    # 3. สร้าง job ใหม่
                    job_id = str(uuid.uuid4())
                    job = job_table(
                        job_id=job_id,
                        org_parent_id=parent_org_id,
                        org_parent_name=parent_org_name,
                        org_id=org_id,
                        org_name=org_name,
                        status="pending",
                        requested_timestamp=date.today(),
                        run_type='organization',
                        active=True
                    )
                    Session.add(job)
                    Session.commit()

                    # 4. รัน metrics
                    job.status = "running"
                    job.started_timestamp = date.today()
                    Session.commit()

                    def _process_batch(packages):
                        for pkg in packages:
                            try:
                                metrics.calculate_metrics_for_dataset(pkg, job_id=job_id)
                            except Exception as e:
                                log.error(f'Job failed for {org_name}: {str(e)}')
                                Session.rollback()
                                job.status = "fail"
                                job.finish_timestamp = date.today()
                                Session.commit()

                    org_packages(_process_batch, org_name, job)

                    # 5. ปิดงาน
                    job.status = "finish"
                    job.finish_timestamp = date.today()
                    job.active = True

                    timestamp_end = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    log.info(f"Finished organization: {org_name}: date_time_end[{timestamp_end}]")
                    Session.commit()

                except Exception as e:
                    log.error(f"Job failed for {org_name}. Error: {e}")
                    Session.rollback()
                    if 'job' in locals():
                        job.status = "fail"
                        job.finish_timestamp = date.today()
                        Session.commit()

        else:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            log.info(f"Processing organization: {organization}: date_time_start[{timestamp}]")

            parent_org_id, parent_org_name = get_parent_organization(organization)
            org_id = get_org_id_from_name(organization)      
            # 1. ตรวจสอบ org_id
            if not org_id:
                log.error(f"Organization '{organization}' not found in CKAN.")
                raise ValueError(f"Organization '{organization}' not found in CKAN.")

            # 2. ปิด job เก่าทั้งหมดของ org (active=False)
            old_all_jobs = Session.query(job_table).filter(
                job_table.org_id == org_id,
                job_table.active == True
            ).all()

            for old_job in old_all_jobs:
                old_job.active = False
            Session.commit()
            if old_all_jobs:
                log.info("Deactivated %s previous active job(s) for organization %s", len(old_all_jobs), organization)

            # 3. ตรวจสอบ job ของวันนี้ → ถ้ามี → ลบ metrics + job
            today = date.today()
            today_jobs = Session.query(job_table).filter(
                job_table.org_id == org_id,
                job_table.requested_timestamp >= datetime(today.year, today.month, today.day)
            ).all()

            if today_jobs:
                for job in today_jobs:
                    # ลบ metrics ของ job เก่า
                    Session.query(qa_table).filter(
                        qa_table.job_id == job.job_id
                    ).delete(synchronize_session='fetch')

                    # ลบ job เอง
                    Session.delete(job)

                Session.commit()
                log.info("Deleted %s old job(s) and metrics for organization %s today",
                        len(today_jobs), organization)
            
            # 4. สร้าง job ใหม่
            job_id = str(uuid.uuid4())
            job = job_table(
                job_id=job_id,
                org_parent_id=parent_org_id,
                org_parent_name=parent_org_name,
                org_id=org_id,
                org_name=organization,
                status="pending",
                requested_timestamp=date.today(),
                run_type='organization',
                active=True
            )
            Session.add(job)
            Session.commit()
            try:
                # เปลี่ยนเป็น running
                job.status = "running"
                job.started_timestamp = date.today()
                Session.commit()
                def _process_batch(packages):
                    for pkg in packages:
                        try:
                            metrics.calculate_metrics_for_dataset(pkg,job_id=job_id)
                        except Exception as e:                     
                            log.error('Job failed at Cli: Failed to calculate metrics')
                            # log.error('Failed to calculate metrics for %s. Error: %s',
                            #         pkg, str(e))
                            Session.rollback()   # เคลียร์ transaction ที่ error ไปแล้ว
                            job.status = "fail"
                            job.finish_timestamp = date.today()
                            Session.commit()        
                
                org_packages(_process_batch, organization,job)

                # จบงาน → mark finish
                job.status = "finish"
                job.finish_timestamp = date.today()
                job.activate = True

                timestamp_end = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                log.info(f"Finished organization: {organization}: date_time_end[{timestamp_end}]")
                Session.commit()
            except Exception as e:
                log.error('Job failed at Cli: Failed to calculate metrics')
                # log.error("Job %s failed. Error: %s", job_id, str(e))
                Session.rollback()   # เคลียร์ transaction ที่ error ไปแล้ว
                job.status = "fail"
                job.finish_timestamp = date.today()
                Session.commit()
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

def org_packages(handler,org_name,job):
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
            job.status = "fail"
            job.finish_timestamp = date.today(),
            Session.commit()
            log.error('Job failed at Cli-org package')
            log.error('Failed to process package batch. Error: %s', str(e))
            log.exception(e)

@quality.command(u'delete', help='Delete data quality metrics')
@click.option('--dataset',
              help='Delete a dataset by name. Use "all" to delete all datasets.')
@click.option('--organization',
              default=None,
              help='Delete quality metrics by organization')
@click.option('--job_id',
              default=None,
              help='Delete quality metrics by job id')
def del_metrict(organization=None, dataset=None, job_id=None):
    if dataset:
        if dataset == 'all':
            Session.query(qa_table).delete()
            Session.commit()
            log.info("Deleted all data quality metrics")
        else:
            pkg = Session.query(package_table.c.id).filter(
                package_table.c.name == dataset,
                package_table.c.type == 'dataset'
            ).first()

            if not pkg:
                log.error("Dataset %s not found", dataset)
                return

            list_ref = [pkg[0]]

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
            Session.query(qa_table).delete()
            Session.commit()
            log.info("Deleted all data quality metrics")
        else:
            org = model.Group.get(organization)
            pkg = Session.query(package_table.c.id).filter(
                package_table.c.owner_org == org.id
            )

            list_ref = [row[0] for row in pkg.all()]
            for result in pkg.all():
                res = Session.query(resource_table.c.id).filter(
                    resource_table.c.package_id == result[0]
                ).all()
                list_ref += [rw[0] for rw in res]

            Session.query(qa_table).filter(
                qa_table.ref_id.in_(list_ref)
            ).delete(synchronize_session='fetch')
            Session.commit()
            log.info("Deleted data quality metrics for organization %s", organization)

    elif job_id:
        # ตรวจสอบว่า job มีอยู่จริงไหม
        job_to_delete = Session.query(job_table).filter(job_table.job_id == job_id).first()

        if not job_to_delete:
            log.warning("No job found for job_id %s", job_id)
            return

        org_id = job_to_delete.org_id
        org_name = job_to_delete.org_name
        was_active = job_to_delete.active

        # ลบ metrics ก่อน
        deleted_metrics = Session.query(qa_table).filter(
            qa_table.job_id == job_id
        ).delete(synchronize_session='fetch')

        # ลบ job
        Session.delete(job_to_delete)
        Session.commit()
        log.info("Deleted job %s and %s metrics", job_id, deleted_metrics)

        # ถ้า job ที่ลบเป็น active ให้หางานก่อนหน้า (ของ org เดียวกัน) ที่จบแล้ว (finish)
        if was_active:
            last_finished_job = (
                Session.query(job_table)
                .filter(
                    job_table.org_id == org_id,
                    job_table.status == 'finish'   # ต้องเป็นงานที่จบแล้วเท่านั้น
                )
                .order_by(job_table.requested_timestamp.desc())
                .first()
            )

            if last_finished_job:
                last_finished_job.active = True
                Session.commit()
                log.info(
                    "Set previous finished job (%s) as active for organization %s",
                    last_finished_job.job_id,
                    org_name
                )
            else:
                log.info(
                    "No finished job found to set active for organization %s",
                    org_name
                )
    #-- ok version---
    # elif job_id:
    #     deleted_metrics = Session.query(qa_table).filter(
    #         qa_table.job_id == job_id
    #     ).delete(synchronize_session='fetch')

    #     # ลบ job ด้วย
    #     deleted_jobs = Session.query(job_table).filter(
    #         job_table.job_id == job_id
    #     ).delete(synchronize_session='fetch')

    #     Session.commit()
    #     if deleted_metrics or deleted_jobs:
    #         log.info("Deleted %s metrics and %s jobs for job_id %s",
    #                 deleted_metrics, deleted_jobs, job_id)
    #     else:
    #         log.warning("No data quality metrics or jobs found for job_id %s", job_id)

    else:
        log.error("Please provide either --dataset, --organization, or --job_id")
# @quality.command(u'delete', help='Delete data quality metrics')
# @click.option('--dataset',
#               help='Delete a dataset by name. Use "all" to delete all datasets.')
# @click.option('--organization',
#               default= None,
#               help='Delete quality metrics by organization')
# def del_metrict(organization=None, dataset=None):
#     if dataset:
#         if dataset == 'all':
#             # ลบทุกแถวในตาราง metrics
#             Session.query(qa_table).delete()
#             Session.commit()
#             log.info("Deleted all data quality metrics")
#         else:
#             # หา dataset ที่ตรงกับชื่อ
#             pkg = Session.query(package_table.c.id).filter(
#                 package_table.c.name == dataset,
#                 package_table.c.type == 'dataset'
#             ).first()

#             if not pkg:
#                 log.error("Dataset %s not found", dataset)
#                 return

#             list_ref = [pkg[0]]

#             # เพิ่ม resource ที่อยู่ใน dataset นี้ด้วย
#             res = Session.query(resource_table.c.id).filter(
#                 resource_table.c.package_id == pkg[0]
#             ).all()
#             list_ref += [rw[0] for rw in res]

#             Session.query(qa_table).filter(
#                 qa_table.ref_id.in_(list_ref)
#             ).delete(synchronize_session='fetch')
#             Session.commit()
#             log.info("Deleted data quality metrics for dataset %s", dataset)

#     elif organization:
#         if organization == 'all':
#             obj = Session.query(qa_table).delete()
#             Session.commit()
#         else:
#             org = model.Group.get(organization)
#             pkg = Session.query(package_table.c.id).filter(package_table.c.owner_org == org.id)
            
#             list_ref = [ row[0] for row in pkg.all()]
#             for result in pkg.all():
#                 res = Session.query(resource_table.c.id).filter(resource_table.c.package_id == result[0]).all()
#                 list_ref += [ rw[0] for rw in res]
#             qa = Session.query(qa_table).filter(qa_table.ref_id.in_(list_ref)).delete(synchronize_session='fetch')
#             Session.commit()
#             log.info("Deleted data quality metrics for organization %s", organization)
#     else:
#         log.error("Please provide either --dataset or --organization")
def get_all_organizations():
    """ดึงรายชื่อ organization ทั้งหมดจากฐานข้อมูล CKAN"""
    orgs = model.Session.query(model.Group).filter(model.Group.type == 'organization').all()
    return [org.name for org in orgs]
    
def get_parent_organization(org_id):
    # url = f"https://data.go.th/api/3/action/group_tree_section?type=organization&id={org_id}"
    # url = f"https://ckan-dev.opend.cloud/api/3/action/group_tree_section?type=organization&id={org_id}"
    base_url = config.get("ckan.site_url")
    url = f"{base_url}/api/3/action/group_tree_section?type=organization&id={org_id}"
    log.debug('base_url')
    log.debug(url)
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