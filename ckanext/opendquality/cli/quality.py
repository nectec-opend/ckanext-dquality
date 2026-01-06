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

import ckan.plugins.toolkit as toolkit, os
from ckan.model import package_table, resource_table, Session
import ckanext.opendquality.quality as quality_lib
from logging import getLogger
import ckan.model as model
from ckanext.opendquality.model import DataQualityMetrics as qa_table
from ckanext.opendquality.model import JobDQ as job_table
import uuid
from datetime import datetime, date, timezone, timedelta
import requests
from ckan.plugins.toolkit import config
import time
import ckan.lib.jobs as jobs
from ckan.plugins.toolkit import get_action
from sqlalchemy import desc

log = getLogger(__name__)
# Asia/Bangkok
tz = timezone(timedelta(hours=7))
def build_metrics(dimension='all'):

    dimensions = [
        'completeness','uniqueness','validity','consistency',
        'openness','availability','downloadable','access_api',
        'timeliness','acc_latency','freshness','relevance','utf8','preview'
    ]

    calculators_map = {
        'completeness': quality_lib.Completeness(),
        'uniqueness':   quality_lib.Uniqueness(),
        'validity':     quality_lib.Validity(),
        'consistency':  quality_lib.Consistency(),
        'openness':     quality_lib.Openness(),
        'availability': quality_lib.Availability(),
        'downloadable': quality_lib.Downloadable(),
        'access_api':   quality_lib.AccessAPI(),
        'timeliness':   quality_lib.Timeliness(),
        'acc_latency':  quality_lib.AcceptableLatency(),
        'freshness':    quality_lib.Freshness(),
        'relevance':    quality_lib.Relevance(),
        'utf8':         quality_lib.EncodingUTF8(),
        'preview':      quality_lib.Preview(),
    }

    if dimension == 'all':
        calculators = [calculators_map[dim] for dim in dimensions]
    elif dimension not in dimensions:
        raise ValueError(f"Invalid dimension '{dimension}'")
    else:
        calculators = [calculators_map[dimension]]

    return quality_lib.DataQualityMetrics(metrics=calculators)
class JobCancelledException(Exception):
    pass

def restore_previous_active_job(cancelled_job_id):
    """
    หา job ที่เคย finish ล่าสุด และตั้งเป็น active=True
    โดยหา org_id จาก cancelled_job_id
    """
    try:
        # หา job ที่ถูก cancel ก่อน เพื่อเอา org_id
        cancelled_job = Session.query(job_table).filter_by(job_id=cancelled_job_id).first()
        
        if not cancelled_job:
            log.error(f"[RESTORE] Cannot find cancelled job {cancelled_job_id}")
            return False
        
        org_id = cancelled_job.org_id
        org_name = cancelled_job.org_name
        
        log.info(f"[RESTORE] Looking for previous finished job for org={org_name} (org_id={org_id})")
        
        # หา job ที่ finish ล่าสุด
        last_finished_job = (
            Session.query(job_table)
            .filter(
                job_table.org_id == org_id,
                job_table.status == 'finish',
                job_table.job_id != cancelled_job_id
            )
            .order_by(job_table.requested_timestamp.desc())
            .first()
        )

        if last_finished_job:
            last_finished_job.active = True
            Session.commit()
            log.info(f"[RESTORE] ✓ Restored job {last_finished_job.job_id} as active for org {org_name}")
            return True
        else:
            log.info(f"[RESTORE] No finished job found for org {org_name}")
            return False
            
    except Exception as e:
        log.error(f"[RESTORE] Error: {e}")
        Session.rollback()
        return False
        
# def should_reprocess_dataset(dataset_id, last_state):
#     """
#     ตรวจสอบว่าควรประมวลผล dataset ใหม่หรือไม่
    
#     Args:
#         dataset_id: package_id ของ dataset
#         last_state: state จาก job ก่อนหน้า
    
#     Returns:
#         tuple: (should_process, reason)

#         qa_table, job_table
#     """
#     try:
#         # from ckan.model import Resource, Package
#         # ดึง package ปัจจุบัน
#         package = Session.query(package_table).filter(
#             package_table.c.id == dataset_id
#         ).first()
        
#         if not package:
#             return (True, "ไม่พบ dataset")

#         # ดึง resources ปัจจุบันของ dataset
#         current_resources = Session.query(resource_table).filter(
#             resource_table.c.package_id == dataset_id,
#             resource_table.c.state == 'active'
#         ).all()
        
#         current_resource_ids = {r.id for r in current_resources}
#         current_metadata = {
#             r.id: {
#                 'last_modified': r.last_modified,
#                 'url': r.url,
#                 'format': r.format,
#             } for r in current_resources
#         }
        
#         # ไม่มี state เก่า
#         if not last_state or not last_state.get('resources'):
#             return (True, "ไม่มี state ก่อนหน้า")

       
        
#         last_resource_ids = set(last_state['resources'].keys())
#         last_metadata = last_state['resources']

#          # ⭐ เพิ่ม: เช็ค metadata_modified ของ package
#         if last_state.get('package_metadata_modified'):
#             if package.metadata_modified > last_state['package_metadata_modified']:
#                 return (True, "Package metadata มีการอัปเดท")


#         # ตรวจสอบว่ามี error ใน state เก่าหรือไม่
#         # if last_state.get('has_error'):
#         #     return (True, "พบ error ใน state ก่อนหน้า")

#         # ตรวจสอบว่ามี Connection timeout ใน state เก่าหรือไม่
#         if last_state.get('has_error'):
#             return (True, "พบ Connection timeout - ลองใหม่")
        
#         # ตรวจสอบการเพิ่ม/ลบไฟล์
#         added_files = current_resource_ids - last_resource_ids
#         deleted_files = last_resource_ids - current_resource_ids
        
#         if added_files:
#             return (True, f"มีไฟล์เพิ่ม {len(added_files)} ไฟล์")
#         if deleted_files:
#             return (True, f"มีไฟล์ลบ {len(deleted_files)} ไฟล์")
        
#         # ตรวจสอบการอัปเดทไฟล์
#         for resource_id in current_resource_ids:
#             current_meta = current_metadata[resource_id]
#             last_meta = last_metadata.get(resource_id, {})
            
#             # เช็ค last_modified
#             if current_meta.get('last_modified') and last_meta.get('last_modified'):
#                 if current_meta['last_modified'] > last_meta['last_modified']:
#                     return (True, f"ไฟล์ {resource_id[:8]}... อัปเดท")
            
#             # เช็ค hash
#             if current_meta.get('hash') and last_meta.get('hash'):
#                 if current_meta['hash'] != last_meta['hash']:
#                     return (True, f"ไฟล์ {resource_id[:8]}... เปลี่ยน hash")
            
#             # เช็ค URL
#             if current_meta.get('url') != last_meta.get('url'):
#                 log.debug(current_meta.get('url'))
#                 log.debug(last_meta.get('url'))
#                 return (True, f"ไฟล์ {resource_id[:8]}... เปลี่ยน URL")
            
#             # เช็ค format
#             if current_meta.get('format') != last_meta.get('format'):
#                 return (True, f"ไฟล์ {resource_id[:8]}... เปลี่ยน format")
        
#         return (False, "ไม่มีการเปลี่ยนแปลง")
        
#     except Exception as e:
#         log.error(f"Error in should_reprocess_dataset: {e}")
#         return (True, f"Error: {str(e)[:100]}")

def should_reprocess_dataset(dataset_id, last_state):
    try:
        # ดึง package ปัจจุบัน
        package = Session.query(package_table).filter(
            package_table.c.id == dataset_id
        ).first()
        
        if not package:
            return (True, "ไม่พบ dataset")

        # ดึง resources ปัจจุบันของ dataset
        current_resources = Session.query(resource_table).filter(
            resource_table.c.package_id == dataset_id,
            resource_table.c.state == 'active'
        ).all()
        
        # สร้าง metadata แยกออกมา
        current_resource_ids = set()
        current_metadata = {}
        
        for r in current_resources:
            # ดึง URL จาก extras
            url = r.url  # default
            try:
                if r.extras:
                    extras_data = json.loads(r.extras) if isinstance(r.extras, str) else r.extras
                    url = extras_data.get('original_url', r.url)
            except Exception as e:
                log.warning("Cannot parse extras for resource %s: %s", r.id, e)
            
            log.debug("resource %s url=%s extras=%s", r.id, url, r.extras)

            current_resource_ids.add(r.id)
            current_metadata[r.id] = {
                'last_modified': r.last_modified,
                'url': url,
                'format': r.format,
            }
        
        # ไม่มี state เก่า
        if not last_state or not last_state.get('resources'):
            return (True, "ไม่มี state ก่อนหน้า")
        
        last_resource_ids = set(last_state['resources'].keys())
        last_metadata = last_state['resources']

        # เช็ค metadata_modified ของ package
        if last_state.get('package_metadata_modified'):
            current_package_modified = package.metadata_modified
            last_package_modified = last_state['package_metadata_modified']
            
            if current_package_modified and last_package_modified:
                if current_package_modified > last_package_modified:
                    return (True, "Package metadata มีการอัปเดท")
        
        # ตรวจสอบ error
        if last_state.get('has_error'):
            return (True, "พบ Connection timeout ใน state ก่อนหน้า รันใหม่")
        
        # ตรวจสอบการเพิ่ม/ลบไฟล์
        added_files = current_resource_ids - last_resource_ids
        deleted_files = last_resource_ids - current_resource_ids
        
        if added_files:
            return (True, f"มีไฟล์เพิ่ม {len(added_files)} ไฟล์")
        if deleted_files:
            return (True, f"มีไฟล์ลบ {len(deleted_files)} ไฟล์")
        
        # ตรวจสอบการอัปเดทไฟล์
        for resource_id in current_resource_ids:
            current_meta = current_metadata[resource_id]
            last_meta = last_metadata.get(resource_id, {})
            
            if current_meta.get('last_modified') and last_meta.get('last_modified'):
                if current_meta['last_modified'] > last_meta['last_modified']:
                    log.debug(current_meta.get('last_modified'))
                    log.debug(last_meta.get('last_modified'))
                    return (True, f"ไฟล์ {resource_id[:8]}... อัปเดท")
            
            # if current_meta.get('url') != last_meta.get('url'):
            #     log.debug(current_meta.get('url'))
            #     log.debug(last_meta.get('url'))
            #     return (True, f"ไฟล์ {resource_id[:8]}... เปลี่ยน URL")
            
            # if current_meta.get('format') != last_meta.get('format'):
            #     log.debug(current_meta.get('format'))
            #     log.debug(last_meta.get('format'))
                # return (True, f"ไฟล์ {resource_id[:8]}... เปลี่ยน format")
        
        return (False, "ไม่มีการเปลี่ยนแปลง")
        
    except Exception as e:
        log.error(f"Error in should_reprocess_dataset: {e}")
        return (True, f"Error: {str(e)[:100]}")

def load_last_job_state(org_id):
    """
    ดึง job ล่าสุดของ org และเช็คว่าเป็นวันเดียวกันหรือไม่
    
    Returns:
        dict: {
            'job': job object หรือ None,
            'is_same_day': True/False,
            'job_date': วันที่ของ job
        }
    """
    try:
        # from ckanext.your_extension.model import DataQualityJob
        
        current_date = date.today()
        
        # หา job ล่าสุดที่ status = 'finish'
        last_job = Session.query(job_table).filter(
            job_table.org_id == org_id,
            job_table.status == 'finish',
            job_table.run_type == 'organization'
        ).order_by(desc(job_table.started_timestamp)).first()
        
        if not last_job:
            return {'job': None, 'is_same_day': False, 'job_date': None}
        
        # เช็คว่าเป็นวันเดียวกันหรือไม่
        job_date = last_job.requested_timestamp
        is_same_day = (job_date == current_date)
        
        return {
            'job': last_job,
            'is_same_day': is_same_day,
            'job_date': job_date
        }
        
    except Exception as e:
        log.error(f"Error in load_last_job_state: {e}")
        return {'job': None, 'is_same_day': False, 'job_date': None}


def load_dataset_state_from_job(job_id, dataset_id):
    """
    ดึง state ของ dataset จาก job ที่ระบุ
    โดยดูจาก data_quality_metrics
    
    Returns:
        dict หรือ None
    """
    try:
        # 1. ดึง metrics จาก job เก่า
        # ดึง metrics ของ dataset นี้จาก job ที่ระบุ
        # ดึงทั้ง resource + package metrics   xxx# type = 'resource' เท่านั้น (ไม่เอา package level)
        metrics = Session.query(qa_table).filter(
            qa_table.job_id == job_id,
            qa_table.package_id == dataset_id
            # qa_table.type == 'resource'
        ).all()
        
        if not metrics:
            return None
        
        # แยก resource และ package metrics
        resource_metrics = [m for m in metrics if m.type == 'resource']
        package_metrics = [m for m in metrics if m.type == 'package']

        # ตรวจสอบว่ามี Connection timeout หรือไม่
        has_error = any(
            metric.error is not None and 
            'Connection timed out' in str(metric.error)
            for metric in resource_metrics
        )
        
        # 2. วน loop สร้าง resource_states จาก data_quality_metrics
        # สร้าง resource states
        resource_states = {}
        for metric in resource_metrics:
            resource_id = metric.ref_id
            resource_states[resource_id] = {
                'last_modified': metric.resource_last_modified,  # = resource.last_modified
                'url': metric.url,
                'format': metric.format
            }
        
        # เพิ่ม: เก็บ metadata_modified ของ package
        package_metadata_modified = None
        if package_metrics:
            package_metadata_modified = package_metrics[0].resource_last_modified  # = metadata_modified
        
        return {
            'resources': resource_states,
            'package_metadata_modified': package_metadata_modified,  # เพิ่ม
            'has_error': has_error,
        }
        
    except Exception as e:
        log.error(f"Error in load_dataset_state_from_job: {e}")
        return None


def copy_qa_results(source_job_id, target_job_id, dataset_id, org_id):
    """
    คัดลอก metrics จาก job หนึ่งไปอีก job หนึ่ง
    """
    try:
        # ดึง metrics ทั้งหมดของ dataset นี้จาก source job
        source_metrics = Session.query(qa_table).filter(
            qa_table.job_id == source_job_id,
            qa_table.package_id == dataset_id
        ).all()
        
        copied_count = 0
        
        for metric in source_metrics:
            # ถ้าเป็น resource level ต้องเช็คว่า resource ยังมีอยู่
            if metric.type == 'resource':
                resource = Session.query(resource_table).filter(
                    resource_table.c.id == metric.ref_id,
                    resource_table.c.state == 'active'
                ).first()
                
                if not resource:
                    continue  # skip ถ้า resource ถูกลบแล้ว
            
            # สร้าง metric object ใหม่โดยคัดลอกจากเก่า
            new_metric = qa_table(
                created_at=datetime.now(),
                modified_at=datetime.now(),
                type=metric.type,
                ref_id=metric.ref_id,
                package_id=metric.package_id,
                resource_last_modified=metric.resource_last_modified,
                openness=metric.openness,
                timeliness=metric.timeliness,
                acc_latency=metric.acc_latency,
                freshness=metric.freshness,
                availability=metric.availability,
                downloadable=metric.downloadable,
                access_api=metric.access_api,
                relevance=metric.relevance,
                utf8=metric.utf8,
                preview=metric.preview,
                completeness=metric.completeness,
                uniqueness=metric.uniqueness,
                validity=metric.validity,
                consistency=metric.consistency,
                format=metric.format,
                file_size=metric.file_size,
                execute_time=metric.execute_time,
                error=metric.error,
                url=metric.url,
                metrics=metric.metrics,
                job_id=target_job_id  # ใช้ job_id ใหม่
            )
            
            Session.add(new_metric)
            copied_count += 1
        
        Session.commit()
        log.info(f"📋 คัดลอก {copied_count} metrics จาก job {source_job_id[:8]}...")
        return copied_count
        
    except Exception as e:
        log.error(f"Error in copy_qa_results: {e}")
        Session.rollback()
        return 0

# ======================================================================
def run_dataset_metrics(dataset_id, job_row_id):
    try:
        job = Session.query(job_table).filter_by(job_id=job_row_id).first()
        if not job:
            log.error("[WORKER] Job record not found for job_id=%s", job_row_id)
            return

        # mark running
        job.status = "running"
        job.started_timestamp = datetime.now(tz)
        Session.commit()

        # run metric calculation
        metrics = build_metrics('all')
        metrics.calculate_metrics_for_dataset(
            dataset_id,
            job_id=job_row_id
        )

        # mark finish
        job.status = "finish"
        job.finish_timestamp = datetime.now(tz)
        job.active = False
        Session.commit()

    except Exception as e:
        log.error(
            "Job %s for dataset_id %s failed. Error: %s",
            job_row_id, dataset_id, str(e)
        )
        log.exception(e)

        if job:
            job.status = "fail"
            job.finish_timestamp = datetime.now(tz)
            job.active = False
            Session.commit()
def process_org_metrics(org_id, org_name, parent_org_id, parent_org_name, job_row_id):
    log.info(f"[WORKER] Start process_org_metrics for org={org_name}, job_id={job_row_id}")

    start_time = time.time()
    job_cancelled = False
    job_failed = False
    critical_error = None

    try:
        # -----------------------------
        # Fetch job
        # -----------------------------
        job = Session.query(job_table).filter_by(job_id=job_row_id).first()
        if not job:
            raise RuntimeError(f"Job record not found for job_id={job_row_id}")

        # -----------------------------
        # Mark running
        # -----------------------------
        job.status = "running"
        job.started_timestamp = datetime.now(tz)
        Session.commit()

        error_logs = []
        failed_datasets = []
        processed_datasets = 0
        total_datasets = 0

        # -----------------------------
        # Batch processor
        # -----------------------------
        def _process_batch(packages):
            nonlocal processed_datasets, total_datasets
            total_datasets = len(packages)

            for pkg in packages:
                try:
                    job = Session.query(job_table).filter_by(job_id=job_row_id).first()
                    Session.refresh(job)

                    if job.status == "cancel_requested":
                        log.info(f"[CANCEL] Detected cancel_requested for job {job_row_id}")
                        Session.rollback()   # ✅ เพิ่ม
                        job.status = "cancel"
                        job.active = False
                        job.finish_timestamp = datetime.now(tz)
                        if error_logs:
                            job.error_log = "\n".join(error_logs)
                        Session.commit()
                        restore_previous_active_job(job_row_id)
                        raise JobCancelledException()

                    metrics = build_metrics('all')
                    metrics.calculate_metrics_for_dataset(pkg, job_id=job_row_id)
                    processed_datasets += 1

                except JobCancelledException:
                    raise

                except Exception as e:
                    log.exception(f"[WORKER] Dataset {pkg} failed: {e}")
                    error_logs.append(f"Dataset {pkg}: {str(e)[:500]}")
                    failed_datasets.append(pkg)
                    processed_datasets += 1
                    Session.rollback()

        # -----------------------------
        # Run packages
        # -----------------------------
        try:
            org_packages(_process_batch, org_name, job)
        except JobCancelledException:
            job_cancelled = True
        except Exception as e:
            job_failed = True
            critical_error = e
        finally:
            # -----------------------------
            # Finish logic (normal path)
            # -----------------------------
            Session.rollback()
            job = Session.query(job_table).filter_by(job_id=job_row_id).first()

            if job_cancelled:
                job.status = "cancel"
                job.active = False

            elif job_failed:
                job.status = "fail"
                job.active = False

            else:
                job.status = "finish"
                job.active = True

            job.finish_timestamp = datetime.now(tz)
            job.execute_time = round(time.time() - start_time, 3)
            Session.commit()
    except Exception as e:
        # -----------------------------
        # Hard fail
        # -----------------------------
        log.exception(f"[WORKER] Job crashed: {e}")
        try:
            Session.rollback()
            job = Session.query(job_table).filter_by(job_id=job_row_id).first()
            if job:
                job.status = "fail"
                job.active = False
                job.finish_timestamp = datetime.now(tz)
                job.execute_time = round(time.time() - start_time, 3)
                job.error_log = (
                    f"CRITICAL ERROR:\n{str(e)[:1000]}\n\n"
                    f"Traceback:\n{traceback.format_exc()[:2000]}"
                )
                Session.commit()
        except Exception as inner:
            log.error(f"[WORKER] Failed to mark job as fail: {inner}")

    finally:
        try:
            job = Session.query(job_table).filter_by(job_id=job_row_id).first()
            if job and job.status == "running":
                log.warning(f"[FINALIZE] Job {job_row_id} still running → force fail")
                job.status = "fail"
                job.active = False
                job.finish_timestamp = datetime.now(tz)
                job.execute_time = round(time.time() - start_time, 3)
                Session.commit()
        except Exception as e:
            log.error(f"[FINALIZE] Failed to finalize job {job_row_id}: {e}")
            Session.rollback()

# ======================================================================
# ฟังก์ชันใหม่: process_org_metrics_smart (Smart Reprocessing)
# ======================================================================
def process_org_metrics_smart(org_id, org_name, parent_org_id, parent_org_name, job_row_id, last_job_id):
    """
    ฟังก์ชันใหม่ - Smart Reprocessing
    ใช้สำหรับ: org เดิม วันเดียวกัน (เฉพาะที่เปลี่ยนแปลง)
    """
    log.info(f"[WORKER] Start process_org_metrics_smart for org={org_name}, job_id={job_row_id}")
    log.info(f"[SMART] จะเปรียบเทียบกับ job_id={last_job_id}")

    start_time = time.time()
    job_cancelled = False
    job_failed = False
    critical_error = None
    
    stats = {
        'total': 0,
        'processed': 0,
        'copied': 0,
        'failed': 0
    }

    try:
        # -----------------------------
        # Fetch job
        # -----------------------------
        job = Session.query(job_table).filter_by(job_id=job_row_id).first()
        if not job:
            raise RuntimeError(f"Job record not found for job_id={job_row_id}")

        # -----------------------------
        # Mark running
        # -----------------------------
        job.status = "running"
        job.started_timestamp = datetime.now(tz)
        Session.commit()

        error_logs = []
        failed_datasets = []
        processed_datasets = 0
        total_datasets = 0

        # -----------------------------
        # Batch processor (Smart)
        # -----------------------------
        def _process_batch(packages):
            nonlocal processed_datasets, total_datasets
            total_datasets = len(packages)
            stats['total'] = total_datasets

            for pkg in packages:
                try:
                    job = Session.query(job_table).filter_by(job_id=job_row_id).first()
                    Session.refresh(job)

                    if job.status == "cancel_requested":
                        log.info(f"[CANCEL] Detected cancel_requested for job {job_row_id}")
                        Session.rollback()
                        job.status = "cancel"
                        job.active = False
                        job.finish_timestamp = datetime.now(tz)
                        if error_logs:
                            job.error_log = "\n".join(error_logs)
                        Session.commit()
                        restore_previous_active_job(job_row_id)
                        raise JobCancelledException()

                    dataset_id = pkg
                    
                    # ======= Smart Logic เริ่มที่นี่ =======
                    
                    # ดึง state จาก job เก่า
                    last_state = load_dataset_state_from_job(last_job_id, dataset_id)
                    
                    # ตรวจสอบว่าควรประมวลผลใหม่หรือไม่
                    should_process, reason = should_reprocess_dataset(dataset_id, last_state)
                    
                    log.info(f"📦 Dataset {dataset_id[:8]}...: {reason}")
                    
                    if should_process:
                        # ประมวลผลใหม่
                        log.info(f"🔄 ประมวลผลใหม่")
                        metrics = build_metrics('all')
                        metrics.calculate_metrics_for_dataset(pkg, job_id=job_row_id)
                        stats['processed'] += 1
                    else:
                        # คัดลอก state เก่า
                        log.info(f"📋 คัดลอก state จาก job {last_job_id[:8]}...")
                        copied = copy_qa_results(last_job_id, job_row_id, dataset_id, org_id)
                        
                        if copied > 0:
                            stats['copied'] += 1
                        else:
                            # Fallback: ถ้าคัดลอกไม่ได้ให้ประมวลผลใหม่
                            log.warning(f"⚠️  ไม่สามารถคัดลอกได้ - ประมวลผลใหม่")
                            metrics = build_metrics('all')
                            metrics.calculate_metrics_for_dataset(pkg, job_id=job_row_id)
                            stats['processed'] += 1
                    
                    processed_datasets += 1

                except JobCancelledException:
                    raise

                except Exception as e:
                    log.exception(f"[WORKER] Dataset {pkg} failed: {e}")
                    error_logs.append(f"Dataset {pkg}: {str(e)[:500]}")
                    failed_datasets.append(pkg)
                    stats['failed'] += 1
                    processed_datasets += 1
                    Session.rollback()

        # -----------------------------
        # Run packages
        # -----------------------------
        try:
            org_packages(_process_batch, org_name, job)
        except JobCancelledException:
            job_cancelled = True
        except Exception as e:
            job_failed = True
            critical_error = e
        finally:
            # -----------------------------
            # Finish logic (normal path)
            # -----------------------------
            Session.rollback()
            job = Session.query(job_table).filter_by(job_id=job_row_id).first()

            if job_cancelled:
                job.status = "cancel"
                job.active = False

            elif job_failed:
                job.status = "fail"
                job.active = False

            else:
                job.status = "finish"
                job.active = True

                # =================================================================
                #  ลบ job เก่าของวันนี้ (หลังจาก job ใหม่เสร็จแล้ว)
                # =================================================================
                log.info(f" ลบ job เก่าของวันนี้: {last_job_id[:8]}...")
                try:
                    # ลบ metrics ของ job เก่า
                    deleted_metrics = Session.query(qa_table).filter(
                        qa_table.job_id == last_job_id
                    ).delete(synchronize_session=False)
                    
                    log.info(f" ลบ {deleted_metrics} metrics")
                    
                    # ลบ job
                    Session.query(job_table).filter(
                        job_table.job_id == last_job_id
                    ).delete(synchronize_session=False)
                    
                    log.info(f"ลบ job เก่าเรียบร้อย")
                    
                except Exception as e:
                    log.error(f"ไม่สามารถลบ job เก่าได้: {e}")
                    # ไม่ rollback เพราะ job ใหม่สำเร็จแล้ว

                job.finish_timestamp = datetime.now(tz)
                job.execute_time = round(time.time() - start_time, 3)
            
            # Summary
            summary = (
                f" Smart Reprocessing Summary:\n"
                f"Total: {stats['total']}, "
                f"Processed: {stats['processed']}, "
                f"Copied: {stats['copied']}, "
                f"Failed: {stats['failed']}\n"
            )
            
            if error_logs:
                job.error_log = summary + "--- Errors ---\n" + "\n".join(error_logs)
            else:
                job.error_log = summary if stats['total'] > 0 else None
            
            Session.commit()
            
            log.info(
                f"✅ เสร็จสิ้น (SMART): {org_name} - "
                f"Total={stats['total']}, Processed={stats['processed']}, Copied={stats['copied']}"
            )
            
    except Exception as e:
        # -----------------------------
        # Hard fail
        # -----------------------------
        log.exception(f"[WORKER] Job crashed: {e}")
        try:
            Session.rollback()
            job = Session.query(job_table).filter_by(job_id=job_row_id).first()
            if job:
                job.status = "fail"
                job.active = False
                job.finish_timestamp = datetime.now(tz)
                job.execute_time = round(time.time() - start_time, 3)
                job.error_log = (
                    f"CRITICAL ERROR:\n{str(e)[:1000]}\n\n"
                    f"Traceback:\n{traceback.format_exc()[:2000]}"
                )
                Session.commit()
        except Exception as inner:
            log.error(f"[WORKER] Failed to mark job as fail: {inner}")

    finally:
        try:
            job = Session.query(job_table).filter_by(job_id=job_row_id).first()
            if job and job.status == "running":
                log.warning(f"[FINALIZE] Job {job_row_id} still running → force fail")
                job.status = "fail"
                job.active = False
                job.finish_timestamp = datetime.now(tz)
                job.execute_time = round(time.time() - start_time, 3)
                Session.commit()
        except Exception as e:
            log.error(f"[FINALIZE] Failed to finalize job {job_row_id}: {e}")
            Session.rollback()

def _del_metrict( job_id=None):
    if job_id:
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
    else:
        log.error("Please provide either --dataset, --organization, or --job_id")

# =========================
# CLI command
# =========================
def _stop_job(job_id):

    log.info(f"[STOP] Request to stop job_id={job_id}")

    # 1) หา job record จาก DB -----------------------------------------
    job_record = (
        Session.query(job_table)
        .filter(job_table.job_id == job_id)
        .first()
    )

    if not job_record:
        click.echo(f"Job '{job_id}' not found in job_table.")
        return

    if job_record.status in ["finish", "fail", "cancel"]:
        click.echo(f"Job '{job_id}' already {job_record.status}. Nothing to stop.")
        return

    # 2) Update DB - เปลี่ยนแค่ status
    if job_record.status in ["running", "pending"]:
        try:
            #  เปลี่ยนแค่ status เป็น cancel_requested
            # ไม่ต้องตั้ง finish_timestamp และ active ตอนนี้
            # ให้ worker เป็นคนตั้งเอง
            old_status = job_record.status
            job_record.status = "cancel_requested"
            Session.commit()
            
            log.info(f"[STOP] Job {job_id} status changed: {old_status} -> cancel_requested")
            click.echo(f"  Stop request sent for job '{job_id}'")
            click.echo(f"  Status: {old_status} -> cancel_requested")
            click.echo(f"  The worker will stop processing and update the final status.")
            
        except Exception as e:
            Session.rollback()
            log.error(f"[STOP] DB update failed: {e}")
            click.echo(f"✗ DB update failed: {e}")
            return
    else:
        click.echo(f"Job '{job_id}' has status '{job_record.status}' - cannot stop.")

def _calculate(job_id=None, dataset=None, organization=None, dimension='all'):
    log.debug('Starting data quality metrics calculation')
    if six.PY2:
        _register_mock_translator()
    metrics = build_metrics(dimension)

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

            # -------------------------
            # 1. ปิด job เก่า (active=False)
            # -------------------------
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

            # -------------------------
            # 2. ลบงานที่รันวันนี้ออก
            # -------------------------
            today = date.today()
            start_of_day = datetime(today.year, today.month, today.day)
            end_of_day = start_of_day + timedelta(days=1)

            today_jobs = Session.query(job_table).filter(
                job_table.org_name == dataset_name,
                job_table.requested_timestamp >= start_of_day,
                job_table.requested_timestamp < end_of_day
            ).all()
            for job in today_jobs:
                Session.query(qa_table).filter(
                    qa_table.job_id == job.job_id
                ).delete(synchronize_session='fetch')

                Session.delete(job)
                Session.commit()

            if today_jobs:
                log.info("Deleted %s old job(s) for dataset %s today",
                        len(today_jobs), dataset_name)

            # -------------------------
            # 4. บันทึกลง job_table ของระบบคุณ
            # -------------------------
            job_row_id = str(uuid.uuid4())
            job = job_table(
                job_id=job_row_id,
                org_id=None,
                org_name=dataset_name,
                status="pending",
                requested_timestamp=date.today(),
                run_type='dataset',
                active=True
            )
            Session.add(job)
            Session.commit()
            # -------------------------
            # 3. สร้าง CKAN background job (ให้ CKAN generate job id)
            # -------------------------
            # enqueue job ผ่าน CKAN
            ckan_job_id = toolkit.enqueue_job(
                run_dataset_metrics,
                args=[dataset_id,job_row_id],      # arguments to job
                title=f"QA metrics for dataset {dataset_name}"
            )

            log.info(f"CKAN created job id = {ckan_job_id}")

    #------------------------------
    if organization:  
        if organization == 'all':
            all_orgs = get_all_organizations()  # ต้องมีฟังก์ชันคืนชื่อหรือ id ของทุก org
            requested_timestamp = date.today()

            for org_name in all_orgs:
                try:
                    execute_time = 0
                    start_time = time.time()
                    parent_org_id, parent_org_name = get_parent_organization(org_name)
                    org_id = get_org_id_from_name(org_name)

                    if not org_id:
                        log.error(f"Organization '{org_name}' not found in CKAN.")
                        continue

                    # นับจำนวน dataset ของ org
                    dataset_count = Session.query(package_table).filter(
                        package_table.c.owner_org == org_id,
                        package_table.c.type == 'dataset',
                        package_table.c.private == False,
                        package_table.c.state == 'active'
                    ).count()

                    log.info(f"Organization '{org_name}' has {dataset_count} active public datasets.")

                    if dataset_count == 0:
                        log.warning(f"Skip organization '{org_name}' — no active datasets found.")
                        continue
                    #--------- [Start] Processing organization -------------------
                    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    log.info(f"Processing organization: {org_name}: date_time_start[{timestamp}]")
                    
                    # ---------------------------------------------
                    # 1. ปิด job เดิม
                    # ---------------------------------------------
                    old_all_jobs = Session.query(job_table).filter(
                        job_table.org_id == org_id,
                        job_table.active == True
                    ).all()
                    for old_job in old_all_jobs:
                        old_job.active = False
                    Session.commit()

                    # ---------------------------------------------
                    # 2. ลบ job วันนี้ + metrics
                    # ---------------------------------------------
                    today = date.today()
                    today_jobs = Session.query(job_table).filter(
                        job_table.org_id == org_id,
                        job_table.requested_timestamp >= datetime(today.year, today.month, today.day)
                    ).all()

                    for job in today_jobs:
                        Session.query(qa_table).filter(
                            qa_table.job_id == job.job_id
                        ).delete(synchronize_session='fetch')
                        Session.delete(job)
                    Session.commit()

                    # ---------------------------------------------
                    # 4. สร้าง job record ใน job_table ของระบบเรา
                    # ---------------------------------------------
                    job_row_id = str(uuid.uuid4())
                    new_job = job_table(
                        job_id=job_row_id, #job_id,
                        org_parent_id=parent_org_id,
                        org_parent_name=parent_org_name,
                        org_id=org_id,
                        org_name=org_name,
                        status="pending",
                        # requested_timestamp=date.today(),
                        requested_timestamp=requested_timestamp,
                        run_type='organization',
                        active=False
                    )
                    Session.add(new_job)
                    Session.commit()
                    
                    # enqueue job
                    ckan_job = toolkit.enqueue_job(
                        process_org_metrics,
                        args=[org_id, org_name, parent_org_id, parent_org_name, job_row_id],
                        title=f"QA metrics for organization {org_name}"
                    )
                    # ckan_job_id = ckan_job.id
                    # job.job_id = ckan_job_id
                    Session.commit()
                    log.info(f"[CKAN-JOB] Created job = {job_row_id} for org {org_name}")
       
                except Exception as e:
                    log.error(f"Job failed for {org_name}. Error: {e}")
                    Session.rollback()
                    if 'new_job' in locals():
                        job.status = "fail"
                        job.finish_timestamp = datetime.now(tz)#date.today()
                        job.execute_time = 0             
                        Session.commit()

        else:
            execute_time = 0
            start_time = time.time()
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            log.info(f"Processing organization: {organization}: date_time_start[{timestamp}]")

            parent_org_id, parent_org_name = get_parent_organization(organization)
            org_id = get_org_id_from_name(organization)  
            #=================================================================   
            # 1. ตรวจสอบ org_id
            #=================================================================
            if not org_id:
                log.error(f"Organization '{organization}' not found in CKAN.")
                raise ValueError(f"Organization '{organization}' not found in CKAN.")

            # นับจำนวน dataset ของ org
            dataset_count = Session.query(package_table).filter(
                package_table.c.owner_org == org_id,
                package_table.c.type == 'dataset',
                package_table.c.private == False,
                package_table.c.state == 'active'
            ).count()

            log.info(f"Organization '{organization}' has {dataset_count} active public datasets.")

            if dataset_count == 0:
                log.debug(f"Skip organization '{organization}' — no active datasets found.")
            else:   

                # #=========================================================
                # # 2. ลบ job ของ org นี้ที่เป็น "วันนี้" เท่านั้น
                # #=========================================================
                # today = date.today()
                # today_jobs = Session.query(job_table).filter(
                #     job_table.org_id == org_id,
                #     job_table.run_type == 'organization',
                #     job_table.requested_timestamp >= datetime(today.year, today.month, today.day)
                # ).all()

                # for old_job in today_jobs:
                #     Session.query(qa_table).filter(
                #         qa_table.job_id == old_job.job_id
                #     ).delete(synchronize_session=False)

                #     Session.delete(old_job)

                # if today_jobs:
                #     log.info(
                #         "Deleted %s existing job(s) today for org %s",
                #         len(today_jobs), organization
                #     )
                # =================================================================
                # 2. ตรวจสอบว่ามี job ของ org นี้ที่กำลังรันอยู่หรือไม่
                # =================================================================
                running_job = Session.query(job_table).filter(
                    job_table.org_id == org_id,
                    job_table.run_type == 'organization',
                    job_table.status.in_(['pending', 'running']),
                    job_table.active == True
                ).first()
                
                if running_job:
                    log.warning(f" Organization '{organization}' มี job กำลังรันอยู่ (job_id: {running_job.job_id})")
                    log.info(f" Skip")
                    return None
                    #raise SystemExit(0)  # หรือใช้ return/continue
                # ดึง job ล่าสุด
                last_job_info = load_last_job_state(org_id)
                last_job = last_job_info['job']
                is_same_day = last_job_info['is_same_day']
                job_date = last_job_info['job_date']
                # =========================================================
                # 3. ปิด active job เก่า (ของ org นี้เท่านั้น)
                # =========================================================
                Session.query(job_table).filter(
                    job_table.org_id == org_id,
                    job_table.active == True
                ).update(
                    {"active": False},
                    synchronize_session=False
                )

                Session.commit()
                # =================================================================
                # 4. สร้าง record ลง job_table ของระบบ
                # =================================================================
                if job_id is None:
                    job_row_id = str(uuid.uuid4())
                    job = job_table(
                        job_id=job_row_id, #job_id,
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
                else:
                    job_row_id = job_id
                    job = Session.query(job_table).filter_by(job_id=job_id).first()
                    job.job_id = job_id
                    job.org_parent_id = parent_org_id,
                    job.org_parent_name = parent_org_name,
                    job.org_id = org_id,
                    job.org_name = organization,
                    job.requested_timestamp = date.today(),
                    job.run_type = 'organization',
                    job.active = True
                Session.commit()
 
                # # =================================================================
                # # 5. ส่งต่อ job_row_id ไปยัง process_org_metrics พร้อมกับ logic
                # #    สำหรับตรวจสอบ state ก่อนประมวลผลแต่ละ dataset
                # # =================================================================
                # ckan_job = toolkit.enqueue_job(
                #     process_org_metrics,
                #     args=[org_id, organization, parent_org_id, parent_org_name,job_row_id],
                #     title=f"QA metrics for organization {organization}"
                # )
                # log.info(f"[CKAN-JOB] Created job id = {job_row_id} for org {organization}")
                # ======================================================================
                # ตัดสินใจเลือกฟังก์ชัน
                # ======================================================================
                if is_same_day and last_job:
                    # กรณีวันเดียวกัน → ใช้ Smart Reprocessing
                    log.info(f" ใช้ Smart Reprocessing (เปรียบเทียบกับ job: {last_job.job_id[:8]}...)")
                    ckan_job = toolkit.enqueue_job(
                        process_org_metrics_smart,
                        args=[org_id, organization, parent_org_id, parent_org_name, job_row_id, last_job.job_id],
                        title=f"QA metrics (SMART) for {organization}"
                    )
                else:
                    # กรณี org ใหม่ หรือ คนละวัน → ใช้ฟังก์ชันเดิม
                    if last_job:
                        log.info(f" ใช้ Full Reprocessing (job ล่าสุด: {job_date})")
                    else:
                        log.info(f" ใช้ Full Reprocessing (org ใหม่)")
                    
                    ckan_job = toolkit.enqueue_job(
                        process_org_metrics,
                        args=[org_id, organization, parent_org_id, parent_org_name, job_row_id],
                        title=f"QA metrics for {organization}"
                    )
                
                log.info(f"[CKAN-JOB] Created job id = {job_row_id} for org {organization}")


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
def calculate(organization=None, dataset=None,dimension='all'):
    _calculate(None, dataset=dataset, organization=organization, dimension=dimension)
    # jobs.enqueue(_calculate, None, kwargs={'organization': organization, 'dataset': dataset, 'dimension':dimension})          

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
        # log.debug('Fetching dataset batch %d to %d', offset, offset+limit)
        query = Session.query(package_table.c.id).filter(package_table.c.type == 'dataset', package_table.c.private == False, package_table.c.state == 'active')
        query = query.offset(offset).limit(limit)

        count = 0
        packages = []
        for result in query.all():
            packages.append(result[0])
            count += 1

        if not count:
            # log.debug('No more packages to process.')
            break

        offset += limit

        try:
            # log.debug('Processing %d packages in current batch.', count)
            handler(packages)
        except JobCancelledException as e:
            #  จับ cancel แยกออกมา - ไม่ใช่ error
            log.info(f"[CANCEL] Job cancelled during org_packages: {e}")
            raise  # ส่งต่อไปข้างนอก
        except Exception as e:
            log.error('Failed to process package batch. Error: %s', str(e))
            log.exception(e)

def org_packages(handler,org_name,job):
    offset = 0
    limit = 64
    while True:
        # log.debug('Fetching dataset batch %d to %d', offset, offset+limit)
        group = model.Group.get(org_name)
        query = Session.query(package_table.c.id).filter(package_table.c.owner_org == group.id, package_table.c.type == 'dataset', package_table.c.private == False, package_table.c.state == 'active')
        query = query.offset(offset).limit(limit)
        count = 0
        packages = []
        for result in query.all():
            packages.append(result[0])
            count += 1

        if not count:
            # log.debug('No more packages to process.')
            break

        offset += limit

        try:
            # log.debug('Processing %d packages in current batch.', count)
            handler(packages)
        except JobCancelledException as e:
            #  จับ cancel แยกออกมา - ไม่ใช่ error
            log.info(f"[CANCEL] Job cancelled during org_packages: {e}")
            raise  # ส่งต่อไปข้างนอก
        except Exception as e:
            job.status = "fail"
            job.finish_timestamp = datetime.now(tz) #date.today(),
            Session.commit()
            log.error('Job failed at Cli-org package')
            log.error('Failed to process package batch. Error: %s', str(e))

@quality.command(u'stop', help='Stop a running data quality job')
@click.option('--job_id',
              required=True,
              help='Job ID to stop')
def stop_job(job_id):

    _stop_job(job_id)

@quality.command(u'delete', help='Delete data quality metrics')
@click.option('--job_id',
              default=None,
              help='Delete quality metrics by job id')
def del_metrict(job_id=None):
    _del_metrict(job_id)

#---- ok version -----------------
def get_all_organizations():
    """ดึงรายชื่อ organization ทั้งหมดจากฐานข้อมูล CKAN"""
    orgs = model.Session.query(model.Group).filter(model.Group.type == 'organization').all()
    return [org.name for org in orgs]
#---- ***ห้ามลบ**** ok version: test for run some orgs -----------------
# def get_all_organizations():
#     """ดึงรายชื่อ organization จาก CKAN config ถ้ามี; ถ้าไม่มีให้ query จริง"""

#     # อ่านค่าจาก ckan.ini
#     # config_orgs = toolkit.config.get('ckanext.opendquality.orgs', "").strip()
#     config_orgs = (os.environ.get('CKANEXT__OPENDQUALITY__ORGS')
#     or (toolkit.config.get('ckanext.opendquality.orgs') or '').strip())

#     if config_orgs:
#         # แปลงเป็น list
#         org_list = [o.strip() for o in config_orgs.split(",") if o.strip()]
#         log.debug(f"[CONFIG MODE] Using organizations from ckan.ini: {org_list}")
#         return org_list

#     # Fallback: query จาก database จริง
#     # log.debug("[NORMAL MODE] Querying all organizations from CKAN")
#     orgs = (
#         model.Session.query(model.Group)
#         .filter(
#             model.Group.type == 'organization',
#             model.Group.state == 'active'
#         )
#         .all()
#     )
#     return [org.name for org in orgs]

#     # ถ้าไม่มี ENV ให้ดึงจริงจาก DB
#     orgs = model.Session.query(model.Group).filter(model.Group.type == 'organization').all()
#     return [org.name for org in orgs]
    
def get_parent_organization(org_id):
    # url = f"https://data.go.th/api/3/action/group_tree_section?type=organization&id={org_id}"
    # url = f"https://ckan-dev.opend.cloud/api/3/action/group_tree_section?type=organization&id={org_id}"
    base_url = config.get("ckan.site_url")
    url = f"{base_url}/api/3/action/group_tree_section?type=organization&id={org_id}"
    # log.debug('base_url')
    # log.debug(url)
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