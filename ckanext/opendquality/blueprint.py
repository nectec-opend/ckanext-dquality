# encoding: utf-8

from flask import Blueprint, request, Response, jsonify
import ckan.plugins.toolkit as toolkit, os
import ckan.lib.jobs as jobs
from logging import getLogger
from datetime import datetime
# from ckan.common import config
from ckan.model import package_table, Session, Package, Group, Resource
from ckanext.opendquality.model import DataQualityMetrics as DQM , JobDQ
import ckanext.opendquality.quality as quality_lib
import ckan.lib.helpers as h
from sqlalchemy import and_, literal, case, func, Date, cast
from ckanext.opendquality.utils import get_radar_aggregate_all, qa_counts, qa_detail_blocks, get_timeliness_summary, get_relevance_top, get_openness_score, get_openness_counts, get_validity_counts, get_quality_counts, get_resource_format_counts
from ckanext.opendquality.cli.quality import calculate as quality_cli
# from ckanext.myorg import helpers as myh
# from ckanext.opendquality.quality import (
#     Completeness,
#     DataQualityMetrics
# )
log = getLogger(__name__)
qa = Blueprint('opendquality', __name__, url_prefix="/qa")
dquality = quality_lib.OpendQuality()
# metrics  = quality_lib.DataQualityMetrics()#metrics=calculators
as_agency = toolkit.asbool(os.environ.get('CKANEXT__OPENDQUALITY__AGENT', toolkit.config.get('ckanext.opendquality.agent', True)))
enpoint_public = os.environ.get('CKANEXT__OPENDQUALITY__PUBLIC_ENDPOINTS', toolkit.config.get('ckanext.opendquality.public_endpoints', ''))
# EXEMPT_ENDPOINTS = {
#     # 'opendquality.index',
#     'opendquality.admin_report',
#     'opendquality.dashboard',
# }
EXEMPT_ENDPOINTS = set(enpoint_public.split(' ') if enpoint_public else [])

@qa.before_request
def request_before():
    if request.endpoint in EXEMPT_ENDPOINTS:
        return
    user = getattr(toolkit.c, 'userobj', None)
    if not user or not getattr(user, 'sysadmin', False):
        toolkit.abort(403, toolkit._('You do not have permission to access this page.'))

@qa.teardown_request
def shutdown_session(exception=None):
    if exception:
        Session.rollback()
    Session.remove()

def export_data_quality(data_quality, quality_type):
    if quality_type == 'package':
        columes_name = ["รหัสชุดข้อมูล","ชื่อชุดข้อมูล", "ชื่อหน่วยงาน", "type", "openess", "timeliness", "acc_latency", "freshness", "availability", "downloadable", "access_api", "relevance", "utf8", "preview", "completeness", "uniqueness", "validity", "consistency", "format", "file_size", "execute_time", "error", "metrics"]
    else:
        columes_name = ["รหัสชุดข้อมูล", "ชื่อชุดข้อมูล", "รหัสทรัพยากร", "ชื่อทรัพยากร", "ชื่อหน่วยงาน", "type", "openess", "timeliness", "acc_latency", "freshness", "availability", "downloadable", "access_api", "relevance", "utf8", "preview", "completeness", "uniqueness", "validity", "consistency", "format", "file_size", "execute_time", "error", "metrics"]
    import csv
    from io import StringIO
    output = StringIO()
    output.write('\ufeff')
    writer = csv.writer(output)
    writer.writerow(columes_name)
    for row in data_quality:
        if quality_type == 'package':
            writer.writerow([row.package_id, row.package_title, row.org_title, row.dq_type, row.openness, row.timeliness, row.acc_latency, row.freshness, row.availability, row.downloadable, row.access_api, row.relevance, row.utf8, row.preview, row.completeness, row.uniqueness, row.validity, row.consistency, row.format, row.file_size, row.execute_time, row.error, row.metrics])
        else:
            writer.writerow([row.package_id, row.package_title, row.resource_id, row.resource_name, row.org_title, row.dq_type, row.openness, row.timeliness, row.acc_latency, row.freshness, row.availability, row.downloadable, row.access_api, row.relevance, row.utf8, row.preview, row.completeness, row.uniqueness, row.validity, row.consistency, row.format, row.file_size, row.execute_time, row.error, row.metrics])
    output.seek(0)
    response = Response(output.getvalue(), mimetype='text/csv; charset=utf-8')
    response.headers['Content-Disposition'] = f'attachment; filename="data_quality_{quality_type}_report.csv"'
    return response

def build_hierachy_with_versions():
    rows = (
        Session.query(
            JobDQ.org_id,
            JobDQ.org_name,
            JobDQ.requested_timestamp.label('version'),
        )
        .filter(JobDQ.status == 'finish', JobDQ.run_type == 'organization')
        .order_by(JobDQ.requested_timestamp.desc())
        .all()
    )
    versions = {}
    for oid, oname, version in rows:
        oid_s = str(oid).strip() if oid is not None else ""
        item = {
            'value': version.strftime('%Y-%m-%d'),
            'label': version.strftime('%d/%m/%Y'),
            'org_name': oname
        }
        versions.setdefault(oid_s, []).append(item) 

    return versions

def build_agency_orgs():
    rows = (
        Session.query(
            JobDQ.org_id.label('id'),
            JobDQ.org_name.label('name'),
            Group.title.label('title'),
            JobDQ.requested_timestamp
        )
        .join(Group, Group.id == JobDQ.org_id)
        .join(DQM, DQM.job_id == JobDQ.job_id)
        .filter(JobDQ.active == True, JobDQ.status == 'finish', JobDQ.run_type == 'organization')
        .distinct()
        .order_by(JobDQ.requested_timestamp.desc())
        .all()
    )

    return rows

def build_hierachy_with_orgs():
    # 1) เลือกลำดับคอลัมน์ให้ถูก: ... org_id, org_name
    rows = (
        Session.query(
            JobDQ.org_parent_id,
            JobDQ.org_parent_name,
            JobDQ.org_id,
            JobDQ.org_name,
        )
        .filter(JobDQ.active == True, JobDQ.status == 'finish', JobDQ.run_type == 'organization')
        .distinct()
        .all()
    )

    parent_ids = set()
    child_ids = set()
    pairs = [] 

    for pid, pname, cid, cname in rows:
        # 2) sanitize กัน None/ช่องว่าง และบังคับเป็น str
        pid_s = str(pid).strip() if pid is not None else ""
        cid_s = str(cid).strip() if cid is not None else ""
        pname_s = (pname or "").strip()
        cname_s = (cname or "").strip()

        if not pid_s or not cid_s:
            continue

        parent_ids.add(pid_s)
        child_ids.add(cid_s)
        pairs.append((pid_s, pname_s, cid_s, cname_s))

    if not parent_ids:
        return [], {}

    # 3) ดึงชื่อไทยจากตาราง group ทีเดียว
    all_ids = list(parent_ids | child_ids)
    groups = (
        Session.query(Group)
        .filter(
            Group.type == 'organization',
            Group.state == 'active',
            Group.id.in_(all_ids),
        )
        .all()
    )
    gmap = {g.id: g for g in groups}

    def pick_title(g, fallback_name, fallback_id):
        if g:
            return (g.title or g.name or fallback_name or fallback_id)
        return (fallback_name or fallback_id)

    # 4) parents
    parents = []
    for pid in sorted(parent_ids):  # ทุกอย่างเป็น str แล้ว sort ได้
        g = gmap.get(pid)
        # fallback name ของ parent จากแถวแรกที่เจอใน pairs
        pname_fallback = next((pf for p, pf, _, _ in pairs if p == pid), "")
        parents.append({
            'id': pid,
            'title': pick_title(g, pname_fallback, pid),
            'name': (g.name if g else pname_fallback) or pid,
        })

    # 5) children_by_parent
    children_by_parent = {}
    for pid, pname_fallback, cid, cname_fallback in pairs:
        cg = gmap.get(cid)
        item = {
            'id': cid,
            'title': pick_title(cg, cname_fallback, cid),
            'name': (cg.name if cg else cname_fallback) or cid,
        }
        children_by_parent.setdefault(pid, []).append(item)

    # 6) dedup + sort
    parents.sort(key=lambda x: (x['title'] or '').lower())
    for pid, lst in list(children_by_parent.items()):
        dedup = {it['id']: it for it in lst}   # ← แก้ชื่อให้ถูก
        lst = list(dedup.values())
        lst.sort(key=lambda x: (x['title'] or '').lower())
        children_by_parent[pid] = lst

    return parents, children_by_parent


def _get_extra(org_dict, key, default=None):
    """ดึงค่า extras[key] จาก org (รองรับทั้งรูปแบบ list/dict)"""
    extras = org_dict.get('extras') or {}
    if isinstance(extras, dict):
        return extras.get(key, default)
    # บางเวอร์ชัน extras เป็น list[{key:.., value:..}]
    if isinstance(extras, list):
        for item in extras:
            if item.get('key') == key:
                return item.get('value')
    return default

def make_group_query():
    """Group ที่มี package active และมี DQM type='package' ผูกกับ package นั้นๆ"""
    return (Session.query(Group)
            .join(Package, Group.id == Package.owner_org)
            .join(DQM, and_(DQM.ref_id == Package.id,
                            DQM.type == 'package'))
            .filter(Package.state == 'active')
            .distinct())
def make_group_query_main():

    return (Session.query(Group)
            .join(JobDQ, Group.id == JobDQ.org_parent_id)
            .filter(JobDQ.active == True, JobDQ.status == 'finish')
            .distinct())

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


def _register_mock_translator():
    # Workaround until the core translation function defaults to the Flask one
    from paste.registry import Registry
    from ckan.lib.cli import MockTranslator
    registry = Registry()
    registry.prepare()
    from pylons import translator
    registry.register(translator, MockTranslator())

def _get_org_main():
    try:
        q = (make_group_query_main()
             .with_entities(
                 Group.id.label('org_id'),
                 Group.name.label('org_name'),
                 Group.title.label('org_title'),
             ))
        return q.all()
    except Exception:
        Session.rollback()
        raise

def _get_org():
    try:
        q = (make_group_query()
             .with_entities(
                 Group.id.label('org_id'),
                 Group.name.label('org_name'),
                 Group.title.label('org_title'),
             ))
        return q.all()
    except Exception:
        Session.rollback()
        raise

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
def cancel_job():
    if h.check_access('sysadmin') is False:
        toolkit.abort(403, toolkit._('You do not have permission to access this page.'))
    # job = Session.query(JobDQ).filter(JobDQ.job_id == job_id).first()
    # if job and job.status in ['queued', 'running']:
    #     job.status = 'cancelled'
    #     Session.commit()
    #     return True
    # return False
    # request.form.get('job_id')
    return toolkit.redirect_to('opendquality.index')
    # return {'msg': 'Cancel Job', 'data': request.form.get('job_id')}

def delete_job():
    if h.check_access('sysadmin') is False:
        # return toolkit.redirect_to('opendquality.dashboard')
        toolkit.abort(403, toolkit._('You do not have permission to access this page.'))
    # job = Session.query(JobDQ).filter(JobDQ.job_id == job_id).first()
    # if job:
    #     # delete associated DQM records
    #     Session.query(DQM).filter(DQM.job_id == job_id).delete()
    #     Session.delete(job)
    #     Session.commit()
    #     return True
    # return False
    if request.method == 'POST':
        if request.form.get('job_id') is not None:
            from ckanext.opendquality.cli.quality import _del_metrict as del_metric
            del_metric(organization=None, dataset=None, job_id=request.form.get('job_id'))
            return toolkit.redirect_to('opendquality.index')
    
        return toolkit.redirect_to('opendquality.index')
    return toolkit.redirect_to('opendquality.index')
    # return {'msg': 'Delete Job', 'data': request.form.get('job_id')}


def home():
    if h.check_access('sysadmin') is False:
        return toolkit.redirect_to('opendquality.dashboard')

    if request.method == 'POST':
        selected_org_cal = request.form.get('orgs_calc')

        # log.debug(type(selected_org_cal))

        # return {'msg': 'Start QA Job',
        #         'data': selected_org_cal}
        if selected_org_cal != None or selected_org_cal != '':
            jobs.enqueue('ckanext.opendquality.cli.quality._calculate', None, kwargs={'organization': selected_org_cal, 'dataset': None, 'dimension':'all'})
            # quality_cli(selected_org_cal)

        # return {'msg': 'Start QA Job',
        #         'data': request.form.get('orgs_calc')}


    qa_jobs = Session.query(
        JobDQ.job_id,
        JobDQ.org_id,
        JobDQ.org_name,
        Group.title.label('org_title'),
        JobDQ.requested_timestamp,
        JobDQ.started_timestamp,
        JobDQ.finish_timestamp,
        JobDQ.execute_time,
        JobDQ.status,
    ).join(
        Group, Group.id == JobDQ.org_id
    ).order_by(JobDQ.started_timestamp.desc()).all()

    orgs_cal = Session.query(
        Package.owner_org.label('org_id'),
        Group.title.label('org_title'),
        Group.name.label('org_name')
    ).join(
        Package, Package.owner_org == Group.id
    ).filter(
        Package.state == 'active'
    ).distinct().all()

    extra_vars = {
        'title': toolkit._('วัดผลคุณภาพชุดข้อมูล'),
        'home': True,
        'user': toolkit.c.user,
        'userobj': toolkit.c.userobj,
        'site_url': toolkit.request.host_url,
        'site_title': toolkit.config.get('ckan.site_title'),
        'ckan_version': toolkit.config.get('ckan.version'),
        'ckanext_opendquality_version': toolkit.config.get('ckanext-opendquality.version'),
        'enpoint_public': EXEMPT_ENDPOINTS,
        'calc_orgs': orgs_cal,
        'qa_jobs': qa_jobs,
        # 'external_dashboard': external_stats
    }
    return toolkit.render('ckanext/opendquality/index.html', extra_vars)

def admin_report(org_id=None):
    # if h.check_access('sysadmin') is False:
    #     return toolkit.redirect_to('opendquality.dashboard')
    selected_sub = request.args.get('org_sub', None)
    selected_main = request.args.get('org_main', None)
    package_id = request.args.get('package_id', None)
    selected_version = request.args.get('ver_selected', None)
    versions = build_hierachy_with_versions()

    if not as_agency:
        parents ,children_by_parent = build_hierachy_with_orgs()
        sub_options = children_by_parent.get(selected_main, []) if selected_main else []
    else:
        sub_options = build_agency_orgs()
        parents = children_by_parent  = None
    version_options = versions.get(selected_sub, []) if selected_sub else []
    org_id = org_id if org_id is not None else selected_sub

    if selected_version:
        try:
            selected_version = datetime.strptime(selected_version, '%Y-%m-%d').date()
        except ValueError:
            selected_version = None

    export = request.args.get('export') == '1'
    export_all = request.args.get('export_all') == '1'

    base_filters = [
        JobDQ.status == 'finish',
        JobDQ.run_type == 'organization'
    ]

    if selected_version is not None:
        base_filters.append(JobDQ.requested_timestamp == selected_version)
    else:
        base_filters.append(JobDQ.active == True)
    # org_id = request.view_args.get('org_id') or request.args.get('org_id')

    if export_all:
        # -------- รวมทั้ง package และ resource ด้วย UNION ALL --------
        # คอลัมน์ “ร่วม” ที่จะส่งไป render/export (เรียงเหมือนกันทั้งสองขา)
        pkg_cols = [
            Package.title.label('package_title'),
            Package.id.label('package_id'),
            literal(None).label('resource_id'),
            literal(None).label('resource_name'),
            Group.title.label('org_title'),
            Group.id.label('org_id'),
            DQM.openness.label('openness'),
            DQM.timeliness.label('timeliness'),
            DQM.acc_latency.label('acc_latency'),
            DQM.freshness.label('freshness'),
            DQM.availability.label('availability'),
            DQM.downloadable.label('downloadable'),
            DQM.access_api.label('access_api'),
            DQM.relevance.label('relevance'),
            DQM.utf8.label('utf8'),
            DQM.preview.label('preview'),
            DQM.completeness.label('completeness'),
            DQM.uniqueness.label('uniqueness'),
            DQM.validity.label('validity'),
            DQM.consistency.label('consistency'),
            DQM.format.label('format'),
            DQM.file_size.label('file_size'),
            DQM.execute_time.label('execute_time'),
            DQM.error.label('error'),
            DQM.metrics.label('metrics'),
            JobDQ.org_parent_id,
            DQM.type.label('dq_type'),
            DQM.modified_at.label('modified_at')
        ]
        res_cols = [
            Package.title.label('package_title'),
            Package.id.label('package_id'),
            Resource.id.label('resource_id'),
            Resource.name.label('resource_name'),
            Group.title.label('org_title'),
            Group.id.label('org_id'),
            DQM.openness.label('openness'),
            DQM.timeliness.label('timeliness'),
            DQM.acc_latency.label('acc_latency'),
            DQM.freshness.label('freshness'),
            DQM.availability.label('availability'),
            DQM.downloadable.label('downloadable'),
            DQM.access_api.label('access_api'),
            DQM.relevance.label('relevance'),
            DQM.utf8.label('utf8'),
            DQM.preview.label('preview'),
            DQM.completeness.label('completeness'),
            DQM.uniqueness.label('uniqueness'),
            DQM.validity.label('validity'),
            DQM.consistency.label('consistency'),
            DQM.format.label('format'),
            DQM.file_size.label('file_size'),
            DQM.execute_time.label('execute_time'),
            DQM.error.label('error'),
            DQM.metrics.label('metrics'),
            JobDQ.org_parent_id,
            DQM.type.label('dq_type'),
            DQM.modified_at.label('modified_at')
        ]

        q_pkg = (
            Session.query(*pkg_cols)
            .join(Package, Package.id == DQM.ref_id)             # ref_id ชี้ package เมื่อ type='package'
            .join(Group, Group.id == Package.owner_org)
            .join(JobDQ, DQM.job_id == JobDQ.job_id)
            .filter(DQM.type == 'package', *base_filters)
        )
        q_res = (
            Session.query(*res_cols)
            .join(Resource, Resource.id == DQM.ref_id)           # ref_id ชี้ resource เมื่อ type='resource'
            .join(Package, Package.id == DQM.package_id)
            .join(Group, Group.id == Package.owner_org)
            .join(JobDQ, DQM.job_id == JobDQ.job_id)
            .filter(DQM.type == 'resource', *base_filters)
        )

        if org_id:
            q_pkg = q_pkg.filter(JobDQ.org_id == org_id)
            q_res = q_res.filter(JobDQ.org_id == org_id)
        
        if package_id:
            q_res = q_res.filter(DQM.package_id == package_id)
            q_pkg = q_pkg.filter(DQM.package_id == package_id)

        union_sq = q_pkg.union_all(q_res).subquery()
        data_quality = (
            Session.query(union_sq)
            .order_by(union_sq.c.modified_at.desc())
            .all()
        )
        quality_type = 'all'

    else:
        # -------- โหมดปกติ: แยกตามว่ามี package_id ใน query หรือไม่ --------
        if 'package_id' not in request.args:
            quality_type = 'package'
            data_quality = (
                Session.query(
                    Package.title.label('package_title'),
                    Package.id.label('package_id'),
                    Group.title.label('org_title'),
                    Group.id.label('org_id'),
                    DQM.openness.label('openness'),
                    DQM.timeliness.label('timeliness'),
                    DQM.acc_latency.label('acc_latency'),
                    DQM.freshness.label('freshness'),
                    DQM.availability.label('availability'),
                    DQM.downloadable.label('downloadable'),
                    DQM.access_api.label('access_api'),
                    DQM.relevance.label('relevance'),
                    DQM.utf8.label('utf8'),
                    DQM.preview.label('preview'),
                    DQM.completeness.label('completeness'),
                    DQM.uniqueness.label('uniqueness'),
                    DQM.validity.label('validity'),
                    DQM.consistency.label('consistency'),
                    DQM.format.label('format'),
                    DQM.file_size.label('file_size'),
                    DQM.execute_time.label('execute_time'),
                    DQM.error.label('error'),
                    DQM.metrics.label('metrics'),
                    JobDQ.org_parent_id,
                    DQM.type.label('dq_type'),
                    DQM.modified_at.label('modified_at')
                )
                .join(Package, Package.id == DQM.ref_id)
                .join(Group, Group.id == Package.owner_org)
                .join(JobDQ, DQM.job_id == JobDQ.job_id)
                .filter(DQM.type == 'package', *base_filters)
            )
            if org_id:
                data_quality = data_quality.filter(JobDQ.org_id == org_id)

            data_quality = data_quality.order_by(DQM.modified_at.desc()).all()

        else:
            quality_type = 'resource'
            data_quality = (
                Session.query(
                    DQM.package_id,
                    Resource.name.label('resource_name'),
                    Resource.id.label('resource_id'),
                    Package.name.label('package_name'),
                    Package.title.label('package_title'),
                    Group.title.label('org_title'),
                    Group.id.label('org_id'),
                    DQM.openness.label('openness'),
                    DQM.timeliness.label('timeliness'),
                    DQM.acc_latency.label('acc_latency'),
                    DQM.freshness.label('freshness'),
                    DQM.availability.label('availability'),
                    DQM.downloadable.label('downloadable'),
                    DQM.access_api.label('access_api'),
                    DQM.relevance.label('relevance'),
                    DQM.utf8.label('utf8'),
                    DQM.preview.label('preview'),
                    DQM.completeness.label('completeness'),
                    DQM.uniqueness.label('uniqueness'),
                    DQM.validity.label('validity'),
                    DQM.consistency.label('consistency'),
                    DQM.format.label('format'),
                    DQM.file_size.label('file_size'),
                    DQM.execute_time.label('execute_time'),
                    DQM.error.label('error'),
                    DQM.metrics.label('metrics'),
                    JobDQ.org_parent_id,
                    DQM.type.label('dq_type'),
                    DQM.modified_at.label('modified_at')
                )
                .join(Resource, Resource.id == DQM.ref_id)
                .join(Package, Package.id == DQM.package_id)
                .join(Group, Group.id == Package.owner_org)
                .join(JobDQ, DQM.job_id == JobDQ.job_id)
                .filter(DQM.type == 'resource', *base_filters)
            )
            if org_id:
                data_quality = data_quality.filter(JobDQ.org_id == org_id)

            # มี package_id → กรองเฉพาะ package นั้น (ยกเว้น export_all ซึ่งเราแยกไปแล้ว)
            if package_id:
                data_quality = data_quality.filter(DQM.package_id == package_id)

            data_quality = data_quality.order_by(DQM.modified_at.desc()).all()
    # log.debug(data_quality[0].keys())
    if export:
        # Export logic here, e.g., to CSV or JSON
        return export_data_quality(data_quality, quality_type)
        

    extra_vars = {
        'parents': parents,
        'children_by_parent': children_by_parent,
        'sub_options': sub_options,
        'selected_main': selected_main,
        'selected_sub': selected_sub,
        'versions': versions,
        'selected_version': selected_version,
        'version_options': version_options,
        'main_orgs': _get_org_main(),
        'orgs': _get_org(),
        'org_id': org_id if org_id is not None else '',
        'reports': data_quality,
        'title': toolkit._('รายงานคุณภาพชุดข้อมูลเปิดสำหรับ ผู้ดูแลระบบ'),
        'quality_type': quality_type,
        'admin_report': True,
        'user': toolkit.c.user,
        'userobj': toolkit.c.userobj,
        'site_url': toolkit.request.host_url,
        'site_title': toolkit.config.get('ckan.site_title'),
        'ckan_version': toolkit.config.get('ckan.version'),
        'ckanext_opendquality_version': toolkit.config.get('ckanext-opendquality.version'),
        'enpoint_public': EXEMPT_ENDPOINTS,
        # 'external_dashboard': external_stats
    }
    return toolkit.render('ckanext/opendquality/admin_reports.html', extra_vars)

def dashboard(org_id=None):

    selected_sub = request.args.get('org_sub', None)
    selected_main = request.args.get('org_main', None)
    selected_version = request.args.get('ver_selected', None)
    versions = build_hierachy_with_versions()
    if not as_agency:
        parents ,children_by_parent = build_hierachy_with_orgs()
        sub_options = children_by_parent.get(selected_main, []) if selected_main else []
    else:
        sub_options = build_agency_orgs()
        parents = children_by_parent  = None
    version_options = versions.get(selected_sub, []) if selected_sub else []
    org_id = org_id if org_id is not None else selected_sub
    if selected_version:
        try:
            selected_version = datetime.strptime(selected_version, '%Y-%m-%d').date()
        except ValueError:
            selected_version = None

    resource_format_count = get_resource_format_counts(org_id)
    priority = ["PDF", "XLSX", "CSV", "JSON", "XLS", "XML", "TXT"]
    others = sorted([k for k in resource_format_count if k not in priority])
    resource_format_labels = priority + others
    resource_format_data = [resource_format_count.get(lbl, 0) for lbl in resource_format_labels]


    extra_vars = {
        'parents': parents,
        'children_by_parent': children_by_parent,
        'sub_options': sub_options,
        'selected_main': selected_main,
        'selected_sub': selected_sub,
        'versions': versions,
        'selected_version': selected_version,
        'version_options': version_options,
        'orgs': _get_org(),
        'org_id': org_id if org_id is not None else '',
        'title': toolkit._('ผลการตรวจคุณภาพชุดข้อมูลเปิด'),
        'dashboard': True,
        'radar_data': get_radar_aggregate_all(org_id, selected_version),
        'timeliness_summary': get_timeliness_summary(org_id, selected_version),
        'counts': qa_counts(org_id, selected_version),
        'openness_score': get_openness_score(org_id, selected_version),
        'openness_count': get_openness_counts(org_id, selected_version),
        'validity_count': get_validity_counts(org_id, selected_version),
        'quality_count': get_quality_counts(org_id, selected_version),
        'resource_format_count': {'labels': resource_format_labels, 'data': resource_format_data},
        'top_relevance': get_relevance_top(org_id, selected_version),
        'metrics': qa_detail_blocks(org_id, selected_version),
        'user': toolkit.c.user,
        'userobj': toolkit.c.userobj,
        'site_url': toolkit.request.host_url,
        'site_title': toolkit.config.get('ckan.site_title'),
        'ckan_version': toolkit.config.get('ckan.version'),
        'ckanext_opendquality_version': toolkit.config.get('ckanext-opendquality.version'),
        'enpoint_public': EXEMPT_ENDPOINTS,
        # 'external_dashboard': external_stats
    }
    return toolkit.render('ckanext/opendquality/dashboard.html', extra_vars)

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

qa.add_url_rule('/', endpoint="index", view_func=home, methods=['GET', 'POST'])
qa.add_url_rule('/calculate', endpoint="index", view_func=home, methods=['GET', 'POST'])
# qa.add_url_rule('/calculate_quality', view_func=calculate_quality)
# qa.add_url_rule('/reports', endpoint="reports", view_func=quality_reports)
qa.add_url_rule('/admin_report', endpoint="admin_report", view_func=admin_report)
qa.add_url_rule('/admin_report/<org_id>', endpoint="admin_report", view_func=admin_report)
qa.add_url_rule('/dashboard', endpoint="dashboard", view_func=dashboard)
qa.add_url_rule('/dashboard/<org_id>', endpoint="dashboard", view_func=dashboard)
qa.add_url_rule('/cancel_job', endpoint="cancel_job", view_func=cancel_job, methods=['POST'])
qa.add_url_rule('/delete_job', endpoint="delete_job", view_func=delete_job, methods=['POST'])