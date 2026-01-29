from sqlalchemy import func, desc, cast, Integer, case, Float, and_, Numeric
from ckan.model import package_table, Session, Package, Group, Resource
from ckanext.dquality.model import DataQualityMetrics as DQM, JobDQ
from sqlalchemy.dialects import postgresql


order_col = getattr(DQM, "created_at", DQM.id)

LABELS = [
    "Validity", "Completeness", "Consistency",
    "Timeliness", "Relevancy", "Availability"
]

def _clip(x):
    if x is None: 
        return 0.0
    try:
        x = float(x)
    except Exception:
        x = 0.0
    return max(0.0, min(100.0, x))

# def get_radar_aggregate_all(org_id=None, version=None):
#     latest = (
#         Session.query(
#             DQM.ref_id.label("ref_id"),
#             DQM.job_id,
#             DQM.validity, DQM.completeness, DQM.consistency,
#             DQM.freshness, DQM.relevance, DQM.availability,
#             DQM.type,
#             func.row_number().over(
#                 partition_by=DQM.ref_id,
#                 order_by=desc(order_col)
#             ).label("rn"),
#         )
#         # .filter(DQM.type == "package")  # ใส่ถ้าแยกชนิดไว้
#     ).subquery("latest")
    
#     q = (
#         Session.query(
#             func.avg(latest.c.validity),
#             func.avg(latest.c.completeness),
#             func.avg(latest.c.consistency),
#             func.avg(latest.c.freshness),
#             func.avg(latest.c.relevance),
#             func.avg(latest.c.availability),
#         )
#         .select_from(Package)
#         .join(latest, latest.c.ref_id == Package.id)
#         .join(Group, Group.id == Package.owner_org)
#         .join(JobDQ, latest.c.job_id == JobDQ.job_id)
#         .filter(latest.c.type == 'package', JobDQ.status == 'finish', JobDQ.run_type == 'organization')
#     )
#     if version is not None:
#         q = q.filter(JobDQ.requested_timestamp == version)
#     else:
#         q = q.filter(JobDQ.active == True)
#     if org_id is not None:
#         q = q.filter(Group.id == org_id)

#     v_valid, v_comp, v_cons, v_fresh, v_rel, v_avail = q.one()

#     values = [_clip(v) for v in (v_valid, v_comp, v_cons, v_fresh, v_rel, v_avail)]
#     return {
#         "labels": LABELS,
#         "data": values,
#         "label": "All datasets (avg)"
#     }

def get_radar_aggregate_all(org_id=None, version=None):
    latest = (
        Session.query(
            DQM.ref_id.label("ref_id"),
            DQM.job_id,
            DQM.validity, DQM.completeness, DQM.consistency,
            DQM.timeliness, DQM.relevance, DQM.availability,
            DQM.type,
            func.row_number().over(
                partition_by=DQM.ref_id,
                order_by=desc(order_col)
            ).label("rn"),
        )
    ).subquery("latest")

    base_filters = [
        latest.c.type == 'package',
        JobDQ.status == 'finish',
        JobDQ.run_type == 'organization',
        latest.c.rn == 1
    ]

    q = (
        Session.query(
            func.avg(latest.c.validity),
            func.avg(latest.c.completeness),
            func.avg(latest.c.consistency),
            func.avg(latest.c.relevance),
            func.avg(latest.c.availability),
        )
        .select_from(Package)
        .join(latest, latest.c.ref_id == Package.id)
        .join(Group, Group.id == Package.owner_org)
        .join(JobDQ, latest.c.job_id == JobDQ.job_id)
        .filter(*base_filters)
    )

    # query สำหรับ timeliness (freshness == 0)
    q_time = (
        Session.query(
            func.count().label("total"),
            func.sum(
                case(
                    (latest.c.timeliness == 0, 1),
                    else_=0
                )
            ).label("bad")
        )
        .select_from(Package)
        .join(latest, latest.c.ref_id == Package.id)
        .join(Group, Group.id == Package.owner_org)
        .join(JobDQ, latest.c.job_id == JobDQ.job_id)
        .filter(*base_filters)
    )

    if version is not None:
        q = q.filter(JobDQ.requested_timestamp == version)
        q_time = q_time.filter(JobDQ.requested_timestamp == version)
    else:
        q = q.filter(JobDQ.active == True)
        q_time = q_time.filter(JobDQ.active == True)

    if org_id is not None:
        q = q.filter(Group.id == org_id)
        q_time = q_time.filter(Group.id == org_id)

    v_valid, v_comp, v_cons, v_rel, v_avail = q.one()
    total, bad = q_time.one()

    # คำนวณ timeliness ตามสูตรใหม่
    v_timeliness = (bad / total * 100) if total else 0

    values = [
        _clip(v_valid),
        _clip(v_comp),
        _clip(v_cons),
        _clip(v_timeliness),  # << แทน freshness เดิม
        _clip(v_rel),
        _clip(v_avail),
    ]

    return {
        "labels": LABELS,
        "data": values,
        "label": "All datasets (avg)"
    }


def qa_counts(org_id=None, version=None):
    qa_pkg_ids = (
        Session.query(Package.id.label("package_id"))
        .join(DQM, DQM.ref_id == Package.id)
        .join(Group, Group.id == Package.owner_org)
        .join(JobDQ, DQM.job_id == JobDQ.job_id)
        .filter(Package.state == "active")
        .filter(DQM.type == "package", JobDQ.status == 'finish', JobDQ.run_type == 'organization')                    # ถ้ามีคอลัมน์ type
    )
    if version is not None:
        qa_pkg_ids = qa_pkg_ids.filter(JobDQ.requested_timestamp == version)
    else:
        qa_pkg_ids = qa_pkg_ids.filter(JobDQ.active == True)

    if org_id:
        qa_pkg_ids = qa_pkg_ids.filter(Group.id == org_id)

    qa_pkg_ids = qa_pkg_ids.distinct().subquery()
    
    dataset_count = Session.query(func.count()).select_from(qa_pkg_ids).scalar()

    org_count = (
        Session.query(func.count(func.distinct(Package.owner_org)))
        .join(qa_pkg_ids, qa_pkg_ids.c.package_id == Package.id)
        .scalar()
    )
    
    # resource_count = (
    #     Session.query(func.count(Resource.id))
    #     .join(qa_pkg_ids, qa_pkg_ids.c.package_id == Resource.package_id)
    #     .join(DQM,)
    #     .filter(Resource.state == "active")
    #     .scalar()
    # )

    resource_count = (
        Session.query(func.count(DQM.id))
        .join(JobDQ, DQM.job_id == JobDQ.job_id)
        .filter(
            DQM.type == "resource",
        )
    )

    if version is not None:
        resource_count = resource_count.filter(JobDQ.requested_timestamp == version)
    else:
        resource_count = resource_count.filter(JobDQ.active == True)
    if org_id:
        resource_count = resource_count.filter(JobDQ.org_id == org_id)

    resource_count = resource_count.scalar()

    return {
        "organizations": org_count or 0,
        "datasets": dataset_count or 0,
        "resources": resource_count or 0,
    }

def qa_detail_blocks(org_id=None, version=None):

    data = (
        Session.query(
            func.sum(case((DQM.downloadable == 1, 1), else_=0)).label("dl_yes"),
            func.sum(case((DQM.downloadable != 1, 1), else_=0)).label("dl_no"),
            func.sum(case((DQM.access_api == 1, 1), else_=0)).label("api_yes"),
            func.sum(case((DQM.access_api != 1, 1), else_=0)).label("api_no")
        )
        .join(JobDQ, DQM.job_id == JobDQ.job_id)
        .filter(DQM.type == 'resource', JobDQ.status == 'finish')
    )
    if version is not None:
        data = data.filter(JobDQ.requested_timestamp == version)
    else:
        data = data.filter(JobDQ.active == True)
    if org_id:
        data = data.filter(JobDQ.org_id == org_id)

    dl_yes, dl_no, api_yes, api_no = data.one()

    return {
        "availability":{"downloadable": {"yes": int(dl_yes or 0), "no": int(dl_no or 0)},
                        "access_api":  {"yes": int(api_yes or 0), "no": int(api_no or 0)}
        }
    }

def get_relevance_top(org_id=None, version=None, limit=5):
    order_col = getattr(DQM, "created_at", DQM.id)
    latest = (
        Session.query(
            DQM.ref_id.label("package_id"),
            DQM.job_id,
            DQM.relevance,
            DQM.type,
            func.row_number().over(
                partition_by=DQM.ref_id,
                order_by=desc(order_col)
            ).label("rn")
        )
    ).subquery("latest")

    q = (
        Session.query(
            Package.id, Package.title, Package.name,
            Group.title.label("org_title"), Group.name.label("org_name"),
            Group.id.label("org_id"),
            JobDQ.org_parent_id.label('parent_id'),
            latest.c.relevance
        )
        .join(latest, latest.c.package_id == Package.id)
        .join(Group, Group.id == Package.owner_org)
        .join(JobDQ, latest.c.job_id == JobDQ.job_id)
        .filter(latest.c.type == "package", JobDQ.status == 'finish', JobDQ.run_type == 'organization')
        .filter(Package.state == "active")
    )

    if version is not None:
        q = q.filter(JobDQ.requested_timestamp == version)
    else:
        q = q.filter(JobDQ.active == True)

    if org_id:
        q = q.filter(Group.id == org_id)

    q = q.order_by(desc(latest.c.relevance)).limit(limit)

    results = []
    for pid, title, name, org_title, org_name, org_id, parent_id, relevance in q.all():
        results.append({
            "id": pid,
            "title": title,
            "name": name,
            "org": {
                "title": org_title,
                "name": org_name,
                "id": org_id,
                "parent_id": parent_id
            },
            "relevance": float(relevance or 0)
        })

    return results

def get_timeliness_summary(org_id=None, version=None):
    B1_ON_SCHEDULE = case([(DQM.timeliness == -1, 1)], else_=0)
    B2_UPTODATE = case([(DQM.timeliness == 0, 1)], else_=0)
    B3_NEEDS_ATTENTION = case([(DQM.timeliness == 1, 1)], else_=0)
    B4_SHOULD_IMPROVE = case([(DQM.timeliness == 2, 1)], else_=0)
    B5_MUST_IMPROVE = case([(DQM.timeliness == 3, 1)], else_=0)
    # case(
    #     [((DQM.acc_latency > 25) & (DQM.acc_latency <= 50), 1)], else_=0
    # )
    # B5_MUST_IMPROVE = case([(DQM.timeliness == 3, 1)], else_=0)
    OUTDATED_THRESHOLD = 4

    q = Session.query(
        func.avg(DQM.freshness).label('avg_freshness'),
        func.sum(B1_ON_SCHEDULE).label('b1'),
        func.sum(B2_UPTODATE).label('b2'),
        func.sum(B3_NEEDS_ATTENTION).label('b3'),
        func.sum(B4_SHOULD_IMPROVE).label('b4'),
        func.sum(B5_MUST_IMPROVE).label('b5'),
        func.sum(case((DQM.timeliness == OUTDATED_THRESHOLD, 1), else_=0)).label('outdated'),
        func.max(DQM.acc_latency).label('max_latency')
    ).join(Package, Package.id == DQM.ref_id)\
     .join(Group, Group.id == Package.owner_org)\
     .join(JobDQ, DQM.job_id == JobDQ.job_id)\
     .filter(DQM.type == 'package', JobDQ.status == 'finish', JobDQ.run_type == 'organization')

    cond = []
    if org_id:
        cond.append(Group.id == org_id)
    
    if version is not None:
        cond.append(JobDQ.requested_timestamp == version)
    else:
        cond.append(JobDQ.active == True)
    # if package_id:
    #     cond.append(Package.id == package_id)
    # if date_from:
    #     cond.append(cast(JobDQ.created_at, Date) >= date_from)
    # if date_to:
    #     cond.append(cast(JobDQ.created_at, Date) <= date_to)
    if cond:
        q = q.filter(and_(*cond))

    r = q.one()

    return {
        "avg_freshness": float(r.avg_freshness or 0),
        "no_schedules": int(r.b1 or 0),
        "uptodate": int(r.b2 or 0),
        "total_late_update": int((r.b3 or 0) + (r.b4 or 0) + (r.b5 or 0)),
        "latency_buckets": {
            "ล่าช้าค่อนข้างมาก": int(r.b5 or 0),
            "ล่าช้าป่านกลาง": int(r.b4 or 0),
            "ล่าช้าเล็กน้อย": int(r.b3 or 0),
        },
        "outdated_count": int(r.outdated or 0),
        "max_latency": int(r.max_latency or 0)
    }

def get_openness_score(org_id=None, version=None):
    query = (
        Session.query(
            case(
                (DQM.openness == 0, 'Other'),
                else_=func.concat(DQM.openness, ' Star')
            ).label('openness_level'),
            func.count(DQM.id).label('count')
        )
        .join(JobDQ, DQM.job_id == JobDQ.job_id)
        .filter(DQM.type == 'package', JobDQ.status == 'finish', JobDQ.run_type == 'organization')
        .group_by('openness_level')
    )

    if version is not None:
        query = query.filter(JobDQ.requested_timestamp == version)
    else:
        query = query.filter(JobDQ.active == True)

    if org_id:
        query = query.filter(JobDQ.org_id == org_id)
    
    return {row.openness_level: row.count for row in query.all()}

def get_openness_counts(org_id=None, version=None):
    data_type_expr = case(
        (DQM.openness.in_([0, 1]), 'unstructured_resources'),
        else_='structured_resources'
    ).label('data_type')

    data_type_package_expr = case(
        (DQM.openness.in_([0, 1]), 'unstructured_packages'),
        else_='structured_packages'
    ).label('data_type')

    query = (
        Session.query(
            data_type_expr,
            func.count(DQM.id).label('count')
        )
        .join(JobDQ, DQM.job_id == JobDQ.job_id)
        .filter(
            DQM.type == 'resource',
            JobDQ.status == 'finish',
            JobDQ.run_type == 'organization',
            DQM.openness.isnot(None)
        )
    )

    query_package = (
        Session.query(
            data_type_package_expr,
            func.count(DQM.id).label('count')
        )
        .join(JobDQ, DQM.job_id == JobDQ.job_id)
        .filter(
            DQM.type == 'package',
            JobDQ.status == 'finish',
            JobDQ.run_type == 'organization',
            DQM.openness.isnot(None)
        )
    )


    if version is not None:
        query = query.filter(JobDQ.requested_timestamp == version)
        query_package = query_package.filter(JobDQ.requested_timestamp == version)
    else:
        query = query.filter(JobDQ.active == True)
        query_package = query_package.filter(JobDQ.active == True)

    if org_id:
        query = query.filter(JobDQ.org_id == org_id)
        query_package = query_package.filter(JobDQ.org_id == org_id)

    query = query.group_by(data_type_expr)
    query_package = query_package.group_by(data_type_package_expr)

    rows = query.all()
    rows_package = query_package.all()
    summary = {'structured_packages': 0, 'unstructured_packages': 0, 'structured_resources': 0, 'unstructured_resources': 0}
    for row in rows:
        if row.data_type in summary:
            summary[row.data_type] = row.count
    for row in rows_package:
        if row.data_type in summary:
            summary[row.data_type] = row.count

    return summary

def get_validity_counts(org_id=None, version=None):
    validity_value = cast(
        cast(
            func.coalesce(
                DQM.metrics
                    .op('->')('validity')
                    .op('->>')('max_validity'),
                '0'
            ),
            Numeric
        ),
        Integer
    )
    query = (
        Session.query(
            case(
                (DQM.validity < 100, 'un_validity_resources'),
                (DQM.validity == 100, 'validity_resources'),
                else_='other'
            ).label('validity_type'),
            func.count(DQM.id).label('count')
        )
        .join(JobDQ, DQM.job_id == JobDQ.job_id)
        .filter(
            DQM.type == 'resource',
            JobDQ.status == 'finish',
            JobDQ.run_type == 'organization'
        )
    )

    query_package = (
        Session.query(
            case(
                (validity_value < 100, 'un_validity_packages'),
                (validity_value == 100, 'validity_packages'),
                else_='other'
            ).label('validity_type'),
            func.count(DQM.id).label('count')
        )
        .join(JobDQ, DQM.job_id == JobDQ.job_id)
        .filter(
            DQM.type == 'package',
            JobDQ.status == 'finish',
            JobDQ.run_type == 'organization'
        )
    )

    if version is not None:
        query = query.filter(JobDQ.requested_timestamp == version)
        query_package = query_package.filter(JobDQ.requested_timestamp == version)
    else:
        query = query.filter(JobDQ.active == True)
        query_package = query_package.filter(JobDQ.active == True)

    if org_id:
        query = query.filter(JobDQ.org_id == org_id)
        query_package = query_package.filter(JobDQ.org_id == org_id)

    # group by
    query = query.group_by('validity_type')
    result = query.all()

    query_package = query_package.group_by('validity_type')
    result_package = query_package.all()

    summary = {'validity_resources': 0, 'un_validity_resources': 0, 'validity_packages': 0, 'un_validity_packages': 0}
    for row in result:
        if row.validity_type in summary:
            summary[row.validity_type] = row.count
    for row in result_package:
        if row.validity_type in summary:
            summary[row.validity_type] = row.count

    return summary

def get_quality_counts(org_id=None, version=None):
    subquery = (
        Session.query(
            DQM.ref_id.label('package_id'),
            func.avg(DQM.validity).label('avg_validity'),
            func.avg(DQM.completeness).label('avg_completeness'),
            func.avg(DQM.consistency).label('avg_consistency'),
            func.avg(DQM.availability).label('avg_availability'),
            func.avg(DQM.freshness).label('avg_freshness')
        )
        .join(JobDQ, DQM.job_id == JobDQ.job_id)
        .filter(
            DQM.type == 'package',
            JobDQ.status == 'finish',
            JobDQ.run_type == 'organization'
        )
    )

    if version is not None:
        subquery = subquery.filter(JobDQ.requested_timestamp == version)
    else:
        subquery = subquery.filter(JobDQ.active == True)

    if org_id:
        subquery = subquery.filter(JobDQ.org_id == org_id)

    subquery = subquery.group_by(DQM.ref_id).subquery()
    
    query = (
        Session.query(
            case(
                (
                    and_(
                        subquery.c.avg_validity == 100,
                        subquery.c.avg_completeness == 100,
                        subquery.c.avg_consistency == 100,
                        subquery.c.avg_availability == 100,
                        subquery.c.avg_freshness > 0
                    ),
                    'good_quality'
                ),
                else_='need_improvement'
            ).label('quality_type'),
            func.count().label('count')
        )
        .group_by('quality_type')
    )

    result = query.all()

    summary = {
        'good_quality': 0,
        'need_improvement': 0
    }

    for row in result:
        summary[row.quality_type] = row.count

    return summary

def get_resource_format_counts(org_id=None, version=None):
    fmt_norm = func.coalesce(
        func.nullif(func.upper(func.trim(DQM.format)), ''), 'UNKNOWN'
    ).label('format')

    q = (
        Session.query(
            fmt_norm,
            func.count(DQM.id).label('count')
        )
        .join(JobDQ, DQM.job_id == JobDQ.job_id)
        .filter(
            DQM.type == 'resource',
            JobDQ.status == 'finish',
            JobDQ.run_type == 'organization',
            DQM.error != 'Connection timed out'
        )
        .group_by(fmt_norm)
        .order_by(func.count(DQM.id).desc())
    )

    if version is not None:
        q = q.filter(JobDQ.requested_timestamp == version)
    else:
        q = q.filter(JobDQ.active == True)

    if org_id:
        q = q.filter(JobDQ.org_id == org_id)

    rows = q.all()
    counts = {r.format: r.count for r in rows}
    # ตั้งค่า default ให้ format สำคัญ ๆ (ถ้ายังไม่มีให้เป็น 0)
    base = ["PDF", "XLSX", "CSV", "JSON", "XLS", "XML", "TXT"]
    for k in base:
        counts.setdefault(k, 0)

    return counts
    