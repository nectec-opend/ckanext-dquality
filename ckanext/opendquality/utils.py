from sqlalchemy import func, desc, cast, Integer, case, Float, and_
from ckan.model import package_table, Session, Package, Group, Resource
from ckanext.opendquality.model import DataQualityMetrics as DQM, JobDQ
from sqlalchemy.dialects import postgresql

# ใช้ created_at ถ้ามี; ไม่งั้นใช้ id แทน
order_col = getattr(DQM, "created_at", DQM.id)

LABELS = [
    "Validity", "Completeness", "Consistency",
    "Timeliness (Freshness)", "Relevancy", "Availability"
]

def _clip(x):
    if x is None: 
        return 0.0
    try:
        x = float(x)
    except Exception:
        x = 0.0
    return max(0.0, min(100.0, x))

def get_radar_aggregate_all(org_id=None, version=None):
    # เลือก DQM แถวล่าสุดของแต่ละ package
    latest = (
        Session.query(
            DQM.ref_id.label("ref_id"),
            DQM.job_id,
            DQM.validity, DQM.completeness, DQM.consistency,
            DQM.freshness, DQM.relevance, DQM.availability,
            DQM.type,
            func.row_number().over(
                partition_by=DQM.ref_id,
                order_by=desc(order_col)
            ).label("rn"),
        )
        # .filter(DQM.type == "package")  # ใส่ถ้าแยกชนิดไว้
    ).subquery("latest")

    # รวมเป็นค่าเฉลี่ยหนึ่งชุด
    q = (
        Session.query(
            func.avg(latest.c.validity),
            func.avg(latest.c.completeness),
            func.avg(latest.c.consistency),
            func.avg(latest.c.freshness),
            func.avg(latest.c.relevance),
            func.avg(latest.c.availability),
        )
        .select_from(Package)
        .join(latest, latest.c.ref_id == Package.id)
        .join(Group, Group.id == Package.owner_org)
        .join(JobDQ, latest.c.job_id == JobDQ.job_id)
        .filter(latest.c.type == 'package', JobDQ.status == 'finish', JobDQ.run_type == 'organization')
    )
    if version is not None:
        q = q.filter(JobDQ.requested_timestamp == version)
    else:
        q = q.filter(JobDQ.active == True)
    if org_id is not None:
        q = q.filter(Group.id == org_id)

    v_valid, v_comp, v_cons, v_fresh, v_rel, v_avail = q.one()

    values = [_clip(v) for v in (v_valid, v_comp, v_cons, v_fresh, v_rel, v_avail)]

    # รูปแบบพร้อมวาดเรดาร์ชาร์ต (เช่น Chart.js/ECharts)
    return {
        "labels": LABELS,
        "data": values,          # ชุดเดียวรวมทุก package
        # ถ้าต้องการแสดงชื่อชุดบนกราฟ:
        "label": "All datasets (avg)"
    }

def qa_counts(org_id=None, version=None):
    # 1) เลือกชุดข้อมูลที่ "มีแถวใน DQM" (กันซ้ำด้วย DISTINCT)
    qa_pkg_ids = (
        Session.query(Package.id.label("package_id"))
        .join(DQM, DQM.ref_id == Package.id)              # ผูก DQM กับ dataset
        .join(Group, Group.id == Package.owner_org)       # ผูกกับหน่วยงาน
        .join(JobDQ, DQM.job_id == JobDQ.job_id)  # ผูกกับ JobDQ
        .filter(Package.state == "active")                # นับเฉพาะชุดข้อมูล active
        .filter(DQM.type == "package", JobDQ.status == 'finish', JobDQ.run_type == 'organization')                    # ถ้ามีคอลัมน์ type
    )
    if version is not None:
        qa_pkg_ids = qa_pkg_ids.filter(JobDQ.requested_timestamp == version)
    else:
        qa_pkg_ids = qa_pkg_ids.filter(JobDQ.active == True)

    if org_id:
        qa_pkg_ids = qa_pkg_ids.filter(Group.id == org_id)

    qa_pkg_ids = qa_pkg_ids.distinct().subquery()         # <-- รายการ dataset ที่เข้าเกณฑ์

    # 2) นับ datasets (จำนวน row ใน subquery)
    dataset_count = Session.query(func.count()).select_from(qa_pkg_ids).scalar()

    # 3) นับ organizations ที่มี dataset ในรายการนี้
    org_count = (
        Session.query(func.count(func.distinct(Package.owner_org)))
        .join(qa_pkg_ids, qa_pkg_ids.c.package_id == Package.id)
        .scalar()
    )

    # 4) นับ resources ใต้ datasets เหล่านี้ (เฉพาะ active)
    resource_count = (
        Session.query(func.count(Resource.id))
        .join(qa_pkg_ids, qa_pkg_ids.c.package_id == Resource.package_id)
        .filter(Resource.state == "active")
        .scalar()
    )

    return {
        "organizations": org_count or 0,
        "datasets": dataset_count or 0,
        "resources": resource_count or 0,
    }

def qa_detail_blocks(org_id=None, version=None):
    order_col = getattr(DQM, "created_at", DQM.id)
    latest = (
        Session.query(
            DQM.ref_id.label("package_id"),
            DQM.job_id,
            DQM.metrics,
            DQM.downloadable,
            DQM.access_api,
            DQM.type,
            func.row_number().over(
                partition_by=DQM.ref_id,
                order_by=desc(order_col)
            ).label("rn")
        )
    ).subquery("latest")

    base_q = (
        Session.query(
            Package.id.label("pid"),
            Group.id.label("gid"),
            latest.c.metrics,
            latest.c.downloadable,
            latest.c.access_api,
        )
        .join(latest, latest.c.package_id == Package.id)
        .join(Group, Group.id == Package.owner_org)
        .join(JobDQ, latest.c.job_id == JobDQ.job_id)
        .filter(latest.c.type == 'package', JobDQ.status == 'finish', JobDQ.run_type == 'organization')
    )
    if version is not None:
        base_q = base_q.filter(JobDQ.requested_timestamp == version)
    else:
        base_q = base_q.filter(JobDQ.active == True)

    if org_id:
        base_q = base_q.filter(Group.id == org_id)

    base = base_q.subquery("base")

    # ---- JSON helpers (metrics->>'key') ----
    METRICS = base.c.metrics
    if not isinstance(METRICS.type, (postgresql.JSONB, postgresql.JSON)):
        METRICS = cast(METRICS, postgresql.JSONB)

    def jsum_int(key):
        return func.coalesce(func.sum(cast(METRICS.op('->>')(key), Integer)), 0)

    # ---- numeric truthiness (คอลัมน์เป็น float 0/1) ----
    dl_true  = (func.coalesce(cast(base.c.downloadable, Float), 0) > 0)
    api_true = (func.coalesce(cast(base.c.access_api,  Float), 0) > 0)

    agg = Session.query(
        jsum_int('blank_header'),
        jsum_int('duplicate_header'),
        jsum_int('extra_value'),
        jsum_int('downloads'),
        jsum_int('views'),

        func.sum(case((dl_true,  1), else_=0)),   # dl_yes
        func.sum(case((dl_true,  0), else_=1)),   # dl_no
        func.sum(case((api_true, 1), else_=0)),   # api_yes
        func.sum(case((api_true, 0), else_=1)),   # api_no

        func.count(base.c.pid)                    # total datasets considered
    )

    (blank, dup, extra, dw, vw,
     dl_yes, dl_no, api_yes, api_no, total) = agg.one()

    return {
        "validity":    {"blank_header": int(blank or 0),
                        "duplicate_header": int(dup or 0),
                        "extra_value": int(extra or 0)},
        "relevancy":   {"downloads": int(dw or 0), "views": int(vw or 0)},
        "availability":{"downloadable": {"yes": int(dl_yes or 0), "no": int(dl_no or 0)},
                        "access_api":  {"yes": int(api_yes or 0), "no": int(api_no or 0)},
                        "total": int(total or 0)},
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
    B1_NONE_UPDATE = case([(DQM.acc_latency.is_(None), 1)], else_=0)
    B2_ON_SCHEDULE = case([(DQM.acc_latency <= 0, 1)], else_=0)
    B3_NEEDS_ATTENTION = case(
        [((DQM.acc_latency > 0) & (DQM.acc_latency <= 7), 1)], else_=0
    )
    B4_SHOULD_IMPROVE = case(
        [((DQM.acc_latency > 7) & (DQM.acc_latency <= 30), 1)], else_=0
    )
    B5_MUST_IMPROVE = case([(DQM.acc_latency > 30, 1)], else_=0)                        # ต้องปรับปรุง
    OUTDATED_THRESHOLD = 30   # ปรับได้ตามนโยบายองค์กร (เช่น 30/60/90 วัน)

    q = Session.query(
        func.avg(DQM.freshness).label('avg_freshness'),
        func.sum(B1_NONE_UPDATE).label('b1'),
        func.sum(B2_ON_SCHEDULE).label('b2'),
        func.sum(B3_NEEDS_ATTENTION).label('b3'),
        func.sum(B4_SHOULD_IMPROVE).label('b4'),
        func.sum(B5_MUST_IMPROVE).label('b5'),
        func.sum(case((DQM.acc_latency > OUTDATED_THRESHOLD, 1), else_=0)).label('outdated'),
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
        "latency_buckets": {
            "ไม่มีการอัพเดตหลังจัดเก็บ": int(r.b1 or 0),
            "อัพเดตตามรอบ": int(r.b2 or 0),
            "รบกวนปรับปรุง": int(r.b3 or 0),
            "ควรปรับปรุง": int(r.b4 or 0),
            "ต้องปรับปรุง": int(r.b5 or 0)
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
        (DQM.openness.in_([0, 1]), 'unstructured'),
        else_='structured'
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

    if version is not None:
        query = query.filter(JobDQ.requested_timestamp == version)
    else:
        query = query.filter(JobDQ.active == True)

    if org_id:
        query = query.filter(JobDQ.org_id == org_id)

    query = query.group_by(data_type_expr)

    rows = query.all()
    summary = {'structured': 0, 'unstructured': 0}
    for row in rows:
        summary[row.data_type] = row.count

    return summary

def get_validity_counts(org_id=None, version=None):
    query = (
        Session.query(
            case(
                (DQM.validity < 100, 'un_validity'),
                (DQM.validity == 100, 'validity'),
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

    if version is not None:
        query = query.filter(JobDQ.requested_timestamp == version)
    else:
        query = query.filter(JobDQ.active == True)

    if org_id:
        query = query.filter(JobDQ.org_id == org_id)

    # group by
    query = query.group_by('validity_type')
    result = query.all()

    # ค่า default ถ้าไม่มีผลลัพธ์
    summary = {'validity': 0, 'un_validity': 0}
    for row in result:
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

    # แบ่งกลุ่ม good / need improvement
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
    