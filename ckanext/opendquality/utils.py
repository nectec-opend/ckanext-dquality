from sqlalchemy import func, desc, cast, Integer, case, Float
from ckan.model import package_table, Session, Package, Group, Resource
from ckanext.opendquality.model import DataQualityMetrics as DQM
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

def get_radar_aggregate_all(org_id=None):
    # เลือก DQM แถวล่าสุดของแต่ละ package
    latest = (
        Session.query(
            DQM.ref_id.label("ref_id"),
            DQM.validity, DQM.completeness, DQM.consistency,
            DQM.freshness, DQM.relevance, DQM.availability,
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
        .filter(latest.c.rn == 1)
    )
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

def qa_counts(org_id=None):
    # 1) เลือกชุดข้อมูลที่ "มีแถวใน DQM" (กันซ้ำด้วย DISTINCT)
    qa_pkg_ids = (
        Session.query(Package.id.label("package_id"))
        .join(DQM, DQM.ref_id == Package.id)              # ผูก DQM กับ dataset
        .join(Group, Group.id == Package.owner_org)       # ผูกกับหน่วยงาน
        .filter(Package.state == "active")                # นับเฉพาะชุดข้อมูล active
        .filter(DQM.type == "package")                    # ถ้ามีคอลัมน์ type
    )
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

def qa_detail_blocks(org_id=None):
    order_col = getattr(DQM, "created_at", DQM.id)
    latest = (
        Session.query(
            DQM.ref_id.label("package_id"),
            DQM.metrics,
            DQM.downloadable,
            DQM.access_api,
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
        .filter(latest.c.rn == 1)
    )
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