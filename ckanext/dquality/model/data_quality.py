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

import datetime

import ckan.logic as logic
from ckan import model
from ckan.model.meta import metadata, mapper, Session, engine
from ckan.model.types import make_uuid
from ckan.model.domain_object import DomainObject
import ckan.lib.dictization as d

from sqlalchemy import types, ForeignKey, Column, Table, desc, asc, or_


data_quality_metrics_table = Table(
    'data_quality_metrics',
    metadata,
    Column('id', types.UnicodeText,
           primary_key=True, default=make_uuid),
    Column('created_at', types.DateTime,
           default=datetime.datetime.utcnow),
    Column('modified_at', types.DateTime,
           default=datetime.datetime.utcnow),
    Column('type', types.UnicodeText, nullable=False),
    Column('ref_id', types.UnicodeText, nullable=False),
    Column('package_id', types.UnicodeText, nullable=True),
    Column('resource_last_modified', types.DateTime),
    Column('openness', types.Float),
    Column('timeliness', types.Float),
    Column('acc_latency', types.Float),
    Column('freshness', types.Float),
    Column('availability', types.Float),
    Column('downloadable', types.Float),
    Column('access_api', types.Float),
    Column('relevance', types.Float),
    Column('utf8', types.Float),
    Column('preview', types.Float),
    Column('completeness', types.Float),
    Column('uniqueness', types.Float),
    Column('validity', types.Float),   
    Column('consistency', types.Float),
    Column('format', types.String),
    Column('file_size', types.Float),
    Column('execute_time', types.Float),
    Column('error', types.String),
    Column('url', types.String),
    Column('metrics', types.JSON),
    Column('job_id', types.UnicodeText, ForeignKey('data_quality_job.job_id'), nullable=True)
)


class DataQualityMetrics(DomainObject):

    @classmethod
    def get(cls, _type, ref_id):
        return (Session.query(cls)
                       .filter_by(type=_type, ref_id=ref_id)
                       .order_by(data_quality_metrics_table.c.modified_at.desc())
                       .first())

    @classmethod
    def get_dataset_metrics(cls, ref_id):
        return cls.get('package', ref_id)

    @classmethod
    def get_resource_metrics(cls, ref_id):
        return cls.get('resource', ref_id)

    @classmethod
    def update(cls, filter, metrics):
        obj = Session.query(cls).filter_by(**filter)
        if isinstance(metrics, DataQualityMetrics):
            metrics = d.table_dictize(metrics, {'model': cls})
        obj.update(metrics)
        Session.commit()

        return obj.first()

    @classmethod
    def remove(cls, _type, ref_id):
        #pass
        obj = Session.query(cls).filter_by(type=_type, ref_id=ref_id).first()
        if obj:
            Session.delete(obj)
            Session.commit()

mapper(DataQualityMetrics, data_quality_metrics_table)

# ตารางใหม่ job_dq
job_dq_table = Table(
    'data_quality_job',
    metadata,
    Column('job_id', types.UnicodeText,
           primary_key=True, default=make_uuid),
    Column('org_parent_id', types.String),
    Column('org_parent_name', types.String),
    Column('org_id', types.String),
    Column('org_name', types.String),
    Column('status', types.String, nullable=False),
    Column('requested_timestamp', types.Date, default=datetime.date.today),
    Column('started_timestamp', types.DateTime),
    Column('finish_timestamp', types.DateTime),
    Column('run_type', types.String),
    Column('execute_time', types.Float),
    Column('active', types.Boolean, nullable=True)
)


class JobDQ(DomainObject):
    @classmethod
    def get(cls, job_id):
        return Session.query(cls).filter_by(job_id=job_id).first()

    @classmethod
    def update_status(cls, job_id, status, error=None, finish_time=None):
        obj = Session.query(cls).filter_by(job_id=job_id).first()
        if obj:
            obj.status = status
            obj.error = error
            obj.finish_timestamp = finish_time or datetime.datetime.utcnow()
            Session.commit()
        return obj

mapper(JobDQ, job_dq_table)

def setup():
    metadata.create_all(engine)