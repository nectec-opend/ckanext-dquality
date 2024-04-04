# -*- coding: utf-8 -*-
import ckan.plugins.toolkit as toolkit, dateutil
from datetime import datetime, timedelta
from logging import getLogger
import ckan.lib.uploader as uploader
# from ckan.common import config
# from six import text_type
from sqlalchemy import Table, select, join, func, and_
from sqlalchemy.sql import text
# # import ckan.plugins as p
# import ckan.model as model
from ckan.model import Session, User, user_table, Package, PackageExtra
import json
import re
import csv
from goodtables import validate
import pandas as pd
from pandas import read_excel
from tempfile import TemporaryFile
import os.path
import requests
import mimetypes
import openpyxl
from openpyxl import load_workbook
import io
from io import BytesIO
from io import StringIO
import chardet
from six import string_types
import time

# import db, re
from ckanext.opendquality.model import (
    DataQualityMetrics as DataQualityMetricsModel
)

log = getLogger(__name__)
# cache_enabled = p.toolkit.asbool(
#     config.get('ckanext.stats.cache_enabled', False)
# )

# if cache_enabled:
#     log.warn(
#         'ckanext.quality does not support caching in current implementations'
#     )

DATE_FORMAT = '%Y-%m-%d'


class LazyStreamingList(object):
    '''Implements a buffered stream that emulates an iterable object.

    It is used to fetch large data in chunks (pages/buffers). The fetch is
    done using a buffer of predefined size. While the iterator iterates over
    the buffer, when the end of the buffer is reached, a call is made to fetch
    the next buffer before iterating to the next record.

    :param fetch_page: `function`, a function used to fetch the next buffer
        data. The function prototype is:

            .. code-block: python

                def fetch_page(page, page_size):
                    return {
                        'total': 100,
                        'records': [...],
                    }

        The function takes two parameters:
            * `page`, `int`, the page number starting from 0
            * `page_size`, `int`, the number of items per page. Default is 512.

        The function must return a dict containing the records. The dict must
        have the following entries:
            * `total` - total number of items
            * `records` - a list (iterable) over the records fetched for this
                page.
    :param page_size: `int`, the size of each page (buffer). Default is 512.
    '''

    def __init__(self, fetch_page, page_size=512):
        self.fetch_page = fetch_page
        self.page_size = page_size
        self.page = 0
        self.buffer = None
        self.total = 0
    def _fetch_buffer(self):
        if self.total:
            if self.page*self.page_size >= self.total:
                self.buffer = []
                return
        result = self.fetch_page(self.page, self.page_size)
        self.total = result.get('total', 0)
        self.buffer = result.get('records', [])
        self.page += 1

    # def iterator(self):
    #     '''Returns an iterator over all of the records.
    #     '''
    #     current = 0
    #     self._fetch_buffer()
    #     while True:
    #         if current >= self.total or not self.buffer:
    #             # end of all results
    #             raise StopIteration()
    #         for row in self.buffer:
    #             current += 1
    #             yield row
    #         # fetch next buffer
    #         self._fetch_buffer()
    def iterator(self):
        '''Returns an iterator over all of the records.
        '''
        current = 0
        self._fetch_buffer()
        if current >= self.total:
            log.debug('end of all results')
        if self.buffer:
            for row in self.buffer:
                current += 1
                yield row
            self._fetch_buffer()

    def __iter__(self):
        return self.iterator()

    def rewind(self):
        self.buffer = None
        self.page = 0
        self.total = 0
class ResourceCSVData(object):
    '''Represents a CSV data source that can be read with pagination.

    It fetches the CSV data (for a resource) and generates metadata related to
    the CSV file data.
    The column names and data types for each column are calculated:
        * column names are calculated from the firs row in the CSV file. If
            there are multiple columns with the same name, then the column
            names are suffixed with index `_<i>`, for example:
                - if we have the following columns: `a`, `b`, `a`, `a`, then
                the generated columns will have the following names: `a_1`,
                `b`, `a_2` and `a_3`.
        * the data types are guessed from the first row with data (second row
        in the file).

    The data is rerieved in pages. Each page is a `dict` containing:
        * `total` - total number of data rows in the CSV file (the header row
            is not counted)
        * `records` - the records in the requested page - a `list` of `dict`
        * `fields` - `list` of `dict` describing the columns in the CSV file.

    The constructor takes one argument - the data loader for the CSV. This is a
    function (callable) that is called without arguments to load the actual
    data from the CSV file. The expected output of this function is a `list`
    of rows where each row is itself a `list` of values.
    '''

    def __init__(self, resource_loader):
        csv_data = resource_loader()
        self.data = csv_data
        self.fields = self._get_field_types(self._get_fields(csv_data),
                                            csv_data)
        self.column_names = [f['id'] for f in self.fields]
        self.total = 0
        if len(csv_data):
            self.total = len(csv_data) - 1
        # log.debug('%s, Resource CSV data. Total: %d, columns=%s, fields=%s',
        #           str(self), self.total, self.column_names, self.fields)

    def _get_fields(self, csv_data):
        if not csv_data:
            return {}
        columns = csv_data[0]
        columns_count = {}
        for column in columns:
            columns_count[column] = columns_count.get(column, 0) + 1
        numbered = {}
        fields = []
        for column in columns:
            if columns_count[column] > 1:
                numbered[column] = numbered.get(column, 0) + 1
                column = '{}_{}'.format(column, numbered[column])
            fields.append({'id': column})
        return fields

    def _get_field_types(self, fields, csv_data):
        if not csv_data or len(csv_data) == 1:
            for field in fields:
                field['type'] = 'text'
            return fields
        row = csv_data[1]
        for i, field in enumerate(fields):
            field['type'] = self._guess_type(row[i])
        return fields

    def _guess_type(self, value):
        if value is None:
            return 'text'
        if detect_numeric_format(value):
            return 'numeric'
        if detect_date_format(value):
            return 'timestamp'
        return 'text'

    def fetch_page(self, page, limit):
        '''Retrieves one page from the CSV data.

        :param page: `int`, the number of the page to fetch, 0-based.
        :param limit: `int`, the page size.

        :returns: the data page, which is a `dict` containing:
            * `total` - total number of records.
            * `records` - `list` of `dict`, the row (records) in this page.
            * `fields` - `list` of `dict`, metadata for the columns.
        '''
        start = max(page, 0) * limit + 1
        limit = min(start + limit, self.total + 1)
        items = []
        if start < (self.total + 1):
            for i in range(start, limit):
                data_row = self.data[i]
                row_dict = {self.column_names[j]: value
                            for j, value in enumerate(data_row)}
                items.append(row_dict)
        log.debug('ResourceCSVData.fetch_page: '
                  'page=%d, limit=%d, of total %d. Got %d results.',
                  page, limit, self.total, len(items))
        return {
            'total': self.total,
            'records': items,
            'fields': self.fields,
        }

class DataQualityMetrics(object):
 
    def __init__(self, metrics=None, force_recalculate=False):
        self.metrics = metrics or []
        self.force_recalculate = force_recalculate
        self.logger = getLogger('ckanext.DataQualityMetrics')

    def _fetch_dataset(self, package_id):
        return toolkit.get_action('package_show')(
            {
                'ignore_auth': True,
            },
            {
                'id': package_id,
            }
        )
    
    def _data_quality_settings(self, resource):
        settings = {}
        #'completeness', 'uniqueness'
        for dimension in ['validity', 'consistency','openness','downloadable','machine_readable', 'timeliness']:
            for key, value in resource.items():
                prefix = 'dq_%s' % dimension
                if key.startswith(prefix):
                    if settings.get(dimension) is None:
                        settings[dimension] = {}
                    setting = key[len(prefix) + 1:]
                    settings[dimension][setting] = value
        return settings
    def _fetch_resource_data(self, resource):
        def _fetch_page(page, page_size):
            try:
                context = {'ignore_auth': True}
                return toolkit.get_action('datastore_search')(None, {
                    'resource_id': resource['id'],
                    'offset': page*page_size,
                    'limit': page_size,
                })
            except Exception as e:
                self.logger.warning('Failed to fetch data for resource %s. '
                                    'Error: %s', resource['id'], str(e))
                self.logger.exception(e)
                return {
                    'total': 0,
                    'records': [],
                }
       

        _fetch_page = ResourceFetchData(resource)

        result = _fetch_page(0, 1)  # to calculate the total from start
        return {
            'total': result.get('total', 0),
            'records': LazyStreamingList(_fetch_page),
            'fields': result['fields'],  # metadata
        }
    def _fetch_resource_data2(self, resource):
        def _fetch_page2(page, page_size):
            try:
                context = {'ignore_auth': True}
                return toolkit.get_action('datastore_search')(None, {
                    'resource_id': resource['id'],
                    'offset': page*page_size,
                    'limit': page_size,
                })
            except Exception as e:
                self.logger.warning('Failed to fetch data for resource %s. '
                                    'Error: %s', resource['id'], str(e))
                self.logger.exception(e)
                return {
                    'total': 0,
                    'records': [],
                }
       

        _fetch_page2 = ResourceFetchData2(resource)

        result = _fetch_page2(0, 1)  # to calculate the total from start
        return {
            'total': result.get('total', 0),
            'records': LazyStreamingList(_fetch_page2),
            'fields': result['fields'],  # metadata
        }
    def _fetch_resource_file(self, resource):
        _fetch_page = ResourceFetchData(resource)

        result = _fetch_page(0, 1)  # to calculate the total from start
        return {
            'total': result.get('total', 0),
            'records': LazyStreamingList(_fetch_page),
            'fields': result['fields'],  # metadata
        }
    def calculate_metrics_for_dataset(self, package_id):
        '''Calculates the Data Qualtity for the given dataset identified with
        `package_id`.

        The metrics for the resources and dataset are calculated or reused from
        an earlier calculation if there were no changes in the resources data
        from the last time the calculation has been performed.

        Additionaly, if some of the metrics were set manually, either for a
        resource or the dataset, then the calculation for that dimension will
        not be performed and the manual value will be kept intact.

        The calculations for both resources and dataset are kept in a database,
        one entry for dataset and one for each resource.

        :param package_id: `str`, the ID of the dataset (package) for which to
            calculate Data Quality metrics.
        '''
        log.debug('Calculating data quality for dataset: %s',
                          package_id)
        dataset = self._fetch_dataset(package_id)

        results = []
        for resource in dataset['resources']:
            # self.logger.debug ('Calculating data quality for resource: %s',
            #                   resource['id'])
            # self.logger.debug ('Calculating data quality for resource: %s',
            #                   resource)
            resource['data_quality_settings'] = self._data_quality_settings(
                resource)
       
            result = self.calculate_metrics_for_resource(resource)
            if result is None:
                result = {}
            # self.logger.debug('Result: %s', result)
            results.append(result)

        # calculate cumulative
        self.logger.debug('Calculating cumulative data quality metrics for %s',
                          package_id)
        self.calculate_cumulative_metrics(package_id,
                                          dataset['resources'],
                                          results)
        self.logger.info('Data Quality metrcis calculated for dataset: %s.',
                         package_id)
        
    def _get_metrics_record(self, ref_type, ref_id):
        metrics = DataQualityMetricsModel.get(ref_type, ref_id)
        return metrics
    def _new_metrics_record(self, ref_type, ref_id):
        return DataQualityMetricsModel(type=ref_type, ref_id=ref_id)
    #-- check file size ---
    def get_file_size(self, url):
        timeout = 5
        try:
            response = requests.head(url, timeout=timeout)
            if response.status_code == 200:
                size = int(response.headers['Content-Length'])
                return size
            else:
                print("Error: Could not retrieve file size, status code:", response.status_code)
                return None
        except Exception as e:
            print("Error:", e)
            return None 
    def check_connection_url(self,url, timeout):
        log.debug('---check_connection_url--')
        try:
            # Send a GET request to the URL with a timeout
            response = requests.get(url, timeout=timeout)
            
            # Check if the request was successful
            if response.status_code == 200:
                return True
            else:
                return False
        except requests.exceptions.Timeout:
            # If there's a timeout, return False
            return False
        except Exception as e:
            # Handle other exceptions
            print("Error:", e)
            return False   
    def calculate_metrics_for_resource(self, resource):
        log.debug('calculate_metrics_for_resource')
        last_modified = datetime.strptime((resource.get('last_modified') or
                                           resource.get('created')),
                                          '%Y-%m-%dT%H:%M:%S.%f')
        # self.logger.debug ('Resource last modified on: %s', last_modified)
        #-------Check Data Dict using Resource Name -----------
        resource_url = resource['url']
        resource_name = resource['name']
        resource_name = resource_name.lower()
        log.debug(resource_url)
        # initializing test list
        datadict_list = ['datadict', 'data dict','data_dictionary','data dictionary']
        # using list comprehension
        # checking if string contains list element
        res_datadict = [ele for ele in datadict_list if(ele in resource_name)]
        is_datadict = bool(res_datadict)
        results = {}
        file_size_mb = 0
        execute_time = 0
        timeout = 5  # in seconds
        connection_url = False
        # Check if the request was successful
        if self.check_connection_url(resource_url, timeout):     
            start_time = time.time()
            log.debug(start_time)
            connection_url = True
            file_size = self.get_file_size(resource_url)
            # file_size = int(response.headers['Content-Length'])
            if file_size is not None:
                file_size_mb = file_size/1024**2
                log.debug("--- file_size ----")
                log.debug(file_size_mb)
            
            if not is_datadict: #and file_size_mb <= 10:                                                    
                #----- connect model: check records----------------------
                data_quality = self._get_metrics_record('resource', resource['id']) #get data from DB   
                cached_calculation = False
                if data_quality:
                    self.logger.debug('Data Quality calculated for '
                                    'version modified on: %s',
                                    data_quality.resource_last_modified)
                    if data_quality.resource_last_modified >= last_modified:
                        cached_calculation = True
                        # check if all metrics have been calculated or some needs to be
                        # calculated again
                        if all(map(lambda m: m is not None, [
                                    # data_quality.completeness,
                                    # data_quality.uniqueness,
                                    data_quality.validity,
                                    data_quality.timeliness,
                                    data_quality.consistency,
                                    data_quality.openness,
                                    data_quality.downloadable,
                                    data_quality.access_api,
                                    data_quality.machine_readable
                                    ])):
                            self.logger.debug('Data Quality already calculated.')
                            return data_quality.metrics
                        else:
                            self.logger.debug('Data Quality not calculated for '
                                            'all dimensions.')
                    else:
                        data_quality = self._new_metrics_record('resource',
                                                                resource['id'])
                        data_quality.resource_last_modified = last_modified
                        self.logger.debug('Resource changed since last calculated. '
                                        'Calculating data quality again.')
                else:
                    data_quality = self._new_metrics_record('resource', resource['id'])
                    data_quality.resource_last_modified = last_modified
                    self.logger.debug('First data quality calculation.')
                #----------------Calculate Metrics--------------------
                data_quality.ref_id = resource['id']
                data_quality.resource_last_modified = last_modified
                
                data_stream2 = None
                if self.force_recalculate:
                    log.info('Forcing recalculation of the data metrics '
                            'has been set. All except the manually set metric data '
                            'will be recalculated.')
                self.logger.debug('******Calculate Metrics *****')
                self.logger.debug(self.metrics)
                # self.logger.debug(resource)
                consistency_val = 0
                # encoding = ''
                validity_report = {}
                for metric in self.metrics:
                    self.logger.debug(metric)      
                    try:
                        if cached_calculation and getattr(data_quality,
                                                        metric.name) is not None:
                            cached = data_quality.metrics[metric.name]
                            if not self.force_recalculate and not cached.get('failed'):
                                self.logger.debug('Dimension %s already calculated. '
                                                'Skipping...', metric.name)
                                results[metric.name] = cached
                                continue
                            if cached.get('manual'):
                                self.logger.debug('Calculation has been performed '
                                                'manually. Skipping...', metric.name)
                                results[metric.name] = cached
                                continue
                        
                        self.logger.debug('Calculating dimension: %s...', metric)
                    #------------------------------------------------------
                        # if not data_stream:
                        #     data_stream = self._fetch_resource_data(resource)
                        # else:
                        #     if data_stream.get('records') and \
                        #             hasattr(data_stream['records'], 'rewind'):
                        #         data_stream['records'].rewind()
                        #     else:
                        #         data_stream = self._fetch_resource_data(resource)
                        if not data_stream2:
                            data_stream2 = self._fetch_resource_data2(resource)
                        else:
                            if data_stream2.get('records') and \
                                    hasattr(data_stream2['records'], 'rewind'):
                                data_stream2['records'].rewind()
                            else:
                                data_stream2 = self._fetch_resource_data2(resource)
                      
                        # data_stream2 = self._fetch_resource_data2(resource)
                        #------ Check Meta Data --------------------------------
                        log.debug('------ Resource URL-----')
                        log.debug(resource['url'])
                        #using metadata for calculate metrics
                                        
                        if (file_size_mb <= 5 and connection_url):
                            log.debug('------ check all metrics-----')
                            if(metric.name == 'openness' or metric.name == 'downloadable' or metric.name == 'access_api'):
                                log.debug('------ check openness-----')
                                results[metric.name] = metric.calculate_metric(resource)

                            elif(metric.name == 'consistency'):
                                log.debug('------Call consistency -----')                           
                                results[metric.name] = metric.calculate_metric(resource,data_stream2)
                                consistency_val = results[metric.name].get('value')
                                log.debug(consistency_val)
                            elif(metric.name == 'validity'):
                               
                                # results[metric.name] = metric.calculate_metric(resource,data_stream)
                                log.debug('----check validity------')
                                log.debug(data_stream2['total'])
                                results[metric.name] = metric.calculate_metric(resource,data_stream2)         
                                
                                # validity_val = results[metric.name].get('value')
                                validity_report = results[metric.name].get('report')
                                # encoding   = results[metric.name].get('encoding')
    
                            elif(metric.name == 'machine_readable'):
                                log.debug('----machine_readable_val------')       
                                results[metric.name] = metric.calculate_metric_machine(resource,consistency_val,validity_report)
                            elif (metric.name == 'timeliness'):     
                                log.debug('----timeliness------')                                         
                                results[metric.name] = metric.calculate_metric(resource) #,data_stream
                            else:
                                log.debug('----else----')    
                                log.debug(metric.name)                                         
                                results[metric.name] = metric.calculate_metric(resource) #,data_stream

                        else:
                            if(metric.name == 'openness' or metric.name == 'downloadable' or metric.name == 'access_api'):
                                results[metric.name] = metric.calculate_metric(resource)
                            elif(metric.name == 'timeliness'):
                                results[metric.name] = metric.calculate_metric(resource)

                            results['consistency'] = { 'value': 0}
                            results['validity']    =    { 'value': 0}
                            results['machine_readable'] = { 'value': 0}
                            if not connection_url:
                                results['connection_url'] = { 'error': True}
                        
                    except Exception as e:
                        self.logger.error('Failed to calculate metric: %s. Error: %s',
                                        metric, str(e))
                        self.logger.exception(e)
                        results[metric.name] = {
                            'failed': True,
                            'error': str(e),
                        }
                # set results
                for metric, result in results.items():
                    if result.get('value') is not None:
                        setattr(data_quality, metric, result['value'])

                data_quality.metrics = results
                data_quality.modified_at = datetime.now()
                #---- add execute time--
                end_time = time.time()   
                # Calculate the time taken
                execute_time = end_time - start_time
                log.debug("----execute_time----")
                log.debug(execute_time)
                #---- add filepath ----
                upload = uploader.get_resource_uploader(resource)
                filepath = upload.get_path(resource['id'])
                data_quality.filepath = filepath
                data_quality.url = resource['url']
                data_quality.file_size = file_size_mb
                data_quality.execute_time = execute_time
                data_quality.save()
                self.logger.debug('Metrics calculated for resource: %s',
                                resource['id'])
                     
        else:
            self.logger.debug('Connection Timed Out')
            #----- connect model: check records----------------------
            data_quality = self._get_metrics_record('resource', resource['id']) #get data from DB   
            if data_quality:
                self.logger.debug('Data Quality already calculated.')
            else:
                data_quality = self._new_metrics_record('resource', resource['id'])
                data_quality.resource_last_modified = last_modified
                self.logger.debug('First data quality calculation.')
                #----------------Calculate Metrics--------------------
                data_quality.ref_id = resource['id']
                data_quality.resource_last_modified = last_modified
                data_quality.metrics = {'error':'connection timed out'}
                data_quality.modified_at = datetime.now()
                #----------------------
                data_quality.openness = 0
                data_quality.downloadable = 0
                data_quality.access_api = 0
                data_quality.timeliness = 999
                data_quality.openness = 0
                data_quality.consistency = 0
                data_quality.validity = 0
                data_quality.machine_readable = 0      
                #---- add filepath ----
                data_quality.filepath = ''
                data_quality.url = resource['url']
                data_quality.file_size = 0
                data_quality.execute_time = 0
                data_quality.save()
                self.logger.debug('Metrics calculated for resource: %s',
                                    resource['id'])
    
        return results

    def calculate_cumulative_metrics(self, package_id, resources, results):
        '''Calculates the cumulative metrics (reduce phase), from the results
        calculated for each resource in the dataset.

        The cumulative values for the dataset are always calculated from the
        results, except in the case when the values for a particular dimension
        have been set manually. In that case, the manual value is used.

        The results of the calculation are stored in database in a separate
        entry containing the valued for Data Quality metrics for the whole
        dataset.

        :param package_id: `str`, the ID of the CKAN dataset.
        :param resources: `list` of CKAN resources for which Data Qualtiy
            metrics have been calculated.
        :param results: `list` of result `dict`, the results of the calculation
            for each of the resources.
        '''
        self.logger.debug('Cumulative data quality metrics for package: %s',
                          package_id)
        data_quality = self._get_metrics_record('package', package_id)
        if not data_quality:
            data_quality = self._new_metrics_record('package', package_id)
        cumulative = {}
        dataset_results = data_quality.metrics or {}
        for metric in self.metrics:
            cached = dataset_results.get(metric.name, {})
            if cached.get('manual'):
                self.logger.debug('Metric %s is calculated manually. '
                                  'Skipping...', metric.name)
                cumulative[metric.name] = cached
                continue
            metric_results = [res.get(metric.name, {}) for res in results]
            cumulative[metric.name] = metric.calculate_cumulative_metric(
                resources,
                metric_results
            )

        if cumulative != dataset_results:
            data_quality = self._new_metrics_record('package', package_id)
        for metric, result in cumulative.items():
            if result.get('value') is not None:
                setattr(data_quality, metric, result['value'])
        data_quality.metrics = cumulative
        data_quality.modified_at = datetime.now()
        data_quality.save()
        self.logger.debug('Cumulative metrics calculated for: %s', package_id)

class OpendQuality(object):
    
    # @classmethod
    # def top_pageckage_view(cls, limit=3, page_no=0):
    #     pass
   
    def test(cls, limit=3, page_no=0):
        return 'OK'
    @classmethod
    def get_last_modified_datasets(limit=5):
        try:
            package = model.Package
            q = model.Session.query(package.name, package.title, package.type, package.metadata_modified.label('date_modified')).filter(package.state == 'active').order_by(package.metadata_modified.desc()).limit(limit)
            packages = q.all()
        except:
            return []
        return packages
class ResourceFetchData(object):
    '''A callable wrapper for fetching the resource data.

    Based on the availability of the data, when called it will fetch a page of
    the resource data.
    The data for a particular resource may be stored in the Data Store, in CKAN
    storage path or on a remote server. Ideally we want to read the data from
    the data store, however if the data is not available there, we should try
    to download the CSV data directly - either from CKAN or if the resource was
    set as a link to an external server, by downloading it from that server.

    :param resource: `dict`, the resource metadata as retrieved from CKAN
        action `resource_show`.
    '''

    def __init__(self, resource):
        self.resource = resource
        self.download_resource = False
        self.resource_csv = None

    def __call__(self, page, limit):
        '''Fetches one page (with size of `limit`) of data from the resource
        CSV data.

        This call will first try to load the data from the datastore. If that
        fails and the data is not available in the data store, then it will try
        to load the data directly (from CKAN uploads or by downloading the CSV
        file directly from the remote server). Once this happens, and the data
        is loaded into `ResourceCSVData`, then every subsequent call will load
        the data from the cached instance of `ResourceCSVData` and will not try
        to load it from the data store.

        :param page: `int`, the page to fetch, 0-based.
        :param limit: `int`, page size.

        :returns: `dict`, the requested page as `dict` containing:
            * `total` - total number of records in the data.
            * `records` - a `list` of records for this page. Each record is a
                `dict` of format `<column_name>:<row_value>`.
            * `fields` - the records metadata describing each column. It is a
                `list` of `dict`, where each element describes one column. The
                order of the columns is the same as it is appears in the CSV
                file. Each element has:
                    * `id` - `str`, the name of the column
                    * `type` - `str`, the column type (ex. `numeric`, `text`)
        '''
        try:
            page = self.fetch_page(page, limit)
            return page
        except Exception as e:
            log.error('Failed to fetch page %d (limit %d) of resource %s. '
                      'Error: %s',
                      page,
                      limit,
                      self.resource.get('id'),
                      str(e))
            log.exception(e)
            return {
                'total': 0,
                'records': [],
                'fields': {},
            }

    def _fetch_data_datastore(self, page, limit):
        log.debug('Fetch page from datastore: page %d, limit %d', page, limit)
        return toolkit.get_action('datastore_search')({
            'ignore_auth': True,
        }, {
            'resource_id': self.resource['id'],
            'offset': page*limit,
            'limit': limit,
        })

    def _download_resource_from_url(self, url, headers=None):
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()  # Raise an error if request to file failed.
        data = []
        with TemporaryFile(mode='w+b') as tmpf:
            for chunk in resp.iter_content():
                tmpf.write(chunk)
            tmpf.flush()
            tmpf.seek(0, 0)  # rewind to start
            reader = csv.reader(tmpf)
            for row in reader:
                data.append(row)
        return data

    def _download_resource_from_ckan(self, resource):       
        upload = uploader.get_resource_uploader(resource)
        filepath = upload.get_path(resource['id'])
        try:
            data = []
            if os.path.isfile(filepath):
                data = []
                with open(filepath) as csvf:
                    reader = csv.reader(csvf)
                    for row in reader:
                        data.append(row)
                return data
            else:
                # The worker is not in the same machine as CKAN, so it cannot
                # read the resource files from the local file system.
                # We need to retrieve the resource data from the resource URL.
                if 'url' not in resource:
                    raise Exception('Cannot access the resource because the '
                                    'resource URL is not set.')
                log.debug('Resource data is not in this file system '
                          '(File %s does not exist).'
                          'Fetching data from CKAN download url directly.',
                          filepath)
                headers = None
                sysadmin_api_key = _get_sysadmin_user_key()
                if sysadmin_api_key:
                    headers = {'Authorization': sysadmin_api_key}
                return self._download_resource_from_url(
                    resource['url'],
                    headers
                )
        except OSError as e:
            log.error('Failed to download resource from CKAN upload. '
                      'Error: %s', str(e))
            log.exception(e)
            raise e

    def _fetch_data_directly(self):
        if self.resource.get('url_type') == 'upload':
            log.debug('Getting data from CKAN...')
            return self._download_resource_from_ckan(self.resource)
        if self.resource.get('url'):
            log.debug('Getting data from remote URL...')
            return self._download_resource_from_url(self.resource['url'])
        raise Exception('Resource {} is not available '
                        'for download.'.format(self.resource.get('id')))

    def fetch_page(self, page, limit):
        '''Fetches one page (with size of `limit`) of data from the resource
        CSV data.

        This call will first try to load the data from the datastore. If that
        fails and the data is not available in the data store, then it will try
        to load the data directly (from CKAN uploads or by downloading the CSV
        file directly from the remote server). Once this happens, and the data
        is loaded into `ResourceCSVData`, then every subsequent call will load
        the data from the cached instance of `ResourceCSVData` and will not try
        to load it from the data store.

        :param page: `int`, the page to fetch, 0-based.
        :param limit: `int`, page size.

        :returns: `dict`, the requested page as `dict` containing:
            * `total` - total number of records in the data.
            * `records` - a `list` of records for this page. Each record is a
                `dict` of format `<column_name>:<row_value>`.
            * `fields` - the records metadata describing each column. It is a
                `list` of `dict`, where each element describes one column. The
                order of the columns is the same as it is appears in the CSV
                file. Each element has:
                    * `id` - `str`, the name of the column
                    * `type` - `str`, the column type (ex. `numeric`, `text`)
        '''
        log.debug('----call fetch_page at ResourceFetchData-------')
        if self.download_resource:
            if not self.resource_csv:
                self.resource_csv = ResourceCSVData(self._fetch_data_directly)
                log.debug('Resource data downloaded directly.')
            return self.resource_csv.fetch_page(page, limit)
        try:
            page = self._fetch_data_datastore(page, limit)
            log.debug('Data is available in DataStore. '
                      'Using datastore for data retrieval.')
            return page
        except Exception as e:
            log.warning('Failed to load resource data from DataStore. '
                        'Error: %s', str(e))
            log.exception(e)
            self.download_resource = True
            log.debug('Will try to download the data directly.')
            return self.fetch_page(page, limit)
class ResourceFetchData2(object):
    '''A callable wrapper for fetching the resource data.

    Based on the availability of the data, when called it will fetch a page of
    the resource data.
    The data for a particular resource may be stored in the Data Store, in CKAN
    storage path or on a remote server. Ideally we want to read the data from
    the data store, however if the data is not available there, we should try
    to download the CSV data directly - either from CKAN or if the resource was
    set as a link to an external server, by downloading it from that server.

    :param resource: `dict`, the resource metadata as retrieved from CKAN
        action `resource_show`.
    '''

    def __init__(self, resource):
        self.resource = resource
        self.download_resource = False
        self.resource_csv = None

    def __call__(self, page, limit):
        '''Fetches one page (with size of `limit`) of data from the resource
        CSV data.

        This call will first try to load the data from the datastore. If that
        fails and the data is not available in the data store, then it will try
        to load the data directly (from CKAN uploads or by downloading the CSV
        file directly from the remote server). Once this happens, and the data
        is loaded into `ResourceCSVData`, then every subsequent call will load
        the data from the cached instance of `ResourceCSVData` and will not try
        to load it from the data store.

        :param page: `int`, the page to fetch, 0-based.
        :param limit: `int`, page size.

        :returns: `dict`, the requested page as `dict` containing:
            * `total` - total number of records in the data.
            * `records` - a `list` of records for this page. Each record is a
                `dict` of format `<column_name>:<row_value>`.
            * `fields` - the records metadata describing each column. It is a
                `list` of `dict`, where each element describes one column. The
                order of the columns is the same as it is appears in the CSV
                file. Each element has:
                    * `id` - `str`, the name of the column
                    * `type` - `str`, the column type (ex. `numeric`, `text`)
        '''
        try:
            page = self.fetch_page2(page, limit)
            return page
        except Exception as e:
            log.error('2Failed to fetch page %d (limit %d) of resource %s. '
                      'Error: %s',
                      page,
                      limit,
                      self.resource.get('id'),
                      str(e))
            log.exception(e)
            return {
                'total': 0,
                'records': [],
                'fields': {},
            }

    def _fetch_data_datastore(self, page, limit):
        log.debug('Fetch page from datastore: page %d, limit %d', page, limit)
        return toolkit.get_action('datastore_search')({
            'ignore_auth': True,
        }, {
            'resource_id': self.resource['id'],
            'offset': page*limit,
            'limit': limit,
        })
    
    def _download_resource_from_url(self, url, headers=None):
        data = []
        filepath = self.resource['url']
        resource_format = self.resource['format']
        response = requests.get(filepath)
        if self.is_url_file(filepath) and response.status_code == 200 :
            n_rows = 5000
            if(resource_format =='CSV'):
                log.debug('----csv----')     
                log.debug(filepath)
                #-----------------------------------      
                # Create a StringIO object to treat the response content as a file-like object
                encoding = self.detect_encoding(filepath)
                print(encoding)
                try:
                    data_encode = response.content.decode(encoding)  # Decode content to string, errors='ignore'
                    csv_data = StringIO(data_encode)
                    # Use the csv.reader to parse the CSV data
                    csv_reader = csv.reader(csv_data)
                    records_read = 0
                    for row in csv_reader:
                        data.append(row)
                        records_read += 1
                        if records_read >= n_rows:
                            break
                except Exception as e:
                    print("An error occurred:", e)
                    data = []
                #-----------------------------------
                # csv_data = StringIO(response.text)
                # csv_reader = csv.reader(csv_data)
                # records_read = 0
                # for row in csv_reader:
                #     data.append(row)
                #     records_read += 1
                #     if records_read >= n_rows:
                #         break
                #-----------------------------------
                # data_df = pd.read_csv(io.StringIO(data_encode), nrows=n_rows)  # Read CSV data into a DataFrame
                # if data_df is not None:
                #     data = data_df.values.tolist()
                #     data[:0] = [list(data_df.keys())]

            elif(resource_format == 'JSON'):
                # Read JSON data in chunks
                data_chunks = []
                for chunk in pd.read_json(filepath, lines=True, chunksize=n_rows):
                    data_chunks.append(chunk)
                    # Break the loop if the specified number of rows have been read
                    if len(data_chunks) * n_rows >= n_rows:
                        break
                # Concatenate the data chunks into a single DataFrame
                data_df = pd.concat(data_chunks, ignore_index=True)
                data = data_df.values.tolist()

            elif(resource_format == 'XLSX' or resource_format == 'XLS'):
                # Load the workbook from the temporary file
                wb = load_workbook(filename=BytesIO(response.content), read_only=True)
                # Get the active worksheet
                ws = wb.active
                # Iterate over rows and cells to read the data
                records_read = 0
                for row in ws.iter_rows(values_only=True):
                    data.append(row)
                    records_read += 1
                    if records_read >= n_rows:
                        break
                wb.close()
                # data_df = pd.read_excel(filepath, nrows=n_rows)  # Read CSV data into a DataFrame
                # if data_df is not None:
                #     data = data_df.values.tolist()
                #     data[:0] = [list(data_df.keys())]
            else:
                data = []
        else:
            data = []
        return data

    def is_url_file(self,url):
        try:
            response = requests.head(url)
            content_type = response.headers.get('Content-Type')
            if content_type:
                mime_type, _ = mimetypes.guess_type(url)
                if mime_type and mime_type != 'application/octet-stream':
                    return True  # It's a file
            return False  # It's not a file or has unknown content type
        except Exception as e:
            print("An error occurred:", e)
            return False  # Error occurred, not a file
    def detect_encoding(self,url):
        timeout = 5
        sample_size=1024
        try:
            response = requests.get(url, timeout=timeout)
            if response.status_code == 200:
                # Use chardet to detect the encoding of the sample
                sample = response.content[:sample_size]
                result = chardet.detect(sample)
                return result['encoding']
            else:
                print("Failed to download the file. Status code:", response.status_code)
                return None
        except Exception as e:
            print("Error:", e)
            return None   
    def _download_resource_from_ckan(self, resource):       
        upload = uploader.get_resource_uploader(resource)
        filepath = upload.get_path(resource['id'])
        # filepath = resource['url']
        resource_format = resource['format']
        try:
            data = []
            if os.path.isfile(filepath):
                if '\0' in open(filepath).read():
                    print("you have null bytes in your input file")
                else:
                    print("you don't")
                if(resource_format == 'XLSX' or resource_format == 'XLS'):                  
                    data_df = pd.read_excel(filepath)
                    data = data_df.values.tolist()
                elif(resource_format == 'JSON'):
                    try:
                        data_json = pd.read_json(filepath)
                        data_df = pd.DataFrame(data_json)
                        data = data_df.values.tolist()
                        data[:0] = [list(data_df.keys())]
                        print(list(data_df.keys()))
                    except ValueError as e:
                        print('ValueError = ', e)
                        data = []
                elif(resource_format == 'CSV'):
                    with open(filepath) as csvf:
                        reader = csv.reader(csvf)
                        for row in reader:
                            data.append(row)
                else:
                    data = []
                return data
            # if self.is_url_file(filepath):
            #     response = requests.get(filepath)
            #     response.raise_for_status()  # Raise an exception for bad status codes
            #     if(resource_format == 'CSV'):
            #         data = response.content.decode('utf-8')  # Decode content to string
            #         data_df = pd.read_csv(io.StringIO(data))  # Read CSV data into a DataFrame
            #         if data_df is not None:
            #             data = data_df.values.tolist()
            #             data[:0] = [list(data_df.keys())]
                        
            #     elif(resource_format == 'JSON'):
            #         try:
            #             data_json = response.json()
            #             data_json = pd.read_json(filepath)
            #             if data_json is not None:
            #                 data_df = pd.DataFrame(data_json)
            #                 data = data_df.values.tolist()
            #                 data[:0] = [list(data_df.keys())]
                                  
            #         except ValueError as e:
            #                 print('ValueError = ', e)
            #                 data = []

            #     elif(resource_format == 'XLSX' or resource_format == 'XLS'): 
            #         try:
            #             excel_data = io.BytesIO(response.content)         
            #             dataframe = openpyxl.load_workbook(excel_data)  # Load Excel file using openpyxl
            #             # Define variable to read sheet
            #             dataframe1 = dataframe.active
            #             # Iterate the loop to read the cell values
            #             for row in range(0, dataframe1.max_row):
            #                 data_row = []
            #                 for col in dataframe1.iter_cols(1, dataframe1.max_column):
            #                     data_row.append(col[row].value)
            #                 data.append(data_row)

            #         except Exception as e:
            #             print("An error occurred:", e)
            #             data = []
            #     else:
            #         data = []
            #     return data
            else:
                # The worker is not in the same machine as CKAN, so it cannot
                # read the resource files from the local file system.
                # We need to retrieve the resource data from the resource URL.
                if 'url' not in resource:
                    raise Exception('Cannot access the resource because the '
                                    'resource URL is not set.')
                log.debug('Resource data is not in this file system '
                          '(File %s does not exist).'
                          'Fetching data from CKAN download url directly.',
                          filepath)
                headers = None
                # sysadmin_api_key = _get_sysadmin_user_key()
                # if sysadmin_api_key:
                #     headers = {'Authorization': sysadmin_api_key}
                return self._download_resource_from_url(
                    resource['url'],
                    headers
                )
        except OSError as e:
            log.error('Failed to download resource from CKAN upload. '
                      'Error: %s', str(e))
            log.exception(e)
            raise e

    def _fetch_data_directly(self):
        if self.resource.get('url_type') == 'upload':
            log.debug('2 Getting data from CKAN...')
            # return self._download_resource_from_ckan(self.resource)
            return self._download_resource_from_url(self.resource['url'])
        if self.resource.get('url'):
            log.debug('2 Getting data from remote URL...')
            return self._download_resource_from_url(self.resource['url'])
        raise Exception('Resource {} is not available '
                        'for download.'.format(self.resource.get('id')))

    def fetch_page2(self, page, limit):
        '''Fetches one page (with size of `limit`) of data from the resource
        CSV data.

        This call will first try to load the data from the datastore. If that
        fails and the data is not available in the data store, then it will try
        to load the data directly (from CKAN uploads or by downloading the CSV
        file directly from the remote server). Once this happens, and the data
        is loaded into `ResourceCSVData`, then every subsequent call will load
        the data from the cached instance of `ResourceCSVData` and will not try
        to load it from the data store.

        :param page: `int`, the page to fetch, 0-based.
        :param limit: `int`, page size.

        :returns: `dict`, the requested page as `dict` containing:
            * `total` - total number of records in the data.
            * `records` - a `list` of records for this page. Each record is a
                `dict` of format `<column_name>:<row_value>`.
            * `fields` - the records metadata describing each column. It is a
                `list` of `dict`, where each element describes one column. The
                order of the columns is the same as it is appears in the CSV
                file. Each element has:
                    * `id` - `str`, the name of the column
                    * `type` - `str`, the column type (ex. `numeric`, `text`)
        '''
        log.debug('----call fetch_page at ResourceFetchData2-------')
        if self.download_resource:
            if not self.resource_csv:
                self.resource_csv = ResourceCSVData(self._fetch_data_directly)
                # log.debug('2Resource data downloaded directly.')
            return self.resource_csv.fetch_page(page, limit)
        try:
            #------ Pang Edit ------------------       
            self.download_resource = True
            log.debug('Will try to download the data directly.')
            return self.fetch_page2(page, limit)
        except Exception as e:
            log.warning('Failed to load resource data from DataStore. '
                        'Error: %s', str(e))
            log.exception(e)
            self.download_resource = True
            log.debug('Will try to download the data directly.')
            return self.fetch_page2(page, limit)
class Openness():#DimensionMetric
    '''Calculates the openness Data Qualtiy dimension.

    The calculation is performed over all values in the resource data.
    In each row, ever cell is inspected if there is a value present in it.

    The calculation is: `cells_with_value/total_numbr_of_cells * 100`, where:
        * `cells_with_value` is the number of cells containing a value. A cell
            contains a value if the value in the cell is not `None` or an empty
            string or a string containing only whitespace.
        * `total_numbr_of_cells` is the total number of cells expected to be
            populted. This is calculated from the number of rows multiplied by
            the number of columns in the tabular data.

    The return value is a percentage of cells that are populated from the total
    number of cells.
    '''
    
    
    def __init__(self):
        self.name = 'openness'

    def get_openness_score(self,data_format,mimetype):
        openness_score = { "JSON-LD": 5,"N3": 5, "SPARQL": 5, "RDF": 5,
        "TTL": 5, "KML": 3, "GML": 3, "WCS": 3, "NetCDF": 3,
        "TSV": 3, "WFS": 3, "KMZ": 3, "QGIS": 3,
        "ODS": 3, "JSON": 3,"ODB": 3, "ODF": 3,
        "ODG": 3, "XML": 3,"WMS": 3, "WMTS": 3,
        "SVG": 3, "JPEG": 3,"CSV": 3, "Atom Feed": 3,
        "XYZ": 3, "PNG": 3,"RSS": 3, "GeoJSON": 3,
        "IATI": 3, "ICS": 3,"XLS": 2, "MDB": 2,
        "ArcGIS Map Service": 2,"BMP": 2, "TIFF": 2,
        "XLSX": 2, "GIF": 2,"E00": 2, "MrSID": 2,
        "ArcGIS Map Preview": 2,"MOP": 2, "Esri REST": 2,
        "dBase": 2, "SHP": 2,"PPTX": 1, "DOC": 1,
        "ArcGIS Online Map": 1, "ZIP": 1, "GZ": 1,
        "ODT": 1, "RAR": 1,"TXT": 1, "DCR": 1,
        "DOCX": 1, "BIN": 1,"PPT": 1, "ODP": 1,
        "PDF": 1, "ODC": 1,"MXD": 1, "TAR": 1,"EXE": 0,
        "JS": 0,"Perl": 0,"OWL": 0, "HTML": 0,
        "XSLT": 0, "RDFa": 0}

        #if user add a resource as a link, data type will be null
        if(data_format == ''):
            score = 0
            if(mimetype == 'text/csv'):
                data_format = 'CSV'
            elif(mimetype == 'application/pdf'):
                data_format = 'PDF'
            elif(mimetype == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'):
                data_format = 'XLSX'
            elif(mimetype == 'application/vnd.ms-excel'):
                data_format = 'XLS'
            elif(mimetype == 'application/rdf+xml'):
                data_format = 'RDF'
            elif(mimetype == 'application/ld+json'):
                data_format = 'JSON-LD'
            elif(mimetype == 'application/xml'):
                data_format = 'XML'           
            elif(mimetype == 'application/vnd.google-earth.kml+xml'):
                data_format = 'KML'
            elif(mimetype == 'application/gml+xml'):
                data_format = 'GML'          
            elif(mimetype == 'application/json'):
                data_format = 'JSON'
            elif(mimetype == 'image/png'):
                data_format = 'PNG'
            elif(mimetype == 'image/jpeg'):
                data_format = 'JPEG'
            elif(mimetype == 'image/bmp'):
                data_format = 'BMP'
            elif(mimetype == 'image/gif'):
                data_format = 'GIF'
            elif(mimetype == 'image/tiff'):
                data_format = 'TIFF'
            elif(mimetype == 'application/zip'):
                data_format = 'ZIP'
            elif(mimetype == 'application/msword'):
                data_format = 'DOC'
            elif(mimetype == 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'):
                data_format = 'DOCX'
            elif(mimetype == 'application/vnd.oasis.opendocument.text'):
                data_format = 'ODT'
            elif(mimetype == 'application/vnd.oasis.opendocument.spreadsheet'):
                data_format = 'ODS'
            elif(mimetype == 'application/vnd.oasis.opendocument.presentation'):
                data_format = 'ODP'
            elif(mimetype == 'application/vnd.ms-powerpoint'):
                data_format = 'PPT'
            elif(mimetype == 'application/vnd.openxmlformats-officedocument.presentationml.presentation'):
                data_format = 'PPTX'
            elif(mimetype == 'text/html'):
                data_format = 'HTML'
            elif(mimetype == 'application/vnd.rar'):
                data_format = 'RAR'
            elif(mimetype == 'text/plain'):
                data_format = 'TXT'
            if(data_format != '' ):
                score =  openness_score.get(data_format)
        else:
            score =  openness_score.get(data_format)
        # data type is not in the list
        if (score == None):
            score = -1
        return score
    def calculate_metric(self, resource):
        '''Calculates the openness dimension metric for the given resource
        from the resource data.

        :param resource: `dict`, CKAN resource.
        :param data: `dict`, the resource data as a dict with the following
            values:
                * `total`, `int`, total number of rows.
                * `fields`, `list` of `dict`, column metadata - name, type.
                * `records`, `iterable`, iterable over the rows in the resource
                    where each row is a `dict` itself.

        :returns: `dict`, the report contaning the calculated values:
            * `value`, `float`, the percentage of complete values in the data.
            * `total`, `int`, total number of values expected to be populated.
            * `complete`, `int`, number of cells that have value.
        '''
        data_format = resource['format']
        mimetype    = resource['mimetype']
    
        if ((data_format == '' or data_format == None) and (mimetype == '' or mimetype == None)):
            resource_url = resource['url']   
            format_url = resource_url.split(".")[-1]
            data_format = format_url.upper()

        data_format = data_format.replace(".", "")
        data_format = data_format.upper()  
        openness_score = self.get_openness_score(data_format,mimetype)
        # log.debug('Openness score: %f%%', openness_score)
        return {
            'format': data_format,
            'value': openness_score,
        }
        
    def calculate_cumulative_metric(self, resources, metrics):
        '''Calculates the cumulative report for all resources from the
        calculated results for each resource.

        The calculation is done as `all_complete/all_total * 100`, where
            * `all_complete` is the total number of completed values in all
                resources.
            * all_total is the number of expected values (rows*columns) in all
                resources.
        The final value is the percentage of completed values in all resources
        in the dataset.

        :param resources: `list` of CKAN resources.
        :param metrics: `list` of `dict` results for each resource.

        :returns: `dict`, a report for the total percentage of complete values:
            * `value`, `float`, the percentage of complete values in the data.
            * `total`, `int`, total number of values expected to be populated.
            * `complete`, `int`, number of cells that have value.
        '''
        # log.debug('-------------Calculate openness metrics -----------')
        # log.debug(metrics)
        openness_list = []
        total = 0
        
        for item_metric in metrics:
            #check dict is not Empty
            if item_metric:                  
                openness_score = item_metric.get('value')
                total = total+openness_score
                openness_list.append(openness_score)
        if openness_list:
            result_score = max(openness_list)
            return {
                'total': total,
                'value': result_score,
            }
        else:
            return {
                'total': 0,
                'value': 0,
            }
class Downloadable():#DimensionMetric
    '''Calculates the Downloadable Data Qualtiy dimension.

    The calculation is performed over all values in the resource data.
    In each row, ever cell is inspected if there is a value present in it.

    The calculation is: `cells_with_value/total_numbr_of_cells * 100`, where:
        * `cells_with_value` is the number of cells containing a value. A cell
            contains a value if the value in the cell is not `None` or an empty
            string or a string containing only whitespace.
        * `total_numbr_of_cells` is the total number of cells expected to be
            populted. This is calculated from the number of rows multiplied by
            the number of columns in the tabular data.

    The return value is a percentage of cells that are populated from the total
    number of cells.
    '''

    def __init__(self):
        self.name = 'downloadable'

    def is_downloadable(self,url):
        try:
            response = requests.head(url, allow_redirects=True)
            if 'Content-Type' in response.headers:
                content_type = response.headers['Content-Type'].lower()
                if 'text' not in content_type or 'csv' in content_type or 'application/octet-stream' in content_type:
                    return True
            # Check if the URL ends with common file extensions
            if os.path.splitext(url)[1].lower() in ['.zip', '.pdf', '.jpg', '.jpeg', '.png', '.gif', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx','.gml','.kml','.kmz','.json','.geojson','.shp','.wms','.xml']:
                return True
            return False
        except requests.RequestException:
            return False
    def calculate_metric(self, resource):
        '''Calculates the openness dimension metric for the given resource
        from the resource data.

        :param resource: `dict`, CKAN resource.
        :param data: `dict`, the resource data as a dict with the following
            values:
                * `total`, `int`, total number of rows.
                * `fields`, `list` of `dict`, column metadata - name, type.
                * `records`, `iterable`, iterable over the rows in the resource
                    where each row is a `dict` itself.

        :returns: `dict`, the report contaning the calculated values:
            * `value`, `float`, the percentage of complete values in the data.
            * `total`, `int`, total number of values expected to be populated.
            * `complete`, `int`, number of cells that have value.
        '''
        # log.debug ('------------downloadable--')
        # log.debug(resource)
        downloadable_score = 1
        resource_data_format = resource['format'] 
        resource_url    = resource['url']
        
        if self.is_downloadable(resource_url):
            downloadable_score = 1 # 2 can download
            #ตรวจสอบ format ไม่ตรงตามที่กำหนด แต่เว้นว่างได้ เลยตรวจยาก เพราะเค้าอาจจะไม่กำหนดก็ได้
            # if(pd.notna(resource_url) and resource_data_format != ""): 
            #     format_url = resource_url.split(".")[-1]
            #     lower_format = resource_data_format.lower()
            #     if(format_url != lower_format): # set wrong format type
            #         downloadable_score = 0
        else: 
            downloadable_score = 0 # 1 cannot download such as HTML.

        return {
            'format': resource_data_format,
            'value': downloadable_score,
        }
        
    def calculate_cumulative_metric(self, resources, metrics):
        '''Calculates the cumulative report for all resources from the
        calculated results for each resource.

        The calculation is done as `all_complete/all_total * 100`, where
            * `all_complete` is the total number of completed values in all
                resources.
            * all_total is the number of expected values (rows*columns) in all
                resources.
        The final value is the percentage of completed values in all resources
        in the dataset.

        :param resources: `list` of CKAN resources.
        :param metrics: `list` of `dict` results for each resource.

        :returns: `dict`, a report for the total percentage of complete values:
            * `value`, `float`, the percentage of complete values in the data.
            * `total`, `int`, total number of values expected to be populated.
            * `complete`, `int`, number of cells that have value.
        '''
        # log.debug('-------------Calculate downloadable metrics -----------')
        # log.debug(metrics)
        downloadable_list = []
        total = 0
        for item_metric in metrics:
            #check dict is not Empty
            if item_metric:
                downloadable_score = item_metric.get('value')
                total = total+downloadable_score
                downloadable_list.append(downloadable_score)
        
        if downloadable_list:
            result_score = max(downloadable_list)
            return {
                'total': total,
                'value': result_score,
            }
        else:
            return {
                'total': 0,
                'value': 0,
            }
class AccessAPI():#DimensionMetric
    '''Calculates the Downloadable Data Qualtiy dimension.

    The calculation is performed over all values in the resource data.
    In each row, ever cell is inspected if there is a value present in it.

    The calculation is: `cells_with_value/total_numbr_of_cells * 100`, where:
        * `cells_with_value` is the number of cells containing a value. A cell
            contains a value if the value in the cell is not `None` or an empty
            string or a string containing only whitespace.
        * `total_numbr_of_cells` is the total number of cells expected to be
            populted. This is calculated from the number of rows multiplied by
            the number of columns in the tabular data.

    The return value is a percentage of cells that are populated from the total
    number of cells.
    '''

    def __init__(self):
        self.name = 'access_api'
    def calculate_metric(self, resource):
        '''Calculates the API Accesssibility dimension metric for the given resource
        from the resource data.

        :param resource: `dict`, CKAN resource.
        :param data: `dict`, the resource data as a dict with the following
            values:
                * `total`, `int`, total number of rows.
                * `fields`, `list` of `dict`, column metadata - name, type.
                * `records`, `iterable`, iterable over the rows in the resource
                    where each row is a `dict` itself.

        :returns: `dict`, the report contaning the calculated values:
            * `value`, `float`, the percentage of complete values in the data.
            * `total`, `int`, total number of values expected to be populated.
            * `complete`, `int`, number of cells that have value.
        '''
        # log.debug ('-----Access API-----')
        access_api_score = 2 
        log.debug (resource['format'])
        if(resource['datastore_active'] == True and  (resource['format'] == 'CSV' or resource['format'] == 'XLSX')):
            access_api_score = 2
        elif(resource['datastore_active'] == False and  (resource['format'] == 'CSV' or resource['format'] == 'XLSX')): 
            access_api_score = 1
        else:
            access_api_score = 0
        # log.debug('Accessibility API score: %f%%', access_api_score)
        return {
            'datastore': resource['datastore_active'],
            'format': resource['format'],
            'value': access_api_score
        }
   
    def calculate_cumulative_metric(self, resources, metrics):
        '''Calculates the cumulative report for all resources from the
        calculated results for each resource.

        The calculation is done as `all_complete/all_total * 100`, where
            * `all_complete` is the total number of completed values in all
                resources.
            * all_total is the number of expected values (rows*columns) in all
                resources.
        The final value is the percentage of completed values in all resources
        in the dataset.

        :param resources: `list` of CKAN resources.
        :param metrics: `list` of `dict` results for each resource.

        :returns: `dict`, a report for the total percentage of complete values:
            * `value`, `float`, the percentage of complete values in the data.
            * `total`, `int`, total number of values expected to be populated.
            * `complete`, `int`, number of cells that have value.
        '''
        # log.debug('-------------Calculate accessibility metrics -----------')
        # log.debug(metrics)
        access_api_list = []
        total = 0
        
        for item_metric in metrics:
            #check dict is not Empty
            if item_metric:
                access_api_score = item_metric.get('value')
                total = total+access_api_score
                access_api_list.append(access_api_score)
        if access_api_list:
            result_score = max(access_api_list)
            return {
                'total': total,
                'value': result_score,
            }
        else:
            return {
                'total': 0,
                'value': 0,
            }
class MachineReadable():#DimensionMetric
    '''Calculates the MachineReadable Data Qualtiy dimension.

    The calculation is performed over all values in the resource data.
    In each row, ever cell is inspected if there is a value present in it.

    The calculation is: `cells_with_value/total_numbr_of_cells * 100`, where:
        * `cells_with_value` is the number of cells containing a value. A cell
            contains a value if the value in the cell is not `None` or an empty
            string or a string containing only whitespace.
        * `total_numbr_of_cells` is the total number of cells expected to be
            populted. This is calculated from the number of rows multiplied by
            the number of columns in the tabular data.

    The return value is a percentage of cells that are populated from the total
    number of cells.
    '''

    def __init__(self):
        self.name = 'machine_readable'
    def calculate_metric(self,resource,data): 
        return {
            'consistency': 0,
            'validity': '',
            'encoding': '',
            'value': 0
        }
    def calculate_metric_machine(self,resource,consistency_val,validity_report): 
        '''Calculates the openness dimension metric for the given resource
        from the resource data.

        :param resource: `dict`, CKAN resource.
        :param data: `dict`, the resource data as a dict with the following
            values:
                * `total`, `int`, total number of rows.
                * `fields`, `list` of `dict`, column metadata - name, type.
                * `records`, `iterable`, iterable over the rows in the resource
                    where each row is a `dict` itself.

        :returns: `dict`, the report contaning the calculated values:
            * `value`, `float`, the percentage of complete values in the data.
            * `total`, `int`, total number of values expected to be populated.
            * `complete`, `int`, number of cells that have value.
        '''
        #--------------Machine Readable-----------------
        machine_readable_format = ['CSV','XLSX','XLS','JSON','XML'] #'JSON','XML' 
        documenct_format = ['PDF','DOC','DOCX','PPTX','PPT','ODT','ODS','ODP'] #-50
        image_format = ['PNG','JPEG','GIF','TIFF']#-60
        openness_5_star_format = ['RDF','TTL','N3','GeoJSON','WMS','GML','KML','SHP','Esri REST']#100
        validity_chk = True
        encoding_utf8 = False
        data_format = resource['format'] 
        mimetype    = resource['mimetype']   
        machine_readable_score = 100
        # log.debug('-----Machine Readable ------')
        # log.debug(data_format)
        # log.debug(resource['mimetype'])
        # log.debug(resource['id'])
        #if data format and mimetype is null, find format from url
        if ((data_format == '' or data_format == None) and (mimetype == '' or mimetype == None)):
            resource_url = resource['url']   
            format_url = resource_url.split(".")[-1]
            data_format = format_url.upper()
            # log.debug(data_format)
        #---- Tabular Data -------------#
        data_format = data_format.replace(".", "")
        data_format = data_format.upper()
        if data_format in machine_readable_format:
            encoding = validity_report.get('encoding')
            valid    = validity_report.get('valid')          
            # log.debug('-----Check Valid -----Report ------')
            # log.debug(valid)
            # log.debug(encoding_utf8)
            # log.debug(validity_chk)
            # log.debug(consistency_val)

            if "utf-8" in encoding:
                encoding_utf8 = True
            if(validity_report.get('blank-header') > 0 or validity_report.get('duplicate-header') > 0 or validity_report.get('blank-row') > 0 or validity_report.get('duplicate-row') > 0 or 
                        validity_report.get('extra-value') > 0 or validity_report.get('schema-error') > 0):
                validity_chk = False
            
            if(consistency_val >= 0 and consistency_val < 100):
                machine_readable_score = machine_readable_score-15
            if(validity_chk == False):
                machine_readable_score = machine_readable_score-20
            if(encoding_utf8 == False):
                machine_readable_score = machine_readable_score-10
                    
            # log.debug('----machine_readable_val in------')
            # log.debug('MachineReadable score: %f%%', machine_readable_score)
            # log.debug(encoding_utf8)
            # log.debug(consistency_val)
            # log.debug(validity_chk)
            return {
                'consistency': consistency_val,
                'validity': validity_chk,
                'encoding': encoding_utf8,
                'value': machine_readable_score
            }
        elif data_format in openness_5_star_format:
            machine_readable_score = 100
        elif data_format in documenct_format:
            machine_readable_score = machine_readable_score-50
        elif data_format in image_format:
            machine_readable_score = machine_readable_score-60
        else:
            machine_readable_score = machine_readable_score-60
            
        return {
            'consistency': 0,
            'validity': {},
            'encoding': data_format,
            'value': machine_readable_score
        }

    def calculate_cumulative_metric(self, resources, metrics):
        '''Calculates the cumulative report for all resources from the
        calculated results for each resource.

        The calculation is done as `all_complete/all_total * 100`, where
            * `all_complete` is the total number of completed values in all
                resources.
            * all_total is the number of expected values (rows*columns) in all
                resources.
        The final value is the percentage of completed values in all resources
        in the dataset.

        :param resources: `list` of CKAN resources.
        :param metrics: `list` of `dict` results for each resource.

        :returns: `dict`, a report for the total percentage of complete values:
            * `value`, `float`, the percentage of complete values in the data.
            * `total`, `int`, total number of values expected to be populated.
            * `complete`, `int`, number of cells that have value.
        '''
        # log.debug('-------------Calculate machine metrics -----------')
        # log.debug(metrics)
        machine_readable_list = []
        total = 0
        
        for item_metric in metrics:
            #check dict is not Empty
            if item_metric:
                machine_readable_score = item_metric.get('value')
                if isinstance(machine_readable_score, int):
                    total = total+machine_readable_score
                    machine_readable_list.append(machine_readable_score)
        if machine_readable_list:
            result_score = max(machine_readable_list)
            return {
                'total': total,
                'avg_score': total/len(machine_readable_list),
                'value': result_score,
            }
        else:
            return {
                'total': 0,
                'value': 0,
            }
# class Completeness():#DimensionMetric
#     '''Calculates the completeness Data Qualtiy dimension.

#     The calculation is performed over all values in the resource data.
#     In each row, ever cell is inspected if there is a value present in it.

#     The calculation is: `cells_with_value/total_numbr_of_cells * 100`, where:
#         * `cells_with_value` is the number of cells containing a value. A cell
#             contains a value if the value in the cell is not `None` or an empty
#             string or a string containing only whitespace.
#         * `total_numbr_of_cells` is the total number of cells expected to be
#             populted. This is calculated from the number of rows multiplied by
#             the number of columns in the tabular data.

#     The return value is a percentage of cells that are populated from the total
#     number of cells.
#     '''
#     def __init__(self):
#         # super(Completeness, self).__init__('completeness')
#         self.name = 'completeness'

#     def calculate_metric(self, resource, data):
#         '''Calculates the completeness dimension metric for the given resource
#         from the resource data.

#         :param resource: `dict`, CKAN resource.
#         :param data: `dict`, the resource data as a dict with the following
#             values:
#                 * `total`, `int`, total number of rows.
#                 * `fields`, `list` of `dict`, column metadata - name, type.
#                 * `records`, `iterable`, iterable over the rows in the resource
#                     where each row is a `dict` itself.

#         :returns: `dict`, the report contaning the calculated values:
#             * `value`, `float`, the percentage of complete values in the data.
#             * `total`, `int`, total number of values expected to be populated.
#             * `complete`, `int`, number of cells that have value.
#         '''
#         columns_count = len(data['fields'])
#         rows_count = data['total']
#         total_values_count = columns_count * rows_count

#         log.debug('Rows: %d, Columns: %d, Total Values: %d',
#                           rows_count, columns_count, total_values_count)
#         # log.debug(data)
#         total_complete_values = 0
#         for row in data['records']:
#             total_complete_values += self._completenes_row(row)

#         result = \
#             float(total_complete_values)/float(total_values_count) * 100.0 if \
#             total_values_count else 0
#         log.debug ('Complete (non-empty) values: %s',
#                           total_complete_values)
#         log.debug('Completeness score: %f%%', result)
#         return {
#             'value': result,
#             'total': total_values_count,
#             'complete': total_complete_values,
#         }

#     def _completenes_row(self, row):
#         count = 0
#         for _, value in row.items():
#             if value is None:
#                 continue
#             if isinstance(value, str):
#                 if not value.strip():
#                     continue
#             count += 1
#         return count

#     def calculate_cumulative_metric(self, resources, metrics):
#         '''Calculates the cumulative report for all resources from the
#         calculated results for each resource.

#         The calculation is done as `all_complete/all_total * 100`, where
#             * `all_complete` is the total number of completed values in all
#                 resources.
#             * all_total is the number of expected values (rows*columns) in all
#                 resources.
#         The final value is the percentage of completed values in all resources
#         in the dataset.

#         :param resources: `list` of CKAN resources.
#         :param metrics: `list` of `dict` results for each resource.

#         :returns: `dict`, a report for the total percentage of complete values:
#             * `value`, `float`, the percentage of complete values in the data.
#             * `total`, `int`, total number of values expected to be populated.
#             * `complete`, `int`, number of cells that have value.
#         '''
#         total, complete = reduce(lambda total, complete, result: (
#             total + result.get('total', 0),
#             complete + result.get('complete', 0)
#         ), metrics, (0, 0))
#         return {
#             'total': total,
#             'complete': complete,
#             'value': float(complete)/float(total) * 100.0 if total else 0.0,
#         }
# class Uniqueness(): #DimensionMetric
#     '''Calculates the uniqueness of the data.

#     The general calculation is: `unique_values/total_values * 100`, where:
#         * `unique_values` is the number of unique values
#         * `total_values` is the total number of value

#     The dimension value is a percentage of unique values in the data.
#     '''
#     def __init__(self):
#         # super(Uniqueness, self).__init__('uniqueness')
#         self.name = 'uniqueness'

#     def calculate_metric(self, resource, data):
#         '''Calculates the uniqueness of the values in the data for the given
#         resource.

#         For each column of the data, the number of unique values is calculated
#         and the total number of values (basically the number of rows).

#         Then, to calculate the number of unique values in the data, the sum of
#         all unique values is calculated and the sum of the total number of
#         values for each column. The the percentage is calculated from those two
#         values.

#         :param resource: `dict`, CKAN resource.
#         :param data: `dict`, the resource data as a dict with the following
#             values:
#                 * `total`, `int`, total number of rows.
#                 * `fields`, `list` of `dict`, column metadata - name, type.
#                 * `records`, `iterable`, iterable over the rows in the resource
#                     where each row is a `dict` itself.

#         :returns: `dict`, a report on the uniqueness metrics for the given
#             resource data:
#             * `value`, `float`, the percentage of unique values in the data.
#             * `total`, `int`, total number of values in the data.
#             * `unique`, `int`, number unique values in the data.
#             * `columns`, `dict`, detailed report for each column in the data.
#         '''
#         total = {}
#         distinct = {}

#         for row in data['records']:
#             for col, value in row.items():
#                 total[col] = total.get(col, 0) + 1
#                 if distinct.get(col) is None:
#                     distinct[col] = set()
#                 distinct[col].add(value)

#         result = {
#             'total': sum(v for _, v in total.items()),
#             'unique': sum([len(s) for _, s in distinct.items()]),
#             'columns': {},
#         }
#         if result['total'] > 0:
#             result['value'] = (100.0 *
#                                float(result['unique'])/float(result['total']))
#         else:
#             result['value'] = 0.0

#         for col, tot in total.items():
#             unique = len(distinct.get(col, set()))
#             result['columns'][col] = {
#                 'total': tot,
#                 'unique': unique,
#                 'value': 100.0*float(unique)/float(tot) if tot > 0 else 0.0,
#             }
#         return result

#     def calculate_cumulative_metric(self, resources, metrics):
#         '''Calculates uniqueness for all resources based on the metrics
#         calculated in the previous phase for each resource.

#         The calculation is performed based on the total unique values as a
#         percentage of the total number of values present in all data from all
#         given resources.

#         :param resources: `list` of CKAN resources.
#         :param metrics: `list` of `dict` results for each resource.

#         :returns: `dict`, a report for the total percentage of unique values:
#             * `value`, `float`, the percentage of unique values in the data.
#             * `total`, `int`, total number of values in the data.
#             * `unique`, `int`, number of unique values.
#         '''
#         result = {}
#         result['total'] = sum([r.get('total', 0) for r in metrics])
#         result['unique'] = sum([r.get('unique', 0) for r in metrics])
#         if result['total'] > 0:
#             result['value'] = (100.0 *
#                                float(result['unique'])/float(result['total']))
#         else:
#             result['value'] = 0.0

#         return result
class Validity():#DimensionMetric
    '''Calculates Data Quality dimension validity.

    The validity is calculated as a percentage of valid records against the
    total number of records in the data.

    The validation is performed using the validation provided by
    https://github.com/frictionlessdata/goodtables-py.
    '''
    def __init__(self):
        # super(Validity, self).__init__('validity')
        self.name = 'validity'

    def _perform_validation(self, resource, total_rows):
        rc = {}
        rc.update(resource)
        rc['validation_options'] = {
            'row_limit': total_rows,
        }
        return validate_resource_data(rc)

    def calculate_metric(self, resource, data):
        '''Calculates the percentage of valid records in the resource data.

        The validation of the resource data is done based on the resource
        schema. The schema is specified in the resource metadata itself.

        :param resource: `dict`, CKAN resource.
        :param data: `dict`, the resource data as a dict with the following
            values:
                * `total`, `int`, total number of rows.
                * `fields`, `list` of `dict`, column metadata - name, type.
                * `records`, `iterable`, iterable over the rows in the resource
                    where each row is a `dict` itself.

        :returns: `dict`, a report on the validity metrics for the given
            resource data:
            * `value`, `float`, percentage of valid records in the resource
                data.
            * `total`, `int`, total number of records.
            * `valid`, `int`, number of valid records.
        '''
        validation = None
        try:
            validation = self._perform_validation(resource,
                                                  data.get('total', 0))
        except Exception as e:
            log.error('Failed to validate data for resource: %s. '
                              'Error: %s', resource['id'], str(e))
            log.exception(e)
            return {
                'failed': True,
                'error': str(e),
            }

        total_rows = 0
        total_errors = 0
        # errors_code = ''
        encoding = ''
        valid = ''

        dict_error = {'blank-header': 0, 'duplicate-header': 0, 'blank-row': 0 , 'duplicate-row': 0,'extra-value':0,'missing-value':0,'format-error':0, 'schema-error':0,'encoding':''}
        for table in validation.get('tables', []):
            total_rows += table.get('row-count', 0)
            total_errors += table.get('error-count', 0)
            # log.debug('-----Check Validity---')
            # log.debug(type(table))
            for error in table.get('errors', []):
                # errors_code += error.get('code')
                item_code = error.get('code')
                count_val = dict_error[item_code]+1
                dict_error[item_code] = count_val
            encoding = table.get('encoding')
            valid = table.get('valid')
            log.debug("---encoding---")
            log.debug(encoding)
            # log.debug(table.get('errors'))
            dict_error['encoding'] = encoding
            dict_error['valid']    = valid
            # log.debug(dict_error)
            # log.debug(table.get('valid'))
            # log.debug(table.get('format'))
            # log.debug(table.get('encoding'))
            # log.debug(table.get('headers'))
            # errors = table.get('errors', [0])
            # errors_message = errors.get('message')
            # errors_code = errors.get('code')
        if total_rows == 0:
            return {
                'value': 0.0,
                'total': 0,
                'valid': 0,
                'report': dict_error
            }

        valid = total_rows - total_errors
        value = float(valid)/float(total_rows) * 100.0

        return {
            'value': value,
            'total': total_rows,
            'valid': valid,
            'report': dict_error
        }

    def calculate_cumulative_metric(self, resources, metrics):
        '''Calculates the percentage of valid records in all the data for the
        given resources based on the calculation for each individual resource.

        The percentage is calculated based on the total number of valid records
        across all of the data in all resources against the total number of
        records in the data.

        :param resources: `list` of CKAN resources.
        :param metrics: `list` of `dict` results for each resource.

        :returns: `dict`, a report on the validity metrics for all of the data.
            * `value`, `float`, percentage of valid records in the data.
            * `total`, `int`, total number of records.
            * `valid`, `int`, number of valid records.
        '''
        total = sum([r.get('total', 0) for r in metrics])
        valid = sum([r.get('valid', 0) for r in metrics])
        if total == 0:
            return {
                'value': 0.0,
                'total': 0,
                'valid': 0,
            }

        return {
            'value': float(valid)/float(total) * 100.0,
            'total': total,
            'valid': valid,
        }
# class Accuracy():#DimensionMetric
#     '''Calculates Data Qualtiy dimension accuracy.

#     Accuracy of a record is determined through background research and it is
#     not quite possible to determine if a record is accurate or inaccurate via
#     a generic algorithm.

#     This metric calculates the percentage of accurate records *only* for
#     records that already have been marked as accurate or inaccurate in a
#     particular dataset.

#     To calculate this, the record must contain a flag, a column in the dataset,
#     that marks the record as accurate, inaccurate or not determined.

#     If such column is present, then the calculation is:
#     `number_of_accurate/(number_of_accurate + number_of_inaccurate) * 100)`.

#     Accuracy is measured as percentage of accurate records, from the set of
#     records that have been checked and marked as accurate or inaccurate.
#     '''
#     def __init__(self):
#         # super(Accuracy, self).__init__('accuracy')
#         self.name = 'accuracy'

#     def calculate_metric(self, resource, data):
#         '''Calculates the percentage of accurate records for the given resource
#         data.

#         The resource must contain a data quality setting to configure which
#         column contains the flag that marks the record as accurate.

#         :param resource: `dict`, CKAN resource. The resource dict must contain
#             a property `data_quality_settings` which contain the name of the
#             column used to determine the accuracy of that record. This
#             property must have the following format:

#                 .. code-block: python

#                     resource = {
#                         ...
#                         'data_quality_settings': {
#                             'accuracy': {
#                                 'column': '<column name>',
#                             }
#                         }
#                     }
#         :param data: `dict`, the resource data as a dict with the following
#             values:
#                 * `total`, `int`, total number of rows.
#                 * `fields`, `list` of `dict`, column metadata - name, type.
#                 * `records`, `iterable`, iterable over the rows in the resource
#                     where each row is a `dict` itself.

#         :returns: `dict`, a report on the accuracy metrics for the resource
#             data:
#             * `value`, `float`, percentage of accurate records in the data.
#             * `accurate`, `int`, number of accurate records.
#             * `inaccurate`, `int`, number of inaccurate records.
#             * `total`, `int`, `accurate` + `inaccurate` - total number of
#                 checked records.
#         '''
        # settings = resource.get('data_quality_settings',
        #                         {}).get('accuracy', {})
        # column = settings.get('column')
        # if not column:
        #     log.error('Cannot calculate accuracy on this resource '
        #                       'because no accuracy column is specified.')
        #     return {
        #         'failed': True,
        #         'error': 'Missing accuracy column.',
        #     }

        # accurate = 0
        # inaccurate = 0

        # for row in data['records']:
        #     flag = row.get(column)
        #     if flag is None or flag.strip() == '':
        #         # neither accurate or inaccurate
        #         continue
        #     if flag.lower() in ['1', 'yes', 'accurate', 't', 'true']:
        #         accurate += 1
        #     else:
        #         inaccurate += 1

        # total = accurate + inaccurate
        # value = 0.0
        # if total:
        #     value = float(accurate)/float(total) * 100.0

        # log.debug('Accurate: %d', accurate)
        # log.debug('Inaccurate: %d', inaccurate)
        # log.debug('Accuracy: %f%%', value)
        # return {
        #     'value': value,
        #     'total': total,
        #     'accurate': accurate,
        #     'inaccurate': inaccurate,
        # }

    # def calculate_cumulative_metric(self, resources, metrics):
    #     '''Calculates the percentage of accurate records in all data for the
    #     given resources, based on the calculations for individual resources.

    #     The value is calculated as percentage of accurate records of the total
    #     number of checked records (all accurate + all inaccurate records).

    #     :param resources: `list` of CKAN resources.
    #     :param metrics: `list` of `dict` results for each resource.

    #     :returns: `dict`, a report on the accuracy metrics for all data in all
    #         resources:
    #         * `value`, `float`, percentage of accurate records in the data.
    #         * `accurate`, `int`, number of accurate records.
    #         * `inaccurate`, `int`, number of inaccurate records.
    #         * `total`, `int`, `accurate` + `inaccurate` - total number of
    #             checked records.
    #     '''
        # accurate = sum([r.get('accurate', 0) for r in metrics])
        # inaccurate = sum([r.get('inaccurate', 0) for r in metrics])

        # total = accurate + inaccurate
        # value = 0.0
        # if total:
        #     value = float(accurate)/float(total) * 100.0

        # return {
        #     'value': value,
        #     'total': total,
        #     'accurate': accurate,
        #     'inaccurate': inaccurate,
        # }
class Consistency():#DimensionMetric
    '''Calculates Data Quality dimension consistency.

    This dimension gives a metric of how consistent are the values of same type
    accross the dataset (or resource).

    The type of the values is determined by the type of the column in CKAN's
    data store (like int, numeric, timestamp, text etc).
    For each type, a validator for the consistency is used to try to categorize
    the value in some category - for example all dates with same formats would
    be in the same category, all numbers that use the same format, how many
    of the numbers in a column are floating point and how many are integer.

    For each column, we may get multiple categories (plus a special category
    called `unknown` which means we could not determine the category for those
    values), and we count the number of values for each category.
    The consistency, for a given column, would be the number of values in the
    category with most values, expressed as a percentage of the total number of
    values in that column.

    To calculate the consistency for the whole data, we calculate the total
    number of consistent values (sum of all columns) expressed as a percentage
    of the total number of values present in the data.
    '''
    def __init__(self):
        # super(Consistency, self).__init__('consistency')
        self.name = 'consistency'

    def validate_date(self, field, value, _type, report):
        date_format = detect_date_format(value) or 'unknown'
        formats = report['formats']
        formats[date_format] = formats.get(date_format, 0) + 1

    def validate_numeric(self, field, value, _type, report):
        num_format = detect_numeric_format(value) or 'unknown'
        formats = report['formats']
        formats[num_format] = formats.get(num_format, 0) + 1

    def validate_int(self, field, value, _type, report):
        num_format = detect_numeric_format(value) or 'unknown'
        formats = report['formats']
        formats[num_format] = formats.get(num_format, 0) + 1

    def validate_string(self, field, value, _type, report):
        formats = report['formats']
        formats[_type] = formats.get(_type, 0) + 1

    def get_consistency_validators(self):
        return {
            'timestamp': self.validate_date,
            'numeric': self.validate_numeric,
            'int': self.validate_int,
            'string': self.validate_string,
            'text': self.validate_string,
        }

    def calculate_metric(self, resource , data):
        '''Calculates the consistency of the resource data.

        For each of the columns of the resource data, we determine the number
        of values that belong in the same category (have the same format, are
        of exactly the same type, etc). Then, we find the category with most
        values, per column, and use that expressed as percentage to calculate
        the consistency for that column.
        The final value is calculated from the results obtained for each
        column.

        :param resource: `dict`, CKAN resource.
        :param data: `dict`, the resource data as a dict with the following
            values:
                * `total`, `int`, total number of rows.
                * `fields`, `list` of `dict`, column metadata - name, type.
                * `records`, `iterable`, iterable over the rows in the resource
                    where each row is a `dict` itself.

        :returns: `dict`, a report on the consistency metrics the date in the
            resource:
            * `value`, `float`, percentage of consistent records in the data.
            * `consistent`, `int`, number of consistent records.
            * `total`, `int`, total number ofrecords.
            * `report`, `dict`, detailed, per column, report for the
                consistency of the data.
        '''
        validators = self.get_consistency_validators()
        fields = {f['id']: f for f in data['fields']}
        report = {f['id']: {'count': 0, 'formats': {}} for f in data['fields']}
        for row in data['records']:
            for field, value in row.items():
                field_type = fields.get(field, {}).get('type')
                validator = validators.get(field_type)
                field_report = report[field]
                if validator:
                    validator(field, value, field_type, field_report)
                    field_report['count'] += 1
        for field, field_report in report.items():        
            if field_report['formats']:
                most_consistent = max([count if fmt != 'unknown' else 0
                                    for fmt, count
                                    in field_report['formats'].items()])
                field_report['consistent'] = most_consistent

        total = sum([f.get('count', 0) for _, f in report.items()])
        consistent = sum([f.get('consistent', 0) for _, f in report.items()])
        value = float(consistent)/float(total) * 100.0 if total else 0.0
        #---- check key in report {} : for excel, if key is not str type, set report null because the data structure is invalid---
        list_key = report.keys()
        for key_item in list_key:
            if not isinstance(key_item, str):
                report = {}
                break
        #------------------------------------------------
        if (consistent == None):
            consistent = 0
        return {
            'total': total,
            'consistent': consistent,
            'value': value,
            'report': report,
        }
    def calculate_cumulative_metric(self, resources, metrics):
        '''Calculates the total percentage of consistent values in the data for
        all the given resources.

        :param resources: `list` of CKAN resources.
        :param metrics: `list` of `dict` results for each resource.

        :returns: `dict`, a report on the consistency metrics the date in the
            resource:
            * `value`, `float`, percentage of consistent records in the data.
            * `consistent`, `int`, number of consistent records.
            * `total`, `int`, total number ofrecords.
            * `report`, `dict`, detailed, per column, report for the
                consistency of the data.
        '''
        total = sum([r.get('total', 0) for r in metrics])
        consistent = sum([r.get('consistent', 0) for r in metrics])

        # FIXME: This is not the proper calculation. We need to merge all
        # data to calculate the consistency properly.
        value = float(consistent)/float(total) * 100.0 if total else 0.0
        return {
            'total': total,
            'consistent': consistent,
            'value': value,
        }
class Timeliness():#DimensionMetric
    '''Calculates the timeliness Data Quality dimension.

    The timeliness of a records is the difference between the times when the
    measurement was made and the time that record have entered our system.

    The measurement requires extra configuration - the name of the column in
    the resource data that holds the time of when the measurement was made. If
    this time is missing or the setting is not configured, then the calculation
    cannot be performed automatically.

    The calculation is performed by taking a time delta between the time that
    the resource have been last modified (the time when the data have entered
    and was stored in CKAN) and the time in the specified column, for each
    record.

    The total difference (time delta in resolution of seconds) is then divided
    by the number of records checked, givin the average time delta in seconds.
    This average is then used as the value for this dimension.
    '''
    def __init__(self):
        # super(Timeliness, self).__init__('timeliness')
        self.name = 'timeliness'

    def calculate_metric(self, resource): #, data
        '''Calculates the timeliness of the data in the given resource.

        :param resource: `dict`, CKAN resource. The resource is expected to
            have a property `data_quality_settings` that configures the column
            that holds the timestamp of when the measurement was taken. This
            property is a `dict`, having the following structure:

                .. code-block: python

                    resoruce = {
                        ...
                        'data_quality_settings': {
                            'timeliness': {
                                'column': '<the name of the column',
                            }
                        }
                    }

        :param data: `dict`, the resource data as a dict with the following
            values:
                * `total`, `int`, total number of rows.
                * `fields`, `list` of `dict`, column metadata - name, type.
                * `records`, `iterable`, iterable over the rows in the resource
                    where each row is a `dict` itself.

        :returns: `dict`, a report on the timeliness metrics for the given
            resource data:
            * `value`, `str`, string representation of the average time delta,
                for example: `'+3:24:00'`, `'+3 days, 3:24:00'`
            * `total`, `int`, total number of seconds that the measurements
                have beed delayed.
            * `average`, `int`, the average delay in seocnds.
            * `records`, `int`, number of checked records.
        '''
        package_id = resource.get('package_id')
        packages = Package.get(package_id)
        update_frequency_unit = Session.query(PackageExtra).filter(PackageExtra.key == 'update_frequency_unit', PackageExtra.package_id == package_id).first()
        update_frequency_interval = Session.query(PackageExtra).filter(PackageExtra.key == 'update_frequency_interval', PackageExtra.package_id == package_id).first()

        if resource.get('last_modified') is not None:
            created = dateutil.parser.parse(resource.get('last_modified') or resource.get('created'))
        else:
            created = dateutil.parser.parse(resource.get('created'))


        measured_count = 0
        total_delta = 0
        diff_date = (created.date() - datetime.now().date()).days
        tln = abs((created.date() - datetime.now().date()).days)
        if update_frequency_unit.value == u'วัน':
            if update_frequency_interval.value != '':
                tln_val = int(update_frequency_interval.value) - tln
            else:
                tln_val = 1 - tln
        elif update_frequency_unit.value == u'สัปดาห์':
            if update_frequency_interval.value != '':
                tln_val = (7*int(update_frequency_interval.value)) - tln
            else:
                tln_val = 7 - tln
        elif update_frequency_unit.value == u'เดือน':
            if update_frequency_interval.value != '':
                tln_val = (30 * int(update_frequency_interval.value)) - tln
            else:
                tln_val = 30 - tln
        elif update_frequency_unit.value == u'ไตรมาส':
            if update_frequency_interval.value != '':
                tln_val = (90 * int(update_frequency_interval.value)) - tln
            else:
                tln_val = 90 - tln
        elif update_frequency_unit.value == u'ครึ่งปี':
            if update_frequency_interval.value != '':
                tln_val = (180 * int(update_frequency_interval.value)) - tln
            else:
                tln_val = 180 - tln
        elif update_frequency_unit.value == u'ปี':
            if update_frequency_interval.value != '':
                tln_val =  (365 * int(update_frequency_interval.value)) - tln
            else:
                tln_val = 365 - tln
        else:
            tln_val = 999

        return {         
            'frequency': update_frequency_unit.value,
            'value': tln_val,
            'date_diff': diff_date
        }

    def calculate_cumulative_metric(self, resources, metrics):
        '''Calculates the timeliness of all data in all of the given resources
        based on the results for each individual resource.

        The average delay is calculated by getting the total number of seconds
        that all records have been delayed, then dividing that to the number of
        total records in all the data.

        :param resources: `list` of CKAN resources.
        :param metrics: `list` of `dict` results for each resource.

        :returns: `dict`, a report for the timeliness of the data:
            * `value`, `str`, string representation of the average time delta,
                for example: `'+3:24:00'`, `'+3 days, 3:24:00'`
            * `total`, `int`, total number of seconds that the measurements
                have beed delayed.
            * `average`, `int`, the average delay in seocnds.
            * `records`, `int`, number of checked records.
        '''
        timeliness_list = []
        total = 0
        
        for item_metric in metrics:
            #check dict is not Empty
            # if ((item_metric) or (item_metric is not None)):
            if item_metric:
                timeliness_score = item_metric.get('value')
                total = total+timeliness_score
                timeliness_list.append(timeliness_score)
        if timeliness_list:
            result_score = min(timeliness_list)
            return {
                'total': total,
                'value': result_score,
            }
        else:
            return {
                'total': 0,
                'value': 0,
            }
        
_all_date_formats = [
    '%Y-%m-%d',
    '%y-%m-%d',
    '%Y/%m/%d',
    '%y/%m/%d',
    '%Y.%m.%d',
    '%y.%m.%d',
    '%d-%m-%Y',
    '%d-%m-%y',
    '%d/%m/%Y',
    '%d/%m/%y',
    '%d.%m.%Y',
    '%d.%m.%y',
    '%m-%d-%Y',
    '%m-%d-%y',
    '%m/%d/%Y',
    '%m/%d/%y',
    '%m.%d.%Y',
    '%m.%d.%y',
]


def _generate_time_formats():
    additional = []
    for time_sep in ['T', ' ', ', ', '']:
        if time_sep:
            for date_fmt in _all_date_formats:
                for time_fmt in ['%H:%M:%S',
                                 '%H:%M:%S.%f',
                                 '%H:%M:%S.%fZ',
                                 '%H:%M:%S.%f%Z',
                                 '%H:%M:%S.%f%z']:
                    additional.append(date_fmt + time_sep + time_fmt)
    return additional


_all_date_formats += _generate_time_formats()


_all_numeric_formats = [
    r'^\d+$',
    r'^[+-]\d+$',
    r'^(\d{1,3},)+(\d{3})$',
    r'^(\d{1,3},)+(\d{3})\.\d+$',
    r'^[+-](\d{1,3},)(\d{3})+$',
    r'^[+-](\d{1,3},)+(\d{3})\.\d+$',
    r'^\d+\.\d+$',
    r'^[+-]\d+\.\d+$',
]


def detect_date_format(datestr):
    '''Tries to detect the date-time format from the given date or timestamp
    string.

    :param datestr: `str`, the date string

    :returns: `str`, the guessed format of the date, otherwise `None`.
    '''
    datestr = str(datestr)
    log.debug(datestr)
    if re.match(r'$\d^', datestr):
        return 'unix-timestamp'
    if re.match(r'$\d+\.\d+', datestr):
        return 'timestamp'
    if re.match(r'$\d+[\+\-]\d+^', datestr):
        return 'timestamp-tz'
    for dt_format in _all_date_formats:
        try:
            datetime.strptime(datestr, dt_format)
            return dt_format
        except:
            pass
    return None


def detect_numeric_format(numstr):
    '''Tries to detect the format of a number (int, float, number format with
    specific separator such as comma "," etc).

    :param numstr: `str`, `int`, `float`, the number string (or numeric value)
        to try to guess the format for.

    :returns: `str`, the guessed number format, otherwise `None`.
    '''
    for parser in [int, float]:
        try:
            parser(numstr)
            return parser.__name__
        except:
            pass

    for num_format in _all_numeric_formats:
        m = re.match(num_format, str(numstr))
        if m:
            return num_format
    return None


# resource validation
def validate_resource_data(resource):
    '''Performs a validation of a resource data, given the resource metadata.

    :param resource: CKAN resource data_dict.

    :returns: `dict`, a validation report for the resource data.
    '''
    log.debug(u'Validating resource {}'.format(resource['id']))

    options = toolkit.config.get(
        u'ckanext.validation.default_validation_options')
    if options:
        options = json.loads(options)
    else:
        options = {}
    resource_options = resource.get(u'validation_options')
    if resource_options and isinstance(resource_options, string_types):#basestring):
        resource_options = json.loads(resource_options)
    if resource_options:
        options.update(resource_options)

    dataset = toolkit.get_action('package_show')(
        {'ignore_auth': True}, {'id': resource['package_id']})

    source = None
    if resource.get(u'url_type') == u'upload':
        upload = uploader.get_resource_uploader(resource)
        if isinstance(upload, uploader.ResourceUpload):
            source = upload.get_path(resource[u'id'])
        else:
            # Upload is not the default implementation (ie it's a cloud storage
            # implementation)
            pass_auth_header = toolkit.asbool(
                toolkit.config.get(u'ckanext.validation.pass_auth_header',
                                   True))
            if dataset[u'private'] and pass_auth_header:
                s = requests.Session()
                s.headers.update({
                    u'Authorization': t.config.get(
                        u'ckanext.validation.pass_auth_header_value',
                        _get_site_user_api_key())
                })

                options[u'http_session'] = s
    if not source:
        source = resource[u'url']

    schema = resource.get(u'schema')
    if schema and isinstance(schema, string_types):#basestring):
        if schema.startswith('http'):
            r = requests.get(schema)
            schema = r.json()
        else:
            schema = json.loads(schema)

    _format = resource[u'format'].lower()

    report = _validate_table(source, _format=_format, schema=schema, **options)

    # Hide uploaded files
    for table in report.get('tables', []):
        if table['source'].startswith('/'):
            table['source'] = resource['url']
    for index, warning in enumerate(report.get('warnings', [])):
        report['warnings'][index] = re.sub(r'Table ".*"', 'Table', warning)

    return report


def _validate_table(source, _format=u'csv', schema=None, **options):
    report = validate(source, format=_format, schema=schema, **options)
    log.debug(u'Validating source: {}'.format(source))

    return report

def _get_site_user_api_key():
    site_user_name = toolkit.get_action('get_site_user')({
        'ignore_auth': True
    }, {})
    site_user = toolkit.get_action('get_site_user')(
        {'ignore_auth': True}, {'id': site_user_name})
    return site_user['apikey']
