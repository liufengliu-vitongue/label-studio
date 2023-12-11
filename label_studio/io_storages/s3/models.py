"""This file and its contents are licensed under the Apache License 2.0. Please see the included NOTICE for copyright information and LICENSE for a copy of the license.
"""
import os
import hashlib
import json
import logging
import re
import pandas
from io import StringIO
# import boto3
from core.feature_flags import flag_set
from core.redis import start_job_async_or_sync
from django.conf import settings
from django.db import models
from django.db.models.signals import post_save, pre_delete
from django.dispatch import receiver
from django.utils.translation import gettext_lazy as _
from io_storages.base_models import (
    ExportStorage,
    ExportStorageLink,
    ImportStorage,
    ImportStorageLink,
    ProjectStorageMixin,
)
from io_storages.s3.utils import get_client_and_resource, resolve_s3_url
from tasks.models import Annotation
from tasks.validation import ValidationError as TaskValidationError

from label_studio.io_storages.s3.utils import AWS

from webhooks.models import WebhookAction
from webhooks.utils import emit_webhooks_for_instance
from tasks.models import Task
from data_export.models import DataExport
from core.utils.common import batch
from data_export.serializers import (
    ExportDataSerializer
)
from data_export.api import ExportAPI
from label_studio_converter.exports import csv2
from label_studio_converter.converter import Converter
from datetime import datetime
import time

logger = logging.getLogger(__name__)
# logging.getLogger('botocore').setLevel(logging.CRITICAL)
# boto3.set_stream_logger(level=logging.INFO)

clients_cache = {}


class S3StorageMixin(models.Model):
    bucket = models.TextField(_('bucket'), null=True, blank=True, help_text='S3 bucket name')
    prefix = models.TextField(_('prefix'), null=True, blank=True, help_text='S3 bucket prefix')
    regex_filter = models.TextField(
        _('regex_filter'), null=True, blank=True, help_text='Cloud storage regex for filtering objects'
    )
    use_blob_urls = models.BooleanField(
        _('use_blob_urls'), default=False, help_text='Interpret objects as BLOBs and generate URLs'
    )
    aws_access_key_id = models.TextField(_('aws_access_key_id'), null=True, blank=True, help_text='AWS_ACCESS_KEY_ID')
    aws_secret_access_key = models.TextField(
        _('aws_secret_access_key'), null=True, blank=True, help_text='AWS_SECRET_ACCESS_KEY'
    )
    aws_session_token = models.TextField(_('aws_session_token'), null=True, blank=True, help_text='AWS_SESSION_TOKEN')
    aws_sse_kms_key_id = models.TextField(
        _('aws_sse_kms_key_id'), null=True, blank=True, help_text='AWS SSE KMS Key ID'
    )
    region_name = models.TextField(_('region_name'), null=True, blank=True, help_text='AWS Region')
    s3_endpoint = models.TextField(_('s3_endpoint'), null=True, blank=True, help_text='S3 Endpoint')

    def get_client_and_resource(self):
        # s3 client initialization ~ 100 ms, for 30 tasks it's a 3 seconds, so we need to cache it
        cache_key = f'{self.aws_access_key_id}:{self.aws_secret_access_key}:{self.aws_session_token}:{self.region_name}:{self.s3_endpoint}'
        if cache_key in clients_cache:
            return clients_cache[cache_key]

        result = get_client_and_resource(
            self.aws_access_key_id,
            self.aws_secret_access_key,
            self.aws_session_token,
            self.region_name,
            self.s3_endpoint,
        )
        clients_cache[cache_key] = result
        return result

    def get_client(self):
        client = self.get_client_and_resource()
        return client

    # def get_client_and_bucket(self, validate_connection=True):
    #     client, s3 = self.get_client_and_resource()
    #     if validate_connection:
    #         self.validate_connection(client)
    #     return client, s3.Bucket(self.bucket)

    def validate_connection(self, client=None):
        logger.debug('validate_connection')
        if client is None:
            client = self.get_client()
        logger.debug(
            f'[Class {self.__class__.__name__}]: Test connection to bucket {self.bucket} '
        )
        result = client.getBucketMetadata(bucketName=self.bucket)
        if result.get('status')!=200:
            logger.error(result)
            raise KeyError(
                f'Test connection to bucket {self.bucket} error: code({result.status}),message({result.reason})'
                )
            


    @property
    def path_full(self):
        prefix = self.prefix or ''
        return f'{self.url_scheme}://{self.bucket}/{prefix}'

    @property
    def type_full(self):
        return 'Amazon AWS S3'

    class Meta:
        abstract = True


class S3ImportStorageBase(S3StorageMixin, ImportStorage):

    url_scheme = 'obs'

    presign = models.BooleanField(_('presign'), default=True, help_text='Generate presigned URLs')
    presign_ttl = models.PositiveSmallIntegerField(
        _('presign_ttl'), default=1, help_text='Presigned URLs TTL (in minutes)'
    )
    recursive_scan = models.BooleanField(
        _('recursive scan'), default=False, help_text=_('Perform recursive scan over the bucket content')
    )

    def iterkeys(self):
        client = self.get_client()
        if self.prefix:
            list_kwargs = {'Prefix': self.prefix.rstrip('/') + '/'}
            if not self.recursive_scan:
                list_kwargs['Delimiter'] = '/'
            # bucket_iter = bucket.objects.filter(**list_kwargs).all()
            bucket_iter = client.listObjects(bucketName=self.bucket,prefix=self.prefix)
        else:
            bucket_iter = client.listObjects(bucketName=self.bucket)
        regex = re.compile(str(self.regex_filter)) if self.regex_filter else None
        iter = bucket_iter.body.get('contents')
        for obj in iter:
            key = obj.get('key')
            if key.endswith('/'):
                logger.debug(key + ' is skipped because it is a folder')
                continue
            if regex and not regex.match(key):
                logger.debug(key + ' is skipped by regex filter')
                continue
            yield key
    
    def _scan_and_create_links(self, link_class):
        """
        If file is json return one task,if file is csv return task list,else others return file url to task
        """
        # set in progress status for storage info
        self.info_set_in_progress()

        tasks_existed = tasks_created = 0
        maximum_annotations = self.project.maximum_annotations
        task = self.project.tasks.order_by('-inner_id').first()
        max_inner_id = (task.inner_id + 1) if task else 1

        tasks_for_webhook = []
        for key in self.iterkeys():
            # w/o Dataflow
            # pubsub.push(topic, key)
            # -> GF.pull(topic, key) + env -> add_task()
            logger.debug(f'Scanning key {key}')
            self.info_update_progress(last_sync_count=tasks_created, tasks_existed=tasks_existed)

            # skip if task already exists
            if link_class.exists(key, self):
                logger.debug(f'{self.__class__.__name__} link {key} already exists')
                tasks_existed += 1  # update progress counter
                continue

            logger.debug(f'{self}: found new key {key}')
            try:
                data = self.get_data(key)
            except (UnicodeDecodeError, json.decoder.JSONDecodeError) as exc:
                logger.debug(exc, exc_info=True)
                raise ValueError(
                    f'Error loading JSON from file "{key}".\nIf you\'re trying to import non-JSON data '
                    f'(images, audio, text, etc.), edit storage settings and enable '
                    f'"Treat every bucket object as a source file"'
                )

            is_csv = data.get('is_csv', False)
            if is_csv:
                # Decode bytes to string
                decoded_str = data["data"].decode('utf-8')

                # Use pandas to read the CSV data
                df = pandas.read_csv(StringIO(decoded_str))
                # df_table=df.to_json(orient ='table')
                df_table=df.to_dict('records')
                for row in df_table:
                    dict_row ={
                        "data":row
                    }
                    task = self.add_task(dict_row, self.project, maximum_annotations, max_inner_id, self, key, link_class)
                    max_inner_id += 1

                    # update progress counters for storage info
                    tasks_created += 1

                    # add task to webhook list
                    tasks_for_webhook.append(task)
                        
            else:
                task = self.add_task(data, self.project, maximum_annotations, max_inner_id, self, key, link_class)
                max_inner_id += 1

                # update progress counters for storage info
                tasks_created += 1

                # add task to webhook list
                tasks_for_webhook.append(task)

            # settings.WEBHOOK_BATCH_SIZE
            # `WEBHOOK_BATCH_SIZE` sets the maximum number of tasks sent in a single webhook call, ensuring manageable payload sizes.
            # When `tasks_for_webhook` accumulates tasks equal to/exceeding `WEBHOOK_BATCH_SIZE`, they're sent in a webhook via
            # `emit_webhooks_for_instance`, and `tasks_for_webhook` is cleared for new tasks.
            # If tasks remain in `tasks_for_webhook` at process end (less than `WEBHOOK_BATCH_SIZE`), they're sent in a final webhook
            # call to ensure all tasks are processed and no task is left unreported in the webhook.
            if len(tasks_for_webhook) >= settings.WEBHOOK_BATCH_SIZE:
                emit_webhooks_for_instance(
                    self.project.organization, self.project, WebhookAction.TASKS_CREATED, tasks_for_webhook
                )
                tasks_for_webhook = []
        if tasks_for_webhook:
            emit_webhooks_for_instance(
                self.project.organization, self.project, WebhookAction.TASKS_CREATED, tasks_for_webhook
            )

        self.project.update_tasks_states(
            maximum_annotations_changed=False, overlap_cohort_percentage_changed=False, tasks_number_changed=True
        )

        # sync is finished, set completed status for storage info
        self.info_set_completed(last_sync_count=tasks_created, tasks_existed=tasks_existed)


    def scan_and_create_links(self):
        return self._scan_and_create_links(S3ImportStorageLink)
    

    def _get_validated_task(self, parsed_data, key):
        """Validate parsed data with labeling config and task structure"""
        if not isinstance(parsed_data, dict):
            raise TaskValidationError(
                'Error at ' + str(key) + ':\n' 'Cloud storage supports one task (one dict object) per JSON file only. '
            )
        return parsed_data

    def get_data(self, key):
        uri = f'{self.url_scheme}://{self.bucket}/{key}'
        if self.use_blob_urls:
            data_key = settings.DATA_UNDEFINED_NAME
            return {data_key: uri}

        # read task json from bucket and validate it
        client = self.get_client_and_resource()
        obj = client.getObject(self.bucket, key,loadStreamInMemory=True).body.buffer
        if uri.endswith(".csv"):
            value={
                "is_csv":True,
                "data":obj
            }
            return value
        else:
            value = json.loads(obj)
            if not isinstance(value, dict):
                raise ValueError(f'Error on key {key}: For S3 your JSON file must be a dictionary with one task')

            value = self._get_validated_task(value, key)
            return value

    def generate_http_url(self, url):
        return resolve_s3_url(url, self.get_client(), self.presign, expires_in=self.presign_ttl * 60)

    def get_blob_metadata(self, key):
        return AWS.get_blob_metadata(
            key,
            self.bucket,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            aws_session_token=self.aws_session_token,
            region_name=self.region_name,
            s3_endpoint=self.s3_endpoint,
        )

    class Meta:
        abstract = True


class S3ImportStorage(ProjectStorageMixin, S3ImportStorageBase):
    class Meta:
        abstract = False


class S3ExportStorage(S3StorageMixin, ExportStorage):
    is_export_csv = models.BooleanField(
        _('is_export_csv'), null=True, blank=True, help_text='Export csv to storage enabled'
    )
    def save_annotation(self, annotation):
        client = self.get_client_and_resource()
        logger.debug(f'Creating new object on {self.__class__.__name__} Storage {self} for annotation {annotation}')
        ser_annotation = self._get_serialized_data(annotation)

        # get key that identifies this object in storage
        key = S3ExportStorageLink.get_key(annotation)
        key = str(self.prefix) + '/' + key if self.prefix else key

        # put object into storage
        additional_params = {}

        self.cached_user = getattr(self, 'cached_user', annotation.task.project.organization.created_by)
        if flag_set(
            'fflag_feat_back_lsdv_3958_server_side_encryption_for_target_storage_short', user=self.cached_user
        ):
            if self.aws_sse_kms_key_id:
                additional_params['SSEKMSKeyId'] = self.aws_sse_kms_key_id
                additional_params['ServerSideEncryption'] = 'aws:kms'
            else:
                additional_params['ServerSideEncryption'] = 'AES256'

        # client.Object(self.bucket, key).put(Body=json.dumps(ser_annotation), **additional_params)
        client.putContent(bucketName=self.bucket, objectKey=key, content=json.dumps(ser_annotation))

        # create link if everything ok
        S3ExportStorageLink.create(annotation, self)
        
    @staticmethod
    def generate_export_file(project, tasks):
        # prepare for saving
        now = datetime.now()
        data = json.dumps(tasks, ensure_ascii=False)
        md5 = hashlib.md5(json.dumps(data).encode('utf-8')).hexdigest()   # nosec
        name = 'project-' + str(project.id) + '-at-' + now.strftime('%Y-%m-%d-%H-%M') + f'-{md5[0:8]}'

        input_json = DataExport.save_export_files(project, now, {}, data, md5, name)
        sep = ','
        
    
        start_time = time.time()
        logger.debug('Convert CSV started')

        # these keys are always presented
        keys = {'annotator', 'annotation_id', 'created_at', 'updated_at', 'lead_time'}

        # make 2 passes: the first pass is to get keys, otherwise we can't write csv without headers
        converter = Converter(
            config=project.get_parsed_config(),
            project_dir=None,
            upload_dir=os.path.join(settings.MEDIA_ROOT, settings.UPLOAD_DIR),
            download_resources=None,
        )
        item_iterator = converter.iter_from_json_file
        logger.debug('Prepare column names for CSV ...')
        for item in item_iterator(json_file=input_json):
            record = csv2.prepare_annotation_keys(item=item)
            keys.update(record)

        # the second pass is to write records to csv
        logger.debug(
            f'Prepare done in {time.time()-start_time:0.2f} sec. Write CSV rows now ...'
        )
        index=0
        csv_content=""
        key_list=[]
        for key in keys:
            if index==0:
                index+=1
            else:
                csv_content+=sep
            csv_content+='"'+key+'"'
            key_list.append(key)
        csv_content+='\n'
        line_num=0
        for item in item_iterator(input_json):
            line_num+=1

            record = csv2.prepare_annotation(item)
            df=pandas.DataFrame(record,index=[0],columns=key_list)
            index=0
            line=df.to_csv(index=False,header=False)
            csv_content+=line

        logger.debug(f'CSV conversion finished in {time.time()-start_time:0.2f} sec')
        return csv_content,name,line_num
        

    
    def save_all_annotations(self):
        annotation_exported = 0
        total_annotations = Annotation.objects.filter(project=self.project).count()
        self.info_set_in_progress()
        # export csv file
        if self.is_export_csv:
            query = Task.objects.filter(project=self.project)
            task_ids = query.values_list('id', flat=True)

            logger.debug('Serialize tasks for export')
            for _task_ids in batch(task_ids, 10000):
                query_set= query.filter(id__in=_task_ids)
                tasks = ExportDataSerializer(
                    # ExportAPI.get_task_queryset(queryset=query_set),
                    query_set.select_related('project').prefetch_related('annotations', 'predictions'),
                    many=True,
                    expand=['drafts'],
                    context={'interpolate_key_frames': None},
                ).data
                logger.debug('Prepare export files')
                
                csv_content,file_name,line_num= self.generate_export_file(self.project, tasks)
                
                client = self.get_client_and_resource()
                # get key that identifies this object in storage
                key = str(self.prefix) + '/' + file_name+".csv"
                client.putContent(bucketName=self.bucket, objectKey=key, content=csv_content)
                annotation_exported += line_num
                self.info_update_progress(last_sync_count=annotation_exported, total_annotations=total_annotations)
        
        # export json file
        else:
            for annotation in Annotation.objects.filter(project=self.project):
                self.save_annotation(annotation)

                # update progress counters
                annotation_exported += 1
                self.info_update_progress(last_sync_count=annotation_exported, total_annotations=total_annotations)

        self.info_set_completed(last_sync_count=annotation_exported, total_annotations=total_annotations)

    def delete_annotation(self, annotation):
        client = self.get_client_and_resource()
        logger.debug(f'Deleting object on {self.__class__.__name__} Storage {self} for annotation {annotation}')

        # get key that identifies this object in storage
        key = S3ExportStorageLink.get_key(annotation)
        key = str(self.prefix) + '/' + key if self.prefix else key

        # delete object from storage
        # s3.Object(self.bucket, key).delete()
        client.deleteObject(bucketName=self.bucket, objectKey=key)

        # delete link if everything ok
        S3ExportStorageLink.objects.filter(storage=self, annotation=annotation).delete()


def async_export_annotation_to_s3_storages(annotation):
    project = annotation.project
    if hasattr(project, 'io_storages_s3exportstorages'):
        for storage in project.io_storages_s3exportstorages.all():
            logger.debug(f'Export {annotation} to S3 storage {storage}')
            # if this storage setting that export csv enable, skip to sync json file to storage.
            if storage.is_export_csv:
                break
            storage.save_annotation(annotation)


@receiver(post_save, sender=Annotation)
def export_annotation_to_s3_storages(sender, instance, **kwargs):
    storages = getattr(instance.project, 'io_storages_s3exportstorages', None)
    if storages and storages.exists():  # avoid excess jobs in rq
        start_job_async_or_sync(async_export_annotation_to_s3_storages, instance)


@receiver(pre_delete, sender=Annotation)
def delete_annotation_from_s3_storages(sender, instance, **kwargs):
    links = S3ExportStorageLink.objects.filter(annotation=instance)
    for link in links:
        storage = link.storage
        if storage.can_delete_objects:
            logger.debug(f'Delete {instance} from S3 storage {storage}')  # nosec
            storage.delete_annotation(instance)


class S3ImportStorageLink(ImportStorageLink):
    storage = models.ForeignKey(S3ImportStorage, on_delete=models.CASCADE, related_name='links')

    @classmethod
    def exists(cls, key, storage):
        storage_link_exists = super(S3ImportStorageLink, cls).exists(key, storage)
        # TODO: this is a workaround to be compatible with old keys version - remove it later
        prefix = str(storage.prefix) or ''
        return (
            storage_link_exists
            or cls.objects.filter(key=prefix + key, storage=storage.id).exists()
            or cls.objects.filter(key=prefix + '/' + key, storage=storage.id).exists()
        )


class S3ExportStorageLink(ExportStorageLink):
    storage = models.ForeignKey(S3ExportStorage, on_delete=models.CASCADE, related_name='links')
