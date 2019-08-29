"""Check for existence of specified files on S3"""
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
import os

class S3DataExistsOperator(BaseOperator):

    ui_color = '#A6E6A6'
    template_fields = ("prefix",)

    @apply_defaults
    def __init__(self,
                aws_conn_id="",
                bucket=None,
                prefix=None,
                key=None,
                wildcard_key = None,
                 *args, **kwargs):

        super(S3DataExistsOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.bucket=bucket
        self.prefix=prefix
        self.key=key
        self.wildcard_key=wildcard_key

    def execute(self, context):
        self.log.info(f'S3DataExistsOperator')
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        rendered_prefix = self.prefix.format(**context)

        success = s3_hook.check_for_bucket(self.bucket)
        if success:
            self.log.info("Found the bucket: {}".format(self.bucket))
        else:
            self.log.info("Invalid bucket: {}".format(self.bucket))
            raise FileNotFoundError("No S3 bucket named {}".format(self.bucket))

        success = s3_hook.check_for_prefix(prefix=rendered_prefix,
                                        delimiter='/',
                                        bucket_name=self.bucket)
        if success:
            self.log.info("Found the prefix: {}".format(rendered_prefix))
        else:
            self.log.info("Invalid prefix: {}".format(rendered_prefix))
            raise FileNotFoundError("No prefix named {}/{} ".format(self.bucket, rendered_prefix))

        if self.wildcard_key is not None:
            full_key = os.path.join(rendered_prefix, self.wildcard_key)
            success = s3_hook.check_for_wildcard_key(wildcard_key = full_key,
                                            bucket_name=self.bucket,
                                            delimiter='/')
            if success:
                self.log.info("Found the key: {}".format(full_key))
            else:
                self.log.info("Invalid key: {}".format(full_key))
                raise FileNotFoundError("No key named {}/{} ".format(self.bucket, full_key))

        if self.key is not None:
            full_key = os.path.join(rendered_prefix, self.key)
            success = s3_hook.check_for_key(key = full_key,
                                            bucket_name=self.bucket)
            if success:
                self.log.info("Found the key: {}".format(full_key))
            else:
                self.log.info("Invalid key: {}".format(full_key))
                raise FileNotFoundError("No key named {}/{} ".format(self.bucket, full_key))
