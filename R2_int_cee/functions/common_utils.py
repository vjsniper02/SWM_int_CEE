import logging
import json
import os
import boto3
import botocore.exceptions
from botocore.exceptions import ClientError
import paramiko
from boto3 import client as boto3_client
from boto3 import resource


class CommonUtils:
    def __init__(self, event):
        self.event = event
        self.region = os.environ["SEIL_AWS_REGION"]
        # prepare the logger with correlation ID
        self.logger = logging.getLogger("common_utils")
        self.logger.setLevel(logging.INFO)
        self.logger.info(self.event)

    def get_ssm_parameter(self, parameter_name: str) -> str:
        """Function to retrive AWS SSM parameters"""
        try:
            ssm = boto3.client("ssm")
            response = ssm.get_parameter(Name=parameter_name, WithDecryption=True)
            return response["Parameter"]["Value"]
        except botocore.exceptions.ClientError as e:
            self.logger.info(f"exception error in getting ssm param: {e}")
            return e

    def connect_to_sftp_ssh(
        self, hostname: str, port: int, username: str, ssh_key: str
    ) -> object:
        """Funtion to create connection to SFTP server"""
        transport = paramiko.Transport((hostname, port))
        transport.connect(username=username, pkey=ssh_key)
        sftp = paramiko.SFTPClient.from_transport(transport)
        return sftp

    def connect_to_sftp_passd(
        self, hostname: str, port: int, username: str, password: str
    ) -> object:
        """Funtion to create connection to SFTP server"""
        transport = paramiko.Transport((hostname, port))
        transport.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        return sftp

    def transfer_file(self, s3_bucket: str, s3_key: str, sftp: object, sftp_path: str):
        """Funtion to transfer file to SFTP location"""
        s3 = boto3.client("s3")
        s3_object = s3.get_object(Bucket=s3_bucket, Key=s3_key)
        sftp_resp = sftp.putfo(s3_object["Body"], sftp_path + s3_key)
        self.logger.info(sftp_resp)

    def connect_to_s3(self, bucket_name: str):
        """Funtion to connect s3 bucket"""
        s3 = boto3.resource("s3")
        bucket = s3.Bucket(bucket_name)
        return bucket

    def get_secret(self, secret_name: str) -> dict:
        "Get secret from Secret Manager"
        session = boto3.session.Session()
        client = session.client(service_name="secretsmanager")
        try:
            get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        except ClientError as e:
            raise e
        secret = json.loads(get_secret_value_response["SecretString"])

        if secret is None:
            return None

        secret_dict = {
            "ftp_url": secret.get("server", "test.com"),
            "ftp_port": secret.get("port", 21),
            "user_id": secret.get("username", "seven-sftp"),
            "password": secret.get("password", "sftp-password"),
            "key_value": secret.get("SSHKey", "1234"),
            "path": secret.get("path", "/root"),
        }

        return secret_dict


class CloudWatchLogsHandler(logging.Handler):
    def __init__(self, log_group_name, log_stream_name):
        super().__init__()
        self.log_group_name = log_group_name
        self.log_stream_name = log_stream_name
        self.cw_logs_client = boto3.client("logs")
        self.logger = logging.getLogger("common_utils")
        self.logger.setLevel(logging.INFO)

    def emit(self, record):
        try:
            log_entry = self.format(record)
            self.cw_logs_client.put_log_events(
                logGroupName=self.log_group_name,
                logStreamName=self.log_stream_name,
                logEvents=[
                    {"timestamp": int(record.created * 1000), "message": log_entry}
                ],
            )
        except Exception as e:
            self.logger.info(f"Failed to log message to CloudWatch Logs: {e}")


def setup_custom_logger(name, log_group_name, log_stream_name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # Create CloudWatch Logs handler and set level to debug
    cw_handler = CloudWatchLogsHandler(log_group_name, log_stream_name)
    cw_handler.setLevel(logging.DEBUG)

    # Add CloudWatch Logs handler to logger
    logger.addHandler(cw_handler)

    return logger


def create_log_stream(cw_logs_client, log_group_name, log_stream_name):
    try:
        cw_logs_client.create_log_stream(
            logGroupName=log_group_name, logStreamName=log_stream_name
        )
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceAlreadyExistsException":
            raise
