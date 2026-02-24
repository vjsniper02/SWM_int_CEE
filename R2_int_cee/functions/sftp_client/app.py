import json
import logging
import boto3
from io import BytesIO
import paramiko
from botocore.exceptions import ClientError
import io
import os
import time
import base64
from io import StringIO
from functions.common_utils import CommonUtils, create_log_stream, setup_custom_logger

logger = logging.getLogger("cee_notification_engine")
logger.setLevel(logging.INFO)


def put_log_events(log_group_name, log_stream_name, message):
    # Initialize the CloudWatch Logs client
    client = boto3.client("logs")

    log_event = {"message": message, "timestamp": int(round(time.time() * 1000))}
    # Get the sequence token for the log stream
    response = client.describe_log_streams(
        logGroupName=log_group_name, logStreamNamePrefix=log_stream_name
    )
    log_streams = response.get("logStreams", [0])
    sequence_token = log_streams[0].get("uploadSequenceToken", None)

    log_event_request = {
        "logGroupName": log_group_name,
        "logStreamName": log_stream_name,
        "logEvents": [log_event],
    }
    if sequence_token:
        log_event_request["sequenceToken"] = sequence_token

    client.put_log_events(**log_event_request)


def lambda_handler(event, context):
    """lambda handler function to orchestrate file transfer"""
    common_utils = CommonUtils(event)
    transaction_id = event["payload"]["id"]
    date_time = event["payload"]["dateTime"]
    logger.info(f"Event data: {event}")
    s3_filepath = event["body"]["files"][0]["path"]
    transfer_from_bucket, key = s3_filepath.replace("s3://", "").split("/", 1)

    path_start = os.environ["HOLDINGS_SFTP_SECRET_NAME"]

    # Extract the CRM ID from the payload
    crmid = event["payload"]["crmid"]
    # Convert the CRM ID to a string (if it's not already a string) and encode it into bytes
    crmid_bytes = crmid.encode("utf-8")
    # Encode the bytes into Base64
    crmid_base64 = base64.b64encode(crmid_bytes).decode("utf-8")

    sec_path = f"{path_start}/{crmid_base64}/Holdings/FTP"
    # # Set up the CloudWatch log group name
    LOG_GROUP_NAME = os.environ["LOG_GROUP_NAME"]
    # # Set up the log stream name
    LOG_STREAM_NAME = os.environ["LOG_STREAM_NAME"]

    try:
        ftp_dict = common_utils.get_secret(sec_path)
        sftp_path = ftp_dict["path"]

        if ftp_dict["key_value"] != "1234":
            private_key_file = StringIO()
            private_key_file.write(ftp_dict["key_value"])
            private_key_file.seek(0)
            ssh_key = paramiko.RSAKey.from_private_key(private_key_file)

        try:
            # Connect to Landmark SFTP server
            if "ssh_key" in locals():
                sftp = common_utils.connect_to_sftp_ssh(
                    ftp_dict["ftp_url"],
                    int(ftp_dict["ftp_port"]),
                    ftp_dict["user_id"],
                    ssh_key,
                )
            else:
                sftp = common_utils.connect_to_sftp_passd(
                    ftp_dict["ftp_url"],
                    int(ftp_dict["ftp_port"]),
                    ftp_dict["user_id"],
                    ftp_dict["password"],
                )

            common_utils.transfer_file(
                transfer_from_bucket,
                key,
                sftp,
                sftp_path,
            )

            sftp.close()
            # REPORT|ftp|Holdings|Success|agencyName|contactEmail|Holdings_2024-04-12 0702 000343_SEVNET.h.zip
            message = f"REPORT|ftp|Holdings|success|{event['payload']['agencyName']}|{event['payload']['destination']}|{key}||{transaction_id}|{date_time}"
            put_log_events(LOG_GROUP_NAME, LOG_STREAM_NAME, message)
        except paramiko.AuthenticationException as auth_error:
            logger.info(f"Authentication Error : {auth_error}")
            message = f"REPORT|ftp|Holdings|failure|{event['payload']['agencyName']}|{event['payload']['destination']}|{key}|Authentication Error|{transaction_id}|{date_time}"
            put_log_events(LOG_GROUP_NAME, LOG_STREAM_NAME, message)
        except paramiko.SSHException as ssh_error:
            logger.info(f"SSH Exception : {ssh_error}")
            message = f"REPORT|ftp|Holdings|failure|{event['payload']['agencyName']}|{event['payload']['destination']}|{key}|SSH Exception Error|{transaction_id}|{date_time}"
            put_log_events(LOG_GROUP_NAME, LOG_STREAM_NAME, message)
        except FileNotFoundError as file_error:
            logger.info(f"File Not Found : {file_error}")
            message = f"REPORT|ftp|Holdings|failure|{event['payload']['agencyName']}|{event['payload']['destination']}|{key}|File Not Found Error|{transaction_id}|{date_time}"
            put_log_events(LOG_GROUP_NAME, LOG_STREAM_NAME, message)
        except Exception as e:
            logger.info(f"Error occured on SFTP transfer : {e}")
            message = f"REPORT|ftp|Holdings|failure|{event['payload']['agencyName']}|{event['payload']['destination']}|{key}|Error occured on SFTP transfer|{transaction_id}|{date_time}"
            put_log_events(LOG_GROUP_NAME, LOG_STREAM_NAME, message)
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            logger.info(f"The secret '{sec_path}' was not found")
            message = f"REPORT|ftp|Holdings|failure|{event['payload']['agencyName']}|{event['payload']['destination']}|{key}|ResourceNotFoundException|{transaction_id}|{date_time}"
            put_log_events(LOG_GROUP_NAME, LOG_STREAM_NAME, message)
    except Exception as e:
        logger.info(f"Exception occured on getting sftp sever details: {e}")
        message = f"REPORT|ftp|Holdings|failure|{event['payload']['agencyName']}|{event['payload']['destination']}|{key}|Error Exception occured|{transaction_id}|{date_time}"
        put_log_events(LOG_GROUP_NAME, LOG_STREAM_NAME, message)
