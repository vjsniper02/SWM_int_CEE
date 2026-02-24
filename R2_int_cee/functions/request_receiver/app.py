"""Module to provide generic SWM agency files transfers and Email Notifications"""

import os
import json
import boto3
import logging
import uuid
import datetime
import time
import pytz
from boto3 import client as boto3_client
from botocore.exceptions import ClientError
from functions.common_utils import CommonUtils, create_log_stream, setup_custom_logger

logger = logging.getLogger("cee_notification_engine")
logger.setLevel(logging.INFO)

ARN_SF_ADAPTOR = os.environ["ARN_SF_ADAPTOR_SERVICE"]
lambda_client = boto3_client("lambda", region_name=os.environ["SEIL_AWS_REGION"])


def put_log_events(log_group_name, log_stream_name, message):
    # Initialize the CloudWatch Logs client
    client = boto3.client("logs")

    # Serialize dictionary messages to JSON strings
    if isinstance(message, dict):
        message = json.dumps(message)

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


def get_aest_datetime():
    """
    Returns the current date and time in Australian Eastern Standard Time (AEST).

    Returns:
      datetime.datetime: The current date and time in AEST.
    """
    aest = pytz.timezone("Australia/Sydney")
    now_utc = datetime.datetime.now(pytz.utc)
    now_aest = now_utc.astimezone(aest)
    return now_aest

def get_sf_email_preference(holdings_id, event_id, aest_date_time, holdings_file_name):
    logger.info(f"Connecting to salesforce to get email preference data")
    payload = {
        "invocationType": "QUERY",
        "query": f"SELECT+Name,+SWM_Include_in_Holdings__c,+SWM_Holdings_Email_Address__c,+SWM_SFTP_Holdings__c,+SWM_CommonCRMID__c+from+Account+WHERE+SWM_External_Holdings_ID__c+='{holdings_id}'",
    }
    # # Set up the CloudWatch log group name
    LOG_GROUP_NAME = os.environ["LOG_GROUP_NAME"]
    # # Set up the log stream name
    LOG_STREAM_NAME = os.environ["LOG_STREAM_NAME"]
    try:
        invoke_response = lambda_client.invoke(
            FunctionName=ARN_SF_ADAPTOR,
            InvocationType="RequestResponse",
            Payload=json.dumps(payload),
        )
        downstream_response = json.loads(invoke_response["Payload"].read())
        if downstream_response["records"][0]["SWM_Include_in_Holdings__c"] == True:
            if downstream_response["records"][0]["SWM_Holdings_Email_Address__c"] == "" or downstream_response["records"][0]["SWM_Holdings_Email_Address__c"] == None:
                logger.info(f"Error retrieving preference details from Salesforce External Holdings ID [{holdings_id}]")
                message = f"REPORT|email|Holdings|failure||||SFERRORDEFAULTED Error retrieving preference details from Salesforce External Holdings ID [{holdings_id}]|{holdings_file_name}|{event_id}|{aest_date_time}|DEFAULTED"
                put_log_events(LOG_GROUP_NAME, LOG_STREAM_NAME, message)
                return False
        logger.info(f"Email preference response received: {downstream_response}")
        return (
            [downstream_response["records"][0]["SWM_Holdings_Email_Address__c"]],
            downstream_response["records"][0]["Name"],
            downstream_response["records"][0]["SWM_SFTP_Holdings__c"],
            downstream_response["records"][0]["SWM_CommonCRMID__c"],
        )
    except Exception as e:
        logger.info(f"Error getting salesforce email preference details: {e}")
        message = f"REPORT|email|Holdings|failure||||SFERRORDEFAULTED External Holdings ID [{holdings_id}] not found for Agency|{holdings_file_name}|{event_id}|{aest_date_time}|DEFAULTED"
        put_log_events(LOG_GROUP_NAME, LOG_STREAM_NAME, message)
        return False


def get_sf_email_template(event):
    logger.info(f"Connecting to salesforce to get email template data")
    common_utils = CommonUtils(event)
    email_template_name = common_utils.get_ssm_parameter(
        os.environ["SF_EMAIL_TEMPLATE_NAME"]
    )
    payload = {
        "invocationType": "QUERY",
        "query": f"SELECT+Subject,+HtmlValue+from+EmailTemplate+WHERE+DeveloperName='{email_template_name}'",
    }
    try:
        invoke_response = lambda_client.invoke(
            FunctionName=ARN_SF_ADAPTOR,
            InvocationType="RequestResponse",
            Payload=json.dumps(payload),
        )
        downstream_response = json.loads(invoke_response["Payload"].read())
        logger.info(f"Email template response received: {downstream_response}")
        return downstream_response
    except Exception as e:
        logger.info(f"Error getting salesforce Email Template: {e}")
        return False


def convert_email_list(email_list):
    return [email.strip() for emails in email_list for email in emails.split(";")]


def extract_single_key_name_from_path(payload):
    if (
        isinstance(payload, dict)
        and "body" in payload
        and isinstance(payload["body"], dict)
        and "files" in payload["body"]
        and isinstance(payload["body"]["files"], list)
        and payload["body"]["files"]
    ):  # Check if the list is not empty
        first_file_info = payload["body"]["files"][0]
        if (
            isinstance(first_file_info, dict)
            and "path" in first_file_info
            and isinstance(first_file_info["path"], str)
        ):
            path = first_file_info["path"]
            key_name = os.path.basename(path)
            return key_name
    return None


def lambda_handler(event, context):
    """
    Lambda handler function to redirect calls to SFTP and Email client based on request payload
    """
    logger.info(f"Raw evnt: {event}")
    if event.get("id") is not None and event["id"] != "":
        event_id = event["id"]
    else:
        event_id = str(uuid.uuid4())

    aest_date_time = get_aest_datetime()
    try:
        ssm_client = boto3.client("ssm")
        email_domain = (
            ssm_client.get_parameter(Name=os.environ["EMAIL_DOMAIN"])
            .get("Parameter")
            .get("Value")
        )
    except ClientError as e:
        logger.info(f"Send from: {e}")

    keys = []
    bucket = ""
    folder_name = ""
    transaction_type = ""
    agency_name = ""
    if not (event["body"].get("files") is None):
        s3_filepaths = event["body"]["files"]
        for file_path in s3_filepaths:
            bucket, key = file_path["path"].replace("s3://", "").split("/", 1)
            folder_name = key.split("/")
            folder_name = folder_name[0]
            keys.append(key)
    if event["body"]["type"] == "Holdings":
        holdings_file_name = extract_single_key_name_from_path(event)
        transaction_type = event["body"]["type"]
        from_email = f"no-reply@{email_domain}"
        logger.info(f"Received event data for Holdings: {event}")
        holdings_id = event["body"]["externalHoldingsId"]
        to_email, agency_name, sftp_send, crmid = get_sf_email_preference(
            holdings_id, event_id, aest_date_time, holdings_file_name
        )
        if len(to_email) > 0 and all(item is not None for item in to_email):
            to_email = convert_email_list(to_email)
        template_resp = get_sf_email_template(event)
        body = template_resp["records"][0]["HtmlValue"]
        subject = template_resp["records"][0]["Subject"]
    else:
        transaction_type = event["body"]["type"]
        from_email = f"ebooking-no-reply@{email_domain}"
        logger.info(f"Received event data for Ebookings/Others: {event}")
        to_email = event["body"]["emails"]
        body = event["body"]["body"]
        subject = event["body"]["subject"]
        sftp_send = "False"
        crmid = "000000"

    event["payload"] = {
        "id": event_id,
        "dateTime": f"{aest_date_time}",
        "type": transaction_type,
        "sftp": sftp_send,
        "crmid": crmid,
        "source": from_email,
        "destination": to_email,
        "agencyName": agency_name,
        "bodyData": body,
        "subject": subject,
        "attachment": keys,
        "bucketName": bucket,
    }
    return event
