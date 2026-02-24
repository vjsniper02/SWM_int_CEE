import os
import json
import boto3
import logging
import time
from botocore.exceptions import ClientError
from botocore.exceptions import NoCredentialsError
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from functions.common_utils import CommonUtils, create_log_stream, setup_custom_logger

logger = logging.getLogger("cee_notification_engine")
logger.setLevel(logging.INFO)


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


def send_email_with_attachments(
    subject,
    html_body,
    sender,
    recipient,
    bucket_name,
    attachment_keys,
    transaction_id,
    date_time,
):
    ses_client = boto3.client("ses", region_name=os.environ["SEIL_AWS_REGION"])

    # Create the MIME object
    msg = MIMEMultipart("mixed")
    msg["From"] = sender
    if len(recipient) > 0 and all(item is not None for item in recipient):
        msg["To"] = ", ".join(recipient)
    else:
        msg["To"] = ""
    msg["Subject"] = subject
    msg_body = MIMEMultipart("alternative")
    html_part = MIMEText(html_body, "html")
    msg_body.attach(html_part)
    # Attach the body
    msg.attach(msg_body)
    attachment_key_name = "" 
    # Attach the file as an attachment if available
    s3 = boto3.client("s3", region_name=os.environ["SEIL_AWS_REGION"])
    if len(attachment_keys) != 0:
        for attachment_key in attachment_keys:
            response = s3.get_object(Bucket=bucket_name, Key=attachment_key)
            file_content = response["Body"].read()
            attachment_key_name = attachment_key
        part = MIMEApplication(file_content, Name=attachment_key)
        part["Content-Disposition"] = f'attachment; filename="{attachment_key}"'
        msg.attach(part)

    # # Set up the CloudWatch log group name
    LOG_GROUP_NAME = os.environ["LOG_GROUP_NAME"]
    # # Set up the log stream name
    LOG_STREAM_NAME = os.environ["LOG_STREAM_NAME"]
    # # Set up the log stream name for logging messag ID along with attachment name
    LOG_STREAM_NAME_MESSAGE_ID = os.environ["LOG_STREAM_NAME_MESSAGE_ID"]
    for recipient_item in recipient:
        logger.info(recipient_item)
        try:
            response = ses_client.send_raw_email(
                Source=sender,
                Destinations=[recipient_item],
                RawMessage={"Data": msg.as_string()},
                ConfigurationSetName=os.environ["SES_CONFIGURATION_SET"],
            )
            logger.info(f"Email sent! Message ID: { response['MessageId']}")
            if "MessageId" in response:
                # message = f"REPORT|email|Holdings|success|{sender}|{subject}|{recipient}| |{attachment_key}"
                put_log_events(
                    LOG_GROUP_NAME,
                    LOG_STREAM_NAME_MESSAGE_ID,
                    {
                        "messageId": response["MessageId"],
                        "attachmentName": attachment_key_name,
                        "transaction_id": transaction_id,
                        "date_time": date_time,
                    },
                )
            else:
                logger.warning("Email sent, but no MessageId received in the response.")
        except ses_client.exceptions.MessageRejected as e:
            logger.info(f"Email sending failed: Message rejected. Reason: {str(e)}")
            message = f"REPORT|email|Holdings|failure|{sender}|{subject}|{recipient_item}|{str(e)}|{attachment_key_name}|{transaction_id}|{date_time}"
            put_log_events(LOG_GROUP_NAME, LOG_STREAM_NAME, message)
        except ses_client.exceptions.MailFromDomainNotVerifiedException as e:
            logger.info(
                f"Email sending failed: Mail from domain not verified. Reason: {str(e)}"
            )
            message = f"REPORT|email|Holdings|failure|{sender}|{subject}|{recipient_item}|{str(e)}|{attachment_key_name}|{transaction_id}|{date_time}"
            put_log_events(LOG_GROUP_NAME, LOG_STREAM_NAME, message)
        except ses_client.exceptions.ConfigurationSetDoesNotExistException as e:
            logger.info(
                f"Email sending failed: Configuration set does not exist. Reason: {str(e)}"
            )
            message = f"REPORT|email|Holdings|failure|{sender}|{subject}|{recipient_item}|{str(e)}|{attachment_key_name}|{transaction_id}|{date_time}"
            put_log_events(LOG_GROUP_NAME, LOG_STREAM_NAME, message)
        except Exception as e:
            logger.info(f"An unexpected error occurred while sending the email: {str(e)}")
            message = f"REPORT|email|Holdings|failure|{sender}|{subject}|{recipient_item}|{str(e)}|{attachment_key_name}|{transaction_id}|{date_time}"
            put_log_events(LOG_GROUP_NAME, LOG_STREAM_NAME, message)


def lambda_handler(event, context):
    """
    Lamda handler to orchestrate SES email trigger
    """
    logger.info(f"CEE Event data - Email handler: {event}")

    subject = event["payload"]["subject"]
    body = event["payload"]["bodyData"]
    from_email = event["payload"]["source"]
    to_email = event["payload"]["destination"]
    bucket = event["payload"]["bucketName"]
    keys = event["payload"]["attachment"]
    transaction_id = event["payload"]["id"]
    date_time = event["payload"]["dateTime"]

    send_email_with_attachments(
        subject, body, from_email, to_email, bucket, keys, transaction_id, date_time
    )
