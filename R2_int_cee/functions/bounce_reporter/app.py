import json
import logging
import boto3
import os
import datetime
import time
from datetime import datetime, timedelta, timezone

from botocore.exceptions import ClientError
from functions.common_utils import CommonUtils, create_log_stream, setup_custom_logger

logger = logging.getLogger("cee_notification_engine")
logger.setLevel(logging.INFO)

# def get_attachment_name(log_group, log_stream, cw_logs_client, message_id):
#     end_time = int((datetime.utcnow() + timedelta(minutes=2)).timestamp() * 1000)  # Current time in milliseconds
#     start_time = int((datetime.utcnow() - timedelta(days=2, minutes=15)).timestamp() * 1000) # 24 hours ago in milliseconds
#     logger.info(start_time)
#     logger.info(end_time)
#     logger.info(log_group)
#     logger.info(log_stream)
#     try:
#         response = cw_logs_client.filter_log_events(
#             logGroupName=log_group,
#             logStreamNames=[log_stream],
#             startTime=start_time,
#             endTime=end_time,
#             filterPattern=f'"{message_id}"',
#             limit=1000,
#             interleaved=True
#         )
#         logger.info(response)
#         events = response.get('events', [])

#         if events:
#             for event_data in events:
#                 log_event = json.loads(event_data['message'])
#                 if 'attachmentName' in log_event:
#                     logger.info(f"attachmentName found for the given messageId : {log_event['attachmentName']}")
#                     return log_event['attachmentName']
#             logger.info(f"attachmentName not found for the given messageId : {log_event['attachmentName']}")
#             return 404
#         else:
#             logger.info("messageId not found in the specified time range")
#             return 404

#     except Exception as e:
#         logger.info(f"Error exception: {e}")


def get_attachment_transaction_id(
    log_group_name, log_stream_name, cw_logs_client, message_id
):
    client = boto3.client("logs")

    # Define time range (last 2 days)
    start_time = int(
        (datetime.utcnow() - timedelta(days=2, minutes=15)).timestamp() * 1000
    )
    end_time = int(time.time() * 1000)  # Current time in milliseconds

    next_token = None
    attachment_name = None

    while True:
        params = {
            "logGroupName": log_group_name,
            "logStreamName": log_stream_name,
            "startTime": start_time,
            "endTime": end_time,
            "startFromHead": False,  # Fetch newest logs first
            "limit": 1000,
        }

        if next_token:
            params["nextToken"] = next_token  # Handle pagination

        response = client.get_log_events(**params)

        # Loop through the logs to find the matching messageId
        for event in response.get("events", []):
            try:
                log_message = json.loads(event["message"])  # Convert log to JSON
                if log_message.get("messageId") == message_id:
                    attachment_name = log_message.get("attachmentName")
                    transaction_id = log_message.get("transaction_id")
                    date_time = log_message.get("date_time")
                    print(f"Found transaction_id: {transaction_id}")
                    print(f"Found attachmentName: {attachment_name}")
                    return (
                        transaction_id,
                        attachment_name,
                        date_time,
                    )  # Stop searching once found
            except json.JSONDecodeError:
                continue  # Skip logs that are not JSON

        next_token = response.get("nextForwardToken")
        if not next_token:
            break  # No more logs to process

    print("No attachmentName found for the given messageId.")
    return None


def lambda_handler(event, context):
    # Event data from SES
    """
    {
       "Records":[
          {
             "EventSource":"aws:sns",
             "EventVersion":"1.0",
             "EventSubscriptionArn":"arn:aws:sns:ap-southeast-2:019092404871:seil-r2uat-cee-EmailNotificationSNS-xidivQP1cKCV:33929ebc-d39f-48d9-8598-3270bb69d6fd",
             "Sns":{
                "Type":"Notification",
                "MessageId":"3e97ae67-fef4-559e-962b-ce420aa570cd",
                "TopicArn":"arn:aws:sns:ap-southeast-2:019092404871:seil-r2uat-cee-EmailNotificationSNS-xidivQP1cKCV",
                "Subject":"Amazon SES Email Event Notification",
                "Message":{
                   "eventType":"Delivery",
                   "mail":{
                      "timestamp":"2025-02-12T00:57:52.050Z",
                      "source":"ebooking-no-reply@test.code7swm.com.au",
                      "sourceArn":"arn:aws:ses:ap-southeast-2:019092404871:identity/test.code7swm.com.au",
                      "sendingAccountId":"019092404871",
                      "messageId":"01080194f7a98eb2-e4b2fc48-eb54-4cd9-be6c-65785ad98de6-000000",
                      "destination":[
                         "BrJohnson@Seven.com.au"
                      ],
                      "headersTruncated":false,
                      "headers":[
                         {
                            "name":"Content-Type",
                            "value":"multipart/mixed; boundary=\"===============0854943399810140822==\""
                         },
                         {
                            "name":"MIME-Version",
                            "value":"1.0"
                         },
                         {
                            "name":"From",
                            "value":"ebooking-no-reply@test.code7swm.com.au"
                         },
                         {
                            "name":"To",
                            "value":"BrJohnson@Seven.com.au"
                         },
                         {
                            "name":"Subject",
                            "value":"BRQ File is in the past"
                         }
                      ],
                      "commonHeaders":{
                         "from":[
                            "ebooking-no-reply@test.code7swm.com.au"
                         ],
                         "to":[
                            "BrJohnson@Seven.com.au"
                         ],
                         "messageId":"01080194f7a98eb2-e4b2fc48-eb54-4cd9-be6c-65785ad98de6-000000",
                         "subject":"BRQ File is in the past"
                      },
                      "tags":{
                         "ses:source-tls-version":[
                            "TLSv1.3"
                         ],
                         "ses:operation":[
                            "SendRawEmail"
                         ],
                         "ses:configuration-set":[
                            "seil-r2uat-CEEConfigurationSet"
                         ],
                         "ses:outgoing-tls-version":[
                            "TLSv1.3"
                         ],
                         "ses:source-ip":[
                            "3.106.255.209"
                         ],
                         "ses:from-domain":[
                            "test.code7swm.com.au"
                         ],
                         "ses:caller-identity":[
                            "seil-r2uat-cee-CEEEmailFunctionRole-xU8732dkEmoq"
                         ],
                         "ses:outgoing-ip":[
                            "69.169.232.9"
                         ]
                      }
                   },
                   "delivery":{
                      "timestamp":"2025-02-12T00:57:53.386Z",
                      "processingTimeMillis":1336,
                      "recipients":[
                         "BrJohnson@Seven.com.au"
                      ],
                      "smtpResponse":"250 2.6.0 <01080194f7a98eb2-e4b2fc48-eb54-4cd9-be6c-65785ad98de6-000000@ap-southeast-2.amazonses.com> [InternalId=20779051818669, Hostname=MEUP300MB0060.AUSP300.PROD.OUTLOOK.COM] 28826 bytes in 0.126, 222.295 KB/sec Queued mail for delivery",
                      "remoteMtaIp":"52.101.149.2",
                      "reportingMTA":"b232-9.smtp-out.ap-southeast-2.amazonses.com"
                   }
                },
                "Timestamp":"2025-02-12T00:57:53.472Z",
                "SignatureVersion":"1",
                "Signature":"jTADSN/lpbzZE+E2nJ21XS3XcV4UsCWyMorrNqR3YMRJlq+2F0cMY/njRvh433sB/222X/Mhwla5vNDh3ALxCM0uTev+verfvSdHzjyY0TJxVZG3eO5AlW5O+rc/i1EK45cL8UKTvSikKvZtJZDXa3rt0s8n5pXlUFzoeRx66qv7+biK72lHUvQBA+NCMxTb3D3pG4ZLuRqe791hbskss1tLm2B0LP4uJ5I6wPCdObGR90PUo0vzx5W2nZlZ9EgkqpTeK+TfpaxQd/UCk2Eci07WZ+yV2Gsurd7U07h2XoCypmD0alESUWMNIsxJYQeSVC/IflAZgmKl9vXhRcgciw==",
                "SigningCertUrl":"https://sns.ap-southeast-2.amazonaws.com/SimpleNotificationService-9c6465fa7f48f5cacd23014631ec1136.pem",
                "UnsubscribeUrl":"https://sns.ap-southeast-2.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:ap-southeast-2:019092404871:seil-r2uat-cee-EmailNotificationSNS-xidivQP1cKCV:33929ebc-d39f-48d9-8598-3270bb69d6fd",
                "MessageAttributes":{

                }
             }
          }
       ]
    }
    """
    event = f"{json.dumps(event)}"
    event = json.loads(event)
    message = json.loads(event["Records"][0]["Sns"]["Message"])
    logger.info(type(event))
    logger.info(event)

    # Initialize CloudWatch Logs client
    cw_logs_client = boto3.client("logs")
    # Set up the CloudWatch log group name
    LOG_GROUP_NAME = os.environ["LOG_GROUP_NAME"]
    # Set up the log stream name
    LOG_STREAM_NAME = os.environ["LOG_STREAM_NAME"]
    LOG_STREAM_NAME_MESSAGE_ID = os.environ["LOG_STREAM_NAME_MESSAGE_ID"]
    # Create or update the log stream
    create_log_stream(cw_logs_client, LOG_GROUP_NAME, LOG_STREAM_NAME)
    # Set up the logger
    custom_logger = setup_custom_logger(
        "cee_bounce_reporter_logger", LOG_GROUP_NAME, LOG_STREAM_NAME
    )

    # Retrieve the SNS message from the event
    # sns_message = json.dumps(event["Records"][0]["Sns"]["Message"])
    # logger.info(sns_message)
    logger.info(message)
    logger.info(type(message))
    message_id = message["mail"]["commonHeaders"]["messageId"]
    logger.info(message_id)
    subject = message["mail"]["commonHeaders"]["subject"]
    recipients = message["mail"]["destination"]
    sender = message["mail"]["commonHeaders"]["from"]
    time.sleep(30)
    transaction_id, attachment_key, date_time = get_attachment_transaction_id(
        LOG_GROUP_NAME, LOG_STREAM_NAME_MESSAGE_ID, cw_logs_client, message_id
    )
    # ToDo Not to log logs from delivery reporter
    if message["eventType"] == "Bounce":
        if attachment_key != 404:
            log_report_message = f"REPORT|email|Holdings|failure|{sender}|{subject}|{recipients}|E-mail Bounce Back Error|{attachment_key}|{transaction_id}|{date_time}"
            custom_logger.info(log_report_message)
    if message["eventType"] == "Delivery":
        if attachment_key != 404:
            log_report_message = f"REPORT|email|Holdings|success|{sender}|{subject}|{recipients}| |{attachment_key}|{transaction_id}|{date_time}"
            custom_logger.info(log_report_message)
