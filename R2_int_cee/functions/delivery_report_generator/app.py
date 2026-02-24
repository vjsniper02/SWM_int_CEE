import boto3
import os
import logging
import datetime
import sys
from datetime import datetime, timedelta, timezone
from botocore.exceptions import ClientError

logger = logging.getLogger("CEE_bounce_reporter")
logger.setLevel(logging.INFO)
AWS_REGION = os.environ["SEIL_AWS_REGION"]


def count_s3_files_past_24_hours(bucket_name, prefix=""):
    """
    Counts files in an S3 bucket modified in the past 24 hours.

    Args:
        bucket_name (str): The name of the S3 bucket.
        prefix (str, optional): A prefix to filter objects within the bucket. Defaults to ''.

    Returns:
        int: The number of files modified in the past 24 hours.
    """

    s3 = boto3.client("s3")
    count = 0
    now_utc = datetime.now(timezone.utc)
    start_time = now_utc - timedelta(days=1)
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if "Contents" in page:
            for obj in page["Contents"]:
                last_modified = obj["LastModified"].replace(tzinfo=timezone.utc)
                if start_time <= last_modified < now_utc:
                    count += 1
    return count


def extract_logs(log_group_name, log_stream_name):
    """
    Extracts log data in the past 24 hours.

    Args:
        log_group_name (str): The name of the log group.
        log_stream_name (str): The name of the log stream.

    Returns:
        dict: Log data in the past 24 hours.
    """

    # Initialize AWS CloudWatch Logs client
    client = boto3.client("logs")
    logs = []
    next_token = None
    # Get current time
    current_time = datetime.now(timezone.utc)
    # Calculate start and end time for the past 24 hour
    end_time = current_time
    start_time = end_time - timedelta(hours=24)
    logger.info(f" End Time: {end_time}")
    logger.info(f"Start Time: {start_time}")
    # Get logs for the past 24 hour

    while True:
        kwargs = {
            "logGroupName": log_group_name,
            "logStreamName": log_stream_name,
            "startTime": int(start_time.timestamp()) * 1000,
            "endTime": int(end_time.timestamp()) * 1000,
            "startFromHead": True,
        }
        if next_token:
            kwargs["nextToken"] = next_token

        response = client.get_log_events(**kwargs)

        logs.extend(response["events"])

        next_token = response.get("nextForwardToken")

        if not next_token or next_token == kwargs.get("nextToken"):
            break

    response["events"] = logs
    logger.info(f"Total log events for a day: {response}")
    return response


def parse_logdata_to_json(log_data):
    """
    Parse log data to JSON.

    Args:
        log_data (str): Log details extracted for past 24 hours.

    Returns:
        dict: Parsed Log data in the past 24 hours.
    """

    """
    ************ FTP log_parts details *************
    # Identifier(REPORT)        - log_parts[0]
    # Delivery Mode(email/ftp)  - log_parts[1]
    # Domain(Holdings)          - log_parts[2]
    # Status(success/failure)   - log_parts[3]
    # Agency Name               - log_parts[4]
    # Contact email             - log_parts[5]
    # Attachment Name           - log_parts[6]
    # Error message             - log_parts[7]
    # Transaction Id            - log_parts[8]
    # Transaction datetime      - log_parts[9]


    ************ Email log_parts details *************
    Identifier(REPORT)          - log_parts[0]
    # Delivery Mode(email/ftp)  - log_parts[1]
    # Domain(Holdings)          - log_parts[2]
    # Status(success/failure)   - log_parts[3]
    # Sender                    - log_parts[4]
    # Subject                   - log_parts[5]
    # Recipient                 - log_parts[6]
    # Error message             - log_parts[7]
    # Attachment Name           - log_parts[8]
    # Transaction Id            - log_parts[9]
    # Transaction datetime      - log_parts[10]
    # DEFAULTED MESSAGE         - log_parts[11]
    """
    # Parse log events
    log_dict = {}
    transaction_id = ""
    attachment_name = ""
    transaction_datetime = ""
    delivery_mode = ""
    delivery_status = ""
    email_recipient = ""
    delivery_mode_data = ""
    for event in log_data["events"]:
        log_message = event["message"]
        log_parts = log_message.split("|")
        if log_parts[1] == "ftp":
            attachment_name = log_parts[6]

        if log_parts[1] == "email":
            attachment_name = log_parts[8]

        if (
            len(log_parts) >= 3
            and log_parts[0] == "REPORT"
            and log_parts[2] == "Holdings"
            and "Holdings_" in attachment_name
            and attachment_name != "Holdings_"
        ):
            delivery_mode = log_parts[1]
            delivery_status = log_parts[3]
            delivery_error = log_parts[7]

            if log_parts[1] == "ftp":
                transaction_id = log_parts[8]
                transaction_datetime = log_parts[9]
                attachment_name = log_parts[6]

            if log_parts[1] == "email":
                transaction_id = log_parts[9]
                transaction_datetime = log_parts[10]
                attachment_name = log_parts[8]
                email_recipient = log_parts[6]

            if transaction_id in log_dict:
                existing_data = log_dict[transaction_id]
                if delivery_mode == "email":
                    delivery_preference = existing_data[attachment_name][
                        "delivery_preference"
                    ]
                    if "email" in delivery_preference:
                        existing_data[attachment_name]["delivery_preference"]["email"][
                            "recipients"
                        ].append(
                            {
                                email_recipient: {
                                    "delivery_status": delivery_status,
                                    "error": delivery_error,
                                }
                            }
                        )
                    else:
                        delivery_mode = {
                            "recipients": [
                                {
                                    email_recipient: {
                                        "delivery_status": delivery_status,
                                        "error": delivery_error,
                                    }
                                }
                            ]
                        }
                        existing_data[attachment_name]["delivery_preference"][
                            "email"
                        ] = delivery_mode
                if delivery_mode == "ftp":
                    existing_data[attachment_name]["delivery_preference"]["ftp"] = {
                        "delivery_status": delivery_status,
                        "error": delivery_error,
                    }

            else:
                if delivery_mode == "ftp":
                    delivery_mode_data = {
                        "delivery_status": delivery_status,
                        "error": delivery_error,
                    }
                if delivery_mode == "email":
                    delivery_mode_data = {
                        "recipients": [
                            {
                                email_recipient: {
                                    "delivery_status": delivery_status,
                                    "error": delivery_error,
                                }
                            }
                        ]
                    }
                log_dict[transaction_id] = {
                    "dateTime": transaction_datetime,
                    "attachment_name": attachment_name,
                    attachment_name: {
                        "delivery_preference": {delivery_mode: delivery_mode_data}
                    },
                }
    logger.info(f"Parsed log data: {log_dict}")
    return log_dict


def get_delivery_preference(delivery_preference_data):
    """
    Concatenates the keys within the delivery_data dictionary with commas.

    Args:
        delivery_data (dict): The input dictionary.

    Returns:
        str: A comma-separated string of the keys.
    """
    if not isinstance(delivery_preference_data, dict):
        return ""  # Handle invalid input

    keys = []
    for key, value in delivery_preference_data.items():
        if key == "email":
            key = "Email"
        if key == "ftp":
            key = "FTP"
        keys.append(key)

    return ", ".join(keys)


def get_delivery_status(delivery_data):
    """
    Determines the overall delivery status based on email and ftp delivery statuses.

    Args:
        delivery_data (dict): The input dictionary containing email and ftp delivery information.

    Returns:
        str: The overall delivery status ("Success" or "Failure").
    """
    ftp_status = "failure"  # Default to failure
    email_recipients_success = False
    email_recipients_failure = False

    if "email" in delivery_data and "recipients" in delivery_data["email"]:
        recipients = delivery_data["email"]["recipients"]
        if recipients:
            for recipient_dict in recipients:
                for email_address, status_dict in recipient_dict.items():
                    if "delivery_status" in status_dict:
                        if status_dict["delivery_status"] == "success":
                            email_recipients_success = True
                        else:
                            email_recipients_failure = True

    if "ftp" in delivery_data and "delivery_status" in delivery_data["ftp"]:
        ftp_status = delivery_data["ftp"]["delivery_status"]

    if email_recipients_success:
        if "ftp" in delivery_data and ftp_status == "failure":
            return "Success"
        else:
            return "Success"

    if email_recipients_failure and "ftp" in delivery_data and ftp_status == "failure":
        return "Failure"

    if "ftp" in delivery_data and ftp_status == "failure":
        return "Failure"

    if "ftp" in delivery_data and ftp_status == "success":
        return "Success"

    if email_recipients_failure:
        return "Failure"

    return "Success"  # if email success and no ftp, it will return success.

def remove_duplicate_emails(email_list):
    """
    Removes duplicate email addresses from a list while preserving the original order.

    Args:
    email_list: A list of strings representing email addresses.

    Returns:
    A new list containing only unique email addresses from the input list,
    in their original order of appearance.
    """
    seen_emails = set()
    unique_emails = []
    for email in email_list:
        if email not in seen_emails:
            unique_emails.append(email)
            seen_emails.add(email)
    return unique_emails

def get_delivery_comment(delivery_data):
    """
    Generates a delivery comment based on email and ftp delivery statuses.

    Args:
        delivery_data (dict): The input dictionary containing email and ftp delivery information.

    Returns:
        str: The delivery comment.
    """

    email_successes = []
    email_failures = []
    ftp_success = False
    ftp_failure = False
    delivery_message = ""

    if "email" in delivery_data and "recipients" in delivery_data["email"]:
        recipients = delivery_data["email"]["recipients"]
        for recipient_dict in recipients:
            for email_address, status_dict in recipient_dict.items():
                if "delivery_status" in status_dict:
                    if status_dict["delivery_status"] == "success":
                        email_successes.append(email_address.strip("['']"))
                    else:
                        email_failures.append(email_address.strip("['']"))
                        if "SFERRORDEFAULTED" in status_dict["error"]:
                            delivery_message = status_dict["error"]
                            delivery_message = delivery_message.replace(
                                "SFERRORDEFAULTED", ""
                            )

    if "ftp" in delivery_data and "delivery_status" in delivery_data["ftp"]:
        if delivery_data["ftp"]["delivery_status"] == "success":
            ftp_success = True
        else:
            ftp_failure = True
    email_successes = remove_duplicate_emails(email_successes)
    email_failures = remove_duplicate_emails(email_failures)
    
    if email_successes and not email_failures:
        if len(email_successes) == 1:
            comment = f"File delivered to {email_successes[0]}"
        else:
            comment = f"File delivered to {', '.join(email_successes)}"
        if ftp_failure:
            if len(email_successes) == 1:
                comment = f"File delivered to {email_successes[0]} but could not be delivered to ftp location"
            else:
                comment = f"File delivered to {', '.join(email_successes)} but could not be delivered to ftp location"
        if ftp_success:
            if len(email_successes) == 1:
                comment = f"File delivered to {email_successes[0]} and delivered to configured FTP location"
            else:
                comment = f"File delivered to {', '.join(email_successes)} and delivered to configured FTP location"
        return comment

    if email_failures and not email_successes:
        if len(email_failures) == 1:
            comment = f"File could not be delivered to {email_failures[0]}"
        else:
            comment = f"File could not be delivered to {', '.join(email_failures)}"
        if ftp_success:
            if len(email_failures) == 1:
                comment = f"File delivered to ftp location but email was unsuccessful {email_failures[0]}"
            else:
                comment = f"File delivered to ftp location but email was unsuccessful {', '.join(email_failures)}"
        if ftp_failure:
            if len(email_failures) == 1:
                comment = f"File could neither be delivered to {email_failures[0]} nor via FTP"
            else:
                comment = f"File could neither be delivered to {', '.join(email_failures)} nor via FTP"
        if delivery_message != "":
            comment = delivery_message
        return comment

    if email_successes and email_failures:
        if len(email_successes) == 1:
            success_string = email_successes[0]
        else:
            success_string = ", ".join(email_successes)

        if len(email_failures) == 1:
            failure_string = email_failures[0]
        else:
            failure_string = ", ".join(email_failures)

        comment = f"File delivered to {success_string} but could not be delivered to {failure_string}"

        if ftp_failure:
            comment = f"File delivered to {success_string} but could not be delivered to {failure_string} and ftp location"
        return comment

    if ftp_success:
        return "File delivered to configured FTP location"
    if ftp_failure:
        return "File could not be delivered to configured FTP location"

    return "No delivery information available."


def generate_holdings_report_table(parsed_log_data):
    holdings_table = "<html><head></head><body><table border='1' cellspacing='0' cellpadding='3'><tr><th width='4%'></th><th width='28%'>Filename</th><th width='10%'>Interaction Preference</th><th width='13%'>Delivery Status</th><th width='45%'>Comment</th></tr>"
    count = 1
    email_success_count = 0
    for transaction_id, transaction_data in parsed_log_data.items():
        attachement_name = transaction_data["attachment_name"]
        delivery_preference = get_delivery_preference(
            transaction_data[attachement_name]["delivery_preference"]
        )
        delivery_status = get_delivery_status(
            transaction_data[attachement_name]["delivery_preference"]
        )
        delivery_comment = get_delivery_comment(
            transaction_data[attachement_name]["delivery_preference"]
        )
        holdings_table += "<tr>"
        holdings_table += f"<td>{count}</td>"
        holdings_table += f"<td>{attachement_name}</td>"
        holdings_table += f"<td>{delivery_preference}</td>"
        if delivery_status == "Failure":
            holdings_table += f"<td style='color:Tomato;'>{delivery_status}</td>"
        else:
            email_success_count += 1
            holdings_table += f"<td>{delivery_status}</td>"
        holdings_table += f"<td>{delivery_comment}</td>"
        holdings_table += "</tr>"
        count += 1
    holdings_table += "</table></body></html>"
    return email_success_count, holdings_table


# Email - REPORT|email|Holdings|failure|{sender}|{subject}|{recipient}|{str(e)}|{attachment_key}
#         REPORT|email|Holdings|failure|{sender}|{subject}|{recipient- one at a time}|E-mail Bounce Back Error|{attachment_key}
# FTP   - REPORT|ftp|Holdings|Success|agencyName|contactEmail|Holdings_2024-04-12 0702 000343_SEVNET.h.zip|transaction_id
#         REPORT|ftp|Holdings|success|Agency Name|Agency Email|Holdings_2025-03-20 2247 000484_SEVNET.h.zip||32423423-f2r2f32-323r2f|1742491009
def extract_logs_and_generate_reports(log_group_name, log_stream_name):
    lmk_geneated_files = count_s3_files_past_24_hours(
        os.environ["HOLDINGS_FILE_IN_BUCKET"]
    )
    logger.info(lmk_geneated_files)
    # Initialize AWS CloudWatch Logs client
    client = boto3.client("logs")
    logs = []
    next_token = None
    # Get current time
    current_time = datetime.now(timezone.utc)
    # Calculate start and end time for the past 24 hour
    end_time = current_time
    start_time = end_time - timedelta(hours=24)
    logger.info(f" End Time: {end_time}")
    logger.info(f"Start Time: {start_time}")
    # Get logs for the past 24 hour

    while True:
        kwargs = {
            "logGroupName": log_group_name,
            "logStreamName": log_stream_name,
            "startTime": int(start_time.timestamp()) * 1000,
            "endTime": int(end_time.timestamp()) * 1000,
            "startFromHead": True,
        }
        if next_token:
            kwargs["nextToken"] = next_token

        response = client.get_log_events(**kwargs)

        logs.extend(response["events"])

        next_token = response.get("nextForwardToken")

        if not next_token or next_token == kwargs.get("nextToken"):
            break

    response["events"] = logs

    logger.info(f"Total log events for a day: {response}")
    # Initialize HTML tables for FTP and email
    common_table = "<html><head></head><body><table border='1'><tr><th width='4%'></th><th width='28%'>Filename</th><th width='10%'>Interaction Preference</th><th width='13%'>Delivery Status</th><th width='45%'>Comment</th></tr>"
    ftp_transfer_count = 0
    ftp_failure_count = 0
    email_success_count = 0
    email_failure_count = 0
    final_dict = {}
    attachment_name = ""
    attachment_preference = ["Email"]
    comment = ""
    # Parse log events
    for event in response["events"]:
        comment = ""
        log_message = event["message"]
        log_parts = log_message.split("|")
        if len(log_parts) >= 3 and log_parts[0] == "REPORT":
            if log_parts[1] == "ftp" and log_parts[3].lower() == "success":
                ftp_transfer_count += 1
            if log_parts[1] == "ftp" and log_parts[3].lower() == "failure":
                ftp_failure_count += 1
                comment = log_parts[7]

            if log_parts[1] == "email" and log_parts[3].lower() == "success":
                email_success_count += 1
            if log_parts[1] == "email" and log_parts[3].lower() == "failure":
                email_failure_count += 1
                comment = log_parts[7]

            mode_of_delivery = log_parts[1]

            if log_parts[1] == "ftp":
                attachment_name = log_parts[6]
                if "FTP" not in attachment_preference:
                    attachment_preference.append("FTP")

            if log_parts[1] == "email":
                attachment_name = log_parts[8]

                # email_table += "<tr>"
                # email_table += f"<td>{log_parts[4]}</td>"
                # email_table += f"<td>{log_parts[5]}</td>"
                # email_table += f"<td>{log_parts[6]}</td>"
                # email_table += f"<td>{log_parts[7]}</td>"
                # email_table += f"<td>{log_parts[8]}</td>"
                # email_table += "</tr>"

            if attachment_name in final_dict:
                # Concatenate preference and other parameters
                existing_data = final_dict[attachment_name]
                existing_data["preference"][mode_of_delivery] = True
                existing_data["delivery_status"][mode_of_delivery] = log_parts[3]
                existing_data["delivery_comment"][mode_of_delivery] = comment

            else:
                final_dict[attachment_name] = {
                    "preference": {
                        mode_of_delivery: True,
                    },
                    "delivery_status": {
                        mode_of_delivery: log_parts[3],
                    },
                    "delivery_comment": {
                        mode_of_delivery: comment,
                    },
                }
    logger.info(final_dict)
    count = 1
    for filename, file_data in final_dict.items():
        delivery_status = ""
        delivery_comments = ""
        delivery_preference_updates = ""
        for delivery_method, status in file_data["delivery_status"].items():
            if delivery_method == "ftp":
                delivery_method = "FTP"
            if delivery_method == "email":
                delivery_method = "Email"
            delivery_status += f"<b>{delivery_method}</b>: {status}<br><br>"
        for delivery_method, comment in file_data["delivery_comment"].items():
            if delivery_method == "ftp":
                delivery_method = "FTP"
            if delivery_method == "email":
                delivery_method = "Email"
            stripped_comment = comment.strip()
            if stripped_comment != "" and stripped_comment is not None:
                delivery_comments += f"<b>{delivery_method}</b>: {comment}<br><br>"
        for delivery_preference, mode in file_data["preference"].items():
            if delivery_preference == "ftp":
                delivery_preference = "FTP"
            if delivery_preference == "email":
                delivery_preference = "Email"
            delivery_preference_updates += "," + f"<b>{delivery_preference}</b>"
        common_table += "<tr>"
        common_table += f"<td>{count}</td>"
        common_table += f"<td>{filename}</td>"
        common_table += f"<td>{delivery_preference_updates}</td>"
        common_table += f"<td>{delivery_status}</td>"
        common_table += f"<td>{delivery_comments}</td>"
        common_table += "</tr>"
        count += 1
    # Close HTML tables
    common_table += "</table></body></html>"

    logger.info(common_table)
    # Send email with HTML tables
    # send_email(
    #     lmk_geneated_files,
    #     common_table,
    #     ftp_transfer_count,
    #     ftp_failure_count,
    #     email_success_count,
    #     email_failure_count,
    # )


def send_email(
    lmk_geneated_files_count,
    common_content,
    email_success_count,
):

    try:
        ssm_client = boto3.client("ssm")
        holdings_report_email_str = (
            ssm_client.get_parameter(Name=os.environ["HOLDINGS_REPORT_EMAIL"])
            .get("Parameter")
            .get("Value")
        )
        recipients = holdings_report_email_str.split(",")
        receiver_email = [email.strip() for email in recipients]
    except ClientError as e:
        logger.info(f"Send to: {e}")

    try:
        ssm_client = boto3.client("ssm")
        email_domain = (
            ssm_client.get_parameter(Name=os.environ["EMAIL_DOMAIN"])
            .get("Parameter")
            .get("Value")
        )
    except ClientError as e:
        logger.info(f"Send from: {e}")

    # Email configuration
    sender_email = f"delivery_report@{email_domain}"
    subject = "Holdings Delivery Report"
    current_date = datetime.now(timezone.utc)
    formatted_date = current_date.strftime("%d/%m/%Y")
    # AEST = pytz.timezone('Australia/Sydney')
    # datetime_aest = datetime.now(AEST)
    # datetime_aest.strftime('%d/%m/%Y')
    body_html = f"<b>TX Date: {formatted_date}</b><br><br>"
    body_html += f"<p><b>Number of Files generated by Landmark:</b> {lmk_geneated_files_count}</p><br>"
    # if email_failure_count > 0:
    #     body_html += f"<br>Total unsuccessful emails: {email_failure_count}</p><b>Unsuccessful Email Details</b>{email_content}"
    body_html += f"<p><b>Number of Files delivered:</b> {email_success_count}</p><br>"
    body_html += f"<br><br>{common_content}"
    # if ftp_failure_count > 0:
    #     body_html += f"<br>Total unsuccessful FTP:{ftp_failure_count}</p><b>Unsuccessful FTP Details</b><br><br>{ftp_content}"
    # Initialize AWS SES client
    client = boto3.client("ses", region_name=AWS_REGION)
    # Send email
    for recipient in receiver_email:
        try:
            response = client.send_email(
                Source=sender_email,
                Destination={
                    "ToAddresses": [recipient]
                },  # Send to one address at a time
                Message={
                    "Subject": {"Data": subject, "Charset": "UTF-8"},
                    "Body": {"Html": {"Data": body_html, "Charset": "UTF-8"}},
                },
            )
            logger.info(f"Email sent successfully to: {recipient}: {response}")
        except client.exceptions.MessageRejected as e:
            logger.info(f"Error sending email to {recipient}: {e}")
        except Exception as e:
            logger.info(f"General Error sending email to {recipient}: {e}")


def lambda_handler(event, context):

    # Set up the CloudWatch log group name
    log_group_name = os.environ["LOG_GROUP_NAME"]
    # Set up the log stream name
    log_stream_name = os.environ["LOG_STREAM_NAME"]
    lmk_geneated_files = count_s3_files_past_24_hours(
        os.environ["HOLDINGS_FILE_IN_BUCKET"]
    )
    log_data = extract_logs(log_group_name, log_stream_name)
    parsed_log_data = parse_logdata_to_json(log_data)
    email_success_count, holdings_table_data = generate_holdings_report_table(
        parsed_log_data
    )
    send_email(lmk_geneated_files, holdings_table_data, email_success_count)
    # extract_logs_and_generate_reports(log_group_name, log_stream_name)
