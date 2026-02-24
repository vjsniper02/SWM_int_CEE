import boto3
from functions.email_client.app import send_email_with_s3_attachments


def test_send_email_with_s3_attachments(mocker):
    # Mock SES and S3 clients
    mocker.patch("boto3.client")
    mocker.patch("boto3.client().send_raw_email")
    mocker.patch("boto3.client().get_object")

    sender = "sender@example.com"
    recipient = "recipient@example.com"
    subject = "Test Email with S3 Attachments"
    body = "This is a test email with S3 attachments."
    bucket_name = "your-s3-bucket"
    attachment_keys = ["path/to/attachment/file1.txt", "path/to/attachment/file2.pdf"]

    # Call the function
    message_id = send_email_with_s3_attachments(
        sender, recipient, subject, body, bucket_name, attachment_keys
    )

    # Assertions
    assert message_id is not None
    boto3.client.assert_called_with("ses", region_name="your-region")
    boto3.client().send_raw_email.assert_called_once()
    boto3.client().get_object.assert_any_call(
        Bucket=bucket_name, Key="path/to/attachment/file1.txt"
    )
    boto3.client().get_object.assert_any_call(
        Bucket=bucket_name, Key="path/to/attachment/file2.pdf"
    )
