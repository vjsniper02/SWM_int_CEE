import logging
import json
from simple_salesforce import Salesforce
import os
import boto3
import urllib.parse
import requests
from botocore.exceptions import ClientError
import base64

logger = logging.getLogger("cee_salesforce_adaptor_function")
logger.setLevel(logging.INFO)


# Get Landmark SFTP details from Secret Manager
def get_sf_cred():
    logger.info(f"Start get_landmark_secret()")
    secret_name = os.environ["SF_API_PRIVATE_KEY"]
    logger.info(secret_name)
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager")
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise e
    # Decrypts secret using the associated KMS key.
    secret = json.loads(get_secret_value_response["SecretString"])
    secret_dict = {
        "username": secret["username"],
        "consumer_key": secret["consumer_key"],
        "privatekey": secret["privatekey"],
    }
    logger.info(f"End get_landmark_secret()")
    return secret_dict


def create_sf_connection():
    sf_cred = get_sf_cred()
    username = sf_cred["username"]
    consumerkey = sf_cred["consumer_key"]
    privatekey = base64.b64decode(sf_cred["privatekey"])
    secret_name = os.environ["SF_API_PRIVATE_KEY"]
    logger.info(secret_name)
    if secret_name.startswith("prod"):
        sf = Salesforce(
            username=username, consumer_key=consumerkey, privatekey=privatekey
        )
    else:
        sf = Salesforce(
            username=username,
            consumer_key=consumerkey,
            privatekey=privatekey,
            domain="test",
        )
    logger.info(sf)
    return sf


def query_sf_data(sf, query):
    session = requests.Session()
    auth_id = "Bearer " + sf.session_id
    req_headers = {"Authorization": auth_id}
    sf_response = session.get(
        f"https://{sf.sf_instance}/services/data/v58.0/query/?q={query}",
        headers=req_headers,
    )
    logger.info(sf_response)
    sf_response.raise_for_status()

    if isinstance(sf_response.json(), str):
        logger.info(sf_response.json())
        return [sf_response.json()]

    else:
        logger.info(sf_response.json())
        return sf_response.json()


def post_sf_data(sf, payload, entity, brqid):
    session = requests.Session()
    auth_id = "Bearer " + sf.session_id
    req_headers = {"Authorization": auth_id, "Content-Type": "application/json"}
    resp = session.patch(
        f"https://{sf.sf_instance}/services/data/v58.0/sobjects/" + entity + "/SWM_BRQ_Request_ID__c/" + brqid,
        headers=req_headers,
        json=payload,
    )
    resp = sf.Opportunity.create(payload)
    logger.info(f"create opportunity result: {resp}")
    return resp

def create_case(sf, event_data):
    try:
        data = event_data
        resp = sf.Case.create(data)
        return resp
    except Exception as e:
        logger.info(f"Error getting salesforce Account data: {e}")
        return False

def get_file_data(s3_obj):
    try:
        key = s3_obj["key"]
        bucket = s3_obj["bucket_name"]
        s3 = boto3.resource("s3")
        obj = s3.Object(bucket, key)
        content = obj.get()["Body"].read()
        return content
    except Exception as e:
        logger.exception(f"Error extracting data from PRP file: {e}")
        return None


def upload_file_to_sf(sf, opportunity_id, s3_obj):
    s3_obj = json.loads(s3_obj)
    key_split = s3_obj["key"].split("/")
    key = key_split[1]
    data = get_file_data(s3_obj)
    session = requests.Session()
    auth_id = "Bearer " + sf.session_id
    req_headers = {
        "Authorization": auth_id,
        "X-PrettyPrint": "1",
        "filename": key,
        "Content-Type": "application/binary",
        "NEILON__Category__c": "EBookings Request",
    }
    try:
        sf_response = session.post(
            f"https://{sf.sf_instance}/services/apexrest/NEILON/S3Link/v1/uploadfile/{opportunity_id}",
            headers=req_headers,
            data=data,
        )
        logger.info(sf_response.json())
        sf_response.raise_for_status()
        return sf_response.json()  # Upload successful
    except requests.exceptions.RequestException as e:
        logger.info(f"Error uploading file to API: {e}")
        return False  # Upload failed


def lambda_handler(event, context):
    logger.info(f"Event data: {event}")
    sf = create_sf_connection()
    if event["invocationType"] == "CASE":
        event_data = event["data"]
        responseData = create_case(sf, event_data)
    if event["invocationType"] == "QUERY":
        query = event["query"]
        responseData = query_sf_data(sf, query)
    if event["invocationType"] == "SOBJECTS":
        payload = event["payload"]
        entity = event["entity"]
        brqid = event["brqid"]
        responseData = post_sf_data(sf, payload, entity, brqid)
    if event["invocationType"] == "S3LINKAPI":
        record_id = event["record_id"]
        responseData = upload_file_to_sf(sf, record_id, event["s3_obj"])
    logger.info(f"SF response: {responseData}")
    return responseData
