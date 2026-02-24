import json
import boto3
import logging
import json
from simple_salesforce import Salesforce
import os
import boto3 
import requests
from botocore.exceptions import ClientError
import base64

logger = logging.getLogger("b2_salesforce_adaptor_function")
logger.setLevel(logging.INFO)

#Get Landmark SFTP details from Secret Manager
def get_sf_key():
    logger.info(f"Start get_landmark_secret()")
    print(os.environ['SF_API_PRIVATE_KEY'])
    secret_name = os.environ['SF_API_PRIVATE_KEY']
    logger.info(secret_name)    

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager')

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)       
    except ClientError as e:       
        raise e

    # Decrypts secret using the associated KMS key.
    secret = json.loads(get_secret_value_response['SecretString'])        
    
    logger.info(f"End get_landmark_secret()")
    return secret["privatekey"]


#def create_account():
#    session = requests.Session()
#    auth_id = "Bearer " + sf.session_id
#    req_headers = {"Authorization": auth_id}
#    resp = session.post(
#        f"https://{sf.sf_instance}/services/apexrest/UsageSummary/fetchUsages/*",
#        headers=req_headers,
#        json={"usageDate": datetime.today().strftime("%Y-%m-%d")},
#    )
#    resp.raise_for_status()
#    if isinstance(resp.json(), str):
#        return [resp.json()]
#    else:
#        return resp.json()#

def create_sf_connection():
    username = os.environ['SF_API_USER_NAME']
    consumerkey = os.environ['SF_API_CONSUMER_KEY']    
    privatekey = base64.b64decode(get_sf_key())
      
    sf = Salesforce(username=username, consumer_key=consumerkey, privatekey=privatekey,domain="test")
    #sessionId = sf.session_id()
    print(sf)
    return sf

def get_file_name():
    s3Client = boto3.client("s3")
    bucket = s3Client.Bucket(os.environ['SFTP_S3_BUCKET'])
    file_list=[]
    for my_bucket_object in bucket.objects.all():
        print(my_bucket_object.key)
        file = {"path":"s3://seil-dev-sch-file-in/sftp","name":my_bucket_object.key}
        file_list.append(file)
    return file_list

#Construct message structure for CEE interface
def construct_cee_message(crmList,file_list):
    callerInput = {
        "crmIDs":crmList,
        "type":"Program",
        "files":[file_list]
    }
    return callerInput

def get_crm_id(sf):
    session = requests.Session()
    auth_id = "Bearer " + sf.session_id
    req_headers = {"Authorization": auth_id}

    templateQuery = "SELECT+ID+from+EmailTemplate"
    commPreference = "SELECT+SWM_Invoicing_Preference__c,SWM_LandMark_ID__c+from+Account+where+SWM_LandMark_ID__c+='LandmarkID' "

   

    sf_response = session.get(
            #f"https://{sf.sf_instance}/services/data/v57.0/composite/",
            f"https://{sf.sf_instance}/services/data/v58.0/query/?q={templateQuery}",
            f"https://{sf.sf_instance}/services/data/v58.0/query/?q={commPreference}",
            headers=req_headers,
            
        )
    print(sf_response)
    sf_response.raise_for_status()
    sf_response_json =sf_response.json()

    records = sf_response_json["records"]
    print(records)

    crmList = [item["Id"] for item in sf_response_json["records"]]
    print(crmList)

    #if isinstance(sf_response.json(), str):
    #    print(sf_response.json())
    #    return [sf_response.json()]

    #else:
    #    print(sf_response.json())
    #    return sf_response.json()

    return crmList
    
    

def lambda_handler(event, context):  
   
    #url=event["url"]
    #payload = event["content"]

    sf = create_sf_connection()
    crmList = get_crm_id(sf)
    file_list = get_file_name()
    ceeInputMessage = construct_cee_message(crmList,file_list)
    print(ceeInputMessage)

    #create_account()
    

    
    return ceeInputMessage

def get_preference(salesforce_object):
  """Gets the preference for email or ftp from Salesforce.

  Args:
    salesforce_object: The Salesforce object to get the preference from.

  Returns:
    A string representing the preference, either "email" or "ftp".
  """

  preference_field = "Preference__c"
  if preference_field in salesforce_object:
    preference = salesforce_object[preference_field]
  else:
    preference = "email"

  return preference

def get_email_template(salesforce_object):
  """Gets the template for email from Salesforce.

  Args:
    salesforce_object: The Salesforce object to get the preference from.

  Returns:
    Email template.
  """
  email_template = "template"
  return email_template


def lambda_handler(event, context):
  """The Lambda function handler.

  Args:
    event: The event object passed to the Lambda function.
    context: The context object passed to the Lambda function.

  Returns:
    A response object.
  """

  # Get the Salesforce object from the event.
  salesforce_object = json.loads(event["salesforce_object"])

  # Get the preference from the Salesforce object.
  preference = get_preference(salesforce_object)
  template = get_email_template(salesforce_object)

  # Create a response object.
  response = {
    "preference": preference,
    "template": template
  }

  # Return the response object.
  return response