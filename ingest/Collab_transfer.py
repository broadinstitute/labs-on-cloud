__author__ = [ "Asma Bankapur", "Joshua Gould" ]
__copyright__ = "Copyright 2017"
__credits__ = [ "Joshua Gould", "Ruchi Munshi" ]
__license__ = "MIT"
__maintainer__ = "Asma Bankapur"
__email__ = "bankapur@broadinstitute.org"
__status__ = "Development"

from google.cloud import storage
import os
import os.path
from google.auth.transport.requests import AuthorizedSession
import argparse
import json
import google.auth
from oauth2client.service_account import ServiceAccountCredentials
from httplib2 import Http
import subprocess
import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
import ConfigParser
import logging
import errno
from datetime import date

LOG_FORMAT="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
MAIN_LOGGER = "job"

def get_arguments():
    '''
    Arguments taken in by the program are
    service account, source directory 
    * return: parsed args
    '''
    parser=argparse.ArgumentParser()
    parser.add_argument("--service_account_path", dest="service_acc_path",
                        help="service account to write to buckets")
    parser.add_argument("--boto", help="boto cofig file")
    parser.add_argument("--source", dest="source_dir",
                        help="directory to upload")
    parser.add_argument("--email_addresses", help=" ".join(["User email ids you want to",
                                                           "give access to Eg. ",
                                                           "abc@gmail.com,xyz@gmail.com"]))
    parser.add_argument("--config_file", help=" ".join(["config file containing admin settings",
                                                         "and smtp address"]))
    args = parser.parse_args()
    return(args)


def get_basename(dirpath):
    '''
    basename of directory path
    dirpath: path to directory
    *return: basename
    '''
    basename=os.path.basename(os.path.normpath(dirpath))
    return(basename)

def workspace_setup(service_acc_path,source_dir):
    '''
    set up workspace with billing project
    service_acc_path: service acc key
    source_dir: basename 
    *return: response
    '''
    scopes=["https://www.googleapis.com/auth/userinfo.profile",
            "https://www.googleapis.com/auth/userinfo.email"]

    credentials=ServiceAccountCredentials.from_json_keyfile_name(
                                                service_acc_path,
                                                scopes=scopes)
    authed_session=AuthorizedSession(credentials)

    body={"namespace":"regev-collab",
          "name":source_dir,
          "attributes":{},
          "authorizationDomain":[]}
    session=AuthorizedSession(google.auth.default(scopes)[0])
    response=session.post("https://api.firecloud.org/api/workspaces",
                           headers={"Content-type":"application/json",
                                    "Accept": "application/json"},
                           json=body)    
    return(json.loads(response.text),session)

def bucket_setup(response):
    '''
    set up bucket for storage
    response: credentials
    del_days: no. of days to del storage
    *return: bucket name
    '''
    bucket_name=response["bucketName"]
    return(bucket_name)

def upload(source_dir,bucket_name):
    '''
    upload directory with gsutil
    source_dir: directory to upload
    bucket_name: bucket name
    '''
    bucket_name_gs="gs://"+bucket_name
    cmd=" ".join(["gsutil","-m","cp",
                 "-r",source_dir,bucket_name_gs])
    return_code=subprocess.call(cmd,shell=True)
    return(return_code)

def set_service_acc_name(session,config):
    '''
    sets first and last name of service account being
    used
    '''

    response=session.post("https://api.firecloud.org/register/profile",
                          json=config)
    return(response)

def set_permissions(namespace,workspace,acl_updates,session): 
    '''
    set permissions to the given email
    addresses so they can read
    namespace (str): project to which workspace belongs
    workspace (str): Workspace name    
    acl_updates (list(dict)): Acl updates as dicts with two keys:
            "email" - Firecloud user email
            "accessLevel" - one of "OWNER", "READER", "WRITER", "NO ACCESS"
            Example: {"email":"user1@mail.com", "accessLevel":"WRITER"}
    '''
    response=session.patch((os.sep).join(["https://api.firecloud.org/api/workspaces",
                                          namespace,workspace,"acl?inviteUsersNotFound=true"]),
                            headers={"Content-type":"application/json"},
                            data=json.dumps(acl_updates))
    return(response)

def get_flowcellid(base_source_dir):
    '''
    extract flowcell from source dir
    return: flowcellid
    '''
    return(base_source_dir.split("_")[-1])

def get_timestamp(source_dir):
    '''
    get timestamp of source_dir
    return: YYYYMMDD
    '''
    return(str(date.fromtimestamp(os.path.getmtime(source_dir)))) 

def set_tags(session,namespace,name,tags):
    '''
    tags to add to the workspace 
    '''
    response=session.put((os.sep).join(["https://api.firecloud.org/api/workspaces",
                                          namespace,name,"tags"]),
                           headers={"Accept":"application/json",
                                    "Content-type":"application/json"},
                           json=tags) 
    return(response)


def email_user(sender_adrs,recepient_adrs,
               smtp_server,
               flowcellid,firstname,
               lastname):
    '''
    Send email to RECEPIENT from SENDER 
    collect body by cat BODY + list of archiving dirs
    *sender_adrs: From email
    *recepient_adrs: To email
    *admin_adrs: Admin email
    *smtp_server: SMTP server address
    *subject: email subject
    *msg_body: email body
    *flowcellid: collaborator's flowcellid
    *return: Boolean
    ''' 
    msg=MIMEMultipart()
    msg["Subject"]="Regev/KCO Data ready -"+flowcellid
    if flowcellid:
        body="\n".join(["Dear User,\n",
                    "Your sequence run is complete and is available here:",
                    "https://portal.firecloud.org/#workspaces/regev-collab/"+flowcellid,
                    "\n",
                    "FireCloud is a data repository and computational platform maintained", 
                    "and developed by the Broad Institute.",
                    "Please email "+sender_adrs+" with any questions you may have.","\n",
                    "Please note:","\n", 
                    "This data will be automatically deleted from the cloud in 6 weeks (or what ever time we decided).",  
                    "Please make sure you download the data to your institutions secured servers.",
                    "In the case the data is lost, it can still be retrieved from Broad archive.",
                    "\n",
                    "Thank you,","\n",
                    firstname+" "+lastname])
    msg.attach(MIMEText(body, "plain"))
    if not smtp_server:
        job_log.error("Cannot find an outbound smtp server address")
        return (False)
    server=smtplib.SMTP(smtp_server)
    emailbody=msg.as_string()
    server.sendmail(sender_adrs,recepient_adrs,emailbody)
    server.quit()  

def delete_workspace(namespace,name,session):
    response=session.delete((os.sep).join(["https://api.firecloud.org/api/workspaces",
                                           namespace,name]),
                            headers={"Content-type":"application/json"})
    return(response)         
                             

def make_job_logger(log_name,log_file,log_format,log_level=logging.INFO):
    '''
    Logging details output to log file
    *log_file: a file to log error and info,
    *log_format: log format of the log_file,
    *log_level: logging level
 
    *return: job_log
    '''
    hdlr_job = logging.FileHandler(filename=log_file,mode="w")
    hdlr_job.setFormatter(logging.Formatter(log_format))
    job_log=logging.getLogger(log_name)
    job_log.addHandler(hdlr_job)
    job_log.setLevel(log_level)
    return (job_log)

if __name__ == "__main__":

    #parse arguments
    parsed_args=get_arguments()
    #check if directory does not exist
    if not os.path.isdir(parsed_args.source_dir):
        print(parsed_args.source_dir+" does not exist")
        exit(1)
    if not os.listdir(parsed_args.source_dir):
        print(parsed_args.source_dir+" is empty.")
        exit(1)
    #get basename of directory
    source_dir_base=get_basename(parsed_args.source_dir)
    #parse multiple email addresses into a list
    if parsed_args.email_addresses:
        addresses=((parsed_args.email_addresses).strip()).split(",")
    config=ConfigParser.ConfigParser()
    if os.path.isfile(parsed_args.config_file):
        config.read(parsed_args.config_file)
    else:
        print("Config file does not exist.")
        exit(1)
    #parse config file
    config_dict={}
    config_dict["firstName"]=config.get("ADMIN_SETTINGS",
                                        "firstName")
    config_dict["lastName"]=config.get("ADMIN_SETTINGS",
                                       "lastName")
    config_dict["title"]=config.get("ADMIN_SETTINGS",
                                    "title")
    config_dict["contactEmail"]=config.get("ADMIN_SETTINGS",
                                           "contactEmail")
    config_dict["institute"]=config.get("ADMIN_SETTINGS",
                                        "institute")
    config_dict["institutionalProgram"]=config.get("ADMIN_SETTINGS",
                                        "institutionalProgram")
    config_dict["programLocationCity"]=config.get("ADMIN_SETTINGS",
                                             "programLocationCity")
    config_dict["programLocationState"]=config.get("ADMIN_SETTINGS",
                                             "programLocationState")
    config_dict["programLocationCountry"]=config.get("ADMIN_SETTINGS",
                                            "programLocationCountry")
    config_dict["pi"]=config.get("ADMIN_SETTINGS","pi")
    config_dict["nonProfitStatus"]=config.get("ADMIN_SETTINGS",
                                             "nonProfitStatus")
    config_dict["smtp_server"]=config.get("ADMIN_SETTINGS",
                                          "smtp_server")
    config_dict["job_log_location"]=config.get("ADMIN_SETTINGS",
                                          "job_log_location")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=parsed_args.service_acc_path
    #os.environ["BOTO_CONFIG"]=parsed_args.boto
  
    #Set up job log file
    job_log_file=os.path.join(config_dict["job_log_location"],
                              source_dir_base+".job_log")
    job_log=make_job_logger(MAIN_LOGGER,job_log_file,
                            LOG_FORMAT,log_level=logging.INFO)

    #set up workspace
    response,session=workspace_setup(parsed_args.service_acc_path,
                                     source_dir_base)
    if response.has_key("statusCode"):
        if response["statusCode"]!=201:
            job_log.error(" ".join(["Workspace not set", 
                                   "up in FC - error code",
                                   str(response["statusCode"])]))
            job_log.error(" ".join(["Workspace not set",
                                   "up in FC - message",
                                   response["message"]]))
            exit(1)
    job_log.info("Workspace set up")

    #set tags to workspace
    flowcell_id=get_flowcellid(source_dir_base)
    timestamp=get_timestamp(parsed_args.source_dir)
    tags=[flowcell_id,timestamp]
    response_tags=set_tags(session,response["namespace"],
                           response["name"],tags)
    if not response_tags.ok:
        job_log.error(" ".join(["Tags not set",
                                "error code",
                                str(response_tags.status_code)]))
        job_log.info("Deleting workspace...")
        delete_response=delete_workspace(response["namespace"],
                                         response["name"],
                                         session)
        if not delete_response.ok:
            job_log.info(" ".join(["Workspace could not",
                                   "be deleted. Use FC portal",
                                   "to delete created workspace", 
                                   "before restarting."]))
            job_log.error(" ".join(["Error code received", 
                                   "while trying to delete workspace:",
                                   str(delete_response.status_code)]))
            job_log.info("Deleted workspace.") 
        exit(1)
    job_log.info("Tags added to workspace.")

    #set up bucket
    bucket_name=bucket_setup(response)
    response_name_setting=set_service_acc_name(session,config_dict)
    if not response_name_setting.ok:
        job_log.error(" ".join(["Service account details", 
                               "not set up - error code",
                               str(response_name_setting.status_code)]))
        job_log.info("Deleting workspace...")
        delete_response=delete_workspace(response["namespace"],
                                         response["name"],
                                         session)
        if not delete_response.ok:
            job_log.info(" ".join(["Workspace could not",
                                   "be deleted. Use FC portal",
                                   "to delete created workspace", 
                                   "before restarting."]))
            job_log.error(" ".join(["Error code received", 
                                   "while trying to delete workspace:",
                                   str(delete_response.status_code)]))
            job_log.info("Deleted workspace.") 
        exit(1)
    job_log.info("Bucket set up")

    #set permissions on workspace
    acl_updates=[]
    for email_adr in addresses:
        acl_updates.append({"email":email_adr,
                            "accessLevel":"READER",
                            "canShare": False,
                            "canCompute": False})
    response_permissions=set_permissions(response["namespace"],
                                         response["name"],
                                         acl_updates,
                                         session)
    if not response_permissions.ok:
        job_log.error(" ".join(["Permissions not set correctly.",
                               "- error code",
                               str(response_permissions.status_code)]))
        delete_response=delete_workspace(response["namespace"],
                                         response["name"],session)
        job_log.info("Deleting workspace...")
        if not delete_response.ok:
            job_log.info(" ".join(["Workspace could not be deleted.", 
                                   "Use FC portal to delete created",
                                   "workspace before restarting."]))
            job_log.error(" ".join(["Error code received while", 
                                   "trying to delete workspace:",
                                   str(delete_response.status_code)]))
            job_log.info("Deleted workspace.") 
        exit(1)
    job_log.info("Users have been given reader access.")

    #upload data to bucket 
    response_upload=upload(parsed_args.source_dir,bucket_name)
    if response_upload!=0:
        job_log.error(" ".join(["Upload failed."]))
        delete_response=delete_workspace(response["namespace"],
                                         response["name"],session)
        job_log.info("Deleting workspace...")
        if not delete_response.ok:
            job_log.info(" ".join(["Workspace could not be deleted.", 
                                   "Use FC portal to delete created",
                                   "workspace before restarting."]))
            job_log.error(" ".join(["Error code received while", 
                                   "trying to delete workspace:",
                                   str(delete_response.status_code)]))
            job_log.info("Deleted workspace.") 
        exit(1)
    job_log.info("Data has been uploaded.")

    #email user with FC path
    for recepient_adrs in addresses:
        email_user(config_dict["contactEmail"],recepient_adrs,
                            config_dict["smtp_server"],
                            source_dir_base,config_dict["firstName"],
                            config_dict["lastName"])
