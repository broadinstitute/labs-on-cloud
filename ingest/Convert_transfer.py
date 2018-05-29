__author__ = [ "Asma Bankapur", "Joshua Gould" ]
__copyright__ = "Copyright 2017"
__credits__ = [ "Joshua Gould", "Ruchi Munshi" ]
__license__ = "MIT"
__maintainer__ = "Asma Bankapur"
__email__ = "bankapur@broadinstitute.org"
__status__ = "Development"


"""
Program will convert bcl to fastqs
Uses drmma lib to run job on grid
Once fastqs successfully generated
     1)make sample sheet 
     2)gsutil fastqs+samplesheet
     3)write metadata to file 
"""

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
import drmaa
import Collab_transfer
import xml.etree.ElementTree as ET
from six.moves.urllib.parse import urlencode
import sys
import sqlite3

LOG_FORMAT="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
MAIN_LOGGER = "job"
CONVERT_SCRIPT=os.path.sep.join([os.path.dirname(os.path.abspath(__file__)),"bcl2fastq.sh"])
CREATE_SAMPLESHEET_SCRIPT=os.path.sep.join([os.path.dirname(os.path.abspath(__file__)),"create_sample_sheet.py"])
RUNINFO="RunInfo.xml"
MKFASTQ_SCRIPT=os.path.sep.join([os.path.dirname(os.path.abspath(__file__)),"mkfastq.sh"])

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
    parser.add_argument("--samplesheet",dest="nextseq_samplesheet",
                        help="nextseq samplesheet for demultiplexing")
    parser.add_argument("--email_addresses",help=" ".join(["User email ids you want to",
                                                           "give access to Eg. ",
                                                           "abc@gmail.com,xyz@gmail.com"]))
    parser.add_argument("--config_json_file",help=" ".join(["config file containing admin settings,",
                                                        "pipeline settings"
                                                        "and smtp address"]))
    parser.add_argument("--pipeline",choices=["dropseq", "cellranger"],
                                              help="Choose pipeline you want to run")
    parser.add_argument("--genome",dest="org",choices=["hg19","mm10","hg19_mm10","GRCh38"],
                                             help=" ".join(["Choose genome, Note: GRCh38 only",
                                                            "available for Cellranger"]))
    parser.add_argument("--skip_demult",dest="skip_demult",action="store_true",help=" ".join(["Skip demultiplexing",
                                                                          "only run pipeline."]))
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

def make_directory(dirpath):
    '''
    make directory
    dirpath: path to directory
    ''' 
    if not os.path.exists(dirpath):
        os.makedirs(dirpath)
    return(dirpath)

def convertbcls(seq_dir,fastq_dir,nextseq_samplesheet,pipeline):
    '''
    convert bcl to fastq
    seq_dir: bcl dir
    fastq_dir: output dir 
    '''
    if pipeline in ["rnaseq","dropseq"]:
        convert_status=subprocess.call("qsub -v SEQ_DIR="+seq_dir+",FASTQ_DIR="+fastq_dir+",SS="+nextseq_samplesheet+" -sync y "+ CONVERT_SCRIPT, shell=True)
    else:
        convert_status=subprocess.call("qsub -v SEQ_DIR="+seq_dir+",FASTQ_DIR="+fastq_dir+",SS="+nextseq_samplesheet+" -sync y "+ MKFASTQ_SCRIPT, shell=True)
    return(convert_status)

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

def workspace_setup(service_acc_path,source_dir):
    ''' 
    set up workspace with billing project
    service_acc_path: service acc key
    source_dir: basename 
    *return: response
    '''
    scopes=["https://www.googleapis.com/auth/userinfo.profile",
            "https://www.googleapis.com/auth/userinfo.email"]

    #credentials=ServiceAccountCredentials.from_json_keyfile_name(
    #                                            service_acc_path,
    #                                            scopes=scopes)
    credentials,project=google.auth.default()
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
    print "%%%%%%ws cred" 
    print response.text 
    return(json.loads(response.text),session)

def seqrundate(run_info): 
    if os.path.exists(run_info):
        seqdirele=ET.parse(run_info).getroot() 
        return(seqdirele[0][2].text)
    else:
        return(False)


def make_samplesheet(fastq_dir,samplesheet,bucket_path,pipeline):
    cmd_list=["python",CREATE_SAMPLESHEET_SCRIPT,
              "--dir", fastq_dir,
              "--replace="+fastq_dir+":"+bucket_path,
              "--output", samplesheet]
    if pipeline=="cellranger":
        cmd_list.append("--index")
    cmd=" ".join(cmd_list)
    return(subprocess.call(cmd,shell=True))

def get_timestamp(source_dir):
    ''' 
    get timestamp of source_dir
    return: YYYYMMDD
    '''
    return(str(date.fromtimestamp(os.path.getmtime(source_dir))))

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

def upload_fastqs(source_dir,bucket_name):
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

def add_workspace_config(inputs,wdl_info,session,
                         namespace,name): 
    '''
    POST inputs to pipeline and wdl info to session
    inputs: dict with inputs to pipeline
    wdl_info: wdl name and snapshot number
    session: REST session
    namespace and name: workspace
    '''
    body={
          "namespace": namespace,
          "name": name,
          "rootEntityType": "participant",
          "inputs": inputs,
          "outputs": {},
          "prerequisites": {},
          "methodRepoMethod": {"methodNamespace": wdl_info["methodNamespace"],
                              "methodName": wdl_info["methodName"],
                              "methodVersion": wdl_info["methodVersion"]
                            },
          "methodConfigVersion": 1,
          "deleted": False
         }
    print body
    response=session.post((os.sep).join(["https://api.firecloud.org/api/workspaces",
                                          namespace,name,"methodconfigs"]),
                                          headers={"Content-type":"application/json",
                                          "Accept": "application/json"},
                                          json=body)  
    print "method_config"
    print response 
    return(response)

def add_workspace_attr(session,namespace,name,
                       attr_lists):
    '''
    PATCH workspace attr such as star ref,
    genome dict
    session: REST session
    namespace and name: workspace
    '''
    response=session.patch((os.sep).join(["https://api.firecloud.org/api/workspaces",
                                           namespace,name,"setAttributes"]),
                                           headers={"Content-type":"application/json",
                                                    "Accept": "application/json"},
                                           json=attr_lists)
    return(response)

def get_dict(input_dict_file):
    with open(input_dict_file) as idf:
         input_dict=eval(idf.read())
    return input_dict


def workspace_get(service_acc_path,source_dir):
    ''' 
    set up workspace with billing project
    service_acc_path: service acc key
    source_dir: basename 
    *return: response
    '''
    scopes=["https://www.googleapis.com/auth/userinfo.profile",
            "https://www.googleapis.com/auth/userinfo.email"]

    #credentials=ServiceAccountCredentials.from_json_keyfile_name(
    #                                            service_acc_path,
    #                                            scopes=scopes)
    credentials,project=google.auth.default()
    authed_session=AuthorizedSession(credentials)

    body={"namespace":"regev-collab",
          "name":source_dir,
          "attributes":{},
          "authorizationDomain":[]}
    session=AuthorizedSession(google.auth.default(scopes)[0])
    response=session.get(os.path.join("https://api.firecloud.org/api/workspaces",
                                      "regev-collab",source_dir),
                           headers={"Content-type":"application/json",
                                    "Accept": "application/json"})    
    return(json.loads(response.text),session)


def submit_workspace(namespace,name,session):
    '''
    POST workspace submission
    wdl_info: wdl name and snapshot number
    session: REST session
    namespace and name: workspace
    '''
    body={
           "methodConfigurationNamespace": namespace,
           "methodConfigurationName": name,
           "entityType": "participant",
           "entityName": "dummy",
           #"expression": "string",
           "useCallCache": True,
           "workflowFailureMode": "NoNewCalls"
         }
    #print body
    response=session.post((os.sep).join(["https://api.firecloud.org/api/workspaces",
                                          namespace,name,"submissions"]),
                                          headers={"Content-type":"application/json",
                                          "Accept": "application/json"},
                                          json=body)   
    print response 
    return(response,json.loads(response.text))

def update_entities(namespace,name,session,entity_file):
    '''
    import entities  
    '''
    with open (entity_file,"r") as tsv:
        entity_data=tsv.read()
        body=urlencode({"entities":entity_data})
        response=session.post((os.sep).join(["https://api.firecloud.org/api/workspaces",
                                              namespace,name,"importEntities"]),
                                              headers={"Content-type":"application/x-www-form-urlencoded"},
                                              data=body)
    print "entity"
    print response.status_code
    return(response)                                         

def check_response(response_recieved,response_needed):
    '''
    response_recieved: status code from API request
    response_needed: status code needed for success
    '''
    if response_recieved!=response_needed:
        print "Error recieved: "+str(response_recieved)
        sys.exit()

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
    if parsed_args.email_addresses:
        addresses=((parsed_args.email_addresses).strip()).split(",")
    #config=ConfigParser.ConfigParser()
     
    if os.path.isfile(parsed_args.config_json_file):
        with open (parsed_args.config_json_file) as json_file:
            config=json.load(json_file)
    else:
        print("Config file does not exist.")
        exit(1)
    #parse config file
    config_dict={}
    config_dict["firstName"]=config.get("ADMIN_SETTINGS").get("firstName")
    config_dict["lastName"]=config.get("ADMIN_SETTINGS").get("lastName")
    config_dict["title"]=config.get("ADMIN_SETTINGS").get("title")
    config_dict["contactEmail"]=config.get("ADMIN_SETTINGS").get("contactEmail")
    config_dict["institute"]=config.get("ADMIN_SETTINGS").get("institute")
    config_dict["institutionalProgram"]=config.get("ADMIN_SETTINGS").get("institutionalProgram")
    config_dict["programLocationCity"]=config.get("ADMIN_SETTINGS").get("programLocationCity")
    config_dict["programLocationState"]=config.get("ADMIN_SETTINGS").get("programLocationState")
    config_dict["programLocationCountry"]=config.get("ADMIN_SETTINGS").get("programLocationCountry")
    config_dict["pi"]=config.get("ADMIN_SETTINGS").get("pi")
    config_dict["nonProfitStatus"]=config.get("ADMIN_SETTINGS").get("nonProfitStatus")
    config_dict["smtp_server"]=config.get("ADMIN_SETTINGS").get("smtp_server")
    config_dict["job_log_location"]=config.get("ADMIN_SETTINGS").get("job_log_location")
    config_dict["fastq_dir"]=config.get("ADMIN_SETTINGS").get("fastq_dir")
    config_dict["entity_file"]=config.get("PIPELINE_SETTINGS").get("entity_file")

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=parsed_args.service_acc_path
    add_suffix=source_dir_base+"_fastqs"
    if parsed_args.skip_demult:
        fastq_dir=parsed_args.source_dir
        base=source_dir_base
    else:
        fastq_dir=make_directory(os.path.join(config_dict["fastq_dir"],
                                              add_suffix))
        fastq_dir=os.path.join(config_dict["fastq_dir"],add_suffix)
        base=add_suffix
        convert_code=convertbcls(parsed_args.source_dir,fastq_dir,
                                 parsed_args.nextseq_samplesheet,
                                 parsed_args.pipeline)
        #log and exit if code is false
    if os.listdir(fastq_dir):
        ws_response,session=workspace_setup(parsed_args.service_acc_path,
                                         source_dir_base)
        timestamp=get_timestamp(parsed_args.source_dir)
        if parsed_args.skip_demult:
            tags=[timestamp]
        else:
            flowcell_id=get_flowcellid(source_dir_base)
            seqrun_date=seqrundate(os.path.join(parsed_args.source_dir,RUNINFO))
            tags=[flowcell_id,seqrun_date]
        response_tags=set_tags(session,ws_response["namespace"],
                               ws_response["name"],tags)
        bucket_samplesheet="gs://"+os.path.join(ws_response['bucketName'],
                                                base,
                                                "samples.txt")
        bucketname_fastq="gs://"+os.path.join(ws_response['bucketName'],
                                              base)
        
        samplsheet_code=make_samplesheet(fastq_dir,
                                         os.path.join(fastq_dir,"samples.txt"),
                                         bucketname_fastq,
                                         parsed_args.pipeline)
        if samplsheet_code==0:
            upload_response=upload_fastqs(fastq_dir,ws_response['bucketName'])
        if upload_response==0:
            pipeline_settings_general=config.get("PIPELINE_SETTINGS")
            pipeline_settings=config.get("PIPELINE_SETTINGS").get(parsed_args.pipeline)
            input_json=pipeline_settings.get("input_json")            
            wdl_info=pipeline_settings.get("wdl_info") 
            attr_dict=pipeline_settings.get("attr_lists")
            input_dict=get_dict(input_json)
            input_dict[pipeline_settings["sample_file_key"]]='"'+bucket_samplesheet+'"'
            attr_params=attr_dict.get(parsed_args.org)
            ws_attr_response=add_workspace_attr(session,ws_response["namespace"],
                                                ws_response["name"],
                                                attr_params)
            check_response(ws_attr_response.status_code,200)
            config_response=add_workspace_config(input_dict,wdl_info,
                                                 session,
                                                 ws_response["namespace"],
                                                 ws_response["name"])
            check_response(config_response.status_code,201)
            entity_response=update_entities(ws_response["namespace"],
                                            ws_response["name"],
                                            session,
                                            pipeline_settings_general.get("entity_file"))
            check_response(entity_response.status_code,200)
            submit_response,response_text=submit_workspace(ws_response["namespace"],
                                             ws_response["name"],
                                             session)
            check_response(submit_response.status_code,201)
            status=get_status(ws_response["namespace"],
                              ws_response["name"],
                              response_text["submissionId"],
                              session)
            name=ws_response["name"]
            namespace=ws_response["namespace"]
            submission_id=response_text["submissionId"]
            workspace_id=ws_response["workspaceId"]
            conn = sqlite3.connect(config.get("PIPELINE_SETTINGS").get("database_file"))
            c = conn.cursor()
            table_name=config.get("PIPELINE_SETTINGS").get("table_name")
            try:
                c.execute("INSERT INTO {"+table_name+"}".\ 
                          "({submissionId},{namespace},{name},{workspaceId},{status},{pipeline_name})".\ 
                          " VALUES ("+submission_id+","+namespace+","+name+","+workspace_id+","+status+","+parsed_args.pipeline+")".\
                          format(tn=table_name, idf="submissionId", cn="namespace"))
            except sqlite3.IntegrityError:
                print('ERROR: ID already exists in PRIMARY KEY column {}'.format(id_column))
