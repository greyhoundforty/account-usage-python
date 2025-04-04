#!/usr/bin/env python3
# Author: Jon Hall
# Copyright (c) 2023
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


__author__ = 'jonhall'
import os, logging, logging.config, os.path, argparse, pytz
from datetime import datetime, tzinfo, timezone
import pandas as pd
import numpy as np
from ibm_platform_services import IamIdentityV1, UsageReportsV4, GlobalTaggingV1, GlobalSearchV2
from ibm_cloud_sdk_core import ApiException
from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
from ibm_platform_services.resource_controller_v2 import *
from ibm_platform_services.case_management_v1 import *
from dotenv import load_dotenv

def setup_logging(default_path='logging.json', default_level=logging.info, env_key='LOG_CFG'):
    # read logging.json for log parameters to be ued by script
    path = default_path
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = json.load(f)
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)

def getAccountId(IC_API_KEY):
    ##########################################################
    ## Get AccountId for this API Key
    ##########################################################

    try:
        api_key = iam_identity_service.get_api_keys_details(
          iam_api_key=IC_API_KEY
        ).get_result()
    except ApiException as e:
        logging.error("API exception {}.".format(str(e)))
        quit()

    return api_key["account_id"]

def createSDK(IC_API_KEY):
    """
    Create SDK clients
    """
    global iam_identity_service, case_management_service

    try:
        authenticator = IAMAuthenticator(IC_API_KEY)
    except ApiException as e:
        logging.error("API exception {}.".format(str(e)))
        quit()

    try:
        iam_identity_service = IamIdentityV1(authenticator=authenticator)
    except ApiException as e:
        logging.error("API exception {}.".format(str(e)))
        quit()


    try:
        case_management_service = CaseManagementV1(authenticator=authenticator)
        case_management_service.enable_retries(max_retries=5, retry_interval=1.0)
        case_management_service.set_http_config({'timeout': 120})
    except ApiException as e:
        logging.error("API exception {}.".format(str(e)))
        quit()

def getCases():
    all_results = []
    pager = GetCasesPager(client=case_management_service, limit=10)
    while pager.has_next():
        next_page = pager.get_next()
        assert next_page is not None
        all_results.extend(next_page)
    return all_results
def parseCases(account, account_name, cases):
    data =[]
    for case in cases:
        row = {
        "account": account,
        "account_name": account_name,
        "number":  case["number"],
        "short_description": case["short_description"],
        "description": case["description"],
        "created_at": case["created_at"],
        "created_by": case["created_by"]["name"],
        "updated_at": case["updated_at"],
        "updated_by": case["updated_by"]["name"],
        "contact_type": case["contact_type"],
        "status": case["status"],
        "severity": case["severity"],
        "support_tier": case["support_tier"],
        "resolution": case["resolution"]
        }
        data.append(row.copy())

    cases_df = pd.DataFrame(data,
        columns=[
            "account",
            "account_name",
            "number",
            "short_description",
            "description",
            "created_at",
            "created_by",
            "updated_at",
            "updated_by",
            "contact_type",
            "status",
            "severity",
            "support_tier",
            "resolution"])
    return cases_df
def writeFiletoCos(localfile, upload):
    """"
    Write Files to COS
    """

    def multi_part_upload(bucket_name, item_name, file_path):
        """"
        Write Files to COS
        """
        try:
            logging.info("Starting file transfer for {0} to bucket: {1}".format(item_name, bucket_name))
            # set 5 MB chunks
            part_size = 1024 * 1024 * 5

            # set threadhold to 15 MB
            file_threshold = 1024 * 1024 * 15

            # set the transfer threshold and chunk size
            transfer_config = ibm_boto3.s3.transfer.TransferConfig(
                multipart_threshold=file_threshold,
                multipart_chunksize=part_size
            )

            # the upload_fileobj method will automatically execute a multi-part upload
            # in 5 MB chunks for all files over 15 MB
            with open(file_path, "rb") as file_data:
                cos.Object(bucket_name, item_name).upload_fileobj(
                    Fileobj=file_data,
                    Config=transfer_config
                )
            logging.info("Transfer for {0} complete".format(item_name))
        except ClientError as be:
            logging.error("CLIENT ERROR: {0}".format(be))
        except Exception as e:
            logging.error("Unable to complete multi-part upload: {0}".format(e))
        return

    cos = ibm_boto3.resource("s3",
                             ibm_api_key_id=args.COS_APIKEY,
                             ibm_service_instance_id=args.COS_INSTANCE_CRN,
                             config=Config(signature_version="oauth"),
                             endpoint_url=args.COS_ENDPOINT
                             )
    multi_part_upload(args.COS_BUCKET, upload, "./" + localfile)
    return

def writeCases(cases_df):
    """
    Write cases to excel
    """
    logging.info("Creating Cases tab.")
    cases_df.to_excel(writer, "Cases")
    worksheet = writer.sheets['Cases']
    totalrows,totalcols=cases_df.shape
    worksheet.autofilter(0,0,totalrows,totalcols)
    return

if __name__ == "__main__":
    setup_logging()
    load_dotenv()
    parser = argparse.ArgumentParser(description="Get Account Cases.")
    parser.add_argument("--output", default=os.environ.get('output', 'cases.xlsx'), help="Filename Excel input file for list of resources and tags. (including extension of .xlsx)")
    parser.add_argument("--debug", action=argparse.BooleanOptionalAction, help="Set Debug level for logging.")
    args = parser.parse_args()

    if args.debug:
        log = logging.getLogger()
        log.handlers[0].setLevel(logging.DEBUG)
        log.handlers[1].setLevel(logging.DEBUG)

    APIKEYS = os.environ.get('APIKEYS', None)

    if APIKEYS == None:
        logging.error("You must specify apikey and name for each account in .env file or APIKEYS environment variable.")
        quit()
    else:
        """ Convert to List of JSON variable """
        try:
            APIKEYS = json.loads(APIKEYS)
        except ValueError as e:
            logging.error("Invalid List of APIKEYS.")
            quit()


        """" Loop through specified accounts and get cases """
        cases_df = pd.DataFrame()
        for account in APIKEYS:
            if "apikey" in account:
                apikey = account["apikey"]
                createSDK(apikey)
                accountId = getAccountId(apikey)
                logging.info("Getting cases for account {}.".format(accountId))
                cases = getCases()
                logging.info("Parsing cases for account {}.".format(accountId))
                cases_df = pd.concat([cases_df, parseCases(accountId, account["name"], cases)])
            else:
                logging.error("No Apikey found.")
                quit()

        output = args.output
        split_tup = os.path.splitext(args.output)
        """ remove file extension """
        file_name = split_tup[0]
        writer = pd.ExcelWriter(file_name + ".xlsx", engine='xlsxwriter')
        workbook = writer.book
        writeCases(cases_df)
        writer.close()

    logging.info("Getting Cases Complete.")
