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
import ibm_boto3
from ibm_platform_services import IamIdentityV1, UsageReportsV4, GlobalSearchV2
from ibm_cloud_sdk_core import ApiException
from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
from ibm_platform_services.resource_controller_v2 import *
from ibm_botocore.client import Config, ClientError
from ibm_vpc import VpcV1
from dotenv import load_dotenv
from urllib import parse

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
        quit(1)

    return api_key["account_id"]
def populateTagCache():
    ##########################################################
    ## Populate Tagging data into cache
    ##########################################################

    search_cursor = None
    items = []
    while True:
        response = global_search_service.search(query='tags:*',
                                                search_cursor=search_cursor,
                                                fields=["tags"],
                                                limit=1000)


        scan_result = response.get_result()

        items = items + scan_result["items"]
        if "search_cursor" not in scan_result:
            break
        else:
            search_cursor = scan_result["search_cursor"]

    tag_cache = {}
    for resource in items:
        resourceId = resource["crn"]
        tag_cache[resourceId] = resource["tags"]

    return tag_cache
def populateInstanceCache():
    """
    Get VPC instance information and create cache from each VPC regional endpoint
    """
    endpoints = [vpc_service_us_south, vpc_service_us_east, vpc_service_ca_tor]
    items = []

    for endpoint in endpoints:
        """ Get virtual servers """
        instances = endpoint.list_instances()
        while True:
            try:
                result = instances.get_result()
            except ApiException as e:
                logging.error("List VPC virtual server instances with status code{}:{}".format(str(e.code), e.message))
                quit(1)

            items = items + result["instances"]
            if "next" not in result:
                break
            else:
                next = dict(parse.parse_qsl(parse.urlsplit(result["next"]["href"]).query))
                instances = endpoint.list_instances(start=next["start"])

        """ Get Bare Metal"""
        instances = endpoint.list_bare_metal_servers()
        while True:
            try:
                result = instances.get_result()
            except ApiException as e:
                logging.error("List BM server instances with status code{}:{}".format(str(e.code), e.message))
                quit(1)

            items = items + result["bare_metal_servers"]
            if "next" not in result:
                break
            else:
                next = dict(parse.parse_qsl(parse.urlsplit(result["next"]["href"]).query))
                instances = endpoint.list_bare_metal_servers(start=next["start"])


    instance_cache = {}
    for resource in items:
        crn = resource["crn"]
        instance_cache[crn] = resource

    return instance_cache
def createSDK(IC_API_KEY):
    """
    Create SDK clients
    """
    global resource_controller_service, iam_identity_service, global_search_service, usage_reports_service, vpc_service_us_south, vpc_service_us_east, vpc_service_ca_tor

    try:
        authenticator = IAMAuthenticator(IC_API_KEY)
    except ApiException as e:
        logging.error("API exception {}.".format(str(e)))
        quit(1)

    try:
        iam_identity_service = IamIdentityV1(authenticator=authenticator)
    except ApiException as e:
        logging.error("API exception {}.".format(str(e)))
        quit(1)

    try:
        usage_reports_service = UsageReportsV4(authenticator=authenticator)
        usage_reports_service.enable_retries(max_retries=5, retry_interval=1.0)
        usage_reports_service.set_http_config({'timeout': 120})
    except ApiException as e:
        logging.error("API exception {}.".format(str(e)))
        quit(1)

    try:
        resource_controller_service = ResourceControllerV2(authenticator=authenticator)
        resource_controller_service.enable_retries(max_retries=5, retry_interval=1.0)
        resource_controller_service.set_http_config({'timeout': 120})
    except ApiException as e:
        logging.error("API exception {}.".format(str(e)))
        quit(1)

    try:
        global_search_service = GlobalSearchV2(authenticator=authenticator)
        global_search_service.enable_retries(max_retries=5, retry_interval=1.0)
        global_search_service.set_http_config({'timeout': 120})
    except ApiException as e:
        logging.error("API exception {}.".format(str(e)))
        quit(1)

    try:
        vpc_service_us_south = VpcV1(authenticator=authenticator)
        vpc_service_us_south.set_service_url('https://us-south.iaas.cloud.ibm.com/v1')
    except ApiException as e:
        logging.error("API exception {}.".format(str(e)))
        quit(1)

    try:
        vpc_service_us_east = VpcV1(authenticator=authenticator)
        vpc_service_us_east.set_service_url('https://us-east.iaas.cloud.ibm.com/v1')
    except ApiException as e:
        logging.error("API exception {}.".format(str(e)))
        quit(1)

    try:
        vpc_service_ca_tor = VpcV1(authenticator=authenticator)
        vpc_service_ca_tor.set_service_url('https://ca-tor.iaas.cloud.ibm.com/v1')
    except ApiException as e:
        logging.error("API exception {}.".format(str(e)))
        quit(1)
def getCurrentMonthAccountUsage():
    """
    Get IBM Cloud Service from account for current month
    """

    data = []
    usageMonth = datetime.now().strftime("%Y-%m")

    try:
        usage = usage_reports_service.get_account_usage(
            account_id=accountId,
            billingmonth=usageMonth,
            names=True
        ).get_result()
    except ApiException as e:
        if e.code == 424:
            logging.warning("API exception {}.".format(str(e)))
            quit(1)
        else:
            logging.error("API exception {}.".format(str(e)))
            quit(1)

    logging.debug("usage {}={}".format(usageMonth, usage))
    for resource in usage['resources']:
        for plan in resource['plans']:
            for metric in plan['usage']:
                row = {
                    'account_id': usage["account_id"],
                    'account_name': accountName,
                    'month': usageMonth,
                    'currency_code': usage['currency_code'],
                    'billing_country': usage['billing_country'],
                    'resource_id': resource['resource_id'],
                    'resource_name': resource['resource_name'],
                    'billable_charges': resource["billable_cost"],
                    'billable_rated_charges': resource["billable_rated_cost"],
                    'plan_id': plan['plan_id'],
                    'plan_name': plan['plan_name'],
                    'metric': metric['metric'],
                    'unit_name': metric['unit_name'],
                    'quantity': float(metric['quantity']),
                    'rateable_quantity': metric['rateable_quantity'],
                    'cost': metric['cost'],
                    'rated_cost': metric['rated_cost'],
                    }
                if metric['discounts'] != []:
                    """
                    Discount found in usage record
                    """
                    row['discount'] = metric['discounts'][0]['discount'] / 100
                else:
                    """
                    No discount found in usage record
                    """
                    row['discount'] = 0


                if len(metric['price']) > 0:
                    row['price'] = metric['price']
                else:
                    row['price'] = "[]"
                # add row to data
                data.append(row.copy())


    accountUsage = pd.DataFrame(data, columns=['account_id', "account_name", 'month', 'currency_code', 'billing_country', 'resource_id', 'resource_name',
                    'billable_charges', 'billable_rated_charges', 'plan_id', 'plan_name', 'metric', 'unit_name', 'quantity',
                    'rateable_quantity','cost', 'rated_cost', 'discount', 'price'])

    return accountUsage
def listAllResourceInstances():
    """
    Retrieve all Resources for account from resource controller fore resource_type
    """
    all_results = []
    pager = ResourceInstancesPager(
        client=resource_controller_service,
        limit=20
    )

    try:
        while pager.has_next():
            next_page = pager.get_next()
            assert next_page is not None
            all_results.extend(next_page)
        logging.debug("resource_instance={}".format(all_results))
    except ApiException as e:
            logging.error(
                "API Error.  Can not retrieve instances of type {} {}: {}".format(resource_type, str(e.code),
                                                                                         e.message))
            quit(1)
    return all_results
def getTags(resourceId):
    """
    Check Tag_Cache for Resource tags which may have been retrieved previously
    """
    global tag_cache
    if resourceId not in tag_cache:
        logging.debug("Cache miss for Tag {}".format(resourceId))
        tag = []
    else:
        tag = tag_cache[resourceId]
    return tag
def getInstance(instance):
    """
    Check Tag_Cache for Resource tags which may have been retrieved previously
    """
    global instance_cache
    if instance not in instance_cache:
        logging.warning("Cache miss for VPC instance {}".format(instance))
        instance_data = []
    else:
        instance_data = instance_cache[instance]
    return instance_data
def parseResources(accoutName, resources):
    """
    Parse Resource JSON
    """

    data = []
    nytz = pytz.timezone('America/New_York')
    for resource_instance in resources:
        row = {
            "account_id": resource_instance["account_id"],
            "account_name": accountName,
            "service_id": resource_instance["resource_id"],
            "instance_id": resource_instance["id"],
            "region_id": resource_instance["region_id"],
            "name": resource_instance["name"]
        }

        if "created_at" in resource_instance:
            created_at = resource_instance["created_at"]
        else:
            created_at = ""

        if "updated_at" in resource_instance:
            updated_at = resource_instance["updated_at"]
        else:
            updated_at = ""

        """
        get tags attached to instance from cache or resource controller
        """

        tags = getTags(resource_instance["id"])
        # parse role tag into comma delimited list
        if len(tags) > 0:
            role = ",".join([str(item) for item in tags if "role:" in item])
            audit = ",".join([str(item) for item in tags if "audit:" in item])
        else:
            role = ""
            audit = ""


        row_addition = {
            "instance_created_at": created_at,
            "instance_updated_at": updated_at,
            "role": role,
            "audit": audit,
        }

        # combine original row with additions

        row = row | row_addition
        data.append(row.copy())

    resourceDetail = pd.DataFrame(data, columns=['account_id', "account_name", "service_id", "instance_id", "name", "region_id",
                                                 "instance_created_at", "instance_updated_at",
                                                 "role", "audit"])
    return resourceDetail
def createTagListTab(paasUsage):
    """
    Write Service Usage detail tab to excel
    """
    logging.info("Creating currentTags tab.")
    paasUsage.to_excel(writer, "currentTags")
    worksheet = writer.sheets['currentTags']
    totalrows,totalcols=paasUsage.shape
    worksheet.autofilter(0,0,totalrows,totalcols)
    format2 = workbook.add_format({'align': 'left'})
    worksheet.set_column("B:B", 32, format2)
    worksheet.set_column("C:C", 20, format2)
    worksheet.set_column("D:D", 34, format2)
    worksheet.set_column("E:E", 160, format2)
    worksheet.set_column("F:F", 46, format2)
    worksheet.set_column("G:G", 12, format2)
    worksheet.set_column("H:I", 28, format2)
    worksheet.set_column("J:K", 30, format2)
    return
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

if __name__ == "__main__":
    setup_logging()
    load_dotenv()
    parser = argparse.ArgumentParser(description="Get list of tags for every service instance.")
    parser.add_argument("--output", default=os.environ.get('output', 'currentTags.xlsx'), help="Filename Excel output file. (including extension of .xlsx)")
    parser.add_argument("--debug", action=argparse.BooleanOptionalAction, help="Set Debug level for logging.")
    parser.add_argument("--load", action=argparse.BooleanOptionalAction, help="Load dataframes from pkl files.")
    parser.add_argument("--save", action=argparse.BooleanOptionalAction, help="Store dataframes to pkl files.")
    parser.add_argument("--cos", "--COS", action=argparse.BooleanOptionalAction, help="Upload files to COS bucket specified.")
    parser.add_argument("--COS_APIKEY", default=os.environ.get('COS_APIKEY', None),
                        help="COS apikey to use to write output to Object Storage.")
    parser.add_argument("--COS_ENDPOINT", default=os.environ.get('COS_ENDPOINT', None),
                        help="COS endpoint to use to write output tp Object Storage.")
    parser.add_argument("--COS_INSTANCE_CRN", default=os.environ.get('COS_INSTANCE_CRN', None),
                        help="COS Instance CRN to use to write output to Object Storage.")
    parser.add_argument("--COS_BUCKET", default=os.environ.get('COS_BUCKET', None),
                        help="COS Bucket name to use to write output to Object Storage.")

    args = parser.parse_args()

    if args.debug:
        log = logging.getLogger()
        log.handlers[0].setLevel(logging.DEBUG)
        log.handlers[1].setLevel(logging.DEBUG)

    APIKEYS = os.environ.get('APIKEYS', None)
    if not args.load:
        if APIKEYS == None:
            logging.error("You must specify apikey and name for each account in .env file or APIKEYS environment variable.")
            quit(1)
        else:
            accountUsage = pd.DataFrame()
            resources = pd.DataFrame()
            """ Convert to List of JSON variable """
            try:
                APIKEYS = json.loads(APIKEYS)
            except ValueError as e:
                logging.error("Invalid List of APIKEYS.")
                quit(1)
            for account in APIKEYS:
                if "apikey" in account:
                    apikey = account["apikey"]
                    createSDK(apikey)
                    accountId = getAccountId(apikey)
                    if "name" in account:
                        accountName = account["name"]
                        logging.info("Caching Tag Data for {} AccountId: {}.".format(accountName, accountId))
                        tag_cache = populateTagCache()
                        logging.info("Retrieving current list of service instances for {} AccountId: {}.".format(accountName, accountId))
                        resources = pd.concat([resources, parseResources(accountName, listAllResourceInstances())])
                    else:
                        logging.error("No Name for Account found.")
                else:
                    logging.error("No APIKEY found.")
                    quit(1)
    else:
        logging.info("Retrieving Usage and Instance data from stored data file")
        resources = pd.read_pickle("resources.pkl")

    if args.save:
        resources.to_pickle("resources.pkl")

    # Write dataframe to excel
    output = args.output
    split_tup = os.path.splitext(args.output)
    """ remove file extension """
    file_name = split_tup[0]
    timestamp = "_(run@{})".format(datetime.now().strftime("%Y-%m-%d_%H:%M"))
    writer = pd.ExcelWriter(file_name + ".xlsx", engine='xlsxwriter')
    workbook = writer.book
    createTagListTab(resources)
    writer.close()

    """ If --COS then copy files with report end month + timestamp to COS """
    if args.cos:
        """ Write output to COS"""
        logging.info("Writing Pivot Tables to COS.")
        writeFiletoCos(file_name + ".xlsx", file_name + timestamp + ".xlsx")
    logging.info("Generation of currentTags is complete.")
