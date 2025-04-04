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

def attachTag(resource_crn, tag):
    """
    Attach Given Tag to specified resource_crn
    resource_crn: instance to add tag to
    tag:  tag to attach
    :return:
    """
    resource_model = {'resource_id': resource_crn}
    tag_results = global_tagging_service.attach_tag(
        resources=[resource_model],
        tag_names=tag,
        tag_type='user').get_result()

    # assumes a single resource crn specified.
    if tag_results["results"][0]["is_error"]:
        logging.error("Error updating tag {} for resource {}. {}".format(tag, resource_crn, tag_results["results"][0]["message"]))
        logging.debug(tag_results)
    else:
        logging.info("Attached tag {} to resource {}.".format(tag, resource_crn))
    return

def createSDK(IC_API_KEY):
    """
    Create SDK clients
    """
    global resource_controller_service, global_tagging_service, iam_identity_service, global_search_service, usage_reports_service

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
        usage_reports_service = UsageReportsV4(authenticator=authenticator)
    except ApiException as e:
        logging.error("API exception {}.".format(str(e)))
        quit()

    try:
        resource_controller_service = ResourceControllerV2(authenticator=authenticator)
    except ApiException as e:
        logging.error("API exception {}.".format(str(e)))
        quit()

    try:
      global_tagging_service = GlobalTaggingV1(authenticator=authenticator)
      global_tagging_service.enable_retries(max_retries=5, retry_interval=1.0)
      global_tagging_service.set_http_config({'timeout': 120})
    except ApiException as e:
        logging.error("API exception {}.".format(str(e)))
        quit()

    try:
        global_search_service = GlobalSearchV2(authenticator=authenticator)
    except ApiException as e:
        logging.error("API exception {}.".format(str(e)))
        quit()


if __name__ == "__main__":
    setup_logging()
    load_dotenv()
    parser = argparse.ArgumentParser(description="Tag CRN's in an account with audit tag.")
    parser.add_argument("--input", default=os.environ.get('input', 'tags.xlsx'), help="Filename Excel input file for list of resources and tags. (including extension of .xlsx)")
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

        """" Read List of New Tags into DF from sheet named tags"""
        tags_df = pd.read_excel(args.input, sheet_name='ServerDetail')

        """" Loop through specified accounts and tag specified resources """
        for account in APIKEYS:
            if "apikey" in account:
                apikey = account["apikey"]
                createSDK(apikey)
                accountId = getAccountId(apikey)
                logging.info("Tagging instances for account {}.".format(accountId))
                for index, row in tags_df.iterrows():
                    """ tag only resource which match the account authenticated with """
                    if row["account_id"] == accountId:
                        resource_crn = row["instance_id"]
                        """ remove spaces and create list from comma seperated tags """
                        if isinstance(row["new_tag"], str):
                            if len(row["new_tag"]) > 0:
                                """ remove whitespace and any trailing commas, then split into list of tags """
                                tag = row["new_tag"].strip().rstrip(",")
                                tag = tag.replace(" ", "").split(",")
                                attachTag(resource_crn, tag)
            else:
                logging.error("No Apikey found.")
                quit()
    logging.info("Attaching Tags Complete.")
