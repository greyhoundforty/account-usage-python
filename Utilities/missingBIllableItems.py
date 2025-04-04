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
from dateutil.relativedelta import *
import pandas as pd
import numpy as np
import ibm_boto3
from ibm_platform_services import IamIdentityV1, UsageReportsV4, GlobalTaggingV1, GlobalSearchV2
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
def createSDK(IC_API_KEY):
    """
    Create SDK clients
    """
    global resource_controller_service, global_tagging_service, iam_identity_service, global_search_service, usage_reports_service, vpc_service_us_south, vpc_service_us_east, vpc_service_ca_tor

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
        usage_reports_service.enable_retries(max_retries=5, retry_interval=1.0)
        usage_reports_service.set_http_config({'timeout': 120})
    except ApiException as e:
        logging.error("API exception {}.".format(str(e)))
        quit()

    try:
        resource_controller_service = ResourceControllerV2(authenticator=authenticator)
        resource_controller_service.enable_retries(max_retries=5, retry_interval=1.0)
    except ApiException as e:
        logging.error("API exception {}.".format(str(e)))
        quit()

    try:
      global_tagging_service = GlobalTaggingV1(authenticator=authenticator)
    except ApiException as e:
        logging.error("API exception {}.".format(str(e)))
        quit()

    try:
        global_search_service = GlobalSearchV2(authenticator=authenticator)
    except ApiException as e:
        logging.error("API exception {}.".format(str(e)))
        quit()

    try:
        vpc_service_us_south = VpcV1(authenticator=authenticator)
        vpc_service_us_south.set_service_url('https://us-south.iaas.cloud.ibm.com/v1')
    except ApiException as e:
        logging.error("API exception {}.".format(str(e)))
        quit()

    try:
        vpc_service_us_east = VpcV1(authenticator=authenticator)
        vpc_service_us_east.set_service_url('https://us-east.iaas.cloud.ibm.com/v1')
    except ApiException as e:
        logging.error("API exception {}.".format(str(e)))
        quit()

    try:
        vpc_service_ca_tor = VpcV1(authenticator=authenticator)
        vpc_service_ca_tor.set_service_url('https://ca-tor.iaas.cloud.ibm.com/v1')
    except ApiException as e:
        logging.error("API exception {}.".format(str(e)))
        quit()
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

    df = pd.DataFrame.from_dict(items)
    tag_cache = {}
    for resource in items:
        resourceId = resource["crn"]
        tag_cache[resourceId] = resource["tags"]

    return tag_cache, df
def prePopulateResourceCache(accountName, accountId):
    """
    Retrieve all Resources for account from resource controller and pre-populate cache
    """
    logging.info("Resource_cache being pre-populated with active resources in account.")
    all_results = []
    pager = ResourceInstancesPager(
        client=resource_controller_service,
        limit=50
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
        quit()
    # Convert to DF and populdate all ROWS with account info
    resources_df = pd.DataFrame.from_dict(all_results)
    resources_df["accountId"] = accountId
    resources_df["accountName"] = accountName

    resource_cache = {}
    for resource in all_results:
        resourceId = resource["crn"]
        resource_cache[resourceId] = resource

    return resource_cache, resources_df
def populateInstanceCache():
    """
    Get profile information and create cache from each VPC endpoint
    """

    items = []
    instances = vpc_service_us_south.list_instances()
    while True:
        """
        Loop through all items in us-south
        """
        try:
            result = instances.get_result()
        except ApiException as e:
            logging.error("List VPC virtual server instances with status code{}:{}".format(str(e.code), e.message))
            quit()

        items = items + result["instances"]
        if "next" not in result:
            break
        else:
            next = dict(parse.parse_qsl(parse.urlsplit(result["next"]["href"]).query))
            instances = vpc_service_us_south.list_instances(start=next["start"])

    instances = vpc_service_us_east.list_instances()
    while True:
        """
        Loop through all items in us-east
        """
        try:
            result = instances.get_result()
        except ApiException as e:
            logging.error("List VPC virtual server instances with status code{}:{}".format(str(e.code), e.message))
            quit()

        items = items + result["instances"]
        if "next" not in result:
            break
        else:
            next = dict(parse.parse_qsl(parse.urlsplit(result["next"]["href"]).query))
            instances = vpc_service_us_east.list_instances(start=next["start"])

    instances = vpc_service_ca_tor.list_instances()
    while True:
        """
        Loop through all items in us-east
        """
        try:
            result = instances.get_result()
        except ApiException as e:
            logging.error("List VPC virtual server instances with status code{}:{}".format(str(e.code), e.message))
            quit()

        items = items + result["instances"]
        if "next" not in result:
            break
        else:
            next = dict(parse.parse_qsl(parse.urlsplit(result["next"]["href"]).query))
            instances = vpc_service_ca_tor.list_instances(start=next["start"])

    instance_cache = {}
    for resource in items:
        crn = resource["crn"]
        instance_cache[crn] = resource

    return instance_cache
def getTags(resourceId):
    """
    Check Tag Cache for Resource
    """
    if resourceId not in tag_cache:
        logging.debug("Cache miss for Tag {}".format(resourceId))
        tags = []
    else:
        tags = tag_cache[resourceId]
    return tags
def getInstancesUsage(start, end):
    """
    Get instances resource usage for month of specific resource_id
    """
    global tag_cache, resource_cache

    def getResourceInstancefromCloud(resourceId):
        """
        Retrieve Resource Details from resource controller if not in cache
        """
        logging.debug("Requesting resource data from resource controller for {}".format(resourceId))
        try:
            resource_instance = resource_controller_service.get_resource_instance(
                id=resourceId).get_result()
            logging.debug("resource_instance={}".format(resource_instance))
        except ApiException as e:
                logging.error(
                    "get_resource_instance failed for instance {} {}: {}".format(resourceId, str(e.code),
                                                                                             e.message))
                resource_instance = {}

        return resource_instance
    def getResourceInstance(resourceId):
        """
        Check Cache for Resource Details which may have been retrieved previously
        """
        if resourceId not in resource_cache:
            logging.debug("Cache miss for Resource {}".format(resourceId))
            resource_cache[resourceId] = getResourceInstancefromCloud(resourceId)
        return resource_cache[resourceId]

    data = []
    nytz = pytz.timezone('America/New_York')
    limit = 100  ## set limit of record returned

    while start <= end:
        usageMonth = start.strftime("%Y-%m")
        #logging.info("Retrieving Instances Usage from {}.".format(usageMonth))
        start += relativedelta(months=+1)
        recordstart = 1
        """ Read first Group of records """
        try:
            instances_usage = usage_reports_service.get_resource_usage_account(
                account_id=accountId,
                billingmonth=usageMonth, names=True, limit=limit).get_result()
        except ApiException as e:
            logging.error("Fatal Error with get_resource_usage_account: {}".format(e))
            quit()

        if recordstart + limit > instances_usage["count"]:
            recordstop = instances_usage["count"]
        else:
            recordstop = recordstart + limit - 1
        logging.info(
            "Requesting Instance {} Usage for {}: retrieved from {} to {} of Total {}".format(usageMonth, accountName,
                                                                                               recordstart,
                                                                                               recordstop,
                                                                                               instances_usage[
                                                                                                   "count"]))

        if "next" in instances_usage:
            nextoffset = instances_usage["next"]["offset"]
        else:
            nextoffset = ""
        rows_count = instances_usage["count"]

        while True:
            for instance in instances_usage["resources"]:
                logging.debug("Parsing Details for Instance {} of {} {}".format(recordstart, rows_count, instance["resource_instance_id"]))
                recordstart += 1
                if "pricing_country" in instance:
                    pricing_country = instance["pricing_country"]
                else:
                    pricing_country = ""

                if "billing_country" in instance:
                    billing_country = instance["billing_country"]
                else:
                    billing_country = ""

                if "currency_code" in instance:
                    currency_code = instance["currency_code"]
                else:
                    currency_code = ""

                if "pricing_region" in instance:
                    pricing_region = instance["pricing_region"]
                else:
                    pricing_region = ""

                row = {
                    "account_id": instance["account_id"],
                    "account_name": accountName,
                    "instance_id": instance["resource_instance_id"],
                    "resource_group_id": instance["resource_group_id"],
                    "month": instance["month"],
                    "pricing_country": pricing_country,
                    "billing_country": billing_country,
                    "currency_code": currency_code,
                    "plan_id": instance["plan_id"],
                    "plan_name": instance["plan_name"],
                    "billable": instance["billable"],
                    "pricing_plan_id": instance["pricing_plan_id"],
                    "pricing_region": pricing_region,
                    "region": instance["region"],
                    "service_id": instance["resource_id"],
                    "service_name": instance["resource_name"],
                    "resource_group_name": instance["resource_group_name"],
                    "instance_name": instance["resource_instance_name"]
                }

                # get instance detail from cache or resource controller
                resource_instance = getResourceInstance(instance["resource_instance_id"])

                if "created_at" in resource_instance:
                    created_at = resource_instance["created_at"]
                    """Create Provision Date Field using US East Timezone for Zulu conversion"""
                    provisionDate = pd.to_datetime(created_at, format="%Y-%m-%dT%H:%M:%S.%f").astimezone(nytz)
                    provisionDate = provisionDate.strftime("%Y-%m-%d")
                else:
                    created_at = ""
                    provisionDate = ""

                if "updated_at" in resource_instance:
                    updated_at = resource_instance["updated_at"]
                else:
                    updated_at = ""

                if "deleted_at" in resource_instance:
                    deleted_at = resource_instance["deleted_at"]
                else:
                    deleted_at = ""

                if "state" in resource_instance:
                    state = resource_instance["state"]
                else:
                    state = ""

                """
                For VPC Virtual Servers obtain intended profile and virtual server details
                """
                az = ""
                profile = ""
                cpuFamily = ""
                numberOfVirtualCPUs = ""
                MemorySizeMiB = ""
                NodeName = ""
                NumberOfGPUs = ""
                NumberOfInstStorageDisks = ""
                NumberofCores = ""
                NumberofSockets = ""
                Bandwidth = ""
                if "extensions" in resource_instance:
                    if "VirtualMachineProperties" in resource_instance["extensions"]:
                        profile = resource_instance["extensions"]["VirtualMachineProperties"]["Profile"]
                        cpuFamily = resource_instance["extensions"]["VirtualMachineProperties"]["CPUFamily"]
                        numberOfVirtualCPUs = resource_instance["extensions"]["VirtualMachineProperties"]["NumberOfVirtualCPUs"]
                        MemorySizeMiB = resource_instance["extensions"]["VirtualMachineProperties"]["MemorySizeMiB"]
                        NodeName = resource_instance["extensions"]["VirtualMachineProperties"]["NodeName"]
                        NumberOfGPUs = resource_instance["extensions"]["VirtualMachineProperties"]["NumberOfGPUs"]
                        NumberOfInstStorageDisks = resource_instance["extensions"]["VirtualMachineProperties"]["NumberOfInstStorageDisks"]

                    elif "BMServerProperties" in resource_instance["extensions"]:
                        profile = resource_instance["extensions"]["BMServerProperties"]["Profile"]
                        MemorySizeMiB = resource_instance["extensions"]["BMServerProperties"]["MemorySizeMiB"]
                        NodeName = resource_instance["extensions"]["BMServerProperties"]["NodeName"]
                        NumberofCores = resource_instance["extensions"]["BMServerProperties"]["NumberOfCores"]
                        NumberofSockets = resource_instance["extensions"]["BMServerProperties"]["NumberOfSockets"]
                        Bandwidth = resource_instance["extensions"]["BMServerProperties"]["Bandwidth"]
                    if "Resource" in resource_instance["extensions"]:
                        if "AvailabilityZone" in resource_instance["extensions"]["Resource"]:
                            az = resource_instance["extensions"]["Resource"]["AvailabilityZone"]

                # get tags attached to instance from cache or resource controller
                tags = getTags(instance["resource_instance_id"])
                logging.debug("Instance {} tags: {}".format(instance["resource_instance_id"], tags))

                # parse role tag into comma delimited list
                if len(tags) > 0:
                    role = ",".join([str(item.split(":")[1]) for item in tags if "role:" in item])
                    audit = ",".join([str(item.split(":")[1]) for item in tags if "audit:" in item])
                else:
                    role = ""
                    audit = ""


                row_addition = {
                    "provision_date": provisionDate,
                    "instance_created_at": created_at,
                    "instance_updated_at": updated_at,
                    "instance_deleted_at": deleted_at,
                    "instance_state": state,
                    "instance_profile": profile,
                    "cpu_family": cpuFamily,
                    "numberOfVirtualCPUs": numberOfVirtualCPUs,
                    "MemorySizeMiB":  MemorySizeMiB,
                    "NodeName":  NodeName,
                    "NumberOfGPUs": NumberOfGPUs,
                    "NumberOfInstStorageDisks": NumberOfInstStorageDisks,
                    "instance_role": role,
                    "audit": audit,
                    "availability_zone": az,
                    "BMnumberofCores": NumberofCores,
                    "BMnumberofSockets": NumberofSockets,
                    "BMbandwidth": Bandwidth
                }

                # combine original row with additions
                row = row | row_addition

                for usage in instance["usage"]:
                    metric = usage["metric"]
                    unit = usage["unit"]
                    quantity = float(usage["quantity"])
                    cost = usage["cost"]
                    rated_cost = usage["rated_cost"]
                    rateable_quantity = float(usage["rateable_quantity"])
                    price = usage["price"]
                    metric_name = usage["metric_name"]
                    unit_name = usage["unit_name"]

                    # For servers estimate days of usage
                    if (instance["resource_name"] == "Virtual Server for VPC" or instance["resource_name"] == "Bare Metal Servers for VPC") and unit.find("HOUR") != -1:
                        estimated_days = np.ceil(float(quantity)/24)
                    else:
                        estimated_days = ""

                    if usage["discounts"] != []:
                        """
                        Discount found in usage record, convert to decimal
                        """
                        discount = usage['discounts'][0]['discount'] / 100
                    else:
                        """
                        No discount found set to zero
                        """
                        discount = 0

                    row_addition = {
                        "metric": metric,
                        "unit": unit,
                        "quantity": quantity,
                        "cost": cost,
                        "rated_cost": rated_cost,
                        "rateable_quantity": rateable_quantity,
                        "price": price,
                        "discount": discount,
                        "metric_name": metric_name,
                        'unit_name': unit_name,
                        'estimated_days': estimated_days
                    }

                    row = row | row_addition
                    data.append(row.copy())

            if nextoffset != "":
                if recordstart + limit > instances_usage["count"]:
                    recordstop = instances_usage["count"]
                else:
                    recordstop = recordstart + limit - 1
                logging.info("Requesting Instance {} Usage for {}: retrieving from {} to {} of Total {}".format(usageMonth, accountName, recordstart,
                                                                                              recordstop,
                                                                                              instances_usage["count"]))

                try:
                    instances_usage = usage_reports_service.get_resource_usage_account(
                        account_id=accountId,
                        billingmonth=usageMonth, names=True,limit=limit, start=nextoffset).get_result()
                except ApiException as e:
                    logging.error("Fatal Error with get_resource_usage_account: {}".format(e))
                    quit()

                if "next" in instances_usage:
                    nextoffset = instances_usage["next"]["offset"]
                else:
                    nextoffset = ""
            else:
                break


        instancesUsage = pd.DataFrame(data, columns=['account_id', "account_name", "month", "service_name", "service_id", "instance_name","instance_id", "plan_name", "plan_id", "region", "pricing_region",
                                                 "resource_group_name","resource_group_id", "billable", "pricing_country", "billing_country", "currency_code", "pricing_plan_id", "provision_date",
                                                 "instance_created_at", "instance_updated_at", "instance_deleted_at", "instance_state", "instance_profile", "cpu_family",
                                                 "numberOfVirtualCPUs", "MemorySizeMiB", "NodeName", "NumberOfGPUs", "NumberOfInstStorageDisks", "availability_zone", "BMnumberofCores", "BMnumberofSockets", "BMbandwidth",
                                                 "instance_role", "audit", "metric", "metric_name", "unit", "unit_name", "quantity", "cost", "rated_cost", "rateable_quantity", "estimated_days", "price", "discount"])

    return instancesUsage
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
            "resource_group_id": resource_instance["resource_group_id"],
            "region_id": resource_instance["region_id"],
            "name": resource_instance["name"]
        }

        if "created_at" in resource_instance:
            created_at = resource_instance["created_at"]
            """Create Provision Date Field using US East Timezone for Zulu conversion"""
            provisionDate = pd.to_datetime(created_at, format="%Y-%m-%dT%H:%M:%S.%f").astimezone(nytz)
            provisionDate = provisionDate.strftime("%Y-%m-%d")
        else:
            created_at = ""
            provisionDate = ""

        if "updated_at" in resource_instance:
            updated_at = resource_instance["updated_at"]
        else:
            updated_at = ""

        if "deleted_at" in resource_instance:
            deleted_at = resource_instance["deleted_at"]
            """Create deProvision Date Field using US East Timezone for Zulu conversion"""
            if deleted_at != None:
                deprovisionDate = pd.to_datetime(deleted_at, format="%Y-%m-%dT%H:%M:%S.%f").astimezone(nytz)
                deprovisionDate = deprovisionDate.strftime("%Y-%m-%d")
            else:
                deprovisionDate = ""
        else:
            deleted_at = ""
            deprovisionDate = ""

        if "state" in resource_instance:
            state = resource_instance["state"]
        else:
            state = ""

        az = ""
        region = ""
        profile = ""
        cpuFamily = ""
        numberOfVirtualCPUs = ""
        MemorySizeMiB = ""
        NodeName = ""
        NumberOfGPUs = ""
        NumberOfInstStorageDisks = ""
        NumberofCores = ""
        NumberofSockets = ""
        Bandwidth = ""
        LifecycleAction = ""
        Capacity = ""
        IOPS = ""

        if "extensions" in resource_instance:
            if "VirtualMachineProperties" in resource_instance["extensions"]:
                profile = resource_instance["extensions"]["VirtualMachineProperties"]["Profile"]
                cpuFamily = resource_instance["extensions"]["VirtualMachineProperties"]["CPUFamily"]
                numberOfVirtualCPUs = resource_instance["extensions"]["VirtualMachineProperties"]["NumberOfVirtualCPUs"]
                MemorySizeMiB = resource_instance["extensions"]["VirtualMachineProperties"]["MemorySizeMiB"]
                NodeName = resource_instance["extensions"]["VirtualMachineProperties"]["NodeName"]
                NumberOfGPUs = resource_instance["extensions"]["VirtualMachineProperties"]["NumberOfGPUs"]
                NumberOfInstStorageDisks = resource_instance["extensions"]["VirtualMachineProperties"][
                    "NumberOfInstStorageDisks"]

            elif "BMServerProperties" in resource_instance["extensions"]:
                profile = resource_instance["extensions"]["BMServerProperties"]["Profile"]
                MemorySizeMiB = resource_instance["extensions"]["BMServerProperties"]["MemorySizeMiB"]
                NodeName = resource_instance["extensions"]["BMServerProperties"]["NodeName"]
                NumberofCores = resource_instance["extensions"]["BMServerProperties"]["NumberOfCores"]
                NumberofSockets = resource_instance["extensions"]["BMServerProperties"]["NumberOfSockets"]
                Bandwidth = resource_instance["extensions"]["BMServerProperties"]["Bandwidth"]

            elif "VolumeInfo" in resource_instance["extensions"]:
                Capacity = resource_instance["extensions"]["VolumeInfo"]["Capacity"]
                IOPS = resource_instance["extensions"]["VolumeInfo"]["IOPS"]

            if "Resource" in resource_instance["extensions"]:
                if "AvailabilityZone" in resource_instance["extensions"]["Resource"]:
                    az = resource_instance["extensions"]["Resource"]["AvailabilityZone"]
                if "Location" in resource_instance["extensions"]["Resource"]:
                    region = resource_instance["extensions"]["Resource"]["Location"]["Region"]
                if "LifecycleAction" in resource_instance["extensions"]["Resource"]:
                    LifecycleAction = resource_instance["extensions"]["Resource"]["LifecycleAction"]

        # Lookup instance related detail
        numa_count = ""
        boot_volume_attachment = ""
        primary_network_interface_primary_ip = ""
        primary_network_interface_subnet = ""
        vpc= ""
        if resource_instance["resource_id"] == "is.instance":
            """ Check Cache """
            instance = getInstance(resource_instance["id"])
            if "numa_count" in instance:
                numa_count = instance["numa_count"]
            if "boot_volume_attachment" in instance:
                boot_volume_attachment = instance["boot_volume_attachment"]["name"]
            if "primary_network_interface" in instance:
                primary_network_interface_primary_ip = instance["primary_network_interface"]["primary_ip"]["address"]
                primary_network_interface_subnet = instance["primary_network_interface"]["subnet"]["name"]
            if "vpc" in instance:
                vpc = instance["vpc"]["name"]

        # get tags attached to instance from cache or resource controller
        tags = getTags(resource_instance["id"])
        # parse role tag into comma delimited list
        if len(tags) > 0:
            role = ",".join([str(item.split(":")[1]) for item in tags if "role:" in item])
            audit = ",".join([str(item.split(":")[1]) for item in tags if "audit:" in item])
        else:
            role = ""
            audit = ""

        row_addition = {
            "provision_date": provisionDate,
            "deprovision_date": deprovisionDate,
            "instance_created_at": created_at,
            "instance_updated_at": updated_at,
            "instance_deleted_at": deleted_at,
            "instance_state": state,
            "instance_profile": profile,
            "cpu_family": cpuFamily,
            "numa_count": numa_count,
            "boot_volume_attachment": boot_volume_attachment,
            "primary_network_interface_primary_ip": primary_network_interface_primary_ip,
            "primary_network_interface_subnet": primary_network_interface_subnet,
            "vpc": vpc,
            "numberOfVirtualCPUs": numberOfVirtualCPUs,
            "MemorySizeMiB": MemorySizeMiB,
            "NodeName": NodeName,
            "NumberOfGPUs": NumberOfGPUs,
            "NumberOfInstStorageDisks": NumberOfInstStorageDisks,
            "instance_role": role,
            "audit": audit,
            "availability_zone": az,
            "region": region,
            "lifecycleAction": LifecycleAction,
            "BMnumberofCores": NumberofCores,
            "BMnumberofSockets": NumberofSockets,
            "BMbandwidth": Bandwidth,
            "capacity": Capacity,
            "iops": IOPS
        }

        # combine original row with additions

        row = row | row_addition
        data.append(row.copy())


    resourceDetail = pd.DataFrame(data, columns=['account_id', "account_name", "service_id", "instance_id", "name", "resource_group_id", "region_id",
                                                 "provision_date", "deprovision_date", "instance_created_at", "instance_updated_at", "instance_deleted_at",
                                                 "instance_state", "lifecycleAction",  "instance_profile", "cpu_family", "numa_count", "boot_volume_attachment", "primary_network_interface_subnet",
                                                 "primary_network_interface_primary_ip", "vpc", "numberOfVirtualCPUs", "MemorySizeMiB", "NodeName", "NumberOfGPUs",
                                                 "NumberOfInstStorageDisks", "region", "availability_zone", "BMnumberofCores", "BMnumberofSockets", "BMbandwidth", "capacity", "iops",
                                                 "instance_role", "audit"])
    return resourceDetail
def createMissingCRNTab(paasUsage):
    """
    Write Service Usage detail tab to excel
    """
    logging.info("Creating Server Detail tab.")

    paasUsage.to_excel(writer, "MissingCRNs")
    worksheet = writer.sheets['MissingCRNs']
    totalrows,totalcols=paasUsage.shape
    worksheet.autofilter(0,0,totalrows,totalcols)
    return
def createInstanceUsageTab(instancesUsage):
    """
    Write detail tab to excel
    """
    logging.info("Creating Instance Usage detail tab.")

    instancesUsage.to_excel(writer, "Instance_Usage")
    worksheet = writer.sheets['Instance_Usage']
    format1 = workbook.add_format({'num_format': '$#,##0.00'})
    format2 = workbook.add_format({'align': 'left'})
    worksheet.set_column("A:C", 12, format2)
    worksheet.set_column("D:E", 25, format2)
    worksheet.set_column("F:G", 18, format1)
    worksheet.set_column("H:I", 25, format2)
    worksheet.set_column("J:J", 18, format1)
    totalrows,totalcols=instancesUsage.shape
    worksheet.autofilter(0,0,totalrows,totalcols)
    return
def createResourceControllerTab(resources):
    """
    Write detail tab to excel
    """
    logging.info("Creating Resource Controller detail tab.")

    resources.to_excel(writer, "Resource_Controller")
    worksheet = writer.sheets['Resource_Controller']
    format1 = workbook.add_format({'num_format': '$#,##0.00'})
    format2 = workbook.add_format({'align': 'left'})
    totalrows,totalcols=resources.shape
    worksheet.autofilter(0,0,totalrows,totalcols)
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
    parser = argparse.ArgumentParser(description="Search all accounts for Server Items missing from Usage Reporting")
    parser.add_argument("--output", default=os.environ.get('output', 'missingCRNs.xlsx'), help="Filename Excel output file. (including extension of .xlsx)")
    parser.add_argument("--debug", action=argparse.BooleanOptionalAction, help="Set Debug level for logging.")
    parser.add_argument("--start", help="Start Month YYYY-MM.")
    parser.add_argument("--end", help="End Month YYYY-MM.")
    parser.add_argument("--month", help="Report Month YYYY-MM.")
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

    if args.month != None:
        start = datetime.strptime(args.month, "%Y-%m")
        end = datetime.strptime(args.month, "%Y-%m")
    else:
        start = datetime.strptime(args.start, "%Y-%m")
        end = datetime.strptime(args.end, "%Y-%m")

    APIKEYS = os.environ.get('APIKEYS', None)
    if APIKEYS == None:
        logging.error("You must specify apikey and name for each account in .env file or APIKEYS environment variable.")
        quit()
    else:
        accountUsage = pd.DataFrame()
        resources = pd.DataFrame()
        """ Convert to List of JSON variable """
        try:
            APIKEYS = json.loads(APIKEYS)
        except ValueError as e:
            logging.error("Invalid List of APIKEYS.")
            quit()

        """ Read all usage for PKL file if specified otherwise load via api"""
        if args.load:
            citiUsage = pd.read_pickle("../citiUsage.pkl")
            resources_df = pd.read_pickle("../resource_df.pkl")
            tags_df = pd.read_pickle("../tags.pkl")
        else:
            citiUsage = pd.DataFrame()
            resources_df = pd.DataFrame()
            tags_df = pd.DataFrame()

        if not args.load:
            for account in APIKEYS:
                if "apikey" in account:
                    apikey = account["apikey"]
                    createSDK(apikey)
                    accountId = getAccountId(apikey)
                    if "name" in account:
                        accountName = account["name"]

                        """ Get all Tag Data to match to resources and usage data """
                        logging.info("Caching Tag Data for {} AccountId: {}.".format(accountName, accountId))
                        tag_cache, df = populateTagCache()
                        tags_df = pd.concat([tags_df, df])

                        """ Get all resource controller instances for account """
                        logging.info("Caching Current Controller Data for {} AccountId: {}.".format(accountName, accountId))
                        resource_cache, df = prePopulateResourceCache(accountName, accountId)
                        resources_df = pd.concat([resources_df, df])

                        """Get Usage Data for account for range of months"""
                        instanceUsage = getInstancesUsage(start, end)
                        citiUsage = pd.concat([citiUsage, instanceUsage])

                    else:
                        logging.error("No Name for Account found.")
                else:
                    logging.error("No APIKEY found.")
                    quit()

        """ Check for missing billing records """
        logging.info("Searching for missing billing records for all accounts.")
        missing = pd.DataFrame()
        for index, record in resources_df.iterrows():
            if "resource_id" in record:
                if record["resource_id"] == "is.instance" or record["resource_id"] == "is.bare-metal-server":
                    id = record["id"]
                    try:
                        """ parse with miroseconds"""
                        created_at = datetime.strptime(record["created_at"],'%Y-%m-%dT%H:%M:%S.%fZ')
                    except:
                        """ parse without microseconds """
                        created_at = datetime.strptime(record["created_at"],'%Y-%m-%dT%H:%M:%SZ')

                    if created_at < start:
                        startmonth = start
                    else:
                        startmonth = created_at.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

                    while startmonth <= end:
                        usageMonth = startmonth.strftime("%Y-%m")
                        startmonth += relativedelta(months=+1)
                        """ Check if server is missing in this months usage data """
                        servers = citiUsage.query('instance_id == @id and month == @usageMonth')
                        if servers["instance_id"].count() == 0:
                            tags = tags_df.query('crn == @id')
                            if tags["tags"].count() != 0:
                                tag = tags.iloc[0]["tags"]
                                role = ",".join([str(item.split(":")[1]) for item in tag if "role:" in item])
                            else:
                                role = ""
                            newrow = record.to_dict()
                            newrow["role"] = role
                            newrow["month"] = usageMonth
                            logging.error("{} {} was not found in {} usage data.".format(record["name"],id, usageMonth))
                            missing = pd.concat([missing, pd.DataFrame([newrow], columns=newrow.keys(), index=["id"] )])
            else:
                logging.error("No resource_id foumd.")
                quit()

        """
        Save Datatables for report generation testing (use --LOAD to reload without API pull)
        """
        if args.save:
            citiUsage.to_pickle("citiUsage.pkl")
            resources_df.to_pickle("resource_df.pkl")
            tags_df.to_pickle("tags.pkl")

        # Write dataframe to excel
        output = args.output
        split_tup = os.path.splitext(args.output)
        """ remove file extension """
        file_name = split_tup[0]
        timestamp = "_(run@{})".format(datetime.now().strftime("%Y-%m-%d_%H:%M"))

        writer = pd.ExcelWriter(file_name + ".xlsx", engine='xlsxwriter')
        workbook = writer.book
        createInstanceUsageTab(citiUsage)
        createResourceControllerTab(resources_df)
        createMissingCRNTab(missing)
        writer.close()
    """ If --COS then copy files with report end month + timestamp to COS """
    if args.cos:
        """ Write output to COS"""
        logging.info("Writing Pivot Tables to COS.")
        writeFiletoCos(file_name + ".xlsx", file_name + timestamp + ".xlsx")

    logging.info("Current non billed server report is complete.")
