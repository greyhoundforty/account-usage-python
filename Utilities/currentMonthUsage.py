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
def listAllResourceInstances(resource_type):
    """
    Retrieve all Resources for account from resource controller fore resource_type
    """
    all_results = []
    pager = ResourceInstancesPager(
        client=resource_controller_service,
        resource_id=resource_type,
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
        numberOfVirtualCPUs = 0
        MemorySizeMiB = 0
        NodeName = ""
        NumberOfGPUs = 0
        NumberOfInstStorageDisks = ""
        NumberofCores = 0
        NumberofSockets = 0
        Bandwidth = ""
        OSName = ""
        OSVendor = ""
        OSVersion =""
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
                OSName = resource_instance["extensions"]["VirtualMachineProperties"]["OSName"]
                OSVendor = resource_instance["extensions"]["VirtualMachineProperties"]["OSVendor"]
                OSVersion = resource_instance["extensions"]["VirtualMachineProperties"]["OSVersion"]

            elif "BMServerProperties" in resource_instance["extensions"]:
                profile = resource_instance["extensions"]["BMServerProperties"]["Profile"]
                MemorySizeMiB = resource_instance["extensions"]["BMServerProperties"]["MemorySizeMiB"]
                NodeName = resource_instance["extensions"]["BMServerProperties"]["NodeName"]
                NumberofCores = resource_instance["extensions"]["BMServerProperties"]["NumberOfCores"]
                NumberofSockets = resource_instance["extensions"]["BMServerProperties"]["NumberOfSockets"]
                Bandwidth = resource_instance["extensions"]["BMServerProperties"]["Bandwidth"]
                OSName = resource_instance["extensions"]["BMServerProperties"]["OSName"]
                OSVendor = resource_instance["extensions"]["BMServerProperties"]["OSVendor"]
                OSVersion = resource_instance["extensions"]["BMServerProperties"]["OSVersion"]

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

        # Lookup instance related detaail from VPC cache
        numa_count = ""
        boot_volume_attachment = ""
        primary_network_interface_primary_ip = ""
        primary_network_interface_subnet = ""
        vpc= ""
        total_network_bandwidth = ""
        total_volume_bandwidth = ""
        if resource_instance["resource_id"] == "is.instance" or resource_instance["resource_id"] == "is.bare-metal-server":
            """ Check VPC Cache """
            instance = getInstance(resource_instance["id"])
            if "numa_count" in instance:
                numa_count = instance["numa_count"]
            if "boot_volume_attachment" in instance:
                boot_volume_attachment = instance["boot_volume_attachment"]["name"]
            if "primary_network_interface" in instance:
                primary_network_interface_primary_ip = instance["primary_network_interface"]["primary_ip"]["address"]
                primary_network_interface_subnet = instance["primary_network_interface"]["subnet"]["name"]
            if "total_network_bandwidth" in instance:
                total_network_bandwidth = instance["total_network_bandwidth"]
            if "total_volume_bandwidth" in instance:
                total_volume_bandwidth = instance["total_volume_bandwidth"]
            if "vpc" in instance:
                vpc = instance["vpc"]["name"]

        bm_disks = ""
        BMRawStorage = ""
        if resource_instance["resource_id"] == "is.bare-metal-server":
            if "disks" in instance:
                bm_disks = len(instance["disks"])
                BMRawStorage = 0
                for storage in instance["disks"]:
                    if storage["interface_type"] == "nvme":
                        BMRawStorage = BMRawStorage + float(storage["size"])

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
            "boot_volume_attachment": boot_volume_attachment,
            "region": region,
            "vpc": vpc,
            "availability_zone": az,
            "primary_network_interface_subnet": primary_network_interface_subnet,
            "primary_network_interface_primary_ip": primary_network_interface_primary_ip,
            "numberOfVirtualCPUs": numberOfVirtualCPUs,
            "MemorySizeMiB": MemorySizeMiB,
            "numa_count": numa_count,
            "total_network_bandwidth": total_network_bandwidth ,
            "total_volume_bandwidth": total_volume_bandwidth ,
            "NodeName": NodeName,
            "NumberOfGPUs": NumberOfGPUs,
            "NumberOfInstStorageDisks": NumberOfInstStorageDisks,
            "lifecycleAction": LifecycleAction,
            "BMnumberofCores": NumberofCores,
            "BMnumberofSockets": NumberofSockets,
            "BMbandwidth": Bandwidth,
            "BMDisks": bm_disks,
            "BMRawStorage": BMRawStorage,
            "OSName": OSName,
            "OSVendor": OSVendor,
            "OSVersion": OSVersion,
            "capacity": Capacity,
            "iops": IOPS,
            "instance_role": role,
            "audit": audit,
        }

        # combine original row with additions

        row = row | row_addition
        data.append(row.copy())

    resourceDetail = pd.DataFrame(data, columns=['account_id', "account_name", "service_id", "instance_id", "name", "resource_group_id", "region_id",
                                                 "provision_date", "deprovision_date", "instance_created_at", "instance_updated_at", "instance_deleted_at",
                                                 "instance_state", "lifecycleAction",  "instance_profile", "cpu_family", "boot_volume_attachment",
                                                 "region", "vpc", "availability_zone", "primary_network_interface_subnet", "primary_network_interface_primary_ip",
                                                 "numberOfVirtualCPUs", "MemorySizeMiB", "numa_count", "total_network_bandwidth", "total_volume_bandwidth", "NodeName", "NumberOfGPUs",
                                                 "NumberOfInstStorageDisks", "BMnumberofCores", "BMnumberofSockets", "BMbandwidth", "BMDisks", "BMRawStorage", "capacity", "iops",
                                                 "OSName", "OSVendor", "OSVersion", "instance_role", "audit"])
    return resourceDetail

def createServerListTab(paasUsage):
    """
    Write Service Usage detail tab to excel
    """
    logging.info("Creating Server Detail tab.")

    paasUsage.to_excel(writer, "ServerDetail")
    worksheet = writer.sheets['ServerDetail']
    totalrows,totalcols=paasUsage.shape
    worksheet.autofilter(0,0,totalrows,totalcols)
    return

def createWorkerVcpuTab(instancesUsage):
    """
    Create VCPU deployed by role, account, and az
    """

    logging.info("Calculating Virtual Server vCPU deployed.")

    servers = instancesUsage.query('service_id == "is.instance"  and instance_role.str.contains("symphony-worker")')
    vcpu = pd.pivot_table(servers, index=["account_name",  "region", "availability_zone", "instance_role"],
                                    values=["instance_id", "numberOfVirtualCPUs"],
                                    aggfunc={"instance_id": "nunique", "numberOfVirtualCPUs": np.sum},
                                    margins=True, margins_name="Total",
                                    fill_value=0).rename(columns={'instance_id': 'instance_count'})

    new_order = ["instance_count", "numberOfVirtualCPUs"]
    vcpu = vcpu.reindex(new_order, axis=1)
    vcpu.to_excel(writer, 'SymphonyWorkerVCPU')
    worksheet = writer.sheets['SymphonyWorkerVCPU']
    format2 = workbook.add_format({'align': 'left'})
    format3 = workbook.add_format({'num_format': '#,##0'})
    worksheet.set_column("A:D", 30, format2)
    worksheet.set_column("E:F", 18, format3)
    return
def createScaleCpuTab(instancesUsage):
    """
    Create BM VCPU deployed by role, account, and az
    """

    logging.info("Calculating Bare Metal vCPU deployed.")
    servers = instancesUsage.query('service_id == "is.bare-metal-server" and instance_role.str.contains("scale-storage")')
    vcpu = pd.pivot_table(servers, index=["account_name", "region", "availability_zone", "instance_role"],
                                    values=["instance_id", "BMnumberofCores", "BMnumberofSockets"],
                                    aggfunc={"instance_id": "nunique", "BMnumberofCores": np.sum, "BMnumberofSockets": np.sum},
                                    margins=True, margins_name="Total",
                                    fill_value=0).rename(columns={'instance_id': 'instance_count', "BMnumberofCores": "Cores", "BMnumberofSockets": "Sockets"})

    new_order = ["instance_count", "Cores", "Sockets"]
    vcpu = vcpu.reindex(new_order, axis=1)
    vcpu.to_excel(writer, 'ScaleBareMetalCores')
    worksheet = writer.sheets['ScaleBareMetalCores']
    format2 = workbook.add_format({'align': 'left'})
    format3 = workbook.add_format({'num_format': '#,##0'})
    worksheet.set_column("A:D", 30, format2)
    worksheet.set_column("E:G", 18, format3)
    return
def createProvisionAllTab(instancesUsage):
    """
    Create Pivot by Original Provision Date
    """

    logging.info("Calculating vCPU by provision date.")
    servers = instancesUsage.query('(service_id == "is.instance") or (service_id == "is.bare-metal-server")')

    vcpu = pd.pivot_table(servers, index=["audit","account_name", "region", "availability_zone", "instance_role", "instance_profile", "provision_date", "deprovision_date"],
                                    values=["instance_id", "numberOfVirtualCPUs"],
                                    aggfunc={"instance_id": "nunique", "numberOfVirtualCPUs": np.sum},
                                    fill_value=0).rename(columns={'instance_id': 'instance_count'})

    new_order = ["instance_count", "numberOfVirtualCPUs"]
    vcpu = vcpu.reindex(new_order, axis=1)
    #vcpu = vcpu.reset_index()
    vcpu.to_excel(writer, 'ProvisionDateAllRoles')
    worksheet = writer.sheets['ProvisionDateAllRoles']
    format2 = workbook.add_format({'align': 'left'})
    format3 = workbook.add_format({'num_format': '#,##0'})
    worksheet.set_column("A:G", 30, format2)
    worksheet.set_column("H:J", 18, format3)
    return
def createProvisionScaleTab(instancesUsage):
    """
    Create Pivot by Of Scale Servers by Date
    """

    logging.info("Calculating vCPU by provision date scale storage nodes only.")
    servers = instancesUsage.query(
        'service_id == "is.bare-metal-server" and instance_role.str.contains("scale-storage")')

    vcpu = pd.pivot_table(servers, index=["account_name", "region", "availability_zone", "instance_role", "instance_profile", "provision_date", "deprovision_date"],
                                    values=["instance_id", "BMnumberofCores", "BMnumberofSockets"],
                                    aggfunc={"instance_id": "nunique", "BMnumberofCores": np.sum, "BMnumberofSockets": np.sum},
                                    margins=True, margins_name="Total",
                                    fill_value=0).rename(columns={'instance_id': 'instance_count',  "BMnumberofCores": "Cores", "BMnumberofSockets": "Sockets"})

    new_order = ["instance_count", "Cores", "Sockets"]
    vcpu = vcpu.reindex(new_order, axis=1)
    #vcpu = vcpu.reset_index()
    vcpu.to_excel(writer, 'ProvisionDateScaleRole')
    worksheet = writer.sheets['ProvisionDateScaleRole']
    format2 = workbook.add_format({'align': 'left'})
    format3 = workbook.add_format({'num_format': '#,##0'})
    worksheet.set_column("A:F", 30, format2)
    worksheet.set_column("G:I", 18, format3)
    return
def createProvisionWorkersTab(instancesUsage):
    """
    Create Pivot by Original Provision Date
    """

    logging.info("Calculating vCPU by provision date symphony-workers only.")
    servers = instancesUsage.query('service_id == "is.instance" and instance_role.str.contains("symphony-worker")')

    vcpu = pd.pivot_table(servers, index=["account_name", "region", "availability_zone", "instance_role", "instance_profile", "provision_date", "deprovision_date"],
                                    values=["instance_id", "numberOfVirtualCPUs"],
                                    aggfunc={"instance_id": "nunique", "numberOfVirtualCPUs": np.sum},
                                    margins=True, margins_name="Total",
                                    fill_value=0).rename(columns={'instance_id': 'instance_count'})

    new_order = ["instance_count", "numberOfVirtualCPUs"]
    vcpu = vcpu.reindex(new_order, axis=1)
    vcpu.to_excel(writer, 'ProvisionDateWorkerRole')
    worksheet = writer.sheets['ProvisionDateWorkerRole']
    format2 = workbook.add_format({'align': 'left'})
    format3 = workbook.add_format({'num_format': '#,##0'})
    worksheet.set_column("A:F", 30, format2)
    worksheet.set_column("G:I", 18, format3)
    return
def createUsageSummaryTab(paasUsage):
    logging.info("Creating Usage Summary tab.")
    usageSummary = pd.pivot_table(paasUsage, index=["account_name", "resource_name"],
                                    values=["cost"],
                                    aggfunc=np.sum, margins=True, margins_name="Total",
                                    fill_value=0)

    usageSummary.to_excel(writer, 'UsageSummary', startcol=0, startrow=2)
    worksheet = writer.sheets['UsageSummary']
    boldtext = workbook.add_format({'bold': True, 'bg_color': '#FFFF00'})
    worksheet.write(0, 0, "WARNING: Month to date usage up to {}".format(datetime.now().strftime("%Y-%m-%d @ %H:%M")), boldtext)
    format1 = workbook.add_format({'num_format': '$#,##0.00'})
    format2 = workbook.add_format({'align': 'left'})
    worksheet.set_column("A:A", 60, format2)
    worksheet.set_column("B:B", 35, format2)
    worksheet.set_column("C:J", 18, format1)
def createMetricSummary(paasUsage):
    logging.info("Creating Metric Plan Summary tab.")
    metricSummaryPlan = pd.pivot_table(paasUsage, index=["account_name", "resource_name", "plan_name", "metric"],
                                 values=["rateable_quantity", "cost"],
                                 aggfunc=np.sum, margins=True, margins_name="Total",
                                 fill_value=0)
    new_order = ["rateable_quantity", "cost"]
    metricSummaryPlan = metricSummaryPlan.reindex(new_order, axis=1, level=0)
    metricSummaryPlan.to_excel(writer, 'MetricPlanSummary', startcol=0, startrow=2)
    worksheet = writer.sheets['MetricPlanSummary']
    boldtext = workbook.add_format({'bold': True, 'bg_color': '#FFFF00'})
    worksheet.write(0, 0, "WARNING: Month to date usage up to {}".format(datetime.now().strftime("%Y-%m-%d @ %H:%M")), boldtext)
    format1 = workbook.add_format({'num_format': '$#,##0.00'})
    format2 = workbook.add_format({'align': 'left'})
    format3 = workbook.add_format({'num_format': '#,##0.00000'})
    worksheet.set_column("A:A", 60, format2)
    worksheet.set_column("B:B", 40, format2)
    worksheet.set_column("C:C", 40, format2)
    worksheet.set_column("D:D", 40, format2)
    worksheet.set_column("E:E", 30, format3)
    worksheet.set_column("F:ZZ", 15, format1)
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
    parser = argparse.ArgumentParser(description="Calculate Citi Usage Month to Date.")
    parser.add_argument("--output", default=os.environ.get('output', 'currentMonthUsage.xlsx'), help="Filename Excel output file. (including extension of .xlsx)")
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
                        logging.info("Caching VPC Instance Data for {} AccountId: {}.".format(accountName, accountId))
                        instance_cache = populateInstanceCache()
                        logging.info("Retrieving Month to Date Account Usage for {}: {}.".format(accountName, accountId))
                        accountUsage = pd.concat([accountUsage, getCurrentMonthAccountUsage()])
                        logging.info("Retrieving current list of Virtual Servers from {} AccountId: {}.".format(accountName, accountId))
                        resources = pd.concat([resources, parseResources(accountName, listAllResourceInstances("is.instance"))])
                        logging.info("Retrieving current list of Bare Metal Servers from {} AccountId: {}.".format(accountName, accountId))
                        resources = pd.concat([resources, parseResources(accountName, listAllResourceInstances("is.bare-metal-server"))])
                    else:
                        logging.error("No Name for Account found.")
                else:
                    logging.error("No APIKEY found.")
                    quit(1)
    else:
        logging.info("Retrieving Usage and Instance data from stored data file")
        accountUsage = pd.read_pickle("accountUsage.pkl")
        resources = pd.read_pickle("resources.pkl")

    if args.save:
        accountUsage.to_pickle("accountUsage.pkl")
        resources.to_pickle("resources.pkl")

    # Write dataframe to excel
    output = args.output
    split_tup = os.path.splitext(args.output)
    """ remove file extension """
    file_name = split_tup[0]
    timestamp = "_(run@{})".format(datetime.now().strftime("%Y-%m-%d_%H:%M"))
    writer = pd.ExcelWriter(file_name + ".xlsx", engine='xlsxwriter')
    workbook = writer.book
    createUsageSummaryTab(accountUsage)
    createMetricSummary(accountUsage)
    createWorkerVcpuTab(resources)
    createScaleCpuTab(resources)
    createProvisionAllTab(resources)
    createProvisionWorkersTab(resources)
    createProvisionScaleTab(resources)
    createServerListTab(resources)
    writer.close()

    """ If --COS then copy files with report end month + timestamp to COS """
    if args.cos:
        """ Write output to COS"""
        logging.info("Writing Pivot Tables to COS.")
        writeFiletoCos(file_name + ".xlsx", file_name + timestamp + ".xlsx")
    logging.info("Current Server Resource Report is complete.")
