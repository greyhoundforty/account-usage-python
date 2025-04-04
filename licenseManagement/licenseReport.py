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

import os, logging, logging.config, os.path, argparse
import pandas as pd
import numpy as np
import ibm_boto3
import pysftp
from ibm_botocore.client import Config, ClientError
from ibm_platform_services import IamIdentityV1, UsageReportsV4, GlobalSearchV2
from ibm_cloud_sdk_core import ApiException
from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
from ibm_platform_services.resource_controller_v2 import *
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


def populateVPCInstanceCache():
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


def getResourcesFromController():
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
    except ApiException as e:
        logging.error(
            "API Error.  Can not retrieve resources. {}: {}".format(str(e.code), e.message))
        quit(1)

    resources_df = pd.DataFrame.from_dict(all_results)
    resource_cache = {}
    for resource in all_results:
        resourceId = resource["crn"]
        resource_cache[resourceId] = resource

    return resource_cache, resources_df


def getResourceInstanceCache(resourceId):
    """
    Check Cache for Resource Details which may have been retrieved previously
    """
    if resourceId not in resource_cache:
        logging.error("Cache miss for Resource {}".format(resourceId))
        quit(1)

    return resource_cache[resourceId]


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


def parseResources(accountName, resources):
    """
    Parse Resource datafrrame
    """
    global resource_cache
    data = []
    for index, resource_instance in resources.iterrows():
        logging.debug(resource_instance)
        if resource_instance["resource_id"] == "is.instance" or resource_instance["resource_id"] == "is.bare-metal-server":
            row = {
                "account_id": resource_instance["account_id"],
                "account_name": accountName,
                "service_id": resource_instance["resource_id"],
                "instance_id": resource_instance["id"],
                "resource_group_id": resource_instance["resource_group_id"],
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

            if "deleted_at" in resource_instance:
                deleted_at = resource_instance["deleted_at"]
            else:
                deleted_at = ""

            if "state" in resource_instance:
                state = resource_instance["state"]
            else:
                state = ""

            """
            Do not include servers in failed state with license data
            Break out of for loop and don't write record to datatable
            because data is incomplete, and resource doesn't exist
            Write a warning to logfile for traceability 
            """

            if state == "failed":
                logging.warning("GUID {} is in failed state.  Excluding from license data.".format(resource_instance["id"]))
                break

            az = ""
            region = ""
            profile = ""
            numberOfVirtualCPUs = ""
            MemorySizeMiB = ""
            NodeName = ""
            NumberOfGPUs = ""
            NumberofCores = ""
            NumberofSockets = ""
            ThreadsPerCore = ""
            Bandwidth = ""
            OSName = ""
            OSVendor = ""
            OSVersion = ""
            LifecycleAction = ""
            numa_count = ""
            primary_network_interface_primary_ip = ""
            primary_network_interface_subnet = ""
            vpc = ""
            total_network_bandwidth = ""
            total_volume_bandwidth = ""
            BMRawStorage = ""
            numAttachedDataVolumes = ""
            totalDataVolumeCapacity = ""
            attachedDataVolumeDetail = ""
            boot_volume_attachment = ""
            boot_volume_capacity = ""

            """ Get extension data from resource controller """
            if "extensions" in resource_instance:
                if "VirtualMachineProperties" in resource_instance["extensions"]:
                    profile = resource_instance["extensions"]["VirtualMachineProperties"]["Profile"]
                    numberOfVirtualCPUs = resource_instance["extensions"]["VirtualMachineProperties"]["NumberOfVirtualCPUs"]
                    MemorySizeMiB = resource_instance["extensions"]["VirtualMachineProperties"]["MemorySizeMiB"]
                    NodeName = resource_instance["extensions"]["VirtualMachineProperties"]["NodeName"]
                    NumberOfGPUs = resource_instance["extensions"]["VirtualMachineProperties"]["NumberOfGPUs"]
                    OSName = resource_instance["extensions"]["VirtualMachineProperties"]["OSName"]
                    OSVendor = resource_instance["extensions"]["VirtualMachineProperties"]["OSVendor"]
                    OSVersion = resource_instance["extensions"]["VirtualMachineProperties"]["OSVersion"]

                elif "BMServerProperties" in resource_instance["extensions"]:
                    profile = resource_instance["extensions"]["BMServerProperties"]["Profile"]
                    MemorySizeMiB = resource_instance["extensions"]["BMServerProperties"]["MemorySizeMiB"]
                    NodeName = resource_instance["extensions"]["BMServerProperties"]["NodeName"]
                    NumberofCores = float(resource_instance["extensions"]["BMServerProperties"]["NumberOfCores"])
                    NumberofSockets = float(resource_instance["extensions"]["BMServerProperties"]["NumberOfSockets"])
                    Bandwidth = resource_instance["extensions"]["BMServerProperties"]["Bandwidth"]
                    OSName = resource_instance["extensions"]["BMServerProperties"]["OSName"]
                    OSVendor = resource_instance["extensions"]["BMServerProperties"]["OSVendor"]
                    OSVersion = resource_instance["extensions"]["BMServerProperties"]["OSVersion"]

                if "Resource" in resource_instance["extensions"]:
                    if "AvailabilityZone" in resource_instance["extensions"]["Resource"]:
                        az = resource_instance["extensions"]["Resource"]["AvailabilityZone"]
                    if "Location" in resource_instance["extensions"]["Resource"]:
                        region = resource_instance["extensions"]["Resource"]["Location"]["Region"]
                        city = ""
                        state = ""
                        country = ""
                        if region == "us-south":
                            city = "Dallas"
                            stateprov = "Texas"
                            country = "United States"
                        elif region == "us-east":
                            city = "Ashburn"
                            stateprov = "Virginia"
                            country = "United States"
                        elif region == "ca-tor":
                            city = "Toronto"
                            stateprov = "Ontario"
                            country = "Canada"
                    if "LifecycleAction" in resource_instance["extensions"]["Resource"]:
                        LifecycleAction = resource_instance["extensions"]["Resource"]["LifecycleAction"]

            """ Check VPC Cache for details not stored in resource controller """
            vpcinstance = getInstance(resource_instance["id"])
            bootVolumeCRN = ""
            if "boot_volume_attachment" in vpcinstance:
                bootVolumeCRN = vpcinstance["boot_volume_attachment"]["volume"]["crn"]
                """ Get cached resource controller data for volume """
                resourceDetail = getResourceInstanceCache(bootVolumeCRN)
                if "extensions" in resourceDetail:
                    bootCapacity = resourceDetail["extensions"]["VolumeInfo"]["Capacity"]
                    bootIops = resourceDetail["extensions"]["VolumeInfo"]["IOPS"]
                    boot_volume_capacity = float(resourceDetail["extensions"]["VolumeInfo"]["Capacity"])
                else:
                    bootCapacity = "100"
                    bootIops = "3000"
                    boot_volume_capacity = 100

                boot_volume_attachment = {
                    "id": vpcinstance["boot_volume_attachment"]["id"],
                    "name": vpcinstance["boot_volume_attachment"]["name"],
                    "capacity": bootCapacity,
                    "iops": bootIops
                }

            if "volume_attachments" in vpcinstance:
                numAttachedDataVolumes = len(vpcinstance["volume_attachments"]) - 1
                totalDataVolumeCapacity = 0
                attachedDataVolumeDetail = []
                for volume in vpcinstance["volume_attachments"]:
                    volumerow = {}
                    volumeCRN = volume["volume"]["crn"]
                    volumerow["name"] = volume["volume"]["name"]
                    volumerow["id"] = volume["volume"]["id"]
                    """ Ignore if Boot Volume """
                    if bootVolumeCRN != volumeCRN:
                        """ Lookup Volume by CRN from Cache """
                        resourceDetail = getResourceInstanceCache(volumeCRN)
                        if "extensions" in resourceDetail:
                            if "VolumeInfo" in resourceDetail["extensions"]:
                                if "Capacity" in resourceDetail["extensions"]["VolumeInfo"]:
                                    volumerow["capacity"] = resourceDetail["extensions"]["VolumeInfo"]["Capacity"]
                                    totalDataVolumeCapacity = totalDataVolumeCapacity + float(volumerow["capacity"])
                                if "IOPS" in resourceDetail["extensions"]["VolumeInfo"]:
                                    volumerow["iops"] = resourceDetail["extensions"]["VolumeInfo"]["IOPS"]
                        attachedDataVolumeDetail.append(volumerow)

            if "numa_count" in vpcinstance:
                numa_count = vpcinstance["numa_count"]

            if "primary_network_interface" in vpcinstance:
                primary_network_interface_primary_ip = vpcinstance["primary_network_interface"]["primary_ip"]["address"]
                primary_network_interface_subnet = vpcinstance["primary_network_interface"]["subnet"]["name"]
            if "total_network_bandwidth" in vpcinstance:
                total_network_bandwidth = vpcinstance["total_network_bandwidth"]
            if "total_volume_bandwidth" in vpcinstance:
                total_volume_bandwidth = vpcinstance["total_volume_bandwidth"]
            if "vpc" in vpcinstance:
                vpc = vpcinstance["vpc"]["name"]

            if resource_instance["resource_id"] == "is.bare-metal-server":
                if "cpu" in vpcinstance:
                    ThreadsPerCore = float(vpcinstance["cpu"]["threads_per_core"])
                if "disks" in vpcinstance:
                    BMRawStorage = 0
                    for storage in vpcinstance["disks"]:
                        if storage["interface_type"] == "nvme":
                            BMRawStorage = BMRawStorage + float(storage["size"])

            # get tags attached to vpcinstance from cache or resource controller
            tags = getTags(resource_instance["id"])
            # parse role tag into comma delimited list
            if len(tags) > 0:
                role = ",".join([str(item.split(":")[1]) for item in tags if "role:" in item])
                audit = ",".join([str(item.split(":")[1]) for item in tags if "audit:" in item])
            else:
                role = ""
                audit = ""

            row_addition = {
                "instance_created_at": created_at,
                "instance_updated_at": updated_at,
                "instance_deleted_at": deleted_at,
                "instance_state": state,
                "instance_profile": profile,
                "region": region,
                "city": city,
                "stateprov": stateprov,
                "country": country,
                "vpc": vpc,
                "availability_zone": az,
                "primary_network_interface_subnet": primary_network_interface_subnet,
                "primary_network_interface_primary_ip": primary_network_interface_primary_ip,
                "numberOfVirtualCPUs": numberOfVirtualCPUs,
                "MemorySizeMiB": MemorySizeMiB,
                "numa_count": numa_count,
                "total_network_bandwidth": total_network_bandwidth,
                "total_volume_bandwidth": total_volume_bandwidth,
                "bootVolumeCapacity": boot_volume_capacity,
                "bootVolumeAttachment": boot_volume_attachment,
                "numAttachedDataVolumes": numAttachedDataVolumes,
                "totalDataVolumeCapacity": totalDataVolumeCapacity,
                "attachedDataVolumes": attachedDataVolumeDetail,
                "NodeName": NodeName,
                "NumberOfGPUs": NumberOfGPUs,
                "lifecycleAction": LifecycleAction,
                "BMnumberofCores": NumberofCores,
                "BMnumberofSockets": NumberofSockets,
                "BMThreadsPerCore": ThreadsPerCore,
                "BMbandwidth": Bandwidth,
                "BMRawStorage": BMRawStorage,
                "OSName": OSName,
                "OSVendor": OSVendor,
                "OSVersion": OSVersion,
                "instance_role": role,
                "audit": audit,
            }

            # combine original row with additions

            row = row | row_addition
            data.append(row.copy())

    resourceDetail = pd.DataFrame(data, columns=['account_id', "account_name", "service_id", "instance_id", "name",
                                                 "resource_group_id", "instance_created_at",
                                                 "instance_updated_at", "instance_deleted_at",
                                                 "instance_state", "lifecycleAction", "instance_profile",
                                                 "region", "city", "stateprov", "country", "vpc", "availability_zone", "primary_network_interface_subnet",
                                                 "primary_network_interface_primary_ip",
                                                 "numberOfVirtualCPUs", "MemorySizeMiB", "numa_count",
                                                 "total_network_bandwidth", "total_volume_bandwidth", "NodeName",
                                                 "NumberOfGPUs", "bootVolumeCapacity", "bootVolumeAttachment",
                                                 "numAttachedDataVolumes", "totalDataVolumeCapacity", "attachedDataVolumes", "BMnumberofCores",
                                                 "BMnumberofSockets", "BMThreadsPerCore", "BMbandwidth",
                                                 "BMRawStorage", "OSName", "OSVendor", "OSVersion", "instance_role", "audit"])
    return resourceDetail


def createServerListTab(servers):
    """
    Write Service Usage detail tab to excel
    """
    logging.info("Creating Server Detail tab.")

    """ Drop audit Column for external consumption """
    servers=servers.drop("audit", axis=1)

    """ Create Excel Tab """
    #servers.to_excel(writer, "ServerDetail")
    #worksheet = writer.sheets['ServerDetail']
    #totalrows, totalcols = servers.shape
    #worksheet.autofilter(0, 0, totalrows, totalcols)

    """ Create RAW output from detail """
    servers.to_csv(file_name + ".csv", index=False, sep="|")
    # servers.to_json("server-detail.json", orient="records")

def createSymphonyLicense(instancesUsage):
    """
    Create License table for Symphony
    """

    logging.info("Calculating Symphony Licenses.")

    servers = instancesUsage.query('service_id == "is.instance" and (instance_role.str.contains("symphony") or instance_role == "smc")')
    vcpu = pd.pivot_table(servers, index=["account_name", "instance_role"],
                          values=["numberOfVirtualCPUs"],
                          aggfunc={"numberOfVirtualCPUs": np.sum},
                          margins=True, margins_name="Total",
                          fill_value=0).rename(columns={'numberOfVirtualCPUs': 'vCPU'}, index={'account_name': 'Account', 'instance_role': 'Role'})
    vcpu.to_excel(writer, 'Symphony Licenses', startcol=0, startrow=2)
    worksheet = writer.sheets['Symphony Licenses']
    format2 = workbook.add_format({'align': 'left'})
    format3 = workbook.add_format({'num_format': '#,##0', 'align': 'right'})
    boldtext = workbook.add_format({'bold': True})
    worksheet.write(0, 0, "Deployed Symphony Licenses on {}".format(datetime.now().strftime("%Y-%m-%d %H:%M")), boldtext)
    worksheet.set_column("A:B", 30, format2)
    worksheet.set_column("C:C", 15, format3)
    return


def createWindowsLicense(instancesUsage):
    """
    Create License table for Windows
    """

    logging.info("Calculating Windows Virtual Server Licenses.")

    servers = instancesUsage.query(
        'service_id == "is.instance" and OSVendor.str.contains("Microsoft") and OSName.str.contains("byol")')
    vcpu = pd.pivot_table(servers, index=["account_name", "OSVersion"],
                          values=["numberOfVirtualCPUs"],
                          aggfunc={"numberOfVirtualCPUs": np.sum},
                          margins=True, margins_name="Total",
                          fill_value=0).rename(columns={'numberOfVirtualCPUs': 'vCPU'}, index={"account_name": "Account", "instance_role": "Role"})

    vcpu.to_excel(writer, 'Microsoft Licenses', startcol=0, startrow=2)
    worksheet = writer.sheets['Microsoft Licenses']
    format2 = workbook.add_format({'align': 'left'})
    format3 = workbook.add_format({'num_format': '#,##0', 'align': 'right'})
    boldtext = workbook.add_format({'bold': True})
    worksheet.write(0, 0, "Deployed Microsoft OS Licenses on {}".format(datetime.now().strftime("%Y-%m-%d %H:%M")), boldtext)
    worksheet.set_column("A:B", 30, format2)
    worksheet.set_column("C:C", 15, format3)
    return


def createRhelLicense(instancesUsage):
    """
    Create License table for RHEL
    """
    logging.info("Calculating Red Hat Licenses.")

    servers = instancesUsage.query(
        'service_id == "is.instance" and OSVendor.str.contains("Red Hat") and OSName.str.contains("byol")')
    vcpu = pd.pivot_table(servers, index=["account_name"],
                          values=["instance_id"],
                          aggfunc={"instance_id": "nunique"},
                          margins=True, margins_name="Total",
                          fill_value=0).rename(columns={'instance_id': 'server_count'},index={"account_name": "Account"})

    vcpu.to_excel(writer, 'RedHat Licenses', startcol=0, startrow=3)
    worksheet = writer.sheets['RedHat Licenses']
    format2 = workbook.add_format({'align': 'left'})
    format3 = workbook.add_format({'num_format': '#,##0', 'align': 'right'})
    boldtext = workbook.add_format({'bold': True})
    worksheet.write(0, 0, "Deployed RedHat Licenses on {}".format(datetime.now().strftime("%Y-%m-%d %H:%M")), boldtext)
    worksheet.write(2, 0, "Virtual Server RHEL Licenses", boldtext)
    worksheet.set_column("A:A", 30, format2)
    worksheet.set_column("B:B", 15, format3)

    """
    Create License table for RHEL on BM Servers
    """

    servers = instancesUsage.query('service_id == "is.bare-metal-server" and OSName.str.contains("byol")')
    sockets = pd.pivot_table(servers, index=["account_name",  "BMnumberofSockets"],
                          values=["instance_id"],
                          aggfunc={"instance_id": "nunique"},
                          margins=True, margins_name="Total",
                          fill_value=0).rename(columns={'instance_id': 'server_count'}, index={'account_name': 'Account', 'BMnumberofSockets': 'Sockets'})
    sockets.to_excel(writer, 'RedHat Licenses', startcol=3, startrow=3)

    worksheet.write(2, 3, "BareMetal Server RHEL Licenses", boldtext)
    worksheet.set_column("D:D", 30, format2)
    worksheet.set_column("E:E", 18, format3)
    worksheet.set_column("F:F", 18, format3)

    return


def createScaleLicense(instancesUsage):
    """
    Create License table for IBM Scale on Virtual
    """
    logging.info("Calculating Scale & SKLM Licenses.")

    servers = instancesUsage.query('instance_role.str.contains("scale-gui")')
    vcpu = pd.pivot_table(servers, index=["account_name"],
                          values=["totalDataVolumeCapacity"],
                          aggfunc={"totalDataVolumeCapacity": np.sum},
                          margins=True, margins_name="Total",
                          fill_value=0).rename(columns={"totalDataVolumeCapacity": "Storage"}, index={"account_name": "Account", "instance_role": "Role"})

    vcpu.to_excel(writer, 'Scale & GKLM Licenses', startcol=0, startrow=3)
    worksheet = writer.sheets['Scale & GKLM Licenses']
    format2 = workbook.add_format({'align': 'left'})
    format3 = workbook.add_format({'num_format': '#,##0', 'align': 'right'})
    worksheet.set_column("A:A", 30, format2)
    worksheet.set_column("B:B", 15, format3)

    boldtext = workbook.add_format({'bold': True})
    worksheet.write(0, 0, "Deployed IBM Scale & GKLM Licenses on {}".format(datetime.now().strftime("%Y-%m-%d %H:%M")), boldtext)
    worksheet.write(2, 0, "Virtual Server Scale Licenses", boldtext)

    """
    Create License table for IBM Scale on BM
    """

    servers = instancesUsage.query('instance_role.str.contains("scale-storage")')
    storage = pd.pivot_table(servers, index=["account_name"],
                          values=["BMRawStorage"],
                          aggfunc={"BMRawStorage": np.sum},
                          margins=True, margins_name="Total",
                          fill_value=0).rename(columns={"BMRawStorage": "Storage"})

    storage.to_excel(writer, 'Scale & GKLM Licenses', startcol=3, startrow=3)
    worksheet.write(2, 3, "Baremetal Scale Licenses", boldtext)
    worksheet.set_column("D:D", 30, format2)
    worksheet.set_column("E:E", 18, format3)

    """
    Create SKLM table for IBM Guardium
    """

    servers = instancesUsage.query('instance_role.str.contains("sgklm")')
    storage = pd.pivot_table(servers, index=["account_name"],
                          values=["instance_id"],
                          aggfunc={"instance_id": "nunique"},
                          margins=True, margins_name="Total",
                          fill_value=0).rename(columns={"instance_id": "server_count"})

    storage.to_excel(writer, 'Scale & GKLM Licenses', startcol=6, startrow=3)
    worksheet.write(2, 6, "GKLM Licenses", boldtext)
    worksheet.set_column("G:G", 30, format2)
    worksheet.set_column("H:H", 18, format3)

    return

def createSSO(instancesUsage):
    """
    Create License table for Symphony
    """

    logging.info("Calculating SSO Licenses.")

    servers = instancesUsage.query('service_id == "is.instance" and (instance_role == "sso")')
    vcpu = pd.pivot_table(servers, index=["account_name", "instance_role"],
                          values=["numberOfVirtualCPUs"],
                          aggfunc={"numberOfVirtualCPUs": np.sum},
                          margins=True, margins_name="Total",
                          fill_value=0).rename(columns={'numberOfVirtualCPUs': 'vCPU'}, index={'account_name': 'Account', 'instance_role': 'Role'})
    vcpu.to_excel(writer, 'SSO Licenses', startcol=0, startrow=2)
    worksheet = writer.sheets['SSO Licenses']
    format2 = workbook.add_format({'align': 'left'})
    format3 = workbook.add_format({'num_format': '#,##0', 'align': 'right'})
    boldtext = workbook.add_format({'bold': True})
    worksheet.write(0, 0, "Deployed SSO"
                          " Licenses on {}".format(datetime.now().strftime("%Y-%m-%d %H:%M")), boldtext)
    worksheet.set_column("A:B", 30, format2)
    worksheet.set_column("C:C", 15, format3)
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
    totalrows, totalcols = instancesUsage.shape
    worksheet.autofilter(0, 0, totalrows, totalcols)
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
            quit(1)
        return

    cos = ibm_boto3.resource("s3",
                             ibm_api_key_id=args.COS_APIKEY,
                             ibm_service_instance_id=args.COS_INSTANCE_CRN,
                             config=Config(signature_version="oauth"),
                             endpoint_url=args.COS_ENDPOINT
                             )
    multi_part_upload(args.COS_BUCKET, upload, "./" + localfile)
    return

class Sftp:
    def __init__(self, hostname, username, password, public_key, port=22):
        """Constructor Method"""
        # Set connection object to None (initial value)
        self.connection = None
        self.hostname = hostname
        self.username = username
        self.password = password
        self.public_key = public_key
        self.port = port

    def connect(self):
        """Connects to the sftp server and returns the sftp connection object"""

        try:
            # Get the sftp connection object
            self.CnOpts = pysftp.CnOpts()
            """ Add Host Public Key """
            #self.CnOpts.hostkeys = self.CnOpts.hostkeys.add(self.hostname, keytype="ssh-rsa", key=self.public_key)
            self.CnOpts.hostkeys = None
            self.CnOpts.HostKeyAlgorithms = "+ssh-rsa"
            self.CnOpts.PubkeyAcceptedAlgorithms = "+ssh-rsa"
            self.connection = pysftp.Connection(
                host=self.hostname,
                username=self.username,
                password=self.password,
                port=self.port,
                cnopts=self.CnOpts
            )
        except Exception as err:
            #raise Exception(err)
            logging.error("Connection Exception Error: {}".format(err))
            quit(1)
        logging.info(f"Connected to {self.hostname} as {self.username}.")

    def disconnect(self):
        """Closes the sftp connection"""
        self.connection.close()
        logging.info(f"Disconnected from host {self.hostname}")

    def listdir(self, remote_path):
        """lists all the files and directories in the specified path and returns them"""
        for obj in self.connection.listdir(remote_path):
            yield obj

    def listdir_attr(self, remote_path):
        """lists all the files and directories (with their attributes) in the specified path and returns them"""
        for attr in self.connection.listdir_attr(remote_path):
            yield attr

    def download(self, remote_path, target_local_path):
        """
        Downloads the file from remote sftp server to local.
        Also, by default extracts the file to the specified target_local_path
        """

        try:
            logging.info(f"downloading from {self.hostname} as {self.username} [(remote path : {remote_path});(local path: {target_local_path})]")

            # Create the target directory if it does not exist
            path, _ = os.path.split(target_local_path)
            if not os.path.isdir(path):
                try:
                    os.makedirs(path)
                except Exception as err:
                    #raise Exception(err)
                    logging.error("File path Exception: {}".format(err))
                    quit(1)

            # Download from remote sftp server to local
            self.connection.get(remote_path, target_local_path)
            logging.info("download completed")

        except Exception as err:
            #raise Exception(err)
            logging.error("Download Exception: {}".format(err))
            quit(1)



    def upload(self, source_local_path, remote_path):
        """
        Uploads the source files from local to the sftp server.
        """
        try:
            logging.info(f"uploading to {self.hostname} as {self.username} [(remote path: {remote_path});(source local path: {source_local_path})]"
            )

            # Download file from SFTP
            self.connection.put(source_local_path, remote_path)
            logging.info("upload completed")

        except Exception as err:
            #raise Exception(err)
            logging.error("Upload Exception: {}".format(err))
            quit(1)

def writeFiletoSFTP(localfile, remotefile):
    """"
    Write Files to SFTP
    """

    sftp = Sftp(
        hostname=SFTP_HOSTNAME,
        username=SFTP_USERNAME,
        password=SFTP_PASSWORD,
        public_key=SFTP_PUBLIC_KEY,
    )

    sftp.connect()
    sftp.upload(localfile, remotefile)
    sftp.disconnect()

    return


if __name__ == "__main__":
    setup_logging()
    load_dotenv()
    parser = argparse.ArgumentParser(description="Determine License Usage.")
    parser.add_argument("--output", default=os.environ.get('output', 'license-usage.xlsx'),
                        help="Filename Excel output file. (including extension of .xlsx)")
    parser.add_argument("--debug", action=argparse.BooleanOptionalAction, help="Set Debug level for logging.")
    parser.add_argument("--load", action=argparse.BooleanOptionalAction, help="Load dataframes from pkl files.")
    parser.add_argument("--save", action=argparse.BooleanOptionalAction, help="Store dataframes to pkl files.")
    parser.add_argument("--cos", "--COS", action=argparse.BooleanOptionalAction, help="Write output to COS bucket destination specified.")
    parser.add_argument("--sftp", action=argparse.BooleanOptionalAction, help="Write output to SFTP destination specified.")
    parser.add_argument("--COS_APIKEY", default=os.environ.get('COS_APIKEY', None), help="COS apikey to use to write output to Object Storage.")
    parser.add_argument("--COS_ENDPOINT", default=os.environ.get('COS_ENDPOINT', None), help="COS endpoint to use to wbrite output tp Object Storage.")
    parser.add_argument("--COS_INSTANCE_CRN", default=os.environ.get('COS_INSTANCE_CRN', None), help="COS Instance CRN to use to write output to Object Storage.")
    parser.add_argument("--COS_BUCKET", default=os.environ.get('COS_BUCKET', None), help="COS Bucket name to use to write output to Object Storage.")
    parser.add_argument("--SFTP_USERNAME", default=os.environ.get('SFTP_USERNAME', None), help="SFTP User Name for Authentication.")
    parser.add_argument("--SFTP_HOSTNAME", default=os.environ.get('SFTP_HOSTNAME', None), help="SFTP Server Hostname or IP Address.")
    parser.add_argument("--SFTP_PASSWORD", default=os.environ.get('SFTP_PASSWORD', None), help="SFTP Password for User to be Authenticated.")
    parser.add_argument("--SFTP_PUBLIC_KEY", default=os.environ.get('SFTP_PUBLIC_KEY', None), help="SFTP Public Key of Server to be Authenticated by (Not user Public Key)")
    parser.add_argument("--SFTP_PATH", default=os.environ.get('SFTP_PATH', "."), help="SFTP destination path for file")
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
            resources = pd.DataFrame()
            """ Convert to List of JSON variable """
            try:
                APIKEYS = json.loads(APIKEYS)
            except ValueError as e:
                logging.error("Invalid List of APIKEYS.")
                quit(1)
            resources = pd.DataFrame()
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
                        instance_cache = populateVPCInstanceCache()
                        logging.info(
                            "Retrieving current resources from {} AccountId: {}.".format(accountName, accountId))
                        """ Get All Resources into Cache & Dataframe """
                        resource_cache, resources_df = getResourcesFromController()
                        resources = pd.concat([resources, parseResources(accountName, resources_df)])
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

    output = args.output
    split_tup = os.path.splitext(args.output)
    """ remove file extension """
    file_name = split_tup[0]
    timestamp = "_{}".format(datetime.now().strftime("%Y%m%d_%H%M"))

    writer = pd.ExcelWriter(file_name + ".xlsx", engine='xlsxwriter')
    workbook = writer.book
    createServerListTab(resources)
    createSymphonyLicense(resources)
    createScaleLicense(resources)
    createWindowsLicense(resources)
    createRhelLicense(resources)
    createSSO(resources)
    writer.close()

    """ Copy files created based on Flags chosen """
    """ If --COS then copy files with timestamp to COS """
    if args.cos:
        """ Write Server Detail to COS """
        logging.info("Writing Server Detail to COS.")
        writeFiletoCos(file_name + ".csv", file_name + timestamp + ".csv")

        """ Write Pivot File to COS"""
        logging.info("Writing Pivot Tables to COS.")
        writeFiletoCos(file_name + ".xlsx", file_name + timestamp + ".xlsx")

    if args.sftp:
        SFTP_USERNAME = args.SFTP_USERNAME
        SFTP_HOSTNAME = args.SFTP_HOSTNAME
        SFTP_PASSWORD = args.SFTP_PASSWORD
        SFTP_PUBLIC_KEY = args.SFTP_PUBLIC_KEY
        SFTP_PATH = args.SFTP_PATH

        """ Write Server Detail to SFTP """
        logging.info("Writing Server Detail to SFTP.")
        writeFiletoSFTP(file_name + ".csv", SFTP_PATH + "/" + file_name + timestamp + ".csv")

        """ Write Pivot File to SFTP """
        logging.info("Writing Pivot Tables to SFTP.")
        writeFiletoSFTP(file_name + ".xlsx", SFTP_PATH + "/" + file_name + timestamp + ".xlsx")
    logging.info("Current License Report is complete.")
