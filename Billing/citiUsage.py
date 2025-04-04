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
import os, logging, logging.config, os.path, argparse, calendar, pytz, yaml
from datetime import datetime, tzinfo, timezone
import pandas as pd
import numpy as np
import ibm_boto3
from dateutil.relativedelta import *
from ibm_platform_services import IamIdentityV1, UsageReportsV4, GlobalSearchV2
from ibm_platform_services.resource_controller_v2 import *
from ibm_cloud_sdk_core import ApiException
from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
from ibm_botocore.client import Config, ClientError
from dotenv import load_dotenv
from yaml import Loader

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
def readAppConf(filename):
    """
    Read application Configuration into Dictionary
    :param filename: filename of application configuration in YAML format
    :return:
    """

    stream = open(filename, 'r')
    applicationConf = yaml.load(stream, Loader=Loader)
    return applicationConf
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
def createSDK(IC_API_KEY):
    """
    Create SDK clients
    """
    global usage_reports_service, resource_controller_service, iam_identity_service, global_search_service

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
def prePopulateTagCache():
    """
    Pre Populate Tagging data into cache
    """
    logging.info("Tag Cache being pre-populated with tags.")
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
def prePopulateResourceCache():
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
                "API Error.  Can not retrieve instances {}: {}".format(str(e.code),
                                                                                  e.message))
            quit(1)

        resource_cache = {}
        for resource in all_results:
            resourceId = resource["crn"]
            resource_cache[resourceId] = resource

        return resource_cache
def getAccountUsage(start, end):
    """
    Get IBM Cloud Service from account for range of months.
    """

    data = []
    while start <= end:
        usageMonth = start.strftime("%Y-%m")
        logging.info("Retrieving Account Usage from {}.".format(usageMonth))
        start += relativedelta(months=+1)

        try:
            usage = usage_reports_service.get_account_usage(
                account_id=accountId,
                billingmonth=usageMonth,
                names=True
            ).get_result()
        except ApiException as e:
            if e.code == 424:
                logging.warning("API exception {}.".format(str(e)))
                continue
            else:
                logging.error("API exception {}.".format(str(e)))
                quit(1)

        logging.debug("usage {}={}".format(usageMonth, usage))
        for resource in usage['resources']:
            for plan in resource['plans']:
                if "pricing_region" in plan:
                    pricing_region = plan["pricing_region"]
                else:
                    pricing_region = ""
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
                        'pricing_region': pricing_region,
                        'metric': metric['metric'],
                        'unit_name': metric['unit_name'],
                        'quantity': float(metric['quantity']),
                        'rateable_quantity': metric['rateable_quantity'],
                        'cost': metric['cost'],
                        'rated_cost': metric['rated_cost'],
                        }
                    if metric['discounts'] != []:
                        """
                        Discount found in usage record, convert to decimal
                        """
                        row['discount'] = metric['discounts'][0]['discount'] / 100
                    else:
                        """
                        No discount found set to zero
                        """
                        row["discount"] = 0

                    if len(metric['price']) > 0:
                        row['price'] = metric['price']
                    else:
                        row['price'] = "[]"
                    # add row to data
                    data.append(row.copy())


    accountUsage = pd.DataFrame(data, columns=['account_id', "account_name", 'month', 'currency_code', 'billing_country', 'resource_id', 'resource_name',
                    'billable_charges', 'billable_rated_charges', 'plan_id', 'plan_name', 'pricing_region', 'metric', 'unit_name', 'quantity',
                    'rateable_quantity','cost', 'rated_cost', 'discount', 'price'])

    return accountUsage
def getInstancesUsage(start,end):
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
                logging.warning(
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

    data = []
    nytz = pytz.timezone('America/New_York')
    limit = 100  ## set limit of record returned

    """Iterate through multiple months """
    while start <= end:
        usageMonth = start.strftime("%Y-%m")
        start += relativedelta(months=+1)
        recordstart = 1
        """ Read first Group of records """
        try:
            instances_usage = usage_reports_service.get_resource_usage_account(
                account_id=accountId,
                billingmonth=usageMonth, names=True, limit=limit).get_result()
        except ApiException as e:
            logging.error("Fatal Error with get_resource_usage_account: {}".format(e))
            quit(1)

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

        while True:
            for instance in instances_usage["resources"]:
                logging.debug("Parsing Details for Instance {}.".format(instance["resource_instance_id"]))
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
                recordstart = recordstart + limit
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
                    logging.error("Error with get_resource_usage_account: {}".format(e))
                    quit(1)

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
def createServiceDetail(paasUsage):
    """
    Write Service Usage detail tab to excel
    """
    logging.info("Creating ServiceUsageDetail tab.")

    paasUsage.to_excel(writer, "ServiceUsageDetail")
    worksheet = writer.sheets['ServiceUsageDetail']
    format1 = workbook.add_format({'num_format': '$#,##0.00'})
    format2 = workbook.add_format({'align': 'left'})
    worksheet.set_column("A:C", 12, format2)
    worksheet.set_column("D:E", 25, format2)
    worksheet.set_column("F:G", 18, format1)
    worksheet.set_column("H:I", 25, format2)
    worksheet.set_column("J:J", 18, format1)
    totalrows,totalcols=paasUsage.shape
    worksheet.autofilter(0,0,totalrows,totalcols)
    return
def createInstancesDetailTab(instancesUsage):
    """
    Write detail tab to excel
    """
    logging.info("Creating instances detail tab.")

    instancesUsage.to_excel(writer, "Instances_Detail")
    worksheet = writer.sheets['Instances_Detail']
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
def createUsageSummaryTab(paasUsage):
    logging.info("Creating Usage Summary tab.")
    usageSummary = pd.pivot_table(paasUsage, index=["account_name", "resource_name"],
                                    columns=["month"],
                                    values=["cost"],
                                    aggfunc=np.sum, margins=True, margins_name="Total",
                                    fill_value=0)
    new_order = ["rated_cost", "cost"]
    usageSummary = usageSummary.reindex(new_order, axis=1, level=0)
    usageSummary.to_excel(writer, 'Usage_Summary')
    worksheet = writer.sheets['Usage_Summary']
    format1 = workbook.add_format({'num_format': '$#,##0.00'})
    format2 = workbook.add_format({'align': 'left'})
    worksheet.set_column("A:A", 35, format2)
    worksheet.set_column("B:ZZ", 18, format1)
def createMetricSummary(paasUsage):
    logging.info("Creating Metric Plan Summary tab.")
    metricSummaryPlan = pd.pivot_table(paasUsage, index=["account_name", "resource_name", "plan_name", "metric"],
                                 columns=["month"],
                                 values=["rateable_quantity", "cost"],
                                 aggfunc=np.sum, margins=True, margins_name="Total",
                                 fill_value=0)
    new_order = ["rateable_quantity", "cost"]
    metricSummaryPlan = metricSummaryPlan.reindex(new_order, axis=1, level=0)
    metricSummaryPlan.to_excel(writer, 'MetricPlanSummary')
    worksheet = writer.sheets['MetricPlanSummary']
    format1 = workbook.add_format({'num_format': '$#,##0.00'})
    format2 = workbook.add_format({'align': 'left'})
    format3 = workbook.add_format({'num_format': '#,##0.00000'})
    worksheet.set_column("A:A", 30, format2)
    worksheet.set_column("B:B", 40, format2)
    worksheet.set_column("C:C", 40, format2)
    worksheet.set_column("D:D", 40, format2)
    worksheet.set_column("E:H", 30, format3)
    worksheet.set_column("I:ZZ", 15, format1)
    return
def createVcpuTab(instancesUsage,end):
    """
    Create VCPU deployed by role, account, and az
    """

    logging.info("Calculating Virtual Server vCPU deployed.")
    usageMonth = datetime.strftime(end, "%Y-%m")
    servers = instancesUsage.query('service_id == "is.instance" and (metric == "VCPU_HOURS" or metric =="INSTANCE_HOURS_MULTI_TENANT") and instance_role.str.contains("symphony-worker") and month == @usageMonth')
    vcpu = pd.pivot_table(servers, index=["account_name", "region", "availability_zone", "instance_role", "audit"],
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
def createBMvcpuTab(instancesUsage,end):
    """
    Create BM VCPU deployed by role, account, and az
    """

    logging.info("Calculating Bare Metal vCPU deployed.")
    usageMonth = datetime.strftime(end, "%Y-%m")
    servers = instancesUsage.query('service_id == "is.bare-metal-server" and metric == "BARE_METAL_SERVER_HOURS" and instance_role.str.contains("scale-storage") and month == @usageMonth')
    vcpu = pd.pivot_table(servers, index=["account_name", "region", "availability_zone", "instance_role", "audit"],
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
def createProvisionAllTab(instancesUsage, end):
    """
    Create Pivot by Original Provision Date
    """

    logging.info("Calculating vCPU by provision date.")
    usageMonth = datetime.strftime(end, "%Y-%m")
    servers = instancesUsage.query('(service_id == "is.instance" and (metric == "VCPU_HOURS" or metric =="INSTANCE_HOURS_MULTI_TENANT")) or (service_id == "is.bare-metal-server" and metric == "BARE_METAL_SERVER_HOURS") and month == @usageMonth')

    vcpu = pd.pivot_table(servers, index=["account_name", "region", "availability_zone", "instance_role", "audit", "instance_profile", "provision_date", "estimated_days"],
                                    values=["instance_id", "numberOfVirtualCPUs"],
                                    aggfunc={"instance_id": "nunique", "numberOfVirtualCPUs": np.sum},
                                    fill_value=0).rename(columns={'instance_id': 'instance_count', 'estimated_days': 'days_used'})

    new_order = ["instance_count", "numberOfVirtualCPUs"]
    vcpu = vcpu.reindex(new_order, axis=1)
    #vcpu = vcpu.reset_index()
    vcpu.to_excel(writer, 'ProvisionDateAllRoles')
    worksheet = writer.sheets['ProvisionDateAllRoles']
    format2 = workbook.add_format({'align': 'left'})
    format3 = workbook.add_format({'num_format': '#,##0'})
    worksheet.set_column("A:F", 30, format2)
    worksheet.set_column("G:I", 18, format3)
    #totalrows,totalcols=vcpu.shape
    #worksheet.autofilter(0,0,totalrows,totalcols)
    return
def createProvisionScaleTab(instancesUsage, end):
    """
    Create Pivot by Of Scale Servers by Date
    """

    logging.info("Calculating vCPU by provision date scale storage nodes. only.")
    usageMonth = datetime.strftime(end, "%Y-%m")
    servers = instancesUsage.query(
        'service_id == "is.bare-metal-server" and metric == "BARE_METAL_SERVER_HOURS" and instance_role.str.contains("scale-storage") and month == @usageMonth')

    vcpu = pd.pivot_table(servers, index=["account_name", "region", "availability_zone", "instance_role", "instance_profile", "audit", "provision_date", "estimated_days"],
                                    values=["instance_id", "BMnumberofCores", "BMnumberofSockets"],
                                    aggfunc={"instance_id": "nunique", "BMnumberofCores": np.sum, "BMnumberofSockets": np.sum},
                                    margins=True, margins_name="Total",
                                    fill_value=0).rename(columns={'instance_id': 'instance_count', 'estimated_days': 'days_used',  "BMnumberofCores": "Cores", "BMnumberofSockets": "Sockets"})

    new_order = ["instance_count", "Cores", "Sockets"]
    vcpu = vcpu.reindex(new_order, axis=1)
    #vcpu = vcpu.reset_index()
    vcpu.to_excel(writer, 'ProvisionDateScaleRole')
    worksheet = writer.sheets['ProvisionDateScaleRole']
    format2 = workbook.add_format({'align': 'left'})
    format3 = workbook.add_format({'num_format': '#,##0'})
    worksheet.set_column("A:F", 30, format2)
    worksheet.set_column("G:I", 18, format3)
    #totalrows,totalcols=vcpu.shape
    #worksheet.autofilter(0,0,totalrows,totalcols)
    return
def createProvisionWorkersTab(instancesUsage, end):
    """
    Create Pivot by Original Provision Date
    """

    logging.info("Calculating vCPU by provision date symphony-workers only.")
    usageMonth = datetime.strftime(end, "%Y-%m")
    servers = instancesUsage.query('service_id == "is.instance" and instance_role.str.contains("symphony-worker") and month == @usageMonth and (metric == "VCPU_HOURS" or metric =="INSTANCE_HOURS_MULTI_TENANT")')

    vcpu = pd.pivot_table(servers, index=["account_name", "region", "availability_zone", "instance_role", "audit", "instance_profile", "provision_date", "estimated_days"],
                                    values=["instance_id", "numberOfVirtualCPUs"],
                                    aggfunc={"instance_id": "nunique", "numberOfVirtualCPUs": np.sum},
                                    margins=True, margins_name="Total",
                                    fill_value=0).rename(columns={'instance_id': 'instance_count', 'estimated_days': 'days_used'})

    new_order = ["instance_count", "numberOfVirtualCPUs"]
    vcpu = vcpu.reindex(new_order, axis=1)
    #vcpu = vcpu.reset_index()
    vcpu.to_excel(writer, 'ProvisionDateWorkerRole')
    worksheet = writer.sheets['ProvisionDateWorkerRole']
    format2 = workbook.add_format({'align': 'left'})
    format3 = workbook.add_format({'num_format': '#,##0'})
    worksheet.set_column("A:F", 30, format2)
    worksheet.set_column("G:I", 18, format3)
    #totalrows,totalcols=vcpu.shape
    #worksheet.autofilter(0,0,totalrows,totalcols)
    return
def createTrueUp(accountUsage, end):
    """
    Calculate table for variable usage items for TrueUp (Appendix F - table 14)
     - IBM Cloud Object Storage  service_id dff97f5c-bc5e-4455-b470-411c3edbe49c
     - Direct LInk 2.0 Data Charges service_id 86fb7610-0f92-11ea-a6a3-8b96ed1570d8
     - IBM Cloud Activity Tracker service_id dcc46a60-e13b-11e8-a015-757410dab16b
     - IBM Cloud Monitoring service_id 090c2c10-8c38-11e8-bec2-493df9c49eb8
     - IBM Cloud Secrets Managers service_id ebc0cdb0-af2a-11ea-98c7-29e5db822649
     - IBM Cloud DNS Service service_id b4ed8a30-936f-11e9-b289-1d079699cbe5
    """
    def calculateAllocations(variableServices):
        """
        Based on contract calculate allocations based on hosts.
        Allotments are monthly but calculated quarterly based on contract start of June 2022
        (june, july, august), (sept, oct,nov), (dec,jan,feb), (march, april, may)

         - IBM Cloud Object Storage  service_id dff97f5c-bc5e-4455-b470-411c3edbe49c
            Will exist accross all accounts. Smart Tier Regional quoted, but not being used.

         - Direct LInk 2.0 Data Charges service_id 86fb7610-0f92-11ea-a6a3-8b96ed1570d8
            Direct LInk instances should be in HPC Common only

         - Transit Gateway Data Charge  service_id f38a4da0-c353-11e9-83b6-a36a57a97a06
            Only used by IBM admins

         - IBM Cloud Activity Tracker service_id dcc46a60-e13b-11e8-a015-757410dab16b
            Provisioned in each account (application).  Are allocations managed per application?

         - IBM Cloud Monitoring service_id 090c2c10-8c38-11e8-bec2-493df9c49eb8
            Monitoring per account (application).  Allocations accross accounts.

         - IBM Cloud Secrets Managers service_id ebc0cdb0-af2a-11ea-98c7-29e5db822649
            Should be in HPC Common (currently deployed in ACE), one per region

         - IBM Cloud DNS Service service_id b4ed8a30-936f-11e9-b289-1d079699cbe5
            Should be one instance in common accounnt, one zone per application, query allocation her compute node
        """

        table = pd.DataFrame(variableServices,
                             columns=["month", "resource_id", "resource_name", "plan_name",
                                      "metric", "quantity", "rateable_quantity", "cost"]).groupby(
            ["month", "resource_id", "resource_name", "plan_name", "metric"], sort=False,
            as_index=False).agg({"quantity": np.sum, "rateable_quantity": np.sum, "cost": np.sum})


        logging.info("Calculating Variable Usage for {}.".format(billingMonth))

        for index, row in table.iterrows():
            if row["resource_id"] == objectstorage:
                """
                Object Storage
                Metrics
                    SMART_TIER_BANDWIDTH - 0 allocted
                    SMART_TIER_STORAGE - 0 allocted 
                    SMART_TIER_CLASS_A_CALLS - 0 allocted
                    SMART_TIER_CLASS_B_CALLS - 0 allocted
                    SMART_TIER_RETRIEVAL -0 allocted
                    SMART_TIER_BANDWIDTH - 0 allocted
                    SMART_TIER_CLASS_A_CALLS - 0 allocted
                    SMART_TIER_CLASS_B_CALLS - 0 allocted
                    SMART_TIER_RETRIEVAL - 0 allocted
                    STANDARD_BANDWIDTH - 0 allocted
                    STANDARD_CLASS_A_CALLS - 0 allocted
                    STANDARD_CLASS_B_CALLS - 0 allocted
                    SMART_TIER_STORAGE - 30GB per compute node
                    STANDARD_STORAGE - not quoted in contract, but should be combined with Smart Tier storage.
                Appendix F Table 14 only states a charge for Short Term Storage; originally assumed
                to be cross Region Standard Tier; but apply to all
                """
                table.at[index, "contract_category"] = "variable"

            elif row["resource_id"] == directlink:
                """
                Direct LInk 2.0 Data Charges service_id 86fb7610-0f92-11ea-a6a3-8b96ed1570d8
                Direct LInk instances should be in HPC Common only
                Metrics
                    Instance_1Gbps_Metered_Port - In base charge 
                    GIGABYTE_TRANSMITTED_OUTBOUND - 100GB per compute node
                """
                if row["metric"] == "GIGABYTE_TRANSMITTED_OUTBOUND":
                    table.at[index, "contract_category"] = "variable"

            elif row["resource_id"] == activitytracker:
                """
                Activity Tracker
                Metrics
                    GIGABYTE_MONTHS - 10 MB per compute node
                """
                if row["metric"] == "GIGABYTE_MONTHS":
                    table.at[index, "contract_category"] = "variable"

            elif row["resource_id"] == monitoringservice:
                """
                IBM Cloud Monitoring 
                METRICS
                    API_CALL_HOURS  - 0 allocated
                    TIME_SERIES_HOURS - 66.6K per compute node
                """
                if row["metric"] == "TIME_SERIES_HOURS" or row["metric"] == "API_CALL_HOURS":
                    table.at[index, "contract_category"] = "variable"

            elif row["resource_id"] == secretsmanager:
                """
                IBM Cloud Secrets Managers 
                Metrics
                INSTANCES - fixed, in common one per region - 1 per AZ
                ACTIVE_SECRETS  - 1 secret per compute node            
                """
                if row["metric"] == "ACTIVE_SECRETS":
                    table.at[index, "contract_category"] = "variable"

            elif row["resource_id"] == dnsservice:
                """
                IBM Cloud DNS Service
                Metrics
                    MILLION_ITEMS - 100,000 (DNS queries) per compute host
                    ITEMS - 1 zone per app
                    NUMBERGLB - 0 allocated
                    NUMBERPOOLS - 0 allocated
                    NUMBERHEALTHCHECK - 0 allocated
                    RESOLVERLOCATIONS - 0 allocated
                    MILLION_ITEMS_CREXTERNALQUERIES - 0 allocated            
                """
                if row["metric"] == "MILLION_ITEMS":
                    table.at[index, "contract_category"] = "variable"
            elif row["resource_id"] == transitgateway:
                """
                Intention is to have discounted transitgateway, but show actuals
                """
                table.at[index, "contract_category"] = "variable"

        return table

    global objectstorage, directlink, transitgateway, activitytracker, monitoringservice, secretsmanager, dnsservice, allocationTable, applicationConfiguration

    """
    Define Service ID's for variable services included in Trueup
    """
    objectstorage = "dff97f5c-bc5e-4455-b470-411c3edbe49c"
    directlink = "86fb7610-0f92-11ea-a6a3-8b96ed1570d8"
    activitytracker = "dcc46a60-e13b-11e8-a015-757410dab16b"
    monitoringservice = "090c2c10-8c38-11e8-bec2-493df9c49eb8"
    secretsmanager = "ebc0cdb0-af2a-11ea-98c7-29e5db822649"
    dnsservice = "b4ed8a30-936f-11e9-b289-1d079699cbe5"
    transitgateway = "f38a4da0-c353-11e9-83b6-a36a57a97a06"
    billingMonth = datetime.strftime(end, "%Y-%m")

    logging.info("Creating Variable Services tab.")

    """
    get usage for all metrics for each of the services in Appendix F - Table 14 for usage month.
    """
    variableServices = accountUsage.query('month == @billingMonth and ' \
        ' (resource_id == @objectstorage or resource_id == @directlink' \
        ' or resource_id == @activitytracker or resource_id == @monitoringservice or resource_id == @secretsmanager' \
        ' or resource_id == @dnsservice or resource_id == @transitgateway)')

    """
    Build dataframe of each service metric relevant to the allocations
    """
    allocationTable = calculateAllocations(variableServices)

    """
    Filter only those allocations which are varaible
    """
    allocationTable = allocationTable.query('contract_category == "variable"')
    overage = pd.DataFrame(allocationTable,
                         columns=["month", "resource_id", "resource_name", "plan_name",
                                  "metric", "contract_category", "rateable_quantity", "cost"]).groupby(
        ["resource_id", "resource_name", "plan_name", "metric"], sort=False,
        as_index=False).agg({"rateable_quantity": np.sum, "cost": np.sum})

    trueupPivot = pd.pivot_table(overage, index=["resource_name", "metric"],
                                       values=["rateable_quantity", "cost"],
                                       aggfunc={"rateable_quantity": np.sum, "cost": np.sum}, margins=True,
                                       fill_value=0)
    new_order = ["rateable_quantity", "cost"]
    trueupPivot = trueupPivot.reindex(new_order, axis=1)
    # create tab
    sheet_name = "TrueUp"
    trueupPivot.to_excel(writer, sheet_name, startcol=0, startrow=3)
    worksheet = writer.sheets[sheet_name]
    format1 = workbook.add_format({'num_format': '$#,##0.00'})
    format2 = workbook.add_format({'align': 'left'})
    format3 = workbook.add_format({'num_format': '#,##0.00'})
    boldtext = workbook.add_format({'bold': True})
    bolddollars = workbook.add_format({'bold': True, 'num_format': '$#,##0.00'})
    yellow = workbook.add_format({'bg_color': '#FFFF00'})
    worksheet.set_column("A:B", 30, format2)
    worksheet.set_column("C:C", 18, format3)
    worksheet.set_column("D:D", 18, format1)

    """
    Adjust to reflect contract allocations 
    """
    aceAllocation=11.69
    simplicitiAllocation=11.69
    equitiesAllocation=7.49
    commoditiesAllocation=7.49

    """
    Create Manual calculations table at bottom of pivot output
    """
    row = len(trueupPivot.index) + 5
    actual = row - 1 # actual value from pivot
    worksheet.write(0, 0, "Variable Usage Allocation & Trueup Calculator for {}".format(billingMonth), boldtext)
    subtotals=[]
    for application in applicationConfiguration:
        if "allocation" in application:
            servers = instancesUsage.query(
                'account_id == @application["account"] and service_id == "is.instance" and instance_role.str.contains("symphony-worker") and month == @billingMonth and (metric == "VCPU_HOURS" or metric =="INSTANCE_HOURS_MULTI_TENANT")').shape[0]

            worksheet.write(row, 0, application["name"], boldtext)
            worksheet.write(row, 1, "Allocated per Compute Node $", boldtext)
            worksheet.write(row, 3, application["allocation"], bolddollars)
            worksheet.write(row+1, 1, "Compute Node Quantity", boldtext)
            worksheet.write(row+1, 3, servers, yellow)
            worksheet.write(row+2, 1, "Sub-Total Allocated:", boldtext)
            subtotals.append(row+3)
            formula = "=d"+str(row+1)+"*d"+str(row+2)
            worksheet.write_formula(row+2, 3, formula,bolddollars)
            row = row + 5

    # Total Allocated
    worksheet.write(row, 1, "Total Allocated", boldtext)
    formula = "="
    for i in subtotals:
        formula = formula + "+d" + str(i)
    worksheet.write_formula(row, 3, formula, bolddollars)
    worksheet.write(row+1, 1, "Overage:", boldtext)

    formula = "=(d" + str(actual) + "-d" + str(row + 1) + ")"
    worksheet.write_formula(row+1, 3, formula, bolddollars)
    return
def createApplicationChargesTabs(instancesUsage, month):
    """
    Routine to create the Application Specific Contract Charges and write them out to Excel Tabs
    :param instancesUsage: dataframe of detailed usage information from Usage & Recource Controller
    :param month: month to calculate charges for
    :return:
    """
    global applicationConfiguration
    def calculatePerAccountCharges(appName, appAccount, componentName, chargeName, role, charge_type, profile, regionCharges):
        """
        Calculate Per Account Charge for Compute components for specific account
        :param appName: Name of Application
        :param appAccount: Cloud Account to search against
        :param componentName: Name of the Component being charged
        :param chargeName: Name of Charge
        :param role: Tagged Role to search against
        :param charge_type:  Type of Charge
        :param profile: profile type to search against
        :param regionCharges: Charges for each region
        :return:
        """

        logging.info("Creating {} -- {} per Account charges for {}.".format(componentName, chargeName, appName))

        if profile == "any":
            operational = servers.query('(instance_role.str.contains(@role) and account_id == @appAccount)')
        else:
            # filter to specific profile type if specified
            operational = servers.query('(instance_role.str.contains(@role) and instance_profile == @profile and account_id == @appAccount)')

        table0 = pd.DataFrame(operational,
                              columns=["region", "availability_zone", "instance_id",
                                       "instance_name", "instance_role"]).groupby(['region', 'availability_zone', 'instance_id', 'instance_name', 'instance_role'],sort=False, as_index=False).agg("count")

        table0["contract_category"] = "{} - {}".format(componentName, chargeName)
        table0["availability_zone"] = ""  #remove zone because charge is per account
        table0["region"] = "" #remove region because charge is per account.
        table0["metric"] = type

        # Consolidate Table to one row per Region, AZ and contract Category
        table1 = pd.DataFrame(table0, columns=["region", "availability_zone", "contract_category", "metric"]).groupby(['region', 'availability_zone', 'contract_category', "metric"],sort=False, as_index=False).agg("count")

        table1["contract_rate"] = ""
        table1["unit_rate"] = ""
        # For per account, match to any region
        for index, row in table1.iterrows():
            contract_rate = (list(filter(lambda x: x["name"] == "any", regionCharges))[0]["contract_rate"])
            table1.at[index, "contract_rate"] = contract_rate
            table1.at[index, "unit_rate"] = "${:,.2f}".format(contract_rate)
            table1.at[index, "period"] = charge_type
        table1["estimated_days"] = ""
        table1["quantity"] = 1
        return table1
    def calculateServicePerAccountCharges(appName, appAccount, componentName, chargeName, role, charge_type, service, regionCharges):
        """
        Calculate Per Account Charge for Service components in specified account
        :param appName: Name of Application
        :param appAccount: Cloud Account to search against
        :param componentName: Name of the Component being charged
        :param chargeName: Name of Charge
        :param role: Tagged Role to search against
        :param charge_type:  Type of Charge
        :param profile: profile type to search against
        :param regionCharges: Charges for each region
        :return:
        """

        logging.info("Creating {} -- {} per Account charges for {}.".format(componentName, chargeName, appName))

        if role == "any":
            # include all services found.  Note for PerRegion charge # of instances is not used in calculating fixed charged.  Therefore this would only impact charge if there was one.
            operational = services.query('(service_id.str.contains(@service) and account_id == @appAccount)')
        else:
            # filter to specific instance_role if specified
            operational = services.query('(service_id.str.contains(@service) and (instance_role.str.contains(@role) and account_id == @appAccount)')

        table0 = pd.DataFrame(operational,
                              columns=["region", "availability_zone", "instance_id",
                                       "instance_name", "instance_role"]).groupby(['region', 'availability_zone', 'instance_id', 'instance_name', 'instance_role'],sort=False, as_index=False).agg("count")

        table0["contract_category"] = "{} - {}".format(componentName, chargeName)
        table0["availability_zone"] = ""  #remove zone because charge is per account
        table0["region"] = "" #remove region because charge is per account.
        table0["metric"] = type
        # Consolidate Table to one row per Region, AZ and contract Category
        table1 = pd.DataFrame(table0, columns=["region", "availability_zone", "contract_category", "metric"]).groupby(['region', 'availability_zone', 'contract_category', "metric"],sort=False, as_index=False).agg("count")

        table1["contract_rate"] = ""
        table1["unit_rate"] = ""
        # For per account, match to any region
        for index, row in table1.iterrows():
            contract_rate = (list(filter(lambda x: x["name"] == "any", regionCharges))[0]["contract_rate"])
            table1.at[index, "contract_rate"] = contract_rate
            table1.at[index, "unit_rate"] = "${:,.2f}".format(contract_rate)
            table1.at[index, "period"] = charge_type
        table1["estimated_days"] = ""
        table1["quantity"] = 1
        return table1
    def calculatePerRegionCharges(appName, appAccount, componentName, chargeName, role, charge_type, profile, regionCharges):
        """
        Calculate Base Per Region Charge for Compute components in account
        :param appName: Name of Application
        :param appAccount: Cloud Account to search against
        :param componentName: Name of the Component being charged
        :param chargeName: Name of Charge
        :param role: Tagged Role to search against
        :param charge_type:  Type of Charge
        :param profile: profile type to search against
        :param regionCharges: Charges for each region
        :return:
        """

        logging.info("Creating {} -- {} per Region charges for {}.".format(componentName, chargeName, appName))

        if profile == "any":
            operational = servers.query('(instance_role.str.contains(@role) and account_id == @appAccount)')
        else:
            # filter to specific profile type if specified
            operational = servers.query('(instance_role.str.contains(@role) and instance_profile == @profile and account_id == @appAccount)')

        table0 = pd.DataFrame(operational,
                              columns=["region", "availability_zone", "instance_id",
                                       "instance_name", "instance_role"]).groupby(['region', 'availability_zone', 'instance_id', 'instance_name', 'instance_role'],sort=False, as_index=False).agg("count")

        table0["contract_category"] = "{} - {}".format(componentName, chargeName)
        table0["availability_zone"] = ""  #remove zone because charge is per region not per zone
        table0["metric"] = type

        # Consolidate Table to one row per Region, AZ and contract Category
        table1 = pd.DataFrame(table0, columns=["region", "availability_zone", "contract_category", "metric"]).groupby(['region', 'availability_zone', 'contract_category', "metric"],sort=False, as_index=False).agg("count")

        table1["contract_rate"] = ""
        table1["unit_rate"] = ""
        # Lookup contract charges from regionCharges variable
        for index, row in table1.iterrows():
            contract_rate = (list(filter(lambda x: x["name"] == row["region"], regionCharges))[0]["contract_rate"])
            table1.at[index, "contract_rate"] = contract_rate
            table1.at[index, "unit_rate"] = "${:,.2f}".format(contract_rate)
            table1.at[index, "period"] = charge_type
        table1["estimated_days"] = ""
        table1["quantity"] = 1
        return table1
    def calculateServicePerRegionCharges(appName, appAccount, componentName, chargeName, role, charge_type, service, regionCharges):
        """
        Calculate Base Per Region Charge for Service in an account
        :param appName: Name of Application
        :param appAccount: Cloud Account to search against
        :param componentName: Name of the Component being charged
        :param chargeName: Name of Charge
        :param role: Tagged Role to search against
        :param charge_type:  Type of Charge
        :param service: service_name to search against
        :param regionCharges: Charges for each region
        :return:
        """

        logging.info("Creating {} -- {} per Region charges for {}.".format(componentName, chargeName, appName))
        if role == "any":
            # include all services found.  Note for PerRegion charge # of instances is not used in calculating fixed charged.  Therefore this would only impact charge if there was one.
            operational = services.query('(service_id.str.contains(@service) and account_id == @appAccount)')
        else:
            # filter to specific instance_role if specified
            operational = services.query('(service_id.str.contains(@service) and (instance_role.str.contains(@role) and account_id == @appAccount)')

        table0 = pd.DataFrame(operational,
                              columns=["region", "availability_zone", "instance_id",
                                       "instance_name", "instance_role"]).groupby(['region', 'availability_zone', 'instance_id', 'instance_name', 'instance_role'],sort=False, as_index=False).agg("count")

        table0["contract_category"] = "{} - {}".format(componentName, chargeName)
        table0["availability_zone"] = ""
        table0["metric"] = type

        # Consolidate Table to one row per Region, AZ and contract Category
        table1 = pd.DataFrame(table0, columns=["region", "availability_zone", "contract_category", "metric"]).groupby(['region', 'availability_zone', 'contract_category', "metric"],sort=False, as_index=False).agg("count")

        table1["contract_rate"] = ""
        table1["unit_rate"] = ""

        # Lookup contract charges from regionCharges variable
        for index, row in table1.iterrows():
            contract_rate = (list(filter(lambda x: x["name"] == row["region"], regionCharges))[0]["contract_rate"])
            table1.at[index, "contract_rate"] = contract_rate
            table1.at[index, "unit_rate"] = "${:,.2f}".format(contract_rate)
            table1.at[index, "period"] = charge_type
        table1["estimated_days"] = ""
        table1["quantity"] = 1
        return table1
    def calculatePerAzCharges(appName, appAccount, componentName, chargeName, role, charge_type, profile, regionCharges):
        """
        Calculate Base Per AZ for Operational Components based on all accounts (charge once if exists in AZ in any account)
        :param appName: Name of Application
        :param appAccount: Cloud Account to search against
        :param componentName: Name of the Component being charged
        :param chargeName: Name of Charge
        :param role: Tagged Role to search against
        :param charge_type:  Type of Charge
        :param profile: Profile to search against
        :param regionCharges: Charges for each region
        :return:
        """

        logging.info("Creating {} -- {} per AZ charges for {}.".format(componentName, chargeName, appName))
        if profile == "any":
            operational = servers.query('(instance_role.str.contains(@role))')
        else:
            # filter to specific profile type if specified
            operational = servers.query('(instance_role.str.contains(@role) and instance_profile == @profile)')

        table0 = pd.DataFrame(operational,
                              columns=["region", "availability_zone", "instance_id",
                                       "instance_name", "instance_role"]).groupby(
            ['region', 'availability_zone', 'instance_id', 'instance_name', 'instance_role'],
            sort=False,
            as_index=False).agg("count")

        table0["contract_category"] = "{} - {}".format(componentName, chargeName)
        table0["metric"] = type

        # Consolidate Table to one row per Region, AZ and contract Category
        table1 = pd.DataFrame(table0,
                              columns=["region", "availability_zone", "contract_category", "metric"]).groupby(
            ['region', 'availability_zone', 'contract_category', "metric"],
            sort=False, as_index=False).agg("count")

        table1["contract_rate"] = ""
        table1["unit_rate"] = ""
        # Lookup contract charges from regionCharges variable
        for index, row in table1.iterrows():
            contract_rate = (list(filter(lambda x: x["name"] == row["region"], regionCharges))[0]["contract_rate"])
            table1.at[index, "contract_rate"] = contract_rate
            table1.at[index, "unit_rate"] = "${:,.2f}".format(contract_rate)
            table1.at[index, "period"] = charge_type
        table1["estimated_days"] = ""
        table1["quantity"] = 1
        return table1
    def calculatePerAzPerAppCharges(appName, appAccount, componentName, chargeName, role, charge_type, profile,regionCharges):
        """
        Calculate Per AZ Per App Components
        :param appName: Name of Application
        :param appAccount: Cloud Account to search against
        :param componentName: Name of the Component being charged
        :param chargeName: Name of Charge
        :param role: Tagged Role to search against
        :param charge_type:  Type of Charge
        :param profile: Profile to search against
        :param regionCharges: Charges for each region
        :return:
        """

        logging.info("Creating {} -- {} per AZ charges per App for {}.".format(componentName, chargeName, appName))
        if profile == "any":
            operational = servers.query('(instance_role.str.contains(@role)  and account_id == @appAccount)')
        else:
            # filter to specific profile type if specified
            operational = servers.query('(instance_role.str.contains(@role) and instance_profile == @profile and account_id == @appAccount)')

        table0 = pd.DataFrame(operational,
                             columns=["region", "availability_zone", "instance_id",
                                      "instance_name", "instance_role"]).groupby(
            ['region', 'availability_zone', 'instance_id', 'instance_name', 'instance_role'],
            sort=False,
            as_index=False).agg("count")

        table0["contract_category"] = "{} - {}".format(componentName,chargeName)
        table0["metric"] = type

        # Consolidate Table to one row per Region, AZ and contract Category
        table1 = pd.DataFrame(table0,
                              columns=["region", "availability_zone", "contract_category", "metric"]).groupby(['region', 'availability_zone', 'contract_category', 'metric'],
                                                        sort=False, as_index=False).agg("count")

        table1["contract_rate"] = ""
        table1["unit_rate"] = ""
        # Lookup contract charges from regionCharges variable
        for index, row in table1.iterrows():
            contract_rate = (list(filter(lambda x:x["name"]==row["region"],regionCharges))[0]["contract_rate"])
            table1.at[index, "contract_rate"] = contract_rate
            table1.at[index, "unit_rate"] = "${:,.2f}".format(contract_rate)
            table1.at[index, "period"] = charge_type
        table1["estimated_days"] = ""
        table1["quantity"] = 1
        return table1
    def calculatePerNodeCharges(appName, appAccount, componentName, chargeName, role, charge_type, profile,regionCharges, daysInMonth):
        """
        Calculate Per Node Charges for Application
        :param appName: Name of Application
        :param appAccount: Cloud Account to search against
        :param componentName: Name of the Component being charged
        :param chargeName: Name of Charge
        :param role: Tagged Role to search against
        :param charge_type:  Type of Charge
        :param profile: Profile to search against
        :param regionCharges: Charges for each region
        :param daysInMonth: days used to determine monthly or daily charge
        :return: table of itemized contract charges
        """

        logging.info("Creating {} -- {} per Node charges for {}.".format(componentName, chargeName, appName))
        # Filter list of servers by role and account and profile
        if charge_type == "monthly":
            if profile == "any":
                listofservers = servers.query('(instance_role.str.contains(@role) and account_id == @appAccount and estimated and estimated_days == @daysInMonth)')
            else:
                listofservers = servers.query('(instance_role.str.contains(@role) and instance_profile == @profile and account_id == @appAccount and estimated_days == @daysInMonth)')
        else:
            if profile == "any":
                listofservers = servers.query('(instance_role.str.contains(@role) and account_id == @appAccount and estimated_days != daysInMonth)')
            else:
                listofservers = servers.query('(instance_role.str.contains(@role) and instance_profile == @profile and account_id == @appAccount and estimated_days != @daysInMonth)')

        if len(listofservers) == 0:
            logging.warning("No per node servers found for {} role={}, type={}, profile={}".format(appName,role,charge_type,profile))


        table = pd.DataFrame(listofservers,
                             columns=["region", "availability_zone", "estimated_days", "instance_id",
                                      "instance_name",
                                      "instance_profile"]).groupby(
            ['region', 'availability_zone', 'estimated_days', 'instance_id', 'instance_name', 'instance_profile'],
            sort=False,
            as_index=False).agg("count")

        table["contract_category"] = "{} - {}".format(componentName, chargeName)
        table["metric"] = type
        table["contract_rate"] = ""
        # Lookup contract charges from regionCharges variable
        for index, row in table.iterrows():
            # Get Contract Rate, calculate daily rate of rate * days
            contract_rate = (list(filter(lambda x:x["name"]==row["region"],regionCharges))[0]["contract_rate"])
            table.at[index, "period"] = charge_type
            if charge_type == "monthly":
                table.at[index, "contract_rate"] = contract_rate
                table.at[index, "unit_rate"] = "${:,.2f}".format(contract_rate)
            elif charge_type == "daily":
                """ Check for early provisioning flag and zero charges if <= early provisioning days specified"""
                if float(row["estimated_days"]) <= float(earlyProvisioning):
                    estimated_days = 0
                else:
                    estimated_days = row["estimated_days"]
                table.at[index, "contract_rate"] = float(contract_rate * estimated_days)
                table.at[index, "unit_rate"] = "${:,.2f}".format(contract_rate)
            else:
                logging.error("Invalid charge Type {} for application {} {} --{}.".format(charge_type,appName,componentName,chargeName))
                quit(1)

        if charge_type == "monthly":
            table["estimated_days"] = ""

        table1 = pd.DataFrame(table,
                              columns=["region", "availability_zone", "contract_category", "metric", "period", "estimated_days", "unit_rate", "contract_rate", "instance_id"]).groupby(['region', 'availability_zone', 'contract_category', 'metric', "period", "unit_rate", 'estimated_days'],
                                                        sort=False, as_index=False).agg({ "contract_rate": np.sum, "instance_id": "count"}).rename(columns={'instance_id': 'quantity'})

        return table1
    def calculatePerServiceInstanceCharges(appName, appAccount, componentName, chargeName, role, charge_type, service, metric, regionCharges):
        """
        Calculate Per Service Charges for Application
        :param appName: Name of Application
        :param appAccount: Cloud Account to search against
        :param componentName: Name of the Component being charged
        :param chargeName: Name of Charge
        :param role: Tagged Role to search against
        :param charge_type:  Type of Charge
        :param service: service id of service to identify
        :param metric: unique metric to count for charge quantity
        :param regionCharges: Charges for each region

        :return: table of itemized contract charges
        """

        logging.info("Creating {} -- {} per service instance charges for {}.".format(componentName, chargeName, appName))
        # Per service instance only support Monthly Charges.  Filter list of servers by role and account and profile
        if profile == "any":
            listofserviceinstances = services.query('(instance_role.str.contains(@role) and account_id == @appAccount and service_id == @service and metric == @metric)')
        else:
            listofserviceinstances = services.query('(account_id == @appAccount and service_id == @service and metric == @metric)')

        if len(listofserviceinstances) == 0:
            logging.warning("No service instances found for {} role={}, type={}, service={}, metric={}.".format(appName,role,charge_type,service, metric))

        table = pd.DataFrame(listofserviceinstances,
                             columns=["region", "availability_zone", "estimated_days", "instance_id",
                                      "instance_name",
                                      "instance_profile"]).groupby(
            ['region', 'availability_zone', 'estimated_days', 'instance_id', 'instance_name', 'instance_profile'],
            sort=False,
            as_index=False).agg("count")


        table["contract_category"] = "{} - {}".format(componentName, chargeName)
        table["metric"] = type
        table["contract_rate"] = ""
        table["estimated_days"] = ""
        # Lookup contract charges from regionCharges variable
        for index, row in table.iterrows():
            # Get Contract Rate, calculate daily rate of rate * days
            table.at[index, "period"] = charge_type
            contract_rate = (list(filter(lambda x:x["name"]==row["region"],regionCharges))[0]["contract_rate"])
            table.at[index, "contract_rate"] = contract_rate
            table.at[index, "unit_rate"] = "${:,.2f}".format(contract_rate)

        table1 = pd.DataFrame(table, columns=["region", "availability_zone", "contract_category", "metric", "period", "estimated_days", "unit_rate", "contract_rate", "instance_id"])\
                                .groupby(['region', 'availability_zone', 'contract_category', 'metric',"period", "unit_rate", 'estimated_days'],
                                sort=False, as_index=False).agg({"contract_rate": np.sum, "instance_id": "count"}).rename(columns={'instance_id': 'quantity'})

        return table1

    daysInMonth = calendar.monthrange(month.year, month.month)[1]
    billingMonth = datetime.strftime(month, "%Y-%m")
    """Query filters on last month to calculate counts of servers for all contract billing tabs."""
    servers = instancesUsage.query('month == @billingMonth and (service_id == "is.instance" and (metric == "VCPU_HOURS" or metric =="INSTANCE_HOURS_MULTI_TENANT")) or (service_id == "is.bare-metal-server" and metric == "BARE_METAL_SERVER_HOURS")')
    services = instancesUsage.query('month == @billingMonth and (service_id != "is.instance" or service_id != "is.bare-metal-server")')


    for application in applicationConfiguration:

        appName = application["name"]
        tabName = application["tab"]
        appAccount = application["account"]
        appComponents = application["components"]
        logging.info("Calculating {} contract charges for {}.".format(billingMonth,appName))

        """Initialize charges DataFrame"""
        charges = pd.DataFrame()

        for component in appComponents:
            componentName = component["name"]
            type = component["type"]
            """ Iterate through charges and determine if charge type calculations """
            for charge in component["charge"]:
                chargeName = charge["name"]
                role = charge["role"]
                charge_type = charge["type"]
                if "profile" in charge:
                    profile = charge["profile"]
                else:
                    profile = ""
                if "service" in charge:
                    service = charge["service"]
                else:
                    service = ""
                regionCharge = charge["region"]
                if "metric" in charge:
                    metric = charge["metric"]
                else:
                    metric = ""

                if type == "per_account":
                    """ Determine Per Account Charges """
                    if service != "":
                        """ Calculate charge based on whether compute instance exists """
                        charges = pd.concat([charges,
                                             calculateServicePerAccountCharges(appName, appAccount, componentName, chargeName, role,
                                                                       charge_type, service, regionCharge)])
                    else:
                        """ Calculate charge based on whether service instance exists """
                        charges = pd.concat([charges,
                                             calculatePerAccountCharges(appName, appAccount, componentName, chargeName, role,
                                                                       charge_type, profile, regionCharge)])
                elif type == "per_region":
                    """ Determine per Region charges """
                    if service != "":
                        """ Calculate per region charge if service exists in region """
                        charges = pd.concat([charges,
                                        calculateServicePerRegionCharges(appName, appAccount, componentName, chargeName, role,
                                                            charge_type, service, regionCharge)])
                    else:
                        """ Calculate per region charge if compute exists in any zone in region """
                        charges = pd.concat([charges,
                                        calculatePerRegionCharges(appName, appAccount, componentName, chargeName, role,
                                                            charge_type, profile, regionCharge)])

                elif type == "per_az":
                    """ Determine per AZ charges (compute only) """
                    charges = pd.concat([charges,
                                         calculatePerAzCharges(appName, appAccount, componentName, chargeName, role,
                                                               charge_type, profile, regionCharge)])
                elif type == "per_az_per_app":
                    """ Determine per AZ charges (compute only) """
                    charges = pd.concat([charges,
                                         calculatePerAzPerAppCharges(appName, appAccount, componentName, chargeName, role,
                                                               charge_type, profile, regionCharge)])
                elif type == "per_node":
                    """ Determine per node charges should be calculated """
                    charges = pd.concat([charges,
                                         calculatePerNodeCharges(appName, appAccount, componentName, chargeName, role,
                                                                 charge_type, profile, regionCharge, daysInMonth)])
                elif type == "per_service_instance":
                    """ Determine per service instance charges should be calculated requires (metric and role) need to be set """
                    charges = pd.concat([charges,calculatePerServiceInstanceCharges(appName, appAccount, componentName, chargeName, role,
                                                                 charge_type, service, metric, regionCharge)])
                else:
                    """ contract charge type not recognized """
                    logging.error("Unrecognized Charge Type of {} in {}.  Unable to generate billing data.".format(type, appName))
                    quit(1)

        """ Setup worksheet formatting"""
        format1 = workbook.add_format({'num_format': '$#,##0.00'})
        format2 = workbook.add_format({'align': 'left'})
        format3 = workbook.add_format({'num_format': '#,##0'})
        bold = workbook.add_format({'bold': True})
        sheet_name = tabName

        """Create Pivot for Charges by Region AZ """
        chargesPivot = pd.pivot_table(charges, index=["metric", "region", "availability_zone", "contract_category", "period", "unit_rate", "estimated_days", "quantity"],
                                      values=["contract_rate"],
                                      aggfunc={"contract_rate": np.sum},
                                      fill_value=0)

        """ Write Application ChargesPivot Table to Excel Tab"""

        totalCharges = charges["contract_rate"].sum()
        totalrows, totalcols = chargesPivot.shape
        chargesPivot.to_excel(writer, sheet_name=sheet_name, startrow=3, startcol=0)
        worksheet = writer.sheets[sheet_name]
        """ If earlyProvisioning flag specified and was used to calculate charges add Note to bottom of table """
        if float(earlyProvisioning) > 0 and 0 in charges["contract_rate"].unique():
            totalrows, totalcols = chargesPivot.shape
            boldtext = workbook.add_format({'bold': True})
            worksheet.write(totalrows + 5, 0, "Note: Early provisioning specified. Line item Contract Rates for per Node daily charges less than {} days were not calculated for {}".format(earlyProvisioning, billingMonth), boldtext)
        worksheet.write(0, 0, "{} for {}".format(appName, billingMonth), bold)
        worksheet.write(totalrows + 4, 8, totalCharges, format1)
        worksheet.set_column("A:A", 20, format2)
        worksheet.set_column("B:B", 15, format2)
        worksheet.set_column("C:C", 18, format2)
        worksheet.set_column("D:D", 50, format2)
        worksheet.set_column("E:E", 18, format2)
        worksheet.set_column("F:F", 18, format1)
        worksheet.set_column("G:H", 18, format3)
        worksheet.set_column("I:I", 18, format1)
        application["contractTotal"] = totalCharges
    return
def createReconciliation(accountUsage, month):
    """
    Create a reconcilation view that compare Account Usage Charges w/support against Citi billing categories
    :param accountUsage: dataframe of detailed usage information from Usage & Recource Controller
    :param month: month to calculate charges for
    :return:
    """
    global applicationConfiguration

    # Sum account service usage for each account by billing month
    billingMonth = month.strftime("%Y-%m")
    data = []

    for application in applicationConfiguration:

        appName = application["name"]
        appAccount = application["account"]
        contractCharges = application["contractTotal"]
        logging.info("Calculating {} Reconciliation for {}.".format(billingMonth,appName))

        """Sum discounted cost for usage charges"""
        usage = accountUsage.query('month == @billingMonth and account_id == @appAccount')["cost"].sum()

        """Sum rated_cost to calculate Support charges.  Support charges calculated as 10% of list usage @ 75% discount"""
        support = accountUsage.query('month == @billingMonth and account_id == @appAccount')["rated_cost"].sum() * .10 * .25

        """
        Build table & dataframe from usage and billing for reconciliation purposes
        """
        data.append({"Category": appName, "Billing": contractCharges, "Usage": usage, "Estimated_Support": support, "TotalUsage_w/support": usage + support, "Delta": contractCharges - usage - support })


    reconcile = pd.DataFrame(data, columns=['Category', 'Billing', 'Usage', 'Estimated_Support', 'TotalUsage_w/support', 'Delta'])
    reconcilePivot = pd.pivot_table(reconcile, index=["Category"],
                                    values=["Billing", "Usage", 'Estimated_Support', 'TotalUsage_w/support', 'Delta'],
                                    aggfunc={"Billing": np.sum, "Usage": np.sum, 'Estimated_Support': np.sum, 'TotalUsage_w/support': np.sum, 'Delta': np.sum},
                                    margins=True, margins_name="Total",
                                    fill_value=0)
    new_order = ['Billing', 'Usage', 'Estimated_Support', 'TotalUsage_w/support', 'Delta']
    reconcilePivot = reconcilePivot.reindex(new_order, axis=1)

    format1 = workbook.add_format({'num_format': '$#,##0.00'})
    format2 = workbook.add_format({'align': 'left'})
    bold = workbook.add_format({'bold': True})
    sheet_name = "RECONCILE"
    reconcilePivot.to_excel(writer, sheet_name=sheet_name, startrow=3, startcol=0)
    worksheet = writer.sheets[sheet_name]
    worksheet.write(0, 0, "Account Usage vs Application Billing for {}".format(billingMonth), bold)
    worksheet.set_column("A:A", 35, format2)
    worksheet.set_column("B:F", 18, format1)
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
    parser = argparse.ArgumentParser(description="Calculate Citi Usage and Billing per contract.")
    parser.add_argument("--conf", default=os.environ.get('conf', 'apps.yaml'), help="Filename for application configuraiton file (default = apps.yaml)")
    parser.add_argument("--output", default=os.environ.get('output', 'citiUsage.xlsx'), help="Filename for Excel output file. (include extension of .xlsx)")
    parser.add_argument("--early", default=os.environ.get('early', 0), help="Ignore early provisioning by specified number of day.")
    parser.add_argument("--cos", "--COS", action=argparse.BooleanOptionalAction, help="Upload output to COS.")
    parser.add_argument("--load", action=argparse.BooleanOptionalAction, help="Load dataframes from pkl files.")
    parser.add_argument("--save", action=argparse.BooleanOptionalAction, help="Store dataframes to pkl files.")
    parser.add_argument("--start", help="Start Month YYYY-MM.")
    parser.add_argument("--end", help="End Month YYYY-MM.")
    parser.add_argument("--month", help="Report Month YYYY-MM.")
    parser.add_argument("--COS_APIKEY", default=os.environ.get('COS_APIKEY', None), help="COS apikey to use to write output to Object Storage.")
    parser.add_argument("--COS_ENDPOINT", default=os.environ.get('COS_ENDPOINT', None), help="COS endpoint to use to write output tp Object Storage.")
    parser.add_argument("--COS_INSTANCE_CRN", default=os.environ.get('COS_INSTANCE_CRN', None), help="COS Instance CRN to use to write output to Object Storage.")
    parser.add_argument("--COS_BUCKET", default=os.environ.get('COS_BUCKET', None), help="COS Bucket name to use to write output to Object Storage.")
    args = parser.parse_args()
    applicationConfiguration = readAppConf(args.conf)

    if args.month != None:
        start = datetime.strptime(args.month, "%Y-%m")
        end = datetime.strptime(args.month, "%Y-%m")

    elif args.start != None and args.end != None:
        start = datetime.strptime(args.start, "%Y-%m")
        end = datetime.strptime(args.end, "%Y-%m")

    else:
        """ If no date range or month provided specify use month """
        start = datetime.today()
        end = datetime.today()
        start += relativedelta(months=-1)
        end += relativedelta(months=-1)

    earlyProvisioning = args.early
    """
    Verify does not include current in progress month
    """
    now = datetime.now()
    if end > now:
        logging.error("This usage report can not include future months.")
        quit(1)
    elif end.year == now.year and end.month == now.month:
        logging.error("This usage report can only be used with previous months.  Current month results are not complete until after the 2nd of the following month.")
        quit(1)

    if args.load:
        logging.info("Retrieving Usage and Instance data from stored data file")
        accountUsage = pd.read_pickle("accountUsage.pkl")
        instancesUsage = pd.read_pickle("instanceUsage.pkl")
    else:
        APIKEYS = os.environ.get('APIKEYS', None)
        if APIKEYS == None:
            logging.error("You must provide a list of IBM Cloud ApiKeys for each Citi Account using APIKEY environment variable, "\
                "they should be in list format containing the apikey and name for each account.  example [{'apikey': key, 'name': account_name}]")
            quit(1)
        else:
            """
            Convert to List of JSON variables
            """
            try:
                apikeys = json.loads(APIKEYS)
            except ValueError as e:
                logging.error("Invalid List of APIKEYS.  The list should be in the format " \
                    "containing an apikey and name for each account.  example [{'apikey': key, 'name': account_name}]")
                quit(1)

            """
            Establish Dataframes for data, and iterate through accounts.
            """
            instancesUsage = pd.DataFrame()
            accountUsage = pd.DataFrame()
            for account in apikeys:
                apikey = account["apikey"]
                createSDK(apikey)
                accountId = getAccountId(apikey)
                accountName = account["name"]
                logging.info("Retrieving Usage and Instance data from {} AccountId: {}.".format(accountName, accountId))
                """
                Pre-populate Account Data to accelerate report generation
                """
                tag_cache = prePopulateTagCache()
                resource_cache = prePopulateResourceCache()

                """
                Pull Account Usage from Start to End Months at Account Summary and Instance Detail level
                """
                accountUsage = pd.concat([accountUsage, getAccountUsage(start, end)])
                instancesUsage = pd.concat([instancesUsage, getInstancesUsage(start, end)])

                """
                Save Datatables for report generation testing (use --LOAD to reload without API pull)
                """
            if args.save:
                accountUsage.to_pickle("accountUsage.pkl")
                instancesUsage.to_pickle("instanceUsage.pkl")

    """
    Generate Excel Report based on data pulled
    """
    # set variables to track billing for RECONCILE tab
    commonBilling = 0
    aceBilling = 0
    simplicitiBilling = 0

    # Write dataframe to excel
    output = args.output
    split_tup = os.path.splitext(args.output)
    """ remove file extension """
    file_name = split_tup[0]
    timestamp = "_(run@{})".format(datetime.now().strftime("%Y-%m-%d_%H:%M"))

    writer = pd.ExcelWriter(file_name + ".xlsx", engine='xlsxwriter')
    workbook = writer.book
    createServiceDetail(accountUsage)
    createInstancesDetailTab(instancesUsage)
    createUsageSummaryTab(accountUsage)
    createMetricSummary(accountUsage)
    createTrueUp(accountUsage, end)
    createVcpuTab(instancesUsage, end)
    createBMvcpuTab(instancesUsage, end)
    createProvisionAllTab(instancesUsage, end)
    createProvisionWorkersTab(instancesUsage, end)
    createProvisionScaleTab(instancesUsage, end)
    createApplicationChargesTabs(instancesUsage, end)
    createReconciliation(accountUsage, end)
    writer.close()
    """ If --COS then copy files with report end month + timestamp to COS """
    if args.cos:
        """ Write output to COS"""
        logging.info("Writing Pivot Tables to COS.")
        writeFiletoCos(file_name + ".xlsx", file_name + "_" + datetime.strftime(end, "%Y-%m") + timestamp + ".xlsx")
    logging.info("Billing Report is complete.")
