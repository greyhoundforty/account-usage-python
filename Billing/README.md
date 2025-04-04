# IBM Cloud Usage Script for HPC-as-a-Service @ Citi
*citiUsage* collects IBM Cloud Usage metrics (Summary and Instance Data from resource controller ) and determines the correct billing metrics based on the Citi Contracts additionally it aids in calculating variable usage allocations to determine if there are quarterly TrueUp charges required.

## Billing Methodology
The Citi account structure is one common account for common management components and then one account per HPC workload.   Accross all workloads the common management charges are described in the Ace/Simpliciti contract as a per zone charge per month.   Then each contract
described a Per App Per Zone charge(s) for each HPC application deployed to that zone in the associated account.   Additionally, each contract specifies specific per node charges for a specific instance profile.  In the case of ACE, this also includes VPC Bare Metal Profiles.
These charges are calculated either on a daily or monthly bases.   For the purpose of billing daily charges are the total ROUNDUP(hours / 24,0).  In other words the number of hours is divided by 24 and rounded up to the next full day.   If the number of days = days in the month
then the monthly rate is charged instead.

### Required Files
Script | Description
------ | -----------
cituiUsage.py| Export usage detail by usage month to an Excel file.
requirements.txt | Package requirements
Dockerfile      | Docker Build File used by Code Engine to build container
apps.yaml | Contract Billing COnfiguration
logging.json | LOGGER config used by script
.env | specify environment variables such as APIKEYS in this file


### Identity & Access Management Requirements
| APIKEY | Description | Min Access Permissions
| ------ | ----------- | ----------------------
| IBM Cloud API Key | API Key for each account with access to usage | IAM Billing Viewer Role 

### Installation Instructions & Requirements
1. Python 3.9+ required 
2. Install required packages  
````
pip install -r requirements.txt
````
3. APIKEYS for each account are required; store a list of apikey/name keypairs for each account in JSON format in the ***.env*** file located in the script execution directory or specify via command line using --APIKEYS<br>
```
APIKEYS='[
 {"apikey": "apikey1", "name": "Citi - HPC Common"},
 {"apikey": "apikey2", "name": "Citi - HPC ACE"},
 {"apikey": "apikey3", "name": "Citi - HPC Simpliciti"},
 {"apikey": "apikey4", "name": "Citi - HPC Equities"},
 {"apikey": "apikey5", "name": "Citi - HPC Commodities"}
 ]'
```

4.  Modify apps.yaml to match contract billing items and rates.    Each HPC application must have a name, tab name, account, allocation and list of billing components.   Each component should have a name, type (per_az, per_az_per_app, or per_node) and the associated charge
detail.   Each charge, should have a name, type (daily, monthly) and the role tag used for the resource, and profile type (use any if not specific to one profile).   The charge should be specified for each by region.  All regions must be configured to bill correctly.
```azure
- name: Common Application Services
  tab: CommonAppServices
  account: d648f13245d9401883148f3320cee0c8
  components:
    - name: Management Other
      type: per_az
      charge:
        - name: Base Platform
          role: symphony-master
          type: monthly
          profile: any
          region:
            - name: us-east
              contract_rate: 271.05
            - name: us-south
              contract_rate: 271.05
            - name: ca-tor
              contract_rate: 259.79
    - name: DirectLink
      type: per_service_instance
      charge:
        - name: Direct Link Connect 5 Gbps
          role: any
          type: monthly
          service: 86fb7610-0f92-11ea-a6a3-8b96ed1570d8
          metric: INSTANCE_FIVEGBPS_UNMETERED_PORT
          region:
            - name: us
              contract_rate: 1469.54
            - name: ca
              contract_rate: 1469.54
        - name: Direct Link Connect 1 Gbps
          role: any
          type: monthly
          service: 86fb7610-0f92-11ea-a6a3-8b96ed1570d8
          metric: INSTANCE_ONEGBPS_UNMETERED_PORT
          region:
            - name: us
              contract_rate: 516.89
            - name: ca
              contract_rate: 516.89
            - name: eu
              contract_rate: 516.89
- name: Common Reporting Services
  tab: CommonReportServices
  account: c8b43f39d8524b11bde8c0a1c1767159
  components:
    - name: Reporting Service
      type: per_account
      charge:
        - name: Common Reporting
          role: any
          type: monthly
          service: dashdb-for-transactions
          region:
            - name: any
              contract_rate: 25424.00
- name: ACE Application Services
  tab: AceAppServices
  account: 7d9c34bc0f0c40efb78ec1407588dba5
  allocation: 11.69
  components:
    - name: SMC Component
      type: per_account
      charge:
        - name: SMC Console
          role: smc
          type: monthly
          profile: any
          region:
            - name: any
              contract_rate: 2720.00
    - name: Other Operational Component
      type: per_az_per_app
      charge:
        - name: Compute Essential
          role: symphony-master
          type: monthly
          profile: any
          region:
            - name: us-east
              contract_rate: 2610.77
            - name: us-south
              contract_rate: 2610.77
            - name: ca-tor
              contract_rate: 2690.56
        - name: Storage Essential
          role: scale-gui
          type: monthly
          profile: any
          region:
            - name: us-east
              contract_rate: 2300.23
            - name: us-south
              contract_rate: 2300.23
            - name: ca-tor
              contract_rate: 2367.97
    - name: Compute Clusters
      type: per_node
      charge:
        - name: Compute Node 1 Monthly
          role: symphony-worker
          type: monthly
          profile: mx2-32x256-cl
          region:
            - name: us-east
              contract_rate: 461.88
            - name: us-south
              contract_rate: 461.88
            - name: ca-tor
              contract_rate: 475.85
        - name: Compute Node 1 Daily
          role: symphony-worker
          type: daily
          profile: mx2-32x256-cl
          region:
            - name: us-east
              contract_rate: 16.69
            - name: us-south
              contract_rate: 16.69
            - name: ca-tor
              contract_rate: 17.20
    - name: Storage Clusters
      type: per_node
      charge:
        - name: Storage Node 1 Monthly
          role: scale-storage
          type: monthly
          profile: bx2d-metal-192x768
          region:
            - name: us-east
              contract_rate: 2833.00
            - name: us-south
              contract_rate: 2833.00
            - name: ca-tor
              contract_rate: 2921.62
        - name: Storage Node 1 Daily
          role: scale-storage
          type: daily
          profile: bx2d-metal-192x768
          region:
            - name: us-east
              contract_rate: 103.07
            - name: us-south
              contract_rate: 103.07
            - name: ca-tor
              contract_rate: 106.29
        - name: Storage Node 2 Monthly
          role: scale-storage
          type: monthly
          profile: bx2d-metal-96x384
          region:
            - name: us-east
              contract_rate: 2833.00
            - name: us-south
              contract_rate: 2833.00
            - name: ca-tor
              contract_rate: 2921.62
        - name: Storage Node 2 Daily
          role: scale-storage
          type: daily
          profile: bx2d-metal-96x384
          region:
            - name: us-east
              contract_rate: 103.07
            - name: us-south
              contract_rate: 103.07
            - name: ca-tor
              contract_rate: 106.29
```
### Charge Type Definitions
* ***per_account*** - charges the contract rate (only once) if at least one instance of the service or compute matching the tag exists in any region or zone.
* ***per_region*** - charges the contract rate (only once) per region if at least on instance of the service or compute matchin the tag exists in that region.
* ***per_az*** - charges the contract rate (only once) per zone if at least on instance of compute matching the tag exists in any account
* ***per_az_per_app*** - charges the contract rate (only once) per zone if at least on instance of compute matching the tag exists in specified account
* ***per_node*** charges the contract rate per compute node matching the tag and profile in specified account.
* ***per_service_instance*** charges the contract rate per service instance matching the tag and service_id in specified account.
### other parameter definitions
* ***name*** - Application or Charge name shown in output
* ***tab*** - Excel Tab name for charges to appear on
* ***account*** - Account_Id to filter on
* ***allocation*** - Amount of allocation per node to use in TrueUp calculation
* ***components*** - List of Components to charge for specified account.
* ***service*** service_id for charge to be based on
* ***profile*** compute profile for charge to be based on (any = wildcard)
* ***role*** role: tag to base compute or service charge on.

## citiUsage.py
### Output Description
An Excel worksheet is created with multiple tabs from the collected data formatted consistent with contract billing terms.

### Excel Tabs
1. ***ServiceUsageDetail*** is a summary view of usage data for each service and each month specified
2. ***Instances_detail*** is a detail view of usage data for each instance of each service for each month specified.
3. ***UsageSummary*** is a summary view of Each Account, Each Service, for Each Month showing Rated Cost (list), and Cost (discounted price)
4. ***MetricPlanSummary** is a summary view of Each Account, Each Service, Plan, and Metric showing Quantity and Cost (discounted price) for each month
5. ***SymphonyWorkerVCPU*** is a summary view of # of Symphony Worker Instances and vCPU for VSIs by  Account, Region, and AZ.
6. ***ScaleBareMetalCores*** is a summary view of the # of Scale Storage nodes and the associated cores & Sockets by Account, Region and AZ.
7. ***ProvisionDateAllRoles*** is a summary of Virtual Servers and Bare Metal Servers by account, availability zone, instance_role and provisioning date.  This is only calculated for the last month specified.
8. ***ProvisionDateScaleRole*** is a summary of Bare Metal Servers used as Scale Nodes by account, availability zone, and provisioning date.
9. ***ProvisionDateWorkerRole*** is a summary of Virtual Servers used as Symphony-Workers by account, availability zone, and provisioning date.
10. ***TrueUp*** calculates the variable usage as specified in Appendix F - Table 14.  This is only calculated for the last month specified.
11. ***APP_AppServices_*** is a Contract Billing Tear Sheet for each application  This is only calculated for the last month specified.
12. ***RECONCILE*** Compares Contract Billing against actual account Usage and Support Charges.  Billing should be greater than Usage+Support.  This is only calculated for the last month specified.
<br><br>
***Caveats***
- A range of months can be specified with (--start --end) or a single month with (--month);  Specify dates with YYYY-MM format
- Script is intended to be run using full months; the inclusion of a current month (an incomplete month) will result in error.
- If a range of months is specfiied, Usage Summary and MetricPlanSummary views will provide month to month comparisons.
- All other tabs, including the Billing Tear Sheets are calculated for only the last month specified if range is provided/
- The --early command line parameter can be used to specify a threshold for the number of days to supress calculation of the daily
rate.  If the actual usage days in a given month is less than or equal to the specified --early parameter a contract daily rate will not
be calculated for those servers.
- For example if you wish to provision servers on the June 25th, but not plan to charge until July you would specify --early 5  These
servers would not generate a contract daily rate for the 5 days in the month and will be billed the monthly rate on the 
next monthly invoice. 
- When using --early, the line item details will still be shown, but the contract_rate will be zero.  At the bottom of the table for those
applications the parameter affected, it will be noted that these items were not calculated because of the use of --early.
- --early only affects per node calculations; and affects all applications if specified.  It nodes not impact per_az or per_az_app charges as
they only have a monthly rate.


```
usage: citiUsage.py [-h] [--conf CONF] [--output OUTPUT] [--early EARLY] [--cos | --no-cos | --COS | --no-COS] [--start START] [--end END] [--month MONTH] [--COS_APIKEY COS_APIKEY] [--COS_ENDPOINT COS_ENDPOINT]
                    [--COS_INSTANCE_CRN COS_INSTANCE_CRN] [--COS_BUCKET COS_BUCKET]

Calculate Citi Usage and Billing per contract.

options:
  -h, --help            show this help message and exit
  --conf CONF           Filename for application configuraiton file (default = apps.yaml)
  --output OUTPUT       Filename for Excel output file. (include extension of .xlsx)
  --early EARLY         Ignore early provisioning by specified number of day.
  --cos, --no-cos, --COS, --no-COS
                        Upload output to COS.
  --start START         Start Month YYYY-MM.
  --end END             End Month YYYY-MM.
  --month MONTH         Report Month YYYY-MM.
  --COS_APIKEY COS_APIKEY
                        COS apikey to use to write output to Object Storage.
  --COS_ENDPOINT COS_ENDPOINT
                        COS endpoint to use to write output tp Object Storage.
  --COS_INSTANCE_CRN COS_INSTANCE_CRN
                        COS Instance CRN to use to write output to Object Storage.
  --COS_BUCKET COS_BUCKET
                        COS Bucket name to use to write output to Object Storage.


python citiUsage.py --start 2022-06 --end 2022-08 --output citiUsage.xlsx
```
## Running Billing Report as a Code Engine Job
Requirements
* Creation of an Object Storage Bucket to store the script output at execution time.  
* Creation of an IBM Cloud Object Storage Service API Key with read/write access to bucket above
* Creation of an IBM Cloud API Keys with View access to Billing and Resource Controller for all resources for each account

### Setting up IBM Code Engine to run report from IBM Cloud Portal
1. Open IBM Cloud Code Engine Console from IBM Cloud Portal (left Navigation)
2. Create project, build job and job.
   - Select Start creating from Start from source code.  
   - Select Job  
   - Enter a name for the job such as licenseReport. Use a name for your job that is unique within the project.  
   - Select a project from the list of available projects of if this is the first one, create a new one. Note that you must have a selected project to deploy an app.  
   - Enter the URL for this GitHub repository and click specify build details. Make adjustments if needed to URL and Branch name. Click Next.  
   - Select Dockerfile for Strategy, Dockerfile for Dockerfile, 10m for Timeout, and Medium for Build resources. Click Next.  
   - Select a container registry location, such as IBM Registry, Dallas.  
   - Select Automatic for Registry access.  
   - Select an existing namespace or enter a name for a new one, for example, mynamespace. 
   - Enter a name for your image and optionally a tag.  
   - Click Done.  
   - Click Create.  
2. Create ***configmaps*** and ***secrets***.  
    - From project list, choose newly created project.  
    - Select secrets and configmaps  
    - Click create, choose config map, and give it a name. Add the following key value pairs    
      - ***COS_BUCKET*** = Bucket within COS instance to write report file to.  
      - ***COS_ENDPOINT*** = Public COS Endpoint (including https://) for bucket to write report file to  
      - ***COS_INSTANCE_CRN*** = COS Service Instance CRN in which bucket is located.<br>
    - Select secrets and configmaps (again)
    - Click create, choose secrets, and give it a name. Add the following key value pairs
      - ***APIKEYS*** = an list of Accounts and APIKEYS.  See ***.env*** example above for format.
      - ***COS_APIKEY*** = your COS Api Key with writer access to appropriate bucket
3. Choose the job previously created.  
   - Click on the Environment variables tab.   
   - Click add, choose reference to full configmap, and choose configmap created in previous step and click add.  
   - Click add, choose reference to full secret, and choose secrets created in previous step and click add.  

4. Specify Any command line parameters using Command Overrides.<br>
   - Click Command Overrides (see tables above) <br>
   - Under Arguments section specify command line arguments with one per line.
    ```
    --cos
    ```
5. To configure the report to run at a specified date and time configure an Event Subscription.
   - From Project, Choose Event Subscription
   - Click Create
   - Choose Event type of Periodic timer
   - Name subscription; click Next
   - Select cron pattern or type your own.  
   - he following pattern will run the job at 07 UTC (2am CDT) on the 3rd of every month. 
    ```
    00 07  03 * *
    ```
   - Click Next
   - Leave Custom event data blank, click Next.
   - Choose Event Consumer.  Choose Component Type of Job, Choose The Job Name for the job you created in Step 1.   Click Next.
   - Review configuration Summary; click create.
6. To Run report "On Demand" click ***Submit job***