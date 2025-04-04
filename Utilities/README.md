# IBM Cloud Utilities for HPC-as-a-Service @ Citi
*citiUsage* collects IBM Cloud Usage metrics (Summary and Instance Data) and determines the correct billing metrics based on the Citi Contracts additionally it aids in calculating variable usage allocations to determine if there are quarterly TrueUp charges required.


### Required Files
Script | Description
------ | -----------
currentMonthUsage.py | Create a report of current month to date usage and a list of symphony-workers by provisioning date that are currently active in the account.
attachTag.py    | Attah audit tags to servers
missingBillableItems.py | Detect CRNs from resource controller that are missing billign usage records
requirements.txt | Package requirements
logging.json | LOGGER config used by script
.env | (optional) to specify environment variables such as APIKEYS


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





## currentMonthUsage.py
### Output Description
An Excel worksheet is created with a table of month to date service and service metric usage for each account.
Additionally a table of symphony-worker and scale-storage servers by provisioning date is created to aid in project deployment management.

### Excel Tabs
1. ***UsageSummary*** tab shows month to date usage for all services by account.  Note: usage up to date and time is displayed at top of worksheet.
2. ***MetricPlanSummary*** shows month to date usage of all servie metrics by account.  Note: usage up to date and time is displayed at top of worksheet.
3. ***SymphonyWorkerVCPU*** is a summary view of # of Symphony Worker Instances and vCPU for VSIs by  Account, Region, and AZ.
4. ***ScaleBareMetalCores*** is a summary view of the # of Scale Storage nodes and the associated cores & Sockets by Account, Region and AZ.
5. ***ProvisionDateAllRoles*** is a summary of Virtual Servers and Bare Metal Servers by account, availability zone, instance_role and provisioning date.  This is only calculated for the last month specified.
6. ***ProvisionDateScaleRole*** is a summary of Bare Metal Servers used as Scale Nodes by account, availability zone, and provisioning date.
7. ***ProvisionDateWorkerRole*** is a summary of Virtual Servers used as Symphony-Workers by account, availability zone, and provisioning date.
8. ***ServerDetail*** this tab is the detail of active virtual servers in the specified accounts. 
<br><br>
```azure
python currentMonthUsage.py --help

usage: currentMonthUsages.py [-h] [--output OUTPUT] 

Calculate Citi Usage.

options:
  -h, --help           show this help message and exit
  --output OUTPUT      Filename Excel output file. (including extension of .xlsx)

python currentMonthUsage.py --output currentMonthUsage.xlsx
```
## attachTag.py
### Input Description
An Excel worksheet serves as the input file for user tags to be added to existing VPC Server Instances (Virtual and BM).
Ideally the input file is first created using *currentMonthUsage.py* script.   Next a new column named *new_tags* should be added to the *ServerDetail* tab.
This column should be populated wit a comma delimited list of valid IBM Cloud tags which should be added to the specific
server instances (as specified by each rows  *accound_id* and *instance_id* columns).    All other columns on this tab are ignored.
Tags are additive and do not replace existing tags.  Those should be detached via the portal or command line.   The error log will capture any invalid rows in file
such as invalid tags, unrecognized instance_id or invalid account_id.

### Notes: 
- IBM Cloud Tag Documentation: https://cloud.ibm.com/docs/account?topic=account-tag&interface=ui
- Tags should include a key:pair combination in the *new_tags* column
- IBM Cloud Tags must be less than 128 characters each
- The permitted characters are A-Z, 0-9, spaces, underscore, hyphen, period, and colon.
- All valid tags will be attached to server.  However, these billing scripts only parse tags starting with role: and audit: 

```azure
python attachTag.py --help

usage: attachTag.py [-h] [--input INPUT] 

Attach Tags to Servers in VPC.

options:
options:
  -h, --help           show this help message and exit
  --input INPUT        Filename Excel input file for list of resources and tags. (including extension of .xlsx)
  --debug, --no-debug  Set Debug level for logging.

python attachTag.py --input currentMonthUsage.xlsx
```