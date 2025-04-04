# Citi License Usage Report
*licenseReport.py* using resource controller, VPC, and Tagging Data from each of the specified accounts calculates the number of required licenses for Spectrum Symphony, Spectrum Scale, Guardium Key License Manager,
Red Hat, and Microsoft.

## Table of Contents
1. [Identity and Access Management Requirements](#identity-&-access-management-requirements)
2. [Output Description](#output-description)
3. [Script Installation](#script-installation-instructions)
3. [Script Execution](#script-execution-instructions)
4. [Other included Scripts](other-scripts.md)
5. [Code Engine: Configuring licenseManagement to automatically produce output each week](#running-report-as-a-code-engine-job)

### Required Files
 Script          | Description
|-----------------| -----------
| licenseReport.py | Export usage detail by usage month to an Excel file.
| requirements.txt | Package requirements
| Dockerfile      | Docker Build File used by Code Engine to build container
| logging.json    | LOGGER config used by script
| .env            | Used to specify environment variables such as APIKEYS


### Identity & Access Management Requirements
| APIKEY | Description | Min Access Permissions
| ------ | ----------- | ----------------------
| IBM Cloud API Key | API Key for each account with access to usage | IAM Billing Viewer Role 
| COS API KEY | API Key with WRITER permissions to destination bucket

## Output Description
*licenseReport.py* creates two files locally.  The first is a Pipe Delimited CSV file with the detail data for each Virtual Server and Bare Metal Server in the accounts specified.   The second is an XLSX file with
Pivots for each of the license categories.   These files can be uploaded to COS, and SFTP Server or both.  The --output parameter can be used to change the name of the file.  For COS and SFTP the files are appended
with the timestamp from when the report was run.

### Excel Pivot Tabs
| Tab Name          | Description of Tab 
|-------------------|-------------------------------------------------------------
| Spectrum Symphony |  Table of the Spectrum Symphony Licenses deployed on workers and amster nodes in each of the accounts.
| Scale & GKLM Licenses | 3 tables,  First are the Scale Licenses deployed on Virtual Servers with Total Storage,  Second is the Bare Metal Scale LIcenses with total RAW Storage, and third is the count of GKLM Servers.
| Microsoft Licenses | A table by Microsoft OS Version deployed and total vCPU count
| RedHat Licenses | 2 tables.  First the RHEL Licenses deployed on Virtual Servers per account and total vCPU.  Second Total Bare Metal Server RHEL licenses deployed per account with total server Counts by 2 vs 4 Socket servers
* Note: Pivot Data only counts BYOL licenses  


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

4.  Include additional environment variables for COS and/or SFTP in ***.env*** or as parameters.
```azure
COS_ENDPOINT='https://s3.us-south.cloud-object-storage.appdomain.cloud'
COS_INSTANCE_CRN=crn:v1:bluemix:public:cloud-object-storage:global:a/7a24585774d8b3c897d0c9b47ac48461:76f5e20f-96ea-4102-b106-0ac5906a7590
COS_APIKEY=xyz
COS_BUCKET=licenses
SFTP_USERNAME=root
SFTP_HOSTNAME='myftpserver.ibm.com'
SFTP_PRIVATE_KEY='~/mysecrets/id_rsa'
SFTP_PUBLIC_KEY='ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDQ6H4W/5PCtVb6BEgbxNdgDbrJsAFD/Y13mz+qVhM6kHmoOBu5tbbQh7LGfCjpHzZ2A59m2i3zpFNwA9r06UErIfG8U020QAnirrmpo1qqB9tMI7BRSyvf5NFXnUklyszQSsXxxM6eYiQLiHDNnVN7Qyzgq5YcZ8eb559KzmyretdPulEBQvWKZyUbE03kX8ScNTI87p/jX/464viudryjtLgUNuJoFtCCYdoolvnNZsAq3wBl9LOgNaT33nP1ys1R4azG3pC921WX5+g4txws7tVzjPB/e5caOYdGXbFnYi2TXY3agX0wCNj/p/nPEO29c7s7kzEZN9o8ygSrj+Yn'

```

## Script Execution Instructions

| Parameter                | Environment Variable | Default               | Description                   
|--------------------------|----------------------|-----------------------|-------------------------------
| --debug                  |                      | --no-debug            | Use debug Level for logging
| --cos, --COS             |                      | --no-cos              | Upload output to COS buckjet specified
| --sftp, --SFTP           |                      | --no-sftp             | Upload output to SFTP Server specified
| --COS_APIKEY             | COS_APIKEY           | None                  | COS API to be used to write output file to object storage, if not specified file written locally. 
| --COS_BUCKET             | COS_BUCKET           | None                  | COS Bucket to be used to write output file to. 
| --COS_ENDPOINT           | COS_ENDPOINT         | None                  | COS Endpoint (with https://) to be used to write output file to. 
| --COS_INSTANCE_CRN       | COS_INSTANCE_CRN     | None                  | COS Instance CRN to be used to write output file to.  
| --SFTP_USERNAME          | SFTP_USERNAME        | None                  | SFTP User Name for Authentication
| --SFTP_HOSTNAME          | SFTP_HOSTNAME        | None                  | SFTP Server Hostname or IP Address
| --SFTP_PRIVATE_KEY       | SFTP_PRIVATE_KEY     | None                  | Location of SFTP Private Key (file) to be used for authentication
| --SFTP_PUBLIC_KEY        | SFTP_PUBLIC_KEY      | None                  | SFTP Public Key of Server to be Authenticated by (i.e. Known Hosts)
| --SFTP_PATH              | SFTP_PATH            | None                  | SFTP destination path for file
| --output                 | output               | invoice-analysis.xlsx | Output file name used


```bazaar
usage: licenseReport.py [-h] [--output OUTPUT] [--debug | --no-debug] [--cos | --no-cos | --COS | --no-COS] [--sftp | --no-sftp] [--COS_APIKEY COS_APIKEY] [--COS_ENDPOINT COS_ENDPOINT] [--COS_INSTANCE_CRN COS_INSTANCE_CRN]
                        [--COS_BUCKET COS_BUCKET] [--SFTP_USERNAME SFTP_USERNAME] [--SFTP_HOSTNAME SFTP_HOSTNAME] [--SFTP_PRIVATE_KEY SFTP_PRIVATE_KEY] [--SFTP_PUBLIC_KEY SFTP_PUBLIC_KEY] [--SFTP_PATH SFTP_PATH]

Determine License Usage.

options:
  -h, --help            show this help message and exit
  --output OUTPUT       Filename Excel output file. (including extension of .xlsx)
  --debug, --no-debug   Set Debug level for logging.
  --cos, --no-cos, --COS, --no-COS
                        Write output to COS bucket destination specified.
  --sftp, --no-sftp     Write output to SFTP destination specified.
  --COS_APIKEY COS_APIKEY
                        COS apikey to use to write output to Object Storage.
  --COS_ENDPOINT COS_ENDPOINT
                        COS endpoint to use to write output tp Object Storage.
  --COS_INSTANCE_CRN COS_INSTANCE_CRN
                        COS Instance CRN to use to write output to Object Storage.
  --COS_BUCKET COS_BUCKET
                        COS Bucket name to use to write output to Object Storage.
  --SFTP_USERNAME SFTP_USERNAME
                        SFTP User Name for Authentication.
  --SFTP_HOSTNAME SFTP_HOSTNAME
                        SFTP Server Hostname or IP Address.
  --SFTP_PRIVATE_KEY SFTP_PRIVATE_KEY
                        CSFTP Private Key for User to be Authenticated.
  --SFTP_PUBLIC_KEY SFTP_PUBLIC_KEY
                        SFTP Public Key of Server to be Authenticated by (Not user Public Key)
  --SFTP_PATH SFTP_PATH
                        SFTP destination path for file



```

## Running Report as a Code Engine Job
Requirements
* Creation of an Object Storage Bucket to store the script output in at execution time.  Because Code Engine instances don't persist you must use either COS or SFTP (or both) as a destination for report output.
* Creation of an IBM Cloud Object Storage Service API Key with read/write access to bucket above
* Creation of an IBM Cloud API Key with View access to Resource Controller and VPC Resources

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
   - Select an existing namespace or enter a name for a new one, for example, newnamespace. 
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
    - If using SFTP you must create a Secret using CLI
      - Log in to the CLI for appropriate account, region and resource group where Code Engine will run and select Code Engine Project you just created.
      ```
      ibmcloud login --apikey <<myapikey>> -r us-south -g vpc_prod
      ibmcloud ce project select --name "License Management"
      ```
      - Create a secret volume from the local private key file
      ```
      ibmcloud ce secret create --name sec-secret-vol --from_file ~/.ssh/id_rsa
      ```
      - Attach the secret as a volume to the code engine job so it can be accessed by yhe script at runtime.
      ```
      ibmcloud ce job modify --name licenseReport --mount-secret /mysecrets=sec-secret-vol
      ```
    - Note: /mysecrets becomes the path for the private key, the file name (key) is thef private key file name.  This should be specified in the SFTP_PRIVATE_KEY variable (ie /mysecerts/id_rsa)
3. Choose the job previously created.  
   - Click on the Environment variables tab.   
   - Click add, choose reference to full configmap, and choose configmap created in previous step and click add.  
   - Click add, choose reference to full secret, and choose secrets created in previous step and click add.  

4. Specify Any command line parameters using Command Overrides.<br>
   - Click Command Overrides (see tables above) <br>
   - Under Arguments section specify command line arguments with one per line.
    ```
    --cos
    --sftp
    ```
5. To configure the report to run at a specified date and time configure an Event Subscription.
   - From Project, Choose Event Subscription
   - Click Create
   - Choose Event type of Periodic timer
   - Name subscription; click Next
   - Select cron pattern or type your own.  
   - he following pattern will run the job at 07 UTC (2am CDT) on the 20th of every month. 
    ```
    00 07  20 * *
    ```
   - Click Next
   - Leave Custom event data blank, click Next.
   - Choose Event Consumer.  Choose Component Type of Job, Choose The Job Name for the job you created in Step 1.   Click Next.
   - Review configuration Summary; click create.
6. To Run report "On Demand" click ***Submit job***
