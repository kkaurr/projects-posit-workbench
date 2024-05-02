## RStudio content repository

## Initialising repository
Follow steps in [init.md](./init.md)

## New RStudio project

Start a new R Session. Choose to create a new project. Choose to checkout from git and choose this repository. Make sure you have a generated private/public key and have configured your github with it from tools -> global options -> git.

### Creating a new app

Directories with `manifest.json` are considered deployable apps.

1. File -> New File
2. Choose a sub-directory
3. Generate manifest
#### R based content:
```R
setwd("shiny-test-app")
install.packages("rsconnect")
rsconnect::writeManifest()
```

#### For Python-based content
You can use the `rsconnect-python` package to create the manifest.json. Ensure that the Python environment you are using for your project is activated, then create a manifest specific to your type of project (`notebook`, `api`, `dash`, `bokeh`, or `streamlit`):

Terminal:
```bash
pip install rsconnect-python
~/.local/bin/rsconnect write-manifest ${TYPE} --overwrite .
```

**Note: Everytime you add more files or dependencies to the app, make sure to generate the manifest and checking it into github.**


### Removing an app

Removing `manifest.json` from an app directory (or deleting the app directory itself) will undeploy an app from Connect.

## Deploying apps to Conenct

First, user needs to generate an API key within RStudio Connect.  
[<img src=".images/RS_Connect.png" width="800"/>](.images/RS_Connect.png)  
[<img src=".images/RS_Connect_API.png" width="450"/>](.images/RS_Connect_API.png)  
[<img src=".images/RS_Connect_API_createkey.png" width="650"/>](.images/RS_Connect_API_createkey.png)  

Build pipeline asks for API key of the commit author if it is not already stored in `/latitude/data/rstudio/connect/api_keys`. Set environment variable `OVERWRITE_CONNECT_API_KEY` to `true` when triggering a new build in BuildKite to overwrite the existing stored API key for author.


## Detecting if the code is running in Connect

Environment variable `R_CONFIG_ACTIVE` provides the value `rsconnect` when code is run in Connect and can be used to detect.

# Connecting to datasources

## Snowflake

RStudio Workbench and Connect have been configured with ODBC data sources. The following datasources are available:

|Team|Datasource DSN|Workspace db env variable|
|----------|----------|----------|
|Customer analytics|`SNOWFLAKEC360`|`CUSTOMER_ANALYTICS_WORKSPACE_DB`|
|Risk Modelling|`RISKMODELLING`|`RISK_MODELLING_WORKSPACE_DB`|


### Personal snowflake token

Use your personal credentials in Workbench. Connect will use preconfigured credentials.
Create a file `.Renviron` inside `##project##` driectory. Set you snowflake credendtials in it.

e.g.
```bash
SNOWFLAKE_C360_TOKEN='ver:1-hint:33625302831438-ETMsDgAAAXwku9CNABRBRVMvQ0JDL1BLQ1M1UGFkZGluZwEAABAAECxrCVMHf6nXe2TItAzESZAAAABgvtxqFRlOGz7TJ/2wTAX7XlGjz4VDffCYvfV6OkHItjzXHbm9ZFbwnzqJHKLeBA1oeLXKlYDEjhsO3r/XEOkGjdckNeIeWlQ6hEX/PuYosj8GSXRiCznFPbH44mcWPYKrABT+Z9uwz5XwgVqMWHXiZKBzXTzciw=='
SNOWFLAKE_UID=700005763@latitudefs.com
```

### Connecting to snowflake

Use pre-configured ODBC datasources where `uid` and `token` are set conditionally if they are availale as environment variables. This is to ensure the code is portable when it gets deployed to Connect where credentials are embedded for a service account and the user's personal token is not used.

#### Connecting to datasource in `R`

```R
library(DBI)
database <- "PD_CUSTOMER_ANALYTICS"
myconn <- DBI::dbConnect(odbc::odbc(), "SNOWFLAKEC360", 
                         database = "PD_CUSTOMER_ANALYTICS", 
                         uid = if(Sys.getenv('SNOWFLAKE_UID') != "") Sys.getenv('SNOWFLAKE_UID') else NULL,
                         token = if(Sys.getenv('SNOWFLAKE_C360_TOKEN') != "") Sys.getenv('SNOWFLAKE_C360_TOKEN') else NULL)
```

Snowflake tokens expire every few minutes. You need to refresh the token, update the `.Renviron` file and reload the environment variables.
Either restart Rsession or 

```R
readRenviron("~/##project##/.Renviron")
```

#### Connecting to datasource in `python`

Use `lfs.ml.common` library.

```python
from lfs.ml.common.snowflake_processor import SnowflakeProcessor

sn_proc = SnowflakeProcessor(warehouse='CUSTOMER_ANALYTICS_WH', env='R')
sn_proc.execute(sql="select CURRENT_DATE()")
```

The code will ask to input SSO and Snowflake token in Workbench. Once it is scheduled in RConnect, it uses a Snowflake service account.

### Writing to workspace database as part of your content

RStudio is configured to allow you to use a writeable database as part of your script. The database name is available as an environment variable. Refer to the table above for the environment variable for your team.

## Developing dash app

[Video](https://rstudio.wistia.com/medias/owqq821xne)

### Roles:

#### Customer analytics team

Your personal account with role  `CNP-ACCESS-PLATFORM-CUSTOMER-ANALYTICS` is used in Workbench IDE.
User `RSTUDIO_CUST_ANALYTICS` with role `RSTUDIO_CUST_ANALYTICS` will be used in Connect once the content is deployed.
Make sure if a role is specified as part of the connection, it is a role that is available to the Connect user.


Note: 
Updated version of this README may be available [here](https://github.com/LatitudeFinancial/rstudio-content/blob/master/README.md).
