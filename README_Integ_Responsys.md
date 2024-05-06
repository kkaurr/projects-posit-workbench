# C360 Responsys Integration
This repository contains code to deploy and send all SQL models (tables, views) required for Responsys integration. These models are deployed using dbt for snowflake transformation logic and airflow for orchestration and sftp integration.
Main purpose of this repository is to provide data analysts with ability to create, schedule and transfer data to Responsys with minimal engineering assistance.      
Secondary purpose is to extract and isolate all Responsys integration components into separate repository for easier maintenance.


## Architecture
Repository consists of two main parts:
1. DBT transformations that are scheduled to run daily at 6am (7pm UTC)
   - [airflow "deploy DBT Models" dag](https://datalake2-airflow-prod.prod.datalake2.lfscnp.com/graph?dag_id=c360_models_integ_responsys_deploy_dbt_models) is taking its input from `models` folder using convention `models/$schema_name/tables/$table_name.sql`
   - and producing output into snowflake `INTEG_RESPONSYS` database using convention `$schema_name/$table_name`
2. Responsys sftp transfer that is scheduled daily at 8am (9pm UTC)
    - [airflow "tables transfer" dag](https://datalake2-airflow-prod.prod.datalake2.lfscnp.com/graph?dag_id=c360_models_integ_responsys_tables_transfer) is taking input from `airflow/table_list.py` configuration and resolving against `INTEG_RESPONSYS` 
    - and producing output to Responsys sftp `files.responsys.net/$schema_name/$table_name_$yyyymmdd.csv.pgp` e.g(`files.responsys.net/campaign_plcards/sup_nbo_prediction_20220315.csv.pgp`)

<img src="doco/high_level.png" alt="drawing" width="100%" align="center"/>
 
## How to
- [Add table to Responsys sftp transfer](#ct01)
- [Remove table from Responsys sftp transfer](#ct02)
- [Add dbt transformation to daily run](#ct03)
- [Remove dbt transformation from daily run](#ct04)
- [Release to production](#ct05)
- [Trigger dbt changes immediately](#ct06)
- [Resend data to Responsys sftp only for one table](#ct07)

#### NB: Remember to coordinate with other people working in this repository in #c360-customer-analytics slack channel when you are
- Releasing into production (production airflow will become unresponsive for several minutes)
- Manually triggering dbt re-run
- Manually resending table into responsys

### <a name="ct01"></a>Add table to Responsys sftp transfer
- Register tables that are ready to be sent to Responsys in`airflow/table_list.py` e.g. 
```    
    {
        'schema_name': 'campaign_plcards',
        'table_name': 'pet_cards_customer'
    }
```
- approve and merge pull request into master branch
- wait for [buildkite](https://buildkite.com/latitude-financial/c360-models-integ-responsys) to successfully build your changes
- (optionally) verify that your table has been added in [non-prod  airflow](https://datalake2-airflow-test.test.datalake2-np.lfscnp.com/graph?dag_id=c360_models_integ_responsys_tables_transfer)
- [release changes](#ct05) into production
- verify table has been added to [production  transfer dag](https://datalake2-airflow-prod.prod.datalake2.lfscnp.com/graph?dag_id=c360_models_integ_responsys_tables_transfer)

### <a name="ct02"></a>Remove table from Responsys sftp transfer
For tables that are no longer required to be sent simply delete entry from `airflow/table_list.py`
and follow merge & release steps from adding [section](#add-table-to-responsys-sftp-transfer)

### <a name="ct03"></a>Add dbt transformation to daily run
dbt setup is similar to existing pattern in pd-customer-analytics project,  
for more information look at corresponding [readme](https://github.com/LatitudeFinancial/c360-models-pd-customer-analytics/blob/master/README.md)
important differences:
- current tagging strategy is to run sql transformations with all tags excluding `archive` one using separate airflow dag https://datalake2-airflow-prod.prod.datalake2.lfscnp.com/graph?dag_id=c360_models_integ_responsys_deploy_dbt_models 
- don't deploy transformations with 'once' tag during build-kite phase (see [reasons](#reasons-to-remove-dbt-from-buildkite))

### <a name="ct04"></a>Remove dbt transformation from daily run
Similar to pd-customer-analytics project you need to tag sql transformation as `archive` e.g. 

### <a name="ct06"></a>Trigger dbt changes immediately
* approve and merge pr into master
* [release dbt changes](#release-to-production) into production
* manually trigger dbt transformation dag in [production](https://datalake2-airflow-prod.prod.datalake2.lfscnp.com/graph?dag_id=c360_models_integ_responsys_deploy_dbt_models)

* <img src="doco/trigger_dag.jpg" alt="drawing" width="60%"/>
* verify that desired data are present in snowflake `select * from INTEG_RESPONSYS.CAMPAIGN_LPAY.PET_CARDS_CUSTOMER`

### <a name="ct07"></a>Resend data to Responsys sftp only for one table
* go into responsys data transfer [dag](https://datalake2-airflow-prod.prod.datalake2.lfscnp.com/graph?dag_id=c360_models_integ_responsys_tables_transfer) in production
* left-click on required square with text `{schema_name}_{table_name}_export_csv_file`
* <img src="doco/resend.png" alt="drawing" width="60%"/>
* click `clear` button in the popup screen
* <img src="doco/clear.png" alt="drawing" width="30%"/>
* verify and approve steps to be `cleared`/rerun (bail if there are other tables that you don't intend to restart)
* <img src="doco/approve.png" alt="drawing" width="60%"/>

### <a name="ct05"></a> Release to production
* wait for [buildkite](https://buildkite.com/latitude-financial/c360-models-integ-responsys) successfully building and running dbt-test on production data
* you should see a green build with "Passed in XXm and blocked" status
* <img src="doco/release_1.png" alt="drawing" width="60%"/>
* click on green button with "pause" symbol inside to proceed further
* release dbt changes by clicking `🚀Release - Production` button
* <img src="doco/release_2.png" alt="drawing" width="60%"/>
* if needed follow [release airflow](doco/af_release.md) changes documentation

### Reasons to remove dbt from buildkite
- buildkite has problems with relatively long (1h+) running processes
- having two independent systems(buildkite and airflow) working with the same target database leads to "race condition"-like problems  