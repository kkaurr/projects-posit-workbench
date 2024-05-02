{{
    config(
        materialized='table',
        schema='lcm_onboarding',
        alias='rpt_lcm_onboarding',
        tags=['daily']
    )
}}
 
with base as (
select distinct 
CUST_ACCT_APPL_ID,
open_dt,
current_date() - open_dt AS DAYS_ON_BOOK,
case when DAYS_ON_BOOK<=60 then 'Y' ELSE 'N' END AS CURRENT_PROGRAM,  
crm_product,
crm_product_variant,
originating_merchant,
application_channel,
TASK_DONE,
case when control_group='N' THEN 'Treatment' else 'Control' end as control_group
from {{ var("integ_responsys_db")}}.campaign_plcards.pet_lcm_onboarding
)
,mobile_provision as (
select cust_acct_appl_id,
to_date(min(AUTHP_DATE)) as mobile_provision_dt 
from {{ var("pd_customer_analytics_db") }}.conformed.vw_vp2_mobile_wallet 
where CRM_MOBILEWALLET_ACTIVE_STATUS = 'Y' and cust_acct_appl_id in (select cust_acct_appl_id from base)
group by 1
Union All
select cust_acct_appl_id,
to_date(min(FAS_DATE_STAMP)) as mobile_provision_dt 
from {{ var("pd_customer_analytics_db") }}.conformed.vw_vp8_mobile_wallet 
where CRM_MOBILEWALLET_ACTIVE_STATUS = 'Y' and cust_acct_appl_id in (select cust_acct_appl_id from base)
group by 1
)
,link as (
select account_number as cust_acct_appl_id,
  to_date(min(event_date)) as link_dt
 from {{ var("conformed_db") }}.ACCOUNT_LINKING_EVENTS.ACCOUNT_LINKING_EVENTS_CURR 
 where linking_type='LINKED' and account_number in (select cust_acct_appl_id from base)
 group by 1
 )
,activate as (
select cust_acct_appl_id,
to_date(CRM_CARD_ACTIVATION_DT_AEST) as activation_dt 
from {{ var("pd_customer_analytics_db") }}.customer_analytics.tbl_dm_acct_card
)
,txn_table1 as
(   select
         cust_acct_appl_id,
         to_date(MIN(trandate)) AS first_sch_dt,
         sum(crm_amt_net) as amount_sch,
         count(case when crm_amt_net>0 then 1 else 0 end)  as freq_sch
    from
        {{ var("pd_customer_analytics_db") }}.customer_analytics.tbl_dm_txn_card
    where
        cust_acct_appl_id in (
            select
                cust_acct_appl_id
            from
                base
        ) 
        and crm_tran_type in ('SCH')
        group by 1
)
--RF transaction
,txn_table2 as
(   select
         cust_acct_appl_id,
         to_date(MIN(trandate)) AS first_rf_dt,
         sum(crm_amt_net) as amount_rf,
         count(case when crm_amt_net>0 then 1 else 0 end)  as freq_rf
    from
        {{ var("pd_customer_analytics_db") }}.customer_analytics.tbl_dm_txn_card
    where
        cust_acct_appl_id in (
            select
                cust_acct_appl_id
            from
                base
        ) 
        and crm_tran_type in ('RF')
        group by 1
)
,txn_table3 as
(   select
         cust_acct_appl_id,
         to_date(MIN(trandate)) AS first_repay_dt
    from
        {{ var("pd_customer_analytics_db") }}.customer_analytics.tbl_dm_txn_card
    where
        cust_acct_appl_id in (
            select
                cust_acct_appl_id
            from
                base
        ) 
        and crm_tran_type in ('PAYMENT')
        group by 1
)
--duplicate found, use 'SCPR' asc_action_desc, to_date('ASC_CREATE_DT') ODS_ASM   CONFORMED_EXADATA_DWH.ODS_ASM.TB_CASE
-- use 'SCPR' auc_action_desc, to_date('AUC_CREATE_DT') ODS_ASM256   CONFORMED_EXADATA_DWH.ODS_ASM256.TB_CASE_AU
-- STATEMENT PREFERENCE
,STMT as (
select right(asc_acct,16) as cust_acct_appl_id,
to_date(min(ASC_CREATE_DT)) as stmt_dt 
from {{ var("conformed_exadata_db") }}.ODS_ASM.TB_CASE
where asc_action_code = 'SCPR' and cust_acct_appl_id in (select cust_acct_appl_id from base)
group by 1
Union All
select right(auc_account,16) as cust_acct_appl_id,
to_date(min(AUC_CREATE_DT)) as stmt_dt 
from {{ var("conformed_exadata_db") }}.ODS_ASM256.TB_CASE_AU
where auc_action_code = 'SCPR' and cust_acct_appl_id in (select cust_acct_appl_id from base)
group by 1
)
select base.*
,link_dt
,link_dt-open_dt as link_day
,mobile_provision_dt
,mobile_provision_dt-open_dt as mobile_provision_day
,activation_dt
,activation_dt-open_dt as activation_day
,first_sch_dt
,first_sch_dt-open_dt as sch_day
,amount_sch
,freq_sch
,first_rf_dt
,first_rf_dt-open_dt as rf_day
,amount_rf
,freq_rf
,first_repay_dt
,first_repay_dt-open_dt as repay_day
,stmt_dt
,stmt_dt-open_dt as stmt_day
from base
left join mobile_provision
using (cust_acct_appl_id)
left join link
using (cust_acct_appl_id)
left join activate
using (cust_acct_appl_id)
left join txn_table1
using (cust_acct_appl_id)
left join txn_table2
using (cust_acct_appl_id)
left join txn_table3
using (cust_acct_appl_id)
left join STMT
using (cust_acct_appl_id)
