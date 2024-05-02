{{
    config(
        materialized='table',
        schema='lcm_onboarding',
        alias='rpt_lcm_onboarding_campaign',
        tags=['daily']
    )
}}
 
with lcm_onboarding_campaign_name as (
    select distinct campaign_id,
                --get the latest launch_date for multiple launch_date value
                CAMPAIGN_NAME,
                rtrim(split_part(CAMPAIGN_NAME, '_', 3)||'_'||split_part(CAMPAIGN_NAME, '_', 4)||' '||split_part(CAMPAIGN_NAME, '_', 5)||' '||split_part(CAMPAIGN_NAME, '_', 6)) as campaign_group
  from  {{ var("raw_db") }}."RESPONSYS_CED_EMAIL"."LAUNCH_STATE"
     where MARKETING_STRATEGY = 'Onboarding' and MARKETING_PROGRAM='Lifecycle'
     and launch_status in ('C', 'S') and launch_type in ('S','P','R')
)
,
 campaign_all as (
-- The table_from column is to indicate where the action is coming from and the number inside the value is to get the order in the POWER BI to sort the value from left to right
  select CUSTOMER_ID
,min(to_date(EVENT_CAPTURED_DT)) as EVENT_CAPTURED_DT 
,CAMPAIGN_ID
,'1. Email Sent' as table_from
from  {{ var("raw_db") }}.RESPONSYS_CED_EMAIL.sent where campaign_id in (select campaign_id from lcm_onboarding_campaign_name)
group by 1,3
  union all   
select CUSTOMER_ID
,min(to_date(EVENT_CAPTURED_DT)) as EVENT_CAPTURED_DT
,CAMPAIGN_ID
,'2. Email Unique Open' as table_from 
from  {{ var("raw_db") }}.RESPONSYS_CED_EMAIL.open where campaign_id in (select campaign_id from lcm_onboarding_campaign_name)
group by 1,3
  union all  
select CUSTOMER_ID
,min(to_date(EVENT_CAPTURED_DT)) as EVENT_CAPTURED_DT
,CAMPAIGN_ID
,'30. Unique Click' as table_from 
from  {{ var("raw_db") }}.RESPONSYS_CED_EMAIL.CLICK where campaign_id in (select campaign_id from lcm_onboarding_campaign_name)
group by 1,3
  union all  
select CUSTOMER_ID
,min(to_date(EVENT_CAPTURED_DT)) as EVENT_CAPTURED_DT
,CAMPAIGN_ID
,'31. Get the App Click' as table_from 
from  {{ var("raw_db") }}.RESPONSYS_CED_EMAIL.CLICK where campaign_id in (select campaign_id from lcm_onboarding_campaign_name) and 
OFFER_NAME like ('Get_the_app%')
group by 1,3
  union all 
select CUSTOMER_ID
,min(to_date(EVENT_CAPTURED_DT)) as EVENT_CAPTURED_DT
,CAMPAIGN_ID
,'32. Where to Shop Page Click' as table_from 
from  {{ var("raw_db") }}.RESPONSYS_CED_EMAIL.CLICK where campaign_id in (select campaign_id from lcm_onboarding_campaign_name) and 
OFFER_NAME like ('Tell_me_more%')
group by 1,3
  union all 
select CUSTOMER_ID
,min(to_date(EVENT_CAPTURED_DT)) as EVENT_CAPTURED_DT
,CAMPAIGN_ID
,'33. Digital Wallet Page Click' as table_from 
from  {{ var("raw_db") }}.RESPONSYS_CED_EMAIL.CLICK where campaign_id in (select campaign_id from lcm_onboarding_campaign_name) and 
OFFER_NAME like ('Find_out_more%')
group by 1,3
  union all 
select CUSTOMER_ID
,min(to_date(EVENT_CAPTURED_DT)) as EVENT_CAPTURED_DT
,CAMPAIGN_ID
,'34. Ways to Pay Click' as table_from 
from  {{ var("raw_db") }}.RESPONSYS_CED_EMAIL.CLICK where campaign_id in (select campaign_id from lcm_onboarding_campaign_name) and 
OFFER_NAME like ('Ways_to_pay%')
group by 1,3
  union all 
select CUSTOMER_ID
,min(to_date(EVENT_CAPTURED_DT)) as EVENT_CAPTURED_DT
,CAMPAIGN_ID
,'35. Pay Bill Click' as table_from 
from  {{ var("raw_db") }}.RESPONSYS_CED_EMAIL.CLICK where campaign_id in (select campaign_id from lcm_onboarding_campaign_name) and 
OFFER_NAME like ('Pay_Bill%')
group by 1,3
  union all 
select CUSTOMER_ID
,min(to_date(EVENT_CAPTURED_DT)) as EVENT_CAPTURED_DT
,CAMPAIGN_ID
,'36. Survey click' as table_from 
from  {{ var("raw_db") }}.RESPONSYS_CED_EMAIL.CLICK where campaign_id in (select campaign_id from lcm_onboarding_campaign_name) and 
OFFER_CATEGORY like ('Survey%')
group by 1,3
  union all 
select CUSTOMER_ID
,min(to_date(EVENT_CAPTURED_DT)) as EVENT_CAPTURED_DT
,CAMPAIGN_ID
,'5. Email Bounce' as table_from 
from  {{ var("raw_db") }}.RESPONSYS_CED_EMAIL.bounce where campaign_id in (select campaign_id from lcm_onboarding_campaign_name)
group by 1,3
  union all 
select CUSTOMER_ID
,min(to_date(EVENT_CAPTURED_DT)) as EVENT_CAPTURED_DT
,CAMPAIGN_ID
,'6. Email Opt_out' as table_from 
from  {{ var("raw_db") }}.RESPONSYS_CED_EMAIL.opt_out where campaign_id in (select campaign_id from lcm_onboarding_campaign_name)
group by 1,3
  union all 
select CUSTOMER_ID
,min(to_date(EVENT_CAPTURED_DT)) as EVENT_CAPTURED_DT
,CAMPAIGN_ID
,'7. Email Skipped' as table_from
from  {{ var("raw_db") }}.RESPONSYS_CED_EMAIL.skipped where campaign_id in (select campaign_id from lcm_onboarding_campaign_name)
group by 1,3
  
)

select cam.*,
CAMPAIGN_NAME,
CAMPAIGN_GROUP 
from campaign_all cam
left join lcm_onboarding_campaign_name cam_name
using (CAMPAIGN_ID)
where CUSTOMER_ID not like '%TEST%'
and EVENT_CAPTURED_DT>=date'2023-08-10'

