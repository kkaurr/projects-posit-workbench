{{
    config(
        materialized='table',
        schema='analytics_insight_reporting',
        alias='rpt_20230704_ca_857_travelq_sg_campaign',
        tags=['daily']
    )
}}
 
with lcm_campaign_name as (
    select distinct campaign_id,
                --get the latest launch_date for multiple launch_date value
                max(to_date(LAUNCH_COMPLETED_DT)) as launch_date,
                CAMPAIGN_NAME,
                lower(split_part(CAMPAIGN_NAME, '_', -1)) as campaign_type ,
                split_part(CAMPAIGN_NAME, '_', 1)||'-'||split_part(CAMPAIGN_NAME, '_', 2) as campaign_group
  from  {{ var("raw_db") }}."RESPONSYS_CED_EMAIL"."LAUNCH_STATE"
     where (MARKETING_STRATEGY = 'Spend & Get (LCM)') and MARKETING_PROGRAM='TRAVELQ'
     and launch_status in ('C', 'S') and launch_type in ('S','P','R')
     and campaign_group='SG-28D23'
     
 group by 1,3,4,5
 having launch_date between date'2023-07-04' and date'2023-09-30')
 ,
 segment as (
  select distinct subseg,
  offer,
  case when subseg='1.SCH_MORE_OVER_$500'   then    'A. Avg Mthly + $2,000'
when    subseg='2.SCH_MORE_UNDER_$500'  then    'B. Avg Mthly + $1,500'
when    subseg='3.SCH_LESS' then    'C. Avg Mthly + $1,200'
when    subseg='4.SCH_INACTIVE' then    'D. $1,000'
end as THRESHOLD
  from {{ var("integ_responsys_db")}}.CAMPAIGN_PLCARDS.pet_ca_857_travelq_sg_campaign_attribute)
 ,
 edm as
(select CUSTOMER_ID,CAMPAIGN_ID,CAMPAIGN_NAME,CONTROL_GROUP,
SENT_DT_AEST as "1. email sent",
SKIPPED_DT_AEST as "00. email skipped",
FIRST_OPENED_DT_AEST as "2. email open",
FIRST_ACTIVATED_DT_AEST as "3. email activate",
FIRST_BOUNCE_DT as "6. email bounce",
FIRST_OPTOUT_DT as "70. email opt_out"
 from {{ var("pd_customer_analytics_db")}}.contact_history.crmvw_responsys_email_contact_event_data where CAMPAIGN_ID in (select CAMPAIGN_ID from lcm_campaign_name)
)
,
sms as
(select CUSTOMER_ID,CAMPAIGN_ID,CAMPAIGN_NAME,null as CONTROL_GROUP,
SENT_DT_AEST as "4. sms sent",
SKIPPED_DT_AEST as "01. sms skipped",
FIRST_ACTIVATED_DT_AEST as "5. sms activate",
OPTOUT_DT_AEST as "71. sms opt_out"
 from {{ var("pd_customer_analytics_db")}}.contact_history.crmvw_responsys_sms_contact_event_data where CAMPAIGN_ID in (select CAMPAIGN_ID from lcm_campaign_name)
)
,
engagement as (
SELECT
  CUSTOMER_ID,
  CAMPAIGN_ID,
  CAMPAIGN_NAME,
  table_from,
  TO_DATE(action_date) AS EVENT_CAPTURED_DT
FROM
  edm
UNPIVOT(
  action_date FOR table_from IN (
    "1. email sent",
    "00. email skipped",
    "2. email open",
    "3. email activate",
    "6. email bounce",
    "70. email opt_out"
  )
)
UNION ALL
SELECT
  CUSTOMER_ID,
  CAMPAIGN_ID,
  CAMPAIGN_NAME,
  table_from,
  TO_DATE(action_date) AS EVENT_CAPTURED_DT
FROM
  sms
UNPIVOT(
  action_date FOR table_from IN (
"4. sms sent",
"01. sms skipped",
"5. sms activate",
"71. sms opt_out"
  )
)
)
select
  c.customer_id,
   a.EVENT_CAPTURED_DT,
   a.TABLE_FROM,
   b.*,
   c.SUBSEG AS SUBSEGMENT,
c.offer as offer,
c.crm_product,
c.crm_product_variant,
to_date(c.CAMPAIGN_START_DATE) as CAMPAIGN_START_DATE,
to_date(c.CAMPAIGN_END_DATE) as CAMPAIGN_END_DATE,
c.TMP_SCHEME_SPENDING_UP_TO_DATE,
c.SPENDING_POST30DAYS,
c.SPENDING_POST60DAYS,
c.SPENDING_POST90DAYS,
c.SPEND_OR_NOT_FLAG,
c.SPEND_QUALIFIED_FLAG,
c.OFFER_ACTIVATED_FLAG,
c.EMAIL_OPENED_FLAG,
c.OFFER_QUALIFIED_FLAG,
   c.control_group,
   EVENT_CAPTURED_DT-to_date(c.CAMPAIGN_START_DATE) as days_to_action,
   segment.THRESHOLD as threshold_name
  from {{ var("integ_responsys_db")}}.CAMPAIGN_PLCARDS.pet_ca_857_travelq_sg_campaign_attribute c
   left join engagement a
   using (customer_id)
   left join lcm_campaign_name b
   on a.campaign_id=b.campaign_id
   left join segment
   using (subseg)
   -- Condition: either responsys event between start and end date, Or if no action from Responsys then has to be in control group.
   -- ACMA & Responsys Eligibility filters may drop more people in Treatment group and they will NOT be in the reporting
  where ((EVENT_CAPTURED_DT>=CAMPAIGN_START_DATE and EVENT_CAPTURED_DT<=CAMPAIGN_END_DATE)
         or  (EVENT_CAPTURED_DT is null and c.control_group='Y'))
         and customer_id not like 'TEST%'