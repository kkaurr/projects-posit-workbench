{{
    config(
        materialized='table',
        schema='analytics_insight_reporting',
        alias='rpt_20240227_ca_28deg_pruchaserate_campaign',
        tags=['daily']
    )
}}
 
 
{% set lcm_start_date = '2024-02-27' %}
{% set lcm_end_date = '2024-03-08' %}
{% set marketing_strategy = 'Utilisation'  %}
{% set marketing_program = 'Applications' %}

with segment as (
  select distinct seg,
  offer,
  case when SEG = 'IR' THEN '1.99% IR TILL 11SEP'
when SEG = 'IF3' THEN 'INTEREST FREE 3 MONTHS TILL 11JUN'
when SEG = 'IF6' THEN 'INTEREST FREE 6 MONTHS TILL 11SEP'
end as THRESHOLD
  from {{ var("integ_responsys_db")}}.CAMPAIGN_PLCARDS.PET_202402_28DEG_PURCHASERATE_CAMPAIGN_ATTRIBUTE
 )
 ,
 edm as
(select CUSTOMER_ID,CAMPAIGN_ID,CAMPAIGN_NAME,CONTROL_GROUP,
SENT_DT_AEST as "1. email sent",
SKIPPED_DT_AEST as "00. email skipped",
FIRST_OPENED_DT_AEST as "2. email open",
FIRST_ACTIVATED_DT_AEST as "3. email activate",
FIRST_BOUNCE_DT as "6. email bounce",
FIRST_OPTOUT_DT as "7. email opt_out"
 from {{ var("pd_customer_analytics_db")}}.contact_history.crmvw_responsys_email_contact_event_data 
  where MARKETING_STRATEGY = '{{ marketing_strategy}}'
 and MARKETING_PROGRAM='{{ marketing_program}}'
 and SENT_DT_AEST between to_date( '{{ lcm_start_date}}') and to_date( '{{ lcm_end_date}}')
)
,
sms as
(select CUSTOMER_ID,CAMPAIGN_ID,CAMPAIGN_NAME,null as CONTROL_GROUP,
SENT_DT_AEST as "4. sms sent",
SKIPPED_DT_AEST as "01. sms skipped",
FIRST_ACTIVATED_DT_AEST as "5. sms activate",
OPTOUT_DT_AEST as "8. sms opt_out"
 from {{ var("pd_customer_analytics_db")}}.contact_history.crmvw_responsys_sms_contact_event_data 
  where MARKETING_STRATEGY = '{{ marketing_strategy}}'
 and MARKETING_PROGRAM='{{ marketing_program}}'
 and SENT_DT_AEST between to_date( '{{ lcm_start_date}}') and to_date( '{{ lcm_end_date}}')
)
,
engagement as (
SELECT
  CUSTOMER_ID,
  CAMPAIGN_ID,
  CAMPAIGN_NAME,
  lower(split_part(CAMPAIGN_NAME, '_', -1)) as campaign_type,
  split_part(CAMPAIGN_NAME, '_', 1)||'-'||split_part(CAMPAIGN_NAME, '_', 2) as campaign_group,
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
    "7. email opt_out"
  )
)
UNION ALL
SELECT
  CUSTOMER_ID,
  CAMPAIGN_ID,
  CAMPAIGN_NAME,
  lower(split_part(CAMPAIGN_NAME, '_', -1)) as campaign_type,
  split_part(CAMPAIGN_NAME, '_', 1)||'-'||split_part(CAMPAIGN_NAME, '_', 2) as campaign_group,
  table_from,
  TO_DATE(action_date) AS EVENT_CAPTURED_DT
FROM
  sms
UNPIVOT(
  action_date FOR table_from IN (
"4. sms sent",
"01. sms skipped",
"5. sms activate",
"8. sms opt_out"
  )
)
)
select
c.customer_id,
a.EVENT_CAPTURED_DT,
a.TABLE_FROM,
a.CAMPAIGN_ID,
a.CAMPAIGN_NAME,
a.campaign_type,
a.campaign_group,
c.SEG AS SUBSEGMENT,
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
'N' as SPEND_QUALIFIED_FLAG,
c.OFFER_ACTIVATED_FLAG,
c.EMAIL_OPENED_FLAG,
c.OFFER_QUALIFIED_FLAG,
   c.control_group,
   EVENT_CAPTURED_DT-to_date(c.CAMPAIGN_START_DATE) as days_to_action,
   segment.THRESHOLD as threshold_name
  from {{ var("integ_responsys_db")}}.CAMPAIGN_PLCARDS.PET_202402_28DEG_PURCHASERATE_CAMPAIGN_ATTRIBUTE c
   left join engagement a
   using (customer_id)
   left join segment
   using (seg)
   -- Condition: either responsys event between start and end date, Or if no action from Responsys then has to be in control group.
   -- ACMA & Responsys Eligibility filters may drop more people in Treatment group and they will NOT be in the reporting
  where ((EVENT_CAPTURED_DT>=CAMPAIGN_START_DATE and EVENT_CAPTURED_DT<=CAMPAIGN_END_DATE)
         or  (EVENT_CAPTURED_DT is null and c.control_group='Y'))
         and customer_id not like 'TEST%'