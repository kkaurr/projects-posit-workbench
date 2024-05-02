{{
    config(
        materialized='table',
        schema='campaign_plcards',
        alias = 'pet_lcm_onboarding',
        tags=['daily']
    )
}}

--needs to be changed once the lcm-onboarding launch date is settled
-- we have 3 days lag for new customers, making the launch date 07 Aug (day_on_book = 0) can make sure the first edm are sent after 10 Aug (due to pricing change edm out on 2 Aug) as it triggers when days_on_books >=3 and <=6
{% set onboarding_start_date = '2023-08-07' %}

-- If they are in the approved merchant list as shown below when channel is Merchant, then the channel remains untouched
-- If they are not in the approved merchant list, change the channel from Merchant to Internet for DMD team to send generic email. 
-- Otherwise the email title will have uncleaned data and may lead to unexpected incident. 
-- Currently The merchant list covers 92.5% customers when channel is Merchant
-- ref CA-880
{% set approved_merchant_list = ['Harvey Norman','Amart Furniture','JB Hi Fi','The Good Guys','Domayne','Michael Hill Jeweller','Snooze','Joyce Mayne','Forty Winks','Big Save Furniture','Michael Hill','Breo','Freedom','RPG Samsung Stores','Bed Shed',"Zamel''s"] %}



with card as
(select
    CUST_ACCT_APPL_ID,
    customer_master_id,
    clv_customer_id,
    to_date(OPEN_DT_AEST) as open_dt,
    crm_product,
    crm_product_variant,
    -- Clean all the NZ prefix and suffix, if they are in the approved merchant list, then show as merchant. Otherwise change it back to generic message by chaning them to Online
    case when  REPLACE(REPLACE(top_dealer_name, 'NZ ', ''), ' NZ', '') 
                    IN ({% for merchant in approved_merchant_list %}
                       ('{{ merchant }}'){% if not loop.last %}, {% endif %}
                     {% endfor %})
                    then APPLICATION_CHANNEL 
                    else 'Internet' end as APPLICATION_CHANNEL,
    REPLACE(REPLACE(top_dealer_name, 'NZ ', ''), ' NZ', '') as originating_merchant,  -- Clean all the NZ prefix and suffix
    MOBILEAPP_LINK_FLAG,
    crm_otb_amt,
    --clean data as some can be null
    case
        when CRM_CARD_ACTIVATION_FLAG = 1 then 'Y'
        else 'N'
    end as CRM_CARD_ACTIVATION_FLAG,
    --clean data as some can be null
    case
        when ACH_FLAG = 'Y' then 'Y'
        else 'N'
    end as ACH_FLAG,
    case 
        when CURR_BAL_AMT>0 then 'Y'
        else 'N'
    end as AMT_DUE_FLAG,
    current_date() - open_dt AS DAYS_ON_BOOK
from
     {{ var("pd_customer_analytics_db") }}.CUSTOMER_ANALYTICS.tbl_dm_acct_card

where
    open_dt >=to_date( '{{ onboarding_start_date}}')
    and crm_product in ('GOMC', 'GEMV', 'GEMVNZ', '28DEGMC')
),
mobilewallet as (
        select
            distinct cust_acct_appl_id,
            'Y' as mobile_provisioned_flag
        from
             {{ var("pd_customer_analytics_db") }}.conformed.vw_vp2_mobile_wallet
        where
            CRM_MOBILEWALLET_ACTIVE_STATUS = 'Y' and CRM_MOBILEWALLET_TYPE in ('ApplePay','FitbitPay','GarminPay','GooglePay','SamsungPay')
            and cust_acct_appl_id in (
                select
                    cust_acct_appl_id
                from
                    card
            )
        union all
        select
            distinct cust_acct_appl_id,
            'Y' as mobile_provisioned_flag
        from
             {{ var("pd_customer_analytics_db") }}.conformed.vw_vp8_mobile_wallet
        where
            CRM_MOBILEWALLET_ACTIVE_STATUS = 'Y' and CRM_MOBILEWALLET_TYPE in ('ApplePay','FitbitPay','GarminPay','GooglePay','SamsungPay')
            and cust_acct_appl_id in (
                select
                    cust_acct_appl_id
                from
                    card
            )
    )
,
acct_details as
(    select
        cust_acct_appl_id,
        STMT_PREF_FLAG
    from
        {{ var("pd_customer_analytics_db") }}.CUSTOMER_ANALYTICS.tbl_dm_acct_details
    where
        cust_acct_appl_id in (
            select
                cust_acct_appl_id
            from
                card
        )
),
eligibility as
(   select
        cust_acct_appl_id,
        MARKETABLE_EMOB_FLAG,
        serviceable_flag,
        DNS_CLVLE_DIGITAL_FLAG as CEM_OPTOUT_FLAG
    from
        {{ var("pd_customer_analytics_db") }}.CUSTOMER_ANALYTICS.TBL_DM_ACCT_ELIGIBILITY
    where
        cust_acct_appl_id in (
            select
                cust_acct_appl_id
            from
                card
        )
), 
txn_card_used as
(   select
        distinct cust_acct_appl_id,
        'Y' as card_used_flag    
    from
        {{ var("pd_customer_analytics_db") }}.customer_analytics.tbl_dm_txn_card
    where
        cust_acct_appl_id in (
            select
                cust_acct_appl_id
            from
                card
        ) 
        and crm_tran_type in ('SCH', 'RF', 'CASH')
),
txn_sch_used as
(   select
        distinct cust_acct_appl_id,
        'Y' as Scheme_Purchase_Flag    
    from
        {{ var("pd_customer_analytics_db") }}.customer_analytics.tbl_dm_txn_card
    where
        cust_acct_appl_id in (
            select
                cust_acct_appl_id
            from
                card
        ) 
        and crm_tran_type in ('SCH')
),
txn_repaid as
(   select
        distinct cust_acct_appl_id,
        'Y' as Pay_Statement_Flag    
    from
        {{ var("pd_customer_analytics_db") }}.customer_analytics.tbl_dm_txn_card
    where
        cust_acct_appl_id in (
            select
                cust_acct_appl_id
            from
                card
        ) 
        and crm_tran_type in ('PAYMENT')
)
select
    card.*
    ,case
        when MOBILE_PROVISIONED_FLAG = 'Y' then 'Y'
        else 'N'
    END AS MOBILE_PROVISIONED_FLAG,
    STMT_PREF_FLAG
    ,case
        when MARKETABLE_EMOB_FLAG = 'Y' then 'Y'
        else 'N'
    END AS MARKETABLE_EMOB_FLAG
    ,case
        when serviceable_flag = 'Y' then 'Y'
        else 'N'
    END AS serviceable_flag,
    CEM_OPTOUT_FLAG
    ,case
        when card_used_flag = 'Y' then 'Y'
        else 'N'
    END AS card_used_flag
    ,case
        when Scheme_Purchase_Flag = 'Y' then 'Y'
        else 'N'
    END AS Scheme_Purchase_Flag
    ,case
        when Pay_Statement_Flag = 'Y' then 'Y'
        else 'N'
    END AS Pay_Statement_Flag
    , CASE 
      WHEN MOBILEAPP_LINK_FLAG = 'Y' THEN 1 ELSE 0 
    END +
    CASE 
      WHEN CRM_CARD_ACTIVATION_FLAG = 'Y' THEN 1 ELSE 0 
    END +
    CASE 
      WHEN card_used_flag = 'Y' THEN 1 ELSE 0 
    END +
    CASE 
      WHEN Pay_Statement_Flag = 'Y' THEN 1 ELSE 0 
    END AS TASK_DONE
    ,case when MOD(ABS(CAST(HASH(TO_VARCHAR(CUST_ACCT_APPL_ID)) AS NUMBER)), 100) < 10 THEN 'Y' ELSE 'N' END AS control_group
from
    card
    left join mobilewallet using (cust_acct_appl_id)
    left join acct_details using (cust_acct_appl_id)
    left join eligibility using (cust_acct_appl_id)
    left join txn_card_used using (cust_acct_appl_id)
    left join txn_sch_used using (cust_acct_appl_id)
    left join txn_repaid using (cust_acct_appl_id)
