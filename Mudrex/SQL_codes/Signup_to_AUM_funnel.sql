with signup1 as
(select 
 -- /*
        case when date_part(dayofweek, date_add('minute',330,u.created_at)) in (5,6) 
        then date(date_add('day',4,date_trunc('week',date_add('minute',330,u.created_at)))) 
        else date(date_add('day',4,date_trunc('week',date_add('day',-5, date_add('minute',330,u.created_at))))) end as week_dt
        , -- */
        date(date_add('minute',330,u.created_at)) as dt  -- signup_date
        ,date(date_trunc('month',u.created_at)) as month_dt
        -- ,count(distinct u.user_id)
        ,u.user_id
        ,u.phone_country_id
from Users u 
where 1=1
and date_add('minute',330,created_at) >= date_add('day',-{{N}}, current_date)
-- and u.user_id=812199
and u.client_id is null
and u.user_id not in (select user_id from internal_users)
-- and u.phone_country_id in (93,103)
-- group by 1,2,3,4,5
)
,
signup as 
(
select 
case when upper({{Date_type}})='D' then dt
     when upper({{Date_type}})='W' then week_dt
     when upper({{Date_type}})='M' then month_dt
     end as dt_flag
,user_id
,phone_country_id
from signup1
)
-- select * from signup
-- /*
,
kyc as 
( select z.user_id, 
  kyc_status
  from 
    (select user_id, 
     status as kyc_status,
     row_number() over (partition by user_id order by created_at desc) as rnk  
     from kyc_status
    ) z
   join signup s on s.user_id=z.user_id
   where rnk = 1 and kyc_status in ('APPROVED','MIN_KYC_DONE','AADHAR_VERIFIED')
   group by 1,2
)
-- */
, dep1 as 
(select a.user_id, 
created_at,
sum(amount) as total_deposited_amount,
row_number () over ( partition by a.user_id order by a.created_at asc ) as rnk -- first deposit identifier
from 
    (select user_id,
    date(date_add('minute',330,created_at)) as created_at,
    sum(actual_crypto_amount) as amount
    from fiat_transaction
    where transaction_type = 'BUY' and status = 'COMPLETED' 
    and user_id not in (82263,82266,222419,413689,84964)
    group by 1,2
    
    union all
                        
    select user_id,
    date(date_add('minute',330,insert_time)) as created_at, 
    sum(usd_value) as amount
    from broker_deposit_history
    where transfer_state is null
    and user_id not in (82263,82266,222419,413689,84964)
    group by 1,2
    
    union all
                        
    select user_id,
    date(date_add('minute',330,insert_time)) as created_at, 
    sum(usd_value) as amount
    from broker_deposit_history
    where transfer_state = 'Transferred'
    and user_id not in (82263,82266,222419,413689,84964)
    group by 1,2) a
    join signup s on s.user_id=a.user_id
group by 1,2
) 

,dep as (
select 
user_id
,sum(total_deposited_amount) as total_deposited_amount
,sum(case when rnk=1 then total_deposited_amount end) as first_deposited_amount
from dep1
group by 1
)
-- select * from dep
,ret1 as
(
select
up.user_id
,case when date_part(dayofweek, date_add('minute',330,day)) in (5,6) 
then date(date_add('day',4,date_trunc('week',date_add('minute',330,day)))) 
else date(date_add('day',4,date_trunc('week',date_add('day',-5, date_add('minute',330,day))))) end as week_dt
,date(day) as up_date -- datex
,sum(real_balance) as real_balance 
,sum(virtual_balance) as virtual_balance 
,sum(total_balance) as total_balance 
-- ,row_number() over ( partition by up.user_id,date(day) order by day desc) as rnk_d
,row_number() over ( partition by up.user_id order by date(day) asc) as rnk_u
from user_daily_portfolio_v2 up  -- select * from User_daily_portfolio  order by day  desc limit 10
join signup s on s.user_id=up.user_id 
where 1=1
and up.user_id not in (select user_id from internal_users)
-- and up.portfolio_type in ('SPOT','VAULT')
group by 1,2,3
having sum(real_balance)>2

)
-- select count(*) from ret1
, first_inv as
(select user_id
,up_date
,real_balance
,total_balance
from ret1 
where rnk_u=1 
)   
, ret as 
(select -- date(datex) as datex, 
        fi.user_id, 
-- fi.real_balance as real_aum_d00,
-- fi.total_balance as total_aum_d00,
sum(case when date(fi.up_date)=date(up.up_date) then fi.real_balance else 0 end ) as real_aum_d0,
sum(case when date(fi.up_date)=date(up.up_date) then fi.total_balance else 0 end ) as total_aum_d0,
sum(case when datediff(day,fi.up_date,up.up_date) =7 then up.real_balance else 0 end) as real_aum_7d,  
sum(case when datediff(day,fi.up_date,up.up_date) =7 then up.total_balance else 0 end) as total_aum_7d,
sum(case when datediff(day,fi.up_date,up.up_date) =30 then up.real_balance else 0 end) as real_aum_1m, 
sum(case when datediff(day,fi.up_date,up.up_date) =30 then up.total_balance else 0 end) as total_aum_1m
from first_inv fi  -- select * from User_daily_portfolio  order by user_id,datex limit 10 -- ip
left join ret1 up on fi.user_id=up.user_id
-- where up.rnk_d=1 -- and datediff(day,fi.up_date,up.up_date) in (0,7,30)
group by 1 -- ,2,3
)  -- select * from ret where user_id=808371
--select count(distinct ret.user_id) from signup s left join ret on ret.user_id=s.user_id where date(s.week_dt)='2022-12-16'select count(distinct ret.user_id) from signup s left join ret on ret.user_id=s.user_id where date(s.week_dt)='2022-12-16'
select 
s.dt_flag,
count(distinct s.user_id) as number_of_signups
-- ,count(distinct case when phone_country_id is not null then s.user_id end) as signup_to_phone_verification
,(1.00*count(distinct case when phone_country_id is not null then s.user_id end)/nullif(count(distinct s.user_id),0)) as signup_to_phone_verification_per
-- ,(1.00*count(distinct kyc.user_id)/nullif(count(distinct case when phone_country_id is not null then s.user_id end),0)) as phone_ver_to_min_kyc
,(1.00*count(distinct dep.user_id)/nullif(count(distinct case when phone_country_id is not null then s.user_id end),0)) as phone_verification_to_Fd_per
,(1.00*sum(dep.first_deposited_amount)/nullif(count(distinct dep.user_id),0)) as avg_first_deposit
-- ,(1.00*sum(dep.total_deposited_amount)/nullif(count(distinct dep.user_id),0)) as avg_deposit
-- ,(1.00*count(distinct case when isnull(ret.total_aum_d0,0)>0 then ret.user_id end )/nullif(count(distinct s.user_id),0)) as signup_to_D0_retention_per
,(1.00*count(distinct ret.user_id ))/nullif(count(distinct dep.user_id),0) as Fd_to_D0_retention_real_per
-- ,(1.00*sum(ret.real_aum_d0)) as D0_ret_amount
-- ,sum(dep.first_deposited_amount) as FD_amount
-- ,(1.00*sum(ret.real_aum_d0))/nullif(sum(dep.first_deposited_amount),0) as FD_to_D0_amount_ret_per
-- ,count(distinct dep.user_id) as fd_user
-- ,count(distinct ret.user_id) as ret_d0_user
-- ,count(distinct case when isnull(ret.real_aum_d0,0)>0  then ret.user_id end)
,(1.00*sum(ret.real_aum_d0)/nullif(count(distinct ret.user_id),0)) as avg_aum_retention_D0
,(1.00*count(distinct case when ret.real_aum_7d>2 then ret.user_id end)/nullif(count(distinct ret.user_id ),0)) as retention_1w_real
,(1.00*sum(ret.real_aum_7d)/nullif(count(distinct case when ret.real_aum_7d>2 then ret.user_id end),0)) as avg_aum_1w_real
,(1.00*count(distinct case when ret.real_aum_1m>2 then ret.user_id end)/nullif(count(distinct ret.user_id ),0)) as retention_1m_real
,(1.00*sum(ret.real_aum_1m)/nullif(count(distinct case when ret.real_aum_1m>2 then ret.user_id end),0)) as avg_aum_1m_real
,(1.00*sum(ret.real_aum_d0))/nullif(sum(dep.first_deposited_amount),0) as D0_amount_ret_per
,(1.00*sum(ret.real_aum_7d))/nullif(sum(ret.real_aum_d0),0) as "1w_amount_ret_per"
,(1.00*sum(ret.real_aum_1m))/nullif(sum(ret.real_aum_d0),0) as "1m_amount_ret_per"
-- ,count(distinct dep.user_id) as number_of_deposits
from signup s
left join kyc on kyc.user_id=s.user_id
left join dep on dep.user_id=s.user_id
left join ret on ret.user_id=s.user_id
group by 1
order by 1 desc
-- */
