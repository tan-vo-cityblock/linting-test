WITH base_table AS
(    select
        mrt_prio.patientid,
        mrt_prio.carepathway,
        mrt_prio.cohortgolivedate,
        mrt_prio.dayssincelastoutreachcycle,
        mrt_prio.latestOutreachAttemptAt,
        all_outreaches.attemptedAt AS outreach_date
    from {{ ref('outreach_prioritization') }} mrt_prio
        full join {{ ref('outreach_attempt') }} all_outreaches
    on all_outreaches.patientid = mrt_prio.patientid),

days_0_15 as(
    select
        patientid,
        case when count(distinct date(outreach_date)) >=3 then 1 else 0 end as outreach_count
    from base_table
    where date(outreach_date) BETWEEN cohortgolivedate AND date_add(cohortgolivedate, INTERVAL 14 day)
    group by 1
),

days_15_30 as(
    select
        patientid,
        case when count(distinct date(outreach_date)) >=3 then 1 else 0 end as outreach_count
    from base_table
    where date(outreach_date) BETWEEN date_add(cohortgolivedate, INTERVAL 15 day) AND date_add(cohortgolivedate, INTERVAL 29 day)
    group by 1
),

days_30_45 as(
    select
        patientid,
        case when count(distinct date(outreach_date)) >=3 then 1 else 0 end as outreach_count
    from base_table
    where date(outreach_date) BETWEEN date_add(cohortgolivedate, INTERVAL 30 day) AND date_add(cohortgolivedate, INTERVAL 44 day)
    group by 1
),

days_45_60 as(
    select
        patientid,
        case when count(distinct date(outreach_date)) >=3 then 1 else 0 end as outreach_count
    from base_table
    where date(outreach_date) BETWEEN date_add(cohortgolivedate, INTERVAL 45 day) AND date_add(cohortgolivedate, INTERVAL 59 day)
    group by 1
),

days_60_75 as(
    select
        patientid,
        case when count(distinct date(outreach_date)) >=3 then 1 else 0 end as outreach_count
    from base_table
    where date(outreach_date) BETWEEN date_add(cohortgolivedate, INTERVAL 60 day) AND date_add(cohortgolivedate, INTERVAL 74 day)
    group by 1
),

days_75_90 as(
    select
        patientid,
        case when count(distinct date(outreach_date)) >=3 then 1 else 0 end as outreach_count
    from base_table
    where date(outreach_date) BETWEEN date_add(cohortgolivedate, INTERVAL 75 day) AND date_add(cohortgolivedate, INTERVAL 89 day)
    group by 1
),

days_90_105 as(
    select
        patientid,
        case when count(distinct date(outreach_date)) >=3 then 1 else 0 end as outreach_count
    from base_table
    where date(outreach_date) BETWEEN date_add(cohortgolivedate, INTERVAL 90 day) AND date_add(cohortgolivedate, INTERVAL 104 day)
    group by 1
),

days_105_120 as(
    select
        patientid,
        case when count(distinct date(outreach_date)) >=3 then 1 else 0 end as outreach_count
    from base_table
    where date(outreach_date) BETWEEN date_add(cohortgolivedate, INTERVAL 105 day) AND date_add(cohortgolivedate, INTERVAL 119 day)
    group by 1
),

days_120_135 as(
    select
        patientid,
        case when count(distinct date(outreach_date)) >=3 then 1 else 0 end as outreach_count
    from base_table
    where date(outreach_date) BETWEEN date_add(cohortgolivedate, INTERVAL 120 day) AND date_add(cohortgolivedate, INTERVAL 134 day)
    group by 1
),

days_135_150 as(
    select
        patientid,
        case when count(distinct date(outreach_date)) >=3 then 1 else 0 end as outreach_count
    from base_table
    where date(outreach_date) BETWEEN date_add(cohortgolivedate, INTERVAL 135 day) AND date_add(cohortgolivedate, INTERVAL 149 day)
    group by 1
),

days_150_165 as(
    select
        patientid,
        case when count(distinct date(outreach_date)) >=3 then 1 else 0 end as outreach_count
    from base_table
    where date(outreach_date) BETWEEN date_add(cohortgolivedate, INTERVAL 150 day) AND date_add(cohortgolivedate, INTERVAL 164 day)
    group by 1
),

days_165_180 as(
    select
        patientid,
        case when count(distinct date(outreach_date)) >=3 then 1 else 0 end as outreach_count
    from base_table
    where date(outreach_date) BETWEEN date_add(cohortgolivedate, INTERVAL 165 day) AND date_add(cohortgolivedate, INTERVAL 179 day)
    group by 1
),

days_180_195 as(
    select
        patientid,
        case when count(distinct date(outreach_date)) >=3 then 1 else 0 end as outreach_count
    from base_table
    where date(outreach_date) BETWEEN date_add(cohortgolivedate, INTERVAL 180 day) AND date_add(cohortgolivedate, INTERVAL 194 day)
    group by 1
),

days_195_210 as(
    select
        patientid,
        case when count(distinct date(outreach_date)) >=3 then 1 else 0 end as outreach_count
    from base_table
    where date(outreach_date) BETWEEN date_add(cohortgolivedate, INTERVAL 195 day) AND date_add(cohortgolivedate, INTERVAL 209 day)
    group by 1
),

days_210_225 as(
    select
        patientid,
        case when count(distinct date(outreach_date)) >=3 then 1 else 0 end as outreach_count
    from base_table
    where date(outreach_date) BETWEEN date_add(cohortgolivedate, INTERVAL 210 day) AND date_add(cohortgolivedate, INTERVAL 224 day)
    group by 1
),

days_225_240 as(
    select
        patientid,
        case when count(distinct date(outreach_date)) >=3 then 1 else 0 end as outreach_count
    from base_table
    where date(outreach_date) BETWEEN date_add(cohortgolivedate, INTERVAL 225 day) AND date_add(cohortgolivedate, INTERVAL 239 day)
    group by 1
),

days_240_255 as(
    select
        patientid,
        case when count(distinct date(outreach_date)) >=3 then 1 else 0 end as outreach_count
    from base_table
    where date(outreach_date) BETWEEN date_add(cohortgolivedate, INTERVAL 240 day) AND date_add(cohortgolivedate, INTERVAL 254 day)
    group by 1
),

days_255_270 as(
    select
        patientid,
        case when count(distinct date(outreach_date)) >=3 then 1 else 0 end as outreach_count
    from base_table
    where date(outreach_date) BETWEEN date_add(cohortgolivedate, INTERVAL 255 day) AND date_add(cohortgolivedate, INTERVAL 269 day)
    group by 1
),

days_270_285 as(
    select
        patientid,
        case when count(distinct date(outreach_date)) >=3 then 1 else 0 end as outreach_count
    from base_table
    where date(outreach_date) BETWEEN date_add(cohortgolivedate, INTERVAL 270 day) AND date_add(cohortgolivedate, INTERVAL 284 day)
    group by 1
),

days_285_300 as(
    select
        patientid,
        case when count(distinct date(outreach_date)) >=3 then 1 else 0 end as outreach_count
    from base_table
    where date(outreach_date) BETWEEN date_add(cohortgolivedate, INTERVAL 285 day) AND date_add(cohortgolivedate, INTERVAL 299 day)
    group by 1
),

days_300_315 as(
    select
        patientid,
        case when count(distinct date(outreach_date)) >=3 then 1 else 0 end as outreach_count
    from base_table
    where date(outreach_date) BETWEEN date_add(cohortgolivedate, INTERVAL 300 day) AND date_add(cohortgolivedate, INTERVAL 314 day)
    group by 1
),

days_315_330 as(
    select
        patientid,
        case when count(distinct date(outreach_date)) >=3 then 1 else 0 end as outreach_count
    from base_table
    where date(outreach_date) BETWEEN date_add(cohortgolivedate, INTERVAL 315 day) AND date_add(cohortgolivedate, INTERVAL 329 day)
    group by 1
),

days_330_345 as(
    select
        patientid,
        case when count(distinct date(outreach_date)) >=3 then 1 else 0 end as outreach_count
    from base_table
    where date(outreach_date) BETWEEN date_add(cohortgolivedate, INTERVAL 330 day) AND date_add(cohortgolivedate, INTERVAL 344 day)
    group by 1
),

days_345_360 as(
    select
        patientid,
        case when count(distinct date(outreach_date)) >=3 then 1 else 0 end as outreach_count
    from base_table
    where date(outreach_date) BETWEEN date_add(cohortgolivedate, INTERVAL 345 day) AND date_add(cohortgolivedate, INTERVAL 360 day)
    group by 1
)

select
    patientid,
    ifnull((outreach_count_0_15_days + outreach_count_15_30_days),0) AS outreach_cycles_0_30_days,
    ifnull((outreach_count_30_45_days + outreach_count_45_60_days),0) AS outreach_cycles_30_60_days,
    ifnull((outreach_count_60_75_days + outreach_count_75_90_days),0) AS outreach_cycles_60_90_days,
    ifnull((outreach_count_90_105_days + outreach_count_105_120_days),0) AS outreach_cycles_90_120_days,
    ifnull((outreach_count_120_135_days + outreach_count_135_150_days + outreach_count_150_165_days + outreach_count_165_180_days),0) AS outreach_cycles_120_180_days,
    ifnull((outreach_count_180_195_days + outreach_count_195_210_days + outreach_count_210_225_days + outreach_count_225_240_days + outreach_count_240_255_days + outreach_count_255_270_days),0) AS outreach_cycles_180_270_days,
    ifnull((outreach_count_270_285_days + outreach_count_285_300_days + outreach_count_300_315_days + outreach_count_315_330_days + outreach_count_330_345_days + outreach_count_345_360_days),0) AS outreach_cycles_270_360_days
from
    (select
        mrt_prio.patientid AS patientid,
        days_0_15.outreach_count AS outreach_count_0_15_days,
        days_15_30.outreach_count AS outreach_count_15_30_days,
        days_30_45.outreach_count AS outreach_count_30_45_days,
        days_45_60.outreach_count AS outreach_count_45_60_days,
        days_60_75.outreach_count AS outreach_count_60_75_days,
        days_75_90.outreach_count as outreach_count_75_90_days,
        days_90_105.outreach_count as outreach_count_90_105_days,
        days_105_120.outreach_count as outreach_count_105_120_days,
        days_120_135.outreach_count as outreach_count_120_135_days,
        days_135_150.outreach_count as outreach_count_135_150_days,
        days_150_165.outreach_count as outreach_count_150_165_days,
        days_165_180.outreach_count as outreach_count_165_180_days,
        days_180_195.outreach_count as outreach_count_180_195_days,
        days_195_210.outreach_count as outreach_count_195_210_days,
        days_210_225.outreach_count as outreach_count_210_225_days,
        days_225_240.outreach_count as outreach_count_225_240_days,
        days_240_255.outreach_count as outreach_count_240_255_days,
        days_255_270.outreach_count as outreach_count_255_270_days,
        days_270_285.outreach_count as outreach_count_270_285_days,
        days_285_300.outreach_count as outreach_count_285_300_days,
        days_300_315.outreach_count as outreach_count_300_315_days,
        days_315_330.outreach_count as outreach_count_315_330_days,
        days_330_345.outreach_count as outreach_count_330_345_days,
        days_345_360.outreach_count as outreach_count_345_360_days
    from {{ ref('outreach_prioritization') }} mrt_prio
    left join days_0_15 on mrt_prio.patientid = days_0_15.patientid
    left join days_15_30 on mrt_prio.patientid = days_15_30.patientid
    left join days_30_45 on mrt_prio.patientid = days_30_45.patientid
    left join days_45_60 on mrt_prio.patientid = days_45_60.patientid
    left join days_60_75 on mrt_prio.patientid = days_60_75.patientid
    left join days_75_90 on mrt_prio.patientid = days_75_90.patientid
    left join days_90_105 on mrt_prio.patientid = days_90_105.patientid
    left join days_105_120 on mrt_prio.patientid = days_105_120.patientid
    left join days_120_135 on mrt_prio.patientid = days_120_135.patientid
    left join days_135_150 on mrt_prio.patientid = days_135_150.patientid
    left join days_150_165 on mrt_prio.patientid = days_150_165.patientid
    left join days_165_180 on mrt_prio.patientid = days_165_180.patientid
    left join days_180_195 on mrt_prio.patientid = days_180_195.patientid
    left join days_195_210 on mrt_prio.patientid = days_195_210.patientid
    left join days_210_225 on mrt_prio.patientid = days_210_225.patientid
    left join days_225_240 on mrt_prio.patientid = days_225_240.patientid
    left join days_240_255 on mrt_prio.patientid = days_240_255.patientid
    left join days_255_270 on mrt_prio.patientid = days_255_270.patientid
    left join days_270_285 on mrt_prio.patientid = days_270_285.patientid
    left join days_285_300 on mrt_prio.patientid = days_285_300.patientid
    left join days_300_315 on mrt_prio.patientid = days_300_315.patientid
    left join days_315_330 on mrt_prio.patientid = days_315_330.patientid
    left join days_330_345 on mrt_prio.patientid = days_330_345.patientid
    left join days_345_360 on mrt_prio.patientid = days_345_360.patientid
    )
order by outreach_cycles_90_120_days desc
