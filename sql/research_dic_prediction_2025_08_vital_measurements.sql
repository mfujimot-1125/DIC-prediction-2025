with
    extract_vital_measurements as (
        select
            icu_stay_id,
            time,
            timestamp_trunc(time, day) as start_time,
            coalesce(vs.bt_core, vs.bt_surface) as bt,
            hr,
            rr,
            coalesce(invasive_sbp, non_invasive_sbp) as sbp,
            coalesce(invasive_mbp, non_invasive_mbp) as mbp,
            coalesce(invasive_dbp, non_invasive_dbp) as dbp,
            spo2
        from `medicu-beta.snapshots_one_icu.vital_measurements_20250428` as vs
        where
            icu_stay_id in (
                select icu_stay_id
                from
                    {{
                        ref(
                            "medicu",
                            "research_dic_prediction_2025_01_eligibility_criteria",
                        )
                    }}
            )
    ),
    join_vital_measurements as (
        select distinct
            icu_stay_id,
            time_window_index,
            start_time,
            end_time,
            percentile_cont(bt, 0.5) over icu_stay_24hr as bt50,
            percentile_cont(bt, 0.9) over icu_stay_24hr as bt90,
            -- hr
            percentile_cont(hr, 0.1) over icu_stay_24hr as hr10,
            percentile_cont(hr, 0.5) over icu_stay_24hr as hr50,
            percentile_cont(hr, 0.9) over icu_stay_24hr as hr90,
            stddev(hr) over icu_stay_24hr as hr_sd,
            -- rr
            percentile_cont(rr, 0.1) over icu_stay_24hr as rr10,
            percentile_cont(rr, 0.5) over icu_stay_24hr as rr50,
            percentile_cont(rr, 0.9) over icu_stay_24hr as rr90,
            stddev(rr) over icu_stay_24hr as rr_sd,
            -- sbp
            percentile_cont(sbp, 0.1) over icu_stay_24hr as sbp10,
            percentile_cont(sbp, 0.5) over icu_stay_24hr as sbp50,
            percentile_cont(sbp, 0.9) over icu_stay_24hr as sbp90,
            stddev(sbp) over icu_stay_24hr as sbp_sd,
            -- mbp
            percentile_cont(mbp, 0.1) over icu_stay_24hr as mbp10,
            percentile_cont(mbp, 0.5) over icu_stay_24hr as mbp50,
            percentile_cont(mbp, 0.9) over icu_stay_24hr as mbp90,
            stddev(mbp) over icu_stay_24hr as mbp_sd,
            -- dbp
            percentile_cont(dbp, 0.1) over icu_stay_24hr as dbp10,
            percentile_cont(dbp, 0.5) over icu_stay_24hr as dbp50,
            percentile_cont(dbp, 0.9) over icu_stay_24hr as dbp90,
            stddev(dbp) over icu_stay_24hr as dbp_sd,
            -- spo2
            percentile_cont(spo2, 0.1) over icu_stay_24hr as spo2_10,
            percentile_cont(spo2, 0.5) over icu_stay_24hr as spo2_50,
            percentile_cont(spo2, 0.9) over icu_stay_24hr as spo2_90,
            stddev(spo2) over icu_stay_24hr as spo2_sd
        from {{ ref("medicu", "research_dic_prediction_2025_02_time_windows") }}
        left join extract_vital_measurements using (icu_stay_id, start_time)
        window icu_stay_24hr as (partition by icu_stay_id, time_window_index)
    )
select *
from join_vital_measurements
