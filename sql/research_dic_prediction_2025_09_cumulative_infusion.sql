with
    extract_infusion as (
        select icu_stay_id, start_time, end_time, ml_per_hour
        from `medicu-beta.snapshots_one_icu_derived.input_amount_rate_20250428`
        where
            source = 'infusions'
            and icu_stay_id in (
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
    infusion_joined as (
        select
            tw.icu_stay_id,
            tw.time_window_index,
            tw.start_time,
            tw.end_time,
            timestamp_diff(
                least(tw.end_time, infusion.end_time),
                greatest(tw.start_time, infusion.start_time),
                hour
            )
            * infusion.ml_per_hour as volume
        from {{ ref("medicu", "research_dic_prediction_2025_02_time_windows") }} tw
        left join
            extract_infusion infusion
            on tw.icu_stay_id = infusion.icu_stay_id
            and tw.start_time <= infusion.end_time
            and infusion.start_time < tw.end_time
    )
select
    icu_stay_id,
    time_window_index,
    start_time,
    end_time,
    sum(volume) as cumulative_infusion_volume
from infusion_joined
group by icu_stay_id, time_window_index, start_time, end_time
