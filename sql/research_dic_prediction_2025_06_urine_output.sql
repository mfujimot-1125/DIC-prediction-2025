with
    uo_extract as (
        select icu_stay_id, time, sum(urine) as urine_output
        from `medicu-beta.snapshots_one_icu.out_20250428`
        where
            urine is not null
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
        group by icu_stay_id, time
    ),
    join_urine_output as (
        select
            tw.icu_stay_id,
            tw.time_window_index,
            tw.start_time,
            tw.end_time,
            sum(uo.urine_output) as urine_output
        from {{ ref("medicu", "research_dic_prediction_2025_02_time_windows") }} tw
        left join
            uo_extract as uo
            on tw.icu_stay_id = uo.icu_stay_id
            and tw.start_time <= uo.time
            and uo.time < tw.end_time
        group by tw.icu_stay_id, tw.time_window_index, tw.start_time, tw.end_time
    )
select *
from join_urine_output
