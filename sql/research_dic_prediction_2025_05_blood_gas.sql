with
    extract_blood_gas as (
        select
            pivot.icu_stay_id,
            pivot.time,
            pivot.ph,
            pivot.base_excess,
            pivot.lactate,
            pivot.glucose
        from
            (
                select icu_stay_id, time, field_name, value
                from {{ ref("medicu", "one_icu_blood_gas") }}
                where
                    field_name in ('ph', 'base_excess', 'lactate', 'glucose')
                    and sample_type = 'arterial_blood_gas'
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
            ) pivot (
                any_value(value) for field_name
                in ('ph', 'base_excess', 'lactate', 'glucose')
            ) as pivot
    ),
    join_blood_gas as (
        select
            tw.icu_stay_id,
            tw.time_window_index,
            tw.start_time,
            tw.end_time,
            min(bg.ph) as ph,
            min(bg.base_excess) as base_excess,
            max(bg.lactate) as lactate,
            max(bg.glucose) as glucose
        from {{ ref("medicu", "research_dic_prediction_2025_02_time_windows") }} tw
        left join
            extract_blood_gas as bg
            on tw.icu_stay_id = bg.icu_stay_id
            and tw.start_time <= bg.time
            and bg.time < tw.end_time
        group by tw.icu_stay_id, tw.time_window_index, tw.start_time, tw.end_time
    )

select *
from join_blood_gas
