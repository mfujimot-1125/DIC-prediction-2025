with
    sofa_joined as (
        select
            tw.icu_stay_id,
            tw.time_window_index,
            tw.start_time,
            tw.end_time,
            max(sofa.respiration_24hours) as sofa_respiration
        from {{ ref("medicu", "research_dic_prediction_2025_02_time_windows") }} tw
        left join
            {{ ref("medicu", "one_icu_derived_sofa_hourly") }} sofa
            on tw.icu_stay_id = sofa.icu_stay_id
            and tw.start_time <= sofa.start_time
            and sofa.end_time < tw.end_time
        group by tw.icu_stay_id, tw.time_window_index, tw.start_time, tw.end_time
    )
select *
from sofa_joined
