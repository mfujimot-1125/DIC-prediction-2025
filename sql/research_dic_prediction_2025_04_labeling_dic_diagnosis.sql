with
    first_dic as (
      select
        icu_stay_id,
        timestamp_trunc(min(start_time), day) as first_dic_time,
        1 as label_dic_diagnosis
      from {{ ref("medicu", "one_icu_derived_dic_hourly") }}
      where isth_dic_score >= 5 and time_window_index >= 0
      group by icu_stay_id
    ),
    dic_score_joined as (
         select
            tw.icu_stay_id,
            tw.time_window_index,
            tw.start_time,
            tw.end_time,
            ifnull(label_dic_diagnosis, 0) as label_dic_diagnosis
            from {{ ref("medicu", "research_dic_prediction_2025_02_time_windows") }} tw
            left join first_dic
            on tw.icu_stay_id = first_dic.icu_stay_id 
            and tw.end_time = first_dic.first_dic_time
    )
select *
from dic_score_joined
