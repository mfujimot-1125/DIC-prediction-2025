with
    dic_score_joined as (
        select
            tw.icu_stay_id,
            tw.in_time,
            tw.out_time,
            tw.time_window_index,
            tw.start_time,
            tw.end_time,
            dic.time_window_index as dic_time_window_index,
            dic.isth_platelet_count_score,
            dic.isth_pt_score,
            dic.isth_fibrinogen_score,
            dic.isth_d_dimer_score
        from {{ ref("medicu", "research_dic_prediction_2025_02_time_windows") }} tw
        left join
            {{ ref("medicu", "one_icu_derived_dic_hourly") }} dic
            on tw.icu_stay_id = dic.icu_stay_id
            and dic.start_time >= tw.end_time
            and datetime_add(tw.end_time, interval 1 day) >= dic.end_time
            and dic.isth_platelet_count_score is not null
    ),
    labeling_dic_diagnosis as (
        select
            icu_stay_id,
            time_window_index,
            start_time,
            end_time,
            case
                when
                    min_by(ifnull(isth_platelet_count_score, 0), dic_time_window_index)
                    + min_by(ifnull(isth_pt_score, 0), dic_time_window_index)
                    + min_by(ifnull(isth_fibrinogen_score, 0), dic_time_window_index)
                    + min_by(ifnull(isth_d_dimer_score, 0), dic_time_window_index)
                    >= 5
                then 1
                else 0
            end as label_dic_diagnosis
        from dic_score_joined
        group by icu_stay_id, time_window_index, start_time, end_time
    )
select *
from labeling_dic_diagnosis
