with
    define_first_time_window_start_time as (
        select
            extended_icu_stays.*,
            timestamp_trunc(in_time, day) as first_time_window_start_time
        from
            {{ ref("medicu", "one_icu_derived_extended_icu_stays") }} extended_icu_stays
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
    censor_outcome_or_treatment as (
        select icu_stay_id, timestamp_trunc(min(start_time), day) as censoring_time
        from {{ ref("medicu", "one_icu_derived_dic_hourly") }}
        where isth_dic_24hours >= 5
        group by icu_stay_id
        union all
        select icu_stay_id, timestamp_trunc(min(time), day) as censoring_time
        from {{ ref("medicu", "one_icu_injections") }}
        left join
            {{ ref("medicu", "one_icu_injection_components") }} using (injection_id)
        where
            injection_product_name
            in ("nonthron", "neuart", "anthrobin", "acolan", "recomodulin")
        group by icu_stay_id
        union all
        select icu_stay_id, timestamp_trunc(min(start_time), day) as censoring_time
        from {{ ref("medicu", "one_icu_blood_transfusions") }}
        left join
            {{ ref("medicu", "one_icu_blood_transfusion_components") }} using (
                blood_transfusion_id
            )
        where blood_product_name like "pc%"
        group by icu_stay_id
        union all
        select icu_stay_id, timestamp_trunc(out_time, day) as censoring_time
        from {{ ref("medicu", "one_icu_derived_extended_icu_stays") }}
        union all
        select icu_stay_id, datetime_add(first_time_window_start_time, interval 7 day) as censoring_time
        from define_first_time_window_start_time
    ),
    first_censoring as (
        select icu_stay_id, min(censoring_time) as censoring_time
        from censor_outcome_or_treatment
        group by icu_stay_id
    ),
    define_time_window_end_time as (
        select
            define_first_time_window_start_time.*,
            timestamp_sub(censoring_time, interval 1 day) as time_window_end_time
        from define_first_time_window_start_time
        left join first_censoring using (icu_stay_id)
    ),
    generate_time_window_indices as (
        select
            define_time_window_end_time.*,
            generate_array(
                0,
                cast(
                    floor(
                        timestamp_diff(
                            time_window_end_time, first_time_window_start_time, day
                        )
                    ) as int64
                )
            ) as time_window_indices
        from define_time_window_end_time
    ),
    generate_time_windows as (
        with
            time_window_index_joined as (
                select twi.*, time_window_index
                from generate_time_window_indices twi
                cross join unnest(twi.time_window_indices) as time_window_index
            )
        select
            icu_stay_id,
            in_time,
            out_time,
            time_window_index,
            timestamp_add(
                first_time_window_start_time, interval time_window_index day
            ) as start_time,
            timestamp_add(
                first_time_window_start_time, interval time_window_index + 1 day
            ) as end_time
        from time_window_index_joined
    )
select *
from generate_time_windows
