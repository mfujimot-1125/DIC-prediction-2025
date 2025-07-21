with
    gcs as (
        select icu_stay_id, time, gcs_e, gcs_v, gcs_m
        from `medicu-beta.snapshots_one_icu.gcs_20250428`
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
    gcs_joined as (
        select
            tw.icu_stay_id,
            tw.time_window_index,
            tw.start_time,
            tw.end_time,
            {{ max_by_ignore_nulls("gcs.gcs_e", "gcs.time") }} as gcs_e,
            {{ max_by_ignore_nulls("gcs.gcs_v", "gcs.time") }} as gcs_v,
            {{ max_by_ignore_nulls("gcs.gcs_m", "gcs.time") }} as gcs_m
        from {{ ref("medicu", "research_dic_prediction_2025_02_time_windows") }} tw
        left join
            gcs
            on tw.icu_stay_id = gcs.icu_stay_id
            and tw.start_time <= gcs.time
            and gcs.time < tw.end_time
        group by icu_stay_id, time_window_index, start_time, end_time
    )
select *
from gcs_joined
