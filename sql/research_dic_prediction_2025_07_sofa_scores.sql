with
    pf_joined as (
        with
            pa as (
                select
                    icu_stay_id,
                    time,
                    case
                        when count(case when is_adjusted is true then 1 end) > 0
                        then max(case when is_adjusted is true then value end)
                        else max(case when is_adjusted is false then value end)
                    end as pao2,
                    case
                        when count(case when is_adjusted is true then 1 end) > 0
                        then true
                        else false
                    end as is_adjusted
                from {{ ref("medicu", "one_icu_blood_gas") }}
                where
                    field_name = 'po2'
                    and sample_type = 'arterial_blood_gas'
                    and value is not null
                group by icu_stay_id, time
            ),
            union_all_respiratory_support as (
                select icu_stay_id, start_time, end_time, fio2_ordered as fio2
                from {{ ref("medicu", "one_icu_mechanical_ventilations") }}
                union all
                select icu_stay_id, start_time, end_time, fio2_ecmo as fio2
                from {{ ref("medicu", "one_icu_ecmo") }}
                where type in ('vv', 'vav')
                union all
                select icu_stay_id, start_time, end_time, fio2_nippv as fio2
                from
                    {{
                        ref(
                            "medicu",
                            "one_icu_non_invasive_positive_pressure_ventilations",
                        )
                    }}
                union all
                select icu_stay_id, start_time, end_time, estimated_fio2 as fio2
                from
                    {{ ref("medicu", "one_icu_derived_oxygen_therapy_estimated_fio2") }}
                union all
                select icu_stay_id, start_time, end_time, fio2_nhf as fio2
                from {{ ref("medicu", "one_icu_high_flow_oxygen_therapy") }}
            ),
            join_fio2 as (
                select pa.icu_stay_id, pa.time, pao2, coalesce(max(rs.fio2), 21) as fio2
                from pa
                left join
                    union_all_respiratory_support rs
                    on pa.icu_stay_id = rs.icu_stay_id
                    and pa.time >= rs.start_time
                    and pa.time < rs.end_time
                group by icu_stay_id, time, pao2
            ),
            pao2fio2ratio as (
                select icu_stay_id, time, pao2, fio2, 100 * pao2 / fio2 as pao2fio2ratio
                from join_fio2
            )
        select
            tw.icu_stay_id,
            tw.time_window_index,
            tw.start_time,
            tw.end_time,
            min(pf.pao2fio2ratio) as pfratio
        from {{ ref("medicu", "research_dic_prediction_2025_02_time_windows") }} tw
        left join
            pao2fio2ratio pf
            on tw.icu_stay_id = pf.icu_stay_id
            and tw.start_time <= pf.time
            and pf.time < tw.end_time
        group by tw.icu_stay_id, tw.time_window_index, tw.start_time, tw.end_time
    )
select *
from pf_joined
