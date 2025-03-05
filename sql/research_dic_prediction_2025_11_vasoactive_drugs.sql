with
    vasoactive_drugs as (
        with
            extend_icu_stays as (
                {{
                    derived_extended_icu_stays(
                        "one_icu_body_weight_measurements",
                        "one_icu_icu_stays",
                        "one_icu_hospital_admissions",
                        "one_icu_patients",
                    )
                }}
            ),
            weights as (select icu_stay_id, body_weight_imputed from extend_icu_stays),
            infusions as (
                select
                    infusion_id,
                    icu_stay_id,
                    start_time,
                    end_time,
                    active_ingredient_name,
                    active_ingredient_per_unit_injection_product
                    * unit_injection_product_per_ml
                    * ml_per_hour as rate,
                    body_weight_imputed,
                from {{ macro_input("one_icu_infusions") }}
                left join
                    {{ macro_input("one_icu_infusion_components") }} using (infusion_id)
                left join
                    {{ macro_input("standard_injection_product_active_ingredients") }}
                    using (injection_product_name)
                left join weights using (icu_stay_id)
            ),
            drugs as (
                select
                    icu_stay_id,
                    start_time,
                    end_time,
                    rate,
                    body_weight_imputed,
                    active_ingredient_name,
                    case
                        when active_ingredient_name in ('adrenaline', 'noradrenaline')
                        then rate * 1000 / 60 / body_weight_imputed
                        when active_ingredient_name = 'vasopressin'
                        then rate / 60
                    end as converted_rate
                from infusions
                where
                    active_ingredient_name
                    in ('adrenaline', 'noradrenaline', 'vasopressin')
            ),
            rates as (
                select *
                from
                    drugs pivot (
                        max(converted_rate) for active_ingredient_name
                        in ('adrenaline', 'noradrenaline', 'vasopressin')
                    )
            )
        select *
        from rates
    ),
    vasoactive_drugs_joined as (
        select
            tw.icu_stay_id,
            tw.time_window_index,
            tw.start_time,
            tw.end_time,
            max(adrenaline) as adrenaline,
            max(noradrenaline) as noradrenaline,
            max(vasopressin) as vasopressin
        from {{ ref("medicu", "research_dic_prediction_2025_02_time_windows") }} tw
        left join
            vasoactive_drugs vd
            on tw.icu_stay_id = vd.icu_stay_id
            and tw.end_time > vd.start_time
            and tw.start_time <= vd.end_time
        group by tw.icu_stay_id, tw.time_window_index, tw.start_time, tw.end_time
    )
select *
from vasoactive_drugs_joined
