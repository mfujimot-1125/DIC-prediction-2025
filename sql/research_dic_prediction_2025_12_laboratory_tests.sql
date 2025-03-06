with
    pivot_laboratory_tests as (
        select *
        from
            (
                select icu_stay_id, time, field_name, value
                from {{ ref("medicu", "one_icu_laboratory_tests_blood") }}
                where
                    field_name in (
                        'wbc',
                        'hemoglobin',
                        'platelet',
                        'creatinine',
                        'total_bilirubin',
                        'crp',
                        'albumin',
                        'activated_partial_thromboplastin_time',
                        'international_normalized_ratio_of_prothrombin_time',
                        'd_dimer',
                        'fibrinogen',
                        'fdp'
                    )
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
                max(value) for field_name in (
                    'wbc',
                    'hemoglobin',
                    'platelet',
                    'creatinine',
                    'total_bilirubin',
                    'crp',
                    'albumin',
                    'activated_partial_thromboplastin_time',
                    'international_normalized_ratio_of_prothrombin_time',
                    'd_dimer',
                    'fibrinogen',
                    'fdp'
                )
            )
    ),
    forward_filled as (
        select
            icu_stay_id,
            time,
            {{
                forward_filling(
                    "wbc",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time"],
                )
            }},
            {{
                forward_filling(
                    "hemoglobin",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time"],
                )
            }},
            {{
                forward_filling(
                    "platelet",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time"],
                )
            }},
            {{
                forward_filling(
                    "creatinine",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time"],
                )
            }},
            {{
                forward_filling(
                    "total_bilirubin",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time"],
                )
            }},
            {{
                forward_filling(
                    "crp",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time"],
                )
            }},
            {{
                forward_filling(
                    "albumin",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time"],
                )
            }},
            {{
                forward_filling(
                    "activated_partial_thromboplastin_time",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time"],
                )
            }},
            {{
                forward_filling(
                    "international_normalized_ratio_of_prothrombin_time",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time"],
                )
            }},
            {{
                forward_filling(
                    "d_dimer",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time"],
                )
            }},
            {{
                forward_filling(
                    "fibrinogen",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time"],
                )
            }},
            {{
                forward_filling(
                    "fdp",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time"],
                )
            }}
        from pivot_laboratory_tests
    ),
    join_laboratory_tests as (
        select
            tw.icu_stay_id,
            tw.time_window_index,
            tw.start_time,
            tw.end_time,
            lab.wbc as wbc,
            lab.hemoglobin as hemoglobin,
            lab.platelet as platelet,
            lab.creatinine as creatinine,
            lab.total_bilirubin as total_bilirubin,
            lab.crp as crp,
            lab.albumin as albumin,
            lab.activated_partial_thromboplastin_time as aptt,
            lab.international_normalized_ratio_of_prothrombin_time as ptinr,
            lab.d_dimer as d_dimer,
            lab.fibrinogen as fibrinogen,
            lab.fdp as fdp
        from {{ ref("medicu", "research_dic_prediction_2025_02_time_windows") }} tw
        left join
            forward_filled as lab
            on tw.icu_stay_id = lab.icu_stay_id
            and tw.start_time <= lab.time
            and lab.time < tw.end_time
    )

select *
from join_laboratory_tests
