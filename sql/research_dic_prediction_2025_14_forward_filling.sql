with
    forward_filling as (
        select
            icu_stay_id,
            hospital_id,
            female,
            height,
            age,
            time_window_index,
            label_dic_diagnosis,
            infected_nervous_system,
            infected_cardiovascular,
            infected_respiratory,
            infected_abdomen,
            infected_urinary_tract,
            infected_soft_tissue,
            infected_other,
            charlson_comorbidity_index,
            {{
                forward_filling(
                    "ph",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time_window_index"],
                )
            }},
            {{
                forward_filling(
                    "base_excess",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time_window_index"],
                )
            }},
            {{
                forward_filling(
                    "lactate",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time_window_index"],
                )
            }},
            {{
                forward_filling(
                    "glucose",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time_window_index"],
                )
            }},
            {{
                forward_filling(
                    "urine_output",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time_window_index"],
                )
            }},
            {{
                forward_filling(
                    "pfratio",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time_window_index"],
                )
            }},
            {{
                forward_filling(
                    "bt50",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time_window_index"],
                )
            }},
            {{
                forward_filling(
                    "bt90",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time_window_index"],
                )
            }},
            {{
                forward_filling(
                    "hr10",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time_window_index"],
                )
            }},
            {{
                forward_filling(
                    "hr50",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time_window_index"],
                )
            }},
            {{
                forward_filling(
                    "hr90",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time_window_index"],
                )
            }},
            {{
                forward_filling(
                    "hr_sd",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time_window_index"],
                )
            }},
            {{
                forward_filling(
                    "rr10",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time_window_index"],
                )
            }},
            {{
                forward_filling(
                    "rr50",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time_window_index"],
                )
            }},
            {{
                forward_filling(
                    "rr90",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time_window_index"],
                )
            }},
            {{
                forward_filling(
                    "rr_sd",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time_window_index"],
                )
            }},
            {{
                forward_filling(
                    "sbp10",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time_window_index"],
                )
            }},
            {{
                forward_filling(
                    "sbp50",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time_window_index"],
                )
            }},
            {{
                forward_filling(
                    "sbp90",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time_window_index"],
                )
            }},
            {{
                forward_filling(
                    "sbp_sd",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time_window_index"],
                )
            }},
            {{
                forward_filling(
                    "mbp10",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time_window_index"],
                )
            }},
            {{
                forward_filling(
                    "mbp50",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time_window_index"],
                )
            }},
            {{
                forward_filling(
                    "mbp90",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time_window_index"],
                )
            }},
            {{
                forward_filling(
                    "mbp_sd",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time_window_index"],
                )
            }},
            {{
                forward_filling(
                    "dbp10",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time_window_index"],
                )
            }},
            {{
                forward_filling(
                    "dbp50",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time_window_index"],
                )
            }},
            {{
                forward_filling(
                    "dbp90",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time_window_index"],
                )
            }},
            {{
                forward_filling(
                    "dbp_sd",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time_window_index"],
                )
            }},
            {{
                forward_filling(
                    "spo2_10",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time_window_index"],
                )
            }},
            {{
                forward_filling(
                    "spo2_50",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time_window_index"],
                )
            }},
            {{
                forward_filling(
                    "spo2_90",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time_window_index"],
                )
            }},
            {{
                forward_filling(
                    "spo2_sd",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time_window_index"],
                )
            }},
            {{
                forward_filling(
                    "cumulative_infusion_volume",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time_window_index"],
                )
            }},
            {{
                forward_filling(
                    "gcs_e",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time_window_index"],
                )
            }},
            {{
                forward_filling(
                    "gcs_v",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time_window_index"],
                )
            }},
            {{
                forward_filling(
                    "gcs_m",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time_window_index"],
                )
            }},
            adrenaline,
            noradrenaline,
            vasopressin,
            {{
                forward_filling(
                    "wbc",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time_window_index"],
                )
            }},
            {{
                forward_filling(
                    "hemoglobin",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time_window_index"],
                )
            }},
            {{
                forward_filling(
                    "platelet",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time_window_index"],
                )
            }},
            {{
                forward_filling(
                    "creatinine",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time_window_index"],
                )
            }},
            {{
                forward_filling(
                    "total_bilirubin",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time_window_index"],
                )
            }},
            {{
                forward_filling(
                    "crp",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time_window_index"],
                )
            }},
            {{
                forward_filling(
                    "albumin",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time_window_index"],
                )
            }},
            {{
                forward_filling(
                    "aptt",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time_window_index"],
                )
            }},
            {{
                forward_filling(
                    "ptinr",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time_window_index"],
                )
            }},
            {{
                forward_filling(
                    "d_dimer",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time_window_index"],
                )
            }},
            {{
                forward_filling(
                    "fibrinogen",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time_window_index"],
                )
            }},
            {{
                forward_filling(
                    "fdp",
                    "unbounded",
                    partition_keys=["icu_stay_id"],
                    order=["time_window_index"],
                )
            }}
        from {{ ref("medicu", "research_dic_prediction_2025_13_join_all_features") }}
    )
select *
from forward_filling
