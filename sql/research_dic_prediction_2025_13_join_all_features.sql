with
    join_static_variables as (
        select time_windows.*, static_variables.* except (icu_stay_id)
        from
            {{ ref("medicu", "research_dic_prediction_2025_02_time_windows") }} time_windows
        left join
            {{ ref("medicu", "research_dic_prediction_2025_03_static_variables") }} static_variables
            using (icu_stay_id)
    ),
    join_labeling_dic_diagnosis as (
        select
            join_static_variables.*,
            labeling_dic_diagnosis.* except (
                icu_stay_id, time_window_index, start_time, end_time
            )
        from join_static_variables
        left join
            {{ ref("medicu", "research_dic_prediction_2025_04_labeling_dic_diagnosis") }} labeling_dic_diagnosis 
            using (icu_stay_id, time_window_index)
    ),
    join_blood_gas as (
        select
            join_labeling_dic_diagnosis.*,
            blood_gas.* except (icu_stay_id, time_window_index, start_time, end_time)
        from join_labeling_dic_diagnosis
        left join
            {{ ref("medicu", "research_dic_prediction_2025_05_blood_gas") }} blood_gas
            using (icu_stay_id, time_window_index)
    ),
    join_urine_output as (
        select
            join_blood_gas.*,
            urine_output.* except (icu_stay_id, time_window_index, start_time, end_time)
        from join_blood_gas
        left join
            {{ ref("medicu", "research_dic_prediction_2025_06_urine_output") }} urine_output
            using (icu_stay_id, time_window_index)
    ),
    join_pf_ratios as (
        select
            join_urine_output.*,
            pf_ratios.* except (icu_stay_id, time_window_index, start_time, end_time)
        from join_urine_output
        left join
            {{ ref("medicu", "research_dic_prediction_2025_07_pfratios") }} pf_ratios
            using (icu_stay_id, time_window_index)
    ),
    join_vital_measurements as (
        select
            join_pf_ratios.*,
            vital_measurements.* except (
                icu_stay_id, time_window_index, start_time, end_time
            )
        from join_sofa_scores
        left join
            {{ ref("medicu", "research_dic_prediction_2025_08_vital_measurements") }} vital_measurements
            using (icu_stay_id, time_window_index)
    ),
    join_cumulative_infusion as (
        select
            join_vital_measurements.*,
            cumulative_infusion.* except (
                icu_stay_id, time_window_index, start_time, end_time
            )
        from join_vital_measurements
        left join
            {{ ref("medicu", "research_dic_prediction_2025_09_cumulative_infusion") }} cumulative_infusion
            using (icu_stay_id, time_window_index)
    ),
    join_gcs as (
        select
            join_cumulative_infusion.*,
            gcs.* except (icu_stay_id, time_window_index, start_time, end_time)
        from join_cumulative_infusion
        left join
            {{ ref("medicu", "research_dic_prediction_2025_10_gcs") }} gcs using (
                icu_stay_id, time_window_index
            )
    ),
    join_vasoactive_drugs as (
        select
            join_gcs.*,
            vasoactive_drugs.* except (
                icu_stay_id, time_window_index, start_time, end_time
            )
        from join_gcs
        left join
            {{ ref("medicu", "research_dic_prediction_2025_11_vasoactive_drugs") }} vasoactive_drugs
            using (icu_stay_id, time_window_index)
    ),
    join_laboratory_tests as (
        select
            join_vasoactive_drugs.*,
            laboratory_tests.* except (
                icu_stay_id, time_window_index, start_time, end_time
            )
        from join_vasoactive_drugs
        left join
            {{ ref("medicu", "research_dic_prediction_2025_12_laboratory_tests") }} laboratory_tests
            using (icu_stay_id, time_window_index)
    )
select *
from join_laboratory_tests
