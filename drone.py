import pandas as pd
import query as q


def optimized(trial_id):
    engine = q.pd_connect()
    df = pd.read_sql("with TrialList as (select distinct ttf.trial_id from public.mio150_t_trial_fact as ttf where ttf.trial_id in ('" + trial_id + "')) select trial.project_id as Master_Prt, trial.protocol_id as Derived_Prt, trial.trial_id as Trial, trial.trial_year as Trial_Year, trial.country_code as Country_Code, trial.state_prov as State_Province, trial.site_type as Site_Type, trial.investigator as Syngenta_FieldScientist, trial.trial_origin as Trial_Placement, trial.latitude as Latitude, trial.longitude as Longitude, null as Cooperator, trial.study_design_code as Exp_Design, null as Quality, trial.trial_status_code as trial_status_code, assmt.assessment_number as Assmt_Num, null as Last_Assessment_Flag, assmt.plant_date as Plant_Date, assmt.rating_date as Assmt_Date, assmt.emerge_date as Emergence_Date, null as First_Appl_Date_Trt, null as First_Appl_Date_Trial, null as Most_Recent_Appl_Date_Trt, null as Most_Recent_Appl_Date_Trial,  null as DAP, null as DAE, null as DAF_Appl_Trial, null as DAMR_Appl_Trial, null as DAF_Appl_Trt, null as DAMR_Appl_Trt, assmt.crop_id_code as Crop_Code, assmt.crop_common_name as Crop, assmt.crop_variety as Variety, assmt.crop_description as Crop_Attributes, assmt.crop_stage_min as Crop_Stg_Min, assmt.crop_stage_max as Crop_Stg_Max, assmt.pest_code as Pest_Code, assmt.pest_scientific_name as Pest, assmt.pest_description as Pest_Attributes, assmt.pest_stage_min as Pest_Stg_Min, assmt.pest_stage_max as Pest_Stg_Max, assmt.rating_data as Assmt_Type, assmt.part_rated1 as Part_Rated, assmt.Part_Rated_Role, assmt.rating_unit as Unit_Reporting_Basis, null as ConTrt, trial.treatment_number as Trt_Num, trial.Trt_Flag, trial.FactorA, trial.FactorA_Level_Desc, trial.FactorB, trial.FactorB_Level_Desc, trial.FactorC, trial.FactorC_Level_Desc, trial.MITF_Code, trial.MITF_Level, assmt.plot_number as Plot_ID, null as Range, null as Row, assmt.repetition_number as Rep_Num, null as Trt_Label, assmt.SE_Name, null as Assmt_Label, assmt.Rep_Value, null as Check_Mean_Value, null as Trt_Mean_Value, null as Check_PCT FROM (select distinct t01.treatment_fact_guid,t01.project_id, t01.protocol_id, t01.trial_id, t01.trial_year, t01.country_code, case when t01.state_prov_code is not null then t01.state_prov_code else t01.state_prov end as state_prov, t01.site_type, t01.investigator, t01.trial_origin, t01.latitude, t01.longitude, t01.study_design_code, t01.trial_status_code, t01.treatment_number, null as Trt_Flag,null as FactorA, null as FactorA_Level_Desc, null as FactorB, null as FactorB_Level_Desc, null as FactorC, null as FactorC_Level_Desc, null as MITF_Code, null as MITF_Level, null as Trt_Label from TrialList inner join public.mio150_t_trial_fact as t01 on TrialList.trial_id = t01.trial_id) as trial inner join (select distinct t02.treatment_fact_guid, t02.operation_fact_guid, t02.assessment_number, t02.plant_date, t02.rating_date, t02.emerge_date, t02.crop_id_code, t02.crop_common_name, t02.crop_variety, t02.crop_description, t02.crop_stage_min, t02.crop_stage_max, t02.pest_code, t02.pest_scientific_name, t02.pest_description, t02.pest_stage_min, t02.pest_stage_max, t02.rating_data, t02.part_rated1, null as Part_Rated_Role, t02.rating_unit, t02.plot_number, t02.repetition_number, t02.se_description as SE_Name, t02.observation_value_num as Rep_Value from TrialList inner join public.mio150_t_trial_fact as t02 on TrialList.trial_id = t02.trial_id where operation_type = 'assessment') as assmt on trial.treatment_fact_guid = assmt.treatment_fact_guid order by trial.project_id, trial.protocol_id, trial.trial_id, assmt.assessment_number, trial.treatment_number, assmt.repetition_number;", engine)
    return df


def drone(dp_shortname):
    engine = q.pd_connect()
    df= pd.read_sql("SELECT protocol_id as derived_prt, trial_arm as trial, plot_num as plot_id, row as row, treatment_num as trt_num, metric_name, data_output as trt_mean_value FROM mio.spectrum_schema.trial_drone_data WHERE protocol_id='" + dp_shortname + "';", engine)
    return df


def combine(trial_id, shortname):
    opt = optimized(trial_id)
    drone_df = drone(shortname)
    ###Checks for non-unique columns that can be applied to drone DataFrame. i.e. State, Trial-ID, etc.
    copy_cols = []
    for col in opt.columns:
        unique_values = len(opt[col].unique())
        if unique_values == 1:
            copy_cols.append(col)
        else:
            pass
    ###Inserts columns copied from optimized dataset to drone dataframe
    for col in copy_cols:
        if col not in drone_df.columns:
            drone_df.insert(len(drone_df.columns), col, opt[col][0])

    ###Outer join between the two dataframes
    output = pd.concat([opt, drone_df], axis=0)
    return output


def print_xlsx(trial_id, shortname):
    df = combine(trial_id, shortname)
    df.to_excel(shortname + '.xlsx', index=False, header=True)
