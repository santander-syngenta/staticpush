select distinct case when project_id is null then protocol_id else project_id end as Master_Prt,case when protocol_id is null then project_id else protocol_id end as Derived_Prt,														
m150_0.trial_id as Trial,trial_year as Trial_Year,country_code as Country_Code,state_prov_code as State_province_code,state_prov as State_province_name,														
city  as City,site_type as Site_Type,investigator  as Syngenta_FieldScientist, trial_origin as Trial_Placement, 														
latitude as Latitude,longitude as Longitude, cooperator as Cooperator, study_design_code as Exp_Design,														
                            trial_reliability as Quality, 														
                            trial_status as Trial_Status, climate_zone_code as Climate_Zone_Code,climate_zone_decode as Climate_Zone_Decode,														
                            null as Trial_TrialDesignType, null as Forced_Single_Factor, -- potentially add later														
                            first_trial_application, last_trial_application, application_dates														
            from public.mio150_trial_fact_cp m150_0														
            left outer join (select distinct trial_id, min(application_date) as first_trial_application, max(application_date) as last_trial_application,														
							    listagg(distinct application_date, ', ') within group (order by appl_application_code) as application_dates							
							            from public.mio150_trial_fact_cp 							
							            where trial_id ='USNB0H1232021'							
							group by 1) appl_dates on m150_0.trial_id = appl_dates.trial_id							
where m150_0.trial_id ='USNB0H1232021'														
and operation_type is not null;														