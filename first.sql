select distinct project_id,protocol_id,m150_0.trial_id,trial_year,country_code,state_prov_code,state_prov,city,site_type,investigator,trial_origin,latitude,longitude,cooperator,study_design_code,												
null as Quality,										
trial_status,climate_zone_code,climate_zone_decode,												
null as Trial_TrialDesignType, null as Forced_Single_Factor,												
first_trial_application, last_trial_application, application_dates												
from public.mio150_trial_fact_cp m150_0												
left outer join (select distinct trial_id, min(application_date) as first_trial_application, max(application_date) as last_trial_application,												
listagg(distinct application_date, ', ') within group (order by appl_application_code) as application_dates					
from public.mio150_trial_fact_cp 					
where trial_id ='USNB0H1232021'					
group by 1) appl_dates on m150_0.trial_id = appl_dates.trial_id					
where m150_0.trial_id ='USNB0H1232021';												
