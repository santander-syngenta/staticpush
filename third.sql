select distinct m150_2.trial_id, m150_2.plot_number, treatment_no, repetition_number, plot_block_number, plot_col_number,										
crop_numbers, crop_common_names, variety_names,	plant_dates, emerge_dates,							
first_plot_application, last_plot_application, application_codes, application_dates, app_method_placement_timing								
from public.mio150_trial_fact_cp m150_2										
left outer join (select distinct trial_id, plot_number, 										
listagg(distinct crop_number, ', ') within group (order by crop_number) as crop_numbers,					
listagg(distinct crop_common_name, ', ') within group (order by crop_number) as crop_common_names,					
listagg(distinct variety_name, ', ') within group (order by crop_number) as variety_names,					
listagg(distinct plant_date, ', ') within group (order by crop_number) as plant_dates,					
listagg(distinct emerge_date , ', ') within group (order by crop_number) as emerge_dates					
from public.mio150_trial_fact_cp						
where trial_id = 'USNB0H1232021'						
and record_type = 'application_product'						
group by 1,2) app_crops on m150_2.trial_id = app_crops.trial_id and m150_2.plot_number = app_crops.plot_number						
left outer join (select distinct trial_id, plot_number, 										
min(application_date) as first_plot_application,					
max(application_date) as last_plot_application,					
listagg(distinct appl_application_code, ', ') within group (order by appl_application_code) as application_codes,					
listagg(distinct application_date , ', ') within group (order by appl_application_code) as application_dates,					
listagg(distinct case when application_method is not null then application_method || ' '  else '' end || 					
case when application_placement is not null then application_placement || ' '  else '' end ||					
case when application_timing is not null then application_timing || ' '  else '' end,  ', ') within group (order by appl_application_code) as app_method_placement_timing					
from public.mio150_trial_fact_cp						
where trial_id = 'USNB0H1232021'						
and record_type = 'application_product'						
group by 1,2) appl on m150_2.trial_id = appl.trial_id and m150_2.plot_number = appl.plot_number						
where m150_2.trial_id = 'USNB0H1232021'									
and record_type = 'application_product'									
order by 1,2										
;										
