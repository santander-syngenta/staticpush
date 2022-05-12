with TrialList as (select distinct trial_id from public.mio150_trial_fact_cp where trial_id in ('USNB0H1232021'))										
select distinct m150_2.trial_id, m150_2.plot_number, treatment_no, repetition_number, plot_block_number, plot_col_number,										
		crop_numbers, crop_common_names, variety_names,	plant_dates, emerge_dates							
from TrialList inner join public.mio150_trial_fact_cp m150_2 on TrialList.trial_id = m150_2.trial_id 										
    left outer join (select m150_2a.trial_id, plot_number, 										
					listagg(distinct crop_number, ', ') within group (order by crop_number) as crop_numbers,					
					listagg(distinct crop_common_name, ', ') within group (order by crop_number) as crop_common_names,					
					listagg(distinct variety_name, ', ') within group (order by crop_number) as variety_names,					
					listagg(distinct plant_date, ', ') within group (order by crop_number) as plant_dates,					
					listagg(distinct emerge_date , ', ') within group (order by crop_number) as emerge_dates					
				from TrialList inner join public.mio150_trial_fact_cp m150_2a on TrialList.trial_id = m150_2a.trial_id 						
				and record_type = 'application_product'						
				group by 1,2) app_crops on m150_2.trial_id = app_crops.trial_id and m150_2.plot_number = app_crops.plot_number						
where record_type = 'application_product'										
/*this could be repeated for targets and extended to get stage information*/										
order by 1,2;										