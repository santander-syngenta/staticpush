with TrialList as (select distinct trial_id from public.mio150_trial_fact_cp where trial_id in ('USNB0H1232021'))							
select 							
trial_id as Trial,							
null as Assmt_Num,							
null as Last_Assessment_Flag,							
null as Plant_Date,							
Assmt_Date,							
null as Emergence_Date,							
null as First_Appl_Date_Trt,							
null as First_Appl_Date_Trial,							
null as Most_Recent_Appl_Date_Trt,							
null as Most_Recent_Appl_Date_Trial,							
null as DAP,							
null as DAE,							
null as DAF_Appl_Trial,							
null as DAMR_Appl_Trial,							
null as DAF_Appl_Trt,							
null as DAMR_Appl_Trt,							
null as Crop_Code,							
null as Crop,							
null as Variety,							
null as Crop_Attributes,							
null as Crop_Stg_Min,							
null as Crop_Stg_Max,							
null as Pest_Code,							
null as Pest,							
null as Pest_Attributes,							
null as Pest_Stg_Min,							
null as Pest_Stg_Max,							
Assmt_Type,							
null as Part_Rated,							
null as Part_Rated_Role,							
null as Unit_Reporting_Basis,							
case when Plot_Type = 'CHK' then cast(Plot_ID as nvarchar) || '_chk' else cast(Plot_ID as nvarchar)end as Plot_ID,							
null as Range,							
null as Row,							
Plot_Type,							
treatment_number,							
null as Rep_Num,							
null as SE_Name,							
Assmt_Label,							
Rep_Value,							
null as Check_Mean_Value,							
null as Trt_Mean_Value,							
null as Check_PCT,							
null as artificial_population, 							
sub_Values,							
sub_Nums,							
sub_RowCol							
from (select UAV.trial_id,							
		UAV.Assmt_Date,					
		UAV.Assmt_Type,					
		UAV.Plot_ID,					
		UAV.Plot_Type,					
		UAV.treatment_number,					
		UAV.Assmt_Label,					
        avg(UAV.data_output) as Rep_Value, 							
        listagg(case when strpos(text_output,'000') > strpos(text_output,'.') then left(text_output, strpos(text_output,'000')-1) 							
				 	else case when strpos(text_output,'999') > strpos(text_output,'.') then left(text_output, strpos(text_output,'999')-2) || cast(cast(right(left(text_output, strpos(text_output,'999')-1),1) as integer) + 1 as nvarchar)		
				 	else text_output end end, ', ') within group (order by subplot_num) as sub_Values,		
        listagg(UAV.subplot_num, ', ') within group (order by subplot_num) as sub_Nums,							
        listagg(cast(UAV.uav_row as nvarchar) || ', ' || cast(UAV.uav_col as nvarchar), '; ') within group (order by subplot_num) as sub_RowCol							
		from (select distinct trial_arm as trial_id, 					
		                        flight_date as Assmt_Date,					
		                        metric_name as Assmt_Type,					
		                        plot_num as Plot_ID, 					
		                        trt_chk_plottype as Plot_Type, 					
		                        treatment_num as treatment_number, 					
		                        case when company is not null then company  || ' ' else '' end ||					
		                             case when site_name is not null then site_name  ||' '  else '' end ||					
		                             case when flightblock_name is not null then flightblock_name  || ' '  else '' end ||					
		                             case when flight_date is not null then flight_date  || ' '  else '' end ||					
		                             case when flight_time is not null then flight_time  || ' '  else '' end ||					
		                             case when metric_name is not null then metric_name  || ' '  else '' end ||					
		                             case when generated_on is not null then ' generated on: ' || generated_on end as Assmt_Label,					
		                        data_output, 					
		                        cast(data_output as nvarchar) as text_output,					
		         				subplot_num, uav_raw."column" as uav_col, uav_raw."row" as uav_row	
		                from spectrum_schema.trial_drone_data uav_raw					
		                where uav_raw.trial_arm in (select trial_id from TrialList)					
		                and uav_raw.data_output is not null and plot_num is not null) UAV					
		group by 1,2,3,4,5,6,7)                    					
;							
