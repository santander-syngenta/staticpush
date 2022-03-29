select																				
trial_id as Trial,																				
Assmt_Num,																				
Last_Assessment_Flag,																				
Plant_Date,																				
Assmt_Date,																				
Emergence_Date,																				
null as First_Appl_Date_Trt,																				
First_Appl_Date_Trial,																				
null as Most_Recent_Appl_Date_Trt,																				
null as Most_Recent_Appl_Date_Trial,																				
DAP,																				
DAE,																				
DAF_Appl_Trial,																				
null as DAMR_Appl_Trial,																				
null as DAF_Appl_Trt,																				
null as DAMR_Appl_Trt,																				
Crop_Code,																				
Crop,																				
Variety,																				
Crop_Attributes,																				
Crop_Stg_Min,																				
Crop_Stg_Max,																				
Pest_Code,																				
Pest,																				
Pest_Attributes,																				
Pest_Stg_Min,																				
Pest_Stg_Max,																				
Assmt_Type,																				
Part_Rated,																				
Part_Rated_Role,																				
Unit_Reporting_Basis,																				
 cast(Plot_ID as nvarchar) as Plot_ID,																				
Range,																				
Row,																				
treatment_number,																				
Rep_Num,																				
SE_Name,																				
Assmt_Label,																				
Rep_Value,																				
null as Check_Mean_Value,																				
null as Trt_Mean_Value,																				
null as Check_PCT,																				
null as artificial_population, 																				
sub_Values,																				
sub_Nums,																				
null as sub_RowCol																				
from (select AssessHeader.*, case when subsamples > 1 then SubVal.Rep_Value else RepVal.Rep_Value end as Rep_Value, SubVal.sub_Values, SubVal.sub_Nums																				
		from (select distinct m150_3a.trial_id, 																			
										m150_3a.assessment_number as Assmt_Num, 																			
										case when rating_date = MaxAssmt.LastAssmt then 'Y' else 'N' end as Last_Assessment_Flag,																			
										CropDetails.plant_date as Plant_Date,																			
										rating_date as Assmt_Date,																			
										CropDetails.emerge_date as Emergence_Date,																			
										AppDates.FirstAppl as First_Appl_Date_Trial,																			
										Assmt_Date - CropDetails.plant_date as DAP,																			
										Assmt_Date - CropDetails.emerge_date as DAE,																			
										Assmt_Date - AppDates.FirstAppl as DAF_Appl_Trial,																			
										crop_species_id as Crop_Code,																			
										crop_name as Crop,																			
										crop_variety as Variety,																			
										CropDetails.crop_description as Crop_Attributes,																			
										crop_stage_min as Crop_Stg_Min,																			
										crop_stage_max as Crop_Stg_Max,																			
										pest_code as Pest_Code,																			
										pest_name as Pest,																			
										pest_artificial_population as artificial_population,																			
										PestDetails.pest_description as Pest_Attributes,																			
										pest_stage_min as Pest_Stg_Min,																			
										pest_stage_max as Pest_Stg_Max,																			
										rating_data as Assmt_Type,																			
										part_rated1 as Part_Rated,																			
										null as Part_Rated_Role,																			
										case when reporting_basis_unit is not null then rating_unit || ' per ' || cast(reporting_basis as INT) || ' ' || reporting_basis_unit else rating_unit end as Unit_Reporting_Basis,																			
										m150_3a.plot_no as Plot_ID, 																			
										treatment_number, 																			
										plot_block_number as Row,																			
										plot_col_number as Range,																			
										repetition_number as Rep_Num, 																			
										standard_evaluation_name as SE_Name, 																			
										se_description,																			
										case when length(m150_3a.assessment_number) = 1 then '00' || cast(m150_3a.assessment_number as nvarchar) 																			
											 when length(m150_3a.assessment_number) = 2 then '0' || cast(m150_3a.assessment_number as nvarchar) 																			
											 else cast(m150_3a.assessment_number as nvarchar) end || ' ' ||																			
											 case when crop_name is not null then crop_name  || ' ' else '' end ||																			
											 case when crop_variety is not null then crop_variety  ||' '  else '' end ||																			
											 case when pest_name is not null then pest_name  || ' '  else '' end ||																			
											 case when rating_data is not null then rating_data  || ' '  else '' end ||																			
											 case when part_rated1 is not null then part_rated1  || ' '  else '' end ||																			
											case when reporting_basis_unit is not null then rating_unit || ' per ' || cast(reporting_basis as INT) || ' ' || reporting_basis_unit else case when rating_unit is not null then rating_unit  || ' '  else '' end end ||																			
											 case when AppDates.FirstAppl is not null then cast(Assmt_Date - AppDates.FirstAppl as nvarchar) end as Assmt_Label,																			
										subsamples, 																			
										null as Check_Mean_Value,																			
										null as Trt_Mean_Value,																			
										null as Check_PCT																			
							from public.mio150_trial_fact_cp m150_3a																			
							  inner join (select m150_3b.trial_id, max(rating_date) as LastAssmt 																			
										  from public.mio150_trial_fact_cp m150_3b																			
										  where m150_3b.trial_id = 'USNB0H1232021'																			
											 group by m150_3b.trial_id) MaxAssmt on m150_3a.trial_id = MaxAssmt.trial_id																			
							  left outer join (select distinct m150_3c.trial_id, crop_number, plant_date, emerge_date, crop_description 																			
												from public.mio150_trial_fact_cp m150_3c																			
												where m150_3c.trial_id = 'USNB0H1232021'																			
												and record_type = 'application_product') CropDetails on m150_3a.trial_id = CropDetails.trial_id and m150_3a.crop_id_code = CropDetails.crop_number																			
							  left outer join (select distinct m150_3d.trial_id, pest_number, pest_description 																			
												from public.mio150_trial_fact_cp m150_3d																			
												where m150_3d.trial_id = 'USNB0H1232021'																			
												and record_type = 'application_product') PestDetails on m150_3a.trial_id = PestDetails.trial_id and m150_3a.crop_id_code = PestDetails.pest_number																			
							  left outer join (select distinct m150_3e.trial_id, min(application_date) FirstAppl																			
												from public.mio150_trial_fact_cp m150_3e																			
												where m150_3e.trial_id = 'USNB0H1232021'																			
												and record_type = 'application_product'																			
												group by m150_3e.trial_id) AppDates on m150_3a.trial_id = AppDates.trial_id																			
						   where m150_3a.trial_id = 'USNB0H1232021'																			
						   and operation_type = 'assessment' 																			
						   ) AssessHeader																			
				left outer join (select m150_3b.trial_id, assessment_number, plot_no, observation_value_num as Rep_Value, record_type 																			
									from public.mio150_trial_fact_cp m150_3b																			
									where record_type <> 'assessment-subsample') RepVal on AssessHeader.trial_id = RepVal.trial_id and AssessHeader.Assmt_Num = RepVal.assessment_number and AssessHeader.Plot_ID = RepVal.plot_no																			
				left outer join (select m150_3c.trial_id, assessment_number, plot_no, avg(observation_value_num) as Rep_Value, listagg(observation_value_num, ',') within group (order by subsample_no) as sub_Values,																			
									  listagg(subsample_no, ',') within group (order by subsample_no) as sub_Nums 																			
									from public.mio150_trial_fact_cp m150_3c																			
									  where record_type = 'assessment-subsample'																			
									  group by m150_3c.trial_id, assessment_number, plot_no																			
								   ) SubVal on AssessHeader.trial_id = SubVal.trial_id and AssessHeader.Assmt_Num = SubVal.assessment_number and AssessHeader.Plot_ID = SubVal.plot_no)																			
;																				
																		
