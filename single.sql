with TrialList as (select distinct trial_id from public.mio150_trial_fact_cp where trial_id ='USNB0H1232021')													
select case when Level_0.project_id is null then Level_0.protocol_id else Level_0.project_id end as Master_Prt,													
case when Level_0.protocol_id is null then Level_0.project_id else Level_0.protocol_id end as Derived_Prt,													
Level_0.trial_id as Trial,													
Level_0.trial_year as Trial_Year,													
Level_0.country_code as Country_Code,													
Level_0.state_prov_code as State_province_code,													
Level_0.state_prov as State_province_name,													
Level_0.city as City,													
Level_0.site_type as Site_Type,													
Level_0.investigator as Syngenta_FieldScientist,													
Level_0.trial_origin as Trial_Placement,													
Level_0.latitude as Latitude,													
Level_0.longitude as Longitude,													
Level_0.cooperator as Cooperator,													
Level_0.study_design_code as Exp_Design,													
/*Quality should come from BioHome*/													
null as Quality,													
Level_0.trial_status as Trial_Status,													
null as Assmt_Num,													
null as Last_Assessment_Flag,													
Level_2.plant_dates as Plant_Date,													
Level_3.Assmt_Date,													
Level_2.emerge_dates as Emergence_Date,													
null as First_Appl_Date_Trt,													
null as First_Appl_Date_Trial,													
null as Most_Recent_Appl_Date_Trt,													
null as Most_Recent_Appl_Date_Trial,													
case when Level_2.plant_dates not like '%,%' then Level_3.Assmt_Date - to_date(Level_2.plant_dates, 'YYYY-MM-DD') else null end as DAP,													
case when Level_2.emerge_dates not like '%,%' then Level_3.Assmt_Date - to_date(Level_2.plant_dates, 'YYYY-MM-DD') else null end as DAE,													
null as DAF_Appl_Trial,													
null as DAMR_Appl_Trial,													
null as DAF_Appl_Trt,													
null as DAMR_Appl_Trt,													
null as Crop_Code,													
Level_2.crop_common_names as Crop,													
Level_2.variety_names as Variety,													
null as Crop_Attributes,													
null as Crop_Stg_Min,													
null as Crop_Stg_Max,													
null as Pest_Code,													
null as Pest,													
null as Pest_Attributes,													
null as Pest_Stg_Min,													
null as Pest_Stg_Max,													
Level_3.Assmt_Type,													
null as Part_Rated,													
null as Part_Rated_Role,													
null as Unit_Reporting_Basis,													
/*Consolidated Treatment Number really does not fit here because it is based on the order that trials load into BITS*/													
null as ConTrt,													
Level_1.treatment_number as Trt_Num,													
Level_1.Product_Names,													
Level_1.Product_Rate_Unit,													
Level_1.App_Schedule,													
Level_1.Application_Dates,													
Level_1.Application_Description,													
Level_1.Method,													
Level_1.Placement,													
Level_1.Timing,													
Level_1.Spray_Volume,													
Level_1.Trt_Flag,													
Level_1.FactorsAndLevels,													
/*Add Factors and Levels when they are available*/													
null as FactorA,													
null as FactorA_Level_Desc,													
null as FactorB,													
null as FactorB_Level_Desc,													
null as FactorC,													
null as FactorC_Level_Desc,													
Level_1.MITF_Code,													
Level_1.MITF_Level,													
Level_2.plot_number as Plot_ID,													
Level_2.plot_block_number as Range,													
Level_2.plot_col_number as Row,													
Level_2.repetition_number as Rep_Num,													
case when Level_3.Plot_Type = 'CHK' then 'Paired Check' else Level_1.Trt_Label end as Trt_Label,    --- treatment_no, repetition_number, plot_block_number, plot_col_number,													
null as SE_Name,													
Level_3.Assmt_Label,													
Level_3.Rep_Value,													
null as Check_Mean_Value,													
null as Trt_Mean_Value,													
null as Check_PCT,													
Level_0.climate_zone_code as Climate_Zone_Code,													
Level_0.climate_zone_decode as Climate_Zone_Decode,													
Level_0.Trial_TrialDesignType,													
Level_0.Forced_Single_Factor,													
null as artificial_population, 													
/*Add when mio002 is more complete for CP Products*/													
null as ComponentIndividualAIRateAndUnit,													
null as ComponentRateAndUnit,													
Level_3.sub_Values,													
Level_3.sub_Nums,													
Level_3.sub_RowCol  as sub_RowCol													
from (select distinct project_id,protocol_id,m150_0.trial_id,trial_year,country_code,state_prov_code,state_prov,city,site_type,investigator,trial_origin,latitude,longitude,cooperator,study_design_code,													
			            null as Quality, -- potentially add later from BioHome data										
			            trial_status,climate_zone_code,climate_zone_decode,										
			            null as Trial_TrialDesignType, null as Forced_Single_Factor, -- potentially add later										
			            first_trial_application, last_trial_application, application_dates			            							
            from TrialList inner join public.mio150_trial_fact_cp m150_0 on TrialList.trial_id = m150_0.trial_id													
                left outer join (select distinct m150_0a.trial_id, min(application_date) as first_trial_application, max(application_date) as last_trial_application,													
					    listagg(distinct application_date, ', ') within group (order by appl_application_code) as application_dates								
					            from TrialList inner join public.mio150_trial_fact_cp m150_0a on TrialList.trial_id = m150_0a.trial_id 								
					group by 1) appl_dates on m150_0.trial_id = appl_dates.trial_id								
) as Level_0													
inner join (select distinct m150_1.trial_id, m150_1.treatment_number,  													
	                case when strpos(',' || untreated_list  || ',',',' || m150_1.treatment_number || ',') > 0 then 'Primary Check'    -- Secondary check identified in Optimized Dataset - possibly from BioDesign                        												
	                     else case when m150_1.treatment_number = reference_treatment then 'Standard' 												
	                     else '' end end as Trt_Flag, MTFlist.MITF_Code, MTFlist.MITF_Level, Prdlist.PrdFlat as Product_Names, Prdlist.RateFlat as Product_Rate_Unit, Prdlist.AppCodeFlat as App_Schedule, Prdlist.MAFFlat as Application_Description,												
	                     Prdlist.AppDatesFlat as Application_Dates, Prdlist.MethodFlat as Method, Prdlist.PlacementFlat as Placement, Prdlist.TimingFlat as Timing, Prdlist.SprVol as Spray_Volume,												
	                     Factorlist.FactorsAndLevels,												
	                case when len(m150_1.treatment_number) = 3 then cast(m150_1.treatment_number as nvarchar) 												
	                     when len(m150_1.treatment_number) = 2 then '0' || cast(m150_1.treatment_number as nvarchar) 												
	                     when len(m150_1.treatment_number) = 1 then '00' || cast(m150_1.treatment_number as nvarchar)  else cast(m150_1.treatment_number as nvarchar) end || ' ' ||												
	                case when MTFlist.MITF_Code is not null then 'MTF:{' || MTFlist.MITF_Code || '} ' else '' end ||												
	                case when Prdlist.PrdFlat is not null then  Prdlist.PrdFlat || ' ' else '' end || 												
	                case when Prdlist.RateFlat is not null then '{' || Prdlist.RateFlat|| '} ' else '' end ||												
	                case when Prdlist.AppCodeFlat is not null then 'MAF:{' || Prdlist.AppCodeFlat || '} ' else '' end ||												
	                case when Prdlist.MethodFlat is not null then 'MAF:{' || Prdlist.MethodFlat || '} ' else '' end ||												
	                case when Prdlist.PlacementFlat is not null then 'MAF:{' || Prdlist.PlacementFlat || '} ' else '' end ||												
	                case when Prdlist.TimingFlat is not null then 'MAF:{' || Prdlist.TimingFlat || '} ' else '' end ||                                                                                                                                                    												
	                case when Prdlist.MAFFlat is not null then '{' || Prdlist.MAFFlat || '} ' else '' end as Trt_Label												
			from TrialList inner join public.mio150_trial_fact_cp m150_1 on TrialList.trial_id = m150_1.trial_id 										
			     left outer join (select m150_1a.trial_id, treatment_number, left(treatment_name, strpos(treatment_name, ':' )-1) as MITF_Code, right(treatment_name, len(treatment_name)-strpos(treatment_name, ':' )-1) as MITF_Level										
	                      from TrialList inner join public.mio150_trial_fact_cp m150_1a on TrialList.trial_id = m150_1a.trial_id												
	                       and record_type = 'application_product'												
	                       and product_type = 'MTF') as MTFlist on m150_1.trial_id = MTFlist.trial_id and m150_1.treatment_number = MTFlist.treatment_number												
		     left outer join (select TagPrd.trial_id, TagPrd.treatment_number, listagg(TagPrd.trt_application_code, '; ') within group (order by TagPrd.trt_application_code) as AppCodeFlat,											
	                              listagg(PrdsInTAG, '; ') within group (order by TagPrd.trt_application_code) as PrdFlat, 												
	                              listagg(RatesInTAG, '; ') within group (order by TagPrd.trt_application_code) as RateFlat, 												
	                              listagg(AppDates, '; ') within group (order by TagPrd.trt_application_code) as AppDatesFlat, 												
	                              listagg(AppMethod, '; ') within group (order by TagPrd.trt_application_code) as MethodFlat,												
	                              listagg(AppPlacement, '; ') within group (order by TagPrd.trt_application_code) as PlacementFlat,												
	                              listagg(AppTiming, '; ') within group (order by TagPrd.trt_application_code) as TimingFlat,												
	                              listagg(SprVol, '; ') within group (order by TagPrd.trt_application_code) as SprVol,												
	                              listagg(MAFInTAG, '; ') within group (order by TagPrd.trt_application_code) as MAFFlat												
	                         from (select trial_id, treatment_number, trt_application_code, listagg(treatment_name, ' + ') within group (order by treatment_name) as PrdsInTAG,												
	                                              listagg(RateAndUnit, ' + ') within group (order by treatment_name) as RatesInTAG, listagg(application_desc, ' + ') within group (order by treatment_name) as MAFInTAG												
	                                 from (select distinct m150_1b.trial_id, m150_1b.treatment_number, trt_application_code, treatment_name, case when application_rate < 10000000 then cast(cast(application_rate as decimal(12, 4)) as nvarchar) 												
	                                         else '' end || ' ' || application_rate_unit as RateAndUnit,												
	                                                       application_desc												
	                                              from TrialList inner join public.mio150_trial_fact_cp m150_1b on TrialList.trial_id = m150_1b.trial_id												
	                                               and record_type = 'application_product'												
	                                               and product_type <> 'MTF')                                                                                                                                                              												
	                                       group by trial_id, treatment_number, trt_application_code) TagPrd												
	                                left outer join (select trial_id, treatment_number, trt_application_code, listagg(application_date, ', ') within group (order by appl_application_code) as AppDates,												
	                                                        listagg(distinct application_method, ' | ') within group (order by appl_application_code) as AppMethod,												
	                                                        listagg(distinct application_placement, ' | ') within group (order by appl_application_code) as AppPlacement,												
	                                                        listagg(distinct application_timing, ' | ') within group (order by appl_application_code) as AppTiming,												
	                                                        listagg(distinct spray_volume, ' | ') within group (order by appl_application_code) as SprVol												
	                                                        from (select distinct m150_1c.trial_id, treatment_number, trt_application_code, appl_application_code, application_date, application_method, application_placement, application_timing,												
	                                                        case when spray_vol < 1000000 then cast(cast(spray_vol as decimal(7, 1)) as nvarchar) || ' ' || spray_vol_unit end as spray_volume												
	                                                  from TrialList inner join public.mio150_trial_fact_cp m150_1c on TrialList.trial_id = m150_1c.trial_id												
	                                                  and record_type = 'application_product' and strpos(trt_application_code,appl_application_code) > 0)												
	 												  group by trial_id, treatment_number, trt_application_code) ApplInfo on TagPrd.trial_id = ApplInfo.trial_id and TagPrd.treatment_number = ApplInfo.treatment_number and TagPrd.trt_application_code = ApplInfo.trt_application_code
	                                                               group by TagPrd.trial_id, TagPrd.treatment_number) as Prdlist on m150_1.trial_id = Prdlist.trial_id and m150_1.treatment_number = Prdlist.treatment_number												
	     left outer join (select trial_id, treatment_number, listagg(factorial_factor || factorial_level, ', ') within group (order by factorial_factor, factorial_level) as FactorsAndLevels												
	                          from (select distinct m150_1d.trial_id, treatment_number, factorial_factor, factorial_level												
	                                  from TrialList inner join public.mio150_trial_fact_cp m150_1d on TrialList.trial_id = m150_1d.trial_id												
	                                  and record_type = 'application_product') 												
	    group by  trial_id, treatment_number) as Factorlist on m150_1.trial_id = Factorlist.trial_id and m150_1.treatment_number = Factorlist.treatment_number												
) as Level_1 on Level_0.trial_id = Level_1.trial_id  													
inner join (select distinct m150_2.trial_id, m150_2.plot_number, treatment_no, repetition_number, plot_block_number, plot_col_number,													
					crop_numbers, crop_common_names, variety_names,	plant_dates, emerge_dates,							
					first_plot_application, last_plot_application, application_codes, application_dates, app_method_placement_timing								
			from TrialList inner join public.mio150_trial_fact_cp m150_2 on TrialList.trial_id = m150_2.trial_id 										
			    left outer join (select distinct m150_2a.trial_id, plot_number, 										
								listagg(distinct crop_number, ', ') within group (order by crop_number) as crop_numbers,					
								listagg(distinct crop_common_name, ', ') within group (order by crop_number) as crop_common_names,					
								listagg(distinct variety_name, ', ') within group (order by crop_number) as variety_names,					
								listagg(distinct plant_date, ', ') within group (order by crop_number) as plant_dates,					
								listagg(distinct emerge_date , ', ') within group (order by crop_number) as emerge_dates					
							from TrialList inner join public.mio150_trial_fact_cp m150_2a on TrialList.trial_id = m150_2a.trial_id 						
							and record_type = 'application_product'						
							group by 1,2) app_crops on m150_2.trial_id = app_crops.trial_id and m150_2.plot_number = app_crops.plot_number						
			    left outer join (select distinct m150_2b.trial_id, plot_number, 										
								min(application_date) as first_plot_application,					
								max(application_date) as last_plot_application,					
								listagg(distinct appl_application_code, ', ') within group (order by appl_application_code) as application_codes,					
								listagg(distinct application_date , ', ') within group (order by appl_application_code) as application_dates,					
								listagg(distinct case when application_method is not null then application_method || ' '  else '' end || 					
								case when application_placement is not null then application_placement || ' '  else '' end ||					
								case when application_timing is not null then application_timing || ' '  else '' end,  ', ') within group (order by appl_application_code) as app_method_placement_timing					
							from TrialList inner join public.mio150_trial_fact_cp m150_2b on TrialList.trial_id = m150_2b.trial_id 						
							and record_type = 'application_product'						
							group by 1,2) appl on m150_2.trial_id = appl.trial_id and m150_2.plot_number = appl.plot_number						
				where record_type = 'application_product'									
			) as Level_2 on Level_0.trial_id = Level_2.trial_id and Level_1.treatment_number = Level_2.treatment_no       										
left outer join (select													
					trial_id,								
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
					null as Plot_Type,								
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
					    from (select distinct m150_3.trial_id, 								
	                                    m150_3.assessment_number as Assmt_Num, 												
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
	                                    m150_3.plot_no as Plot_ID, 												
	                                    treatment_number, 												
	                                    plot_block_number as Row,												
	                                    plot_col_number as Range,												
	                                    repetition_number as Rep_Num, 												
	                                    standard_evaluation_name as SE_Name, 												
	                                    se_description,												
	                                    case when length(m150_3.assessment_number) = 1 then '00' || cast(m150_3.assessment_number as nvarchar) 												
	                                         when length(m150_3.assessment_number) = 2 then '0' || cast(m150_3.assessment_number as nvarchar) 												
	                                         else cast(m150_3.assessment_number as nvarchar) end || ' ' ||												
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
	                        from TrialList inner join public.mio150_trial_fact_cp m150_3 on TrialList.trial_id = m150_3.trial_id 												
	                          inner join (select m150_3a.trial_id, max(rating_date) as LastAssmt 												
	                                      from TrialList inner join public.mio150_trial_fact_cp m150_3a on TrialList.trial_id = m150_3a.trial_id 												
	                                         group by m150_3a.trial_id) MaxAssmt on m150_3.trial_id = MaxAssmt.trial_id												
	                          left outer join (select distinct m150_3b.trial_id, crop_number, plant_date, emerge_date, crop_description 												
	                                            from TrialList inner join public.mio150_trial_fact_cp m150_3b on TrialList.trial_id = m150_3b.trial_id 												
	                                            and record_type = 'application_product') CropDetails on m150_3.trial_id = CropDetails.trial_id and m150_3.crop_id_code = CropDetails.crop_number												
	                          left outer join (select distinct m150_3c.trial_id, pest_number, pest_description 												
	                                            from TrialList inner join public.mio150_trial_fact_cp m150_3c on TrialList.trial_id = m150_3c.trial_id 												
	                                            and record_type = 'application_product') PestDetails on m150_3.trial_id = PestDetails.trial_id and m150_3.crop_id_code = PestDetails.pest_number												
	                          left outer join (select distinct m150_3d.trial_id, min(application_date) FirstAppl												
	                                            from TrialList inner join public.mio150_trial_fact_cp m150_3d on TrialList.trial_id = m150_3d.trial_id 												
	                                            and record_type = 'application_product'												
	                                            group by m150_3d.trial_id) AppDates on m150_3.trial_id = AppDates.trial_id												
	                       where operation_type = 'assessment' 												
	                       ) AssessHeader												
		            left outer join (select m150_3e.trial_id, assessment_number, plot_no, observation_value_num as Rep_Value, record_type 											
		                                from TrialList inner join public.mio150_trial_fact_cp m150_3e on TrialList.trial_id = m150_3e.trial_id 											
		                                where record_type <> 'assessment-subsample') RepVal on AssessHeader.trial_id = RepVal.trial_id and AssessHeader.Assmt_Num = RepVal.assessment_number and AssessHeader.Plot_ID = RepVal.plot_no											
		            left outer join (select m150_3f.trial_id, assessment_number, plot_no, avg(observation_value_num) as Rep_Value, listagg(observation_value_num, ',') within group (order by subsample_no) as sub_Values,											
		                                  listagg(subsample_no, ',') within group (order by subsample_no) as sub_Nums 											
		                                from TrialList inner join public.mio150_trial_fact_cp m150_3f on TrialList.trial_id = m150_3f.trial_id 											
		                                  where record_type = 'assessment-subsample'											
		                                  group by m150_3f.trial_id, assessment_number, plot_no											
		                               ) SubVal on AssessHeader.trial_id = SubVal.trial_id and AssessHeader.Assmt_Num = SubVal.assessment_number and AssessHeader.Plot_ID = SubVal.plot_no)											
			union										
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
				) as Level_3 on Level_0.trial_id = Level_3.trial_id and Level_2.plot_number = Level_3.plot_id 									
order by Master_Prt, Derived_Prt, Trial, Level_3.Assmt_Date, Level_3.Assmt_label, Level_1.treatment_number, Level_2.repetition_number													
;													
