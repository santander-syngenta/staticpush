select Prdlist.Trial,									
Prdlist.Trt_Num,									
case when strpos(',' || untreated_list  || ',',',' || Prdlist.Trt_Num || ',') > 0 then 'Primary Check'    -- Secondary check identified in Optimized Dataset - possibly from BioDesign                        									
	                     else case when Prdlist.Trt_Num = reference_treatment then 'Standard' 								
	                     else '' end end as Trt_Flag,								
MTFlist.MITF_Code,									
MTFlist.MITF_Level,									
Prdlist.Product_Names,									
PrdList.Product_Rate_unit,									
PrdList.App_Schedule,									
PrdList.Application_Description,									
PrdList.Method,									
PrdList.Placement,									
PrdList.Timing,									
Factorlist.FactorsAndLevels,									
case when len(Prdlist.Trt_Num) = 3 then cast(Prdlist.Trt_Num as nvarchar) 									
     when len(Prdlist.Trt_Num) = 2 then '0' || cast(Prdlist.Trt_Num as nvarchar) 									
     when len(Prdlist.Trt_Num) = 1 then '00' || cast(Prdlist.Trt_Num as nvarchar)  else cast(Prdlist.Trt_Num as nvarchar) end || ' ' ||									
case when MTFlist.MITF_Code is not null then 'MTF:{' || MTFlist.MITF_Code || ' ' || MTFlist.MITF_Level ||'} ' else '' end ||									
case when Prdlist.Product_Names is not null then  Prdlist.Product_Names || ' ' else '' end || 									
case when Prdlist.Product_Rate_unit is not null then '{' || Product_Rate_unit || '} ' else '' end ||									
case when Prdlist.App_Schedule is not null then '{' || Prdlist.App_Schedule || '} ' else '' end ||									
case when Prdlist.Application_Description is not null then 'MAF:{' || Prdlist.Application_Description || '} ' else ''  end ||									
case when PrdList.Method is not null then '{' || PrdList.Method || '} ' else '' end ||									
case when PrdList.Placement is not null then '{' || PrdList.Placement || '} ' else '' end ||									
case when PrdList.Timing is not null then '{' || PrdList.Timing|| '} ' else '' end as Trt_Label									
from (select distinct trial_id, untreated_list, reference_treatment from public.mio150_trial_fact_cp m150 where trial_id ='USNB0H1232021' and operation_type is not null) TrialList									
inner join (select Trial, Trt_Num, 									
	          listagg(PrdsInTAG, '; ') within group (order by trt_application_code) as Product_Names, 								
	          listagg(RatesInTAG, '; ') within group (order by trt_application_code) as Product_Rate_Unit,								
	          listagg(trt_application_code, '; ') within group (order by trt_application_code) as App_Schedule,								
	          listagg(MAF, '; ') within group (order by trt_application_code) as Application_Description,								
	          listagg(appMethod, '; ') within group (order by trt_application_code) as Method,								
	          listagg(appPlacement, '; ') within group (order by trt_application_code) as Placement,								
	          listagg(appTiming, '; ') within group (order by trt_application_code) as Timing                              								
	     from (select Trial, Trt_Num, trt_application_code, MAF, PrdsInTAG, RatesInTAG, appMethod, appPlacement, appTiming,                                  								
	             listagg(application_date, ', ') within group (order by appl_application_code) as appDate,								
	             listagg(Spray_Volume, ', ') within group (order by appl_application_code) as appVol								
	           from (select trial_id as Trial, treatment_number as Trt_Num, trt_application_code, application_desc as MAF,								
	                         listagg(treatment_name, ' + ') within group (order by treatment_name) as PrdsInTAG,								
	                         listagg(distinct RateAndUnit, ' + ') within group (order by treatment_name) as RatesInTAG 								
	                 from (select distinct m150_1b.trial_id, m150_1b.treatment_number, trt_application_code, treatment_name, 								
	                       case when application_rate < 10000000 then cast(cast(application_rate as decimal(12, 4)) as nvarchar) 								
	                         else '' end || ' ' || application_rate_unit as RateAndUnit,								
	                                       application_desc								
	                              from public.mio150_trial_fact_cp m150_1b								
                                   where trial_id ='USNB0H1232021'									
	                               and record_type = 'application_product'								
	                               and product_type <> 'MTF')  TagPrd   								
	                         group by TagPrd.trial_id, TagPrd.treatment_number, trt_application_code, application_desc) TAGflat								
	                   left outer join (select distinct m150_1a.trial_id, appl_application_code, application_date,  								
			   	               	case when spray_vol < 1000000 then cast(cast(spray_vol as decimal(7, 1)) as nvarchar) || ' ' || spray_vol_unit end as Spray_Volume, 				
			                    case when application_method is not null then application_method end as appMethod, 						
								case when application_placement is not null then application_placement end as appPlacement, 	
								case when application_timing is not null then application_timing end as appTiming	
							from public.mio150_trial_fact_cp m150_1a		
							where trial_id ='USNB0H1232021'		
							and record_type = 'application_product'	) appl on TAGflat.trial = appl.trial_id and strpos(TAGflat.trt_application_code,appl.appl_application_code) > 0	
	                 group by Trial, Trt_Num, trt_application_code, MAF, PrdsInTAG, RatesInTAG, appMethod, appPlacement, appTiming) TAGwithApp                                                                          								
	          group by Trial, Trt_Num ) as Prdlist on TrialList.trial_id = Prdlist.trial								
 left outer join (select m150_1c.trial_id, treatment_number, left(treatment_name, strpos(treatment_name, ':' )-1) as MITF_Code, right(treatment_name, len(treatment_name)-strpos(treatment_name, ':' )-1) as MITF_Level									
 	 from public.mio150_trial_fact_cp m150_1c								
 	   where trial_id ='USNB0H1232021'								
	   and record_type = 'application_product'								
	   and product_type = 'MTF') as MTFlist on PrdList.trial = MTFlist.trial_id and PrdList.Trt_Num = MTFlist.treatment_number								
  left outer join (select trial_id, treatment_number, 									
                   listagg(factorial_factor || factorial_level, ', ') within group (order by factorial_factor, factorial_level) as FactorsAndLevels									
          from (select distinct m150_1d.trial_id, treatment_number, factorial_factor, factorial_level									
                      from public.mio150_trial_fact_cp m150_1d									
                      where trial_id ='USNB0H1232021'									
                      and record_type = 'application_product') 									
    		group by trial_id, treatment_number) as Factorlist on PrdList.trial = Factorlist.trial_id and PrdList.Trt_Num = Factorlist.treatment_number							
order by 1,2									
;									