with TrialList as (select distinct trial_id from public.mio150_trial_fact_cp where trial_id in ('USNB0H1232021'))                   
select distinct m150_1.trial_id, m150_1.treatment_number,  															
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
from public.mio150_trial_fact_cp m150_1															
left outer join (select m150_1a.trial_id, treatment_number, left(treatment_name, strpos(treatment_name, ':' )-1) as MITF_Code, right(treatment_name, len(treatment_name)-strpos(treatment_name, ':' )-1) as MITF_Level															
from public.mio150_trial_fact_cp m150_1a															
where m150_1a.trial_id = 'USNB0H1232021'															
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
              from public.mio150_trial_fact_cp m150_1b															
               where m150_1b.trial_id = 'USNB0H1232021'															
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
                  from public.mio150_trial_fact_cp m150_1c															
                  where m150_1c.trial_id = 'USNB0H1232021'															
                  and record_type = 'application_product' and strpos(trt_application_code,appl_application_code) > 0)															
group by trial_id, treatment_number, trt_application_code) ApplInfo on TagPrd.trial_id = ApplInfo.trial_id and TagPrd.treatment_number = ApplInfo.treatment_number and TagPrd.trt_application_code = ApplInfo.trt_application_code			
                               group by TagPrd.trial_id, TagPrd.treatment_number) as Prdlist on m150_1.trial_id = Prdlist.trial_id and m150_1.treatment_number = Prdlist.treatment_number															
left outer join (select trial_id, treatment_number, listagg(factorial_factor || factorial_level, ', ') within group (order by factorial_factor, factorial_level) as FactorsAndLevels															
from (select distinct m150_1d.trial_id, treatment_number, factorial_factor, factorial_level															
  from public.mio150_trial_fact_cp m150_1d															
  where m150_1d.trial_id = 'USNB0H1232021'															
  and record_type = 'application_product') 															
group by  trial_id, treatment_number) as Factorlist on m150_1.trial_id = Factorlist.trial_id and m150_1.treatment_number = Factorlist.treatment_number															
where m150_1.trial_id = 'USNB0H1232021'															
;															
