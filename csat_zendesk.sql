(select 
	client_account,
	'daily' as period,
	local_date_created,
	channel,
	assignee_id::varchar as assignee_id,
	email_address,
	agent_name,
	supervisor,
	'csat_p' as kpi_metric,
	score::numeric as score,
	(select target from sd_performance_target where client_account = 'urbanstems' 
		and channel = 'overall' and kpi = 'csat' and (local_date_created between start_date and end_date))	
		as target,
	case
 		when score >= (select km.base from kpi_data_warehouse.sd_kpi_metrics km where km.rating = 5 and km.client_account = 'urbanstems' and 
	  			km.channel = 'overall' and km.kpi = 'csat' and km.metric is null)
		then 5
 		when score >= (select km.base from kpi_data_warehouse.sd_kpi_metrics km where km.rating = 4 and km.client_account = 'urbanstems' and 
	  			km.channel = 'overall' and km.kpi = 'csat' and km.metric is null) 
		then 4
 		when score >= (select km.base from kpi_data_warehouse.sd_kpi_metrics km where km.rating = 3 and km.client_account = 'urbanstems'  and 
	  			km.channel = 'overall' and km.kpi = 'csat' and km.metric is null) 
		then 3
 		when score >= (select km.base from kpi_data_warehouse.sd_kpi_metrics km where km.rating = 2 and km.client_account = 'urbanstems' and 
	  			km.channel = 'overall' and km.kpi = 'csat' and km.metric is null) 
		then 2
 		when score <= (select km.ceiling from kpi_data_warehouse.sd_kpi_metrics km where km.rating = 1 and km.client_account = 'urbanstems' and 
	  			km.channel = 'overall' and km.kpi = 'csat' and km.metric is null) 
	  	then 1
   end as rating,
   (select weight from kpi_data_warehouse.sd_kpi_weights 
	where client_account = 'urbanstems' and channel = 'overall' and kpi = 'csat' and metric is null 
	and (local_date_created between start_date and end_date)) as weight	
		from
			(select 
				distinct 
				subteam as client_account,
				'overall' as channel,
				local_date_created,
				assignee_id,
				agent_name,
				agent_email as email_address,
				supervisor,
				d_csat::float/d_total as score
					from 
						(select 
							ticket_id,
							subteam,
							channel,
							local_date_created,
							hour_,
							assignee_id,
							agent_name,
							agent_email,
							case when supervisor is null then 'Urbanstems Flex' else supervisor end as supervisor,
							csat,
							/*hour*/
							count(ticket_id) over (partition by subteam,local_date_created,agent_name,hour_) as h_total,
							count(ticket_id) over (partition by subteam,local_date_created,agent_name,csat,hour_) as h_csat,
							count(ticket_id) over (partition by subteam,local_date_created,hour_) as h_total_t,
							count(ticket_id) over (partition by subteam,local_date_created,csat,hour_) as h_csat_t,
							count(ticket_id) over (partition by subteam,local_date_created,supervisor,hour_) as h_total_v,
							count(ticket_id) over (partition by subteam,local_date_created,supervisor,csat,hour_) as h_csat_v,
							/*daily*/
							count(ticket_id) over (partition by subteam,local_date_created,agent_name) as d_total,
							count(ticket_id) over (partition by subteam,local_date_created,agent_name,csat) as d_csat,
							count(ticket_id) over (partition by subteam,local_date_created) as d_total_t,
							count(ticket_id) over (partition by subteam,local_date_created,csat) as d_csat_t,
							count(ticket_id) over (partition by subteam,local_date_created,supervisor) as d_total_v,
							count(ticket_id) over (partition by subteam,local_date_created,csat,supervisor) as d_csat_v,
							/*weekly*/
							count(ticket_id) over (partition by subteam,date(date_trunc('week',local_date_created)),agent_name) as w_total,
							count(ticket_id) over (partition by subteam,date(date_trunc('week',local_date_created)),agent_name,csat) as w_csat,
							count(ticket_id) over (partition by subteam,date(date_trunc('week',local_date_created))) as w_total_t,
							count(ticket_id) over (partition by subteam,date(date_trunc('week',local_date_created)),csat) as w_csat_t,
							count(ticket_id) over (partition by subteam,date(date_trunc('week',local_date_created)),supervisor) as w_total_v,
							count(ticket_id) over (partition by subteam,date(date_trunc('week',local_date_created)),csat,supervisor) as w_csat_v,
							/*month*/
							count(ticket_id) over (partition by subteam,date(date_trunc('month',local_date_created)),agent_name) as m_total,
							count(ticket_id) over (partition by subteam,date(date_trunc('month',local_date_created)),agent_name,csat) as m_csat,
							count(ticket_id) over (partition by subteam,date(date_trunc('month',local_date_created))) as m_total_t,
							count(ticket_id) over (partition by subteam,date(date_trunc('month',local_date_created)),csat) as m_csat_t,
							count(ticket_id) over (partition by subteam,date(date_trunc('month',local_date_created)),supervisor) as m_total_v,
							count(ticket_id) over (partition by subteam,date(date_trunc('month',local_date_created)),csat,supervisor) as m_csat_v
								from 
									(select 
										ticket_id,
										subteam,
										channel,
										local_date_created,
										hour_,
										assignee_id,
										agent_name,
										agent_email,
										case when supervisor is null then job_supervisor else supervisor end as supervisor,
										csat 
											from
												(select
													distinct 
													ticket_id,
													channel,
													local_date_created,
													hour_,
													assignee_id,
													satisfaction_rating_score as csat
														from 
															(select 
																distinct
																ticket_id,
																channel,
																local_date_created,
																extract(hour from local_time_created::time) as hour_,
																assignee_id,
																satisfaction_rating_score 
															from 
																zendesk_email_tickets zet 
															where 
																client_account = 'urbanstems'
																and (satisfaction_rating_score = 'good' or satisfaction_rating_score = 'bad')
															union all
															select 
																distinct
																ticket_id,
																channel,
																local_date_created,
																extract(hour from local_time_created::time) as hour_,
																assignee_id,
																satisfaction_rating_score 
															from 
																zendesk_chat_tickets zct 
															where 
																client_account = 'urbanstems'
																and (satisfaction_rating_score = 'good' or satisfaction_rating_score = 'bad')
															union all
															select 
																distinct
																ticket_id,
																channel,
																local_date_created,
																extract(hour from local_time_created::time) as hour_,
																assignee_id,
																satisfaction_rating_score 
															from 
																zendesk_helpdesk_tickets zht 
															where 
																client_account = 'urbanstems'
																and (satisfaction_rating_score = 'good' or satisfaction_rating_score = 'bad')
															union all	
															select 
																distinct
																ticket_id,
																channel,
																local_date_created,
																extract(hour from local_time_created::time) as hour_,
																assignee_id,
																satisfaction_rating_score
															from 
																zendesk_call_tickets zct 
															where 
																client_account = 'urbanstems'
																and (satisfaction_rating_score = 'good' or satisfaction_rating_score = 'bad')) as all_csat
												) as distinct_id
											left join 
												(select 
													subteam, 
													zendesk_id as _id, 
													agent_name, 
													agent_email, 
													start_date, 
													end_date, 
													is_core_team 
												from sd_subteam_urbanstems
												where is_core_team = true
												) a on (assignee_id::varchar = _id::varchar and local_date_created between start_date and end_date)
											left join 
												t_dsat_urbanstems tdu on ticket_id::varchar = trunc(ext_interaction_id::numeric)::varchar
											left join 
												(select 
													distinct
													start_date as date_,
													supervisor,
													agent_name as agent_ 
												from 
													sd_utilization 
												where
													division_name = 'UrbanStems') su on (local_date_created = date_ and agent_ = agent_name)
											left join 
												(select 
													full_name,
													job_supervisor
														from
															(select 
																concat(first_name,' ',last_name) as full_name,
																email_address,
																job_department,
																job_division,
																profile_status,
																job_supervisor,
																status_effectivity_date,
																job_effectivity_date,
																created_on,
																max(status_effectivity_date) over (partition by concat(first_name,' ',last_name)) as max_status,
																max(job_effectivity_date) over (partition by concat(first_name,' ',last_name)) as job_status,
																max(created_on) over (partition by concat(first_name,' ',last_name)) as create_status
															from 
																sd_hris_team_roster shtr 
															where job_division = 'UrbanStems') a
													where 
														max_status = status_effectivity_date 
														and job_status = job_effectivity_date 
														and created_on = create_status
														and profile_status = 'Active'
												) bamboo_visor on (agent_name = full_name)
									where agent_email is not null and ext_interaction_id is null) visor) raw_
				where csat = 'good') final_
order by local_date_created desc, agent_name)

union all 

(select 
	client_account,
	'daily' as period,
	local_date_created,
	channel,
	assignee_id,
	email_address,
	agent_name,
	supervisor,
	'csat_p' as kpi_metric,
	score::numeric as score,
	(select target from sd_performance_target where client_account = 'urbanstems' 
		and channel = 'overall' and kpi = 'csat' and (local_date_created between start_date and end_date))	
		as target,
	case
 		when score >= (select km.base from kpi_data_warehouse.sd_kpi_metrics km where km.rating = 5 and km.client_account = 'urbanstems' and 
	  			km.channel = 'overall' and km.kpi = 'csat' and km.metric is null)
		then 5
 		when score >= (select km.base from kpi_data_warehouse.sd_kpi_metrics km where km.rating = 4 and km.client_account = 'urbanstems' and 
	  			km.channel = 'overall' and km.kpi = 'csat' and km.metric is null) 
		then 4
 		when score >= (select km.base from kpi_data_warehouse.sd_kpi_metrics km where km.rating = 3 and km.client_account = 'urbanstems'  and 
	  			km.channel = 'overall' and km.kpi = 'csat' and km.metric is null) 
		then 3
 		when score >= (select km.base from kpi_data_warehouse.sd_kpi_metrics km where km.rating = 2 and km.client_account = 'urbanstems' and 
	  			km.channel = 'overall' and km.kpi = 'csat' and km.metric is null) 
		then 2
 		when score <= (select km.ceiling from kpi_data_warehouse.sd_kpi_metrics km where km.rating = 1 and km.client_account = 'urbanstems' and 
	  			km.channel = 'overall' and km.kpi = 'csat' and km.metric is null) 
	  	then 1
   end as rating,
   (select weight from kpi_data_warehouse.sd_kpi_weights 
	where client_account = 'urbanstems' and channel = 'overall' and kpi = 'csat' and metric is null 
	and (local_date_created between start_date and end_date)) as weight	
		from
			(select 
				distinct 
				subteam as client_account,
				'overall' as channel,
				local_date_created,
				'' as assignee_id,
				'Team Total' as agent_name,
				'' as email_address,
				'Team Total' as supervisor,
				d_csat_t::float/d_total_t as score
					from 
						(select 
							ticket_id,
							subteam,
							channel,
							local_date_created,
							hour_,
							assignee_id,
							agent_name,
							agent_email,
							case when supervisor is null then 'Urbanstems Flex' else supervisor end as supervisor,
							csat,
							/*hour*/
							count(ticket_id) over (partition by subteam,local_date_created,agent_name,hour_) as h_total,
							count(ticket_id) over (partition by subteam,local_date_created,agent_name,csat,hour_) as h_csat,
							count(ticket_id) over (partition by subteam,local_date_created,hour_) as h_total_t,
							count(ticket_id) over (partition by subteam,local_date_created,csat,hour_) as h_csat_t,
							count(ticket_id) over (partition by subteam,local_date_created,supervisor,hour_) as h_total_v,
							count(ticket_id) over (partition by subteam,local_date_created,supervisor,csat,hour_) as h_csat_v,
							/*daily*/
							count(ticket_id) over (partition by subteam,local_date_created,agent_name) as d_total,
							count(ticket_id) over (partition by subteam,local_date_created,agent_name,csat) as d_csat,
							count(ticket_id) over (partition by subteam,local_date_created) as d_total_t,
							count(ticket_id) over (partition by subteam,local_date_created,csat) as d_csat_t,
							count(ticket_id) over (partition by subteam,local_date_created,supervisor) as d_total_v,
							count(ticket_id) over (partition by subteam,local_date_created,csat,supervisor) as d_csat_v,
							/*weekly*/
							count(ticket_id) over (partition by subteam,date(date_trunc('week',local_date_created)),agent_name) as w_total,
							count(ticket_id) over (partition by subteam,date(date_trunc('week',local_date_created)),agent_name,csat) as w_csat,
							count(ticket_id) over (partition by subteam,date(date_trunc('week',local_date_created))) as w_total_t,
							count(ticket_id) over (partition by subteam,date(date_trunc('week',local_date_created)),csat) as w_csat_t,
							count(ticket_id) over (partition by subteam,date(date_trunc('week',local_date_created)),supervisor) as w_total_v,
							count(ticket_id) over (partition by subteam,date(date_trunc('week',local_date_created)),csat,supervisor) as w_csat_v,
							/*month*/
							count(ticket_id) over (partition by subteam,date(date_trunc('month',local_date_created)),agent_name) as m_total,
							count(ticket_id) over (partition by subteam,date(date_trunc('month',local_date_created)),agent_name,csat) as m_csat,
							count(ticket_id) over (partition by subteam,date(date_trunc('month',local_date_created))) as m_total_t,
							count(ticket_id) over (partition by subteam,date(date_trunc('month',local_date_created)),csat) as m_csat_t,
							count(ticket_id) over (partition by subteam,date(date_trunc('month',local_date_created)),supervisor) as m_total_v,
							count(ticket_id) over (partition by subteam,date(date_trunc('month',local_date_created)),csat,supervisor) as m_csat_v
								from 
									(select 
										ticket_id,
										subteam,
										channel,
										local_date_created,
										hour_,
										assignee_id,
										agent_name,
										agent_email,
										case when supervisor is null then job_supervisor else supervisor end as supervisor,
										csat 
											from
												(select
													distinct 
													ticket_id,
													channel,
													local_date_created,
													hour_,
													assignee_id,
													satisfaction_rating_score as csat
														from 
															(select 
																distinct
																ticket_id,
																channel,
																local_date_created,
																extract(hour from local_time_created::time) as hour_,
																assignee_id,
																satisfaction_rating_score 
															from 
																zendesk_email_tickets zet 
															where 
																client_account = 'urbanstems'
																and (satisfaction_rating_score = 'good' or satisfaction_rating_score = 'bad')
															union all
															select 
																distinct
																ticket_id,
																channel,
																local_date_created,
																extract(hour from local_time_created::time) as hour_,
																assignee_id,
																satisfaction_rating_score 
															from 
																zendesk_chat_tickets zct 
															where 
																client_account = 'urbanstems'
																and (satisfaction_rating_score = 'good' or satisfaction_rating_score = 'bad')
															union all
															select 
																distinct
																ticket_id,
																channel,
																local_date_created,
																extract(hour from local_time_created::time) as hour_,
																assignee_id,
																satisfaction_rating_score 
															from 
																zendesk_helpdesk_tickets zht 
															where 
																client_account = 'urbanstems'
																and (satisfaction_rating_score = 'good' or satisfaction_rating_score = 'bad')
															union all	
															select 
																distinct
																ticket_id,
																channel,
																local_date_created,
																extract(hour from local_time_created::time) as hour_,
																assignee_id,
																satisfaction_rating_score
															from 
																zendesk_call_tickets zct 
															where 
																client_account = 'urbanstems'
																and (satisfaction_rating_score = 'good' or satisfaction_rating_score = 'bad')) as all_csat
												) as distinct_id
											left join 
												(select 
													subteam, 
													zendesk_id as _id, 
													agent_name, 
													agent_email, 
													start_date, 
													end_date, 
													is_core_team 
												from sd_subteam_urbanstems
												where is_core_team = true
												) a on (assignee_id::varchar = _id::varchar and local_date_created between start_date and end_date)
											left join 
												t_dsat_urbanstems tdu on ticket_id::varchar = trunc(ext_interaction_id::numeric)::varchar
											left join 
												(select 
													distinct
													start_date as date_,
													supervisor,
													agent_name as agent_ 
												from 
													sd_utilization 
												where
													division_name = 'UrbanStems') su on (local_date_created = date_ and agent_ = agent_name)
											left join 
												(select 
													full_name,
													job_supervisor
														from
															(select 
																concat(first_name,' ',last_name) as full_name,
																email_address,
																job_department,
																job_division,
																profile_status,
																job_supervisor,
																status_effectivity_date,
																job_effectivity_date,
																created_on,
																max(status_effectivity_date) over (partition by concat(first_name,' ',last_name)) as max_status,
																max(job_effectivity_date) over (partition by concat(first_name,' ',last_name)) as job_status,
																max(created_on) over (partition by concat(first_name,' ',last_name)) as create_status
															from 
																sd_hris_team_roster shtr 
															where job_division = 'UrbanStems') a
													where 
														max_status = status_effectivity_date 
														and job_status = job_effectivity_date 
														and created_on = create_status
														and profile_status = 'Active'
												) bamboo_visor on (agent_name = full_name)
									where agent_email is not null and ext_interaction_id is null) visor) raw_
				where csat = 'good') final_
order by local_date_created desc, agent_name)

union all 

(select 
	client_account,
	'daily' as period,
	local_date_created,
	channel,
	assignee_id,
	email_address,
	agent_name,
	supervisor,
	'csat_p' as kpi_metric,
	score::numeric as score,
	(select target from sd_performance_target where client_account = 'urbanstems' 
		and channel = 'overall' and kpi = 'csat' and (local_date_created between start_date and end_date))	
		as target,
	case
 		when score >= (select km.base from kpi_data_warehouse.sd_kpi_metrics km where km.rating = 5 and km.client_account = 'urbanstems' and 
	  			km.channel = 'overall' and km.kpi = 'csat' and km.metric is null)
		then 5
 		when score >= (select km.base from kpi_data_warehouse.sd_kpi_metrics km where km.rating = 4 and km.client_account = 'urbanstems' and 
	  			km.channel = 'overall' and km.kpi = 'csat' and km.metric is null) 
		then 4
 		when score >= (select km.base from kpi_data_warehouse.sd_kpi_metrics km where km.rating = 3 and km.client_account = 'urbanstems'  and 
	  			km.channel = 'overall' and km.kpi = 'csat' and km.metric is null) 
		then 3
 		when score >= (select km.base from kpi_data_warehouse.sd_kpi_metrics km where km.rating = 2 and km.client_account = 'urbanstems' and 
	  			km.channel = 'overall' and km.kpi = 'csat' and km.metric is null) 
		then 2
 		when score <= (select km.ceiling from kpi_data_warehouse.sd_kpi_metrics km where km.rating = 1 and km.client_account = 'urbanstems' and 
	  			km.channel = 'overall' and km.kpi = 'csat' and km.metric is null) 
	  	then 1
   end as rating,
   (select weight from kpi_data_warehouse.sd_kpi_weights 
	where client_account = 'urbanstems' and channel = 'overall' and kpi = 'csat' and metric is null 
	and (local_date_created between start_date and end_date)) as weight	
		from
			(select 
				distinct 
				subteam as client_account,
				'overall' as channel,
				local_date_created,
				'' as assignee_id,
				'Team Total' as agent_name,
				'' as email_address,
				supervisor,
				d_csat_v::float/d_total_v as score
					from 
						(select 
							ticket_id,
							subteam,
							channel,
							local_date_created,
							hour_,
							assignee_id,
							agent_name,
							agent_email,
							case when supervisor is null then 'Urbanstems Flex' else supervisor end as supervisor,
							csat,
							/*hour*/
							count(ticket_id) over (partition by subteam,local_date_created,agent_name,hour_) as h_total,
							count(ticket_id) over (partition by subteam,local_date_created,agent_name,csat,hour_) as h_csat,
							count(ticket_id) over (partition by subteam,local_date_created,hour_) as h_total_t,
							count(ticket_id) over (partition by subteam,local_date_created,csat,hour_) as h_csat_t,
							count(ticket_id) over (partition by subteam,local_date_created,supervisor,hour_) as h_total_v,
							count(ticket_id) over (partition by subteam,local_date_created,supervisor,csat,hour_) as h_csat_v,
							/*daily*/
							count(ticket_id) over (partition by subteam,local_date_created,agent_name) as d_total,
							count(ticket_id) over (partition by subteam,local_date_created,agent_name,csat) as d_csat,
							count(ticket_id) over (partition by subteam,local_date_created) as d_total_t,
							count(ticket_id) over (partition by subteam,local_date_created,csat) as d_csat_t,
							count(ticket_id) over (partition by subteam,local_date_created,supervisor) as d_total_v,
							count(ticket_id) over (partition by subteam,local_date_created,csat,supervisor) as d_csat_v,
							/*weekly*/
							count(ticket_id) over (partition by subteam,date(date_trunc('week',local_date_created)),agent_name) as w_total,
							count(ticket_id) over (partition by subteam,date(date_trunc('week',local_date_created)),agent_name,csat) as w_csat,
							count(ticket_id) over (partition by subteam,date(date_trunc('week',local_date_created))) as w_total_t,
							count(ticket_id) over (partition by subteam,date(date_trunc('week',local_date_created)),csat) as w_csat_t,
							count(ticket_id) over (partition by subteam,date(date_trunc('week',local_date_created)),supervisor) as w_total_v,
							count(ticket_id) over (partition by subteam,date(date_trunc('week',local_date_created)),csat,supervisor) as w_csat_v,
							/*month*/
							count(ticket_id) over (partition by subteam,date(date_trunc('month',local_date_created)),agent_name) as m_total,
							count(ticket_id) over (partition by subteam,date(date_trunc('month',local_date_created)),agent_name,csat) as m_csat,
							count(ticket_id) over (partition by subteam,date(date_trunc('month',local_date_created))) as m_total_t,
							count(ticket_id) over (partition by subteam,date(date_trunc('month',local_date_created)),csat) as m_csat_t,
							count(ticket_id) over (partition by subteam,date(date_trunc('month',local_date_created)),supervisor) as m_total_v,
							count(ticket_id) over (partition by subteam,date(date_trunc('month',local_date_created)),csat,supervisor) as m_csat_v
								from 
									(select 
										ticket_id,
										subteam,
										channel,
										local_date_created,
										hour_,
										assignee_id,
										agent_name,
										agent_email,
										case when supervisor is null then job_supervisor else supervisor end as supervisor,
										csat 
											from
												(select
													distinct 
													ticket_id,
													channel,
													local_date_created,
													hour_,
													assignee_id,
													satisfaction_rating_score as csat
														from 
															(select 
																distinct
																ticket_id,
																channel,
																local_date_created,
																extract(hour from local_time_created::time) as hour_,
																assignee_id,
																satisfaction_rating_score 
															from 
																zendesk_email_tickets zet 
															where 
																client_account = 'urbanstems'
																and (satisfaction_rating_score = 'good' or satisfaction_rating_score = 'bad')
															union all
															select 
																distinct
																ticket_id,
																channel,
																local_date_created,
																extract(hour from local_time_created::time) as hour_,
																assignee_id,
																satisfaction_rating_score 
															from 
																zendesk_chat_tickets zct 
															where 
																client_account = 'urbanstems'
																and (satisfaction_rating_score = 'good' or satisfaction_rating_score = 'bad')
															union all
															select 
																distinct
																ticket_id,
																channel,
																local_date_created,
																extract(hour from local_time_created::time) as hour_,
																assignee_id,
																satisfaction_rating_score 
															from 
																zendesk_helpdesk_tickets zht 
															where 
																client_account = 'urbanstems'
																and (satisfaction_rating_score = 'good' or satisfaction_rating_score = 'bad')
															union all	
															select 
																distinct
																ticket_id,
																channel,
																local_date_created,
																extract(hour from local_time_created::time) as hour_,
																assignee_id,
																satisfaction_rating_score
															from 
																zendesk_call_tickets zct 
															where 
																client_account = 'urbanstems'
																and (satisfaction_rating_score = 'good' or satisfaction_rating_score = 'bad')) as all_csat
												) as distinct_id
											left join 
												(select 
													subteam, 
													zendesk_id as _id, 
													agent_name, 
													agent_email, 
													start_date, 
													end_date, 
													is_core_team 
												from sd_subteam_urbanstems
												where is_core_team = true
												) a on (assignee_id::varchar = _id::varchar and local_date_created between start_date and end_date)
											left join 
												t_dsat_urbanstems tdu on ticket_id::varchar = trunc(ext_interaction_id::numeric)::varchar
											left join 
												(select 
													distinct
													start_date as date_,
													supervisor,
													agent_name as agent_ 
												from 
													sd_utilization 
												where
													division_name = 'UrbanStems') su on (local_date_created = date_ and agent_ = agent_name)
											left join 
												(select 
													full_name,
													job_supervisor
														from
															(select 
																concat(first_name,' ',last_name) as full_name,
																email_address,
																job_department,
																job_division,
																profile_status,
																job_supervisor,
																status_effectivity_date,
																job_effectivity_date,
																created_on,
																max(status_effectivity_date) over (partition by concat(first_name,' ',last_name)) as max_status,
																max(job_effectivity_date) over (partition by concat(first_name,' ',last_name)) as job_status,
																max(created_on) over (partition by concat(first_name,' ',last_name)) as create_status
															from 
																sd_hris_team_roster shtr 
															where job_division = 'UrbanStems') a
													where 
														max_status = status_effectivity_date 
														and job_status = job_effectivity_date 
														and created_on = create_status
														and profile_status = 'Active'
												) bamboo_visor on (agent_name = full_name)
									where agent_email is not null and ext_interaction_id is null) visor) raw_
				where csat = 'good') final_
order by local_date_created desc, agent_name)

union all 

(select 
	client_account,
	'weekly' as period,
	local_date_created,
	channel,
	assignee_id::varchar as assignee_id,
	email_address,
	agent_name,
	supervisor,
	'csat_p' as kpi_metric,
	score::numeric as score,
	(select target from sd_performance_target where client_account = 'urbanstems' 
		and channel = 'overall' and kpi = 'csat' and (local_date_created between start_date and end_date))	
		as target,
	case
 		when score >= (select km.base from kpi_data_warehouse.sd_kpi_metrics km where km.rating = 5 and km.client_account = 'urbanstems' and 
	  			km.channel = 'overall' and km.kpi = 'csat' and km.metric is null)
		then 5
 		when score >= (select km.base from kpi_data_warehouse.sd_kpi_metrics km where km.rating = 4 and km.client_account = 'urbanstems' and 
	  			km.channel = 'overall' and km.kpi = 'csat' and km.metric is null) 
		then 4
 		when score >= (select km.base from kpi_data_warehouse.sd_kpi_metrics km where km.rating = 3 and km.client_account = 'urbanstems'  and 
	  			km.channel = 'overall' and km.kpi = 'csat' and km.metric is null) 
		then 3
 		when score >= (select km.base from kpi_data_warehouse.sd_kpi_metrics km where km.rating = 2 and km.client_account = 'urbanstems' and 
	  			km.channel = 'overall' and km.kpi = 'csat' and km.metric is null) 
		then 2
 		when score <= (select km.ceiling from kpi_data_warehouse.sd_kpi_metrics km where km.rating = 1 and km.client_account = 'urbanstems' and 
	  			km.channel = 'overall' and km.kpi = 'csat' and km.metric is null) 
	  	then 1
   end as rating,
   (select weight from kpi_data_warehouse.sd_kpi_weights 
	where client_account = 'urbanstems' and channel = 'overall' and kpi = 'csat' and metric is null 
	and (local_date_created between start_date and end_date)) as weight	
		from
			(select 
				distinct 
				subteam as client_account,
				'overall' as channel,
				date(date_trunc('week',local_date_created)) as local_date_created,
				assignee_id,
				agent_name,
				agent_email as email_address,
				supervisor,
				w_csat::float/w_total as score
					from 
						(select 
							ticket_id,
							subteam,
							channel,
							local_date_created,
							hour_,
							assignee_id,
							agent_name,
							agent_email,
							case when supervisor is null then 'Urbanstems Flex' else supervisor end as supervisor,
							csat,
							/*hour*/
							count(ticket_id) over (partition by subteam,local_date_created,agent_name,hour_) as h_total,
							count(ticket_id) over (partition by subteam,local_date_created,agent_name,csat,hour_) as h_csat,
							count(ticket_id) over (partition by subteam,local_date_created,hour_) as h_total_t,
							count(ticket_id) over (partition by subteam,local_date_created,csat,hour_) as h_csat_t,
							count(ticket_id) over (partition by subteam,local_date_created,supervisor,hour_) as h_total_v,
							count(ticket_id) over (partition by subteam,local_date_created,supervisor,csat,hour_) as h_csat_v,
							/*daily*/
							count(ticket_id) over (partition by subteam,local_date_created,agent_name) as d_total,
							count(ticket_id) over (partition by subteam,local_date_created,agent_name,csat) as d_csat,
							count(ticket_id) over (partition by subteam,local_date_created) as d_total_t,
							count(ticket_id) over (partition by subteam,local_date_created,csat) as d_csat_t,
							count(ticket_id) over (partition by subteam,local_date_created,supervisor) as d_total_v,
							count(ticket_id) over (partition by subteam,local_date_created,csat,supervisor) as d_csat_v,
							/*weekly*/
							count(ticket_id) over (partition by subteam,date(date_trunc('week',local_date_created)),agent_name) as w_total,
							count(ticket_id) over (partition by subteam,date(date_trunc('week',local_date_created)),agent_name,csat) as w_csat,
							count(ticket_id) over (partition by subteam,date(date_trunc('week',local_date_created))) as w_total_t,
							count(ticket_id) over (partition by subteam,date(date_trunc('week',local_date_created)),csat) as w_csat_t,
							count(ticket_id) over (partition by subteam,date(date_trunc('week',local_date_created)),supervisor) as w_total_v,
							count(ticket_id) over (partition by subteam,date(date_trunc('week',local_date_created)),csat,supervisor) as w_csat_v,
							/*month*/
							count(ticket_id) over (partition by subteam,date(date_trunc('month',local_date_created)),agent_name) as m_total,
							count(ticket_id) over (partition by subteam,date(date_trunc('month',local_date_created)),agent_name,csat) as m_csat,
							count(ticket_id) over (partition by subteam,date(date_trunc('month',local_date_created))) as m_total_t,
							count(ticket_id) over (partition by subteam,date(date_trunc('month',local_date_created)),csat) as m_csat_t,
							count(ticket_id) over (partition by subteam,date(date_trunc('month',local_date_created)),supervisor) as m_total_v,
							count(ticket_id) over (partition by subteam,date(date_trunc('month',local_date_created)),csat,supervisor) as m_csat_v
								from 
									(select 
										ticket_id,
										subteam,
										channel,
										local_date_created,
										hour_,
										assignee_id,
										agent_name,
										agent_email,
										case when supervisor is null then job_supervisor else supervisor end as supervisor,
										csat 
											from
												(select
													distinct 
													ticket_id,
													channel,
													local_date_created,
													hour_,
													assignee_id,
													satisfaction_rating_score as csat
														from 
															(select 
																distinct
																ticket_id,
																channel,
																local_date_created,
																extract(hour from local_time_created::time) as hour_,
																assignee_id,
																satisfaction_rating_score 
															from 
																zendesk_email_tickets zet 
															where 
																client_account = 'urbanstems'
																and (satisfaction_rating_score = 'good' or satisfaction_rating_score = 'bad')
															union all
															select 
																distinct
																ticket_id,
																channel,
																local_date_created,
																extract(hour from local_time_created::time) as hour_,
																assignee_id,
																satisfaction_rating_score 
															from 
																zendesk_chat_tickets zct 
															where 
																client_account = 'urbanstems'
																and (satisfaction_rating_score = 'good' or satisfaction_rating_score = 'bad')
															union all
															select 
																distinct
																ticket_id,
																channel,
																local_date_created,
																extract(hour from local_time_created::time) as hour_,
																assignee_id,
																satisfaction_rating_score 
															from 
																zendesk_helpdesk_tickets zht 
															where 
																client_account = 'urbanstems'
																and (satisfaction_rating_score = 'good' or satisfaction_rating_score = 'bad')
															union all	
															select 
																distinct
																ticket_id,
																channel,
																local_date_created,
																extract(hour from local_time_created::time) as hour_,
																assignee_id,
																satisfaction_rating_score
															from 
																zendesk_call_tickets zct 
															where 
																client_account = 'urbanstems'
																and (satisfaction_rating_score = 'good' or satisfaction_rating_score = 'bad')) as all_csat
												) as distinct_id
											left join 
												(select 
													subteam, 
													zendesk_id as _id, 
													agent_name, 
													agent_email, 
													start_date, 
													end_date, 
													is_core_team 
												from sd_subteam_urbanstems
												where is_core_team = true
												) a on (assignee_id::varchar = _id::varchar and local_date_created between start_date and end_date)
											left join 
												t_dsat_urbanstems tdu on ticket_id::varchar = trunc(ext_interaction_id::numeric)::varchar
											left join 
												(select 
													distinct
													start_date as date_,
													supervisor,
													agent_name as agent_ 
												from 
													sd_utilization 
												where
													division_name = 'UrbanStems') su on (local_date_created = date_ and agent_ = agent_name)
											left join 
												(select 
													full_name,
													job_supervisor
														from
															(select 
																concat(first_name,' ',last_name) as full_name,
																email_address,
																job_department,
																job_division,
																profile_status,
																job_supervisor,
																status_effectivity_date,
																job_effectivity_date,
																created_on,
																max(status_effectivity_date) over (partition by concat(first_name,' ',last_name)) as max_status,
																max(job_effectivity_date) over (partition by concat(first_name,' ',last_name)) as job_status,
																max(created_on) over (partition by concat(first_name,' ',last_name)) as create_status
															from 
																sd_hris_team_roster shtr 
															where job_division = 'UrbanStems') a
													where 
														max_status = status_effectivity_date 
														and job_status = job_effectivity_date 
														and created_on = create_status
														and profile_status = 'Active'
												) bamboo_visor on (agent_name = full_name)
									where agent_email is not null and ext_interaction_id is null) visor) raw_
				where csat = 'good') final_
order by local_date_created desc, agent_name)

union all 

(select 
	client_account,
	'weekly' as period,
	local_date_created,
	channel,
	assignee_id,
	email_address,
	agent_name,
	supervisor,
	'csat_p' as kpi_metric,
	score::numeric as score,
	(select target from sd_performance_target where client_account = 'urbanstems' 
		and channel = 'overall' and kpi = 'csat' and (local_date_created between start_date and end_date))	
		as target,
	case
 		when score >= (select km.base from kpi_data_warehouse.sd_kpi_metrics km where km.rating = 5 and km.client_account = 'urbanstems' and 
	  			km.channel = 'overall' and km.kpi = 'csat' and km.metric is null)
		then 5
 		when score >= (select km.base from kpi_data_warehouse.sd_kpi_metrics km where km.rating = 4 and km.client_account = 'urbanstems' and 
	  			km.channel = 'overall' and km.kpi = 'csat' and km.metric is null) 
		then 4
 		when score >= (select km.base from kpi_data_warehouse.sd_kpi_metrics km where km.rating = 3 and km.client_account = 'urbanstems'  and 
	  			km.channel = 'overall' and km.kpi = 'csat' and km.metric is null) 
		then 3
 		when score >= (select km.base from kpi_data_warehouse.sd_kpi_metrics km where km.rating = 2 and km.client_account = 'urbanstems' and 
	  			km.channel = 'overall' and km.kpi = 'csat' and km.metric is null) 
		then 2
 		when score <= (select km.ceiling from kpi_data_warehouse.sd_kpi_metrics km where km.rating = 1 and km.client_account = 'urbanstems' and 
	  			km.channel = 'overall' and km.kpi = 'csat' and km.metric is null) 
	  	then 1
   end as rating,
   (select weight from kpi_data_warehouse.sd_kpi_weights 
	where client_account = 'urbanstems' and channel = 'overall' and kpi = 'csat' and metric is null 
	and (local_date_created between start_date and end_date)) as weight	
		from
			(select 
				distinct 
				subteam as client_account,
				'overall' as channel,
				date(date_trunc('week',local_date_created)) as local_date_created,
				'' as assignee_id,
				'Team Total' as agent_name,
				'' as email_address,
				'Team Total' as supervisor,
				w_csat_t::float/w_total_t as score
					from 
						(select 
							ticket_id,
							subteam,
							channel,
							local_date_created,
							hour_,
							assignee_id,
							agent_name,
							agent_email,
							case when supervisor is null then 'Urbanstems Flex' else supervisor end as supervisor,
							csat,
							/*hour*/
							count(ticket_id) over (partition by subteam,local_date_created,agent_name,hour_) as h_total,
							count(ticket_id) over (partition by subteam,local_date_created,agent_name,csat,hour_) as h_csat,
							count(ticket_id) over (partition by subteam,local_date_created,hour_) as h_total_t,
							count(ticket_id) over (partition by subteam,local_date_created,csat,hour_) as h_csat_t,
							count(ticket_id) over (partition by subteam,local_date_created,supervisor,hour_) as h_total_v,
							count(ticket_id) over (partition by subteam,local_date_created,supervisor,csat,hour_) as h_csat_v,
							/*daily*/
							count(ticket_id) over (partition by subteam,local_date_created,agent_name) as d_total,
							count(ticket_id) over (partition by subteam,local_date_created,agent_name,csat) as d_csat,
							count(ticket_id) over (partition by subteam,local_date_created) as d_total_t,
							count(ticket_id) over (partition by subteam,local_date_created,csat) as d_csat_t,
							count(ticket_id) over (partition by subteam,local_date_created,supervisor) as d_total_v,
							count(ticket_id) over (partition by subteam,local_date_created,csat,supervisor) as d_csat_v,
							/*weekly*/
							count(ticket_id) over (partition by subteam,date(date_trunc('week',local_date_created)),agent_name) as w_total,
							count(ticket_id) over (partition by subteam,date(date_trunc('week',local_date_created)),agent_name,csat) as w_csat,
							count(ticket_id) over (partition by subteam,date(date_trunc('week',local_date_created))) as w_total_t,
							count(ticket_id) over (partition by subteam,date(date_trunc('week',local_date_created)),csat) as w_csat_t,
							count(ticket_id) over (partition by subteam,date(date_trunc('week',local_date_created)),supervisor) as w_total_v,
							count(ticket_id) over (partition by subteam,date(date_trunc('week',local_date_created)),csat,supervisor) as w_csat_v,
							/*month*/
							count(ticket_id) over (partition by subteam,date(date_trunc('month',local_date_created)),agent_name) as m_total,
							count(ticket_id) over (partition by subteam,date(date_trunc('month',local_date_created)),agent_name,csat) as m_csat,
							count(ticket_id) over (partition by subteam,date(date_trunc('month',local_date_created))) as m_total_t,
							count(ticket_id) over (partition by subteam,date(date_trunc('month',local_date_created)),csat) as m_csat_t,
							count(ticket_id) over (partition by subteam,date(date_trunc('month',local_date_created)),supervisor) as m_total_v,
							count(ticket_id) over (partition by subteam,date(date_trunc('month',local_date_created)),csat,supervisor) as m_csat_v
								from 
									(select 
										ticket_id,
										subteam,
										channel,
										local_date_created,
										hour_,
										assignee_id,
										agent_name,
										agent_email,
										case when supervisor is null then job_supervisor else supervisor end as supervisor,
										csat 
											from
												(select
													distinct 
													ticket_id,
													channel,
													local_date_created,
													hour_,
													assignee_id,
													satisfaction_rating_score as csat
														from 
															(select 
																distinct
																ticket_id,
																channel,
																local_date_created,
																extract(hour from local_time_created::time) as hour_,
																assignee_id,
																satisfaction_rating_score 
															from 
																zendesk_email_tickets zet 
															where 
																client_account = 'urbanstems'
																and (satisfaction_rating_score = 'good' or satisfaction_rating_score = 'bad')
															union all
															select 
																distinct
																ticket_id,
																channel,
																local_date_created,
																extract(hour from local_time_created::time) as hour_,
																assignee_id,
																satisfaction_rating_score 
															from 
																zendesk_chat_tickets zct 
															where 
																client_account = 'urbanstems'
																and (satisfaction_rating_score = 'good' or satisfaction_rating_score = 'bad')
															union all
															select 
																distinct
																ticket_id,
																channel,
																local_date_created,
																extract(hour from local_time_created::time) as hour_,
																assignee_id,
																satisfaction_rating_score 
															from 
																zendesk_helpdesk_tickets zht 
															where 
																client_account = 'urbanstems'
																and (satisfaction_rating_score = 'good' or satisfaction_rating_score = 'bad')
															union all	
															select 
																distinct
																ticket_id,
																channel,
																local_date_created,
																extract(hour from local_time_created::time) as hour_,
																assignee_id,
																satisfaction_rating_score
															from 
																zendesk_call_tickets zct 
															where 
																client_account = 'urbanstems'
																and (satisfaction_rating_score = 'good' or satisfaction_rating_score = 'bad')) as all_csat
												) as distinct_id
											left join 
												(select 
													subteam, 
													zendesk_id as _id, 
													agent_name, 
													agent_email, 
													start_date, 
													end_date, 
													is_core_team 
												from sd_subteam_urbanstems
												where is_core_team = true
												) a on (assignee_id::varchar = _id::varchar and local_date_created between start_date and end_date)
											left join 
												t_dsat_urbanstems tdu on ticket_id::varchar = trunc(ext_interaction_id::numeric)::varchar
											left join 
												(select 
													distinct
													start_date as date_,
													supervisor,
													agent_name as agent_ 
												from 
													sd_utilization 
												where
													division_name = 'UrbanStems') su on (local_date_created = date_ and agent_ = agent_name)
											left join 
												(select 
													full_name,
													job_supervisor
														from
															(select 
																concat(first_name,' ',last_name) as full_name,
																email_address,
																job_department,
																job_division,
																profile_status,
																job_supervisor,
																status_effectivity_date,
																job_effectivity_date,
																created_on,
																max(status_effectivity_date) over (partition by concat(first_name,' ',last_name)) as max_status,
																max(job_effectivity_date) over (partition by concat(first_name,' ',last_name)) as job_status,
																max(created_on) over (partition by concat(first_name,' ',last_name)) as create_status
															from 
																sd_hris_team_roster shtr 
															where job_division = 'UrbanStems') a
													where 
														max_status = status_effectivity_date 
														and job_status = job_effectivity_date 
														and created_on = create_status
														and profile_status = 'Active'
												) bamboo_visor on (agent_name = full_name)
									where agent_email is not null and ext_interaction_id is null) visor) raw_
				where csat = 'good') final_
order by local_date_created desc, agent_name)

union all 

(select 
	client_account,
	'weekly' as period,
	local_date_created,
	channel,
	assignee_id,
	email_address,
	agent_name,
	supervisor,
	'csat_p' as kpi_metric,
	score::numeric as score,
	(select target from sd_performance_target where client_account = 'urbanstems' 
		and channel = 'overall' and kpi = 'csat' and (local_date_created between start_date and end_date))	
		as target,
	case
 		when score >= (select km.base from kpi_data_warehouse.sd_kpi_metrics km where km.rating = 5 and km.client_account = 'urbanstems' and 
	  			km.channel = 'overall' and km.kpi = 'csat' and km.metric is null)
		then 5
 		when score >= (select km.base from kpi_data_warehouse.sd_kpi_metrics km where km.rating = 4 and km.client_account = 'urbanstems' and 
	  			km.channel = 'overall' and km.kpi = 'csat' and km.metric is null) 
		then 4
 		when score >= (select km.base from kpi_data_warehouse.sd_kpi_metrics km where km.rating = 3 and km.client_account = 'urbanstems'  and 
	  			km.channel = 'overall' and km.kpi = 'csat' and km.metric is null) 
		then 3
 		when score >= (select km.base from kpi_data_warehouse.sd_kpi_metrics km where km.rating = 2 and km.client_account = 'urbanstems' and 
	  			km.channel = 'overall' and km.kpi = 'csat' and km.metric is null) 
		then 2
 		when score <= (select km.ceiling from kpi_data_warehouse.sd_kpi_metrics km where km.rating = 1 and km.client_account = 'urbanstems' and 
	  			km.channel = 'overall' and km.kpi = 'csat' and km.metric is null) 
	  	then 1
   end as rating,
   (select weight from kpi_data_warehouse.sd_kpi_weights 
	where client_account = 'urbanstems' and channel = 'overall' and kpi = 'csat' and metric is null 
	and (local_date_created between start_date and end_date)) as weight	
		from
			(select 
				distinct 
				subteam as client_account,
				'overall' as channel,
				date(date_trunc('week',local_date_created)) as local_date_created,
				'' as assignee_id,
				'Team Total' as agent_name,
				'' as email_address,
				supervisor,
				w_csat_v::float/w_total_v as score
					from 
						(select 
							ticket_id,
							subteam,
							channel,
							local_date_created,
							hour_,
							assignee_id,
							agent_name,
							agent_email,
							case when supervisor is null then 'Urbanstems Flex' else supervisor end as supervisor,
							csat,
							/*hour*/
							count(ticket_id) over (partition by subteam,local_date_created,agent_name,hour_) as h_total,
							count(ticket_id) over (partition by subteam,local_date_created,agent_name,csat,hour_) as h_csat,
							count(ticket_id) over (partition by subteam,local_date_created,hour_) as h_total_t,
							count(ticket_id) over (partition by subteam,local_date_created,csat,hour_) as h_csat_t,
							count(ticket_id) over (partition by subteam,local_date_created,supervisor,hour_) as h_total_v,
							count(ticket_id) over (partition by subteam,local_date_created,supervisor,csat,hour_) as h_csat_v,
							/*daily*/
							count(ticket_id) over (partition by subteam,local_date_created,agent_name) as d_total,
							count(ticket_id) over (partition by subteam,local_date_created,agent_name,csat) as d_csat,
							count(ticket_id) over (partition by subteam,local_date_created) as d_total_t,
							count(ticket_id) over (partition by subteam,local_date_created,csat) as d_csat_t,
							count(ticket_id) over (partition by subteam,local_date_created,supervisor) as d_total_v,
							count(ticket_id) over (partition by subteam,local_date_created,csat,supervisor) as d_csat_v,
							/*weekly*/
							count(ticket_id) over (partition by subteam,date(date_trunc('week',local_date_created)),agent_name) as w_total,
							count(ticket_id) over (partition by subteam,date(date_trunc('week',local_date_created)),agent_name,csat) as w_csat,
							count(ticket_id) over (partition by subteam,date(date_trunc('week',local_date_created))) as w_total_t,
							count(ticket_id) over (partition by subteam,date(date_trunc('week',local_date_created)),csat) as w_csat_t,
							count(ticket_id) over (partition by subteam,date(date_trunc('week',local_date_created)),supervisor) as w_total_v,
							count(ticket_id) over (partition by subteam,date(date_trunc('week',local_date_created)),csat,supervisor) as w_csat_v,
							/*month*/
							count(ticket_id) over (partition by subteam,date(date_trunc('month',local_date_created)),agent_name) as m_total,
							count(ticket_id) over (partition by subteam,date(date_trunc('month',local_date_created)),agent_name,csat) as m_csat,
							count(ticket_id) over (partition by subteam,date(date_trunc('month',local_date_created))) as m_total_t,
							count(ticket_id) over (partition by subteam,date(date_trunc('month',local_date_created)),csat) as m_csat_t,
							count(ticket_id) over (partition by subteam,date(date_trunc('month',local_date_created)),supervisor) as m_total_v,
							count(ticket_id) over (partition by subteam,date(date_trunc('month',local_date_created)),csat,supervisor) as m_csat_v
								from 
									(select 
										ticket_id,
										subteam,
										channel,
										local_date_created,
										hour_,
										assignee_id,
										agent_name,
										agent_email,
										case when supervisor is null then job_supervisor else supervisor end as supervisor,
										csat 
											from
												(select
													distinct 
													ticket_id,
													channel,
													local_date_created,
													hour_,
													assignee_id,
													satisfaction_rating_score as csat
														from 
															(select 
																distinct
																ticket_id,
																channel,
																local_date_created,
																extract(hour from local_time_created::time) as hour_,
																assignee_id,
																satisfaction_rating_score 
															from 
																zendesk_email_tickets zet 
															where 
																client_account = 'urbanstems'
																and (satisfaction_rating_score = 'good' or satisfaction_rating_score = 'bad')
															union all
															select 
																distinct
																ticket_id,
																channel,
																local_date_created,
																extract(hour from local_time_created::time) as hour_,
																assignee_id,
																satisfaction_rating_score 
															from 
																zendesk_chat_tickets zct 
															where 
																client_account = 'urbanstems'
																and (satisfaction_rating_score = 'good' or satisfaction_rating_score = 'bad')
															union all
															select 
																distinct
																ticket_id,
																channel,
																local_date_created,
																extract(hour from local_time_created::time) as hour_,
																assignee_id,
																satisfaction_rating_score 
															from 
																zendesk_helpdesk_tickets zht 
															where 
																client_account = 'urbanstems'
																and (satisfaction_rating_score = 'good' or satisfaction_rating_score = 'bad')
															union all	
															select 
																distinct
																ticket_id,
																channel,
																local_date_created,
																extract(hour from local_time_created::time) as hour_,
																assignee_id,
																satisfaction_rating_score
															from 
																zendesk_call_tickets zct 
															where 
																client_account = 'urbanstems'
																and (satisfaction_rating_score = 'good' or satisfaction_rating_score = 'bad')) as all_csat
												) as distinct_id
											left join 
												(select 
													subteam, 
													zendesk_id as _id, 
													agent_name, 
													agent_email, 
													start_date, 
													end_date, 
													is_core_team 
												from sd_subteam_urbanstems
												where is_core_team = true
												) a on (assignee_id::varchar = _id::varchar and local_date_created between start_date and end_date)
											left join 
												t_dsat_urbanstems tdu on ticket_id::varchar = trunc(ext_interaction_id::numeric)::varchar
											left join 
												(select 
													distinct
													start_date as date_,
													supervisor,
													agent_name as agent_ 
												from 
													sd_utilization 
												where
													division_name = 'UrbanStems') su on (local_date_created = date_ and agent_ = agent_name)
											left join 
												(select 
													full_name,
													job_supervisor
														from
															(select 
																concat(first_name,' ',last_name) as full_name,
																email_address,
																job_department,
																job_division,
																profile_status,
																job_supervisor,
																status_effectivity_date,
																job_effectivity_date,
																created_on,
																max(status_effectivity_date) over (partition by concat(first_name,' ',last_name)) as max_status,
																max(job_effectivity_date) over (partition by concat(first_name,' ',last_name)) as job_status,
																max(created_on) over (partition by concat(first_name,' ',last_name)) as create_status
															from 
																sd_hris_team_roster shtr 
															where job_division = 'UrbanStems') a
													where 
														max_status = status_effectivity_date 
														and job_status = job_effectivity_date 
														and created_on = create_status
														and profile_status = 'Active'
												) bamboo_visor on (agent_name = full_name)
									where agent_email is not null and ext_interaction_id is null) visor) raw_
				where csat = 'good') final_
order by local_date_created desc, agent_name)

union all 

(select 
	client_account,
	'monthly' as period,
	local_date_created,
	channel,
	assignee_id::varchar as assignee_id,
	email_address,
	agent_name,
	supervisor,
	'csat_p' as kpi_metric,
	score::numeric as score,
	(select target from sd_performance_target where client_account = 'urbanstems' 
		and channel = 'overall' and kpi = 'csat' and (local_date_created between start_date and end_date))	
		as target,
	case
 		when score >= (select km.base from kpi_data_warehouse.sd_kpi_metrics km where km.rating = 5 and km.client_account = 'urbanstems' and 
	  			km.channel = 'overall' and km.kpi = 'csat' and km.metric is null)
		then 5
 		when score >= (select km.base from kpi_data_warehouse.sd_kpi_metrics km where km.rating = 4 and km.client_account = 'urbanstems' and 
	  			km.channel = 'overall' and km.kpi = 'csat' and km.metric is null) 
		then 4
 		when score >= (select km.base from kpi_data_warehouse.sd_kpi_metrics km where km.rating = 3 and km.client_account = 'urbanstems'  and 
	  			km.channel = 'overall' and km.kpi = 'csat' and km.metric is null) 
		then 3
 		when score >= (select km.base from kpi_data_warehouse.sd_kpi_metrics km where km.rating = 2 and km.client_account = 'urbanstems' and 
	  			km.channel = 'overall' and km.kpi = 'csat' and km.metric is null) 
		then 2
 		when score <= (select km.ceiling from kpi_data_warehouse.sd_kpi_metrics km where km.rating = 1 and km.client_account = 'urbanstems' and 
	  			km.channel = 'overall' and km.kpi = 'csat' and km.metric is null) 
	  	then 1
   end as rating,
   (select weight from kpi_data_warehouse.sd_kpi_weights 
	where client_account = 'urbanstems' and channel = 'overall' and kpi = 'csat' and metric is null 
	and (local_date_created between start_date and end_date)) as weight	
		from
			(select 
				distinct 
				subteam as client_account,
				'overall' as channel,
				date(date_trunc('month',local_date_created)) as local_date_created,
				assignee_id,
				agent_name,
				agent_email as email_address,
				supervisor,
				m_csat::float/m_total as score
					from 
						(select 
							ticket_id,
							subteam,
							channel,
							local_date_created,
							hour_,
							assignee_id,
							agent_name,
							agent_email,
							case when supervisor is null then 'Urbanstems Flex' else supervisor end as supervisor,
							csat,
							/*hour*/
							count(ticket_id) over (partition by subteam,local_date_created,agent_name,hour_) as h_total,
							count(ticket_id) over (partition by subteam,local_date_created,agent_name,csat,hour_) as h_csat,
							count(ticket_id) over (partition by subteam,local_date_created,hour_) as h_total_t,
							count(ticket_id) over (partition by subteam,local_date_created,csat,hour_) as h_csat_t,
							count(ticket_id) over (partition by subteam,local_date_created,supervisor,hour_) as h_total_v,
							count(ticket_id) over (partition by subteam,local_date_created,supervisor,csat,hour_) as h_csat_v,
							/*daily*/
							count(ticket_id) over (partition by subteam,local_date_created,agent_name) as d_total,
							count(ticket_id) over (partition by subteam,local_date_created,agent_name,csat) as d_csat,
							count(ticket_id) over (partition by subteam,local_date_created) as d_total_t,
							count(ticket_id) over (partition by subteam,local_date_created,csat) as d_csat_t,
							count(ticket_id) over (partition by subteam,local_date_created,supervisor) as d_total_v,
							count(ticket_id) over (partition by subteam,local_date_created,csat,supervisor) as d_csat_v,
							/*weekly*/
							count(ticket_id) over (partition by subteam,date(date_trunc('week',local_date_created)),agent_name) as w_total,
							count(ticket_id) over (partition by subteam,date(date_trunc('week',local_date_created)),agent_name,csat) as w_csat,
							count(ticket_id) over (partition by subteam,date(date_trunc('week',local_date_created))) as w_total_t,
							count(ticket_id) over (partition by subteam,date(date_trunc('week',local_date_created)),csat) as w_csat_t,
							count(ticket_id) over (partition by subteam,date(date_trunc('week',local_date_created)),supervisor) as w_total_v,
							count(ticket_id) over (partition by subteam,date(date_trunc('week',local_date_created)),csat,supervisor) as w_csat_v,
							/*month*/
							count(ticket_id) over (partition by subteam,date(date_trunc('month',local_date_created)),agent_name) as m_total,
							count(ticket_id) over (partition by subteam,date(date_trunc('month',local_date_created)),agent_name,csat) as m_csat,
							count(ticket_id) over (partition by subteam,date(date_trunc('month',local_date_created))) as m_total_t,
							count(ticket_id) over (partition by subteam,date(date_trunc('month',local_date_created)),csat) as m_csat_t,
							count(ticket_id) over (partition by subteam,date(date_trunc('month',local_date_created)),supervisor) as m_total_v,
							count(ticket_id) over (partition by subteam,date(date_trunc('month',local_date_created)),csat,supervisor) as m_csat_v
								from 
									(select 
										ticket_id,
										subteam,
										channel,
										local_date_created,
										hour_,
										assignee_id,
										agent_name,
										agent_email,
										case when supervisor is null then job_supervisor else supervisor end as supervisor,
										csat 
											from
												(select
													distinct 
													ticket_id,
													channel,
													local_date_created,
													hour_,
													assignee_id,
													satisfaction_rating_score as csat
														from 
															(select 
																distinct
																ticket_id,
																channel,
																local_date_created,
																extract(hour from local_time_created::time) as hour_,
																assignee_id,
																satisfaction_rating_score 
															from 
																zendesk_email_tickets zet 
															where 
																client_account = 'urbanstems'
																and (satisfaction_rating_score = 'good' or satisfaction_rating_score = 'bad')
															union all
															select 
																distinct
																ticket_id,
																channel,
																local_date_created,
																extract(hour from local_time_created::time) as hour_,
																assignee_id,
																satisfaction_rating_score 
															from 
																zendesk_chat_tickets zct 
															where 
																client_account = 'urbanstems'
																and (satisfaction_rating_score = 'good' or satisfaction_rating_score = 'bad')
															union all
															select 
																distinct
																ticket_id,
																channel,
																local_date_created,
																extract(hour from local_time_created::time) as hour_,
																assignee_id,
																satisfaction_rating_score 
															from 
																zendesk_helpdesk_tickets zht 
															where 
																client_account = 'urbanstems'
																and (satisfaction_rating_score = 'good' or satisfaction_rating_score = 'bad')
															union all	
															select 
																distinct
																ticket_id,
																channel,
																local_date_created,
																extract(hour from local_time_created::time) as hour_,
																assignee_id,
																satisfaction_rating_score
															from 
																zendesk_call_tickets zct 
															where 
																client_account = 'urbanstems'
																and (satisfaction_rating_score = 'good' or satisfaction_rating_score = 'bad')) as all_csat
												) as distinct_id
											left join 
												(select 
													subteam, 
													zendesk_id as _id, 
													agent_name, 
													agent_email, 
													start_date, 
													end_date, 
													is_core_team 
												from sd_subteam_urbanstems
												where is_core_team = true
												) a on (assignee_id::varchar = _id::varchar and local_date_created between start_date and end_date)
											left join 
												t_dsat_urbanstems tdu on ticket_id::varchar = trunc(ext_interaction_id::numeric)::varchar
											left join 
												(select 
													distinct
													start_date as date_,
													supervisor,
													agent_name as agent_ 
												from 
													sd_utilization 
												where
													division_name = 'UrbanStems') su on (local_date_created = date_ and agent_ = agent_name)
											left join 
												(select 
													full_name,
													job_supervisor
														from
															(select 
																concat(first_name,' ',last_name) as full_name,
																email_address,
																job_department,
																job_division,
																profile_status,
																job_supervisor,
																status_effectivity_date,
																job_effectivity_date,
																created_on,
																max(status_effectivity_date) over (partition by concat(first_name,' ',last_name)) as max_status,
																max(job_effectivity_date) over (partition by concat(first_name,' ',last_name)) as job_status,
																max(created_on) over (partition by concat(first_name,' ',last_name)) as create_status
															from 
																sd_hris_team_roster shtr 
															where job_division = 'UrbanStems') a
													where 
														max_status = status_effectivity_date 
														and job_status = job_effectivity_date 
														and created_on = create_status
														and profile_status = 'Active'
												) bamboo_visor on (agent_name = full_name)
									where agent_email is not null and ext_interaction_id is null) visor) raw_
				where csat = 'good') final_
order by local_date_created desc, agent_name)

union all 

(select 
	client_account,
	'monthly' as period,
	local_date_created,
	channel,
	assignee_id,
	email_address,
	agent_name,
	supervisor,
	'csat_p' as kpi_metric,
	score::numeric as score,
	(select target from sd_performance_target where client_account = 'urbanstems' 
		and channel = 'overall' and kpi = 'csat' and (local_date_created between start_date and end_date))	
		as target,
	case
 		when score >= (select km.base from kpi_data_warehouse.sd_kpi_metrics km where km.rating = 5 and km.client_account = 'urbanstems' and 
	  			km.channel = 'overall' and km.kpi = 'csat' and km.metric is null)
		then 5
 		when score >= (select km.base from kpi_data_warehouse.sd_kpi_metrics km where km.rating = 4 and km.client_account = 'urbanstems' and 
	  			km.channel = 'overall' and km.kpi = 'csat' and km.metric is null) 
		then 4
 		when score >= (select km.base from kpi_data_warehouse.sd_kpi_metrics km where km.rating = 3 and km.client_account = 'urbanstems'  and 
	  			km.channel = 'overall' and km.kpi = 'csat' and km.metric is null) 
		then 3
 		when score >= (select km.base from kpi_data_warehouse.sd_kpi_metrics km where km.rating = 2 and km.client_account = 'urbanstems' and 
	  			km.channel = 'overall' and km.kpi = 'csat' and km.metric is null) 
		then 2
 		when score <= (select km.ceiling from kpi_data_warehouse.sd_kpi_metrics km where km.rating = 1 and km.client_account = 'urbanstems' and 
	  			km.channel = 'overall' and km.kpi = 'csat' and km.metric is null) 
	  	then 1
   end as rating,
   (select weight from kpi_data_warehouse.sd_kpi_weights 
	where client_account = 'urbanstems' and channel = 'overall' and kpi = 'csat' and metric is null 
	and (local_date_created between start_date and end_date)) as weight	
		from
			(select 
				distinct 
				subteam as client_account,
				'overall' as channel,
				date(date_trunc('month',local_date_created)) as local_date_created,
				'' as assignee_id,
				'Team Total' as agent_name,
				'' as email_address,
				'Team Total' as supervisor,
				m_csat_t::float/m_total_t as score
					from 
						(select 
							ticket_id,
							subteam,
							channel,
							local_date_created,
							hour_,
							assignee_id,
							agent_name,
							agent_email,
							case when supervisor is null then 'Urbanstems Flex' else supervisor end as supervisor,
							csat,
							/*hour*/
							count(ticket_id) over (partition by subteam,local_date_created,agent_name,hour_) as h_total,
							count(ticket_id) over (partition by subteam,local_date_created,agent_name,csat,hour_) as h_csat,
							count(ticket_id) over (partition by subteam,local_date_created,hour_) as h_total_t,
							count(ticket_id) over (partition by subteam,local_date_created,csat,hour_) as h_csat_t,
							count(ticket_id) over (partition by subteam,local_date_created,supervisor,hour_) as h_total_v,
							count(ticket_id) over (partition by subteam,local_date_created,supervisor,csat,hour_) as h_csat_v,
							/*daily*/
							count(ticket_id) over (partition by subteam,local_date_created,agent_name) as d_total,
							count(ticket_id) over (partition by subteam,local_date_created,agent_name,csat) as d_csat,
							count(ticket_id) over (partition by subteam,local_date_created) as d_total_t,
							count(ticket_id) over (partition by subteam,local_date_created,csat) as d_csat_t,
							count(ticket_id) over (partition by subteam,local_date_created,supervisor) as d_total_v,
							count(ticket_id) over (partition by subteam,local_date_created,csat,supervisor) as d_csat_v,
							/*weekly*/
							count(ticket_id) over (partition by subteam,date(date_trunc('week',local_date_created)),agent_name) as w_total,
							count(ticket_id) over (partition by subteam,date(date_trunc('week',local_date_created)),agent_name,csat) as w_csat,
							count(ticket_id) over (partition by subteam,date(date_trunc('week',local_date_created))) as w_total_t,
							count(ticket_id) over (partition by subteam,date(date_trunc('week',local_date_created)),csat) as w_csat_t,
							count(ticket_id) over (partition by subteam,date(date_trunc('week',local_date_created)),supervisor) as w_total_v,
							count(ticket_id) over (partition by subteam,date(date_trunc('week',local_date_created)),csat,supervisor) as w_csat_v,
							/*month*/
							count(ticket_id) over (partition by subteam,date(date_trunc('month',local_date_created)),agent_name) as m_total,
							count(ticket_id) over (partition by subteam,date(date_trunc('month',local_date_created)),agent_name,csat) as m_csat,
							count(ticket_id) over (partition by subteam,date(date_trunc('month',local_date_created))) as m_total_t,
							count(ticket_id) over (partition by subteam,date(date_trunc('month',local_date_created)),csat) as m_csat_t,
							count(ticket_id) over (partition by subteam,date(date_trunc('month',local_date_created)),supervisor) as m_total_v,
							count(ticket_id) over (partition by subteam,date(date_trunc('month',local_date_created)),csat,supervisor) as m_csat_v
								from 
									(select 
										ticket_id,
										subteam,
										channel,
										local_date_created,
										hour_,
										assignee_id,
										agent_name,
										agent_email,
										case when supervisor is null then job_supervisor else supervisor end as supervisor,
										csat 
											from
												(select
													distinct 
													ticket_id,
													channel,
													local_date_created,
													hour_,
													assignee_id,
													satisfaction_rating_score as csat
														from 
															(select 
																distinct
																ticket_id,
																channel,
																local_date_created,
																extract(hour from local_time_created::time) as hour_,
																assignee_id,
																satisfaction_rating_score 
															from 
																zendesk_email_tickets zet 
															where 
																client_account = 'urbanstems'
																and (satisfaction_rating_score = 'good' or satisfaction_rating_score = 'bad')
															union all
															select 
																distinct
																ticket_id,
																channel,
																local_date_created,
																extract(hour from local_time_created::time) as hour_,
																assignee_id,
																satisfaction_rating_score 
															from 
																zendesk_chat_tickets zct 
															where 
																client_account = 'urbanstems'
																and (satisfaction_rating_score = 'good' or satisfaction_rating_score = 'bad')
															union all
															select 
																distinct
																ticket_id,
																channel,
																local_date_created,
																extract(hour from local_time_created::time) as hour_,
																assignee_id,
																satisfaction_rating_score 
															from 
																zendesk_helpdesk_tickets zht 
															where 
																client_account = 'urbanstems'
																and (satisfaction_rating_score = 'good' or satisfaction_rating_score = 'bad')
															union all	
															select 
																distinct
																ticket_id,
																channel,
																local_date_created,
																extract(hour from local_time_created::time) as hour_,
																assignee_id,
																satisfaction_rating_score
															from 
																zendesk_call_tickets zct 
															where 
																client_account = 'urbanstems'
																and (satisfaction_rating_score = 'good' or satisfaction_rating_score = 'bad')) as all_csat
												) as distinct_id
											left join 
												(select 
													subteam, 
													zendesk_id as _id, 
													agent_name, 
													agent_email, 
													start_date, 
													end_date, 
													is_core_team 
												from sd_subteam_urbanstems
												where is_core_team = true
												) a on (assignee_id::varchar = _id::varchar and local_date_created between start_date and end_date)
											left join 
												t_dsat_urbanstems tdu on ticket_id::varchar = trunc(ext_interaction_id::numeric)::varchar
											left join 
												(select 
													distinct
													start_date as date_,
													supervisor,
													agent_name as agent_ 
												from 
													sd_utilization 
												where
													division_name = 'UrbanStems') su on (local_date_created = date_ and agent_ = agent_name)
											left join 
												(select 
													full_name,
													job_supervisor
														from
															(select 
																concat(first_name,' ',last_name) as full_name,
																email_address,
																job_department,
																job_division,
																profile_status,
																job_supervisor,
																status_effectivity_date,
																job_effectivity_date,
																created_on,
																max(status_effectivity_date) over (partition by concat(first_name,' ',last_name)) as max_status,
																max(job_effectivity_date) over (partition by concat(first_name,' ',last_name)) as job_status,
																max(created_on) over (partition by concat(first_name,' ',last_name)) as create_status
															from 
																sd_hris_team_roster shtr 
															where job_division = 'UrbanStems') a
													where 
														max_status = status_effectivity_date 
														and job_status = job_effectivity_date 
														and created_on = create_status
														and profile_status = 'Active'
												) bamboo_visor on (agent_name = full_name)
									where agent_email is not null and ext_interaction_id is null) visor) raw_
				where csat = 'good') final_
order by local_date_created desc, agent_name)

union all 

(select 
	client_account,
	'monthly' as period,
	local_date_created,
	channel,
	assignee_id,
	email_address,
	agent_name,
	supervisor,
	'csat_p' as kpi_metric,
	score::numeric as score,
	(select target from sd_performance_target where client_account = 'urbanstems' 
		and channel = 'overall' and kpi = 'csat' and (local_date_created between start_date and end_date))	
		as target,
	case
 		when score >= (select km.base from kpi_data_warehouse.sd_kpi_metrics km where km.rating = 5 and km.client_account = 'urbanstems' and 
	  			km.channel = 'overall' and km.kpi = 'csat' and km.metric is null)
		then 5
 		when score >= (select km.base from kpi_data_warehouse.sd_kpi_metrics km where km.rating = 4 and km.client_account = 'urbanstems' and 
	  			km.channel = 'overall' and km.kpi = 'csat' and km.metric is null) 
		then 4
 		when score >= (select km.base from kpi_data_warehouse.sd_kpi_metrics km where km.rating = 3 and km.client_account = 'urbanstems'  and 
	  			km.channel = 'overall' and km.kpi = 'csat' and km.metric is null) 
		then 3
 		when score >= (select km.base from kpi_data_warehouse.sd_kpi_metrics km where km.rating = 2 and km.client_account = 'urbanstems' and 
	  			km.channel = 'overall' and km.kpi = 'csat' and km.metric is null) 
		then 2
 		when score <= (select km.ceiling from kpi_data_warehouse.sd_kpi_metrics km where km.rating = 1 and km.client_account = 'urbanstems' and 
	  			km.channel = 'overall' and km.kpi = 'csat' and km.metric is null) 
	  	then 1
   end as rating,
   (select weight from kpi_data_warehouse.sd_kpi_weights 
	where client_account = 'urbanstems' and channel = 'overall' and kpi = 'csat' and metric is null 
	and (local_date_created between start_date and end_date)) as weight	
		from
			(select 
				distinct 
				subteam as client_account,
				'overall' as channel,
				date(date_trunc('month',local_date_created)) as local_date_created,
				'' as assignee_id,
				'Team Total' as agent_name,
				'' as email_address,
				supervisor,
				m_csat_v::float/m_total_v as score
					from 
						(select 
							ticket_id,
							subteam,
							channel,
							local_date_created,
							hour_,
							assignee_id,
							agent_name,
							agent_email,
							case when supervisor is null then 'Urbanstems Flex' else supervisor end as supervisor,
							csat,
							/*hour*/
							count(ticket_id) over (partition by subteam,local_date_created,agent_name,hour_) as h_total,
							count(ticket_id) over (partition by subteam,local_date_created,agent_name,csat,hour_) as h_csat,
							count(ticket_id) over (partition by subteam,local_date_created,hour_) as h_total_t,
							count(ticket_id) over (partition by subteam,local_date_created,csat,hour_) as h_csat_t,
							count(ticket_id) over (partition by subteam,local_date_created,supervisor,hour_) as h_total_v,
							count(ticket_id) over (partition by subteam,local_date_created,supervisor,csat,hour_) as h_csat_v,
							/*daily*/
							count(ticket_id) over (partition by subteam,local_date_created,agent_name) as d_total,
							count(ticket_id) over (partition by subteam,local_date_created,agent_name,csat) as d_csat,
							count(ticket_id) over (partition by subteam,local_date_created) as d_total_t,
							count(ticket_id) over (partition by subteam,local_date_created,csat) as d_csat_t,
							count(ticket_id) over (partition by subteam,local_date_created,supervisor) as d_total_v,
							count(ticket_id) over (partition by subteam,local_date_created,csat,supervisor) as d_csat_v,
							/*weekly*/
							count(ticket_id) over (partition by subteam,date(date_trunc('week',local_date_created)),agent_name) as w_total,
							count(ticket_id) over (partition by subteam,date(date_trunc('week',local_date_created)),agent_name,csat) as w_csat,
							count(ticket_id) over (partition by subteam,date(date_trunc('week',local_date_created))) as w_total_t,
							count(ticket_id) over (partition by subteam,date(date_trunc('week',local_date_created)),csat) as w_csat_t,
							count(ticket_id) over (partition by subteam,date(date_trunc('week',local_date_created)),supervisor) as w_total_v,
							count(ticket_id) over (partition by subteam,date(date_trunc('week',local_date_created)),csat,supervisor) as w_csat_v,
							/*month*/
							count(ticket_id) over (partition by subteam,date(date_trunc('month',local_date_created)),agent_name) as m_total,
							count(ticket_id) over (partition by subteam,date(date_trunc('month',local_date_created)),agent_name,csat) as m_csat,
							count(ticket_id) over (partition by subteam,date(date_trunc('month',local_date_created))) as m_total_t,
							count(ticket_id) over (partition by subteam,date(date_trunc('month',local_date_created)),csat) as m_csat_t,
							count(ticket_id) over (partition by subteam,date(date_trunc('month',local_date_created)),supervisor) as m_total_v,
							count(ticket_id) over (partition by subteam,date(date_trunc('month',local_date_created)),csat,supervisor) as m_csat_v
								from 
									(select 
										ticket_id,
										subteam,
										channel,
										local_date_created,
										hour_,
										assignee_id,
										agent_name,
										agent_email,
										case when supervisor is null then job_supervisor else supervisor end as supervisor,
										csat 
											from
												(select
													distinct 
													ticket_id,
													channel,
													local_date_created,
													hour_,
													assignee_id,
													satisfaction_rating_score as csat
														from 
															(select 
																distinct
																ticket_id,
																channel,
																local_date_created,
																extract(hour from local_time_created::time) as hour_,
																assignee_id,
																satisfaction_rating_score 
															from 
																zendesk_email_tickets zet 
															where 
																client_account = 'urbanstems'
																and (satisfaction_rating_score = 'good' or satisfaction_rating_score = 'bad')
															union all
															select 
																distinct
																ticket_id,
																channel,
																local_date_created,
																extract(hour from local_time_created::time) as hour_,
																assignee_id,
																satisfaction_rating_score 
															from 
																zendesk_chat_tickets zct 
															where 
																client_account = 'urbanstems'
																and (satisfaction_rating_score = 'good' or satisfaction_rating_score = 'bad')
															union all
															select 
																distinct
																ticket_id,
																channel,
																local_date_created,
																extract(hour from local_time_created::time) as hour_,
																assignee_id,
																satisfaction_rating_score 
															from 
																zendesk_helpdesk_tickets zht 
															where 
																client_account = 'urbanstems'
																and (satisfaction_rating_score = 'good' or satisfaction_rating_score = 'bad')
															union all	
															select 
																distinct
																ticket_id,
																channel,
																local_date_created,
																extract(hour from local_time_created::time) as hour_,
																assignee_id,
																satisfaction_rating_score
															from 
																zendesk_call_tickets zct 
															where 
																client_account = 'urbanstems'
																and (satisfaction_rating_score = 'good' or satisfaction_rating_score = 'bad')) as all_csat
												) as distinct_id
											left join 
												(select 
													subteam, 
													zendesk_id as _id, 
													agent_name, 
													agent_email, 
													start_date, 
													end_date, 
													is_core_team 
												from sd_subteam_urbanstems
												where is_core_team = true
												) a on (assignee_id::varchar = _id::varchar and local_date_created between start_date and end_date)
											left join 
												t_dsat_urbanstems tdu on ticket_id::varchar = trunc(ext_interaction_id::numeric)::varchar
											left join 
												(select 
													distinct
													start_date as date_,
													supervisor,
													agent_name as agent_ 
												from 
													sd_utilization 
												where
													division_name = 'UrbanStems') su on (local_date_created = date_ and agent_ = agent_name)
											left join 
												(select 
													full_name,
													job_supervisor
														from
															(select 
																concat(first_name,' ',last_name) as full_name,
																email_address,
																job_department,
																job_division,
																profile_status,
																job_supervisor,
																status_effectivity_date,
																job_effectivity_date,
																created_on,
																max(status_effectivity_date) over (partition by concat(first_name,' ',last_name)) as max_status,
																max(job_effectivity_date) over (partition by concat(first_name,' ',last_name)) as job_status,
																max(created_on) over (partition by concat(first_name,' ',last_name)) as create_status
															from 
																sd_hris_team_roster shtr 
															where job_division = 'UrbanStems') a
													where 
														max_status = status_effectivity_date 
														and job_status = job_effectivity_date 
														and created_on = create_status
														and profile_status = 'Active'
												) bamboo_visor on (agent_name = full_name)
									where agent_email is not null and ext_interaction_id is null) visor) raw_
				where csat = 'good') final_
order by local_date_created desc, agent_name)
