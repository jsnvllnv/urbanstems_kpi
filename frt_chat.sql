(select 
	distinct 
	subteam as client_account,
	'daily' as period,
	local_date_created,
	channel,
	assignee_id::varchar as assignee_id,
	email_address,
	agent_name,
	supervisor,
	'frt_d' as kpi_metric,
	daily as score,
	(select target from sd_performance_target 
		where client_account = 'urbanstems' 
		and channel = 'chat' 
		and kpi='response_time' 
		and (local_date_created between start_date and end_date)) as target,
	case 
			when daily::numeric <= (select ceiling from sd_kpi_metrics
							where client_account = 'urbanstems'
							and channel = 'chat' and kpi='response_time' and rating = '5') then 5
			when 	(daily::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'chat' and kpi='response_time' and rating = '4')
				and
					daily::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'chat' and kpi='response_time' and rating = '4'))
			then 4
			when 	(daily::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'chat' and kpi='response_time' and rating = '3')
				and
					daily::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'chat' and kpi='response_time' and rating = '3'))
			then 3	
			when 	(daily::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'chat' and kpi='response_time' and rating = '2')
				and
					daily::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'chat' and kpi='response_time' and rating = '2'))
			then 2	
			when daily::numeric >=	(select base from sd_kpi_metrics
							where client_account = 'urbanstems'
							and channel = 'chat' and kpi='response_time' and rating = '1') then 1		
		end as rating,
	(select weight from sd_kpi_weights
	where client_account = 'urbanstems'
	and channel = 'chat' and (local_date_created::date between start_date::date and end_date::date)) as weight
		from
			(select
				*,
				round(avg(response_time_first) over (partition by agent_name, local_date_created, hour_))::varchar as hourly,
				round(avg(response_time_first) over (partition by local_date_created, hour_, subteam))::varchar as t_hourly,
				round(avg(response_time_first) over (partition by supervisor, subteam, local_date_created, hour_))::varchar as v_hourly,
				round(avg(response_time_first) over (partition by agent_name, local_date_created))::varchar as daily,
				round(avg(response_time_first) over (partition by local_date_created,subteam))::varchar as t_daily,
				round(avg(response_time_first) over (partition by supervisor, local_date_created, subteam))::varchar as v_daily,
				round(avg(response_time_first) over (partition by agent_name, date(date_trunc('week',local_date_created))))::varchar as weekly,
				round(avg(response_time_first) over (partition by subteam, date(date_trunc('week',local_date_created))))::varchar as t_weekly,
				round(avg(response_time_first) over (partition by supervisor, subteam, date(date_trunc('week',local_date_created))))::varchar as v_weekly,
				round(avg(response_time_first) over (partition by agent_name, date(date_trunc('month',local_date_created))))::varchar as monthly,
				round(avg(response_time_first) over (partition by subteam, date(date_trunc('month',local_date_created))))::varchar as t_monthly,
				round(avg(response_time_first) over (partition by supervisor, subteam, date(date_trunc('month',local_date_created))))::varchar as v_monthly
					from
						(select
							subteam,
							ticket_id,
							local_date_created,
							hour_,
							channel,
							assignee_id,
							agent_name,
							email_address,
							case when supervisor is null then 'Urbanstems Flex' else supervisor end as supervisor,
							response_time_first
								from 
									(select 
										distinct
										subteam,
										ticket_id,
										local_date_created,
										hour_,
										channel,
										assignee_id,
										agent_name,
										agent_email as email_address,
										case when supervisor is null then job_supervisor else supervisor end as supervisor,
										response_time_first
											from 
												(select
													distinct
													subteam,
													max_date.ticket_id,
													local_date_created,
													hour_,
													channel,
													max_date.assignee_id,
													agent_name,
													agent_email,
													supervisor,
													job_supervisor,
													case when response_time_first='NaN' then null else response_time_first end as response_time_first
														from
															(select 
																distinct
																ticket_id,
																local_date_created,
																extract(hour from local_time_created::time) as hour_,
																channel,
																date_updated,
																assignee_id,
																max(date_updated) over (partition by ticket_id) as max_date
															from 
																zendesk_chat_tickets
															where 
																client_account = 'urbanstems') as max_date 
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
													join zendesk_chat_insights zci on zci.ticket_id = max_date.ticket_id
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
													where max_date = date_updated) as distinct_) extract_ where subteam is not null ) visor_ ) final_
	where daily is not null
	order by local_date_created::date desc, agent_name)
	
union all 

(select 
	distinct 
	subteam as client_account,
	'daily' as period,
	local_date_created,
	channel,
	'' as assignee_id,
	''email_address,
	'Team Total' as agent_name,
	'Team Total' as supervisor,
	'frt_d' as kpi_metric,
	t_daily as score,
	(select target from sd_performance_target 
		where client_account = 'urbanstems' 
		and channel = 'chat' 
		and kpi='response_time' 
		and (local_date_created between start_date and end_date)) as target,
	case 
			when t_daily::numeric <= (select ceiling from sd_kpi_metrics
							where client_account = 'urbanstems'
							and channel = 'chat' and kpi='response_time' and rating = '5') then 5
			when 	(t_daily::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'chat' and kpi='response_time' and rating = '4')
				and
					t_daily::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'chat' and kpi='response_time' and rating = '4'))
			then 4
			when 	(t_daily::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'chat' and kpi='response_time' and rating = '3')
				and
					t_daily::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'chat' and kpi='response_time' and rating = '3'))
			then 3	
			when 	(t_daily::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'chat' and kpi='response_time' and rating = '2')
				and
					t_daily::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'chat' and kpi='response_time' and rating = '2'))
			then 2	
			when t_daily::numeric >=	(select base from sd_kpi_metrics
							where client_account = 'urbanstems'
							and channel = 'chat' and kpi='response_time' and rating = '1') then 1		
		end as rating,
	(select weight from sd_kpi_weights
	where client_account = 'urbanstems'
	and channel = 'chat' and (local_date_created::date between start_date::date and end_date::date)) as weight
		from
			(select
				*,
				round(avg(response_time_first) over (partition by agent_name, local_date_created, hour_))::varchar as hourly,
				round(avg(response_time_first) over (partition by local_date_created, hour_, subteam))::varchar as t_hourly,
				round(avg(response_time_first) over (partition by supervisor, subteam, local_date_created, hour_))::varchar as v_hourly,
				round(avg(response_time_first) over (partition by agent_name, local_date_created))::varchar as daily,
				round(avg(response_time_first) over (partition by local_date_created,subteam))::varchar as t_daily,
				round(avg(response_time_first) over (partition by supervisor, local_date_created, subteam))::varchar as v_daily,
				round(avg(response_time_first) over (partition by agent_name, date(date_trunc('week',local_date_created))))::varchar as weekly,
				round(avg(response_time_first) over (partition by subteam, date(date_trunc('week',local_date_created))))::varchar as t_weekly,
				round(avg(response_time_first) over (partition by supervisor, subteam, date(date_trunc('week',local_date_created))))::varchar as v_weekly,
				round(avg(response_time_first) over (partition by agent_name, date(date_trunc('month',local_date_created))))::varchar as monthly,
				round(avg(response_time_first) over (partition by subteam, date(date_trunc('month',local_date_created))))::varchar as t_monthly,
				round(avg(response_time_first) over (partition by supervisor, subteam, date(date_trunc('month',local_date_created))))::varchar as v_monthly
					from
						(select
							subteam,
							ticket_id,
							local_date_created,
							hour_,
							channel,
							assignee_id,
							agent_name,
							email_address,
							case when supervisor is null then 'Urbanstems Flex' else supervisor end as supervisor,
							response_time_first
								from 
									(select 
										distinct
										subteam,
										ticket_id,
										local_date_created,
										hour_,
										channel,
										assignee_id,
										agent_name,
										agent_email as email_address,
										case when supervisor is null then job_supervisor else supervisor end as supervisor,
										response_time_first
											from 
												(select
													distinct
													subteam,
													max_date.ticket_id,
													local_date_created,
													hour_,
													channel,
													max_date.assignee_id,
													agent_name,
													agent_email,
													supervisor,
													job_supervisor,
													case when response_time_first='NaN' then null else response_time_first end as response_time_first
														from
															(select 
																distinct
																ticket_id,
																local_date_created,
																extract(hour from local_time_created::time) as hour_,
																channel,
																date_updated,
																assignee_id,
																max(date_updated) over (partition by ticket_id) as max_date
															from 
																zendesk_chat_tickets
															where 
																client_account = 'urbanstems') as max_date 
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
													join zendesk_chat_insights zci on zci.ticket_id = max_date.ticket_id
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
													where max_date = date_updated) as distinct_) extract_ where subteam is not null ) visor_ ) final_
	where t_daily is not null
	order by local_date_created::date desc, agent_name)
	
union all 

(select 
	distinct 
	subteam as client_account,
	'daily' as period,
	local_date_created,
	channel,
	'' as assignee_id,
	''email_address,
	'Team Total' as agent_name,
	supervisor,
	'frt_d' as kpi_metric,
	v_daily as score,
	(select target from sd_performance_target 
		where client_account = 'urbanstems' 
		and channel = 'chat' 
		and kpi='response_time' 
		and (local_date_created between start_date and end_date)) as target,
	case 
			when v_daily::numeric <= (select ceiling from sd_kpi_metrics
							where client_account = 'urbanstems'
							and channel = 'chat' and kpi='response_time' and rating = '5') then 5
			when 	(v_daily::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'chat' and kpi='response_time' and rating = '4')
				and
					v_daily::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'chat' and kpi='response_time' and rating = '4'))
			then 4
			when 	(v_daily::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'chat' and kpi='response_time' and rating = '3')
				and
					v_daily::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'chat' and kpi='response_time' and rating = '3'))
			then 3	
			when 	(v_daily::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'chat' and kpi='response_time' and rating = '2')
				and
					v_daily::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'chat' and kpi='response_time' and rating = '2'))
			then 2	
			when v_daily::numeric >=	(select base from sd_kpi_metrics
							where client_account = 'urbanstems'
							and channel = 'chat' and kpi='response_time' and rating = '1') then 1		
		end as rating,
	(select weight from sd_kpi_weights
	where client_account = 'urbanstems'
	and channel = 'chat' and (local_date_created::date between start_date::date and end_date::date)) as weight
		from
			(select
				*,
				round(avg(response_time_first) over (partition by agent_name, local_date_created, hour_))::varchar as hourly,
				round(avg(response_time_first) over (partition by local_date_created, hour_, subteam))::varchar as t_hourly,
				round(avg(response_time_first) over (partition by supervisor, subteam, local_date_created, hour_))::varchar as v_hourly,
				round(avg(response_time_first) over (partition by agent_name, local_date_created))::varchar as daily,
				round(avg(response_time_first) over (partition by local_date_created,subteam))::varchar as t_daily,
				round(avg(response_time_first) over (partition by supervisor, local_date_created, subteam))::varchar as v_daily,
				round(avg(response_time_first) over (partition by agent_name, date(date_trunc('week',local_date_created))))::varchar as weekly,
				round(avg(response_time_first) over (partition by subteam, date(date_trunc('week',local_date_created))))::varchar as t_weekly,
				round(avg(response_time_first) over (partition by supervisor, subteam, date(date_trunc('week',local_date_created))))::varchar as v_weekly,
				round(avg(response_time_first) over (partition by agent_name, date(date_trunc('month',local_date_created))))::varchar as monthly,
				round(avg(response_time_first) over (partition by subteam, date(date_trunc('month',local_date_created))))::varchar as t_monthly,
				round(avg(response_time_first) over (partition by supervisor, subteam, date(date_trunc('month',local_date_created))))::varchar as v_monthly
					from
						(select
							subteam,
							ticket_id,
							local_date_created,
							hour_,
							channel,
							assignee_id,
							agent_name,
							email_address,
							case when supervisor is null then 'Urbanstems Flex' else supervisor end as supervisor,
							response_time_first
								from 
									(select 
										distinct
										subteam,
										ticket_id,
										local_date_created,
										hour_,
										channel,
										assignee_id,
										agent_name,
										agent_email as email_address,
										case when supervisor is null then job_supervisor else supervisor end as supervisor,
										response_time_first
											from 
												(select
													distinct
													subteam,
													max_date.ticket_id,
													local_date_created,
													hour_,
													channel,
													max_date.assignee_id,
													agent_name,
													agent_email,
													supervisor,
													job_supervisor,
													case when response_time_first='NaN' then null else response_time_first end as response_time_first
														from
															(select 
																distinct
																ticket_id,
																local_date_created,
																extract(hour from local_time_created::time) as hour_,
																channel,
																date_updated,
																assignee_id,
																max(date_updated) over (partition by ticket_id) as max_date
															from 
																zendesk_chat_tickets
															where 
																client_account = 'urbanstems') as max_date 
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
													join zendesk_chat_insights zci on zci.ticket_id = max_date.ticket_id
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
													where max_date = date_updated) as distinct_) extract_ where subteam is not null ) visor_ ) final_
	where v_daily is not null
	order by local_date_created::date desc, agent_name)
	
union all 

(select 
	distinct 
	subteam as client_account,
	'weekly' as period,
	date(date_trunc('week',local_date_created)) as local_date_created,
	channel,
	assignee_id::varchar as assignee_id,
	email_address,
	agent_name,
	supervisor,
	'frt_d' as kpi_metric,
	weekly as score,
	(select target from sd_performance_target 
		where client_account = 'urbanstems' 
		and channel = 'chat' 
		and kpi='response_time' 
		and (local_date_created between start_date and end_date)) as target,
	case 
			when weekly::numeric <= (select ceiling from sd_kpi_metrics
							where client_account = 'urbanstems'
							and channel = 'chat' and kpi='response_time' and rating = '5') then 5
			when 	(weekly::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'chat' and kpi='response_time' and rating = '4')
				and
					weekly::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'chat' and kpi='response_time' and rating = '4'))
			then 4
			when 	(weekly::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'chat' and kpi='response_time' and rating = '3')
				and
					weekly::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'chat' and kpi='response_time' and rating = '3'))
			then 3	
			when 	(weekly::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'chat' and kpi='response_time' and rating = '2')
				and
					weekly::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'chat' and kpi='response_time' and rating = '2'))
			then 2	
			when weekly::numeric >=	(select base from sd_kpi_metrics
							where client_account = 'urbanstems'
							and channel = 'chat' and kpi='response_time' and rating = '1') then 1		
		end as rating,
	(select weight from sd_kpi_weights
	where client_account = 'urbanstems'
	and channel = 'chat' and (local_date_created::date between start_date::date and end_date::date)) as weight
		from
			(select
				*,
				round(avg(response_time_first) over (partition by agent_name, local_date_created, hour_))::varchar as hourly,
				round(avg(response_time_first) over (partition by local_date_created, hour_, subteam))::varchar as t_hourly,
				round(avg(response_time_first) over (partition by supervisor, subteam, local_date_created, hour_))::varchar as v_hourly,
				round(avg(response_time_first) over (partition by agent_name, local_date_created))::varchar as daily,
				round(avg(response_time_first) over (partition by local_date_created,subteam))::varchar as t_daily,
				round(avg(response_time_first) over (partition by supervisor, local_date_created, subteam))::varchar as v_daily,
				round(avg(response_time_first) over (partition by agent_name, date(date_trunc('week',local_date_created))))::varchar as weekly,
				round(avg(response_time_first) over (partition by subteam, date(date_trunc('week',local_date_created))))::varchar as t_weekly,
				round(avg(response_time_first) over (partition by supervisor, subteam, date(date_trunc('week',local_date_created))))::varchar as v_weekly,
				round(avg(response_time_first) over (partition by agent_name, date(date_trunc('month',local_date_created))))::varchar as monthly,
				round(avg(response_time_first) over (partition by subteam, date(date_trunc('month',local_date_created))))::varchar as t_monthly,
				round(avg(response_time_first) over (partition by supervisor, subteam, date(date_trunc('month',local_date_created))))::varchar as v_monthly
					from
						(select
							subteam,
							ticket_id,
							local_date_created,
							hour_,
							channel,
							assignee_id,
							agent_name,
							email_address,
							case when supervisor is null then 'Urbanstems Flex' else supervisor end as supervisor,
							response_time_first
								from 
									(select 
										distinct
										subteam,
										ticket_id,
										local_date_created,
										hour_,
										channel,
										assignee_id,
										agent_name,
										agent_email as email_address,
										case when supervisor is null then job_supervisor else supervisor end as supervisor,
										response_time_first
											from 
												(select
													distinct
													subteam,
													max_date.ticket_id,
													local_date_created,
													hour_,
													channel,
													max_date.assignee_id,
													agent_name,
													agent_email,
													supervisor,
													job_supervisor,
													case when response_time_first='NaN' then null else response_time_first end as response_time_first
														from
															(select 
																distinct
																ticket_id,
																local_date_created,
																extract(hour from local_time_created::time) as hour_,
																channel,
																date_updated,
																assignee_id,
																max(date_updated) over (partition by ticket_id) as max_date
															from 
																zendesk_chat_tickets
															where 
																client_account = 'urbanstems') as max_date 
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
													join zendesk_chat_insights zci on zci.ticket_id = max_date.ticket_id
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
													where max_date = date_updated) as distinct_) extract_ where subteam is not null ) visor_ ) final_
	where weekly is not null
	order by date(date_trunc('week',local_date_created)) desc, agent_name)

union all

(select 
	distinct 
	subteam as client_account,
	'weekly' as period,
	date(date_trunc('week',local_date_created)) as local_date_created,
	channel,
	'' as assignee_id,
	'' as email_address,
	'Team Total' as agent_name,
	'Team Total' as supervisor,
	'frt_d' as kpi_metric,
	t_weekly as score,
	(select target from sd_performance_target 
		where client_account = 'urbanstems' 
		and channel = 'chat' 
		and kpi='response_time' 
		and (local_date_created between start_date and end_date)) as target,
	case 
			when t_weekly::numeric <= (select ceiling from sd_kpi_metrics
							where client_account = 'urbanstems'
							and channel = 'chat' and kpi='response_time' and rating = '5') then 5
			when 	(t_weekly::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'chat' and kpi='response_time' and rating = '4')
				and
					t_weekly::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'chat' and kpi='response_time' and rating = '4'))
			then 4
			when 	(t_weekly::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'chat' and kpi='response_time' and rating = '3')
				and
					t_weekly::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'chat' and kpi='response_time' and rating = '3'))
			then 3	
			when 	(t_weekly::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'chat' and kpi='response_time' and rating = '2')
				and
					t_weekly::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'chat' and kpi='response_time' and rating = '2'))
			then 2	
			when t_weekly::numeric >=	(select base from sd_kpi_metrics
							where client_account = 'urbanstems'
							and channel = 'chat' and kpi='response_time' and rating = '1') then 1		
		end as rating,
	(select weight from sd_kpi_weights
	where client_account = 'urbanstems'
	and channel = 'chat' and (local_date_created::date between start_date::date and end_date::date)) as weight
		from
			(select
				*,
				round(avg(response_time_first) over (partition by agent_name, local_date_created, hour_))::varchar as hourly,
				round(avg(response_time_first) over (partition by local_date_created, hour_, subteam))::varchar as t_hourly,
				round(avg(response_time_first) over (partition by supervisor, subteam, local_date_created, hour_))::varchar as v_hourly,
				round(avg(response_time_first) over (partition by agent_name, local_date_created))::varchar as daily,
				round(avg(response_time_first) over (partition by local_date_created,subteam))::varchar as t_daily,
				round(avg(response_time_first) over (partition by supervisor, local_date_created, subteam))::varchar as v_daily,
				round(avg(response_time_first) over (partition by agent_name, date(date_trunc('week',local_date_created))))::varchar as weekly,
				round(avg(response_time_first) over (partition by subteam, date(date_trunc('week',local_date_created))))::varchar as t_weekly,
				round(avg(response_time_first) over (partition by supervisor, subteam, date(date_trunc('week',local_date_created))))::varchar as v_weekly,
				round(avg(response_time_first) over (partition by agent_name, date(date_trunc('month',local_date_created))))::varchar as monthly,
				round(avg(response_time_first) over (partition by subteam, date(date_trunc('month',local_date_created))))::varchar as t_monthly,
				round(avg(response_time_first) over (partition by supervisor, subteam, date(date_trunc('month',local_date_created))))::varchar as v_monthly
					from
						(select
							subteam,
							ticket_id,
							local_date_created,
							hour_,
							channel,
							assignee_id,
							agent_name,
							email_address,
							case when supervisor is null then 'Urbanstems Flex' else supervisor end as supervisor,
							response_time_first
								from 
									(select 
										distinct
										subteam,
										ticket_id,
										local_date_created,
										hour_,
										channel,
										assignee_id,
										agent_name,
										agent_email as email_address,
										case when supervisor is null then job_supervisor else supervisor end as supervisor,
										response_time_first
											from 
												(select
													distinct
													subteam,
													max_date.ticket_id,
													local_date_created,
													hour_,
													channel,
													max_date.assignee_id,
													agent_name,
													agent_email,
													supervisor,
													job_supervisor,
													case when response_time_first='NaN' then null else response_time_first end as response_time_first
														from
															(select 
																distinct
																ticket_id,
																local_date_created,
																extract(hour from local_time_created::time) as hour_,
																channel,
																date_updated,
																assignee_id,
																max(date_updated) over (partition by ticket_id) as max_date
															from 
																zendesk_chat_tickets
															where 
																client_account = 'urbanstems') as max_date 
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
													join zendesk_chat_insights zci on zci.ticket_id = max_date.ticket_id
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
													where max_date = date_updated) as distinct_) extract_ where subteam is not null ) visor_ ) final_
	where t_weekly is not null
	order by date(date_trunc('week',local_date_created)) desc, agent_name)

union all 

(select 
	distinct 
	subteam as client_account,
	'weekly' as period,
	date(date_trunc('week',local_date_created)) as local_date_created,
	channel,
	'' as assignee_id,
	'' as email_address,
	'Team Total' as agent_name,
	supervisor,
	'frt_d' as kpi_metric,
	v_weekly as score,
	(select target from sd_performance_target 
		where client_account = 'urbanstems' 
		and channel = 'chat' 
		and kpi='response_time' 
		and (local_date_created between start_date and end_date)) as target,
	case 
			when v_weekly::numeric <= (select ceiling from sd_kpi_metrics
							where client_account = 'urbanstems'
							and channel = 'chat' and kpi='response_time' and rating = '5') then 5
			when 	(v_weekly::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'chat' and kpi='response_time' and rating = '4')
				and
					v_weekly::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'chat' and kpi='response_time' and rating = '4'))
			then 4
			when 	(v_weekly::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'chat' and kpi='response_time' and rating = '3')
				and
					v_weekly::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'chat' and kpi='response_time' and rating = '3'))
			then 3	
			when 	(v_weekly::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'chat' and kpi='response_time' and rating = '2')
				and
					v_weekly::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'chat' and kpi='response_time' and rating = '2'))
			then 2	
			when v_weekly::numeric >=	(select base from sd_kpi_metrics
							where client_account = 'urbanstems'
							and channel = 'chat' and kpi='response_time' and rating = '1') then 1		
		end as rating,
	(select weight from sd_kpi_weights
	where client_account = 'urbanstems'
	and channel = 'chat' and (local_date_created::date between start_date::date and end_date::date)) as weight
		from
			(select
				*,
				round(avg(response_time_first) over (partition by agent_name, local_date_created, hour_))::varchar as hourly,
				round(avg(response_time_first) over (partition by local_date_created, hour_, subteam))::varchar as t_hourly,
				round(avg(response_time_first) over (partition by supervisor, subteam, local_date_created, hour_))::varchar as v_hourly,
				round(avg(response_time_first) over (partition by agent_name, local_date_created))::varchar as daily,
				round(avg(response_time_first) over (partition by local_date_created,subteam))::varchar as t_daily,
				round(avg(response_time_first) over (partition by supervisor, local_date_created, subteam))::varchar as v_daily,
				round(avg(response_time_first) over (partition by agent_name, date(date_trunc('week',local_date_created))))::varchar as weekly,
				round(avg(response_time_first) over (partition by subteam, date(date_trunc('week',local_date_created))))::varchar as t_weekly,
				round(avg(response_time_first) over (partition by supervisor, subteam, date(date_trunc('week',local_date_created))))::varchar as v_weekly,
				round(avg(response_time_first) over (partition by agent_name, date(date_trunc('month',local_date_created))))::varchar as monthly,
				round(avg(response_time_first) over (partition by subteam, date(date_trunc('month',local_date_created))))::varchar as t_monthly,
				round(avg(response_time_first) over (partition by supervisor, subteam, date(date_trunc('month',local_date_created))))::varchar as v_monthly
					from
						(select
							subteam,
							ticket_id,
							local_date_created,
							hour_,
							channel,
							assignee_id,
							agent_name,
							email_address,
							case when supervisor is null then 'Urbanstems Flex' else supervisor end as supervisor,
							response_time_first
								from 
									(select 
										distinct
										subteam,
										ticket_id,
										local_date_created,
										hour_,
										channel,
										assignee_id,
										agent_name,
										agent_email as email_address,
										case when supervisor is null then job_supervisor else supervisor end as supervisor,
										response_time_first
											from 
												(select
													distinct
													subteam,
													max_date.ticket_id,
													local_date_created,
													hour_,
													channel,
													max_date.assignee_id,
													agent_name,
													agent_email,
													supervisor,
													job_supervisor,
													case when response_time_first='NaN' then null else response_time_first end as response_time_first
														from
															(select 
																distinct
																ticket_id,
																local_date_created,
																extract(hour from local_time_created::time) as hour_,
																channel,
																date_updated,
																assignee_id,
																max(date_updated) over (partition by ticket_id) as max_date
															from 
																zendesk_chat_tickets
															where 
																client_account = 'urbanstems') as max_date 
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
													join zendesk_chat_insights zci on zci.ticket_id = max_date.ticket_id
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
													where max_date = date_updated) as distinct_) extract_ where subteam is not null ) visor_ ) final_
	where v_weekly is not null
	order by date(date_trunc('week',local_date_created)) desc, agent_name)
	
union all 

(select 
	distinct 
	subteam as client_account,
	'monthly' as period,
	date(date_trunc('month',local_date_created)) as local_date_created,
	channel,
	assignee_id::varchar as assignee_id,
	email_address,
	agent_name,
	supervisor,
	'frt_d' as kpi_metric,
	monthly as score,
	(select target from sd_performance_target 
		where client_account = 'urbanstems' 
		and channel = 'chat' 
		and kpi='response_time' 
		and (date(date_trunc('month',local_date_created)) between start_date and end_date)) as target,
	case 
			when monthly::numeric <= (select ceiling from sd_kpi_metrics
							where client_account = 'urbanstems'
							and channel = 'chat' and kpi='response_time' and rating = '5') then 5
			when 	(monthly::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'chat' and kpi='response_time' and rating = '4')
				and
					monthly::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'chat' and kpi='response_time' and rating = '4'))
			then 4
			when 	(monthly::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'chat' and kpi='response_time' and rating = '3')
				and
					monthly::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'chat' and kpi='response_time' and rating = '3'))
			then 3	
			when 	(monthly::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'chat' and kpi='response_time' and rating = '2')
				and
					monthly::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'chat' and kpi='response_time' and rating = '2'))
			then 2	
			when monthly::numeric >=	(select base from sd_kpi_metrics
							where client_account = 'urbanstems'
							and channel = 'chat' and kpi='response_time' and rating = '1') then 1		
		end as rating,
	(select weight from sd_kpi_weights
	where client_account = 'urbanstems'
	and channel = 'chat' and (date(date_trunc('month',local_date_created)) between start_date::date and end_date::date)) as weight
		from
			(select
				*,
				round(avg(response_time_first) over (partition by agent_name, local_date_created, hour_))::varchar as hourly,
				round(avg(response_time_first) over (partition by local_date_created, hour_, subteam))::varchar as t_hourly,
				round(avg(response_time_first) over (partition by supervisor, subteam, local_date_created, hour_))::varchar as v_hourly,
				round(avg(response_time_first) over (partition by agent_name, local_date_created))::varchar as daily,
				round(avg(response_time_first) over (partition by local_date_created,subteam))::varchar as t_daily,
				round(avg(response_time_first) over (partition by supervisor, local_date_created, subteam))::varchar as v_daily,
				round(avg(response_time_first) over (partition by agent_name, date(date_trunc('week',local_date_created))))::varchar as weekly,
				round(avg(response_time_first) over (partition by subteam, date(date_trunc('week',local_date_created))))::varchar as t_weekly,
				round(avg(response_time_first) over (partition by supervisor, subteam, date(date_trunc('week',local_date_created))))::varchar as v_weekly,
				round(avg(response_time_first) over (partition by agent_name, date(date_trunc('month',local_date_created))))::varchar as monthly,
				round(avg(response_time_first) over (partition by subteam, date(date_trunc('month',local_date_created))))::varchar as t_monthly,
				round(avg(response_time_first) over (partition by supervisor, subteam, date(date_trunc('month',local_date_created))))::varchar as v_monthly
					from
						(select
							subteam,
							ticket_id,
							local_date_created,
							hour_,
							channel,
							assignee_id,
							agent_name,
							email_address,
							case when supervisor is null then 'Urbanstems Flex' else supervisor end as supervisor,
							response_time_first
								from 
									(select 
										distinct
										subteam,
										ticket_id,
										local_date_created,
										hour_,
										channel,
										assignee_id,
										agent_name,
										agent_email as email_address,
										case when supervisor is null then job_supervisor else supervisor end as supervisor,
										response_time_first
											from 
												(select
													distinct
													subteam,
													max_date.ticket_id,
													local_date_created,
													hour_,
													channel,
													max_date.assignee_id,
													agent_name,
													agent_email,
													supervisor,
													job_supervisor,
													case when response_time_first='NaN' then null else response_time_first end as response_time_first
														from
															(select 
																distinct
																ticket_id,
																local_date_created,
																extract(hour from local_time_created::time) as hour_,
																channel,
																date_updated,
																assignee_id,
																max(date_updated) over (partition by ticket_id) as max_date
															from 
																zendesk_chat_tickets
															where 
																client_account = 'urbanstems') as max_date 
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
													join zendesk_chat_insights zci on zci.ticket_id = max_date.ticket_id
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
													where max_date = date_updated) as distinct_) extract_ where subteam is not null ) visor_ ) final_
	where monthly is not null
	order by date(date_trunc('month',local_date_created)) desc, agent_name)

union all

(select 
	distinct 
	subteam as client_account,
	'monthly' as period,
	date(date_trunc('month',local_date_created)) as local_date_created,
	channel,
	'' as assignee_id,
	'' as email_address,
	'Team Total' as agent_name,
	'Team Total' as supervisor,
	'frt_d' as kpi_metric,
	t_monthly as score,
	(select target from sd_performance_target 
		where client_account = 'urbanstems' 
		and channel = 'chat' 
		and kpi='response_time' 
		and (date(date_trunc('month',local_date_created)) between start_date and end_date)) as target,
	case 
			when t_monthly::numeric <= (select ceiling from sd_kpi_metrics
							where client_account = 'urbanstems'
							and channel = 'chat' and kpi='response_time' and rating = '5') then 5
			when 	(t_monthly::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'chat' and kpi='response_time' and rating = '4')
				and
					t_monthly::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'chat' and kpi='response_time' and rating = '4'))
			then 4
			when 	(t_monthly::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'chat' and kpi='response_time' and rating = '3')
				and
					t_monthly::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'chat' and kpi='response_time' and rating = '3'))
			then 3	
			when 	(t_monthly::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'chat' and kpi='response_time' and rating = '2')
				and
					t_monthly::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'chat' and kpi='response_time' and rating = '2'))
			then 2	
			when t_monthly::numeric >=	(select base from sd_kpi_metrics
							where client_account = 'urbanstems'
							and channel = 'chat' and kpi='response_time' and rating = '1') then 1		
		end as rating,
	(select weight from sd_kpi_weights
	where client_account = 'urbanstems'
	and channel = 'chat' and (date(date_trunc('month',local_date_created)) between start_date::date and end_date::date)) as weight
		from
			(select
				*,
				round(avg(response_time_first) over (partition by agent_name, local_date_created, hour_))::varchar as hourly,
				round(avg(response_time_first) over (partition by local_date_created, hour_, subteam))::varchar as t_hourly,
				round(avg(response_time_first) over (partition by supervisor, subteam, local_date_created, hour_))::varchar as v_hourly,
				round(avg(response_time_first) over (partition by agent_name, local_date_created))::varchar as daily,
				round(avg(response_time_first) over (partition by local_date_created,subteam))::varchar as t_daily,
				round(avg(response_time_first) over (partition by supervisor, local_date_created, subteam))::varchar as v_daily,
				round(avg(response_time_first) over (partition by agent_name, date(date_trunc('week',local_date_created))))::varchar as weekly,
				round(avg(response_time_first) over (partition by subteam, date(date_trunc('week',local_date_created))))::varchar as t_weekly,
				round(avg(response_time_first) over (partition by supervisor, subteam, date(date_trunc('week',local_date_created))))::varchar as v_weekly,
				round(avg(response_time_first) over (partition by agent_name, date(date_trunc('month',local_date_created))))::varchar as monthly,
				round(avg(response_time_first) over (partition by subteam, date(date_trunc('month',local_date_created))))::varchar as t_monthly,
				round(avg(response_time_first) over (partition by supervisor, subteam, date(date_trunc('month',local_date_created))))::varchar as v_monthly
					from
						(select
							subteam,
							ticket_id,
							local_date_created,
							hour_,
							channel,
							assignee_id,
							agent_name,
							email_address,
							case when supervisor is null then 'Urbanstems Flex' else supervisor end as supervisor,
							response_time_first
								from 
									(select 
										distinct
										subteam,
										ticket_id,
										local_date_created,
										hour_,
										channel,
										assignee_id,
										agent_name,
										agent_email as email_address,
										case when supervisor is null then job_supervisor else supervisor end as supervisor,
										response_time_first
											from 
												(select
													distinct
													subteam,
													max_date.ticket_id,
													local_date_created,
													hour_,
													channel,
													max_date.assignee_id,
													agent_name,
													agent_email,
													supervisor,
													job_supervisor,
													case when response_time_first='NaN' then null else response_time_first end as response_time_first
														from
															(select 
																distinct
																ticket_id,
																local_date_created,
																extract(hour from local_time_created::time) as hour_,
																channel,
																date_updated,
																assignee_id,
																max(date_updated) over (partition by ticket_id) as max_date
															from 
																zendesk_chat_tickets
															where 
																client_account = 'urbanstems') as max_date 
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
													join zendesk_chat_insights zci on zci.ticket_id = max_date.ticket_id
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
													where max_date = date_updated) as distinct_) extract_ where subteam is not null ) visor_ ) final_
	where t_monthly is not null
	order by date(date_trunc('month',local_date_created)) desc, agent_name)

union all 

(select 
	distinct 
	subteam as client_account,
	'monthly' as period,
	date(date_trunc('month',local_date_created)) as local_date_created,
	channel,
	'' as assignee_id,
	'' as email_address,
	'Team Total' as agent_name,
	supervisor,
	'frt_d' as kpi_metric,
	v_monthly as score,
	(select target from sd_performance_target 
		where client_account = 'urbanstems' 
		and channel = 'chat' 
		and kpi='response_time' 
		and (date(date_trunc('month',local_date_created)) between start_date and end_date)) as target,
	case 
			when v_monthly::numeric <= (select ceiling from sd_kpi_metrics
							where client_account = 'urbanstems'
							and channel = 'chat' and kpi='response_time' and rating = '5') then 5
			when 	(v_monthly::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'chat' and kpi='response_time' and rating = '4')
				and
					v_monthly::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'chat' and kpi='response_time' and rating = '4'))
			then 4
			when 	(v_monthly::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'chat' and kpi='response_time' and rating = '3')
				and
					v_monthly::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'chat' and kpi='response_time' and rating = '3'))
			then 3	
			when 	(v_monthly::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'chat' and kpi='response_time' and rating = '2')
				and
					v_monthly::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'chat' and kpi='response_time' and rating = '2'))
			then 2	
			when v_monthly::numeric >=	(select base from sd_kpi_metrics
							where client_account = 'urbanstems'
							and channel = 'chat' and kpi='response_time' and rating = '1') then 1		
		end as rating,
	(select weight from sd_kpi_weights
	where client_account = 'urbanstems'
	and channel = 'chat' and (date(date_trunc('month',local_date_created)) between start_date::date and end_date::date)) as weight
		from
			(select
				*,
				round(avg(response_time_first) over (partition by agent_name, local_date_created, hour_))::varchar as hourly,
				round(avg(response_time_first) over (partition by local_date_created, hour_, subteam))::varchar as t_hourly,
				round(avg(response_time_first) over (partition by supervisor, subteam, local_date_created, hour_))::varchar as v_hourly,
				round(avg(response_time_first) over (partition by agent_name, local_date_created))::varchar as daily,
				round(avg(response_time_first) over (partition by local_date_created,subteam))::varchar as t_daily,
				round(avg(response_time_first) over (partition by supervisor, local_date_created, subteam))::varchar as v_daily,
				round(avg(response_time_first) over (partition by agent_name, date(date_trunc('week',local_date_created))))::varchar as weekly,
				round(avg(response_time_first) over (partition by subteam, date(date_trunc('week',local_date_created))))::varchar as t_weekly,
				round(avg(response_time_first) over (partition by supervisor, subteam, date(date_trunc('week',local_date_created))))::varchar as v_weekly,
				round(avg(response_time_first) over (partition by agent_name, date(date_trunc('month',local_date_created))))::varchar as monthly,
				round(avg(response_time_first) over (partition by subteam, date(date_trunc('month',local_date_created))))::varchar as t_monthly,
				round(avg(response_time_first) over (partition by supervisor, subteam, date(date_trunc('month',local_date_created))))::varchar as v_monthly
					from
						(select
							subteam,
							ticket_id,
							local_date_created,
							hour_,
							channel,
							assignee_id,
							agent_name,
							email_address,
							case when supervisor is null then 'Urbanstems Flex' else supervisor end as supervisor,
							response_time_first
								from 
									(select 
										distinct
										subteam,
										ticket_id,
										local_date_created,
										hour_,
										channel,
										assignee_id,
										agent_name,
										agent_email as email_address,
										case when supervisor is null then job_supervisor else supervisor end as supervisor,
										response_time_first
											from 
												(select
													distinct
													subteam,
													max_date.ticket_id,
													local_date_created,
													hour_,
													channel,
													max_date.assignee_id,
													agent_name,
													agent_email,
													supervisor,
													job_supervisor,
													case when response_time_first='NaN' then null else response_time_first end as response_time_first
														from
															(select 
																distinct
																ticket_id,
																local_date_created,
																extract(hour from local_time_created::time) as hour_,
																channel,
																date_updated,
																assignee_id,
																max(date_updated) over (partition by ticket_id) as max_date
															from 
																zendesk_chat_tickets
															where 
																client_account = 'urbanstems') as max_date 
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
													join zendesk_chat_insights zci on zci.ticket_id = max_date.ticket_id
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
													where max_date = date_updated) as distinct_) extract_ where subteam is not null ) visor_ ) final_
	where v_monthly is not null
	order by date(date_trunc('month',local_date_created)) desc, agent_name)
