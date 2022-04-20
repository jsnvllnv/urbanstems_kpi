(select 
	distinct 
	client_account,
	'daily' as period,
	date(date_trunc('day',local_date_created)) as local_date_created,
	'voice' as channel,
	assignee_id::varchar as assignee_id,
	email_address,
	agent_name,
	supervisor,
	'awt_d' as kpi_metric,
	daily as score,
	(select target from sd_performance_target 
		where client_account = 'urbanstems' 
		and channel = 'call' 
		and kpi='wait_time' 
		and (local_date_created between start_date and end_date)) as target,
	case 
			when daily::numeric <= (select ceiling from sd_kpi_metrics
							where client_account = 'urbanstems'
							and channel = 'call' and kpi='response_time' and rating = '5') then 5
			when 	(daily::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'call' and kpi='response_time' and rating = '4')
				and
					daily::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'call' and kpi='response_time' and rating = '4'))
			then 4
			when 	(daily::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'call' and kpi='response_time' and rating = '3')
				and
					daily::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'call' and kpi='response_time' and rating = '3'))
			then 3	
			when 	(daily::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'call' and kpi='response_time' and rating = '2')
				and
					daily::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'call' and kpi='response_time' and rating = '2'))
			then 2	
			when daily::numeric >=	(select base from sd_kpi_metrics
							where client_account = 'urbanstems'
							and channel = 'call' and kpi='response_time' and rating = '1') then 1		
		end as rating,
	(select weight from sd_kpi_weights
	where client_account = 'urbanstems'
	and channel = 'call' and (local_date_created::date between start_date::date and end_date::date)) as weight
		from 
			(select 
				subteam as client_account,
				local_date_created,
				assignee_id,
				email_address,
				agent_name,
				supervisor,
				dur,
				round(avg(dur) over (partition by agent_name, local_date_created, hour_))::varchar as hourly,
				round(avg(dur) over (partition by local_date_created, hour_, subteam))::varchar as t_hourly,
				round(avg(dur) over (partition by supervisor, subteam, local_date_created, hour_))::varchar as v_hourly,
				round(avg(dur) over (partition by agent_name, local_date_created))::varchar as daily,
				round(avg(dur) over (partition by local_date_created,subteam))::varchar as t_daily,
				round(avg(dur) over (partition by supervisor, local_date_created, subteam))::varchar as v_daily,
				round(avg(dur) over (partition by agent_name, date(date_trunc('week',local_date_created))))::varchar as weekly,
				round(avg(dur) over (partition by subteam, date(date_trunc('week',local_date_created))))::varchar as t_weekly,
				round(avg(dur) over (partition by supervisor, subteam, date(date_trunc('week',local_date_created))))::varchar as v_weekly,
				round(avg(dur) over (partition by agent_name, date(date_trunc('month',local_date_created))))::varchar as monthly,
				round(avg(dur) over (partition by subteam, date(date_trunc('month',local_date_created))))::varchar as t_monthly,
				round(avg(dur) over (partition by supervisor, subteam, date(date_trunc('month',local_date_created))))::varchar as v_monthly
					from
						(select 
							distinct 
							subteam,
							ticket_id,
							local_date_created,
							hour_,
							assignee_id,
							agent_email as email_address,
							agent_name,
							case when supervisor is null then 'Urbanstems Flex' else supervisor end as supervisor,
							dur
								from 
									(select 
										distinct 
										subteam,
										local_date_created,
										hour_,
										ticket_id,
										assignee_id::varchar as assignee_id,
										agent_email,
										agent_name,
										case when supervisor is null then job_supervisor else supervisor end as supervisor,
										dur
											from
												(select 
													distinct
													zct.ticket_id,
													dur,
													channel,
													local_date_created,
													extract(hour from local_time_created::time) as hour_,
													assignee_id,
													via_source_rel,
													zct.status
												from 
													zendesk_call_tickets zct 
												left join 
													(select 
														distinct 
														ticket_id,
														status,
														dur
															from
																(select 
																	distinct
																	ticket_id,
																	forwarded_to,
																	status,
																	extract(minutes from to_timestamp(wait_time, 'mi:ss')::time)*60 + extract(seconds from to_timestamp(wait_time, 'mi:ss')::time) as dur,
																	wait_time,
																	local_date,
																	to_timestamp(wait_time, 'mi:ss')::time as wait_time2
																		from 
																			(select 
																				*,
																				max(wait_time) over (partition by ticket_id) as max_wait
																			from 
																				zendesk_call_history zch 
																			where 
																				client_account = 'urbanstems'
																				) mx
																	where max_wait = wait_time 
																	) dur ) dur1 on dur1.ticket_id = zct.ticket_id 
												where 
													client_account = 'urbanstems'
													and via_source_rel <> 'outbound'
													and (zct.status = 'solved' or zct.status = 'closed')
													) zd
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
									where 
										local_date_created >= '2022-04-04') raw_ ) final_ ) final_2
order by date(date_trunc('day',local_date_created)) desc, agent_name)

union all 

(select 
	distinct 
	client_account,
	'daily' as period,
	date(date_trunc('day',local_date_created)) as local_date_created,
	'voice' as channel,
	'' as assignee_id,
	'' as email_address,
	'Team Total' as agent_name,
	'Team Total' as supervisor,
	'awt_d' as kpi_metric,
	t_daily as score,
	(select target from sd_performance_target 
		where client_account = 'urbanstems' 
		and channel = 'call' 
		and kpi='wait_time' 
		and (local_date_created between start_date and end_date)) as target,
	case 
			when t_daily::numeric <= (select ceiling from sd_kpi_metrics
							where client_account = 'urbanstems'
							and channel = 'call' and kpi='response_time' and rating = '5') then 5
			when 	(t_daily::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'call' and kpi='response_time' and rating = '4')
				and
					t_daily::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'call' and kpi='response_time' and rating = '4'))
			then 4
			when 	(t_daily::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'call' and kpi='response_time' and rating = '3')
				and
					t_daily::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'call' and kpi='response_time' and rating = '3'))
			then 3	
			when 	(t_daily::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'call' and kpi='response_time' and rating = '2')
				and
					t_daily::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'call' and kpi='response_time' and rating = '2'))
			then 2	
			when t_daily::numeric >= (select base from sd_kpi_metrics
							where client_account = 'urbanstems'
							and channel = 'call' and kpi='response_time' and rating = '1') then 1		
		end as rating,
	(select weight from sd_kpi_weights
	where client_account = 'urbanstems'
	and channel = 'call' and (local_date_created::date between start_date::date and end_date::date)) as weight
		from 
			(select 
				subteam as client_account,
				local_date_created,
				assignee_id,
				email_address,
				agent_name,
				supervisor,
				dur,
				round(avg(dur) over (partition by agent_name, local_date_created, hour_))::varchar as hourly,
				round(avg(dur) over (partition by local_date_created, hour_, subteam))::varchar as t_hourly,
				round(avg(dur) over (partition by supervisor, subteam, local_date_created, hour_))::varchar as v_hourly,
				round(avg(dur) over (partition by agent_name, local_date_created))::varchar as daily,
				round(avg(dur) over (partition by local_date_created,subteam))::varchar as t_daily,
				round(avg(dur) over (partition by supervisor, local_date_created, subteam))::varchar as v_daily,
				round(avg(dur) over (partition by agent_name, date(date_trunc('week',local_date_created))))::varchar as weekly,
				round(avg(dur) over (partition by subteam, date(date_trunc('week',local_date_created))))::varchar as t_weekly,
				round(avg(dur) over (partition by supervisor, subteam, date(date_trunc('week',local_date_created))))::varchar as v_weekly,
				round(avg(dur) over (partition by agent_name, date(date_trunc('month',local_date_created))))::varchar as monthly,
				round(avg(dur) over (partition by subteam, date(date_trunc('month',local_date_created))))::varchar as t_monthly,
				round(avg(dur) over (partition by supervisor, subteam, date(date_trunc('month',local_date_created))))::varchar as v_monthly
					from
						(select 
							distinct 
							subteam,
							ticket_id,
							local_date_created,
							hour_,
							assignee_id,
							agent_email as email_address,
							agent_name,
							case when supervisor is null then 'Urbanstems Flex' else supervisor end as supervisor,
							dur
								from 
									(select 
										distinct 
										subteam,
										local_date_created,
										hour_,
										ticket_id,
										assignee_id::varchar as assignee_id,
										agent_email,
										agent_name,
										case when supervisor is null then job_supervisor else supervisor end as supervisor,
										dur
											from
												(select 
													distinct
													zct.ticket_id,
													dur,
													channel,
													local_date_created,
													extract(hour from local_time_created::time) as hour_,
													assignee_id,
													via_source_rel,
													zct.status
												from 
													zendesk_call_tickets zct 
												left join 
													(select 
														distinct 
														ticket_id,
														status,
														dur
															from
																(select 
																	distinct
																	ticket_id,
																	forwarded_to,
																	status,
																	extract(minutes from to_timestamp(wait_time, 'mi:ss')::time)*60 + extract(seconds from to_timestamp(wait_time, 'mi:ss')::time) as dur,
																	wait_time,
																	local_date,
																	to_timestamp(wait_time, 'mi:ss')::time as wait_time2
																		from 
																			(select 
																				*,
																				max(wait_time) over (partition by ticket_id) as max_wait
																			from 
																				zendesk_call_history zch 
																			where 
																				client_account = 'urbanstems'
																				) mx
																	where max_wait = wait_time 
																	) dur ) dur1 on dur1.ticket_id = zct.ticket_id 
												where 
													client_account = 'urbanstems'
													and via_source_rel <> 'outbound'
													and (zct.status = 'solved' or zct.status = 'closed')
													) zd
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
									where 
										local_date_created >= '2022-04-04') raw_ ) final_ ) final_2
order by date(date_trunc('day',local_date_created)) desc, agent_name)

union all 

(select 
	distinct 
	client_account,
	'daily' as period,
	date(date_trunc('day',local_date_created)) as local_date_created,
	'voice' as channel,
	'' as assignee_id,
	'' as email_address,
	'Team Total' as agent_name,
	supervisor,
	'awt_d' as kpi_metric,
	v_daily as score,
	(select target from sd_performance_target 
		where client_account = 'urbanstems' 
		and channel = 'call' 
		and kpi='wait_time' 
		and (local_date_created between start_date and end_date)) as target,
	case 
			when v_daily::numeric <= (select ceiling from sd_kpi_metrics
							where client_account = 'urbanstems'
							and channel = 'call' and kpi='response_time' and rating = '5') then 5
			when 	(v_daily::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'call' and kpi='response_time' and rating = '4')
				and
					v_daily::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'call' and kpi='response_time' and rating = '4'))
			then 4
			when 	(v_daily::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'call' and kpi='response_time' and rating = '3')
				and
					v_daily::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'call' and kpi='response_time' and rating = '3'))
			then 3	
			when 	(v_daily::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'call' and kpi='response_time' and rating = '2')
				and
					v_daily::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'call' and kpi='response_time' and rating = '2'))
			then 2	
			when v_daily::numeric >= (select base from sd_kpi_metrics
							where client_account = 'urbanstems'
							and channel = 'call' and kpi='response_time' and rating = '1') then 1		
		end as rating,
	(select weight from sd_kpi_weights
	where client_account = 'urbanstems'
	and channel = 'call' and (local_date_created::date between start_date::date and end_date::date)) as weight
		from 
			(select 
				subteam as client_account,
				local_date_created,
				assignee_id,
				email_address,
				agent_name,
				supervisor,
				dur,
				round(avg(dur) over (partition by agent_name, local_date_created, hour_))::varchar as hourly,
				round(avg(dur) over (partition by local_date_created, hour_, subteam))::varchar as t_hourly,
				round(avg(dur) over (partition by supervisor, subteam, local_date_created, hour_))::varchar as v_hourly,
				round(avg(dur) over (partition by agent_name, local_date_created))::varchar as daily,
				round(avg(dur) over (partition by local_date_created,subteam))::varchar as t_daily,
				round(avg(dur) over (partition by supervisor, local_date_created, subteam))::varchar as v_daily,
				round(avg(dur) over (partition by agent_name, date(date_trunc('week',local_date_created))))::varchar as weekly,
				round(avg(dur) over (partition by subteam, date(date_trunc('week',local_date_created))))::varchar as t_weekly,
				round(avg(dur) over (partition by supervisor, subteam, date(date_trunc('week',local_date_created))))::varchar as v_weekly,
				round(avg(dur) over (partition by agent_name, date(date_trunc('month',local_date_created))))::varchar as monthly,
				round(avg(dur) over (partition by subteam, date(date_trunc('month',local_date_created))))::varchar as t_monthly,
				round(avg(dur) over (partition by supervisor, subteam, date(date_trunc('month',local_date_created))))::varchar as v_monthly
					from
						(select 
							distinct 
							subteam,
							ticket_id,
							local_date_created,
							hour_,
							assignee_id,
							agent_email as email_address,
							agent_name,
							case when supervisor is null then 'Urbanstems Flex' else supervisor end as supervisor,
							dur
								from 
									(select 
										distinct 
										subteam,
										local_date_created,
										hour_,
										ticket_id,
										assignee_id::varchar as assignee_id,
										agent_email,
										agent_name,
										case when supervisor is null then job_supervisor else supervisor end as supervisor,
										dur
											from
												(select 
													distinct
													zct.ticket_id,
													dur,
													channel,
													local_date_created,
													extract(hour from local_time_created::time) as hour_,
													assignee_id,
													via_source_rel,
													zct.status
												from 
													zendesk_call_tickets zct 
												left join 
													(select 
														distinct 
														ticket_id,
														status,
														dur
															from
																(select 
																	distinct
																	ticket_id,
																	forwarded_to,
																	status,
																	extract(minutes from to_timestamp(wait_time, 'mi:ss')::time)*60 + extract(seconds from to_timestamp(wait_time, 'mi:ss')::time) as dur,
																	wait_time,
																	local_date,
																	to_timestamp(wait_time, 'mi:ss')::time as wait_time2
																		from 
																			(select 
																				*,
																				max(wait_time) over (partition by ticket_id) as max_wait
																			from 
																				zendesk_call_history zch 
																			where 
																				client_account = 'urbanstems'
																				) mx
																	where max_wait = wait_time 
																	) dur ) dur1 on dur1.ticket_id = zct.ticket_id 
												where 
													client_account = 'urbanstems'
													and via_source_rel <> 'outbound'
													and (zct.status = 'solved' or zct.status = 'closed')
													) zd
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
									where 
										local_date_created >= '2022-04-04') raw_ ) final_ ) final_2
order by date(date_trunc('day',local_date_created)) desc, agent_name)

union all 

(select 
	distinct 
	client_account,
	'weekly' as period,
	date(date_trunc('week',local_date_created)) as local_date_created,
	'voice' as channel,
	assignee_id::varchar as assignee_id,
	email_address,
	agent_name,
	supervisor,
	'awt_d' as kpi_metric,
	weekly as score,
	(select target from sd_performance_target 
		where client_account = 'urbanstems' 
		and channel = 'call' 
		and kpi='wait_time' 
		and (local_date_created between start_date and end_date)) as target,
	case 
			when weekly::numeric <= (select ceiling from sd_kpi_metrics
							where client_account = 'urbanstems'
							and channel = 'call' and kpi='response_time' and rating = '5') then 5
			when 	(weekly::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'call' and kpi='response_time' and rating = '4')
				and
					weekly::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'call' and kpi='response_time' and rating = '4'))
			then 4
			when 	(weekly::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'call' and kpi='response_time' and rating = '3')
				and
					weekly::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'call' and kpi='response_time' and rating = '3'))
			then 3	
			when 	(weekly::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'call' and kpi='response_time' and rating = '2')
				and
					weekly::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'call' and kpi='response_time' and rating = '2'))
			then 2	
			when weekly::numeric >=	(select base from sd_kpi_metrics
							where client_account = 'urbanstems'
							and channel = 'call' and kpi='response_time' and rating = '1') then 1		
		end as rating,
	(select weight from sd_kpi_weights
	where client_account = 'urbanstems'
	and channel = 'call' and (local_date_created::date between start_date::date and end_date::date)) as weight
		from 
			(select 
				subteam as client_account,
				local_date_created,
				assignee_id,
				email_address,
				agent_name,
				supervisor,
				dur,
				round(avg(dur) over (partition by agent_name, local_date_created, hour_))::varchar as hourly,
				round(avg(dur) over (partition by local_date_created, hour_, subteam))::varchar as t_hourly,
				round(avg(dur) over (partition by supervisor, subteam, local_date_created, hour_))::varchar as v_hourly,
				round(avg(dur) over (partition by agent_name, local_date_created))::varchar as daily,
				round(avg(dur) over (partition by local_date_created,subteam))::varchar as t_daily,
				round(avg(dur) over (partition by supervisor, local_date_created, subteam))::varchar as v_daily,
				round(avg(dur) over (partition by agent_name, date(date_trunc('week',local_date_created))))::varchar as weekly,
				round(avg(dur) over (partition by subteam, date(date_trunc('week',local_date_created))))::varchar as t_weekly,
				round(avg(dur) over (partition by supervisor, subteam, date(date_trunc('week',local_date_created))))::varchar as v_weekly,
				round(avg(dur) over (partition by agent_name, date(date_trunc('month',local_date_created))))::varchar as monthly,
				round(avg(dur) over (partition by subteam, date(date_trunc('month',local_date_created))))::varchar as t_monthly,
				round(avg(dur) over (partition by supervisor, subteam, date(date_trunc('month',local_date_created))))::varchar as v_monthly
					from
						(select 
							distinct 
							subteam,
							ticket_id,
							local_date_created,
							hour_,
							assignee_id,
							agent_email as email_address,
							agent_name,
							case when supervisor is null then 'Urbanstems Flex' else supervisor end as supervisor,
							dur
								from 
									(select 
										distinct 
										subteam,
										local_date_created,
										hour_,
										ticket_id,
										assignee_id::varchar as assignee_id,
										agent_email,
										agent_name,
										case when supervisor is null then job_supervisor else supervisor end as supervisor,
										dur
											from
												(select 
													distinct
													zct.ticket_id,
													dur,
													channel,
													local_date_created,
													extract(hour from local_time_created::time) as hour_,
													assignee_id,
													via_source_rel,
													zct.status
												from 
													zendesk_call_tickets zct 
												left join 
													(select 
														distinct 
														ticket_id,
														status,
														dur
															from
																(select 
																	distinct
																	ticket_id,
																	forwarded_to,
																	status,
																	extract(minutes from to_timestamp(wait_time, 'mi:ss')::time)*60 + extract(seconds from to_timestamp(wait_time, 'mi:ss')::time) as dur,
																	wait_time,
																	local_date,
																	to_timestamp(wait_time, 'mi:ss')::time as wait_time2
																		from 
																			(select 
																				*,
																				max(wait_time) over (partition by ticket_id) as max_wait
																			from 
																				zendesk_call_history zch 
																			where 
																				client_account = 'urbanstems'
																				) mx
																	where max_wait = wait_time 
																	) dur ) dur1 on dur1.ticket_id = zct.ticket_id 
												where 
													client_account = 'urbanstems'
													and via_source_rel <> 'outbound'
													and (zct.status = 'solved' or zct.status = 'closed')
													) zd
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
									where 
										local_date_created >= '2022-04-04') raw_ ) final_ ) final_2
	where (weekly is not null and weekly::numeric > 0) and agent_name is not null
order by date(date_trunc('week',local_date_created)) desc, agent_name)

union all 

(select 
	distinct 
	client_account,
	'weekly' as period,
	date(date_trunc('week',local_date_created)) as local_date_created,
	'voice' as channel,
	'' as assignee_id,
	'' as email_address,
	'Team Total' as agent_name,
	'Team Total' as supervisor,
	'awt_d' as kpi_metric,
	t_weekly as score,
	(select target from sd_performance_target 
		where client_account = 'urbanstems' 
		and channel = 'call' 
		and kpi='wait_time' 
		and (local_date_created between start_date and end_date)) as target,
	case 
			when t_weekly::numeric <= (select ceiling from sd_kpi_metrics
							where client_account = 'urbanstems'
							and channel = 'call' and kpi='response_time' and rating = '5') then 5
			when 	(t_weekly::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'call' and kpi='response_time' and rating = '4')
				and
					t_weekly::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'call' and kpi='response_time' and rating = '4'))
			then 4
			when 	(t_weekly::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'call' and kpi='response_time' and rating = '3')
				and
					t_weekly::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'call' and kpi='response_time' and rating = '3'))
			then 3	
			when 	(t_weekly::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'call' and kpi='response_time' and rating = '2')
				and
					t_weekly::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'call' and kpi='response_time' and rating = '2'))
			then 2	
			when t_weekly::numeric >=	(select base from sd_kpi_metrics
							where client_account = 'urbanstems'
							and channel = 'call' and kpi='response_time' and rating = '1') then 1		
		end as rating,
	(select weight from sd_kpi_weights
	where client_account = 'urbanstems'
	and channel = 'call' and (local_date_created::date between start_date::date and end_date::date)) as weight
		from 
			(select 
				subteam as client_account,
				local_date_created,
				assignee_id,
				email_address,
				agent_name,
				supervisor,
				dur,
				round(avg(dur) over (partition by agent_name, local_date_created, hour_))::varchar as hourly,
				round(avg(dur) over (partition by local_date_created, hour_, subteam))::varchar as t_hourly,
				round(avg(dur) over (partition by supervisor, subteam, local_date_created, hour_))::varchar as v_hourly,
				round(avg(dur) over (partition by agent_name, local_date_created))::varchar as daily,
				round(avg(dur) over (partition by local_date_created,subteam))::varchar as t_daily,
				round(avg(dur) over (partition by supervisor, local_date_created, subteam))::varchar as v_daily,
				round(avg(dur) over (partition by agent_name, date(date_trunc('week',local_date_created))))::varchar as weekly,
				round(avg(dur) over (partition by subteam, date(date_trunc('week',local_date_created))))::varchar as t_weekly,
				round(avg(dur) over (partition by supervisor, subteam, date(date_trunc('week',local_date_created))))::varchar as v_weekly,
				round(avg(dur) over (partition by agent_name, date(date_trunc('month',local_date_created))))::varchar as monthly,
				round(avg(dur) over (partition by subteam, date(date_trunc('month',local_date_created))))::varchar as t_monthly,
				round(avg(dur) over (partition by supervisor, subteam, date(date_trunc('month',local_date_created))))::varchar as v_monthly
					from
						(select 
							distinct 
							subteam,
							ticket_id,
							local_date_created,
							hour_,
							assignee_id,
							agent_email as email_address,
							agent_name,
							case when supervisor is null then 'Urbanstems Flex' else supervisor end as supervisor,
							dur
								from 
									(select 
										distinct 
										subteam,
										local_date_created,
										hour_,
										ticket_id,
										assignee_id::varchar as assignee_id,
										agent_email,
										agent_name,
										case when supervisor is null then job_supervisor else supervisor end as supervisor,
										dur
											from
												(select 
													distinct
													zct.ticket_id,
													dur,
													channel,
													local_date_created,
													extract(hour from local_time_created::time) as hour_,
													assignee_id,
													via_source_rel,
													zct.status
												from 
													zendesk_call_tickets zct 
												left join 
													(select 
														distinct 
														ticket_id,
														status,
														dur
															from
																(select 
																	distinct
																	ticket_id,
																	forwarded_to,
																	status,
																	extract(minutes from to_timestamp(wait_time, 'mi:ss')::time)*60 + extract(seconds from to_timestamp(wait_time, 'mi:ss')::time) as dur,
																	wait_time,
																	local_date,
																	to_timestamp(wait_time, 'mi:ss')::time as wait_time2
																		from 
																			(select 
																				*,
																				max(wait_time) over (partition by ticket_id) as max_wait
																			from 
																				zendesk_call_history zch 
																			where 
																				client_account = 'urbanstems'
																				) mx
																	where max_wait = wait_time 
																	) dur ) dur1 on dur1.ticket_id = zct.ticket_id 
												where 
													client_account = 'urbanstems'
													and via_source_rel <> 'outbound'
													and (zct.status = 'solved' or zct.status = 'closed')
													) zd
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
									where 
										local_date_created >= '2022-04-04') raw_ ) final_ ) final_2
	where (t_weekly is not null and t_weekly::numeric > 0) and agent_name is not null
order by date(date_trunc('week',local_date_created)) desc, agent_name)

union all 

(select 
	distinct 
	client_account,
	'weekly' as period,
	date(date_trunc('week',local_date_created)) as local_date_created,
	'voice' as channel,
	'' as assignee_id,
	'' as email_address,
	'Team Total' as agent_name,
	supervisor,
	'awt_d' as kpi_metric,
	v_weekly as score,
	(select target from sd_performance_target 
		where client_account = 'urbanstems' 
		and channel = 'call' 
		and kpi='wait_time' 
		and (local_date_created between start_date and end_date)) as target,
	case 
			when v_weekly::numeric <= (select ceiling from sd_kpi_metrics
							where client_account = 'urbanstems'
							and channel = 'call' and kpi='response_time' and rating = '5') then 5
			when 	(v_weekly::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'call' and kpi='response_time' and rating = '4')
				and
					v_weekly::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'call' and kpi='response_time' and rating = '4'))
			then 4
			when 	(v_weekly::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'call' and kpi='response_time' and rating = '3')
				and
					v_weekly::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'call' and kpi='response_time' and rating = '3'))
			then 3	
			when 	(v_weekly::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'call' and kpi='response_time' and rating = '2')
				and
					v_weekly::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'call' and kpi='response_time' and rating = '2'))
			then 2	
			when v_weekly::numeric >=	(select base from sd_kpi_metrics
							where client_account = 'urbanstems'
							and channel = 'call' and kpi='response_time' and rating = '1') then 1		
		end as rating,
	(select weight from sd_kpi_weights
	where client_account = 'urbanstems'
	and channel = 'call' and (local_date_created::date between start_date::date and end_date::date)) as weight
		from 
			(select 
				subteam as client_account,
				local_date_created,
				assignee_id,
				email_address,
				agent_name,
				supervisor,
				dur,
				round(avg(dur) over (partition by agent_name, local_date_created, hour_))::varchar as hourly,
				round(avg(dur) over (partition by local_date_created, hour_, subteam))::varchar as t_hourly,
				round(avg(dur) over (partition by supervisor, subteam, local_date_created, hour_))::varchar as v_hourly,
				round(avg(dur) over (partition by agent_name, local_date_created))::varchar as daily,
				round(avg(dur) over (partition by local_date_created,subteam))::varchar as t_daily,
				round(avg(dur) over (partition by supervisor, local_date_created, subteam))::varchar as v_daily,
				round(avg(dur) over (partition by agent_name, date(date_trunc('week',local_date_created))))::varchar as weekly,
				round(avg(dur) over (partition by subteam, date(date_trunc('week',local_date_created))))::varchar as t_weekly,
				round(avg(dur) over (partition by supervisor, subteam, date(date_trunc('week',local_date_created))))::varchar as v_weekly,
				round(avg(dur) over (partition by agent_name, date(date_trunc('month',local_date_created))))::varchar as monthly,
				round(avg(dur) over (partition by subteam, date(date_trunc('month',local_date_created))))::varchar as t_monthly,
				round(avg(dur) over (partition by supervisor, subteam, date(date_trunc('month',local_date_created))))::varchar as v_monthly
					from
						(select 
							distinct 
							subteam,
							ticket_id,
							local_date_created,
							hour_,
							assignee_id,
							agent_email as email_address,
							agent_name,
							case when supervisor is null then 'Urbanstems Flex' else supervisor end as supervisor,
							dur
								from 
									(select 
										distinct 
										subteam,
										local_date_created,
										hour_,
										ticket_id,
										assignee_id::varchar as assignee_id,
										agent_email,
										agent_name,
										case when supervisor is null then job_supervisor else supervisor end as supervisor,
										dur
											from
												(select 
													distinct
													zct.ticket_id,
													dur,
													channel,
													local_date_created,
													extract(hour from local_time_created::time) as hour_,
													assignee_id,
													via_source_rel,
													zct.status
												from 
													zendesk_call_tickets zct 
												left join 
													(select 
														distinct 
														ticket_id,
														status,
														dur
															from
																(select 
																	distinct
																	ticket_id,
																	forwarded_to,
																	status,
																	extract(minutes from to_timestamp(wait_time, 'mi:ss')::time)*60 + extract(seconds from to_timestamp(wait_time, 'mi:ss')::time) as dur,
																	wait_time,
																	local_date,
																	to_timestamp(wait_time, 'mi:ss')::time as wait_time2
																		from 
																			(select 
																				*,
																				max(wait_time) over (partition by ticket_id) as max_wait
																			from 
																				zendesk_call_history zch 
																			where 
																				client_account = 'urbanstems'
																				) mx
																	where max_wait = wait_time 
																	) dur ) dur1 on dur1.ticket_id = zct.ticket_id 
												where 
													client_account = 'urbanstems'
													and via_source_rel <> 'outbound'
													and (zct.status = 'solved' or zct.status = 'closed')
													) zd
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
									where 
										local_date_created >= '2022-04-04') raw_ ) final_ ) final_2
	where (v_weekly is not null and v_weekly::numeric > 0) and agent_name is not null
order by date(date_trunc('week',local_date_created)) desc, agent_name)

union all 

(select 
	*
		from
			(select 
				distinct 
				client_account,
				'monthly' as period,
				date(date_trunc('month',local_date_created)) as local_date_created,
				'voice' as channel,
				assignee_id::varchar as assignee_id,
				email_address,
				agent_name,
				supervisor,
				'awt_d' as kpi_metric,
				monthly as score,
				(select target from sd_performance_target 
					where client_account = 'urbanstems' 
					and channel = 'call' 
					and kpi='wait_time' 
					and (date(date_trunc('month',local_date_created)) between start_date and end_date)) as target,
				case 
						when monthly::numeric <= (select ceiling from sd_kpi_metrics
										where client_account = 'urbanstems'
										and channel = 'call' and kpi='response_time' and rating = '5') then 5
						when 	(monthly::numeric >= (select base from sd_kpi_metrics
								where client_account = 'urbanstems'
								and channel = 'call' and kpi='response_time' and rating = '4')
							and
								monthly::numeric <= (select ceiling from sd_kpi_metrics
								where client_account = 'urbanstems'
								and channel = 'call' and kpi='response_time' and rating = '4'))
						then 4
						when 	(monthly::numeric >= (select base from sd_kpi_metrics
								where client_account = 'urbanstems'
								and channel = 'call' and kpi='response_time' and rating = '3')
							and
								monthly::numeric <= (select ceiling from sd_kpi_metrics
								where client_account = 'urbanstems'
								and channel = 'call' and kpi='response_time' and rating = '3'))
						then 3	
						when 	(monthly::numeric >= (select base from sd_kpi_metrics
								where client_account = 'urbanstems'
								and channel = 'call' and kpi='response_time' and rating = '2')
							and
								monthly::numeric <= (select ceiling from sd_kpi_metrics
								where client_account = 'urbanstems'
								and channel = 'call' and kpi='response_time' and rating = '2'))
						then 2	
						when monthly::numeric >=	(select base from sd_kpi_metrics
										where client_account = 'urbanstems'
										and channel = 'call' and kpi='response_time' and rating = '1') then 1		
					end as rating,
				(select weight from sd_kpi_weights
				where client_account = 'urbanstems'
				and channel = 'call' and (date(date_trunc('month',local_date_created)) between start_date::date and end_date::date)) as weight
					from 
						(select 
							subteam as client_account,
							local_date_created,
							assignee_id,
							email_address,
							agent_name,
							supervisor,
							dur,
							round(avg(dur) over (partition by agent_name, local_date_created, hour_))::varchar as hourly,
							round(avg(dur) over (partition by local_date_created, hour_, subteam))::varchar as t_hourly,
							round(avg(dur) over (partition by supervisor, subteam, local_date_created, hour_))::varchar as v_hourly,
							round(avg(dur) over (partition by agent_name, local_date_created))::varchar as daily,
							round(avg(dur) over (partition by local_date_created,subteam))::varchar as t_daily,
							round(avg(dur) over (partition by supervisor, local_date_created, subteam))::varchar as v_daily,
							round(avg(dur) over (partition by agent_name, date(date_trunc('week',local_date_created))))::varchar as weekly,
							round(avg(dur) over (partition by subteam, date(date_trunc('week',local_date_created))))::varchar as t_weekly,
							round(avg(dur) over (partition by supervisor, subteam, date(date_trunc('week',local_date_created))))::varchar as v_weekly,
							round(avg(dur) over (partition by agent_name, date(date_trunc('month',local_date_created))))::varchar as monthly,
							round(avg(dur) over (partition by subteam, date(date_trunc('month',local_date_created))))::varchar as t_monthly,
							round(avg(dur) over (partition by supervisor, subteam, date(date_trunc('month',local_date_created))))::varchar as v_monthly
								from
									(select 
										distinct 
										subteam,
										ticket_id,
										local_date_created,
										hour_,
										assignee_id,
										agent_email as email_address,
										agent_name,
										case when supervisor is null then 'Urbanstems Flex' else supervisor end as supervisor,
										dur
											from 
												(select 
													distinct 
													subteam,
													local_date_created,
													hour_,
													ticket_id,
													assignee_id::varchar as assignee_id,
													agent_email,
													agent_name,
													case when supervisor is null then job_supervisor else supervisor end as supervisor,
													dur
														from
															(select 
																distinct
																zct.ticket_id,
																dur,
																channel,
																local_date_created,
																extract(hour from local_time_created::time) as hour_,
																assignee_id,
																via_source_rel,
																zct.status
															from 
																zendesk_call_tickets zct 
															left join 
																(select 
																	distinct 
																	ticket_id,
																	status,
																	dur
																		from
																			(select 
																				distinct
																				ticket_id,
																				forwarded_to,
																				status,
																				extract(minutes from to_timestamp(wait_time, 'mi:ss')::time)*60 + extract(seconds from to_timestamp(wait_time, 'mi:ss')::time) as dur,
																				wait_time,
																				local_date,
																				to_timestamp(wait_time, 'mi:ss')::time as wait_time2
																					from 
																						(select 
																							*,
																							max(wait_time) over (partition by ticket_id) as max_wait
																						from 
																							zendesk_call_history zch 
																						where 
																							client_account = 'urbanstems'
																							) mx
																				where max_wait = wait_time 
																				) dur ) dur1 on dur1.ticket_id = zct.ticket_id 
															where 
																client_account = 'urbanstems'
																and via_source_rel <> 'outbound'
																and (zct.status = 'solved' or zct.status = 'closed')
																) zd
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
												where 
													local_date_created >= '2022-04-04') raw_ ) final_ ) final_2
				where (monthly is not null and monthly::numeric > 0 ) and agent_name is not null ) month_
	where weight is not null 
order by date(date_trunc('month',local_date_created)) desc, agent_name)

union all 

(select 
	*
		from
			(select 
				distinct 
				client_account,
				'monthly' as period,
				date(date_trunc('month',local_date_created)) as local_date_created,
				'voice' as channel,
				'' as assignee_id,
				'' as email_address,
				'Team Total' as agent_name,
				'Team Total' as supervisor,
				'awt_d' as kpi_metric,
				t_monthly as score,
				(select target from sd_performance_target 
					where client_account = 'urbanstems' 
					and channel = 'call' 
					and kpi='wait_time' 
					and (date(date_trunc('month',local_date_created)) between start_date and end_date)) as target,
				case 
						when t_monthly::numeric <= (select ceiling from sd_kpi_metrics
										where client_account = 'urbanstems'
										and channel = 'call' and kpi='response_time' and rating = '5') then 5
						when 	(t_monthly::numeric >= (select base from sd_kpi_metrics
								where client_account = 'urbanstems'
								and channel = 'call' and kpi='response_time' and rating = '4')
							and
								t_monthly::numeric <= (select ceiling from sd_kpi_metrics
								where client_account = 'urbanstems'
								and channel = 'call' and kpi='response_time' and rating = '4'))
						then 4
						when 	(t_monthly::numeric >= (select base from sd_kpi_metrics
								where client_account = 'urbanstems'
								and channel = 'call' and kpi='response_time' and rating = '3')
							and
								t_monthly::numeric <= (select ceiling from sd_kpi_metrics
								where client_account = 'urbanstems'
								and channel = 'call' and kpi='response_time' and rating = '3'))
						then 3	
						when 	(t_monthly::numeric >= (select base from sd_kpi_metrics
								where client_account = 'urbanstems'
								and channel = 'call' and kpi='response_time' and rating = '2')
							and
								t_monthly::numeric <= (select ceiling from sd_kpi_metrics
								where client_account = 'urbanstems'
								and channel = 'call' and kpi='response_time' and rating = '2'))
						then 2	
						when t_monthly::numeric >=	(select base from sd_kpi_metrics
										where client_account = 'urbanstems'
										and channel = 'call' and kpi='response_time' and rating = '1') then 1		
					end as rating,
				(select weight from sd_kpi_weights
				where client_account = 'urbanstems'
				and channel = 'call' and (date(date_trunc('month',local_date_created)) between start_date::date and end_date::date)) as weight
					from 
						(select 
							subteam as client_account,
							local_date_created,
							assignee_id,
							email_address,
							agent_name,
							supervisor,
							dur,
							round(avg(dur) over (partition by agent_name, local_date_created, hour_))::varchar as hourly,
							round(avg(dur) over (partition by local_date_created, hour_, subteam))::varchar as t_hourly,
							round(avg(dur) over (partition by supervisor, subteam, local_date_created, hour_))::varchar as v_hourly,
							round(avg(dur) over (partition by agent_name, local_date_created))::varchar as daily,
							round(avg(dur) over (partition by local_date_created,subteam))::varchar as t_daily,
							round(avg(dur) over (partition by supervisor, local_date_created, subteam))::varchar as v_daily,
							round(avg(dur) over (partition by agent_name, date(date_trunc('week',local_date_created))))::varchar as weekly,
							round(avg(dur) over (partition by subteam, date(date_trunc('week',local_date_created))))::varchar as t_weekly,
							round(avg(dur) over (partition by supervisor, subteam, date(date_trunc('week',local_date_created))))::varchar as v_weekly,
							round(avg(dur) over (partition by agent_name, date(date_trunc('month',local_date_created))))::varchar as monthly,
							round(avg(dur) over (partition by subteam, date(date_trunc('month',local_date_created))))::varchar as t_monthly,
							round(avg(dur) over (partition by supervisor, subteam, date(date_trunc('month',local_date_created))))::varchar as v_monthly
								from
									(select 
										distinct 
										subteam,
										ticket_id,
										local_date_created,
										hour_,
										assignee_id,
										agent_email as email_address,
										agent_name,
										case when supervisor is null then 'Urbanstems Flex' else supervisor end as supervisor,
										dur
											from 
												(select 
													distinct 
													subteam,
													local_date_created,
													hour_,
													ticket_id,
													assignee_id::varchar as assignee_id,
													agent_email,
													agent_name,
													case when supervisor is null then job_supervisor else supervisor end as supervisor,
													dur
														from
															(select 
																distinct
																zct.ticket_id,
																dur,
																channel,
																local_date_created,
																extract(hour from local_time_created::time) as hour_,
																assignee_id,
																via_source_rel,
																zct.status
															from 
																zendesk_call_tickets zct 
															left join 
																(select 
																	distinct 
																	ticket_id,
																	status,
																	dur
																		from
																			(select 
																				distinct
																				ticket_id,
																				forwarded_to,
																				status,
																				extract(minutes from to_timestamp(wait_time, 'mi:ss')::time)*60 + extract(seconds from to_timestamp(wait_time, 'mi:ss')::time) as dur,
																				wait_time,
																				local_date,
																				to_timestamp(wait_time, 'mi:ss')::time as wait_time2
																					from 
																						(select 
																							*,
																							max(wait_time) over (partition by ticket_id) as max_wait
																						from 
																							zendesk_call_history zch 
																						where 
																							client_account = 'urbanstems'
																							) mx
																				where max_wait = wait_time 
																				) dur ) dur1 on dur1.ticket_id = zct.ticket_id 
															where 
																client_account = 'urbanstems'
																and via_source_rel <> 'outbound'
																and (zct.status = 'solved' or zct.status = 'closed')
																) zd
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
												where 
													local_date_created >= '2022-04-04') raw_ ) final_ ) final_2
				where (t_monthly is not null and t_monthly::numeric > 0 ) and agent_name is not null ) month_
	where weight is not null 
order by date(date_trunc('month',local_date_created)) desc, agent_name)

union all 

(select 
	*
		from
			(select 
				distinct 
				client_account,
				'monthly' as period,
				date(date_trunc('month',local_date_created)) as local_date_created,
				'voice' as channel,
				'' as assignee_id,
				'' as email_address,
				'Team Total' as agent_name,
				supervisor,
				'awt_d' as kpi_metric,
				v_monthly as score,
				(select target from sd_performance_target 
					where client_account = 'urbanstems' 
					and channel = 'call' 
					and kpi='wait_time' 
					and (date(date_trunc('month',local_date_created)) between start_date and end_date)) as target,
				case 
						when v_monthly::numeric <= (select ceiling from sd_kpi_metrics
										where client_account = 'urbanstems'
										and channel = 'call' and kpi='response_time' and rating = '5') then 5
						when 	(v_monthly::numeric >= (select base from sd_kpi_metrics
								where client_account = 'urbanstems'
								and channel = 'call' and kpi='response_time' and rating = '4')
							and
								v_monthly::numeric <= (select ceiling from sd_kpi_metrics
								where client_account = 'urbanstems'
								and channel = 'call' and kpi='response_time' and rating = '4'))
						then 4
						when 	(v_monthly::numeric >= (select base from sd_kpi_metrics
								where client_account = 'urbanstems'
								and channel = 'call' and kpi='response_time' and rating = '3')
							and
								v_monthly::numeric <= (select ceiling from sd_kpi_metrics
								where client_account = 'urbanstems'
								and channel = 'call' and kpi='response_time' and rating = '3'))
						then 3	
						when 	(v_monthly::numeric >= (select base from sd_kpi_metrics
								where client_account = 'urbanstems'
								and channel = 'call' and kpi='response_time' and rating = '2')
							and
								v_monthly::numeric <= (select ceiling from sd_kpi_metrics
								where client_account = 'urbanstems'
								and channel = 'call' and kpi='response_time' and rating = '2'))
						then 2	
						when v_monthly::numeric >=	(select base from sd_kpi_metrics
										where client_account = 'urbanstems'
										and channel = 'call' and kpi='response_time' and rating = '1') then 1		
					end as rating,
				(select weight from sd_kpi_weights
				where client_account = 'urbanstems'
				and channel = 'call' and (date(date_trunc('month',local_date_created)) between start_date::date and end_date::date)) as weight
					from 
						(select 
							subteam as client_account,
							local_date_created,
							assignee_id,
							email_address,
							agent_name,
							supervisor,
							dur,
							round(avg(dur) over (partition by agent_name, local_date_created, hour_))::varchar as hourly,
							round(avg(dur) over (partition by local_date_created, hour_, subteam))::varchar as t_hourly,
							round(avg(dur) over (partition by supervisor, subteam, local_date_created, hour_))::varchar as v_hourly,
							round(avg(dur) over (partition by agent_name, local_date_created))::varchar as daily,
							round(avg(dur) over (partition by local_date_created,subteam))::varchar as t_daily,
							round(avg(dur) over (partition by supervisor, local_date_created, subteam))::varchar as v_daily,
							round(avg(dur) over (partition by agent_name, date(date_trunc('week',local_date_created))))::varchar as weekly,
							round(avg(dur) over (partition by subteam, date(date_trunc('week',local_date_created))))::varchar as t_weekly,
							round(avg(dur) over (partition by supervisor, subteam, date(date_trunc('week',local_date_created))))::varchar as v_weekly,
							round(avg(dur) over (partition by agent_name, date(date_trunc('month',local_date_created))))::varchar as monthly,
							round(avg(dur) over (partition by subteam, date(date_trunc('month',local_date_created))))::varchar as t_monthly,
							round(avg(dur) over (partition by supervisor, subteam, date(date_trunc('month',local_date_created))))::varchar as v_monthly
								from
									(select 
										distinct 
										subteam,
										ticket_id,
										local_date_created,
										hour_,
										assignee_id,
										agent_email as email_address,
										agent_name,
										case when supervisor is null then 'Urbanstems Flex' else supervisor end as supervisor,
										dur
											from 
												(select 
													distinct 
													subteam,
													local_date_created,
													hour_,
													ticket_id,
													assignee_id::varchar as assignee_id,
													agent_email,
													agent_name,
													case when supervisor is null then job_supervisor else supervisor end as supervisor,
													dur
														from
															(select 
																distinct
																zct.ticket_id,
																dur,
																channel,
																local_date_created,
																extract(hour from local_time_created::time) as hour_,
																assignee_id,
																via_source_rel,
																zct.status
															from 
																zendesk_call_tickets zct 
															left join 
																(select 
																	distinct 
																	ticket_id,
																	status,
																	dur
																		from
																			(select 
																				distinct
																				ticket_id,
																				forwarded_to,
																				status,
																				extract(minutes from to_timestamp(wait_time, 'mi:ss')::time)*60 + extract(seconds from to_timestamp(wait_time, 'mi:ss')::time) as dur,
																				wait_time,
																				local_date,
																				to_timestamp(wait_time, 'mi:ss')::time as wait_time2
																					from 
																						(select 
																							*,
																							max(wait_time) over (partition by ticket_id) as max_wait
																						from 
																							zendesk_call_history zch 
																						where 
																							client_account = 'urbanstems'
																							) mx
																				where max_wait = wait_time 
																				) dur ) dur1 on dur1.ticket_id = zct.ticket_id 
															where 
																client_account = 'urbanstems'
																and via_source_rel <> 'outbound'
																and (zct.status = 'solved' or zct.status = 'closed')
																) zd
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
												where 
													local_date_created >= '2022-04-04') raw_ ) final_ ) final_2
				where (v_monthly is not null and v_monthly::numeric > 0 ) and agent_name is not null ) month_
	where weight is not null 
order by date(date_trunc('month',local_date_created)) desc, agent_name)
