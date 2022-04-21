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
	daily::numeric as score,
	(select target from sd_performance_target 
		where client_account = 'urbanstems' 
		and channel = 'email' 
		and kpi='response_time' 
		and (local_date_created between start_date and end_date)) as target,
	case 
			when daily::numeric <= (select ceiling from sd_kpi_metrics
							where client_account = 'urbanstems'
							and channel = 'email' and kpi='response_time' and rating = '5') then 5
			when 	(daily::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'email' and kpi='response_time' and rating = '4')
				and
					daily::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'email' and kpi='response_time' and rating = '4'))
			then 4
			when 	(daily::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'email' and kpi='response_time' and rating = '3')
				and
					daily::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'email' and kpi='response_time' and rating = '3'))
			then 3	
			when 	(daily::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'email' and kpi='response_time' and rating = '2')
				and
					daily::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'email' and kpi='response_time' and rating = '2'))
			then 2	
			when daily::numeric >=	(select base from sd_kpi_metrics
							where client_account = 'urbanstems'
							and channel = 'email' and kpi='response_time' and rating = '1') then 1		
		end as rating,
	(select weight from sd_kpi_weights
	where client_account = 'urbanstems'
	and channel = 'email' and (local_date_created::date between start_date::date and end_date::date)) as weight
		from 
			(select 
				subteam,
				ticket_id,
				local_date_created::date as local_date_created,
				channel,
				assignee_id,
				email_address,
				agent_name,
				supervisor,
				frt_calendar::varchar,
				round(avg(frt_calendar) over (partition by agent_name, local_date_created, hour_))::varchar as hourly,
				round(avg(frt_calendar) over (partition by local_date_created, hour_, subteam))::varchar as t_hourly,
				round(avg(frt_calendar) over (partition by supervisor, subteam, local_date_created, hour_))::varchar as v_hourly,
				round(avg(frt_calendar) over (partition by agent_name, local_date_created))::varchar as daily,
				round(avg(frt_calendar) over (partition by local_date_created,subteam))::varchar as t_daily,
				round(avg(frt_calendar) over (partition by supervisor, local_date_created, subteam))::varchar as v_daily,
				round(avg(frt_calendar) over (partition by agent_name, date(date_trunc('week',local_date_created))))::varchar as weekly,
				round(avg(frt_calendar) over (partition by subteam, date(date_trunc('week',local_date_created))))::varchar as t_weekly,
				round(avg(frt_calendar) over (partition by supervisor, subteam, date(date_trunc('week',local_date_created))))::varchar as v_weekly,
				round(avg(frt_calendar) over (partition by agent_name, date(date_trunc('month',local_date_created))))::varchar as monthly,
				round(avg(frt_calendar) over (partition by subteam, date(date_trunc('month',local_date_created))))::varchar as t_monthly,
				round(avg(frt_calendar) over (partition by supervisor, subteam, date(date_trunc('month',local_date_created))))::varchar as v_monthly
			from (
				select 
					distinct
					subteam,
					ticket_id::varchar,
					local_date_created,
					hour_,
					channel,
					assignee_id,
					email_address,
					agent_name,
					case when supervisor is null then 'Urbanstems Flex' else supervisor end as supervisor,
					frt_calendar	
				from (
				select 
					distinct 
					subteam,
					ticket_id,
					local_date_created,
					extract(hour from local_time_created::time) as hour_,
					case when supervisor is null then job_supervisor else supervisor end as supervisor,
					date_updated,
					date_updated - local_date_created as recent_,
					max(date_updated - local_date_created) over (partition by ticket_id) as max_recent, 
					_id as assignee_id,
					agent_email as email_address,
					agent_name,
					frt_calendar,
					zen_tickets.channel
				from  
					zendesk_email_tickets
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
					(select ticket_id as _id_, reply_time_calendar_minutes as frt_calendar, 'email' as channel from zendesk_email_metrics) zen_tickets on _id_::varchar = ticket_id::varchar
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
					client_account = 'urbanstems'
					and (status = 'closed' or status = 'solved')
					and zendesk_email_tickets.channel = 'email'
				order by date_updated 
				) b
				where 
					recent_ = max_recent
					and agent_name is not null 
					) raw ) final_raw
	where daily is not null
	order by local_date_created desc, agent_name )
	
union all 

(select 
	distinct 
	subteam as client_account,
	'daily' as period,
	local_date_created,
	channel,
	'' as assignee_id,
	'' as email_address,
	'Team Total' as agent_name,
	'Team Total' as supervisor,
	'frt_d' as kpi_metric,
	t_daily::numeric as score,
	(select target from sd_performance_target 
		where client_account = 'urbanstems' 
		and channel = 'email' 
		and kpi='response_time' 
		and (local_date_created between start_date and end_date)) as target,
	case 
			when t_daily::numeric <= (select ceiling from sd_kpi_metrics
							where client_account = 'urbanstems'
							and channel = 'email' and kpi='response_time' and rating = '5') then 5
			when 	(t_daily::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'email' and kpi='response_time' and rating = '4')
				and
					t_daily::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'email' and kpi='response_time' and rating = '4'))
			then 4
			when 	(t_daily::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'email' and kpi='response_time' and rating = '3')
				and
					t_daily::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'email' and kpi='response_time' and rating = '3'))
			then 3	
			when 	(t_daily::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'email' and kpi='response_time' and rating = '2')
				and
					t_daily::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'email' and kpi='response_time' and rating = '2'))
			then 2	
			when t_daily::numeric >=	(select base from sd_kpi_metrics
							where client_account = 'urbanstems'
							and channel = 'email' and kpi='response_time' and rating = '1') then 1		
		end as rating,
	(select weight from sd_kpi_weights
	where client_account = 'urbanstems'
	and channel = 'email' and (local_date_created::date between start_date::date and end_date::date)) as weight
		from 
			(select 
				subteam,
				ticket_id,
				local_date_created::date as local_date_created,
				channel,
				assignee_id,
				email_address,
				agent_name,
				supervisor,
				frt_calendar::varchar,
				round(avg(frt_calendar) over (partition by agent_name, local_date_created, hour_))::varchar as hourly,
				round(avg(frt_calendar) over (partition by local_date_created, hour_, subteam))::varchar as t_hourly,
				round(avg(frt_calendar) over (partition by supervisor, subteam, local_date_created, hour_))::varchar as v_hourly,
				round(avg(frt_calendar) over (partition by agent_name, local_date_created))::varchar as daily,
				round(avg(frt_calendar) over (partition by local_date_created,subteam))::varchar as t_daily,
				round(avg(frt_calendar) over (partition by supervisor, local_date_created, subteam))::varchar as v_daily,
				round(avg(frt_calendar) over (partition by agent_name, date(date_trunc('week',local_date_created))))::varchar as weekly,
				round(avg(frt_calendar) over (partition by subteam, date(date_trunc('week',local_date_created))))::varchar as t_weekly,
				round(avg(frt_calendar) over (partition by supervisor, subteam, date(date_trunc('week',local_date_created))))::varchar as v_weekly,
				round(avg(frt_calendar) over (partition by agent_name, date(date_trunc('month',local_date_created))))::varchar as monthly,
				round(avg(frt_calendar) over (partition by subteam, date(date_trunc('month',local_date_created))))::varchar as t_monthly,
				round(avg(frt_calendar) over (partition by supervisor, subteam, date(date_trunc('month',local_date_created))))::varchar as v_monthly
			from (
				select 
					distinct
					subteam,
					ticket_id::varchar,
					local_date_created,
					hour_,
					channel,
					assignee_id,
					email_address,
					agent_name,
					case when supervisor is null then 'Urbanstems Flex' else supervisor end as supervisor,
					frt_calendar	
				from (
				select 
					distinct 
					subteam,
					ticket_id,
					local_date_created,
					extract(hour from local_time_created::time) as hour_,
					case when supervisor is null then job_supervisor else supervisor end as supervisor,
					date_updated,
					date_updated - local_date_created as recent_,
					max(date_updated - local_date_created) over (partition by ticket_id) as max_recent, 
					_id as assignee_id,
					agent_email as email_address,
					agent_name,
					frt_calendar,
					zen_tickets.channel
				from  
					zendesk_email_tickets
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
					(select ticket_id as _id_, reply_time_calendar_minutes as frt_calendar, 'email' as channel from zendesk_email_metrics) zen_tickets on _id_::varchar = ticket_id::varchar
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
					client_account = 'urbanstems'
					and (status = 'closed' or status = 'solved')
					and zendesk_email_tickets.channel = 'email'
				order by date_updated 
				) b
				where 
					recent_ = max_recent
					and agent_name is not null 
					) raw ) final_raw
	where t_daily is not null
	order by local_date_created desc, agent_name )

union all 

(select 
	distinct 
	subteam as client_account,
	'daily' as period,
	local_date_created,
	channel,
	'' as assignee_id,
	'' as email_address,
	'Team Total' as agent_name,
	supervisor,
	'frt_d' as kpi_metric,
	v_daily::numeric as score,
	(select target from sd_performance_target 
		where client_account = 'urbanstems' 
		and channel = 'email' 
		and kpi='response_time' 
		and (local_date_created between start_date and end_date)) as target,
	case 
			when v_daily::numeric <= (select ceiling from sd_kpi_metrics
							where client_account = 'urbanstems'
							and channel = 'email' and kpi='response_time' and rating = '5') then 5
			when 	(v_daily::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'email' and kpi='response_time' and rating = '4')
				and
					v_daily::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'email' and kpi='response_time' and rating = '4'))
			then 4
			when 	(v_daily::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'email' and kpi='response_time' and rating = '3')
				and
					v_daily::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'email' and kpi='response_time' and rating = '3'))
			then 3	
			when 	(v_daily::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'email' and kpi='response_time' and rating = '2')
				and
					v_daily::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'email' and kpi='response_time' and rating = '2'))
			then 2	
			when v_daily::numeric >=	(select base from sd_kpi_metrics
							where client_account = 'urbanstems'
							and channel = 'email' and kpi='response_time' and rating = '1') then 1		
		end as rating,
	(select weight from sd_kpi_weights
	where client_account = 'urbanstems'
	and channel = 'email' and (local_date_created::date between start_date::date and end_date::date)) as weight
		from 
			(select 
				subteam,
				ticket_id,
				local_date_created::date as local_date_created,
				channel,
				assignee_id,
				email_address,
				agent_name,
				supervisor,
				frt_calendar::varchar,
				round(avg(frt_calendar) over (partition by agent_name, local_date_created, hour_))::varchar as hourly,
				round(avg(frt_calendar) over (partition by local_date_created, hour_, subteam))::varchar as t_hourly,
				round(avg(frt_calendar) over (partition by supervisor, subteam, local_date_created, hour_))::varchar as v_hourly,
				round(avg(frt_calendar) over (partition by agent_name, local_date_created))::varchar as daily,
				round(avg(frt_calendar) over (partition by local_date_created,subteam))::varchar as t_daily,
				round(avg(frt_calendar) over (partition by supervisor, local_date_created, subteam))::varchar as v_daily,
				round(avg(frt_calendar) over (partition by agent_name, date(date_trunc('week',local_date_created))))::varchar as weekly,
				round(avg(frt_calendar) over (partition by subteam, date(date_trunc('week',local_date_created))))::varchar as t_weekly,
				round(avg(frt_calendar) over (partition by supervisor, subteam, date(date_trunc('week',local_date_created))))::varchar as v_weekly,
				round(avg(frt_calendar) over (partition by agent_name, date(date_trunc('month',local_date_created))))::varchar as monthly,
				round(avg(frt_calendar) over (partition by subteam, date(date_trunc('month',local_date_created))))::varchar as t_monthly,
				round(avg(frt_calendar) over (partition by supervisor, subteam, date(date_trunc('month',local_date_created))))::varchar as v_monthly
			from (
				select 
					distinct
					subteam,
					ticket_id::varchar,
					local_date_created,
					hour_,
					channel,
					assignee_id,
					email_address,
					agent_name,
					case when supervisor is null then 'Urbanstems Flex' else supervisor end as supervisor,
					frt_calendar	
				from (
				select 
					distinct 
					subteam,
					ticket_id,
					local_date_created,
					extract(hour from local_time_created::time) as hour_,
					case when supervisor is null then job_supervisor else supervisor end as supervisor,
					date_updated,
					date_updated - local_date_created as recent_,
					max(date_updated - local_date_created) over (partition by ticket_id) as max_recent, 
					_id as assignee_id,
					agent_email as email_address,
					agent_name,
					frt_calendar,
					zen_tickets.channel
				from  
					zendesk_email_tickets
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
					(select ticket_id as _id_, reply_time_calendar_minutes as frt_calendar, 'email' as channel from zendesk_email_metrics) zen_tickets on _id_::varchar = ticket_id::varchar
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
					client_account = 'urbanstems'
					and (status = 'closed' or status = 'solved')
					and zendesk_email_tickets.channel = 'email'
				order by date_updated 
				) b
				where 
					recent_ = max_recent
					and agent_name is not null 
					) raw ) final_raw
	where v_daily is not null
	order by local_date_created desc, agent_name )
	
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
	weekly::numeric as score,
	(select target from sd_performance_target 
		where client_account = 'urbanstems' 
		and channel = 'email' 
		and kpi='response_time' 
		and (local_date_created between start_date and end_date)) as target,
	case 
			when weekly::numeric <= (select ceiling from sd_kpi_metrics
							where client_account = 'urbanstems'
							and channel = 'email' and kpi='response_time' and rating = '5') then 5
			when 	(weekly::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'email' and kpi='response_time' and rating = '4')
				and
					weekly::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'email' and kpi='response_time' and rating = '4'))
			then 4
			when 	(weekly::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'email' and kpi='response_time' and rating = '3')
				and
					weekly::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'email' and kpi='response_time' and rating = '3'))
			then 3	
			when 	(weekly::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'email' and kpi='response_time' and rating = '2')
				and
					weekly::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'email' and kpi='response_time' and rating = '2'))
			then 2	
			when weekly::numeric >=	(select base from sd_kpi_metrics
							where client_account = 'urbanstems'
							and channel = 'email' and kpi='response_time' and rating = '1') then 1		
		end as rating,
	(select weight from sd_kpi_weights
	where client_account = 'urbanstems'
	and channel = 'email' and (local_date_created::date between start_date::date and end_date::date)) as weight
		from 
			(select 
				subteam,
				ticket_id,
				local_date_created::date as local_date_created,
				channel,
				assignee_id,
				email_address,
				agent_name,
				supervisor,
				frt_calendar::varchar,
				round(avg(frt_calendar) over (partition by agent_name, local_date_created, hour_))::varchar as hourly,
				round(avg(frt_calendar) over (partition by local_date_created, hour_, subteam))::varchar as t_hourly,
				round(avg(frt_calendar) over (partition by supervisor, subteam, local_date_created, hour_))::varchar as v_hourly,
				round(avg(frt_calendar) over (partition by agent_name, local_date_created))::varchar as daily,
				round(avg(frt_calendar) over (partition by local_date_created,subteam))::varchar as t_daily,
				round(avg(frt_calendar) over (partition by supervisor, local_date_created, subteam))::varchar as v_daily,
				round(avg(frt_calendar) over (partition by agent_name, date(date_trunc('week',local_date_created))))::varchar as weekly,
				round(avg(frt_calendar) over (partition by subteam, date(date_trunc('week',local_date_created))))::varchar as t_weekly,
				round(avg(frt_calendar) over (partition by supervisor, subteam, date(date_trunc('week',local_date_created))))::varchar as v_weekly,
				round(avg(frt_calendar) over (partition by agent_name, date(date_trunc('month',local_date_created))))::varchar as monthly,
				round(avg(frt_calendar) over (partition by subteam, date(date_trunc('month',local_date_created))))::varchar as t_monthly,
				round(avg(frt_calendar) over (partition by supervisor, subteam, date(date_trunc('month',local_date_created))))::varchar as v_monthly
			from (
				select 
					distinct
					subteam,
					ticket_id::varchar,
					local_date_created,
					hour_,
					channel,
					assignee_id,
					email_address,
					agent_name,
					case when supervisor is null then 'Urbanstems Flex' else supervisor end as supervisor,
					frt_calendar	
				from (
				select 
					distinct 
					subteam,
					ticket_id,
					local_date_created,
					extract(hour from local_time_created::time) as hour_,
					case when supervisor is null then job_supervisor else supervisor end as supervisor,
					date_updated,
					date_updated - local_date_created as recent_,
					max(date_updated - local_date_created) over (partition by ticket_id) as max_recent, 
					_id as assignee_id,
					agent_email as email_address,
					agent_name,
					frt_calendar,
					zen_tickets.channel
				from  
					zendesk_email_tickets
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
					(select ticket_id as _id_, reply_time_calendar_minutes as frt_calendar, 'email' as channel from zendesk_email_metrics) zen_tickets on _id_::varchar = ticket_id::varchar
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
					client_account = 'urbanstems'
					and (status = 'closed' or status = 'solved')
					and zendesk_email_tickets.channel = 'email'
				order by date_updated 
				) b
				where 
					recent_ = max_recent
					and agent_name is not null 
					) raw ) final_raw
	where weekly is not null
	order by date(date_trunc('week',local_date_created)) desc, agent_name )

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
	t_weekly::numeric as score,
	(select target from sd_performance_target 
		where client_account = 'urbanstems' 
		and channel = 'email' 
		and kpi='response_time' 
		and (local_date_created between start_date and end_date)) as target,
	case 
			when t_weekly::numeric <= (select ceiling from sd_kpi_metrics
							where client_account = 'urbanstems'
							and channel = 'email' and kpi='response_time' and rating = '5') then 5
			when 	(t_weekly::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'email' and kpi='response_time' and rating = '4')
				and
					t_weekly::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'email' and kpi='response_time' and rating = '4'))
			then 4
			when 	(t_weekly::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'email' and kpi='response_time' and rating = '3')
				and
					t_weekly::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'email' and kpi='response_time' and rating = '3'))
			then 3	
			when 	(t_weekly::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'email' and kpi='response_time' and rating = '2')
				and
					t_weekly::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'email' and kpi='response_time' and rating = '2'))
			then 2	
			when t_weekly::numeric >=	(select base from sd_kpi_metrics
							where client_account = 'urbanstems'
							and channel = 'email' and kpi='response_time' and rating = '1') then 1		
		end as rating,
	(select weight from sd_kpi_weights
	where client_account = 'urbanstems'
	and channel = 'email' and (local_date_created::date between start_date::date and end_date::date)) as weight
		from 
			(select 
				subteam,
				ticket_id,
				local_date_created::date as local_date_created,
				channel,
				assignee_id,
				email_address,
				agent_name,
				supervisor,
				frt_calendar::varchar,
				round(avg(frt_calendar) over (partition by agent_name, local_date_created, hour_))::varchar as hourly,
				round(avg(frt_calendar) over (partition by local_date_created, hour_, subteam))::varchar as t_hourly,
				round(avg(frt_calendar) over (partition by supervisor, subteam, local_date_created, hour_))::varchar as v_hourly,
				round(avg(frt_calendar) over (partition by agent_name, local_date_created))::varchar as daily,
				round(avg(frt_calendar) over (partition by local_date_created,subteam))::varchar as t_daily,
				round(avg(frt_calendar) over (partition by supervisor, local_date_created, subteam))::varchar as v_daily,
				round(avg(frt_calendar) over (partition by agent_name, date(date_trunc('week',local_date_created))))::varchar as weekly,
				round(avg(frt_calendar) over (partition by subteam, date(date_trunc('week',local_date_created))))::varchar as t_weekly,
				round(avg(frt_calendar) over (partition by supervisor, subteam, date(date_trunc('week',local_date_created))))::varchar as v_weekly,
				round(avg(frt_calendar) over (partition by agent_name, date(date_trunc('month',local_date_created))))::varchar as monthly,
				round(avg(frt_calendar) over (partition by subteam, date(date_trunc('month',local_date_created))))::varchar as t_monthly,
				round(avg(frt_calendar) over (partition by supervisor, subteam, date(date_trunc('month',local_date_created))))::varchar as v_monthly
			from (
				select 
					distinct
					subteam,
					ticket_id::varchar,
					local_date_created,
					hour_,
					channel,
					assignee_id,
					email_address,
					agent_name,
					case when supervisor is null then 'Urbanstems Flex' else supervisor end as supervisor,
					frt_calendar	
				from (
				select 
					distinct 
					subteam,
					ticket_id,
					local_date_created,
					extract(hour from local_time_created::time) as hour_,
					case when supervisor is null then job_supervisor else supervisor end as supervisor,
					date_updated,
					date_updated - local_date_created as recent_,
					max(date_updated - local_date_created) over (partition by ticket_id) as max_recent, 
					_id as assignee_id,
					agent_email as email_address,
					agent_name,
					frt_calendar,
					zen_tickets.channel
				from  
					zendesk_email_tickets
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
					(select ticket_id as _id_, reply_time_calendar_minutes as frt_calendar, 'email' as channel from zendesk_email_metrics) zen_tickets on _id_::varchar = ticket_id::varchar
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
					client_account = 'urbanstems'
					and (status = 'closed' or status = 'solved')
					and zendesk_email_tickets.channel = 'email'
				order by date_updated 
				) b
				where 
					recent_ = max_recent
					and agent_name is not null 
					) raw ) final_raw
	where t_weekly is not null
	order by date(date_trunc('week',local_date_created)) desc, agent_name )

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
	v_weekly::numeric as score,
	(select target from sd_performance_target 
		where client_account = 'urbanstems' 
		and channel = 'email' 
		and kpi='response_time' 
		and (local_date_created between start_date and end_date)) as target,
	case 
			when v_weekly::numeric <= (select ceiling from sd_kpi_metrics
							where client_account = 'urbanstems'
							and channel = 'email' and kpi='response_time' and rating = '5') then 5
			when 	(v_weekly::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'email' and kpi='response_time' and rating = '4')
				and
					v_weekly::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'email' and kpi='response_time' and rating = '4'))
			then 4
			when 	(v_weekly::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'email' and kpi='response_time' and rating = '3')
				and
					v_weekly::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'email' and kpi='response_time' and rating = '3'))
			then 3	
			when 	(v_weekly::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'email' and kpi='response_time' and rating = '2')
				and
					v_weekly::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'email' and kpi='response_time' and rating = '2'))
			then 2	
			when v_weekly::numeric >=	(select base from sd_kpi_metrics
							where client_account = 'urbanstems'
							and channel = 'email' and kpi='response_time' and rating = '1') then 1		
		end as rating,
	(select weight from sd_kpi_weights
	where client_account = 'urbanstems'
	and channel = 'email' and (local_date_created::date between start_date::date and end_date::date)) as weight
		from 
			(select 
				subteam,
				ticket_id,
				local_date_created::date as local_date_created,
				channel,
				assignee_id,
				email_address,
				agent_name,
				supervisor,
				frt_calendar::varchar,
				round(avg(frt_calendar) over (partition by agent_name, local_date_created, hour_))::varchar as hourly,
				round(avg(frt_calendar) over (partition by local_date_created, hour_, subteam))::varchar as t_hourly,
				round(avg(frt_calendar) over (partition by supervisor, subteam, local_date_created, hour_))::varchar as v_hourly,
				round(avg(frt_calendar) over (partition by agent_name, local_date_created))::varchar as daily,
				round(avg(frt_calendar) over (partition by local_date_created,subteam))::varchar as t_daily,
				round(avg(frt_calendar) over (partition by supervisor, local_date_created, subteam))::varchar as v_daily,
				round(avg(frt_calendar) over (partition by agent_name, date(date_trunc('week',local_date_created))))::varchar as weekly,
				round(avg(frt_calendar) over (partition by subteam, date(date_trunc('week',local_date_created))))::varchar as t_weekly,
				round(avg(frt_calendar) over (partition by supervisor, subteam, date(date_trunc('week',local_date_created))))::varchar as v_weekly,
				round(avg(frt_calendar) over (partition by agent_name, date(date_trunc('month',local_date_created))))::varchar as monthly,
				round(avg(frt_calendar) over (partition by subteam, date(date_trunc('month',local_date_created))))::varchar as t_monthly,
				round(avg(frt_calendar) over (partition by supervisor, subteam, date(date_trunc('month',local_date_created))))::varchar as v_monthly
			from (
				select 
					distinct
					subteam,
					ticket_id::varchar,
					local_date_created,
					hour_,
					channel,
					assignee_id,
					email_address,
					agent_name,
					case when supervisor is null then 'Urbanstems Flex' else supervisor end as supervisor,
					frt_calendar	
				from (
				select 
					distinct 
					subteam,
					ticket_id,
					local_date_created,
					extract(hour from local_time_created::time) as hour_,
					case when supervisor is null then job_supervisor else supervisor end as supervisor,
					date_updated,
					date_updated - local_date_created as recent_,
					max(date_updated - local_date_created) over (partition by ticket_id) as max_recent, 
					_id as assignee_id,
					agent_email as email_address,
					agent_name,
					frt_calendar,
					zen_tickets.channel
				from  
					zendesk_email_tickets
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
					(select ticket_id as _id_, reply_time_calendar_minutes as frt_calendar, 'email' as channel from zendesk_email_metrics) zen_tickets on _id_::varchar = ticket_id::varchar
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
					client_account = 'urbanstems'
					and (status = 'closed' or status = 'solved')
					and zendesk_email_tickets.channel = 'email'
				order by date_updated 
				) b
				where 
					recent_ = max_recent
					and agent_name is not null 
					) raw ) final_raw
	where v_weekly is not null
	order by date(date_trunc('week',local_date_created)) desc, agent_name )

union all

(select 
	*
		from
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
				monthly::numeric as score,
				(select target from sd_performance_target 
					where client_account = 'urbanstems' 
					and channel = 'email' 
					and kpi='response_time' 
					and (date(date_trunc('month',local_date_created)) between start_date and end_date)) as target,
				case 
						when monthly::numeric <= (select ceiling from sd_kpi_metrics
										where client_account = 'urbanstems'
										and channel = 'email' and kpi='response_time' and rating = '5') then 5
						when 	(monthly::numeric >= (select base from sd_kpi_metrics
								where client_account = 'urbanstems'
								and channel = 'email' and kpi='response_time' and rating = '4')
							and
								monthly::numeric <= (select ceiling from sd_kpi_metrics
								where client_account = 'urbanstems'
								and channel = 'email' and kpi='response_time' and rating = '4'))
						then 4
						when 	(monthly::numeric >= (select base from sd_kpi_metrics
								where client_account = 'urbanstems'
								and channel = 'email' and kpi='response_time' and rating = '3')
							and
								monthly::numeric <= (select ceiling from sd_kpi_metrics
								where client_account = 'urbanstems'
								and channel = 'email' and kpi='response_time' and rating = '3'))
						then 3	
						when 	(monthly::numeric >= (select base from sd_kpi_metrics
								where client_account = 'urbanstems'
								and channel = 'email' and kpi='response_time' and rating = '2')
							and
								monthly::numeric <= (select ceiling from sd_kpi_metrics
								where client_account = 'urbanstems'
								and channel = 'email' and kpi='response_time' and rating = '2'))
						then 2	
						when weekly::numeric >=	(select base from sd_kpi_metrics
										where client_account = 'urbanstems'
										and channel = 'email' and kpi='response_time' and rating = '1') then 1		
					end as rating,
				(select weight from sd_kpi_weights
				where client_account = 'urbanstems'
				and channel = 'email' and (date(date_trunc('month',local_date_created)) between start_date::date and end_date::date)) as weight
					from 
						(select 
							subteam,
							ticket_id,
							local_date_created::date as local_date_created,
							channel,
							assignee_id,
							email_address,
							agent_name,
							supervisor,
							frt_calendar::varchar,
							round(avg(frt_calendar) over (partition by agent_name, local_date_created, hour_))::varchar as hourly,
							round(avg(frt_calendar) over (partition by local_date_created, hour_, subteam))::varchar as t_hourly,
							round(avg(frt_calendar) over (partition by supervisor, subteam, local_date_created, hour_))::varchar as v_hourly,
							round(avg(frt_calendar) over (partition by agent_name, local_date_created))::varchar as daily,
							round(avg(frt_calendar) over (partition by local_date_created,subteam))::varchar as t_daily,
							round(avg(frt_calendar) over (partition by supervisor, local_date_created, subteam))::varchar as v_daily,
							round(avg(frt_calendar) over (partition by agent_name, date(date_trunc('week',local_date_created))))::varchar as weekly,
							round(avg(frt_calendar) over (partition by subteam, date(date_trunc('week',local_date_created))))::varchar as t_weekly,
							round(avg(frt_calendar) over (partition by supervisor, subteam, date(date_trunc('week',local_date_created))))::varchar as v_weekly,
							round(avg(frt_calendar) over (partition by agent_name, date(date_trunc('month',local_date_created))))::varchar as monthly,
							round(avg(frt_calendar) over (partition by subteam, date(date_trunc('month',local_date_created))))::varchar as t_monthly,
							round(avg(frt_calendar) over (partition by supervisor, subteam, date(date_trunc('month',local_date_created))))::varchar as v_monthly
						from (
							select 
								distinct
								subteam,
								ticket_id::varchar,
								local_date_created,
								hour_,
								channel,
								assignee_id,
								email_address,
								agent_name,
								case when supervisor is null then 'Urbanstems Flex' else supervisor end as supervisor,
								frt_calendar	
							from (
							select 
								distinct 
								subteam,
								ticket_id,
								local_date_created,
								extract(hour from local_time_created::time) as hour_,
								case when supervisor is null then job_supervisor else supervisor end as supervisor,
								date_updated,
								date_updated - local_date_created as recent_,
								max(date_updated - local_date_created) over (partition by ticket_id) as max_recent, 
								_id as assignee_id,
								agent_email as email_address,
								agent_name,
								frt_calendar,
								zen_tickets.channel
							from  
								zendesk_email_tickets
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
								(select ticket_id as _id_, reply_time_calendar_minutes as frt_calendar, 'email' as channel from zendesk_email_metrics) zen_tickets on _id_::varchar = ticket_id::varchar
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
								client_account = 'urbanstems'
								and (status = 'closed' or status = 'solved')
								and zendesk_email_tickets.channel = 'email'
							order by date_updated 
							) b
							where 
								recent_ = max_recent
								and agent_name is not null 
								) raw ) final_raw
				where monthly is not null) as monthly 
	where rating is not null
	order by local_date_created desc, agent_name )

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
	t_monthly::numeric as score,
	(select target from sd_performance_target 
		where client_account = 'urbanstems' 
		and channel = 'email' 
		and kpi='response_time' 
		and (date(date_trunc('month',local_date_created)) between start_date and end_date)) as target,
	case 
			when t_monthly::numeric <= (select ceiling from sd_kpi_metrics
							where client_account = 'urbanstems'
							and channel = 'email' and kpi='response_time' and rating = '5') then 5
			when 	(t_monthly::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'email' and kpi='response_time' and rating = '4')
				and
					t_monthly::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'email' and kpi='response_time' and rating = '4'))
			then 4
			when 	(t_monthly::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'email' and kpi='response_time' and rating = '3')
				and
					t_monthly::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'email' and kpi='response_time' and rating = '3'))
			then 3	
			when 	(t_monthly::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'email' and kpi='response_time' and rating = '2')
				and
					t_monthly::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'email' and kpi='response_time' and rating = '2'))
			then 2	
			when t_monthly::numeric >=	(select base from sd_kpi_metrics
							where client_account = 'urbanstems'
							and channel = 'email' and kpi='response_time' and rating = '1') then 1		
		end as rating,
	(select weight from sd_kpi_weights
	where client_account = 'urbanstems'
	and channel = 'email' and (date(date_trunc('month',local_date_created)) between start_date::date and end_date::date)) as weight
		from 
			(select 
				subteam,
				ticket_id,
				local_date_created::date as local_date_created,
				channel,
				assignee_id,
				email_address,
				agent_name,
				supervisor,
				frt_calendar::varchar,
				round(avg(frt_calendar) over (partition by agent_name, local_date_created, hour_))::varchar as hourly,
				round(avg(frt_calendar) over (partition by local_date_created, hour_, subteam))::varchar as t_hourly,
				round(avg(frt_calendar) over (partition by supervisor, subteam, local_date_created, hour_))::varchar as v_hourly,
				round(avg(frt_calendar) over (partition by agent_name, local_date_created))::varchar as daily,
				round(avg(frt_calendar) over (partition by local_date_created,subteam))::varchar as t_daily,
				round(avg(frt_calendar) over (partition by supervisor, local_date_created, subteam))::varchar as v_daily,
				round(avg(frt_calendar) over (partition by agent_name, date(date_trunc('week',local_date_created))))::varchar as weekly,
				round(avg(frt_calendar) over (partition by subteam, date(date_trunc('week',local_date_created))))::varchar as t_weekly,
				round(avg(frt_calendar) over (partition by supervisor, subteam, date(date_trunc('week',local_date_created))))::varchar as v_weekly,
				round(avg(frt_calendar) over (partition by agent_name, date(date_trunc('month',local_date_created))))::varchar as monthly,
				round(avg(frt_calendar) over (partition by subteam, date(date_trunc('month',local_date_created))))::varchar as t_monthly,
				round(avg(frt_calendar) over (partition by supervisor, subteam, date(date_trunc('month',local_date_created))))::varchar as v_monthly
			from (
				select 
					distinct
					subteam,
					ticket_id::varchar,
					local_date_created,
					hour_,
					channel,
					assignee_id,
					email_address,
					agent_name,
					case when supervisor is null then 'Urbanstems Flex' else supervisor end as supervisor,
					frt_calendar	
				from (
				select 
					distinct 
					subteam,
					ticket_id,
					local_date_created,
					extract(hour from local_time_created::time) as hour_,
					case when supervisor is null then job_supervisor else supervisor end as supervisor,
					date_updated,
					date_updated - local_date_created as recent_,
					max(date_updated - local_date_created) over (partition by ticket_id) as max_recent, 
					_id as assignee_id,
					agent_email as email_address,
					agent_name,
					frt_calendar,
					zen_tickets.channel
				from  
					zendesk_email_tickets
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
					(select ticket_id as _id_, reply_time_calendar_minutes as frt_calendar, 'email' as channel from zendesk_email_metrics) zen_tickets on _id_::varchar = ticket_id::varchar
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
					client_account = 'urbanstems'
					and (status = 'closed' or status = 'solved')
					and zendesk_email_tickets.channel = 'email'
				order by date_updated 
				) b
				where 
					recent_ = max_recent
					and agent_name is not null 
					) raw ) final_raw
	where t_monthly is not null
	order by date(date_trunc('month',local_date_created)) desc, agent_name )

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
	v_monthly::numeric as score,
	(select target from sd_performance_target 
		where client_account = 'urbanstems' 
		and channel = 'email' 
		and kpi='response_time' 
		and (date(date_trunc('month',local_date_created)) between start_date and end_date)) as target,
	case 
			when v_monthly::numeric <= (select ceiling from sd_kpi_metrics
							where client_account = 'urbanstems'
							and channel = 'email' and kpi='response_time' and rating = '5') then 5
			when 	(v_monthly::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'email' and kpi='response_time' and rating = '4')
				and
					v_monthly::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'email' and kpi='response_time' and rating = '4'))
			then 4
			when 	(v_monthly::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'email' and kpi='response_time' and rating = '3')
				and
					v_monthly::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'email' and kpi='response_time' and rating = '3'))
			then 3	
			when 	(v_monthly::numeric >= (select base from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'email' and kpi='response_time' and rating = '2')
				and
					v_monthly::numeric <= (select ceiling from sd_kpi_metrics
					where client_account = 'urbanstems'
					and channel = 'email' and kpi='response_time' and rating = '2'))
			then 2	
			when v_monthly::numeric >=	(select base from sd_kpi_metrics
							where client_account = 'urbanstems'
							and channel = 'email' and kpi='response_time' and rating = '1') then 1		
		end as rating,
	(select weight from sd_kpi_weights
	where client_account = 'urbanstems'
	and channel = 'email' and (date(date_trunc('month',local_date_created)) between start_date::date and end_date::date)) as weight
		from 
			(select 
				subteam,
				ticket_id,
				local_date_created::date as local_date_created,
				channel,
				assignee_id,
				email_address,
				agent_name,
				supervisor,
				frt_calendar::varchar,
				round(avg(frt_calendar) over (partition by agent_name, local_date_created, hour_))::varchar as hourly,
				round(avg(frt_calendar) over (partition by local_date_created, hour_, subteam))::varchar as t_hourly,
				round(avg(frt_calendar) over (partition by supervisor, subteam, local_date_created, hour_))::varchar as v_hourly,
				round(avg(frt_calendar) over (partition by agent_name, local_date_created))::varchar as daily,
				round(avg(frt_calendar) over (partition by local_date_created,subteam))::varchar as t_daily,
				round(avg(frt_calendar) over (partition by supervisor, local_date_created, subteam))::varchar as v_daily,
				round(avg(frt_calendar) over (partition by agent_name, date(date_trunc('week',local_date_created))))::varchar as weekly,
				round(avg(frt_calendar) over (partition by subteam, date(date_trunc('week',local_date_created))))::varchar as t_weekly,
				round(avg(frt_calendar) over (partition by supervisor, subteam, date(date_trunc('week',local_date_created))))::varchar as v_weekly,
				round(avg(frt_calendar) over (partition by agent_name, date(date_trunc('month',local_date_created))))::varchar as monthly,
				round(avg(frt_calendar) over (partition by subteam, date(date_trunc('month',local_date_created))))::varchar as t_monthly,
				round(avg(frt_calendar) over (partition by supervisor, subteam, date(date_trunc('month',local_date_created))))::varchar as v_monthly
			from (
				select 
					distinct
					subteam,
					ticket_id::varchar,
					local_date_created,
					hour_,
					channel,
					assignee_id,
					email_address,
					agent_name,
					case when supervisor is null then 'Urbanstems Flex' else supervisor end as supervisor,
					frt_calendar	
				from (
				select 
					distinct 
					subteam,
					ticket_id,
					local_date_created,
					extract(hour from local_time_created::time) as hour_,
					case when supervisor is null then job_supervisor else supervisor end as supervisor,
					date_updated,
					date_updated - local_date_created as recent_,
					max(date_updated - local_date_created) over (partition by ticket_id) as max_recent, 
					_id as assignee_id,
					agent_email as email_address,
					agent_name,
					frt_calendar,
					zen_tickets.channel
				from  
					zendesk_email_tickets
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
					(select ticket_id as _id_, reply_time_calendar_minutes as frt_calendar, 'email' as channel from zendesk_email_metrics) zen_tickets on _id_::varchar = ticket_id::varchar
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
					client_account = 'urbanstems'
					and (status = 'closed' or status = 'solved')
					and zendesk_email_tickets.channel = 'email'
				order by date_updated 
				) b
				where 
					recent_ = max_recent
					and agent_name is not null 
					) raw ) final_raw
	where v_monthly is not null
	order by date(date_trunc('month',local_date_created)) desc, agent_name )
