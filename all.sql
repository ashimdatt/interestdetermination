--sessions_bookmarked
select a.application_id, a.global_user_id,a.created,f.name as session_track,
                                count(1) as num_bookmarks, it.itemid,m.filterid,it.name as session_name,a.batch_id as batchid
                                from public.fact_actions_live a
                                join ratings_item it
                                on cast(a.metadata->>'ItemId' as int)=it.itemid
                                
                                join ratings_topic t on it.parenttopicid = t.topicid
                                join ratings_itemfiltermappings m on it.itemid = m.itemid
                                join ratings_filters f on m.filterid = f.filterid
                                
                                where 
                                t.listtypeid = 2 and it.isdisabled = 0
                                and a.identifier='bookmarkButton'
                                and a.batch_id>(select max(batchid) from ashim.sessions_bookmarked)
                                group by 1,2,3,4,6,7,8,9
                               



--sessions_viewed
select a.application_id,a.global_user_id, a.created,
                            f.name as session_track,
                            count(1) as num_views,it.itemid,m.filterid,it.name as session_name,a.batch_id as batchid
                            from public.fact_views_live a
                            join ratings_item it
                            on cast(a.metadata->>'ItemId' as int)=it.itemid
                            
                            join ratings_topic t on it.parenttopicid = t.topicid
                            join ratings_itemfiltermappings m on it.itemid = m.itemid
                            join ratings_filters f on m.filterid = f.filterid
                            
                            where 
                            t.listtypeid = 2 and it.isdisabled = 0
                            and a.identifier='item'
                            and a.batch_id> (select max(batchid) from ashim.sessions_viewed)
                            
                            group by 1,2,3,4,6,7,8,9

--views_batch
select max(batch_id) from fact_views_live
--sessions_batch
select max(batch_id) from fact_sessions_live
--views_last
select max(max) from ashim.views_batch
--sessions_last
select max(max) from ashim.sessions_batch

--sessions_viewed_timespent

select x.application_id,x.global_user_id,x.created,
              x.name as session_track , sum(x.timespent) as timespent,x.itemid,x.filterid,x.session_name,x.batch_id from (
              
              select a.created,a.batch_id,a.application_id, a.global_user_id,extract(epoch from (a.nextcreated-a.created)) as timespent,f.name,it.itemid,
              f.filterid,it.name as session_name from(
              select *
              , lead(created) over (partition by application_id, global_user_id, session_id order by created) as nextcreated
              from (
              select cast(metadata->>'ItemId' as integer) as itemid,application_id
              , global_user_id
              , session_id
              , created
              , batch_id
              , identifier
              
              , lag(identifier) over (partition by application_id, global_user_id, session_id order by created) as previousidentifier
              , lag(cast(metadata->>'ItemId' as integer)) over (partition by application_id, global_user_id, session_id order by created) as previousitemid
              from
              (select * from
              fact_views_live
              where batch_id> ", views_last$max, "and batch_id <= ", views_batch$max,  
              "union all
              select * from fact_sessions_live
              where batch_id>", sessions_last$max, "and batch_id<=",sessions_batch$max,  ") k
              ) a
              
              where (identifier <> previousidentifier
              and
              identifier='item'
              and (itemid<>previousitemid and previousitemid is not null) or previousitemid is null ) or identifier <> 'item'
              ) a
              
              join ratings_item it
              on a.itemid=it.itemid
              
              join ratings_topic t on it.parenttopicid = t.topicid
              join ratings_itemfiltermappings m on it.itemid = m.itemid
              join ratings_filters f on m.filterid = f.filterid
              
              where 
              --a.batch_id> (select max(batch_id) from ashim.sessions_viewed_timespent) and
              t.listtypeid = 2 and it.isdisabled = 0
              and a.identifier='item') x
              
              
              
              group by 1,2,3,4,6,7,8,9


--sessions_speakers_bookmarked

select a.application_id, a.global_user_id,a.created, f.name as session_track,it.itemid,m.filterid,it.name as session_name,
                                         count(1) as num_bookmarks,a.batch_id as batchid from
                                         (select a.created,a.application_id,a.batch_id,a.global_user_id,mk.sourceitemid as itemid,t.name from 
                                         public.fact_actions_live a
                                         join ratings_item it
                                         on cast(a.metadata->>'ItemId' as int)=it.itemid
                                         
                                         join ratings_itemmappings mk --- to get speakers and corresponding sessions
                                         on mk.targetitemid = it.itemid
                                         join ratings_topic t on it.parenttopicid = t.topicid
                                         where a.batch_id>(select max(batchid) from ashim.sessions_speakers_bookmarked)
                                         and t.listtypeid=4 and it.isdisabled=0
                                         and a.identifier='bookmarkButton') a  ----- new user favs
                                         join ratings_item it
                                         on a.itemid=it.itemid
                                         join ratings_topic t on it.parenttopicid = t.topicid
                                         join ratings_itemfiltermappings m on it.itemid = m.itemid
                                         join ratings_filters f on m.filterid = f.filterid
                                         
                                         where t.listtypeid = 2 and it.isdisabled = 0
                                         
                                         group by 1,2,3,4,5,6,7,9
                                        
--sessions_speakers_viewed

select a.application_id,a.global_user_id,a.created,
                         f.name as session_track, it.itemid,m.filterid,it.name as session_name, sum(a.num_views) as num_views,a.batch_id as batchid
                                     
                                     from(
                                     select a.application_id,a.global_user_id,a.created,a.batch_id,
                                     mk.sourceitemid as sessionid, mk.targetitemid as speakerid, count(1) as num_views
                                     from public.fact_views_live a
                                     join authdb_is_users auth
                                     on lower(auth.globaluserid)=lower(a.global_user_id)
                                     join ashim.ratings_globaluserdetails b
                                     on lower(a.global_user_id)=lower(b.globaluserid)
                                     join ratings_item it
                                     on cast(a.metadata->>'ItemId' as int)=it.itemid
                                     join ratings_itemmappings mk --- to get speakers and corresponding sessions
                                     on mk.targetitemid = it.itemid
                                     join ratings_topic t on it.parenttopicid = t.topicid
                                     where a.batch_id> (select max(batchid) from ashim.sessions_speakers_viewed)
                                     and t.listtypeid = 4 and it.isdisabled = 0 
                                     and a.identifier='item'
                                     group by 1,2,3,4,5,6 ) a    --- speakers only
                                     
                                     join ratings_item it
                                     on a.sessionid=it.itemid
                                     join ratings_topic t on it.parenttopicid = t.topicid
                                     join ratings_itemfiltermappings m on it.itemid = m.itemid
                                     join ratings_filters f on m.filterid = f.filterid
                                     
                                     where 
                                     t.listtypeid = 2 and it.isdisabled = 0   ---- corresponding sessions only
                                     
                                     group by 1,2,3,4,5,6,7,9
                                     

--sessions_speakers_viewed_timespent
select x.application_id,x.global_user_id,x.created,
                                               f.name as session_track ,it.itemid, m.filterid,it.name as session_name,sum(x.timespent) as timespent,x.batchid from (
              
              select a.created,a.application_id, a.global_user_id,a.batch_id as batchid,extract(epoch from (a.nextcreated-a.created)) as timespent,mk.targetitemid,mk.sourceitemid from(
              select *
              , lead(created) over (partition by application_id, global_user_id, session_id order by created) as nextcreated
              from (
              select cast(metadata->>'ItemId' as integer) as itemid,application_id
              , global_user_id
              , session_id
              , created
              , identifier
              ,batch_id
              , lag(identifier) over (partition by application_id, global_user_id, session_id order by created) as previousidentifier
              , lag(cast(metadata->>'ItemId' as integer)) over (partition by application_id, global_user_id, session_id order by created) as previousitemid
              from
              (select * from
              fact_views_live
              where batch_id>", views_last$max, "and batch_id <= ", views_batch$max,  
              "union all
              select * from fact_sessions_live
              where batch_id>", sessions_last$max, "and batch_id<=",sessions_batch$max,") k
                                               ) a
              
              where (identifier <> previousidentifier
              and
              identifier='item'
              and (itemid<>previousitemid and previousitemid is not null) or previousitemid is null ) or identifier <> 'item'
) a
              
              join ratings_item it
              on a.itemid=it.itemid
              
              join ratings_itemmappings mk --- to get speakers and corresponding sessions
              on mk.targetitemid = it.itemid
              join ratings_topic t on it.parenttopicid = t.topicid
              
              where t.listtypeid = 4 and it.isdisabled = 0) x ------ speakers only
              join ratings_item it
              on x.sourceitemid= it.itemid   --------information on on sessions that the speaker view was linked to
              join ratings_topic t on it.parenttopicid = t.topicid
              join ratings_itemfiltermappings m on it.itemid = m.itemid
              join ratings_filters f on m.filterid = f.filterid
              where t.listtypeid = 2 and it.isdisabled = 0   ---sessions only
              
              group by 1,2,3,4,5,6,7,9




--sessions_checkins_status
select a.applicationid,auth.globaluserid as global_user_id,a.created,f.name as session_track,it.itemid, m.filterid,it.name as session_name,
                                     count(1) as num_checkins
                                     
                                     from ratings_usercheckins a
                                     join
                                     ratings_usercheckinnotes bk
                                     on a.checkinid=bk.checkinid
                                     join ratings_item it
                                     on a.itemid=it.itemid
                                     join ratings_topic t on it.parenttopicid = t.topicid
                                     join ratings_itemfiltermappings m on it.itemid = m.itemid
                                     join ratings_filters f on m.filterid = f.filterid 
                                     join authdb_is_users auth
                                     on a.userid=auth.userid
                                     
                                     where 
                                     t.listtypeid=2 and t.isdisabled=0
                                     and a.created> (select max(created) from ashim.sessions_checkins_status)
                                     
                                     group by 1,2,3,4,5,6,7




 --drop table ashim.sessions_bookmarked_checkin_comb_stag
 
 create table if not exists ashim.sessions_bookmarked_checkin_comb_stag (
 application_id varchar,
 global_user_id varchar,
 created timestamp,
 session_track varchar,
 itemid varchar,
 filterid varchar,
 session_name varchar,
 num_checkins int,
 num_bookmarks int)
 
 select coalesce(a.application_id,lower(b.applicationid)) as application_id,
 coalesce(a.global_user_id,b.global_user_id) as global_user_id,
 case when (a.created is not null and a.created<b.created) then a.created 
 when (b.created is not null and b.created<a.created) then b.created 
 else coalesce(a.created,b.created) end as created,
 coalesce(a.session_track,b.session_track) as session_track,
 coalesce(a.itemid,cast(b.itemid as varchar)) as itemid,
 coalesce(a.filterid,cast(b.filterid as varchar)) as filterid,
 coalesce(a.session_name,b.session_name) as session_name,
 sum(b.num_checkins) as num_checkins,sum(a.num_bookmarks) as num_bookmarks
 from
 (select * from ashim.sessions_bookmarked where created > now()-interval '72 hours') a
 full outer join
 (select * from ashim.sessions_checkins_status where created> now()-interval '72 hours') b
 on lower(a.application_id)=lower(b.applicationid)
 and lower(a.global_user_id)=lower(b.global_user_id)
 and a.itemid=cast(b.itemid as varchar)
 and a.filterid=cast(b.filterid as varchar)
 and lower(a.session_track)=lower(b.session_track)
group by 1,2,3,4,5,6,7
 

 ------  combine sessions_bookmarked and sessions_checkins and sessions_speakers_bookmarked

 --drop table ashim.sessions_bookmarked_checkin_speakersb_comb_stag
 
 create table if not exists ashim.sessions_bookmarked_checkin_speakersb_comb_stag (
 
 application_id varchar,
 global_user_id varchar,
 created timestamp,
 session_track varchar,
 itemid varchar,
 filterid varchar,
 session_name varchar,
 num_checkins int,
 num_bookmarks int,
 speakers_bookmarked int)
 
 select coalesce(a.application_id,lower(b.application_id)) as application_id,
 coalesce(a.global_user_id,b.global_user_id) as global_user_id,
 case when (a.created is not null and a.created<b.created) then a.created 
 when (b.created is not null and b.created<a.created) then b.created 
 else coalesce(a.created,b.created) end as created,
 coalesce(a.session_track,b.session_track) as session_track,
 coalesce(a.itemid,cast(b.itemid as varchar)) as itemid,
 coalesce(a.filterid,cast(b.filterid as varchar)) as filterid,
 coalesce(a.session_name,b.session_name) as session_name,
 num_checkins,a.num_bookmarks ,sum(b.num_bookmarks) as speakers_bookmarked
 from
 (select * from ashim.sessions_bookmarked_checkin_comb_stag  where 
 created > now()-interval '72 hours') a
 full outer join
 (select * from ashim.sessions_speakers_bookmarked where created > now()-interval '72 hours') b
 on lower(a.application_id)=lower(b.application_id)
 and lower(a.global_user_id)=lower(b.global_user_id)
 and a.itemid=b.itemid
 and a.filterid=b.filterid
 and lower(a.session_track)=lower(b.session_track)
 
 group by 1,2,3,4,5,6,7,8,9

  ------  combine sessions_bookmarked_checkin_speakersb_comb_stag with sessions speakers_viewed

--drop table ashim.sessions_bookmarked_checkin_speakersb_speakersv_comb_stag

 create table if not exists ashim.sessions_bookmarked_checkin_speakersb_speakersv_comb_stag (
 application_id varchar,
 global_user_id varchar,
 created timestamp,
 session_track varchar,
 itemid varchar,
 filterid varchar,
 session_name varchar,
 num_checkins int,
 num_bookmarks int,
 speakers_bookmarked int,
 speakers_viewed int)
 
 select coalesce(a.application_id,lower(b.application_id)) as application_id,
 coalesce(a.global_user_id,b.global_user_id) as global_user_id,
 case when (a.created is not null and a.created<b.created) then a.created 
 when (b.created is not null and b.created<a.created) then b.created 
 else coalesce(a.created,b.created) end as created,
 coalesce(a.session_track,b.session_track) as session_track,
 coalesce(a.itemid,cast(b.itemid as varchar)) as itemid,
 coalesce(a.filterid,cast(b.filterid as varchar)) as filterid,
 coalesce(a.session_name,b.session_name) as session_name,
 a.num_checkins,a.num_bookmarks,a.speakers_bookmarked, sum(num_views) as speakers_viewed
 from
 (select * from ashim.sessions_bookmarked_checkin_speakersb_comb_stag  where 
 created > now()-interval '72 hours') a
 full outer join
 (select * from ashim.sessions_speakers_viewed where 
 created > now()-interval '72 hours') b
 on lower(a.application_id)=lower(b.application_id)
 and lower(a.global_user_id)=lower(b.global_user_id)
 and a.itemid=b.itemid
 and a.filterid=b.filterid
 and lower(a.session_track)=lower(b.session_track)
 
 group by 1,2,3,4,5,6,7,8,9,10


  ------  combine sessions_bookmarked_checkin_speakersb_speakersv_comb_stag with sessions speakers_viewed_timespent

--drop table ashim.sessions_bookmarked_checkin_speakersb_speakersv_speakersvts_comb_stag
create table if not exists ashim.sessions_bookmarked_checkin_speakersb_speakersv_speakersvts_comb_stag (
 application_id varchar,
 global_user_id varchar,
 created timestamp,
 session_track varchar,
 itemid varchar,
 filterid varchar,
 session_name varchar,
 num_checkins int,
 num_bookmarks int,
 speakers_bookmarked int,
 speakers_viewed int,
 speakers_viewed_timespent decimal)


 select coalesce(a.application_id,lower(b.application_id)) as application_id,
 coalesce(a.global_user_id,b.global_user_id) as global_user_id,
case when (a.created is not null and a.created<b.created) then a.created 
 when (b.created is not null and b.created<a.created) then b.created 
 else coalesce(a.created,b.created) end as created,
 coalesce(a.session_track,b.session_track) as session_track,
 coalesce(a.itemid,cast(b.itemid as varchar)) as itemid,
 coalesce(a.filterid,cast(b.filterid as varchar)) as filterid,
 coalesce(a.session_name,b.session_name) as session_name,
 a.num_checkins,a.num_bookmarks,a.speakers_bookmarked, a.speakers_viewed,sum(b.timespent) as speakers_viewed_timespent
 from
 (select * from ashim.sessions_bookmarked_checkin_speakersb_speakersv_comb_stag  where 
 created > now()-interval '72 hours') a
 full outer join
 (select * from ashim.sessions_speakers_viewed_timespent where created > now()-interval '72 hours') b
 on lower(a.application_id)=lower(b.application_id)
 and lower(a.global_user_id)=lower(b.global_user_id)
 and a.itemid=b.itemid
 and a.filterid=b.filterid
 and lower(a.session_track)=lower(b.session_track)
 
 group by 1,2,3,4,5,6,7,8,9,10,11
 
 

 
 
 ----combine with sessions_viewed
 
 --drop table ashim.speakers_sessionsv_stag

create table if not exists ashim.speakers_sessionsv_stag (
 application_id varchar,
 global_user_id varchar,
 created timestamp,
 session_track varchar,
 itemid varchar,
 filterid varchar,
 session_name varchar,
 num_checkins int,
 num_bookmarks int,
 speakers_bookmarked int,
 speakers_viewed int,
 speakers_viewed_timespent decimal,
 sessions_viewed int)


 select coalesce(a.application_id,lower(b.application_id)) as application_id,
 coalesce(a.global_user_id,b.global_user_id) as global_user_id,
case when (a.created is not null and a.created<b.created) then a.created 
when (b.created is not null and b.created<a.created) then b.created 
else coalesce(a.created,b.created) end as created,
coalesce(a.session_track,b.session_track) as session_track,
coalesce(a.itemid,cast(b.itemid as varchar)) as itemid,
coalesce(a.filterid,cast(b.filterid as varchar)) as filterid,
coalesce(a.session_name,b.name) as session_name,
a.num_checkins,a.num_bookmarks,a.speakers_bookmarked, a.speakers_viewed,speakers_viewed_timespent,
sum(b.num_views) as sessions_viewed
from
(select * from sessions_bookmarked_checkin_speakersb_speakersv_speakersvts_com  where 
created > now()-interval '72 hours') a
full outer join
(select * from ashim.sessions_viewed where created > now()-interval '72 hours') b
on lower(a.application_id)=lower(b.application_id)
and lower(a.global_user_id)=lower(b.global_user_id)
and a.itemid=b.itemid
and a.filterid=b.filterid
and lower(a.session_track)=lower(b.session_track)

group by 1,2,3,4,5,6,7,8,9,10,11,12
 
 
  ----combine with sessions_viewed_timespent
 
 --drop table ashim.speakers_sessionsv_sessionsts_stag

create table if not exists ashim.speakers_sessionsv_sessionsts_stag (
 application_id varchar,
 global_user_id varchar,
 created timestamp,
 session_track varchar,
 itemid varchar,
 filterid varchar,
 session_name varchar,
 num_checkins int,
 num_bookmarks int,
 speakers_bookmarked int,
 speakers_viewed int,
 speakers_viewed_timespent decimal,
 sessions_viewed int,
 sessions_timespent decimal)


 select coalesce(a.application_id,lower(b.application_id)) as application_id,
 coalesce(a.global_user_id,b.global_user_id) as global_user_id,
case when (a.created is not null and a.created<b.created) then a.created 
 when (b.created is not null and b.created<a.created) then b.created 
 else coalesce(a.created,b.created) end as created,
 coalesce(a.session_track,b.session_track) as session_track,
 coalesce(a.itemid,cast(b.itemid as varchar)) as itemid,
 coalesce(a.filterid,cast(b.filterid as varchar)) as filterid,
 coalesce(a.session_name,b.session_name) as session_name,
 a.num_checkins,a.num_bookmarks,a.speakers_bookmarked, a.speakers_viewed,speakers_viewed_timespent,
 a.sessions_viewed, sum(b.timespent) as sessions_timespent
 from
 (select * from ashim.speakers_sessionsv_stag  where 
 created > now()-interval '72 hours') a
 full outer join
 (select * from ashim.sessions_viewed_timespent where created > now()-interval '72 hours') b
 on lower(a.application_id)=lower(b.application_id)
 and lower(a.global_user_id)=lower(b.global_user_id)
 and a.itemid=b.itemid
 and a.filterid=b.filterid
 and lower(a.session_track)=lower(b.session_track)
 
 group by 1,2,3,4,5,6,7,8,9,10,11,12,13
 
 
 --- final table with all actions
 
 --create table ashim.thez_batch(batchid int)
 --INSERT INTO ashim.thez_batch VALUES (1);
 
 create table if not exists public.thez_actions_stag (
 application_id varchar,
 global_user_id varchar,
 created timestamp,
 session_track varchar,
 itemid varchar,
 filterid varchar,
 session_name varchar,
 num_checkins int,
 num_bookmarks int,
 speakers_bookmarked int,
 speakers_viewed int,
 speakers_viewed_timespent decimal,
 sessions_viewed int,
 sessions_timespent decimal,
 batchid int)

--final table
 select a.*,(select batchid from ashim.thez_batch) as batchid from 
 ashim.speakers_sessionsv_sessionsts_stag a
 
 select * from fact_actions_live where identifier='bookmarkButton'
 
 select * from thez_actions_stag where batchid=5
 
 create table if not exists public.thez_scores_final (
 application_id varchar,
 global_user_id varchar,
 created timestamp,
 session_track varchar,
 itemid varchar,
 filterid varchar,
 session_name varchar,
 num_checkins int,
 num_bookmarks int,
 speakers_bookmarked int,
 speakers_viewed int,
 speakers_viewed_timespent decimal,
 sessions_viewed int,
 sessions_timespent decimal,
 batchid int,
 zscore decimal,
 grade_score varchar
)
 
 --select * from thez_scores_final where batchid=5
 
 ----creating table with user_dimension
 -- drop table thez_user_score_comb
 create table thez_user_score_comb(
application_id varchar,
event_name varchar,
startdate timestamp,
enddate timestamp,
 global_user_id varchar,
 title varchar,
 session_track varchar,
 created timestamp,
 itemid varchar,
 filterid varchar,
 session_name varchar,
 emailaddress varchar,
 firstname varchar,
 lastname varchar,
 company varchar,
 phone varchar,
 detailed_title varchar,
 batchid int,
 z_score decimal,
 grade_score varchar)
 
 
select a.application_id,app.name as event_name,app.startdate,app.enddate,a.global_user_id, case when lower(b.title) like '%specialist%' then 'Individual Contributor'
when b.title like '%VP%' or lower(b.title) like '%vice president%' then 'Vice President' 
                                 when lower(b.title) like '%manager%' then 'Manager'
                                 when lower(b.title) like '%director%' then 'Director'
                                 when lower(b.title) like '%engineer%' then 'Engineer' 
                                 when lower(b.title) like '%chief%'  or b.title like '%CEO%' or b.title like '%CMO%'
                                 or b.title like '%CCO%' or b.title like '%COO%' or b.title like '%CRO%' or b.title like '%CTO%'
                                 or b.title like '%CFO%' then 'C-Level exec'
                                 else 'Individual Contributor' end as title, a.session_track,
                                 a.created, a.itemid, a.filterid,a.session_name,
                                 b.emailaddress,b.firstname, b.lastname, b.company,b.phone,b.title as detailed_title,
                                 a.batchid,a.zscore,a.grade_score
                                 from public.thez_scores_final a
                                 join ashim.ratings_globaluserdetails b
                                 on lower(a.global_user_id)=lower(b.globaluserid)
                                 join authdb_applications app
                                 on lower(a.application_id)=lower(app.applicationid)
                                 where a.batchid>(select max(batchid) from thez_user_score_comb)
                                 
select * from public.thez_scores_final where batchid=6


--- semantic topics
drop table ashim.thez_actions_stag_semantic
create table if not exists ashim.thez_actions_stag_semantic (
 application_id varchar,
 global_user_id varchar,
 created timestamp,
 session_track varchar,
 itemid varchar,
 filterid varchar,
 session_name varchar,
 num_checkins int,
 num_bookmarks int,
 speakers_bookmarked int,
 speakers_viewed int,
 speakers_viewed_timespent decimal,
 sessions_viewed int,
 sessions_timespent decimal,
 batchid int,
 semantic_topic varchar)
 
 ---semantic topics scores
create table if not exists public.thez_scores_final_semantic(
 application_id varchar,
 global_user_id varchar,
 created timestamp,
 session_track varchar,
 itemid varchar,
 filterid varchar,
 session_name varchar,
 num_checkins int,
 num_bookmarks int,
 speakers_bookmarked int,
 speakers_viewed int,
 speakers_viewed_timespent decimal,
 sessions_viewed int,
 sessions_timespent decimal,
 batchid int,
 semantic_topic varchar,
 tot int,
 V18 decimal,
 grade_score varchar)


--- semantic topics scores and users

create table if not exists public.thez_user_score_comb_semantic(
 application_id varchar,
event_name varchar,
startdate timestamp,
enddate timestamp,
 global_user_id varchar,
 title varchar,
 session_track varchar,
 created timestamp,
 itemid varchar,
 filterid varchar,
 session_name varchar,
 emailaddress varchar,
 firstname varchar,
 lastname varchar,
 company varchar,
 phone varchar,
 detailed_title varchar,
 batchid int,
 semantic_topic varchar,
 z_score decimal,
 grade_score varchar)
 
 
 
 select a.application_id,a.global_user_id, a.created,
                            f.name as session_track,
                            count(1) as num_views,it.itemid,m.filterid,it.name as session_name,a.batch_id as batchid
                            from public.fact_views_live a
                            join ratings_item it
                            on cast(a.metadata->>'ItemId' as int)=it.itemid
                            
                            join ratings_topic t on it.parenttopicid = t.topicid
                            left join ratings_itemfiltermappings m on it.itemid = m.itemid
                            left join ratings_filters f on m.filterid = f.filterid
                            
                            where 
                            t.listtypeid = 2 and it.isdisabled = 0
                            and a.identifier='item'
                            and a.batch_id> (select max(batchid) from ashim.sessions_viewed)
                            
                            group by 1,2,3,4,6,7,8,9

select coalesce(a.application_id,lower(b.application_id)) as application_id,
                                               coalesce(a.global_user_id,b.global_user_id) as global_user_id,
                                               case when (a.created is not null and a.created<b.created) then a.created 
                                               when (b.created is not null and b.created<a.created) then b.created 
                                               else coalesce(a.created,b.created) end as created,
                                               coalesce(a.session_track,b.session_track) as session_track,
                                               coalesce(a.itemid,cast(b.itemid as varchar)) as itemid,
                                               coalesce(a.filterid,cast(b.filterid as varchar)) as filterid,
                                               coalesce(a.session_name,b.session_name) as session_name,
                                               a.num_checkins,a.num_bookmarks,a.speakers_bookmarked, a.speakers_viewed,speakers_viewed_timespent,
                                               a.sessions_viewed, sum(b.timespent) as sessions_timespent
                                               from
                                               (select * from ashim.speakers_sessionsv_stag  where 
                                               created > now()-interval '72 hours') a
                                               left join
                                               (select * from ashim.sessions_viewed_timespent where created > now()-interval '72 hours') b
                                               on lower(a.application_id)=lower(b.application_id)
                                               and lower(a.global_user_id)=lower(b.global_user_id)
                                               and a.itemid=b.itemid
                                               and a.filterid=b.filterid
                                               and lower(a.session_track)=lower(b.session_track)
                                               
                                               group by 1,2,3,4,5,6,7,8,9,10,11,12,13



select a.application_id, count(distinct auth.userid) as num_users_sessions_viewed, count(1) as num_views_sessions
                            from public.fact_views_live a
                            join ratings_item it
                            on cast(a.metadata->>'ItemId' as int)=it.itemid
                            
                            join ratings_topic t on it.parenttopicid = t.topicid
                            join authdb_is_users auth
                            on lower(a.global_user_id)=lower(auth.globaluserid)
                            join public.authdb_applications app
                            on lower(app.applicationid)=lower(a.application_id)
                            join salesforce.implementation c
                            on lower(a.application_id)=lower(c.applicationid) 
                            where 
                            t.listtypeid = 2 and it.isdisabled = 0
                            and a.identifier='item'
                            and app.enddate <= '2016-07-25' and app.enddate>='2016-01-01'           
                            
                            group by 1
                            
                            
                            
select * from ashim.ratings_leads



                           