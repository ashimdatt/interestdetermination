pw <- {
  "pass"
}
#setwd("/Users/ashimdatta/Enterprise/THEZ/R/thez_production")
getwd()
library("DBI")
library("RPostgreSQL")
library("sqldf")
library("ggplot2")
library("gridExtra")
library("plyr")
library("dplyr")

drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, dbname = "databasename",
                 host = "server", port = 5432,
                 user = "etl", password = pw)


sessions_bookmarked<-dbGetQuery(con,"select a.application_id, a.global_user_id,a.created,f.name as session_track,
                                count(1) as num_bookmarks, it.itemid,m.filterid,it.name as session_name,a.batch_id as batchid
                                from public.fact_actions_live a
                                join ratings_item it
                                on cast(a.metadata->>'ItemId' as bigint)=it.itemid
                                
                                join ratings_topic t on it.parenttopicid = t.topicid
                                left join ratings_itemfiltermappings m on it.itemid = m.itemid
                                left join ratings_filters f on m.filterid = f.filterid
                                
                                where 
                                t.listtypeid = 2 and it.isdisabled = 0
                                and a.identifier='bookmarkButton'
                                and a.batch_id>(select max(batchid) from ashim.sessions_bookmarked)
                                group by 1,2,3,4,6,7,8,9
                                ")

sessions_bookmarked$application_id<-tolower(sessions_bookmarked$application_id)

dbWriteTable(con, c("ashim", "sessions_bookmarked"), value=sessions_bookmarked, append=TRUE, row.names=FALSE)

sessions_viewed<-dbGetQuery(con,"select a.application_id,a.global_user_id, a.created,
                            f.name as session_track,
                            count(1) as num_views,it.itemid,m.filterid,it.name as session_name,a.batch_id as batchid
                            from public.fact_views_live a
                            join ratings_item it
                            on cast(a.metadata->>'ItemId' as bigint)=it.itemid
                            
                            join ratings_topic t on it.parenttopicid = t.topicid
                            left join ratings_itemfiltermappings m on it.itemid = m.itemid
                            left join ratings_filters f on m.filterid = f.filterid
                            
                            where 
                            t.listtypeid = 2 and it.isdisabled = 0
                            and a.identifier='item'
                            and a.batch_id> (select max(batchid) from ashim.sessions_viewed)
                            
                            group by 1,2,3,4,6,7,8,9")


sessions_viewed$application_id<-tolower(sessions_viewed$application_id)

dbWriteTable(con, c("ashim", "sessions_viewed"), value=sessions_viewed, append=TRUE, row.names=FALSE)

views_batch<-dbGetQuery(con,"select max(batch_id) from fact_views_live")
sessions_batch<-dbGetQuery(con,"select max(batch_id) from fact_sessions_live")
views_last<-dbGetQuery(con,"select max(max) from ashim.views_batch")
sessions_last<-dbGetQuery(con,"select max(max) from ashim.sessions_batch")

query<- paste("select x.application_id,x.global_user_id,x.created,
              x.name as session_track , sum(x.timespent) as timespent,x.itemid,x.filterid,x.session_name,x.batch_id from (
              
              select a.created,a.batch_id,a.application_id, a.global_user_id,extract(epoch from (a.nextcreated-a.created)) as timespent,f.name,it.itemid,
              f.filterid,it.name as session_name from(
              select *
              , lead(created) over (partition by application_id, global_user_id, session_id order by created) as nextcreated
              from (
              select cast(metadata->>'ItemId' as bigint) as itemid,application_id
              , global_user_id
              , session_id
              , created
              , batch_id
              , identifier
              
              , lag(identifier) over (partition by application_id, global_user_id, session_id order by created) as previousidentifier
              , lag(cast(metadata->>'ItemId' as bigint)) over (partition by application_id, global_user_id, session_id order by created) as previousitemid
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
              left join ratings_itemfiltermappings m on it.itemid = m.itemid
              left join ratings_filters f on m.filterid = f.filterid
              
              where 
              --a.batch_id> (select max(batch_id) from ashim.sessions_viewed_timespent) and
              t.listtypeid = 2 and it.isdisabled = 0
              and a.identifier='item') x
              
              
              
              group by 1,2,3,4,6,7,8,9")

sessions_viewed_timespent<-dbGetQuery(con,query)


sessions_viewed_timespent$application_id<-tolower(sessions_viewed_timespent$application_id)

dbWriteTable(con, c("ashim", "sessions_viewed_timespent"), value=sessions_viewed_timespent, append=TRUE, row.names=FALSE)


write.csv(sessions_viewed_timespent,"sessions_viewed_timespent.csv")




sessions_speakers_bookmarked<-dbGetQuery(con," select a.application_id, a.global_user_id,a.created, f.name as session_track,it.itemid,m.filterid,it.name as session_name,
                                         count(1) as num_bookmarks,a.batch_id as batchid from
                                         (select a.created,a.application_id,a.batch_id,a.global_user_id,mk.sourceitemid as itemid,t.name from 
                                         public.fact_actions_live a
                                         join ratings_item it
                                         on cast(a.metadata->>'ItemId' as bigint)=it.itemid
                                         
                                         join ratings_itemmappings mk --- to get speakers and corresponding sessions
                                         on mk.targetitemid = it.itemid
                                         join ratings_topic t on it.parenttopicid = t.topicid
                                         where a.batch_id>(select max(batchid) from ashim.sessions_speakers_bookmarked)
                                         and t.listtypeid=4 and it.isdisabled=0
                                         and a.identifier='bookmarkButton') a  ----- new user favs
                                         join ratings_item it
                                         on a.itemid=it.itemid
                                         join ratings_topic t on it.parenttopicid = t.topicid
                                         left join ratings_itemfiltermappings m on it.itemid = m.itemid
                                         left join ratings_filters f on m.filterid = f.filterid
                                         
                                         where t.listtypeid = 2 and it.isdisabled = 0
                                         
                                         group by 1,2,3,4,5,6,7,9
                                         ")

sessions_speakers_bookmarked$application_id<-tolower(sessions_speakers_bookmarked$application_id)

dbWriteTable(con, c("ashim", "sessions_speakers_bookmarked"), value=sessions_speakers_bookmarked, append=TRUE, row.names=FALSE)

write.csv(sessions_speakers_bookmarked,"sessions_speakers_bookmarked.csv")

sessions_speakers_viewed<-dbGetQuery(con,"select a.application_id,a.global_user_id,a.created,
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
                                     on cast(a.metadata->>'ItemId' as bigint)=it.itemid
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
                                     left join ratings_itemfiltermappings m on it.itemid = m.itemid
                                     left join ratings_filters f on m.filterid = f.filterid
                                     
                                     where 
                                     t.listtypeid = 2 and it.isdisabled = 0   ---- corresponding sessions only
                                     
                                     group by 1,2,3,4,5,6,7,9
                                     ")

sessions_speakers_viewed$application_id<-tolower(sessions_speakers_viewed$application_id)

dbWriteTable(con, c("ashim", "sessions_speakers_viewed"), value=sessions_speakers_viewed, append=TRUE, row.names=FALSE)

write.csv(sessions_speakers_viewed,"sessions_speakers_viewed.csv")

query2<-paste("select x.application_id,x.global_user_id,x.created,
              f.name as session_track ,it.itemid, m.filterid,it.name as session_name,sum(x.timespent) as timespent,x.batchid from (
              
              select a.created,a.application_id, a.global_user_id,a.batch_id as batchid,extract(epoch from (a.nextcreated-a.created)) as timespent,mk.targetitemid,mk.sourceitemid from(
              select *
              , lead(created) over (partition by application_id, global_user_id, session_id order by created) as nextcreated
              from (
              select cast(metadata->>'ItemId' as bigint) as itemid,application_id
              , global_user_id
              , session_id
              , created
              , identifier
              ,batch_id
              , lag(identifier) over (partition by application_id, global_user_id, session_id order by created) as previousidentifier
              , lag(cast(metadata->>'ItemId' as bigint)) over (partition by application_id, global_user_id, session_id order by created) as previousitemid
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
              left join ratings_itemfiltermappings m on it.itemid = m.itemid
              left join ratings_filters f on m.filterid = f.filterid
              where t.listtypeid = 2 and it.isdisabled = 0   ---sessions only
              
              group by 1,2,3,4,5,6,7,9")

sessions_speakers_viewed_timespent<-dbGetQuery(con,query2)

sessions_speakers_viewed_timespent$application_id<-tolower(sessions_speakers_viewed_timespent$application_id)

dbWriteTable(con, c("ashim", "sessions_speakers_viewed_timespent"), value=sessions_speakers_viewed_timespent, append=TRUE, row.names=FALSE)

write.csv(sessions_speakers_viewed_timespent,"sessions_speakers_viewed_timespent.csv")
dbWriteTable(con, c("ashim", "views_batch"), value=views_batch, append=TRUE, row.names=FALSE)
dbWriteTable(con, c("ashim", "sessions_batch"), value=sessions_batch, append=TRUE, row.names=FALSE)

sessions_checkins_status<-dbGetQuery(con,"select a.applicationid,auth.globaluserid as global_user_id,a.created,f.name as session_track,it.itemid, m.filterid,it.name as session_name,
                                     count(1) as num_checkins
                                     
                                     from ratings_usercheckins a
                                     join
                                     ratings_usercheckinnotes bk
                                     on a.checkinid=bk.checkinid
                                     join ratings_item it
                                     on a.itemid=it.itemid
                                     join ratings_topic t on it.parenttopicid = t.topicid
                                     left join ratings_itemfiltermappings m on it.itemid = m.itemid
                                     left join ratings_filters f on m.filterid = f.filterid 
                                     join authdb_is_users auth
                                     on a.userid=auth.userid
                                     
                                     where 
                                     t.listtypeid=2 and t.isdisabled=0
                                     and a.created> (select max(created) from ashim.sessions_checkins_status)
                                     
                                     group by 1,2,3,4,5,6,7")

sessions_checkins_status$applicationid<-tolower(sessions_checkins_status$applicationid)

dbWriteTable(con, c("ashim", "sessions_checkins_status"), value=sessions_checkins_status, append=TRUE, row.names=FALSE)

write.csv(sessions_checkins_status,"sessions_checkins_status.csv")

dbGetQuery(con,"DELETE FROM ashim.sessions_bookmarked_checkin_comb_stag 
           WHERE created > now()-interval '72 hours'"  )


sessions_bookmarked_checkin_comb_stag<-dbGetQuery(con,"
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
                                                  ")

dbWriteTable(con, c("ashim", "sessions_bookmarked_checkin_comb_stag"), value=sessions_bookmarked_checkin_comb_stag, append=TRUE, row.names=FALSE)

dbGetQuery(con,"DELETE FROM ashim.sessions_bookmarked_checkin_speakersb_comb_stag
           WHERE created > now()-interval '72 hours'"  )

sessions_bookmarked_checkin_speakersb_comb_stag<-dbGetQuery(con,"select coalesce(a.application_id,lower(b.application_id)) as application_id,
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
                                                            ")

dbWriteTable(con, c("ashim", "sessions_bookmarked_checkin_speakersb_comb_stag"), value=sessions_bookmarked_checkin_speakersb_comb_stag, append=TRUE, row.names=FALSE)

dbGetQuery(con,"DELETE FROM ashim.sessions_bookmarked_checkin_speakersb_speakersv_comb_stag
           WHERE created > now()-interval '72 hours'"  )

sessions_bookmarked_checkin_speakersb_speakersv_comb_stag<-dbGetQuery(con," select coalesce(a.application_id,lower(b.application_id)) as application_id,
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
                                                                      ")


dbWriteTable(con, c("ashim", "sessions_bookmarked_checkin_speakersb_speakersv_comb_stag"), value=sessions_bookmarked_checkin_speakersb_speakersv_comb_stag, append=TRUE, row.names=FALSE)

dbGetQuery(con,"DELETE FROM ashim.sessions_bookmarked_checkin_speakersb_speakersv_speakersvts_com
           WHERE created > now()-interval '72 hours'"  )


sessions_bookmarked_checkin_speakersb_speakersv_speakersvts_com<-dbGetQuery(con,"select coalesce(a.application_id,lower(b.application_id)) as application_id,
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
                                                                            
                                                                            group by 1,2,3,4,5,6,7,8,9,10,11")

dbWriteTable(con, c("ashim", "sessions_bookmarked_checkin_speakersb_speakersv_speakersvts_com"), value= sessions_bookmarked_checkin_speakersb_speakersv_speakersvts_com, append=TRUE, row.names=FALSE)

dbGetQuery(con,"DELETE FROM ashim.speakers_sessionsv_stag
           WHERE created > now()-interval '72 hours'"  )

speakers_sessionsv_stag<-dbGetQuery(con,"select coalesce(a.application_id,lower(b.application_id)) as application_id,
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
                                    (select * from ashim.sessions_bookmarked_checkin_speakersb_speakersv_speakersvts_com  where 
                                    created > now()-interval '72 hours') a
                                    full outer join
                                    (select * from ashim.sessions_viewed where created > now()-interval '72 hours'
                                      and num_views>1 ) b
                                    on lower(a.application_id)=lower(b.application_id)
                                    and lower(a.global_user_id)=lower(b.global_user_id)
                                    and a.itemid=b.itemid
                                    and a.filterid=b.filterid
                                    and lower(a.session_track)=lower(b.session_track)
                                    
                                    group by 1,2,3,4,5,6,7,8,9,10,11,12")

dbWriteTable(con, c("ashim", "speakers_sessionsv_stag"), value=speakers_sessionsv_stag,
             append=TRUE, row.names=FALSE)


dbGetQuery(con,"DELETE FROM ashim.speakers_sessionsv_sessionsts_stag
           WHERE created > now()-interval '72 hours'"  )

speakers_sessionsv_sessionsts_stag<-dbGetQuery(con,"select coalesce(a.application_id,lower(b.application_id)) as application_id,
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
                                               
                                               group by 1,2,3,4,5,6,7,8,9,10,11,12,13")

## left join might miss some rows

dbWriteTable(con, c("ashim", "speakers_sessionsv_sessionsts_stag"), value=speakers_sessionsv_sessionsts_stag,
             append=TRUE, row.names=FALSE)

thez_batch<-dbGetQuery(con,"select max(batchid) as batchid from ashim.thez_batch")

dbGetQuery(con,"DELETE FROM public.thez_actions_stag
           WHERE batchid=(select max(batchid) from ashim.thez_batch)")

thez_actions_stag<-dbGetQuery(con,"select a.*,(select max(batchid) from ashim.thez_batch) as batchid from 
                              ashim.speakers_sessionsv_sessionsts_stag a where a.created > now()-interval '72 hours' ")

dbWriteTable(con, c("public", "thez_actions_stag"), value=thez_actions_stag,
             append=TRUE, row.names=FALSE)

thez_batch$batchid<-thez_batch$batchid+1


masterdata<-thez_actions_stag

masterdata[is.na(masterdata)] <- 0
i<-1
j<-0
x<-0
max<-c(0,0,0,0,0,0,0)
min<-c(0,0,0,0,0,0,0)


for(j in 8:14){
  max[(j-7)]<-max(masterdata[,j])
  min[(j-7)]<-min(masterdata[,j])
}
j<-0


masterdata$V16<-masterdata[,8]/(max[(1)]-min[(1)])+masterdata[,9]/(max[(2)]-min[(2)])+masterdata[,10]/(max[(3)]-min[(3)])+
  masterdata[,11]/(max[(4)]-min[(4)])+masterdata[,12]/(max[(5)]-min[(5)])+masterdata[,13]/(max[(6)]-min[(6)])+
  masterdata[,14]/(max[(7)]-min[(7)])

#masterdata<-masterdata[,c(1:17)]

masterdata[is.na(masterdata)] <- 0

a<-quantile(masterdata$V16,.20, na.rm = TRUE) 
b<-quantile(masterdata$V16,.80, na.rm = TRUE) 

i=0


masterdata$grade_score<-ifelse(masterdata$V16<=a,'C',ifelse(masterdata$V16 >a & masterdata$V16 <=b ,'B','A'))


dbWriteTable(con, c("public", "thez_scores_final"), value=masterdata,
             append=TRUE, row.names=FALSE)

dbGetQuery(con,"DELETE FROM public.thez_user_score_comb
           WHERE batchid=(select max(batchid) from ashim.thez_batch)")

thez_user_score_comb<-dbGetQuery(con,"select a.application_id,app.name as event_name,app.startdate,app.enddate,a.global_user_id, case when lower(b.title) like '%specialist%' then 'Individual Contributor'
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
                                 where a.batchid=(select max(batchid) from public.thez_scores_final) ")

dbWriteTable(con, c("public", "thez_user_score_comb"), value=thez_user_score_comb,
             append=TRUE, row.names=FALSE)

dbWriteTable(con, c("ashim", "thez_batch"), value=thez_batch,
             append=TRUE, row.names=FALSE)
