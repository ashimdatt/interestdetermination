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

thez_actions_stag_semantic<-dbGetQuery(con,"select a.*,b.topic as semantic_topic from public.thez_actions_stag a
 join ben.session_tags_v0 b
                                       on a.itemid=cast(b.itemid as varchar)
                                       and lower(a.application_id)=lower(b.applicationid)
                                       where a.batchid=(select max(batchid) from public.thez_actions_stag)")                                                                                                                      

thez_actions_stag_semantic[is.na(thez_actions_stag_semantic)] <- 0

thez_actions_stag_semantic$tot<-thez_actions_stag_semantic$num_checkins+thez_actions_stag_semantic$num_bookmarks+thez_actions_stag_semantic$speakers_bookmarked+
  thez_actions_stag_semantic$speakers_viewed+thez_actions_stag_semantic$sessions_viewed

thez_actions_stag_semantic2<-thez_actions_stag_semantic[which(thez_actions_stag_semantic$tot>5),]

dbGetQuery(con,"DELETE FROM ashim.thez_actions_stag_semantic
           WHERE batchid=(select max(batchid) from public.thez_actions_stag)")

dbWriteTable(con, c("ashim", "thez_actions_stag_semantic"), value=thez_actions_stag_semantic2,
             append=TRUE, row.names=FALSE)


masterdata<-thez_actions_stag_semantic2
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


masterdata$V18<-masterdata[,8]/(max[(1)]-min[(1)])+masterdata[,9]/(max[(2)]-min[(2)])+masterdata[,10]/(max[(3)]-min[(3)])+
  masterdata[,11]/(max[(4)]-min[(4)])+masterdata[,12]/(max[(5)]-min[(5)])+masterdata[,13]/(max[(6)]-min[(6)])+
  masterdata[,14]/(max[(7)]-min[(7)])

#masterdata<-masterdata[,c(1:17)]

a<-quantile(masterdata$V18,.33,na.rm=TRUE) 
b<-quantile(masterdata$V18,.66,na.rm=TRUE) 

i=0


masterdata$grade_score<-ifelse(masterdata$V18<=a,'C',ifelse(masterdata$V18 >a & masterdata$V18 <=b ,'B','A'))

dbGetQuery(con,"DELETE FROM public.thez_scores_final_semantic
           WHERE batchid=(select max(batchid) from public.thez_actions_stag)")

dbWriteTable(con, c("public", "thez_scores_final_semantic"), value=masterdata,
             append=TRUE, row.names=FALSE)


thez_user_score_comb_semantic<-dbGetQuery(con,"select a.application_id,app.name as event_name,app.startdate,app.enddate,a.global_user_id, case when lower(b.title) like '%specialist%' then 'Individual Contributor'
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
                                 a.batchid,a.semantic_topic,a.V18,a.grade_score
                                 from public.thez_scores_final_semantic a
                                 left
                                 join ashim.ratings_globaluserdetails b
                                 on lower(a.global_user_id)=lower(b.globaluserid)
                                 join authdb_applications app
                                 on lower(a.application_id)=lower(app.applicationid)
                                 where a.batchid=(select max(batchid) from public.thez_scores_final_semantic) ")

dbGetQuery(con,"DELETE FROM thez_user_score_comb_semantic
           WHERE batchid=(select max(batchid) from public.thez_scores_final_semantic)")

dbWriteTable(con, c("public", "thez_user_score_comb_semantic"), value=thez_user_score_comb_semantic,
             append=TRUE, row.names=FALSE)
