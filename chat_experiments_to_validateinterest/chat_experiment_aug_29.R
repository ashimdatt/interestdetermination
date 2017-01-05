pw <- {
  "s0.Much.Data"
}
#setwd("/Users/ashimdatta/Enterprise/chat experiments/Week of August 29- Sep 5")
getwd()
library("DBI")
library("RPostgreSQL")
library("sqldf")
library("ggplot2")
library("gridExtra")
library("plyr")
library("dplyr")

id<-paste("('c41734ad-7747-402b-9aed-2f1538628f59',","'09e5d674-d88a-403a-93eb-284ea96cb154',",
"'984d1e55-e0c3-4869-8454-a39208bae596',","'8df6d9e3-973e-4502-bf86-0b2efcf591bd',","'b7d7a15a-c97a-49d0-ae71-e5aa771f555d',"
,"'72f5bbe3-7be2-4f47-81e2-76515a98709f',","'b06d2395-4518-4bbf-909f-15d697dbd435',","'8830c21b-09f8-42f8-a3dc-43f10eb6e83b',",
"'2b7f174b-b8f3-4462-b607-58ac9f10a8e7',","'78b847e0-e971-4f00-a9ab-8cb24f968c53',","'c0890d72-8b62-47e7-b501-bb8bee65d16d')")


drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, dbname = "analytics",
                 host = "10.223.192.6", port = 5432,
                 user = "etl", password = pw)

query<-paste("select a.application_id, auth.userid,a.created,f.topic,f.itemid, a.num_checkins,a.num_bookmarks,a.speakers_bookmarked,a.speakers_viewed,a.speakers_viewed_timespent,
a.sessions_viewed, a.sessions_timespent
             from public.thez_actions_stag a
             join ben.session_topics_manual f
             on cast(a.itemid as int)=cast(f.itemid as int)
             join public.authdb_is_users auth
             on lower(a.global_user_id)=lower(auth.globaluserid)
            and lower(auth.applicationid)=lower(a.application_id)
             
             where --batchid=(select max(batchid) from public.thez_actions_stag)  and 
             lower(a.application_id) in ", id , sep="")

masterdata<-dbGetQuery(con,query)

masterdata$applicationid<-tolower(masterdata$application_id)

write.csv(masterdata,"masterdata.csv")

masterdata<-read.csv("masterdata.csv")

masterdata[is.na(masterdata)] <- 0

masterdata_agg<-sqldf("select userid, applicationid, topic, sum(num_checkins) as num_checkins, sum(num_bookmarks) as num_bookmarks,
sum(speakers_bookmarked) as speakers_bookmarked , sum(speakers_viewed) as speakers_viewed , sum(speakers_viewed_timespent) as speakers_viewed_timespent,
sum(sessions_viewed) as sessions_viewed, sum(sessions_timespent) as sessions_timespent
from masterdata
                               group by 1,2,3", drv='SQLite')

i<-1
j<-0
x<-0
max<-c(0,0,0,0,0,0,0)
min<-c(0,0,0,0,0,0,0)
j<-0

for(j in 4:10){
  max[(j-3)]<-max(as.numeric(masterdata_agg[,j]))
  min[(j-3)]<-min(as.numeric(masterdata_agg[,j]))
}
j<-0


masterdata_agg$V11<-masterdata_agg[,4]/(max[(1)]-min[(1)])+masterdata_agg[,5]/(max[(2)]-min[(2)])+masterdata_agg[,6]/(max[(3)]-min[(3)])+
  masterdata_agg[,7]/(max[(4)]-min[(4)])+masterdata_agg[,8]/(max[(5)]-min[(5)])+masterdata_agg[,9]/(max[(6)]-min[(6)])+
  masterdata_agg[,10]/(max[(7)]-min[(7)])


a<-quantile(masterdata_agg$V11,.20) 
b<-quantile(masterdata_agg$V11,.80) 

i=0


masterdata_agg$grade_score<-ifelse(masterdata_agg$V11<=a,'C',ifelse(masterdata_agg$V11 >a & masterdata_agg$V11 <=b ,'B','A'))

str(masterdata_agg)

masterdata_agg_topic1<-sqldf("select userid, applicationid,
                                  case when grade_score=='A' then topic end as topicA,
                                  case when grade_score=='B' then topic end as topicB,
                                  case when grade_score=='C' then topic end as topicC
                                  from masterdata_agg
                                  group by 1,2", drv='SQLite' )


#masterdata_agg_topic1_day<-masterdata_agg_topic1[which(masterdata_agg_topic1$applicationid=='8df6d9e3-973e-4502-bf86-0b2efcf591bd'),]

masterdata_agg_topic1_day<-sqldf("select userid, applicationid,coalesce(topicA,topicB,topicC) as topic_1 from masterdata_agg_topic1",drv='SQLite')

masterdata_agg_topic12_day<-sqldf("select a.userid as userid, a.applicationid
                               as applicationid,a.topic_1 as topic_1,b.topic_1 as topic_2,b.rand as rand from masterdata_agg_topic1_day a
                                  join (select userid,applicationid,topic_1,random() as rand from masterdata_agg_topic1_day) b
                                  on a.applicationid=b.applicationid
                                  where a.topic_1!=b.topic_1",drv='SQLite')

masterdata_agg_topic12<-sqldf("select userid, applicationid,max(topic_1) as topic_1,max(rand) as rand
                               from masterdata_agg_topic12_day 
                                group by 1,2 ",drv='SQLite')

masterdata_agg_topic_thez<-sqldf("select a.userid, a.applicationid, a.topic_1, b.topic_2
                               from masterdata_agg_topic12 a
                                join masterdata_agg_topic12_day b
                                on
                                a.userid=b.userid
                               and a.applicationid=b.applicationid
and a.rand=b.rand ",drv='SQLite')


#write.csv(masterdata_agg_topic1_day,"masterdata_agg_topic1_day.csv")

##masterdata_agg_topic1_day<-read.csv("masterdata_agg_topic1_day.csv")

#masterdata_agg_topic1_day$topic_1<-tolower(masterdata_agg_topic1_day$topic_1)
#masterdata_agg_topic1_day$topic_2<-tolower(masterdata_agg_topic1_day$topic_2)

query<-paste("select userid,applicationid from authdb_is_users where lower(applicationid) in", id , sep="")

all_users_day<-dbGetQuery(con,query )

all_users_day<-sqldf("select a.userid, a.applicationid, b.userid as user2 from all_users_day a left join masterdata_agg_topic1_day b on a.userid=b.userid",drv='SQLite')


all_users_day<-all_users_day[which(is.na(all_users_day$user2)),]

set.seed(123)
split <- sample(seq_len(nrow(all_users_day)), size = floor(0.50 * nrow(all_users_day)))
random_all <- all_users_day[split, ]
random_high_low <- all_users_day[-split, ]

## creating the dataset for highest and lowest topic and storing it in random_high_low

masterdata_agg_high_low_count<-sqldf(" select applicationid,topic_1, count(1) as num
                                  from masterdata_agg_topic1_day
                                  group by 1,2", drv='SQLite' )
masterdata_agg_high<-sqldf(" select applicationid,topic_1, max(num)
                                  from masterdata_agg_high_low_count
                               group by 1", drv='SQLite' )
masterdata_agg_low<-sqldf(" select applicationid,topic_1, min(num)
                                  from masterdata_agg_high_low_count
                          group by 1", drv='SQLite' )

random_high_low<-sqldf(" select a.userid,a.applicationid,b.topic_1, c.topic_1 as topic_2
                                  from random_high_low a
                                  join
                                  masterdata_agg_high b
                                  on lower(a.applicationid)=lower(b.applicationid)
                                  join
                                  masterdata_agg_low c
                                 on lower(a.applicationid)=lower(c.applicationid)", drv='SQLite' )

## creating dataset for all random topics

random_all_stag<-sqldf(" select a.userid,a.applicationid,b.topic_1, b.topic_2, random() as rand from
                    random_all a
                       join
                       masterdata_agg_topic_thez b
                       on lower(a.applicationid)=lower(b.applicationid)", drv='SQLite' )

random_all_stag2<-sqldf(" select a.userid,a.applicationid,a.topic_1, a.topic_2, max(rand) from
                  random_all_stag a
                        group by 1,2", drv='SQLite' )
random_all<-random_all_stag2[,c(1:4)]


### adding identification type column to all the datasets

random_all$identification_type<-'random'
random_high_low$identification_type<-'random_high_low'
masterdata_agg_topic_thez$identification_type<-'thez_identified'

## creating final dataset
chat_out_data<-rbind(masterdata_agg_topic_thez,random_high_low,random_all)

## getting event_name

query<-paste("select applicationid, name,description,shortname from authdb_applications where lower(applicationid) in", id , sep="")
event_name<-dbGetQuery(con,query )

chat_out_data<-sqldf(" select a.*, b.name as event_name from
                  chat_out_data a
join event_name b on lower(a.applicationid)=lower(b.applicationid)", drv='SQLite' )

chat_out_data$applicationid=tolower(chat_out_data$applicationid)
chat_out_data$topic_1=tolower(chat_out_data$topic_1)
chat_out_data$topic_2=tolower(chat_out_data$topic_2)
chat_out_data$identification_type=tolower(chat_out_data$identification_type)

ch<-chat_out_data[which(chat_out_data$userid==46589912),]
test_data<-ch

test_data<-chat_out_data[which(chat_out_data$applicationid=='78b847e0-e971-4f00-a9ab-8cb24f968c53'),]

#2b7f174b-b8f3-4462-b607-58ac9f10a8e7
#78b847e0-e971-4f00-a9ab-8cb24f968c53



#test_data<-test_data[which(test_data$userid==48049290),]

#test_data$userid=46944526
# test_data$applicationid= 'bffee970-c8b3-4a2d-89ef-a9c012000abb'




#47789637
#48117886
#47775322
#47398939


library(rjson)
x <- toJSON(unname(split(test_data, 1:nrow(test_data))))
cat(x)


library(httr)
r <- POST("http://10.208.1.238:8080/stream/ashim_chat", 
          body = x)
stop_for_status(r)
content(r, "parsed", "application/json")

library(jsonlite)


chat_out <- fromJSON("http://10.208.1.238:8080/stream/chat_in/0-")

write.csv(chat_out,"chat_results.csv")

chat_out2<-sqldf("select * from chat_out where application_id='BFFEE970-C8B3-4A2D-89EF-A9C012000ABB'
                 and body in ('1','2')",drv="SQLite")
chat_pride<-sqldf("select * from chat_out where application_id='BFFEE970-C8B3-4A2D-89EF-A9C012000ABB'
                  ",drv="SQLite")

x<-ggplot(chat_out2,aes(x=body))+
  geom_bar(aes(fill=body))+ ggtitle("Cleaned Responses on Pride")

chat_out1<-sqldf("select * from chat_out where application_id='C41734AD-7747-402B-9AED-2F1538628F59'
                 and body in ('1','2')",drv="SQLite")


y<-ggplot(chat_out1,aes(x=body))+
  geom_bar(aes(fill=body))+ ggtitle("Responses on Gtec 2016 app")

z<-ggplot(chat_pride,aes(x=body))+
  geom_bar()+  ggtitle("Uncleaned Responses on Pride")+
  coord_flip()

grid.arrange(x,y,z, nrow=2,ncol=2)



#### Not applicable currently

