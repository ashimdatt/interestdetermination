pw <- {
  "s0.Much.Data"
}
##setwd("/Users/ashimdatta/Enterprise/CMS usage dashboard")
getwd()

library("DBI")
library("RPostgreSQL")
library("sqldf")
library("ggplot2")
library("gridExtra")
library("plyr")
library("dplyr")

drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, dbname = "analytics",
                 host = "10.223.192.6", port = 5432,
                 user = "etl", password = pw)

eventcube_summary_raw<-dbGetQuery(con,"select a.* from eventcube.eventcubesummary a
                                  join  salesforce.implementation c
                                  on lower(a.applicationid)=lower(c.applicationid)
                                  where a.startdate > now() - interval '18 months'")

eventcube_summary_raw$percentengaged<-ifelse(eventcube_summary_raw$usersactive==0,0,eventcube_summary_raw$usersengaged/eventcube_summary_raw$usersactive)
eventcube_summary_raw$percentpost<-ifelse(eventcube_summary_raw$usersactive==0,0,eventcube_summary_raw$posts/eventcube_summary_raw$usersactive)
eventcube_summary_raw$percentlikes<-ifelse(eventcube_summary_raw$usersactive==0,0,eventcube_summary_raw$likes/eventcube_summary_raw$usersactive)
eventcube_summary_raw$percentcomments<-ifelse(eventcube_summary_raw$usersactive==0,0,eventcube_summary_raw$comments/eventcube_summary_raw$usersactive)
eventcube_summary_raw$percentcheckins<-ifelse(eventcube_summary_raw$usersactive==0,0,eventcube_summary_raw$checkins/eventcube_summary_raw$usersactive)

## replacing nulls with 0

eventcube_summary_raw2<-eventcube_summary_raw[,c(1:2,5:33, 40:79,82:86)]

eventcube_summary_raw2[is.na(eventcube_summary_raw2)] <- 0
eventcube_summary_raw[,c(1:2,5:33, 40:79,82:86)]<-eventcube_summary_raw2
str(eventcube_summary_raw)
i<-1
j<-0
x<-0
eventcube_summary_raw$V87<-0


i<-1
j<-0
x<-0
max<-c(0,0,0,0,0,0)
min<-c(0,0,0,0,0,0)


for(j in 82:86){
  max[(j-81)]<-max(eventcube_summary_raw[,j])
  
  min[(j-81)]<-min(eventcube_summary_raw[,j])
  
}

j<-0
min[6]<-min(eventcube_summary_raw[,'users'])
max[6]<-max(eventcube_summary_raw[,'users'])

eventcube_summary_raw$V87<-eventcube_summary_raw[,82]/(max[(1)]-min[(1)])+eventcube_summary_raw[,83]/(max[(2)]-min[(2)])+eventcube_summary_raw[,84]/(max[(3)]-min[(3)])+
  eventcube_summary_raw[,85]/(max[(4)]-min[(4)])+eventcube_summary_raw[,86]/(max[(5)]-min[(5)])+eventcube_summary_raw[,'users']/(max[(6)]-min[(6)])


a<-quantile(eventcube_summary_raw$V87,.33,na.rm=TRUE) 
b<-quantile(eventcube_summary_raw$V87,.66,na.rm=TRUE) 

i=0


eventcube_summary_raw$grade_score<-ifelse(eventcube_summary_raw$V87<=a & 
                                            (eventcube_summary_raw$enddate<=Sys.Date()+6 & eventcube_summary_raw$enddate>=Sys.Date())
                                          ,'C',ifelse(eventcube_summary_raw$users<=50,'B',
                                                      ifelse(eventcube_summary_raw$V87 <=b ,'B','A')))


dbGetQuery(con,"truncate table ashim.eventcube_summary_raw_thez")

dbWriteTable(con, c("ashim", "eventcube_summary_raw_thez"), value=eventcube_summary_raw,
             append=TRUE, row.names=FALSE)