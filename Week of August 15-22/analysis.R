pw <- {
  "s0.Much.Data"
}

id<-paste("('c41734ad-7747-402b-9aed-2f1538628f59',","'09e5d674-d88a-403a-93eb-284ea96cb154',",
          "'984d1e55-e0c3-4869-8454-a39208bae596',","'8df6d9e3-973e-4502-bf86-0b2efcf591bd',","'b7d7a15a-c97a-49d0-ae71-e5aa771f555d',"
          ,"'72f5bbe3-7be2-4f47-81e2-76515a98709f',","'b06d2395-4518-4bbf-909f-15d697dbd435',","'8830c21b-09f8-42f8-a3dc-43f10eb6e83b',",
          "'2b7f174b-b8f3-4462-b607-58ac9f10a8e7',","'78b847e0-e971-4f00-a9ab-8cb24f968c53',","'c0890d72-8b62-47e7-b501-bb8bee65d16d',",
          "'0080a67a-a60e-4ede-80ae-53b292702629',","'ea183740-427d-4e9b-8d9c-5c2c92468b61',","'0b6979d8-fd99-4965-b978-7b805a4474bd',",
          "'4e57fcb0-d11e-41ce-a69e-4d4415a77a5f',","'d84b18ac-b91e-4091-a3a8-93278dce977e',","'03a6e58d-dc23-4c9d-b37c-5126d72245bc')")


#setwd("/Users/ashimdatta/Enterprise/chat experiments/Week of Sep 5- Sep 11")
getwd()
library("DBI")
library("RPostgreSQL")
library("sqldf")
library("ggplot2")
library("gridExtra")
library("plyr")
library("dplyr")
library(jsonlite)
library(rjson)
library("RCurl")
library("XML")

## Replies
data4 <- fromJSON(getURL("http://10.208.1.238:8080/stream/chat_in/10001-15000"))

asFrame <- do.call("rbind.fill", lapply(data4, as.data.frame))

#chat_in <- asFrame

chat_in <- rbind(asFrame,chat_in)


### chatout

data5 <- fromJSON(getURL("http://10.208.1.238:8080/stream/ashim_chat/15001-20000"))
asFrame1 <- do.call("rbind.fill", lapply(data5, as.data.frame))

#chat_out <- asFrame1
#chat_out<-chat_out[,c(1,2,3,7,8,9)]

chat_out <- rbind(asFrame1,chat_out)

## chat out recent

query<-paste("select * from chat_out where lower(applicationid) in ", id)

chat_out_rec<-chat_out[which(chat_out$identification_type!='NULL'),]


#write.csv(chat_out,"chat_out.csv")
## chat_in recent

query<-paste("select * from chat_in where lower(application_id) in ", id)

chat_in<-sqldf(query,drv='SQLite')


#chk<-chat_in_out_rec[which(tolower(chat_in_out_rec$application_id)=='03a6e58d-dc23-4c9d-b37c-5126d72245bc' #| chat_in_rec$index==3
                        #   ),]

chat_in_rec<-chat_in[which(chat_in$index==2 #| chat_in_rec$index==3
                               ),]



## chat_out for the valid chatins

chat_in_out_rec<-merge(chat_in_rec,chat_out_rec, by.x=c("user_id"),
                        by.y=c("userid"))


##chat_in_out_thez_identified

chat_in_out_rec_thez_identified<-chat_in_out_rec[which(chat_in_out_rec$identification_type=='thez_identified'),]
chat_in_out_rec_thez_identified<-chat_in_out_rec_thez_identified[
  which(chat_in_out_rec_thez_identified$body==1 | chat_in_out_rec_thez_identified$body==2),]

accuracy_thez<-nrow(chat_in_out_rec_thez_identified[which(chat_in_out_rec_thez_identified$body==1),])/nrow(chat_in_out_rec_thez_identified)

chat_in_out_rec_thez_identified_agg <- sqldf("select body, count(1) as num_records from chat_in_out_rec_thez_identified
                                           group by 1", drv='SQLite')

chat_in_out_rec_thez_identified_tot<-sum(chat_in_out_rec_thez_identified_agg$num_records)
chat_in_out_rec_thez_identified_agg$percent<-chat_in_out_rec_thez_identified_agg$num_records/chat_in_out_rec_thez_identified_tot

z<-ggplot(chat_in_out_rec_thez_identified_agg,aes(x=body,y=(round(percent,2)*100),fill=body))+
  geom_bar(stat='identity')+  xlab("1=correct answer, 2=wrong answer")+
  ylab("Percent of replies")+ggtitle("Responses for thez_identified")



##chat_in_out_random

chat_in_out_rec_random<-chat_in_out_rec[which(chat_in_out_rec$identification_type=='random'),]
chat_in_out_rec_random<-chat_in_out_rec_random[
  which(chat_in_out_rec_random$body==1 | chat_in_out_rec_random$body==2),]

chat_in_out_rec_random_agg<-sqldf("select body, count(1) as num_records from chat_in_out_rec_random
                                           group by 1", drv='SQLite')
chat_in_out_rec_random_tot<-sum(chat_in_out_rec_random_agg$num_records)
chat_in_out_rec_random_agg$percent<-chat_in_out_rec_random_agg$num_records/chat_in_out_rec_random_tot



k<-ggplot(chat_in_out_rec_random_agg,aes(x=body,y=(round(percent,2)*100),fill=body))+
  geom_bar(stat='identity')+  xlab("1=correct answer, 2=wrong answer")+
  ylab("Percent of replies")+ggtitle("Responses for random topics")



#chat_in_out_random_high_low


chat_in_out_rec_random_hl<-chat_in_out_rec[which(chat_in_out_rec$identification_type=='random_high_low'),]
chat_in_out_rec_random_hl<-chat_in_out_rec_random_hl[
  which(chat_in_out_rec_random_hl$body==1 | chat_in_out_rec_random_hl$body==2),]

chat_in_out_rec_random_hl_agg<-sqldf("select body, count(1) as num_records from chat_in_out_rec_random_hl
                                           group by 1", drv='SQLite')
chat_in_out_rec_random_hl_tot<-sum(chat_in_out_rec_random_hl_agg$num_records)
chat_in_out_rec_random_hl_agg$percent<-chat_in_out_rec_random_hl_agg$num_records/chat_in_out_rec_random_hl_tot



l<-ggplot(chat_in_out_rec_random_hl_agg,aes(x=body,y=(round(percent,2)*100),fill=body))+
  geom_bar(stat='identity')+  xlab(" 1=Most popular topic in the event, 2= least popular topic in the event")+
  ylab("Percent of replies")+ggtitle("Responses when the most popular topic and least popular topic were shown")




grid.arrange(k,l,z, nrow=2,ncol=2)
