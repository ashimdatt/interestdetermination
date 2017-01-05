pw <- {
  "s0.Much.Data"
}
#setwd("/Users/ashimdatta/topic validation")
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


masterdata<-dbGetQuery(con,"select a.itemid,lower(a.topic) as manual_topics,lower(b.topic) generated_topics from ben.session_topics_manual a
join ben.session_tags_v0 b
on a.itemid=b.itemid")

manual_matches<-sqldf("select a.itemid, a.manual_topics, b.generated_topics
                      from masterdata a join
                      masterdata b
                      on a.itemid=b.itemid
                      and a.manual_topics=b.generated_topics", drv="SQLite")

manual_not_matches<-sqldf("select a.itemid, a.manual_topics, b.generated_topics
                      from masterdata a left join
                      masterdata b
                      on a.itemid=b.itemid
                      and a.manual_topics=b.generated_topics
                          where b.generated_topics is null", drv="SQLite")

manual_matches_distinct_count<-sqldf("select itemid, count(distinct manual_topics) as num_manual_matches
                                     from manual_matches group by 1", drv="SQLite")
manual_notmatches_distinct_count<-sqldf("select itemid, count(distinct manual_topics) as num_manual_notmatches
                                     from manual_not_matches group by 1", drv="SQLite")

manual_total<-sqldf("select itemid, count(distinct manual_topics) as num_manual
                                     from masterdata group by 1", drv="SQLite")
generated_total<-sqldf("select itemid, count(distinct generated_topics) as num_generated
                                     from masterdata group by 1", drv="SQLite")

combine_dataset1<-sqldf("select a.itemid , a.num_manual, b.num_generated
from manual_total a join generated_total b
on a.itemid=b.itemid", drv="SQLite")

combine_dataset2<-sqldf("select a.itemid , a.num_manual, a.num_generated, b.num_manual_matches
from combine_dataset1 a left join manual_matches_distinct_count b
                        on a.itemid=b.itemid", drv="SQLite")
combine_dataset3<-sqldf("select a.itemid , a.num_manual, a.num_generated, a.num_manual_matches, b.num_manual_notmatches
from combine_dataset2 a left join manual_notmatches_distinct_count b
                        on a.itemid=b.itemid", drv="SQLite")

topic_validation<-combine_dataset3
topic_validation[is.na(topic_validation)] <- 0

topic_validation$precision<- topic_validation$num_manual_matches/topic_validation$num_generated

topic_validation$recall<- topic_validation$num_manual_matches/topic_validation$num_manual

dbGetQuery(con,"DELETE FROM ashim.topic_tagging_validation"  )

dbWriteTable(con, c("ashim", "topic_tagging_validation"), value= topic_validation, append=TRUE, row.names=FALSE)


corr_eqn <- function(x,y, digits = 2) {
  corr_coef <- round(cor(x, y), digits = digits)
  corr_coef<-paste("r = ", corr_coef)
  return(corr_coef)
}

x<-ggplot(topic_validation, aes(x=recall)) +
  geom_histogram(binwidth=.1, colour="black", fill="white")+
  xlab("Recall") + ylab("Frequency") +
  ggtitle("Recall distribution")


y<-ggplot(topic_validation, aes(x=precision)) +
  geom_histogram(binwidth=.1, colour="black", fill="white")+
  xlab("Precision") + ylab("Frequency") +
  ggtitle("Precision distribution")


z<-ggplot(topic_validation, aes(x = recall, y = precision))+geom_point(aes(color=-precision))+ geom_smooth(method = "lm", se = TRUE)+
theme(axis.text.x = element_text(hjust = .3, size = 8),
      axis.title=element_text(size=8))+
  geom_text(x = .5, y = .5,
            label = corr_eqn(topic_validation$recall,
                             topic_validation$precision), parse = TRUE)+
  xlab("Recall") + ylab("Precision") +
  ggtitle("Topic validation- Recall vs Precision")

pdf("validation_plots.pdf")
x
y
z
dev.off()