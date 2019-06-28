data1 <- read.csv("/Users/shunli/Desktop/user_activity/part-00000-4d7d1a1b-b039-47c5-a67d-c614d8d5fed2-c000.csv")
data2 <- read.csv("/Users/shunli/Desktop/user_activity/part-00001-4d7d1a1b-b039-47c5-a67d-c614d8d5fed2-c000.csv")
data3 <- read.csv("/Users/shunli/Desktop/user_activity/part-00002-4d7d1a1b-b039-47c5-a67d-c614d8d5fed2-c000.csv")
data4 <- read.csv("/Users/shunli/Desktop/user_activity/part-00003-4d7d1a1b-b039-47c5-a67d-c614d8d5fed2-c000.csv")
View(data1)
data <- data.frame(rbind(data1,data2))
View(data)
library(dplyr)
data$active <- case_when(
  data$count >= 1000 ~ "most active",
  data$count <= 1000 & data$count >= 500 ~ "moderately active",
  data$count <= 500 & data$count >= 100 ~ "Below_average active",
  data$count <= 100 & data$count >= 0~ "not active")
library(ggplot2)  
ggplot(data,aes(x=data$active))+geom_bar()+xlab("user activity")+ggtitle("user activity")  
