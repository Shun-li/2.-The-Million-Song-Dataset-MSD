data1 <- read.csv("/Users/shunli/Desktop/song-popularity/part-00000-93986cea-7f83-4b7a-8f0d-80aeb1ed8873-c000.csv")
data2 <- read.csv("/Users/shunli/Desktop/song-popularity/part-00001-93986cea-7f83-4b7a-8f0d-80aeb1ed8873-c000.csv")
data3 <- read.csv("/Users/shunli/Desktop/song-popularity/part-00002-93986cea-7f83-4b7a-8f0d-80aeb1ed8873-c000.csv")
data4 <- read.csv("/Users/shunli/Desktop/song-popularity/part-00003-93986cea-7f83-4b7a-8f0d-80aeb1ed8873-c000.csv")
View(data1)
data <- data.frame(rbind(data1,data2))
View(data)
library(dplyr)
data$popularity <- case_when(
  data$count >= 10000 ~ "most popular",
  data$count <= 10000 & data$count >= 5000 ~ "moderately popular",
  data$count <= 5000 & data$count >= 1000 ~ "Below average popular",
  data$count <= 1000 & data$count >= 100~ "not popular",
  data$count <= 100 ~ "dislike")
library(ggplot2)  
ggplot(data,aes(x=data$popularity))+geom_bar()+xlab("song popularity")+ggtitle("song polularity")  
