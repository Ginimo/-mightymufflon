
print("now running")
library(rvest)
library(tidyr)
library(dplyr)
library(tidyverse)



game_dev_url  <- "https://en.wikipedia.org/wiki/List_of_video_game_developers"
indie_gamedev_url <- "https://en.wikipedia.org/wiki/List_of_indie_game_developers"

##### Get Data from Wiki ##### 
  

setwd("C:/Users/Frosty/Documents/TaskScheduler/")

game_dev <- read_html (game_dev_url) %>%
  html_nodes ("table.wikitable:nth-child(11)") %>%
  html_table ()

df.game_dev <- data.frame(game_dev)
colnames(df.game_dev)[1] <- "developer"
df.game_dev <- df.game_dev %>% add_column(developer_type ="commercial")

write.csv(df.game_dev, "game_dev.csv")

indie_game_dev <- read_html (indie_gamedev_url) %>%
  html_nodes ("table.wikitable:nth-child(9)") %>%
  html_table ()

df.indie_game_dev <- data.frame(indie_game_dev)
colnames(df.indie_game_dev)[1] <- "developer"
df.indie_game_dev <- df.indie_game_dev %>% add_column(developer_type ="indie")

write.csv(df.indie_game_dev, "indie_game_dev.csv")



#Merge Commercial with Indie:

all_game_dev <- full_join(df.game_dev, df.indie_game_dev)
all_game_dev <- all_game_dev[order(all_game_dev$developer),]
write.csv(all_game_dev, "all_game_dev.csv", row.names = FALSE)

print("Wiki scraped")
#df.steam <- read.csv("steamstore.csv", sep = "\t", dec =".")
#steam_wiki_merge <-left_join(x=df.steam,y= all_game_dev,by="developer")
#write.csv(steam_wiki_merge, "steam_wiki_merge.csv", row.names = FALSE)
