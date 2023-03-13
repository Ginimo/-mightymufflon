##### Import libraries


library(tidyverse)
library(rvest)
library(RSelenium)
library(ggplot2)


#### Set WD


setwd("C:/Users/Frosty/Documents/TaskScheduler/")


##### Selenium


rD <- rsDriver(port = 4449L, 
               browser = "firefox")

remDr <- rD[["client"]]
#https://store.steampowered.com/search/
remDr$navigate("https://store.steampowered.com/search/?category1=998%2C21&filter=topsellers")

for (i in 1:50){
  webElem <- remDr$findElement("css", "body")
  webElem$sendKeysToElement(list(key = "end"))
  Sys.sleep(1)
}

webpage <- read_html(remDr$getPageSource()[[1]])

#Sys.sleep(2)
#remDr.close()
rD[["server"]]$stop()


#### Scrape Steam Topseller - Basic Infos


get_links <- function(topsellers_html){
  
search_results <- topsellers_html %>% 
    html_nodes(xpath = '//*[@id="search_resultsRows"]')
  
titles <- search_results %>% 
    html_nodes(".title") %>% 
    html_text()
  
hrefs <- search_results %>% 
    html_elements("a") %>% 
    html_attr('href')
  
rel_date <- search_results %>% 
    html_nodes(".col.search_released.responsive_secondrow") %>% 
    html_text()
  
  
return(data.frame(
  titles, hrefs, rel_date
  ))
}

df_more_games<- get_links(webpage)


#### Scrape Steam Topseller - Detailed Infos per Game


get_infos <- function(url){
  
url_xml <- read_html(url)

ratings <- url_xml %>%
  #html_nodes(xpath ='//*[@id="userReviews"]/div/div[2]/span[3]') %>%  
  html_nodes(xpath = '//*[@id="userReviews"]/div[2]/div[2]/span[3]') %>%     
  html_text()

if (length(ratings)!= 1){
  ratings <- url_xml %>%
    html_nodes(xpath ='//*[@id="userReviews"]/div/div[2]/span[3]') %>%     
    html_text()
}

tot_ratings <- ratings[1] %>% 
  str_match("the\\s*(.*?)\\s*user")
tot_ratings <-tot_ratings[,2]

block_infos <- url_xml %>% 
  html_nodes(xpath = '//*[@id="genresAndManufacturer"]') %>% 
  html_text()

title <- block_infos %>% 
  str_match("Title:\\s*(.*?)\\s*Genre:")
title <- title[,2]

genres <- block_infos %>% 
  str_match("Genre:\\s*(.*?)\\s*Developer:")
genres <- genres[,2]

developer <- block_infos %>% 
  str_match("Developer:\\s*(.*?)\\s*Publisher:")
developer <- developer[,2]

publisher <- block_infos %>% 
  str_match("Publisher:\\s*(.*?)\\s*Franchise:")
publisher <- publisher[,2]

franchise <- block_infos %>% 
  str_match("Franchise:\\s*(.*?)\\s*Release")
franchise <- franchise[,2]

}  
  
  
  # return(data.frame(
  #   title, genres, developer, publisher, franchise, ratings, tot_ratings))
  
return(data.frame(title= ifelse(is.null(title),NA,title),
                  genres = ifelse(is.null(genres),NA,genres),
                  developer = ifelse(is.null(developer),NA,developer),
                  publisher = ifelse(is.null(publisher),NA,publisher),
                  franchise = ifelse(is.null(franchise),NA,franchise),
                  ratings = ifelse(is.null(ratings),NA,ratings),
                  tot_ratings = ifelse(is.null(tot_ratings),NA,tot_ratings)))
  
  
#### Bind all information to one df
  
  
df_infos <- data.frame()
for (i in 1:nrow(df_more_games)){
print(i)
row <- cbind(title_old_df = df_more_games[i, 1],
               link_old_df = df_more_games[i, 2],
               get_infos(df_more_games[i, 2]))
df_infos <- rbind(df_infos, row)
}
  
#### Data Transformation
  
  
df_infos_2 <- df_infos
  # df_infos_2$ratings <- df_infos_2$ratings %>% 
  #   substr(1, 3)
  #s <- df_infos$ratings[1]
  
ratings_perc <-c()
for (i in 1:nrow(df_infos_2)){
  perc = regmatches(df_infos_2$ratings[i], 
                    gregexpr("\\d+(\\.\\d+){0,1}%", df_infos_2$ratings[i]))[[1]] %>% 
    as.character()
  if (length(perc) != 0){
    ratings_perc <- c(ratings_perc, perc)
  }
  else{
    ratings_perc <- c(ratings_perc, "NA")
  }
  
}
  
ratings_perc2 <-  gsub('%', '', ratings_perc) %>% 
  as.numeric()

df_infos_3 <- data.frame(df_infos_2, ratings_perc2)

df_infos_4 <- df_infos_3 

df_infos_4$tot_ratings <- gsub(',', '', df_infos_4$tot_ratings) %>% 
  as.numeric()

df_infos_4 <- df_infos_4 %>% 
  dplyr::mutate(neg_ratings = ((100-ratings_perc2) / 100)*tot_ratings, 
                pos_ratings = ratings_perc2/100*tot_ratings)

df_infos_4 <- df_infos_4 %>% 
  dplyr::mutate(title1 = title_old_df, link = link_old_df,  pos_ratings_perc = ratings_perc2) %>% 
  dplyr::select(title1, link, title, genres, developer, 
                publisher, franchise, tot_ratings, pos_ratings, neg_ratings, pos_ratings_perc)
  
  #write.table(df_infos_4, "topselling_df_04.csv", sep = "\t", dec = ".", row.names = FALSE)
  
df_infos_4 <- df_infos_4 %>% 
  dplyr::filter(!is.na(title))
  
  
  #write.table(df_infos_4, "topselling_df_05.csv", sep = "\t", dec = ".", row.names = FALSE)
  
  
#### Scrape Gamestats and combine all Infos
 
   
  #https://games-stats.com/steam/tags/?sort=revenue-total
  # get titles in format:
  # all lowercase
  # replace spaces with '-'
  # replace all special symbols
  # replace ™-symbol with 'tm'
  # replace ®-symbol with 'r'
  
  # convert titles:
  
titles_old <- df_infos_4$title1 %>% 
  tolower()

titles_new <- titles_old %>% 
  str_replace_all(c('™'= 'tm', '®' = 'r', ' ' = '-', ':' = '', '`?`' = '')) %>% 
  str_replace_all(c('---' = '-', '!' = '', "'" = ""))

  # get a list of urls: 
  
links_gamestat <- data.frame()
for (i in 1:length(titles_new)){
  r <- paste0("https://games-stats.com/steam/game/", titles_new[i] , "/")
  row <- cbind(df_infos_4$title1[i], r)
  links_gamestat <- rbind(links_gamestat, row)
}
  
  
  # function for getting revenue value:
  
get_rev <- function(url_rev){
  
out <- tryCatch(
    {
      url_xml <- read_html(url_rev)
      
      rev <- url_xml %>% 
        html_nodes(xpath = "/html/body/section/div/div/div[1]/div[1]/p[14]/text()") %>% 
        html_text()
      
      if (length(rev) == 0){
        return("NA")
      }
      else{
        return(rev)
      }
      
    }, 
    error=function(cond){
      return("NA")
    })
  
}
  
#see <- get_rev("https://games-stats.com/steam/game/ea-sportstm-fifa-23/")
# 360 - 400
  
df_rev <- data.frame()
for (i in 1:nrow(links_gamestat)){
  rev <- get_rev(links_gamestat[i, 2])
  print(i)
  row <- cbind(df_infos_4$title1[i], links_gamestat[i, 2], rev)
  df_rev <- rbind(df_rev, row)
}

  
# transform i.e. clean rev-variable strings:

df_rev2 <- df_rev 

df_rev2$rev <- df_rev2$rev %>% 
  str_replace_all(c('~'= '', ',' = '', ": " = "")) 

df_rev2$rev <- gsub("\\$", "", df_rev2$rev)

df_rev3 <- df_rev2 %>% 
  dplyr::mutate(mill = grepl("million", rev, fixed = TRUE))

df_rev3$rev <- df_rev3$rev %>% 
  str_replace_all(c('million'= '')) 

df_rev3$rev <- df_rev3$rev %>% 
  as.numeric()

df_rev3$mill[df_rev3$mill == TRUE] <- 1000000
df_rev3$mill[df_rev3$mill == FALSE] <- 1

df_rev4 <- df_rev3 %>% 
  dplyr::mutate(revenue = rev*mill) %>% 
  dplyr::select(-rev, -mill)

  
  
  #write.table(df_rev4, "df_rev01.csv", dec = ".", sep= "\t", row.names = FALSE)
  

names(df_rev4)[names(df_rev4)=="V1"] <- "title1"
names(df_rev4)[names(df_rev4)=="V2"] <- "link_gamestat"
  
# Join dfs:

df_all <- full_join(df_infos_4, df_rev4, by = c("title1"))

write.table(df_all, "steamstore.csv", row.names = FALSE, sep = "\t", dec= ".")