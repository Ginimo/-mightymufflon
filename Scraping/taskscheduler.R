library(taskscheduleR)

#### Taskscheduler for Wiki

taskscheduler_create(
  taskname = "my_script_wiki",
  "C://Users//Frosty//Documents//TaskScheduler//my_script_wiki.R",
  schedule = "WEEKLY",
  starttime = "22:00",
  days = "SAT",
  Rexe = file.path(Sys.getenv("R_HOME"), "bin", "Rscript.exe")

)

?taskscheduleR

#### Taskscheduler for Steamstore

taskscheduler_create(
  taskname = "my_script_steamstore",
  "C://Users//Frosty//Documents//TaskScheduler//my_script_steamstore.R",
  schedule = "WEEKLY",
  starttime = "22:00",
  days = "SAT",
  Rexe = file.path(Sys.getenv("R_HOME"), "bin", "Rscript.exe")
  
)

#taskscheduler_delete(taskname = "my_script_steamstore")
