# First time data downloading - might be very large (>1Gb)

firstTime <- function(startdate='2012-10-1'){
  require(lubridate)
  start <- as.Date(startdate)
  endday <- as.Date(today())-5 # omit today because the data is not ready
  all_days <- seq(start, endday, by='day') 
  
  year <- as.POSIXlt(all_days)$year + 1900
  urls <- paste0('http://cran-logs.rstudio.com/', year, '/', all_days, '.csv.gz')
  filenames =  paste0('data/',all_days,'.csv.gz')
  for (i in 1:length(urls)){
    download.file(urls[i], filenames[i])
  }
  data <- loadCSV(filenames[1])
  for (i in 2:length(filenames)){
    data <- bind_rows(data, loadCSV(filenames[i]))
  }
  save(data, file='data/allRecord.RData')
}

## Load CVS data into dplyr 
loadCSV <- function(filename){
  require(dplyr)
  require(lubridate)
  data <- read.csv(gzfile(filename), stringsAsFactors=F)
  data <- tbl_df(data) 
  data %>% transmute(timestamp=ymd_hms(paste(date,time)),
                     size=size,
                     r_version=r_version,
                     r_arch=r_arch,
                     r_os=r_os,
                     package=package,
                     version=version,
                     country=country,
                     ipID=ip_id)
}

firstTime()
