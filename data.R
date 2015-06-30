#' 
#' 
#'

#' Data Path to files
DataPath = './data'

#' Download function
#' @description Download CRAN package downloading log data.  Should not be called directly.
#' @note First time data downloading - might be very large (>2Gb)
CRANlog.download <- function(startdate='2012-10-1', enddate = '2016-1-1'){
  require(lubridate)
  startday <- as.Date(startdate)
  endday <- as.Date(enddate) # omit today because the data is not ready
  all_days <- seq(startday, endday, by='day') 
  
  years <- as.POSIXlt(all_days)$year + 1900
  urls <- paste0('http://cran-logs.rstudio.com/', years, '/', all_days, '.csv.gz')
  filenames =  paste0(DataPath,'/',all_days,'.csv.gz')
  
  # progress report
  pr <- txtProgressBar(max=length(urls))
  
  tryCatch({
    for (i in 1:length(urls)){
      download.file(urls[i], filenames[i], quiet=TRUE)
      setTxtProgressBar(pr, i)
    }
  },
  error = function(e){
    message(e)
  }
  )
}

#' Merge logs into one File
CRANlog.merge <- function(filename='allRecord.RData'){
  require(data.table)
  require(stringr)
  filename = paste0(DataPath,'/', filename)
  filenames = list.files(DataPath,'*.csv.gz', full.names = T)
  if (file.exists(filename)){
    # load exist file, find the lastest date
    olddata = CRANlog.load(filename)
    dates = str_extract_all(filenames, '\\d{4}-\\d{2}-\\d{2}')
    dates = setdiff(strftime(olddata$timestamp,'%Y-%m-%d'), dates)
    filenames = paste0(DatePath,'/',dates, '.csv.gz')  
  } 
  data <- rbindlist(lapply(filenames, loadCSV))
  if (exists('olddata')){
    data <- rbindlist(list(data,olddata))
  }
  save(data, file=filename)
}

#' Load saved log data
#' Currently it is an RData
#' @param RData file name (with path)
#' @return data.talbe
CRANlog.load <- function(filename){
  load(filename)
  return(data)
}

#' Load CVS data into dplyr 
#' @description Load CVS data into dplyr
loadCSV <- function(filename){
  require(dplyr)
  require(lubridate)
  require(data.table)
  data <- read.csv(gzfile(filename), stringsAsFactors=F)
  data <- data.table(data) 
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

CRANlog.merge()

