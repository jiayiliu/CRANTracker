#' 
#' 
#'

#' Data Path to files
DataPath = './data'

#' Download CRAN log files
#' @description Download CRAN package downloading log files.
#' @param startdate Start date to download.  The earliest date is 2012-10-1
#' @param enddate End date to download.  Typically the data is available several days late.
#' @param overwrite Whether to overwrite downloaded files
CRANlog.download <- function(startdate='2012-10-1',
                             enddate = '2016-1-1',
                             overwrite=FALSE){
  require(lubridate)
  startday <- as.Date(startdate)
  endday <- as.Date(enddate) # omit today because the data is not ready
  all_days <- seq(startday, endday, by='day') 
  # create filenames  
  years <- as.POSIXlt(all_days)$year + 1900
  urls <- paste0('http://cran-logs.rstudio.com/', years, '/', all_days, '.csv.gz')
  filenames =  paste0(DataPath,'/',all_days,'.csv.gz')
  # create progress report
  pr <- txtProgressBar(max=length(urls))
  # Start downloading
  tryCatch({
    for (i in 1:length(urls)){
      if (!file.exists(filenames[i])){
        download.file(urls[i], filenames[i], quiet=TRUE)
      } else{
        if (overwrite){
          download.file(urls[i], filenames[i], quiet=TRUE)
        }
      }
      setTxtProgressBar(pr, i)
    }
  },
  error = function(e){
    print(paste0("Downloading failed with ",filenames[i]))
    system(paste('rm',filenames[i]))
  },
  finally = NULL
  )
}

#' Merge logs into one File
#' @param filename The file to store RData
CRANlog.merge <- function(filename='allRecord.RData'){
  require(data.table)
  require(stringr)
  filename = paste0(DataPath,'/', filename)
  filenames = list.files(DataPath,'*.csv.gz', full.names = T)
  dates = str_extract_all(filenames, '\\d{4}-\\d{2}-\\d{2}')
  if (file.exists(filename)){
    # load exist file, find the lastest date
    olddata = CRANlog.load(filename)
    dates = setdiff(strftime(olddata$timestamp,'%Y-%m-%d'), dates)
  } 
  filenames = paste0(DataPath,'/',dates, '.csv.gz')  
  data <- rbindlist(lapply(filenames, CRANlog.loadCSV))
  if (exists('olddata')){
    data <- rbindlist(list(data,olddata))
  }
  save(data, file=filename)
}

#' Insert logs into Database
#' @param first Flag marks whether want to drop existed table
CRANlog.insert <- function(tablename='logs', first=FALSE){
  require(RMySQL)
  require(stringr)
  filenames = list.files(DataPath,'*.csv.gz', full.names = T)
  conn <- dbConnect(MySQL(), dbname='cran', username='jasonliu')
  dates = unlist(str_extract_all(filenames, '\\d{4}-\\d{2}-\\d{2}'))
  if (!first){ # only load un-read files
    # load exist file, find the lastest date
    olddata = dbSendQuery(conn, 'SELECT DISTINCT(DATE(timestamp)) FROM %s', tablename)
    dates = setdiff(dates, olddata$date)
  }
  filenames = paste0(DataPath,'/',dates, '.csv.gz')  
  for (i in filenames){
    if (first){
      logs <- CRANlog.loadCSV(i)
      dbWriteTable(conn, tablename, logs, 
                   field.types=list(timestamp='TIMESTAMP',
                                    size='BIGINT',
                                    r_version='VARCHAR(40)',
                                    r_arch='VARCHAR(40)',
                                    r_os='VARCHAR(40)',
                                    package='VARCHAR(40)',
                                    version='VARCHAR(40)',
                                    country='VARCHAR(40)',
                                    ipID='VARCHAR(40)'),
                   row.names=FALSE, overwrite=TRUE)
      first <- FALSE # Only refresh the table for the first time
    }
    logs <- CRANlog.loadCSV(i)
    dbWriteTable(conn, tablename, logs, overwrite=FALSE, append=TRUE, row.names=FALSE)
  }
  dbDisconnect(conn)
}

#' Load saved log RData
#' @param RData file name (with path)
#' @return data.talbe
CRANlog.load <- function(filename){
  load(filename)
  return(data)
}

#' Load CVS data into dplyr 
#' @description Load CVS data into dplyr
CRANlog.loadCSV <- function(filename){
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

################### Main Functions ###############
CRANlog.download()
#CRANlog.merge()
CRANlog.insert(first=TRUE)
