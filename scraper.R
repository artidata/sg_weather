library(jsonlite)
library(data.table)
library(stringr)

date <- as.character(seq(as.Date("2016-05-01"), as.Date("2017-06-30"), "day"))
measurement <- c("air-temperature","relative-humidity","wind-direction","wind-speed","rainfall")
dtMetadata <- data.table()

dir.create(str_c(getwd(),"/processed_data/JSON"))
set.seed(1)
for (i in sample(1:length(date),10)){
  dateI <- date[i]
  dir.create(str_c(getwd(),"/processed_data/JSON/",dateI))
  lsMeta <- list(date = date[i])

  for (j in 1:length(measurement)){
    measurementJ <- measurement[j]
    lsData <- NULL
    while(is.null(lsData)){
      try({
        lsData <- read_json(str_c("https://api.data.gov.sg/v1/environment/",measurementJ,
                                    "?date=",dateI))
      })
    }
    lsMeta[[measurementJ]] <- length(lsData$items)
    write(toJSON(lsData,pretty = T),
          str_c(getwd(),"/processed_data/JSON/",
                dateI,"/",measurementJ,".json"))
  }
  dtMetadata <- rbindlist(list(dtMetadata,as.data.table(lsMeta)))
  print(dtMetadata[date == dateI,])
}
fwrite(dtMetadata,"metadata 20180508.csv")
