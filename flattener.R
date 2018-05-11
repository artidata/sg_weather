library(data.table)
library(jsonlite)
library(stringr)

dtMetadata <- fread("processed_data/JSON/metadata 20180508.csv")
measurement <- colnames(dtMetadata[,-1])
date <- dtMetadata[,date]
dir.create("processed_data/CSV")
# Verification: Does the station change?
dtStation <- data.table()
for(i in date){
  print(i)
  for(j in measurement){
    if(dtMetadata[date==i,..j]>0){
      ls <-  read_json(str_c("processed_data/JSON/",i,"/",j,".json"))
      dtFoo <- data.table(do.call(rbind,lapply(ls$metadata$stations,
                                          function(x) unlist(x[c("id","device_id","name")]))),
                          do.call(rbind,lapply(ls$metadata$stations,
                                          function(x) unlist(x$location))))
      dtFoo <- setcolorder(dtFoo,c("id","device_id","name","latitude","longitude"))
      if(nrow(dtStation)==0){
        dtStation <- dtFoo
      }else{
        print(fsetdiff(dtFoo,dtStation))
        dtStation <- funion(dtStation,dtFoo)
      }
    }
  }
}

dtStation[id %in% (dtStation[,.N,id][N>1,id])]
#     id device_id                    name latitude longitude
#1: S122      S122          Sembawang Road   1.4173  103.8249
#2: S24B      S24B Upper Changi Road North   1.3678  103.9980
#3: S113      S113      Marine Parade Road   1.3066  103.9107
#4: S24B      S24B Upper Changi Road North 103.9980    1.3678
#5: S122      S122          Sembawang Road 103.8249    1.4173
#6: S113      S113      Marine Parade Road   1.3065  103.9104
#remove 4 and 5 due to switched lat lon
dtStation <- dtStation[!(id=="S24B"&latitude==103.9980)]
dtStation <- dtStation[!(id=="S122"&latitude==103.8249)]
# remove 6 almost the samewith 4 
dtStation <- dtStation[!(id=="S113"&latitude==1.3065)]
dtStation[id=="S97"]
#    id device_id             name latitude longitude
#1: S97       S97 Pioneer Sector 2 103.6641    1.2939
# lat and lon ar switched
dtStation[id=="S97",":="(latitude = 1.2939,
                         longitude = 103.6641)]


fwrite(dtStation,"processed_data/CSV/station.csv")

date2 <- date[which(date=="2016-12-09"):length(date)] #The dates when the measurement are consistent
# WARNING 2017-03-23 wind-direction error on 2017-03-23T18:09:59+08:00 value missing, the JSON file is manually corrected
ptm <- proc.time()
for(i in date2){
  print(i)
  dir.create(str_c("processed_data/CSV/",i))
  for(j in measurement){
    ls <-  read_json(str_c("processed_data/JSON/",i,"/",j,".json"))
    dtJ <- rbindlist(lapply(ls$items, function(x) data.table(
      timestamp = x$timestamp[[1]],
      do.call(rbind,(lapply(x$readings,
                            function(y) cbind(station = y$station[[1]],
                                              value = y$value[[1]])))))))
    dtJ[,time:=str_sub(timestamp,12,19)]
    dtJ <- dtJ[,!"timestamp"]
    setcolorder(dtJ,c("time","station","value"))
    if(anyDuplicated(dtJ[,.(time,station),])>0){ #sanity check for consistency
      print("error")
      }
    dtJ[,foo:=rleid(station),time]
    dtJ[foo>1,time:=NA]
    dtJ[,foo:=NULL]
    fwrite(dtJ,str_c("processed_data/CSV/",i,"/",j,".csv"))
  }
}
proc.time()-ptm

#  user  system elapsed 
#979.22    7.80 1085.53 



