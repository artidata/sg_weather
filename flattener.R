

lsRainfall <- fromJSON("https://api.data.gov.sg/v1/environment/rainfall")


lsAirTemperature <- read_json("https://api.data.gov.sg/v1/environment/air-temperature?date=2016-06-01")
cbind(rbindlist(lapply(lsAirTemperature$metadata$stations,function(x)x$location)),
      rbindlist(lapply(lsAirTemperature$metadata$stations,function(x)x[c("id","device_id","name")])))

lsRelativeHumidity <- read_json("https://api.data.gov.sg/v1/environment/relative-humidity?date=2016-06-01")
cbind(rbindlist(lapply(lsRelativeHumidity$metadata$stations,function(x)x$location)),
      rbindlist(lapply(lsRelativeHumidity$metadata$stations,function(x)x[c("id","device_id","name")])))


rbindlist(lapply(lsAirTemperature$items, function(x) x["timestamp"][1] ))
lapply(lapply(lsAirTemperature$items, 
              function(x) lapply(x["readings"], 
                                 function(y) rbindlist(y))), 
       function(z) z$r)

foo1<- rbindlist(lapply(lsAirTemperature$items, 
                        function(x) cbind(data.table(timestamp = x["timestamp"][[1]]),
                                          lapply(x["readings"], function(y) rbindlist(y))$readings)))
setnames(foo1,"value","temp")


foo2<- rbindlist(lapply(lsRelativeHumidity$items, 
                        function(x) cbind(data.table(timestamp = x["timestamp"][[1]]),
                                          lapply(x["readings"], function(y) rbindlist(y))$readings)))
setnames(foo2,"value","hum")

foo3<- merge(foo1,foo2,by=c("timestamp","station_id"),
             all.x=T,all.y = T)

lsRainfall <- read_json("https://api.data.gov.sg/v1/environment/rainfall")
lsWindDirection <- read_json("https://api.data.gov.sg/v1/environment/wind-direction")
lsWindSpeed <- read_json("https://api.data.gov.sg/v1/environment/wind-speed")


foo <- setDT(lsAirTemperature$metadata$stations)
unlist(lsAirTemperature$metadata$stations)
foo <- setDT(lsAirTemperature$items)

rbindlist(lsAirTemperature$items[[1]]$readings)
rbindlist(lsAirTemperature$items[[2]]$readings)

x <- list(p1 = list(type='A',score=list(c1=10,c2=8)),
          p2 = list(type='B',score=list(c1=9,c2=9)),
          p3 = list(type='B',score=list(c1=9,c2=7)))



rbindlist(lapply(lsAirTemperature$metadata$stations,function(x)x[c("id","name")]))
rbindlist(list.select(lsAirTemperature$metadata$stations,id,device_id,name))
rbindlist(list.select(lsAirTemperature$metadata$stations,)))
