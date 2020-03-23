library(jsonlite)
library(data.table)
library(aws.s3)
library(zip)
#MODIFY BEFORE EXECUTION
toSaveLocal=F #If F, each download iteration will create files that will be deleted after uploaded
toSaveS3Bucket=T #dataset will be zipped reduce the number of put request 
bucket="dataset.artidata.io"
dateStart="2020-03-18"
wdateEnd="2020-03-20"

#Creating Folder structure
dir.create("download_data")
dir.create("download_data/metadata")
dir.create("download_data/metadata/stations")
dir.create("download_data/metadata/reading_type")
dir.create("download_data/metadata/reading_unit")

if(toSaveS3Bucket){
  Sys.setenv("AWS_ACCESS_KEY_ID" = "AKIAJVDKHSAEUYQ5JKSA",
             "AWS_SECRET_ACCESS_KEY" = "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",
             "AWS_DEFAULT_REGION" = "ap-southeast-1")
  # put_object("Open Data Licence.html",
  #            "sg_weather/Open Data Licence.html",
  #            bucket)
  }

date=as.character(seq(as.Date(dateStart),as.Date(dateEnd),"day"))
measurement=c("air-temperature","relative-humidity","wind-direction","wind-speed","rainfall")

for (i in 1:length(date)){
  
  dateI=date[i]
  dir.create(paste0("download_data/",dateI))
  
  for (j in 1:length(measurement)){
    
    measurementJ=measurement[j]
    
    #Scraping or downloading
    lsData=NULL
    while(is.null(lsData)){
      try({
        lsData=read_json(paste0("https://api.data.gov.sg/v1/environment/",measurementJ,"?date=",dateI))
        Sys.sleep(20)})
      if(is.null(lsData)==F){
        if(lsData$api_info$status!="healthy"){
          lsData=NULL}}}
    
    #Saving and compressing measurements
    dtItem=rbindlist(lapply(lsData$items,function(x){
      dtX=dcast(rbindlist(lapply(x$readings,as.data.table),fill=T),...~station_id)
      setnames(dtX,".","timestamp")  
      set(dtX,1,"timestamp",substr(x$timestamp,12,19))
      return(dtX)}),fill = T)
    fwrite(dtItem,paste0("download_data/",dateI,"/",measurementJ,".csv.gz")) 
    
    #Updating metadata
    dtStation=rbindlist(lapply(lsData$metadata$stations,
                     function(x) cbind(as.data.frame(x[c("id","device_id","name")]),
                                       as.data.frame(x$location))))
    if(paste0(measurementJ,".csv")%in%list.files("download_data/metadata/stations/")){
      dtStationPast=fread(paste0("download_data/metadata/stations/",measurementJ,".csv"))
      if(ncol(dtStation)==ncol(dtStationPast)){
        dtStation=funion(dtStationPast,dtStation)
        } else{dtStation=dtStationPast}
      }
    fwrite(dtStation,paste0("download_data/metadata/stations/",measurementJ,".csv"))
    
    dtReadingType=data.table(type=lsData$metadata$reading_type)
    if(paste0(measurementJ,".csv")%in%list.files("download_data/metadata/reading_type/")){
      dtReadingTypePast=fread(paste0("download_data/metadata/reading_type/",measurementJ,".csv"),sep=NULL)
      if(ncol(dtReadingType)==ncol(dtReadingTypePast)){
        dtReadingType=funion(dtReadingTypePast,dtReadingType)
        } else{dtReadingType=dtReadingTypePast}
      }
    fwrite(dtReadingType,paste0("download_data/metadata/reading_type/",measurementJ,".csv"))
    
    dtReadingUnit=data.table(unit=lsData$metadata$reading_unit)
    if(paste0(measurementJ,".csv")%in%list.files("download_data/metadata/reading_unit/")){
      dtReadingUnitPast=fread(paste0("download_data/metadata/reading_unit/",measurementJ,".csv"),sep=NULL)
      if(ncol(dtReadingUnit)==ncol(dtReadingUnitPast)){
        dtReadingUnit=funion(dtReadingUnitPast,dtReadingUnit)
        } else {dtReadingUnit=dtReadingUnitPast}
      }
    fwrite(dtReadingUnit,paste0("download_data/metadata/reading_unit/",measurementJ,".csv"))
  }
  if(toSaveS3Bucket){
    zipr(paste0("download_data/",dateI,".zip"),
         list.files(paste0("download_data/",dateI,"/"),full.names=T),
         include_directories=F)
    put_object(paste0("download_data/",dateI,".zip"),
               paste0("sg_weather/",dateI,".zip"),
               bucket)
    file.remove(paste0("download_data/",dateI,".zip"))
  }
  if(toSaveLocal==F){ 
    unlink(paste0("download_data/",dateI),recursive=T)
  }
}
# 
# zipr(paste0("download_data/metadata.zip"),
#      list.files(paste0("download_data/metadata/"),full.names=T),
#      include_directories=T)
# put_object(paste0("download_data/metadata.zip"),
#            paste0("sg_weather/metadata.zip"),
#            bucket)
