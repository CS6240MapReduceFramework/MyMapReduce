args<-commandArgs(trailingOnly = TRUE)
foldername <- args[1]
#setwd('/Users/ummehabibashaik/Documents/Mapreduce/HW7/input')  
setwd(args[1])

#Load datasets
files<-list.files(path="history/",pattern="part-r-",full.names = TRUE)
history<-c()
for(i in 1:length(files)){
  history <- rbind(history,read.table(file=files[i], header=FALSE, sep="\t"))
}
missedData <- read.table(file='a7validate/04missed.csv.gz', header=FALSE, sep=",")
files<-list.files(path="connections/",pattern="part-r-",full.names = TRUE)
connections<-c()
for(i in 1:length(files)){
  connections <- rbind(connections,read.table(file=files[i], header=FALSE, sep="\t"))
}
request <- read.table(file='a7request/sample.csv', header=TRUE, sep=",")

# Assign labels
colnames(connections) <- c("airline","month","day","connection","origin","origin_flight","destination","layover","destination_flight")
colnames(history)<-c("airline","month","origin","destination","duration")
colnames(missedData)<-c("year","month","day","origin","destination","actual","estimated")

#All possible connections for all requests
request_connections<-merge(request, connections, by.x=c('month','day','origin','destination'), by.y=c('month','day','origin','destination'))

#Populate Duration for first flight all requests
Durations1 <-merge(request_connections, history, by.x=c('month','airline','origin','connection'), by.y=c('month','airline','origin','destination'))
#Populate Duration for second flight all requests
Durations2 <-merge(Durations1, history, by.x=c('month','airline','connection','destination'), by.y=c('month','airline','origin','destination'))
 
#Calculate total duration of iterinary
NewData <- transform(Durations2, total=Durations2$layover+Durations2$duration.x+Durations2$duration.y)

newTotal <- c()
for (i in 1:length(NewData$month)){
  x <- which(missedData$day == NewData$day[i] & missedData$month == NewData$month[i] & 
               ((missedData$origin == as.character(NewData$origin[i]) & missedData$destination == as.character(NewData$connection[i]))
                | (missedData$origin == as.character(NewData$connection[i]) & missedData$destination == as.character(NewData$destination[i]))))
  if(length(x)>0){
    newTotal <- c(newTotal,NewData$total[i]+100)
  }else{
    newTotal <- c(newTotal, NewData$total[i])
  }
}
NewData$Flight_Duration <- newTotal

a<-as.character(NewData$month)
b<-as.character(NewData$day)
c<-as.character(NewData$origin)
d<-as.character(NewData$destination)
e<-paste0(c,d,a,b)
NewData$DesOrg<-e
typeOr<-levels(as.factor(NewData$DesOrg))

typeOr<-levels(as.factor(NewData$DesOrg))

indexData<-data.frame()
for (i in 1:length(typeOr)){
  index<-NewData[which(NewData$DesOrg == typeOr[i]),][which(NewData[which(NewData$DesOrg == typeOr[i]),]$Flight_Duration==min(NewData[which(NewData$DesOrg == typeOr[i]),]$Flight_Duration)),]
  indexData<-rbind(indexData,index)
}
finalData<-indexData[,c('airline','day','month','origin','connection','destination','origin_flight','destination_flight','Flight_Duration')]
write.csv(finalData, file = "../output.csv",row.names=FALSE)

