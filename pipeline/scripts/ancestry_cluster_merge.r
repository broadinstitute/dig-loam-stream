library(reshape2)
args<-commandArgs(trailingOnly=T)
files = paste("qc/",list.files("qc", glob2rx("*.ancestry")),sep="")
x<-data.frame(FILE=files,stringsAsFactors=F)
x$LABEL<-colsplit(colsplit(x$FILE,"\\.ancestry",names=c("LABEL","X2"))$LABEL,"/",names=c("DIR","LABEL"))$LABEL
for(i in 1:nrow(x)) {
	df<-read.table(x$FILE[i],header=F,as.is=T,stringsAsFactors=F)
	names(df)<-c("IID",x$LABEL[i])
	if(i == 1) {
		out<-df
	} else {
		out<-merge(out,df,all=T)
	}
}
hierarchy<-scan(file=args[1],what="character")
out$FINAL<-NA
out$AGREE<-0
for(i in 1:nrow(out)) {
	for(l in rev(hierarchy)) {
		if(! is.na(out[,l][i]) & out[,l][i] != "OUTLIERS") {
			out$FINAL[i]<-out[,l][i]
		}
	}
	un<-unique(unlist(out[i,2:(ncol(out)-2)]))
	if(length(un[! is.na(un) & un != "OUTLIERS"]) == 1) {
		out$AGREE[i]<-1
	}
}
out$FINAL[is.na(out$FINAL)]<-"OUTLIERS"
write.table(out,"qc/ancestry.table",row.names=F,col.names=T,sep="\t",append=F,quote=F)
print(table(out$FINAL))
for(l in unique(out$FINAL)) {
	write.table(out$IID[out$FINAL == l],paste("qc/ancestry.",l,sep=""),row.names=F,col.names=F,sep="\t",append=F,quote=F)
	write.table(paste(out$IID[out$FINAL == l],out$IID[out$FINAL == l],sep=" "),paste("qc/ancestry.",l,".plink",sep=""),row.names=F,col.names=F,sep="\t",append=F,quote=F)
}
write.table(out$IID[out$FINAL != "OUTLIERS"],"qc/ancestry.CLUSTERED",row.names=F,col.names=F,sep="\t",append=F,quote=F)
write.table(paste(out$IID[out$FINAL != "OUTLIERS"],out$IID[out$FINAL != "OUTLIERS"]),"qc/ancestry.CLUSTERED.plink",row.names=F,col.names=F,sep="\t",append=F,quote=F)
