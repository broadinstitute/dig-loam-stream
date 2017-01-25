library(reshape2)
args<-commandArgs(trailingOnly=T)
data<-read.table(args[1],header=T,as.is=T,stringsAsFactors=F)
pcs<-read.table(args[2],header=T,as.is=T,stringsAsFactors=F)
pcs$POP<-NULL
pcs$SUPERPOP<-NULL
pcs$SEX<-NULL
out<-merge(data,pcs,all.y=T)
for(x in names(out)[2:12]) {
	out$ADJ<-glm(eval(parse(text=paste(x,"~PC1+PC2+PC3+PC4+PC5+PC6+PC7+PC8+PC9+PC10",sep=""))),data=out,family="gaussian")$residuals
	names(out)[names(out) == "ADJ"]<-paste(x,"_ADJ",sep="")
}
write.table(out[,c("IID",names(out)[grep("_ADJ",names(out))])],args[3],row.names=F,col.names=T,sep="\t",quote=F,append=F)
