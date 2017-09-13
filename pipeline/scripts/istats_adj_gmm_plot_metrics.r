args<-commandArgs(trailingOnly=T)
library(reshape2)
library(ggplot2)
library(gridExtra)
gg_color_hue <- function(n) {
  hues = seq(15, 375, length=n+1)
  hcl(h=hues, l=65, c=100)[1:n]
}
metrics<-unlist(strsplit(args[1],","))
np<-0
np<-np+1
data_orig<-read.table(args[2],header=T,as.is=T,stringsAsFactors=F)
data<-read.table(args[3],header=T,as.is=T,stringsAsFactors=F)
data_names<-names(data)[names(data) %in% metrics]
oliers<-readLines(args[4])
data$OUTLIER_PCA<-0
if( length(oliers) > 0) {
	data$OUTLIER_PCA[data$IID %in% oliers]<-1
}
pdf(args[5],width=ceiling(length(data_names)/10)*7, height=7)
for(m in metrics) {
	print(m)
	cl<-read.table(gsub("tsv",paste(m,".clu.1",sep=""),args[3]), as.is=T, skip=1)
	cl_levels<-c()
	cl_names<-c()
	if(1 %in% cl$V1) {
		cl_names<-c(cl_names,"X")
		cl_levels<-c(cl_levels,1)
	}
	for(c in sort(unique(cl$V1[cl$V1 != 1]))) {
		cl_names<-c(cl_names,c-1)
		cl_levels<-c(cl_levels,c)
	}
	cl$V1<-factor(cl$V1, levels=cl_levels, labels=cl_names, ordered = TRUE)
	names(cl)[1]<-paste(m,"_CLUSTER",sep="")
	data<-cbind(data,cl)
	color<-gg_color_hue(max(as.numeric(data[,c(paste(m,"_CLUSTER",sep=""))])))
	if("X" %in% cl_names) {
		color[1]<-"#808080"
	}
	pl<-ggplot(data,aes_string(paste(m,"_CLUSTER",sep=""), y=m)) +
		geom_boxplot(data=data[data[,c(paste(m,"_CLUSTER",sep=""))] != "X",],aes_string(colour=paste(m,"_CLUSTER",sep=""))) +
		geom_point(aes_string(colour=paste(m,"_CLUSTER",sep=""))) +
		geom_rug(sides="l") +
		scale_x_discrete(limits=cl_names) +
		scale_colour_manual(breaks=cl_names,limits=cl_names,values=color) +
		theme_bw() +
		guides(color=guide_legend(override.aes = list(shape = 15))) +
		theme(axis.line = element_line(colour = "black"), 
		plot.title = element_text(size = 16,face="bold"),
		panel.grid.major = element_blank(),
		panel.grid.minor = element_blank(),
		panel.border = element_blank(),
		panel.background = element_blank(),
		legend.key = element_blank())
	plot(pl)
	cat(file=args[6],paste("   ",m,": ",nrow(data)," total, ",length(unique(data[,c(m)]))," unique, ",(length(unique(data[,c(m)])) / nrow(data))*100,"%\n",sep=""),append=T)
}
dev.off()
data<-merge(data,data_orig,all=T)
i<-0
for(cl in names(data)[grep("_CLUSTER",names(data))]) {
	print(cl)
	i<-i+1
	f<-gsub("_CLUSTER","",cl)
	f_orig<-gsub("_res","",f)
	temp<-data[,c(f,cl,f_orig,"OUTLIER_PCA","IID")]
	names(temp)[1]<-"VALUE"
	names(temp)[2]<-"CLUSTER"
	names(temp)[3]<-"VALUE_ORIG"
	temp$METRIC<-f
	if(i == 1) {
		sdata<-temp
	} else {
		sdata<-rbind(sdata,temp)
	}
}
sdata$DECISION<-"KEEP"
sdata$DECISION[sdata$CLUSTER == "X" & sdata$OUTLIER_PCA == 0]<-"OUTLIER_IND"
sdata$DECISION[sdata$CLUSTER == "X" & sdata$OUTLIER_PCA == 1]<-"OUTLIER_IND_PCA"
sdata$DECISION[sdata$CLUSTER != "X" & sdata$OUTLIER_PCA == 1]<-"OUTLIER_PCA"
sdata$DECISION<-factor(sdata$DECISION)
write.table(sdata[sdata$DECISION != "KEEP",],args[7],row.names=F,col.names=T,quote=F,sep="\t",append=F)
ancestry<-read.table(args[9],header=F,as.is=T,stringsAsFactors=F)
names(ancestry)[1]<-"IID"
names(ancestry)[2]<-"ANCESTRY"
id_list<-list(all=unique(sdata$IID))
id_list[['AFR']]<-ancestry$IID[ancestry$ANCESTRY == "AFR"]
id_list[['AMR']]<-ancestry$IID[ancestry$ANCESTRY == "AMR"]
id_list[['EAS']]<-ancestry$IID[ancestry$ANCESTRY == "EAS"]
id_list[['EUR']]<-ancestry$IID[ancestry$ANCESTRY == "EUR"]
id_list[['SAS']]<-ancestry$IID[ancestry$ANCESTRY == "SAS"]

oliers_plot<-unique(sdata$IID[sdata$DECISION %in% c("OUTLIER_PCA","OUTLIER_IND","OUTLIER_IND_PCA")])
pdf(args[8],width=20, height=5)
for(p in names(id_list)) {
	i<-0
	sdata_temp<-sdata[sdata$IID %in% id_list[[p]],]
	if(nrow(sdata_temp) > 0) {
		cmd<-"grid.arrange("
		for(f in unique(sdata_temp$METRIC)) {
			i<-i+1
			pl<-ggplot(sdata_temp[sdata_temp$METRIC == f,],aes_string(x="METRIC",y="VALUE")) +
				geom_jitter(data=sdata_temp[sdata_temp$METRIC == f & sdata_temp$DECISION=="KEEP",],color="lightgray",position=position_jitter(height=0)) +
				geom_boxplot(outlier.size=NA,fill=NA) +
				geom_jitter(data=sdata_temp[sdata_temp$METRIC == f & sdata_temp$DECISION=="OUTLIER_PCA",],color="blue",position=position_jitter(height=0)) +
				geom_jitter(data=sdata_temp[sdata_temp$METRIC == f & sdata_temp$DECISION=="OUTLIER_IND_PCA",],color="green",position=position_jitter(height=0)) +
				geom_jitter(data=sdata_temp[sdata_temp$METRIC == f & sdata_temp$DECISION=="OUTLIER_IND",],color="orange",position=position_jitter(height=0)) +
				theme_bw() +
				theme(legend.position="none",
				axis.line = element_line(colour = "black"), 
				plot.title = element_text(size = 16,face="bold"),
				axis.title.x = element_blank(),
				axis.title.y = element_blank(),
				panel.grid.major = element_blank(),
				panel.grid.minor = element_blank(),
				panel.border = element_blank(),
				panel.background = element_blank(),
				legend.key = element_blank())
			assign(paste("plot_",f,sep=""),pl)
			cmd<-paste(cmd,paste("plot_",f,sep=""),",",sep="")
		}
		cmd<-paste(cmd,"ncol=",i,",main=textGrob(\"",toupper(p)," PC Adjusted Metric Values (",length(oliers_plot[oliers_plot %in% sdata_temp$IID])," Outliers)\",just=\"top\"))",sep="")
		print(cmd)
		eval(parse(text=cmd))
		i<-0
		cmd<-"grid.arrange("
		for(f in unique(sdata_temp$METRIC)) {
			i<-i+1
			pl<-ggplot(sdata_temp[sdata_temp$METRIC == f,],aes_string(x="METRIC",y="VALUE")) +
				geom_jitter(data=sdata_temp[sdata_temp$METRIC == f & sdata_temp$DECISION=="KEEP",],color="lightgray",position=position_jitter(height=0)) +
				geom_boxplot(outlier.size=NA,fill=NA) +
				theme_bw() +
				theme(legend.position="none",
				axis.line = element_line(colour = "black"), 
				plot.title = element_text(size = 16,face="bold"),
				axis.title.x = element_blank(),
				axis.title.y = element_blank(),
				panel.grid.major = element_blank(),
				panel.grid.minor = element_blank(),
				panel.border = element_blank(),
				panel.background = element_blank(),
				legend.key = element_blank())
			assign(paste("plot_",f,sep=""),pl)
			cmd<-paste(cmd,paste("plot_",f,sep=""),",",sep="")
		}
		cmd<-paste(cmd,"ncol=",i,",main=textGrob(\"",toupper(p)," PC Adjusted Metric Values (No Outliers)\",just=\"top\"))",sep="")
		print(cmd)
		eval(parse(text=cmd))
		i<-0
		cmd<-"grid.arrange("
		for(f in unique(sdata_temp$METRIC)) {
			i<-i+1
			pl<-ggplot(sdata_temp[sdata_temp$METRIC == f,],aes_string(x="METRIC",y="VALUE_ORIG")) +
				geom_jitter(data=sdata_temp[sdata_temp$METRIC == f & sdata_temp$DECISION=="KEEP",],color="lightgray",position=position_jitter(height=0)) +
				geom_boxplot(outlier.size=NA,fill=NA) +
				geom_jitter(data=sdata_temp[sdata_temp$METRIC == f & sdata_temp$DECISION=="OUTLIER_PCA",],color="blue",position=position_jitter(height=0)) +
				geom_jitter(data=sdata_temp[sdata_temp$METRIC == f & sdata_temp$DECISION=="OUTLIER_IND_PCA",],color="green",position=position_jitter(height=0)) +
				geom_jitter(data=sdata_temp[sdata_temp$METRIC == f & sdata_temp$DECISION=="OUTLIER_IND",],color="orange",position=position_jitter(height=0)) +
				theme_bw() +
				theme(legend.position="none",
				axis.line = element_line(colour = "black"), 
				plot.title = element_text(size = 16,face="bold"),
				axis.title.x = element_blank(),
				axis.title.y = element_blank(),
				panel.grid.major = element_blank(),
				panel.grid.minor = element_blank(),
				panel.border = element_blank(),
				panel.background = element_blank(),
				legend.key = element_blank())
			assign(paste("plot_",f,sep=""),pl)
			cmd<-paste(cmd,paste("plot_",f,sep=""),",",sep="")
		}
		cmd<-paste(cmd,"ncol=",i,",main=textGrob(\"",toupper(p)," Original Metric Values (",length(oliers_plot[oliers_plot %in% sdata_temp$IID])," Outliers)\",just=\"top\"))",sep="")
		print(cmd)
		eval(parse(text=cmd))
		i<-0
		cmd<-"grid.arrange("
		for(f in unique(sdata_temp$METRIC)) {
			i<-i+1
			pl<-ggplot(sdata_temp[sdata_temp$METRIC == f,],aes_string(x="METRIC",y="VALUE_ORIG")) +
				geom_jitter(data=sdata_temp[sdata_temp$METRIC == f & sdata_temp$DECISION=="KEEP",],color="lightgray",position=position_jitter(height=0)) +
				geom_boxplot(outlier.size=NA,fill=NA) +
				theme_bw() +
				theme(legend.position="none",
				axis.line = element_line(colour = "black"), 
				plot.title = element_text(size = 16,face="bold"),
				axis.title.x = element_blank(),
				axis.title.y = element_blank(),
				panel.grid.major = element_blank(),
				panel.grid.minor = element_blank(),
				panel.border = element_blank(),
				panel.background = element_blank(),
				legend.key = element_blank())
			assign(paste("plot_",f,sep=""),pl)
			cmd<-paste(cmd,paste("plot_",f,sep=""),",",sep="")
		}
		cmd<-paste(cmd,"ncol=",i,",main=textGrob(\"",toupper(p)," Original Metric Values (No Outliers)\",just=\"top\"))",sep="")
		print(cmd)
		eval(parse(text=cmd))
	}
}
opn<-0
for(o in oliers_plot) {
	opn<-opn+1
	i<-0
	cmd<-"grid.arrange("
	for(f in unique(sdata$METRIC)) {
		i<-i+1
		pl<-ggplot(sdata[sdata$METRIC == f,],aes_string(x="METRIC",y="VALUE")) +
			geom_jitter(data=sdata[sdata$METRIC == f & sdata$DECISION=="KEEP",],color="lightgray",position=position_jitter(height=0)) +
			geom_boxplot(outlier.size=NA,fill=NA) +
			geom_jitter(data=sdata[sdata$METRIC == f & sdata$DECISION=="OUTLIER_PCA",],color="blue",position=position_jitter(height=0)) +
			geom_jitter(data=sdata[sdata$METRIC == f & sdata$DECISION=="OUTLIER_IND_PCA" & sdata$IID == o,],color="green",position=position_jitter(height=0)) +
			geom_jitter(data=sdata[sdata$METRIC == f & sdata$DECISION!="OUTLIER_IND_PCA" & sdata$IID == o,],color="orange",position=position_jitter(height=0)) +
			theme_bw() +
			theme(legend.position="none",
			axis.line = element_line(colour = "black"), 
			plot.title = element_text(size = 16,face="bold"),
			axis.title.x = element_blank(),
			axis.title.y = element_blank(),
			panel.grid.major = element_blank(),
			panel.grid.minor = element_blank(),
			panel.border = element_blank(),
			panel.background = element_blank(),
			legend.key = element_blank())
		assign(paste("plot_",f,sep=""),pl)
		cmd<-paste(cmd,paste("plot_",f,sep=""),",",sep="")
	}
	cmd<-paste(cmd,"ncol=",i,",main=textGrob(\"Outlier ",o,"\",just=\"top\"))",sep="")
	print(paste("Outlier ",o," (",opn," of ",length(oliers_plot),") ",cmd,sep=""))
	eval(parse(text=cmd))
}
dev.off()
print(warnings())
