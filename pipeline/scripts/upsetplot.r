library(argparse)

parser <- ArgumentParser()
parser$add_argument("--input", dest="input", type="character", help="a comma separated list of labels and files, each delimited by three underscores (eg. ex___file1,omni___file2)")
parser$add_argument("--type", choices=c("bim","fam"), dest="type", type="character", help="a file type")
parser$add_argument("--out", dest="out", type="character", help="an output filename ending in '.png' or '.pdf'")
args<-parser$parse_args()

print(args)

library(UpSetR)
args<-c()
args$input<-"metabochip___/humgen/diabetes2/users/mvg/portal/aspera/SN0117470/FUSION_metabochip_portal.fam,gwas___/humgen/diabetes2/users/mvg/portal/aspera/SN0117470/FUSION_GWAS_portal.fam,ex_cidr___/humgen/diabetes2/users/mvg/portal/aspera/SN0117470/FUSION_exomechip_CIDR_portal.fam,ex_broad___/humgen/diabetes2/users/mvg/portal/aspera/SN0117470/FUSION_exomechip_Broad_portal.fam"
args$type<-"fam"
args$out<-"test.pdf"

ids<-list()
for(inp in unlist(strsplit(args$input,","))) {
	l<-unlist(strsplit(inp,"___"))[1]
	f<-unlist(strsplit(inp,"___"))[2]
	print(f)
	tbl<-read.table(f,header=F,as.is=T,stringsAsFactors=F)
	if(args$type == "fam") {
		ids[[l]]<-tbl[,2]
	} else if(args$type == "bim") {
		tbl$id<-paste(tbl$V1,tbl$V4,tbl$V5,tbl$V6,sep=":")
		ids[[l]]<-tbl$id
	} else {
		stop(paste("file type ",args$type," not supported",sep=""))
	}
}

if(unlist(strsplit(args$out,"\\."))[-1] == "pdf") {
	pdf(args$out,width=0,height=0,paper="a4r",onefile=FALSE)
} else {
	png(args$out,width=5,height=4,units="in",res=300)
}
upset(fromList(ids), order.by = "freq", main.bar.color="#1F76B4", sets.bar.color="#16BE72", line.size=1, number.angles = 30, point.size = 8, empty.intersections = NULL, mainbar.y.label = "Intersection Size", sets.x.label = "Set Size", text.scale = c(3, 3, 2, 2, 3, 3))
dev.off()

#upset(data, nsets = 5, nintersects = 40, sets = NULL, keep.order = F,
#set.metadata = NULL, intersections = NULL, matrix.color = "gray23",
#main.bar.color = "gray23", mainbar.y.label = "Intersection Size",
#mainbar.y.max = NULL, sets.bar.color = "gray23",
#sets.x.label = "Set Size", point.size = 2.2, line.size = 0.7,
#mb.ratio = c(0.7, 0.3), expression = NULL, att.pos = NULL,
#att.color = main.bar.color, order.by = c("freq", "degree"),
#decreasing = c(T, F), show.numbers = "yes", number.angles = 0,
#group.by = "degree", cutoff = NULL, queries = NULL,
#query.legend = "none", shade.color = "gray88", shade.alpha = 0.25,
#matrix.dot.alpha = 0.5, empty.intersections = NULL, color.pal = 1,
#boxplot.summary = NULL, attribute.plots = NULL,
#scale.intersections = "identity", scale.sets = "identity",
#text.scale = 1, set_size.angles = 0)

#metabochipbim<-read.table("/humgen/diabetes2/users/mvg/portal/aspera/SN0117470/FUSION_metabochip_portal.bim",header=F,as.is=T,stringsAsFactors=F)
#gwasbim<-read.table("/humgen/diabetes2/users/mvg/portal/aspera/SN0117470/FUSION_GWAS_portal.bim",header=F,as.is=T,stringsAsFactors=F)
#exomecidrbim<-read.table("/humgen/diabetes2/users/mvg/portal/aspera/SN0117470/FUSION_exomechip_CIDR_portal.bim",header=F,as.is=T,stringsAsFactors=F)
#exomebroadbim<-read.table("/humgen/diabetes2/users/mvg/portal/aspera/SN0117470/FUSION_exomechip_Broad_portal.bim",header=F,as.is=T,stringsAsFactors=F)
#metabochipbim$ID<-paste(metabochipbim$V1,":",metabochipbim$V4,sep="")
#gwasbim$ID<-paste(gwasbim$V1,":",gwasbim$V4,sep="")
#exomecidrbim$ID<-paste(exomecidrbim$V1,":",exomecidrbim$V4,sep="")
#exomebroadbim$ID<-paste(exomebroadbim$V1,":",exomebroadbim$V4,sep="")
#x<-data.frame(ID=unique(c(metabochipbim$ID,gwasbim$ID,exomecidrbim$ID,exomebroadbim$ID)))
#x$metabochip<-0
#x$gwas<-0
#x$exome_cidr<-0
#x$exome_broad<-0
#x$metabochip[x$ID %in% metabochipbim$ID]<-1
#x$gwas[x$ID %in% gwasbim$ID]<-1
#x$exome_cidr[x$ID %in% exomecidrbim$ID]<-1
#x$exome_broad[x$ID %in% exomebroadbim$ID]<-1
#
#pdf("test.upsetr.pdf",width=0,height=0,paper="a4r")
#upset(x, nsets = 4, number.angles = 30, point.size = 3.5, line.size = 2, empty.intersections = "on", mainbar.y.label = "Array Intersections", sets.x.label = "Variants Per Array", text.scale = c(1.3, 1.3, 1, 1, 2, 0.75))
#dev.off()
