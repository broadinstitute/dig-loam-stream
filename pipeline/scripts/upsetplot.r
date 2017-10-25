library(argparse)

parser <- ArgumentParser()
parser$add_argument("--input", dest="input", type="character", help="a comma separated list of labels and files, each delimited by three underscores (eg. ex___file1,omni___file2)")
parser$add_argument("--type", choices=c("bim","fam"), dest="type", type="character", help="a file type")
parser$add_argument("--out", dest="out", type="character", help="an output filename ending in '.png' or '.pdf'")
args<-parser$parse_args()

#print(args)

library(UpSetR)

ids<-list()
for(inp in unlist(strsplit(args$input,","))) {
	l<-unlist(strsplit(inp,"___"))[1]
	f<-unlist(strsplit(inp,"___"))[2]
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
if(unlist(strsplit(args$out,"\\."))[length(unlist(strsplit(args$out,"\\.")))] == "pdf") {
	pdf(args$out,width=0,height=0,paper="a4r",onefile=FALSE)
} else {
	stop(paste("output extension ",unlist(strsplit(args$out,"\\."))[length(unlist(strsplit(args$out,"\\.")))]," not supported",sep=""))
}
upset(fromList(ids), order.by = "freq", main.bar.color="#1F76B4", sets.bar.color="#16BE72", line.size=1, number.angles = 0, point.size = 8, empty.intersections = NULL, mainbar.y.label = "Intersection Size", sets.x.label = "Set Size", text.scale = c(3, 3, 2, 2, 3, 3))
dev.off()
