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
