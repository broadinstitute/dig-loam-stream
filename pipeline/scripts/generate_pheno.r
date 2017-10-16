library(argparse)

parser <- ArgumentParser()
parser$add_argument("--gds-in", dest="gds_in", type="character", help="a plink binary file path")
parser$add_argument("--pheno-in", dest="pheno_in", type="character", help="a phenotype file")
parser$add_argument("--pheno-col", dest="pheno_col", type="character", help="a column name for phenotype")
parser$add_argument("--iid-col", dest="iid_col", help='a column name for sample ID in phenotype file')
parser$add_argument("--samples-include", dest="samples_include", type="character", help="a final list of sample IDs to include")
parser$add_argument("--variants-exclude", nargs=2, dest="variants_exclude", default=NULL, type="character", help="A column name or 1-based number for variant IDs followed by the filename")
parser$add_argument("--trans", dest="trans", type="character", help="a comma separated list of transformation codes")
parser$add_argument("--covars", dest="covars", type="character", help="a '+' separated list of covariates")
parser$add_argument("--out", dest="out", type="character", help="an output filename")
args<-parser$parse_args()

print(args)

library(GENESIS)
library(SNPRelate)
library(GWASTools)
library(gdsfmt)
library(pryr)

print("extracting model specific columns from phenotype file")
cols_keep = c(args$iid_col,args$pheno_col)
if(args$covars != "") {
	cols_keep = c(cols_keep, unlist(strsplit(args$covars,"\\+")))
}
if(args$trans != "") {
	if(grepl(":",args$trans)) {
		cols_keep = c(cols_keep, unlist(strsplit(unlist(strsplit(args$trans,":"))[2],"\\+")))
	}
}
pheno<-read.table(args$pheno_in,header=T,as.is=T,stringsAsFactors=F,sep="\t")
pheno<-pheno[,cols_keep]
pheno<-pheno[complete.cases(pheno),]

print("reading sample and variant IDs from gds file")
geno <- GdsGenotypeReader(filename = args$gds_in)
iids <- getScanID(geno)
vids <- getSnpID(geno)
close(geno)

print("generating sample and variant ID inclusion lists")
samples_incl<-scan(file=args$samples_include,what="character")
variants_excl_df<-read.table(args$variants_exclude[2],header=T,as.is=T,stringsAsFactors=F,sep="\t")
variants_excl<-variants_excl_df[,args$variants_exclude[1]]
samples_incl<-iids[iids %in% samples_incl & iids %in% pheno[,args$iid_col]]
variants_incl<-vids[! vids %in% variants_excl]

print(paste("memory before running king: ",mem_used() / (1024^2),sep=""))
print("running King robust to get kinship matrix")
genofile <- snpgdsOpen(args$gds_in)
king<-snpgdsIBDKING(genofile, sample.id=samples_incl, snp.id=variants_incl, autosome.only=TRUE, remove.monosnp=TRUE, maf=NaN, missing.rate=NaN, type="KING-robust", family.id=NULL, num.thread=4, verbose=TRUE)
kinship <- king$kinship
rownames(kinship)<-king$sample.id
colnames(kinship)<-king$sample.id
snpgdsClose(genofile)

print(paste("memory after running king and before running pcair: ",mem_used() / (1024^2),sep=""))

print("running pcair")
geno <- GdsGenotypeReader(filename = args$gds_in)
genoData <- GenotypeData(geno)
mypcair <- pcair(genoData = genoData, scan.include = samples_incl, snp.include = variants_incl, kinMat = kinship, divMat = kinship, snp.block.size = 10000)

print(paste("memory after running pcair: ",mem_used() / (1024^2),sep=""))

pcs<-data.frame(mypcair$vectors)
names(pcs)[1:20]<-paste("PC",seq(1,20),sep="")
pcs[,args$iid_col]<-row.names(pcs)

out_cols<-colnames(pheno)

out<-merge(pheno,pcs,all.y=T)

INVN <- function(x){
	return(qnorm((rank(x,na.last="keep") - 0.5)/sum(!is.na(x))))
}

if(args$trans != "") {
	print("calculating transformations")
	trans_type<-unlist(strsplit(args$trans,":"))[1]
	if(trans_type == 'invn') {
		print("performing invn transformation")
		trans_lm <- summary(lm(as.formula(paste(args$pheno_col,"~",unlist(strsplit(args$trans,":"))[2],"+",paste(paste("PC",seq(1,20,1),sep=""),collapse="+"),sep="")),data=out))
		print("calculating initial lm to determine number of PCs to include in residual calculation")
		print(trans_lm)
		trans_lm_coef <- trans_lm$coefficients
		sig_covars <- rownames(trans_lm_coef[trans_lm_coef[,"Pr(>|t|)"] <= 0.05,])
		sig_pcs <- sig_covars[grep("^PC",sig_covars)]
		max_sig_pc <- max(as.integer(gsub("PC","",sig_pcs)))
		pcs_incl <- paste("PC",seq(1,max_sig_pc,1),sep="")
		print(paste("including ",paste(pcs_incl,collapse="+")," PCs in calculation of residuals",sep=""))
		trans_lm_final <- summary(lm(as.formula(paste(args$pheno_col,"~",unlist(strsplit(args$trans,":"))[2],"+",paste(pcs_incl,collapse="+"),sep="")),data=out))
		out[,paste(args$pheno_col,"invn",paste(unlist(strsplit(unlist(strsplit(t,":"))[2],"\\+")),collapse="_"),sep="_")]<-INVN(residuals(trans_lm_final))
	} else if(trans_type == 'log') {
		print("performing log transformation")
		out[,paste(args$pheno_col,"_log",sep="")]<-log(out[,args$pheno_col])
	} else {
		print(paste("transformation type ",trans_type," is not yet available",sep=""))
		quit(status=1)
	}
} else {
	notrans_lm <- summary(lm(as.formula(paste(args$pheno_col,"~",args$covars,"+",paste(paste("PC",seq(1,20,1),sep=""),collapse="+"),sep="")),data=out))
	print("calculating initial lm to determine number of PCs to include in model")
	print(notrans_lm)
	trans_lm_coef <- notrans_lm$coefficients
	sig_covars <- rownames(trans_lm_coef[trans_lm_coef[,"Pr(>|t|)"] <= 0.05,])
	sig_pcs <- sig_covars[grep("^PC",sig_covars)]
	max_sig_pc <- max(as.integer(gsub("PC","",sig_pcs)))
	pcs_incl <- paste("PC",seq(1,max_sig_pc,1),sep="")
	out_cols <- c(out_cols,pcs_incl)
}

print("writing phenotype file")
write.table(out[,out_cols],args$out,row.names=F,col.names=T,quote=F,sep="\t",append=F, na="NA")
