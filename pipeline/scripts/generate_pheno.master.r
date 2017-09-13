library(reshape2)

## global variables
dataset<-"BIOME"

## input filenames
phenofile<-"AMPT2D-PHENOTYPES_FINAL.txt"
ancestry_outliers_file<-"../samples_flagged/ancestry_outliers.remove"
cross_array_duplicates_file<-"../samples_flagged/cross.array.duplicates.remove"
cross_array_excessive_sharing_file<-"../samples_flagged/cross.array.excessive_sharing.remove"
ancestry_table_file<-"../ancestry_cluster/ancestry.table"

arrays=list(
	'EX'=list(
		'raw_fam'="/humgen/diabetes/portal/continuous_integration/servers/sandbox/custom_workspace/knowledge_base/ANNEX/T2D_PORTAL/external/BioMe/genotypes/BioMe_EXOMECHIP.fam",
		'istats_remove'="../samples_flagged/BIOME_EX.CLUSTERED.istats.outliers.remove",
		'duplicate_remove'="../samples_flagged/BIOME_EX.duplicates.remove",
		'ibd_sharing_remove'="../samples_flagged/BIOME_EX.excessive_sharing.remove",
		'sexcheck_remove'="../samples_flagged/BIOME_EX.failed_sexcheck.remove"
	),
	'AFFY'=list(
		'raw_fam'="/humgen/diabetes/portal/continuous_integration/servers/sandbox/custom_workspace/knowledge_base/ANNEX/T2D_PORTAL/external/BioMe/genotypes/BioMe_AFFY.fam",
		'istats_remove'="../samples_flagged/BIOME_AFFY.CLUSTERED.istats.outliers.remove",
		'duplicate_remove'="../samples_flagged/BIOME_AFFY.duplicates.remove",
		'ibd_sharing_remove'="../samples_flagged/BIOME_AFFY.excessive_sharing.remove",
		'sexcheck_remove'="../samples_flagged/BIOME_AFFY.failed_sexcheck.remove"
	),
	'ILL'=list(
		'raw_fam'="/humgen/diabetes/portal/continuous_integration/servers/sandbox/custom_workspace/knowledge_base/ANNEX/T2D_PORTAL/external/BioMe/genotypes/BioMe_ILLUMINA.fam",
		'istats_remove'="../samples_flagged/BIOME_ILL.CLUSTERED.istats.outliers.remove",
		'duplicate_remove'="../samples_flagged/BIOME_ILL.duplicates.remove",
		'ibd_sharing_remove'="../samples_flagged/BIOME_ILL.excessive_sharing.remove",
		'sexcheck_remove'="../samples_flagged/BIOME_ILL.failed_sexcheck.remove"
	)
)

## output filenames
arrays_table_file<-'arrays.table'
sample_counts_file<-'sample_counts.table'
sample_counts_genotyped_file<-'sample_counts_genotyped.table'
sample_counts_genotyped_by_ancestry_file<-'sample_counts_genotyped_by_ancestry.table'
categorical_summaries<-'categorical.summaries'
quantitative_summaries<-'quantitative.summaries'
master_pheno<-'pheno.master.tsv'





## begin phenotype file generation

## read pheno file, handle NA's, and rename ID column to IID
dat<-read.table(phenofile,header=T,as.is=T,quote="",stringsAsFactors=F,sep="\t",comment.char="")
dat[is.na(dat)]<-"NA"
dat[dat == ""]<-"NA"
names(dat)[1]<-"IID"
dat<-dat[! is.na(dat$IID),]

names(dat)




#### recode variables for analysis
dat$SEX[which(dat$SEX %in% c("M","m","Male","male"))]<-1
dat$SEX[which(dat$SEX %in% c("F","f","Female","female"))]<-2



## read in cross array samples to remove from all arrays
ancestry_outliers_remove<-scan(ancestry_outliers_file,what="character",sep="\n")
cross_array_duplicates_remove<-scan(cross_array_duplicates_file,what="character",sep="\n")
cross_array_excessive_sharing_remove<-scan(cross_array_excessive_sharing_file,what="character",sep="\n")

dat$GENOTYPED<-0
for(arr in names(arrays)) {

	## create columns to identify genotyping
	genos<-read.table(arrays[[arr]][['raw_fam']],header=F,as.is=T,stringsAsFactors=F)$V2
	dat$GENOTYPED[dat$IID %in% unique(genos)]<-1

}

for(arr in names(arrays)) {

	## create columns to identify genotyping in array
	genos<-read.table(arrays[[arr]][['raw_fam']],header=F,as.is=T,stringsAsFactors=F)$V2
	dat[,paste(arr,'_GENOTYPED',sep="")]<-NA
	dat[,paste(arr,'_GENOTYPED',sep="")][which(dat$GENOTYPED == 1)]<-0
	dat[,paste(arr,'_GENOTYPED',sep="")][dat$IID %in% genos]<-1

	## create columns to identify samples to be remove and kept
	dat[,paste(arr,'_REMOVE',sep="")]<-NA
	dat[,paste(arr,'_REMOVE',sep="")][which(dat[,paste(arr,'_GENOTYPED',sep="")] == 1)]<-0
	dat[,paste(arr,'_KEEP',sep="")]<-NA
	dat[,paste(arr,'_KEEP',sep="")][which(dat[,paste(arr,'_GENOTYPED',sep="")] == 1)]<-0
	dat[,paste(arr,'_ISTATS_REMOVE',sep="")]<-NA
	dat[,paste(arr,'_ISTATS_REMOVE',sep="")][which(dat[,paste(arr,'_GENOTYPED',sep="")] == 1)]<-0
	dat[,paste(arr,'_DUPLICATE_REMOVE',sep="")]<-NA
	dat[,paste(arr,'_DUPLICATE_REMOVE',sep="")][which(dat[,paste(arr,'_GENOTYPED',sep="")] == 1)]<-0
	dat[,paste(arr,'_IBD_SHARING_REMOVE',sep="")]<-NA
	dat[,paste(arr,'_IBD_SHARING_REMOVE',sep="")][which(dat[,paste(arr,'_GENOTYPED',sep="")] == 1)]<-0
	dat[,paste(arr,'_SEXCHECK_REMOVE',sep="")]<-NA
	dat[,paste(arr,'_SEXCHECK_REMOVE',sep="")][which(dat[,paste(arr,'_GENOTYPED',sep="")] == 1)]<-0
	dat[,paste(arr,'_ANCESTRY_OUTLIERS_REMOVE',sep="")]<-NA
	dat[,paste(arr,'_ANCESTRY_OUTLIERS_REMOVE',sep="")][which(dat[,paste(arr,'_GENOTYPED',sep="")] == 1)]<-0
	dat[,paste(arr,'_CROSS_ARRAY_DUPLICATES_REMOVE',sep="")]<-NA
	dat[,paste(arr,'_CROSS_ARRAY_DUPLICATES_REMOVE',sep="")][which(dat[,paste(arr,'_GENOTYPED',sep="")] == 1)]<-0
	dat[,paste(arr,'_CROSS_ARRAY_IBD_SHARING_REMOVE',sep="")]<-NA
	dat[,paste(arr,'_CROSS_ARRAY_IBD_SHARING_REMOVE',sep="")][which(dat[,paste(arr,'_GENOTYPED',sep="")] == 1)]<-0
	istats_remove<-scan(arrays[[arr]][['istats_remove']],what="character",sep="\n")
	duplicate_remove<-scan(arrays[[arr]][['duplicate_remove']],what="character",sep="\n")
	ibd_sharing_remove<-scan(arrays[[arr]][['ibd_sharing_remove']],what="character",sep="\n")
	sexcheck_remove<-scan(arrays[[arr]][['sexcheck_remove']],what="character",sep="\n")
	dat[,paste(arr,'_ISTATS_REMOVE',sep="")][which(dat[,paste(arr,'_GENOTYPED',sep="")] == 1 & dat$IID %in% istats_remove)]<-1
	dat[,paste(arr,'_DUPLICATE_REMOVE',sep="")][which(dat[,paste(arr,'_GENOTYPED',sep="")] == 1 & dat$IID %in% duplicate_remove)]<-1
	dat[,paste(arr,'_IBD_SHARING_REMOVE',sep="")][which(dat[,paste(arr,'_GENOTYPED',sep="")] == 1 & dat$IID %in% ibd_sharing_remove)]<-1
	dat[,paste(arr,'_SEXCHECK_REMOVE',sep="")][which(dat[,paste(arr,'_GENOTYPED',sep="")] == 1 & dat$IID %in% sexcheck_remove)]<-1
	dat[,paste(arr,'_ANCESTRY_OUTLIERS_REMOVE',sep="")][which(dat[,paste(arr,'_GENOTYPED',sep="")] == 1 & dat$IID %in% ancestry_outliers_remove)]<-1
	dat[,paste(arr,'_CROSS_ARRAY_DUPLICATES_REMOVE',sep="")][which(dat[,paste(arr,'_GENOTYPED',sep="")] == 1 & dat$IID %in% cross_array_duplicates_remove)]<-1
	dat[,paste(arr,'_CROSS_ARRAY_IBD_SHARING_REMOVE',sep="")][which(dat[,paste(arr,'_GENOTYPED',sep="")] == 1 & dat$IID %in% cross_array_excessive_sharing_remove)]<-1

	## generate keep and remove columns
	dat[,paste(arr,'_REMOVE',sep="")]<-rowSums(dat[,c(paste(arr,"_ISTATS_REMOVE",sep=""),paste(arr,"_DUPLICATE_REMOVE",sep=""),paste(arr,"_IBD_SHARING_REMOVE",sep=""),paste(arr,"_SEXCHECK_REMOVE",sep=""),paste(arr,"_ANCESTRY_OUTLIERS_REMOVE",sep=""),paste(arr,"_CROSS_ARRAY_DUPLICATES_REMOVE",sep=""),paste(arr,"_CROSS_ARRAY_IBD_SHARING_REMOVE",sep=""))])
	dat[,paste(arr,'_KEEP',sep="")][which(dat[,paste(arr,'_KEEP',sep="")] == 0 & dat[,paste(arr,'_REMOVE',sep="")] == 0)]<-1

}

## blank out any AFFY samples that are also genotyped on ILL
dat$AFFY_OVERLAP_REMOVE<-NA
dat$AFFY_OVERLAP_REMOVE[which(dat$AFFY_GENOTYPED == 1)]<-0
dat$AFFY_OVERLAP_REMOVE[which(dat$ILL_KEEP == 1 & dat$AFFY_KEEP == 1)]<-1
dat$AFFY_KEEP[which(dat$AFFY_OVERLAP_REMOVE == 1)]<-0
dat$ILL_OVERLAP_REMOVE<-NA
dat$ILL_OVERLAP_REMOVE[which(dat$ILL_GENOTYPED == 1)]<-0
dat$EX_OVERLAP_REMOVE<-NA
dat$EX_OVERLAP_REMOVE[which(dat$EX_GENOTYPED == 1)]<-0

## add inferred ancestry
anc<-read.table(ancestry_table_file,header=T,as.is=T,stringsAsFactors=F)
anc<-anc[,c("IID","FINAL")]
names(anc)[2]<-"ANCESTRY_INFERRED"
dat<-merge(dat,anc,all.x=T)

## create columns idenitifying array
dat$ARRAY<-""
for(i in 1:nrow(dat)) {
	for(arr in names(arrays)) {
		if(dat[,paste(arr,'_GENOTYPED',sep="")][i] %in% c(1)) {
			if(dat$ARRAY[i] == "") {
				dat$ARRAY[i]<-arr
			} else {
				dat$ARRAY[i]<-paste(dat$ARRAY[i],arr,sep=",")
			}
		}
	}
}
sink(arrays_table_file)
table(dat$ARRAY)
sink()


## summarize sample counts for all columns in pheno file
traits<-names(dat)
trait.matrix <- matrix(NA,nrow=length(traits),ncol=1,dimnames=list(traits,"N"))
for (i in 1:length(traits)) {
	col.id <- which(names(dat)%in%traits[i])
	if(! FALSE %in% grepl("^[[:digit:]]+|NA",dat[,col.id])) dat[,col.id]<-as.numeric(dat[,col.id])
	id <- which(!is.na(dat[,col.id]))
	if (length(id)!=0) trait.matrix[i,1] <- length(id)
}
max.print <- getOption('max.print')
options(max.print=nrow(trait.matrix) * ncol(trait.matrix))
sink(sample_counts_file)
trait.matrix
sink()
options(max.print=max.print)

## reduce to only genotyped samples
dat<-dat[dat$GENOTYPED == 1,]

## summarize genotyped sample counts for traits before transformations
traits<-names(dat)
trait.matrix <- matrix(NA,nrow=length(traits),ncol=1,dimnames=list(traits,"N"))
for (i in 1:length(traits)) {
	col.id <- which(names(dat)%in%traits[i])
	if(! FALSE %in% grepl("^[[:digit:]]+|NA",dat[,col.id])) dat[,col.id]<-as.numeric(dat[,col.id])
	id <- which(!is.na(dat[,col.id]))
	if (length(id)!=0) trait.matrix[i,1] <- length(id)
}
max.print <- getOption('max.print')
options(max.print=nrow(trait.matrix) * ncol(trait.matrix))
sink(sample_counts_genotyped_file)
trait.matrix
sink()
options(max.print=max.print)

## summarize genotyped sample counts by inferred ancestry for traits before transformations
traits<-names(dat)
traits_cohorts<-unique(dat$ANCESTRY_INFERRED)
trait.matrix <- matrix(NA,nrow=length(traits),ncol=length(traits_cohorts),dimnames=list(traits,traits_cohorts))
for (i in 1:length(traits)) {
	col.id <- which(names(dat)%in%traits[i])
	for (j in 1:length(traits_cohorts)) {
		row.id <- which(dat$ANCESTRY_INFERRED==traits_cohorts[j])
		id <- which(!is.na(dat[row.id,col.id]))
		if (length(id)!=0) trait.matrix[i,j] <- length(id)
	}
}
max.print <- getOption('max.print')
options(max.print=nrow(trait.matrix) * ncol(trait.matrix))
sink(sample_counts_genotyped_by_ancestry_file)
trait.matrix
sink()
options(max.print=max.print)

un<-apply(dat,2,FUN=function(x) length(unique(x)))
un_gr10<-names(un[which(un > 10 & names(un) != "IID")])
un_lt10<-names(un[which(un <= 10 & names(un) != "IID")])

quants<-c()
for(x in un_gr10) {
	dat[,x][which(dat[,x] == "NA")]<-NA
	tt<-tryCatch(as.numeric(as.character(dat[,x])),warning=function(w) w)
	if(! is(tt,"warning")) {
		quants<-c(quants,x)
		dat[,x]<-as.numeric(as.character(dat[,x]))
	}
}

cats<-un_lt10
for(x in un_gr10) {
	tt<-tryCatch(as.numeric(as.character(dat[,x])),warning=function(w) w)
	if(is(tt,"warning")) {
		cats<-c(cats,x)
	}
}

## write summaries of variables to file
sink("categorical.summaries")
lapply(dat[,cats],table)
sink()

fullsummary<-function(x) {
	s<-summary(x)
	if(! "NA's" %in% names(s)) {
		s["NA's"]<-0
	}
	names(s)<-gsub("Min.","min",names(s))
	names(s)<-gsub("1st Qu.","q1",names(s))
	names(s)<-gsub("Median","median",names(s))
	names(s)<-gsub("Mean","mean",names(s))
	names(s)<-gsub("3rd Qu.","q3",names(s))
	names(s)<-gsub("Max.","max",names(s))
	names(s)<-gsub("NA's","missing",names(s))
	return(s)
}

quants_table<-data.frame(t(do.call(cbind,lapply(dat[,quants],fullsummary))))
quants_cols<-colnames(quants_table)
quants_table$Variable<-rownames(quants_table)
quants_table<-quants_table[,c('Variable',quants_cols)]
rownames(quants_table)<-NULL

max.print <- getOption('max.print')
options(max.print=nrow(quants_table) * ncol(quants_table))
options(width=1000)
sink("quantitative.summaries")
quants_table
sink()
options(max.print=max.print)

## write master phenotype file
write.table(dat,master_pheno,row.names=F,col.names=T,sep="\t",quote=F,append=F,na="NA")
