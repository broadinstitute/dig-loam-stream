#!/bin/bash

source /broad/software/scripts/useuse

POPULATION=NA
CHROMOSOME=NA
REPLACE=false
while getopts "s:q:c:p:r" opt; do
	case $opt in
		s)
			SOURCE=$OPTARG
			;;
		q)
			QC_STEP=$OPTARG
			;;
		p)
			POPULATION=$OPTARG
			;;
		r)
			REPLACE=true
			;;
		c)
			CHROMOSOME=$OPTARG
			;;
		\?)
			echo "Invalid option: -$OPTARG" >&2
			exit 1
			;;
		:)
			echo "Option -$OPTARG requires an argument." >&2
			exit 1
			;;
	esac
done	

# Pipeline source file
# Contains input file and external software/script definitions
source $SOURCE

echo ""
echo "Performing QC Step ${QC_STEP} on ${DATA}"

# Description: Align data strand to 1KG reference. Also, update reference allele and variant ID to match 1KG
# Requires: Plink1.9 and, at least, Genotype Harmonizer v1.4.18
# Commandline Requirements: -c
# Input: Plink binary fileset ${DATA}.bed/bim/fam, $KG_VCF_BASE reference VCF file, $LABEL output file prefix
# Output: data/${LABEL}.chr${CHROMOSOME}.bed/bim/fam, data/${LABEL}.chr${CHROMOSOME}.harmonized.bed/bim/fam/log(/nosex?/hh?), data/${LABEL}.chr${CHROMOSOME}.harmonized_idUpdates.txt, data/${LABEL}.chr${CHROMOSOME}.harmonized_snpLog.log
# Example: ./QCpipeline -s $SOURCE -q harmonize -c 22
# Notes: Could also add --variants and --mafAlign as pipeline options, but for now these are static
# Hail?: No
if [ "$QC_STEP" == "harmonize" ]; then
	reuse Java-1.8
	plink --bfile $DATA --chr $CHROMOSOME --keep-allele-order --make-bed --out data/${LABEL}.chr${CHROMOSOME}

	$GENOTYPE_HARMONIZER \
	--input data/${LABEL}.chr${CHROMOSOME} \
	--inputType PLINK_BED \
	--output data/${LABEL}.chr${CHROMOSOME}.harmonized \
	--outputType PLINK_BED \
	--ref $KG_VCF_BASE \
	--refType VCF \
	--keep \
	--update-id \
	--variants 1000 \
	--mafAlign 0.1 \
	--update-id \
	--update-reference-allele \
	--debug

# Description: Compile harmonized data
# Requires: Plink1.9, Tabix
# Commandline Requirements: 
# Input: data/${LABEL}.chr${CHROMOSOME}.harmonized.bed/bim/fam
# Output: data/${LABEL}.harmonized.bed/bim/fam/log(/nosex?/hh?), data/${LABEL}.harmonized.force_a2, data/${LABEL}.harmonized.ref.bed/bim/fam/log(/nosex?/hh?)
# Example: ./QCpipeline -s CAMP.source -q compile
# Notes: 
# Hail?: Possible, but not necessary
elif [ "$QC_STEP" == "compile" ]; then
	reuse Tabix

	echo "data/${LABEL}.chr2.harmonized.bed data/${LABEL}.chr2.harmonized.bim data/${LABEL}.chr2.harmonized.fam" > data/${LABEL}.harmonized.merge.txt
	for i in {3..22}; do echo "data/${LABEL}.chr${i}.harmonized.bed data/${LABEL}.chr${i}.harmonized.bim data/${LABEL}.chr${i}.harmonized.fam" >> data/${LABEL}.harmonized.merge.txt; done
	plink --bfile data/${LABEL}.chr1.harmonized --merge-list data/${LABEL}.harmonized.merge.txt --make-bed --keep-allele-order --out data/${LABEL}.harmonized
	awk '{print $2,$5}' data/${LABEL}.harmonized.bim > data/${LABEL}.harmonized.force_a2
	plink --bfile data/${LABEL}.harmonized --recode vcf-iid bgz --real-ref-alleles --a2-allele data/${LABEL}.harmonized.force_a2 --out data/${LABEL}.harmonized.ref
	tabix -p vcf data/${LABEL}.harmonized.ref.vcf.gz

# Description: Generate the Hail VDS from VCF file
# Requires: $HAIL, Java (version under which Hail was compiled)
# Commandline Requirements: 
# Input: data/${LABEL}.harmonized.vcf.bgz, data/CAMP.sample
# Output: data/${LABEL}.harmonized.vds.log, data/${LABEL}.harmonized.vds/
# Example: ./QCpipeline -s CAMP.source -q load
# Notes: Monomorphic variants are automatically removed during import into Hail
# Hail?: Yes
elif [ "$QC_STEP" == "load" ]; then # directory preparations
	reuse Java-1.8

	$HAIL -l data/${LABEL}.harmonized.ref.vds.log \
	importvcf --force-bgz data/${LABEL}.harmonized.ref.vcf.gz \
	splitmulti \
	deduplicate \
	write \
	-o data/${LABEL}.harmonized.ref.vds \
	count \
	-g

# Description: Generate filtered Plink binary fileset for QC
# Requires: $HAIL, Java (version under which Hail was compiled)
# Commandline Requirements: 
# Input: data/${LABEL}.harmonized.vds/, $REGIONS_EXCLUDE
# Output: data/${LABEL}.filter.variantqc.log, data/${LABEL}.filter.for_qc.log, data/${LABEL}.variantqc.tsv, data/${LABEL}.for_qc.bed/bim/fam/log(/nosex?/hh?)
# Example: ./QCpipeline -s CAMP.source -q filter
# Notes: 
# Hail?: Yes
elif [ "$QC_STEP" == "filter" ]; then # data preparations
	reuse Java-1.8
	reuse Tabix

	#$HAIL -l data/${LABEL}.filter.variantqc.log \
	#read -i data/${LABEL}.harmonized.ref.vds \
	#variantqc \
	#exportvariants -o data/${LABEL}.variantqc.tsv -c "ID = v, Chrom = v.contig, Pos = v.start, Ref = v.ref, Alt = v.alt, va.qc.*"

	$HAIL -l data/${LABEL}.filter.for_qc.log \
	read -i data/${LABEL}.harmonized.ref.vds \
	variantqc \
	filtervariants expr -c 'v.altAllele.isSNP && ! v.altAllele.isComplex && v.isAutosomal && ["A","C","G","T"].toSet.contains(v.altAllele.ref) && ["A","C","G","T"].toSet.contains(v.altAllele.alt) && va.qc.AF >= 0.01 && va.qc.callRate >= 0.98' --keep \
	filtervariants intervals -i $REGIONS_EXCLUDE --remove \
	exportplink -o data/${LABEL}.for_qc

# Description: Generate filtered Plink binary fileset for QC
# Requires: $KING, R, $KINSHIP_CALC_SAMPLE_SHARING_R
# Commandline Requirements: 
# Input: data/${LABEL}.for_qc.bed/bim/fam
# Output: 
# Example: ./QCpipeline -s CAMP.source -q kinship
# Notes: King is preferred to Plink or Hail based IBD calcs due to robust algorithm handling of population stratification
# Hail?: No
elif [ "$QC_STEP" == "kinship" ]; then
	reuse R-3.1

	plink --bfile data/${LABEL}.for_qc --indep-pairwise 1500 150 0.2 --out kinship/${LABEL}.kinship
	plink --bfile data/${LABEL}.for_qc --extract kinship/${LABEL}.kinship.prune.in --make-bed --out kinship/${LABEL}.kinship.pruned
	$KING -b kinship/${LABEL}.kinship.pruned.bed --kinship --prefix kinship/${LABEL}.kinship.pruned.king
	(head -1 kinship/${LABEL}.kinship.pruned.king.kin0; sed '1d' kinship/${LABEL}.kinship.pruned.king.kin0 | awk '{if($8 >= 0.0884) print $0}' | sort -rn -k8,8) > kinship/${LABEL}.kinship.pruned.king.kin0.related
	R --vanilla --args kinship/${LABEL}.kinship.pruned.king.kin0.related kinship/${LABEL}.kinship.pruned.king.sharing_counts.txt < $KINSHIP_CALC_SAMPLE_SHARING_R

# Description: Calculate PCs by projecting data onto 1KG Phase 3 Purcell 5k PCs using existing PCA loadings
# Requires: $HAIL, R, $PLOT_ANCESTRY_PCA_R
# Commandline Requirements: 
# Input: data/${LABEL}.harmonized.vds, $KG_V3_5K_AF, $KG_V3_5K_PCA_LOADINGS
# Output: ancestry/${LABEL}.pca.scores.log, ancestry/${LABEL}.pca.frequencies.tsv, ancestry/.${LABEL}.pca.frequencies.tsv.crc, ancestry/${LABEL}.pca.scores.tsv, ancestry/.${LABEL}.pca.scores.tsv.crc ancestry/CAMP.pca.scores.tsv.plots.pdf
# Example: ./QCpipeline -s CAMP.source -q ancestry
# Notes: Reference PCA loadings are calculated only once, so we import only the loadings and frequencies to do the projection
# Hail?: Yes
elif [ "$QC_STEP" == "ancestry" ]; then
	reuse Java-1.8
	reuse R-3.1

	$HAIL -l ancestry/${LABEL}.pca.scores.log \
	read -i data/${LABEL}.harmonized.ref.vds \
	variantqc \
	annotatevariants table $KG_V3_5K_AF \
	-e Variant \
	-c 'va.refPanelAF = table.refPanelAF' \
	--impute \
	annotatevariants table $KG_V3_5K_PCA_LOADINGS \
	-e 'ID' \
	-c 'va = merge(va, select(table, PC1, PC2, PC3))' \
	--impute \
	annotatevariants expr -c 'va.PCs = [va.PC1, va.PC2, va.PC3]' \
	exportvariants -c "ID = v, refPanelAF = va.refPanelAF, AF = va.qc.AF" \
	-o ancestry/${LABEL}.pca.frequencies.tsv \
	annotatesamples expr -c 'sa.PCs = gs.map(g => let p = va.refPanelAF in if (p == 0 || p == 1) [0.0, 0.0, 0.0] else (g.gt - 2 * p) / sqrt(2 * p * (1 - p)) * va.PCs).sum()' \
	exportsamples -c 'IID = s.id, PC1 = sa.PCs[0], PC2 = sa.PCs[1], PC3 = sa.PCs[2]' \
	-o ancestry/${LABEL}.pca.scores.tsv

	R --vanilla --args ancestry/${LABEL}.pca.scores.tsv < $PLOT_ANCESTRY_PCA_R

elif [ "$QC_STEP" == "ancestry_cluster" ]; then # ancestry inference GMM clustering
	cd ancestry_cluster
	reuse R-3.1

	echo 10 > ${LABEL}_1kg.ref.fet.1
	sed '1d' ../ancestry_pca/${LABEL}_1kg.ref.pca.evec | sed 's/^\s\+//' | sed 's/\s\+/\t/g' | cut -f2-11 | sed 's/\t/ /g' >> ${LABEL}_1kg.ref.fet.1
	$KLUSTAKWIK ${LABEL}_1kg.ref 1 -UseFeatures 1110000000 -UseDistributional 0
	R --vanilla --args $LABEL $KG_ETHNICITY $PHENO ID RACE < $ANCESTRY_CLUSTER_PLOT_R
	cd ..

elif [ "$QC_STEP" == "ancestry_cluster_merge" ]; then # merge ancestry information hierarchically
	cd ancestry_cluster
	if [ ! -f chip.hierarchy ]; then
		echo "file chip.hierarchy missing!"
		exit
	fi
	R --vanilla --args chip.hierarchy < $ANCESTRY_CLUSTER_MERGE_R
	cp ancestry.OUTLIERS ../samples_flagged/ancestry_outliers.remove
	cd ..

elif [ "$QC_STEP" == "ancestry_pca_clustered" ]; then
	cd ancestry_pca_clustered
	reuse R-3.1

	# run eigenstrat on merged data, projecting data onto 1kg PCs for corresponding population
	awk '{print $1,$2}' ../data/${LABEL}.base.fam > ${LABEL}.keep
	plink --bfile ../ancestry_pca/${LABEL}_1kg.ref --keep ../ancestry_cluster/ancestry.CLUSTERED.plink --make-bed --out ${LABEL}_1kg.ref.CLUSTERED
	plink --bfile ${LABEL}_1kg.ref.CLUSTERED --recode --out ${LABEL}_1kg.ref.CLUSTERED
	# generate parameter file for smartpca
	echo "genotypename:       ${LABEL}_1kg.ref.CLUSTERED.ped" > ${LABEL}_1kg.ref.CLUSTERED.pca.par
	echo "snpname:            ${LABEL}_1kg.ref.CLUSTERED.map" >> ${LABEL}_1kg.ref.CLUSTERED.pca.par
	echo "indivname:          ${LABEL}_1kg.ref.CLUSTERED.fam" >> ${LABEL}_1kg.ref.CLUSTERED.pca.par
	echo "evecoutname:        ${LABEL}_1kg.ref.CLUSTERED.pca.evec" >> ${LABEL}_1kg.ref.CLUSTERED.pca.par
	echo "evaloutname:        ${LABEL}_1kg.ref.CLUSTERED.pca.eval" >> ${LABEL}_1kg.ref.CLUSTERED.pca.par
	echo "altnormstyle:       NO" >> ${LABEL}_1kg.ref.CLUSTERED.pca.par
	echo "numoutevec:         10" >> ${LABEL}_1kg.ref.CLUSTERED.pca.par
	echo "numoutlieriter:     0" >> ${LABEL}_1kg.ref.CLUSTERED.pca.par
	echo "nsnpldregress:      0" >> ${LABEL}_1kg.ref.CLUSTERED.pca.par
	echo "noxdata:            YES" >> ${LABEL}_1kg.ref.CLUSTERED.pca.par
	echo "numoutlierevec:     10" >> ${LABEL}_1kg.ref.CLUSTERED.pca.par
	echo "outliersigmathresh: 6" >> ${LABEL}_1kg.ref.CLUSTERED.pca.par
	echo "outlieroutname:     ${LABEL}_1kg.ref.CLUSTERED.pca.outliers" >> ${LABEL}_1kg.ref.CLUSTERED.pca.par
	echo "snpweightoutname:   ${LABEL}_1kg.ref.CLUSTERED.pca.snpwts" >> ${LABEL}_1kg.ref.CLUSTERED.pca.par
	# run smartpca
	$SMARTPCA -p ${LABEL}_1kg.ref.CLUSTERED.pca.par > ${LABEL}_1kg.ref.CLUSTERED.pca.log
	cd ..

elif [ "$QC_STEP" == "plinkseq" ]; then # prepare Plink/Seq project
	reuse PSEQ
	reuse Tabix

	# run Plink/Seq i-stats
	cd data
	if [ ! -f ../data/${LABEL}.qc.bi.chr1-22.bed ]; then
		plink --bfile ${LABEL}.qc.bi --chr 1-22 --make-bed --out ${LABEL}.qc.bi.chr1-22
	fi
	if [ ! -f ../data/${LABEL}.qc.bi.chr1-22.vcf.gz ]; then
		plink --bfile ${LABEL}.qc.bi.chr1-22 --recode vcf-iid bgz --out ${LABEL}.qc.bi.chr1-22.temp
		tabix -p vcf ${LABEL}.qc.bi.chr1-22.temp.vcf.gz
		tabix -H ${LABEL}.qc.bi.chr1-22.temp.vcf.gz > ${LABEL}.qc.bi.chr1-22.temp.header
		sed -i 's/VCFv4\.2/VCFv4\.0/g' ${LABEL}.qc.bi.chr1-22.temp.header
		tabix -r ${LABEL}.qc.bi.chr1-22.temp.header ${LABEL}.qc.bi.chr1-22.temp.vcf.gz > ${LABEL}.qc.bi.chr1-22.vcf.gz
		tabix -p vcf ${LABEL}.qc.bi.chr1-22.vcf.gz
		rm ${LABEL}.qc.bi.chr1-22.temp*
	fi
	cd ../istats
	if [ ! -f ${LABEL}.pseq ]; then
		pseq ${LABEL} new-project
		pseq ${LABEL} index-vcf --vcf ../data/${LABEL}.qc.bi.chr1-22.vcf.gz --id ${LABEL}
	fi
	cd ..
	
elif [ "$QC_STEP" == "sexcheck" ]; then # calculate sample stats for each 1kg population and all together
	cd istats
	plink --bfile ../data/${LABEL}.base --check-sex --out ${LABEL}.sexcheck
	sed '1d' ${LABEL}.sexcheck.sexcheck | awk '{if($5 == "PROBLEM") print $2}' > ../samples_flagged/${LABEL}.failed_sexcheck.remove
	cd ..

elif [ "$QC_STEP" == "istats" ]; then # calculate sample stats for each 1kg population and all together
	reuse R-3.1
	reuse PSEQ

	cd istats
	if [ ! -s ${LABEL}.istats ]; then
		pseq ${LABEL} i-stats > ${LABEL}.istats
	fi
    
	plink --bfile ../data/${LABEL}.qc.bi.chr1-22 --het --out ${LABEL}.het
	plink --bfile ../data/${LABEL}.qc.bi.chr1-22 --het --maf 0.03 --out ${LABEL}.het.high
	plink --bfile ../data/${LABEL}.qc.bi.chr1-22 --het --max-maf 0.03 --out ${LABEL}.het.low
    
	# compile all sample stats into single file
	R --vanilla --args ${LABEL}.istats ${LABEL}.het.het ${LABEL}.het.high.het ${LABEL}.het.low.het ${LABEL}.istats.all < $ISTATS_COMPILE_R

	# calculate PC adjusted istats
	R --vanilla --args ${LABEL}.istats.all ../ancestry_pca_clustered/${LABEL}_1kg.ref.CLUSTERED.pca.evec ${LABEL}.CLUSTERED.istats.all.adj < $CALC_ISTATS_ADJ_R
    
	# calculate PCs for PC adjusted istats metrics
	R --vanilla --args ${LABEL}.CLUSTERED.istats.all.adj ${LABEL}.CLUSTERED.istats.all.adj.corr.pdf ${LABEL}.CLUSTERED.istats.all.adj.pca.loadings ${LABEL}.CLUSTERED.istats.all.adj.pcs.pdf ${LABEL}.CLUSTERED.istats.all.adj.pcs < $ISTATS_ADJ_PCA_R
	cd ..

elif [ "$QC_STEP" == "istats_cluster_pca" ]; then # cluster PCs of adjusted istats metrics
	reuse R-3.1

	cd istats_cluster_pca
	cp ../istats/${LABEL}.CLUSTERED.istats.all.adj.pcs .
	cp ../istats/${LABEL}.CLUSTERED.istats.all.adj .

	# run klustakwik
	n=`head -1 ${LABEL}.CLUSTERED.istats.all.adj.pcs | wc | awk '{print $2-1}'`
	echo $n > ${LABEL}.CLUSTERED.istats.all.adj.fet.1
	sed '1d' ${LABEL}.CLUSTERED.istats.all.adj.pcs | cut -f2- | sed 's/\t/ /g' >> ${LABEL}.CLUSTERED.istats.all.adj.fet.1
	features=1
	for i in `seq 2 $n`; do
		features=${features}1
	done
	echo -e "${LABEL}\t${features}"
	$KLUSTAKWIK ${LABEL}.CLUSTERED.istats.all.adj 1 -UseFeatures $features -UseDistributional 0

	R --vanilla --args ${LABEL}.CLUSTERED.istats.all.adj.pcs ${LABEL}.CLUSTERED.istats.all.adj.clu.1 ${LABEL}.CLUSTERED.istats.all.adj.pcs.outliers ${LABEL}.CLUSTERED.istats.all.adj.pcs.clusters.pdf ${LABEL}.CLUSTERED.istats.all.adj.pcs.clusters_xtab $LABEL < $ISTATS_PCS_GMM_CLUSTER_PLOT_R

	R --vanilla --args ${LABEL}.CLUSTERED.istats.all.adj ${LABEL}.CLUSTERED.istats.all.adj.pcs.outliers ${LABEL}.CLUSTERED.istats.all.adj.stripchart.pdf < $ISTATS_PCS_GMM_PLOT_METRICS_R
	cd ..

elif [ "$QC_STEP" == "istats_cluster_individual" ]; then # cluster adjusted istats metrics and list all outliers
	reuse R-3.1

	cd istats_cluster_individual

	cp ../istats/${LABEL}.CLUSTERED.istats.all.adj.pcs .
	cp ../istats/${LABEL}.CLUSTERED.istats.all.adj .
    
	# run klustakwik
	n=`head -1 ${LABEL}.CLUSTERED.istats.all.adj | wc | awk '{print $2-1}'`
	for feature in `seq 2 $((n+1))`; do
		id=`head -1 ${LABEL}.CLUSTERED.istats.all.adj | awk -v c=$feature '{print $c}'`
		echo $n > ${LABEL}.CLUSTERED.${id}.istats.all.adj.fet.1
		sed '1d' ${LABEL}.CLUSTERED.istats.all.adj | cut -f2- | sed 's/\t/ /g' >> ${LABEL}.CLUSTERED.${id}.istats.all.adj.fet.1
		include=''
		for i in `seq 2 $((n+1))`; do
			if [ $feature -eq $i ]; then
				include=${include}1
			else
				include=${include}0
			fi
		done
		echo -e "${LABEL}\t${include}"
		#R --vanilla --args ../istats/${LABEL}.CLUSTERED.istats.all.adj $id ../ancestry_cluster/ancestry.table ${LABEL}.CLUSTERED.${id}.istats.all.adj.bins < $BIN_METRIC_R
		#$KLUSTAKWIK ${LABEL}.CLUSTERED.${id}.istats.all.adj 1 -UseFeatures $include -UseDistributional 0 -StartCluFile ${LABEL}.CLUSTERED.${id}.istats.all.adj.bins > ${LABEL}.CLUSTERED.${id}.istats.all.adj.fet.1.klustakwik.log
		$KLUSTAKWIK ${LABEL}.CLUSTERED.${id}.istats.all.adj 1 -UseFeatures $include -UseDistributional 0 > ${LABEL}.CLUSTERED.${id}.istats.all.adj.fet.1.klustakwik.log
	done
    
	R --vanilla --args ${LABEL} < $ISTATS_ADJ_GMM_PLOT_METRICS_R
	cd ..

elif [ "$QC_STEP" == "make_master_remove_lists" ]; then
	cd samples_flagged
	(cat ${LABEL}.CLUSTERED.istats.outliers.remove; cat ${LABEL}.duplicates.remove; cat ${LABEL}.excessive_sharing.remove; cat ancestry_outliers.remove; cat ${LABEL}.failed_sexcheck.remove) | sort -u > ${LABEL}.remove
	awk '{print $1" "$1}' ${LABEL}.remove >  ${LABEL}.remove.plink
	cd ..

elif [ "$QC_STEP" == "pedigree" ]; then
	if [ "$POPULATION" == "NA" ]; then
		echo "population (-p) must be set"
		exit
	fi
	reuse R-3.1
	reuse Python-2.7
	cd data_post_qc

	### use PRIMUS Maximum unrelated set identification to generate unrelated samples list for calculating variant stats with founders only
	plink --bfile ../data/${LABEL}.qc.bi.chr1-22 --keep ../ancestry_cluster/ancestry.${POPULATION}.plink --remove ../samples_flagged/${LABEL}.remove.plink --make-bed --out ${LABEL}.qc.bi.chr1-22.${POPULATION}
	plink --bfile ${LABEL}.qc.bi.chr1-22.${POPULATION} --geno 0.02 --maf 0.01 --exclude $REGIONS_EXCLUDE --indep-pairwise 1500 150 0.2 --make-bed --out ${LABEL}.qc.bi.chr1-22.${POPULATION}.prune
	plink --bfile ${LABEL}.qc.bi.chr1-22.${POPULATION}.prune --genome --out ${LABEL}.qc.bi.chr1-22.${POPULATION}.prune.ibd
	(head -1 ${LABEL}.qc.bi.chr1-22.${POPULATION}.prune.ibd.genome; sed '1d' ${LABEL}.qc.bi.chr1-22.${POPULATION}.prune.ibd.genome | awk '{if(($2 != $4 && $10 >= 0.25) || (($2 != $4 && $10 >= 0.7) || ($2 == $4 && $10 <= 0.7 && $10 >= 0.25))) print $0}' | sort -nr -k10,10) > ${LABEL}.qc.bi.chr1-22.${POPULATION}.prune.ibd.genome.problems.related
	(head -1 ${LABEL}.qc.bi.chr1-22.${POPULATION}.prune.ibd.genome; sed '1d' ${LABEL}.qc.bi.chr1-22.${POPULATION}.prune.ibd.genome | awk '{if($2 == $4 && $10 < 0.25) print $0}' | sort -nr -k10,10) > ${LABEL}.qc.bi.chr1-22.${POPULATION}.prune.ibd.genome.problems.unrelated
	$KING -b ${LABEL}.qc.bi.chr1-22.${POPULATION}.prune.bed --kinship --prefix ${LABEL}.qc.bi.chr1-22.${POPULATION}.prune.king
	$KING -b ${LABEL}.qc.bi.chr1-22.${POPULATION}.prune.bed --unrelated --degree 2 --prefix ${LABEL}.qc.bi.chr1-22.${POPULATION}.prune.king.unrelated
	(head -1 ${LABEL}.qc.bi.chr1-22.${POPULATION}.prune.king.kin0; sed '1d' ${LABEL}.qc.bi.chr1-22.${POPULATION}.prune.king.kin0 | awk '{if($8 >= 0.0884) print $0}') > ${LABEL}.qc.bi.chr1-22.${POPULATION}.prune.king.kin0.related
	R --vanilla --args ${LABEL}.qc.bi.chr1-22.${POPULATION}.prune.ibd.genome.problems.related ${LABEL}.qc.bi.chr1-22.${POPULATION}.prune.king.kin0.related ${LABEL}.qc.bi.chr1-22.${POPULATION}.prune.ibd.genome.problems.related.robust < $COMPARE_PLINK_KING_R
	$PRIMUS -p ${LABEL}.qc.bi.chr1-22.${POPULATION}.prune.ibd.genome.problems.related.robust -d 2 --max_gens 3
	python $COMPILE_PEDIGREE_PY ${LABEL}.qc.bi.chr1-22.${POPULATION}.prune.ibd.genome.problems.related.robust_PRIMUS/${LABEL}.qc.bi.chr1-22.${POPULATION}.prune.ibd.genome.problems.related.robust_unrelated_samples.txt "${LABEL}.qc.bi.chr1-22.${POPULATION}.prune.ibd.genome.problems.related.robust_PRIMUS/*/" ${LABEL}.qc.bi.chr1-22.${POPULATION}.prune.ibd.genome.problems.related.robust_PRIMUS/${LABEL}.qc.bi.chr1-22.${POPULATION}.prune.ibd.genome.problems.related.robust_networkXXXXX/${LABEL}.qc.bi.chr1-22.${POPULATION}.prune.ibd.genome.problems.related.robust_networkXXXXX_1.fam ${LABEL}.${POPULATION}.pedigree
	R --vanilla --args ${LABEL}.${POPULATION}.pedigree ${LABEL}.qc.bi.chr1-22.${POPULATION}.prune.ibd.genome.problems.related.robust_PRIMUS/${LABEL}.qc.bi.chr1-22.${POPULATION}.prune.ibd.genome.problems.related.robust_unrelated_samples.txt ${LABEL}.qc.bi.chr1-22.${POPULATION}.fam ${LABEL}.${POPULATION}.unrel ${LABEL}.${POPULATION}.unrel.plink < $GENERATE_UNREL_FILE_R
	cd ..

elif [ "$QC_STEP" == "pedigree_merged" ]; then
	reuse R-3.1
	reuse Python-2.7
	cd data_post_qc_merged

	### use PRIMUS Maximum unrelated set identification to generate unrelated samples list for calculating variant stats with founders only
	echo "../data_impute/BIOME_AFFY.impute.harmonized.chr2.bed ../data_impute/BIOME_AFFY.impute.harmonized.chr2.bim ../data_impute/BIOME_AFFY.impute.harmonized.chr2.fam" > BIOME_AFFY.merge_list.txt
	for i in {3..22}; do echo "../data_impute/BIOME_AFFY.impute.harmonized.chr${i}.bed ../data_impute/BIOME_AFFY.impute.harmonized.chr${i}.bim ../data_impute/BIOME_AFFY.impute.harmonized.chr${i}.fam" >> BIOME_AFFY.merge_list.txt; done
	echo "../data_impute/BIOME_ILL.impute.harmonized.chr2.bed ../data_impute/BIOME_ILL.impute.harmonized.chr2.bim ../data_impute/BIOME_ILL.impute.harmonized.chr2.fam" > BIOME_ILL.merge_list.txt
	for i in {3..22}; do echo "../data_impute/BIOME_ILL.impute.harmonized.chr${i}.bed ../data_impute/BIOME_ILL.impute.harmonized.chr${i}.bim ../data_impute/BIOME_ILL.impute.harmonized.chr${i}.fam" >> BIOME_ILL.merge_list.txt; done
	echo "../data_impute/BIOME_EX.impute.harmonized.chr2.bed ../data_impute/BIOME_EX.impute.harmonized.chr2.bim ../data_impute/BIOME_EX.impute.harmonized.chr2.fam" > BIOME_EX.merge_list.txt
	for i in {3..22}; do echo "../data_impute/BIOME_EX.impute.harmonized.chr${i}.bed ../data_impute/BIOME_EX.impute.harmonized.chr${i}.bim ../data_impute/BIOME_EX.impute.harmonized.chr${i}.fam" >> BIOME_EX.merge_list.txt; done
	plink --bfile ../data_impute/BIOME_AFFY.impute.harmonized.chr1 --merge-list BIOME_AFFY.merge_list.txt --make-bed --keep-allele-order --out BIOME_AFFY.impute.harmonized
	plink --bfile BIOME_AFFY.impute.harmonized --exclude ../data/BIOME_AFFY.multiallelic --make-bed --keep-allele-order --out BIOME_AFFY.impute.harmonized.bi
	mv BIOME_AFFY.impute.harmonized.bi.fam BIOME_AFFY.impute.harmonized.bi.fam.orig
	awk '{$1=$2; print $0}' BIOME_AFFY.impute.harmonized.bi.fam.orig > BIOME_AFFY.impute.harmonized.bi.fam
	plink --bfile ../data_impute/BIOME_ILL.impute.harmonized.chr1 --merge-list BIOME_ILL.merge_list.txt --make-bed --keep-allele-order --out BIOME_ILL.impute.harmonized
	plink --bfile BIOME_ILL.impute.harmonized --exclude ../data/BIOME_ILL.multiallelic --make-bed --keep-allele-order --out BIOME_ILL.impute.harmonized.bi
	mv BIOME_ILL.impute.harmonized.bi.fam BIOME_ILL.impute.harmonized.bi.fam.orig
	awk '{$1=$2; print $0}' BIOME_ILL.impute.harmonized.bi.fam.orig > BIOME_ILL.impute.harmonized.bi.fam
	plink --bfile ../data_impute/BIOME_EX.impute.harmonized.chr1 --merge-list BIOME_EX.merge_list.txt --make-bed --keep-allele-order --out BIOME_EX.impute.harmonized
	plink --bfile BIOME_EX.impute.harmonized --exclude ../data/BIOME_EX.multiallelic --make-bed --keep-allele-order --out BIOME_EX.impute.harmonized.bi
	mv BIOME_EX.impute.harmonized.bi.fam BIOME_EX.impute.harmonized.bi.fam.orig
	awk '{$1=$2; print $0}' BIOME_EX.impute.harmonized.bi.fam.orig > BIOME_EX.impute.harmonized.bi.fam
	echo "BIOME_ILL.impute.harmonized.bi.bed BIOME_ILL.impute.harmonized.bi.bim BIOME_ILL.impute.harmonized.bi.fam" > merge_list.txt
	echo "BIOME_AFFY.impute.harmonized.bi.bed BIOME_AFFY.impute.harmonized.bi.bim BIOME_AFFY.impute.harmonized.bi.fam" >> merge_list.txt
	plink --bfile BIOME_EX.impute.harmonized.bi --merge-list merge_list.txt --merge-equal-pos --make-bed --out BIOME_MERGED.impute.harmonized
	awk '{if(x[$1":"$4]) {x_count[$1":"$4]++; print $2; if(x_count[$1":"$4] == 1) {print x[$1":"$4]}} x[$1":"$4] = $2}' BIOME_MERGED.impute.harmonized.bim > BIOME_MERGED.impute.harmonized.multiallelic
	plink --bfile BIOME_MERGED.impute.harmonized --exclude BIOME_MERGED.impute.harmonized.multiallelic --make-bed --keep-allele-order --out BIOME_MERGED.impute.harmonized.bi
    
	plink --bfile BIOME_MERGED.impute.harmonized.bi --keep ../ancestry_cluster/ancestry.AFR.plink --hardy --out BIOME_MERGED.AFR.hwe
	plink --bfile BIOME_MERGED.impute.harmonized.bi --keep ../ancestry_cluster/ancestry.EUR.plink --hardy --out BIOME_MERGED.EUR.hwe
	sed '1d' BIOME_MERGED.AFR.hwe.hwe | awk '{if($9 < 1e-6) print $2}' > BIOME_MERGED.AFR.hwe.failed.variants
	sed '1d' BIOME_MERGED.EUR.hwe.hwe | awk '{if($9 < 1e-6) print $2}' > BIOME_MERGED.EUR.hwe.failed.variants
	cat BIOME_MERGED.EUR.hwe.failed.variants BIOME_MERGED.AFR.hwe.failed.variants | sort -u > BIOME_MERGED.EUR_AFR.hwe.failed.variants
	plink --bfile BIOME_MERGED.impute.harmonized.bi --exclude BIOME_MERGED.EUR_AFR.hwe.failed.variants --geno 0.02 --maf 0.01 --make-bed --keep-allele-order --out BIOME_MERGED.ibd
    
	plink --bfile BIOME_MERGED.ibd --exclude $REGIONS_EXCLUDE --indep-pairwise 1500 150 0.2 --make-bed --out BIOME_MERGED.ibd.prune
	plink --bfile BIOME_MERGED.ibd.prune --genome --out BIOME_MERGED.ibd.prune.genome
	(head -1 BIOME_MERGED.ibd.prune.genome.genome; sed '1d' BIOME_MERGED.ibd.prune.genome.genome | awk '{if(($2 != $4 && $10 >= 0.25) || (($2 != $4 && $10 >= 0.7) || ($2 == $4 && $10 <= 0.7 && $10 >= 0.25))) print $0}' | sort -nr -k10,10) > BIOME_MERGED.ibd.prune.genome.genome.problems.related
	(head -1 BIOME_MERGED.ibd.prune.genome.genome; sed '1d' BIOME_MERGED.ibd.prune.genome.genome | awk '{if($2 == $4 && $10 < 0.25) print $0}' | sort -nr -k10,10) > BIOME_MERGED.ibd.prune.genome.genome.problems.unrelated
	$KING -b BIOME_MERGED.ibd.prune.bed --kinship --prefix BIOME_MERGED.ibd.prune.king > BIOME_MERGED.ibd.prune.king.kinship.log
	$KING -b BIOME_MERGED.ibd.prune.bed --unrelated --degree 2 --prefix BIOME_MERGED.ibd.prune.king.unrelated > BIOME_MERGED.ibd.prune.king.unrelated.log
	(head -1 BIOME_MERGED.ibd.prune.king.kin0; sed '1d' BIOME_MERGED.ibd.prune.king.kin0 | awk '{if($8 >= 0.0884) print $0}') > BIOME_MERGED.ibd.prune.king.kin0.related
	R --vanilla --args BIOME_MERGED.ibd.prune.king.kin0.related BIOME_MERGED.ibd.prune.king.kin0.related.sharing_counts.txt < $KINSHIP_CALC_SAMPLE_SHARING_R
	R --vanilla --args BIOME_MERGED.ibd.prune.genome.genome.problems.related BIOME_MERGED.ibd.prune.king.kin0.related BIOME_MERGED.ibd.prune.genome.genome.problems.related.robust < $COMPARE_PLINK_KING_R
	(sed '1d' BIOME_MERGED.ibd.prune.genome.genome.problems.related.robust | awk '{print $1}'; sed '1d' BIOME_MERGED.ibd.prune.genome.genome.problems.related.robust | awk '{print $3}') | sort -u > BIOME_MERGED.ibd.prune.genome.genome.problems.related.robust.ids
	(while read line; do grep -w "$line" $PHENO | awk -F'\t' '{if($14 == 1) { $14 = 3 } else { if($14 == 0) { $14 = 2 } else { $14 = 1 } } print $1,$1,$14}'; done < BIOME_MERGED.ibd.prune.genome.genome.problems.related.robust.ids) > BIOME_MERGED.primus.weights
	$PRIMUS -p BIOME_MERGED.ibd.prune.genome.genome.problems.related.robust -d 2 --max_gens 3 --no_PR --high_qtrait BIOME_MERGED.primus.weights
	(cat BIOME_MERGED.ibd.prune.king.unrelatedunrelated.txt; sed '1d' BIOME_MERGED.ibd.prune.genome.genome.problems.related.robust_PRIMUS/BIOME_MERGED.ibd.prune.genome.genome.problems.related.robust_maximum_independent_set) | sort -u > BIOME_MERGED.unrel.plink
	awk '{print $2}' BIOME_MERGED.unrel.plink > BIOME_MERGED.unrel
	cd ..
	
elif [ "$QC_STEP" == "hwe" ]; then # 
	if [ "$POPULATION" == "NA" ]; then
		echo "population (-p) must be set for QC step 4"
		exit
	fi
	if [ ! -d hwe ]; then
		mkdir hwe
	fi
	cd hwe
	if [ "$REPLACE" = true ]; then
		rm ${LABEL}*[!qlog]
	fi

	# calculate HWE P-value within populations using maximum unrelated sets
	plink --bfile ../data/${LABEL}.base --keep ../data_post_qc/${LABEL}.${POPULATION}.unrel.plink --hardy --out ${LABEL}.${POPULATION}.hwe
	sed '1d' ${LABEL}.${POPULATION}.hwe.hwe | awk '{if($9 < 1e-6) print $2}' > ${LABEL}.${POPULATION}.hwe.failed.variants
	cd ..

elif [ "$QC_STEP" == "clean_analysis" ]; then # 
	if [ "$POPULATION" == "NA" ]; then
		echo "population (-p) must be set for QC step 4"
		exit
	fi

	cd hwe
	if [ ! -f ${LABEL}.EUR_AFR.hwe.failed.variants ]; then
		cat ${LABEL}.EUR.hwe.failed.variants ${LABEL}.AFR.hwe.failed.variants | sort -u > ${LABEL}.EUR_AFR.hwe.failed.variants
	fi

	# generate analysis ready VCFs
	cd ../data_clean
	cat ../data/${LABEL}.variants.unplaced ../data/${LABEL}.variants.monomorphic ../data/${LABEL}.variants.duplicate.remove ../data/${LABEL}.missing.lmiss.high ../hwe/${LABEL}.EUR_AFR.hwe.failed.variants | sort -u > ${LABEL}.variants.exclude
	plink --bfile ../data/${LABEL}.base --keep ../ancestry_cluster/ancestry.${POPULATION}.plink --remove ../samples_flagged/${LABEL}.remove.plink --exclude ${LABEL}.variants.exclude --make-bed --out ${LABEL}.${POPULATION}.clean
	plink --bfile ${LABEL}.${POPULATION}.clean --recode vcf-iid bgz --out ${LABEL}.${POPULATION}.clean
	plink --bfile ../data/${LABEL}.base --keep ../ancestry_cluster/ancestry.${POPULATION}.plink --remove ../samples_flagged/${LABEL}.remove.plink --exclude ${LABEL}.variants.exclude --chr 1-22 --make-bed --out ${LABEL}.${POPULATION}.clean.chr1-22
	plink --bfile ${LABEL}.${POPULATION}.clean.chr1-22 --recode vcf-iid bgz --out ${LABEL}.${POPULATION}.clean.chr1-22
	cd ..

elif [ "$QC_STEP" == "clean_impute" ]; then # 
	cd hwe
	if [ ! -f ${LABEL}.EUR_AFR.hwe.failed.variants ]; then
		cat ${LABEL}.EUR.hwe.failed.variants ${LABEL}.AFR.hwe.failed.variants | sort -u > ${LABEL}.EUR_AFR.hwe.failed.variants
	fi
    
	# generate impute ready Plink files
	cd ../data_impute
	plink --bfile ../data/${LABEL}.base --geno 0.02 --exclude ../hwe/${LABEL}.EUR_AFR.hwe.failed.variants --make-bed --out ${LABEL}.impute
	cd ..
    
elif [ "$QC_STEP" == "phase" ]; then # 
	if [ "$CHROMOSOME" == "NA" ]; then
		echo "population (-p) must be set for QC step 4"
		exit
	fi
	reuse Java-1.8
	reuse Tabix
	cd data_impute
    
	if [ ! -f ${LABEL}.impute.chr${CHROMOSOME}.vcf.gz ]; then
		#plink --bfile ${LABEL}.impute --chr $CHROMOSOME --make-bed --out ${LABEL}.impute.chr${CHROMOSOME}
		plink --bfile ${LABEL}.impute --chr $CHROMOSOME --recode vcf-iid bgz --out ${LABEL}.impute.chr${CHROMOSOME}
		tabix -p vcf ${LABEL}.impute.chr${CHROMOSOME}.vcf.gz
	fi
	
	#$EAGLE \
	#--vcf ${LABEL}.impute.chr${CHROMOSOME}.vcf.gz \
	#--geneticMapFile $EAGLE_GENETIC_MAP \
	#--outPrefix ${LABEL}.impute.phased.chr${CHROMOSOME} \
	#--numThreads 32
	#tabix -p vcf ${LABEL}.impute.phased.chr${CHROMOSOME}.vcf.gz
    
	$GENOTYPE_HARMONIZER \
	--input ${LABEL}.impute.chr${CHROMOSOME} \
	--inputType VCF \
	--output ${LABEL}.impute.harmonized.chr${CHROMOSOME} \
	--outputType PLINK_BED \
	--ref $KG_VCF_BASE \
	--refType VCF \
	--keep \
	--update-id \
	--variants 1000 \
	--mafAlign 0.1 \
	--debug
    
    plink --bfile ${LABEL}.impute.harmonized.chr${CHROMOSOME} --recode vcf-iid bgz --keep-allele-order --out ${LABEL}.impute.harmonized.chr${CHROMOSOME}
	tabix -p vcf ${LABEL}.impute.harmonized.chr${CHROMOSOME}.vcf.gz
	cd ..

elif [ "$QC_STEP" == "prepare_vcfs" ]; then
	reuse Tabix

	cd data_clean
	# modify FILTER column to make compatible with EPACTS
	if [ -f ${LABEL}.${POPULATION}.clean.vcf.gz ]; then
		(zcat ${LABEL}.${POPULATION}.clean.vcf.gz | grep "^#"; zcat ${LABEL}.${POPULATION}.clean.vcf.gz | grep -v "^#" | awk '{OFS="\t"; $7 = "PASS"; print $0}') | bgzip -c > ${LABEL}.${POPULATION}.clean.epacts.vcf.gz
		tabix -p vcf ${LABEL}.${POPULATION}.clean.epacts.vcf.gz
	fi
	if [ -f ${LABEL}.${POPULATION}.clean.chr1-22.vcf.gz ]; then
		(zcat ${LABEL}.${POPULATION}.clean.chr1-22.vcf.gz | grep "^#"; zcat ${LABEL}.${POPULATION}.clean.chr1-22.vcf.gz | grep -v "^#" | awk '{OFS="\t"; $7 = "PASS"; print $0}') | bgzip -c > ${LABEL}.${POPULATION}.clean.chr1-22.epacts.vcf.gz
		tabix -p vcf ${LABEL}.${POPULATION}.clean.chr1-22.epacts.vcf.gz
	fi

elif [ "$QC_STEP" == "final_pca" ]; then
	if [ "$POPULATION" == "NA" ]; then
		echo "population (-p) must be set for QC step 4"
		exit
	fi
	reuse R-3.1

	# calculate PCs on clean datasets for inclusion as covariates in analysis
	cd data_clean_pca
	plink --bfile ../data_clean/${LABEL}.${POPULATION}.clean.chr1-22 --indep-pairwise 1500 150 0.2 --out ${LABEL}.${POPULATION}.clean.chr1-22.eig.pre
	plink --bfile ../data_clean/${LABEL}.${POPULATION}.clean.chr1-22 --extract ${LABEL}.${POPULATION}.clean.chr1-22.eig.pre.prune.in --exclude $REGIONS_EXCLUDE --maf 0.01 --make-bed --out ${LABEL}.${POPULATION}.clean.chr1-22.eig
	mv ${LABEL}.${POPULATION}.clean.chr1-22.eig.fam ${LABEL}.${POPULATION}.clean.chr1-22.eig.fam.orig
	R --vanilla --args ${LABEL}.${POPULATION}.clean.chr1-22.eig.fam.orig $KG_ETHNICITY ${LABEL}.${POPULATION}.clean.chr1-22.eig.fam $LABEL < $ADD_ETHNICITY_R
	plink --bfile ${LABEL}.${POPULATION}.clean.chr1-22.eig --recode --out ${LABEL}.${POPULATION}.clean.chr1-22.eig
	echo "genotypename:       ${LABEL}.${POPULATION}.clean.chr1-22.eig.ped" > ${LABEL}.${POPULATION}.clean.chr1-22.eig.pca.par
	echo "snpname:            ${LABEL}.${POPULATION}.clean.chr1-22.eig.map" >> ${LABEL}.${POPULATION}.clean.chr1-22.eig.pca.par
	echo "indivname:          ${LABEL}.${POPULATION}.clean.chr1-22.eig.fam" >> ${LABEL}.${POPULATION}.clean.chr1-22.eig.pca.par
	echo "evecoutname:        ${LABEL}.${POPULATION}.clean.chr1-22.eig.pca.evec" >> ${LABEL}.${POPULATION}.clean.chr1-22.eig.pca.par
	echo "evaloutname:        ${LABEL}.${POPULATION}.clean.chr1-22.eig.pca.eval" >> ${LABEL}.${POPULATION}.clean.chr1-22.eig.pca.par
	echo "altnormstyle:       NO" >> ${LABEL}.${POPULATION}.clean.chr1-22.eig.pca.par
	echo "numoutevec:         10" >> ${LABEL}.${POPULATION}.clean.chr1-22.eig.pca.par
	echo "numoutlieriter:     0" >> ${LABEL}.${POPULATION}.clean.chr1-22.eig.pca.par
	echo "nsnpldregress:      0" >> ${LABEL}.${POPULATION}.clean.chr1-22.eig.pca.par
	echo "noxdata:            YES" >> ${LABEL}.${POPULATION}.clean.chr1-22.eig.pca.par
	echo "numoutlierevec:     10" >> ${LABEL}.${POPULATION}.clean.chr1-22.eig.pca.par
	echo "outliersigmathresh: 6" >> ${LABEL}.${POPULATION}.clean.chr1-22.eig.pca.par
	echo "outlieroutname:     ${LABEL}.${POPULATION}.clean.chr1-22.eig.pca.outliers" >> ${LABEL}.${POPULATION}.clean.eig.pca.par
	echo "snpweightoutname:   ${LABEL}.${POPULATION}.clean.chr1-22.eig.pca.snpwts" >> ${LABEL}.${POPULATION}.clean.eig.pca.par
	$SMARTPCA -p ${LABEL}.${POPULATION}.clean.chr1-22.eig.pca.par > ${LABEL}.${POPULATION}.clean.chr1-22.eig.pca.log
	R --vanilla --args ${LABEL}.${POPULATION}.clean.chr1-22.eig.pca.evec < $PLOT_FINAL_PCS_R
	cd ..

elif [ "$QC_STEP" == "epacts_kinship" ]; then # run this interactively in gsa5
	reuse GCC-5.2
	reuse Tabix
	cd epacts_kinship_matrix

	plink --bfile ../data_clean_pca/${LABEL}.${POPULATION}.clean.chr1-22.eig --recode vcf-iid bgz --out ${LABEL}.${POPULATION}.clean.chr1-22.eig
	tabix -p vcf ${LABEL}.${POPULATION}.clean.chr1-22.eig.vcf.gz

	$EPACTS make-kin --vcf ${LABEL}.${POPULATION}.clean.chr1-22.eig.vcf.gz --out ${LABEL}.${POPULATION}.clean.chr1-22.epacts.kinf --run 16
	# to view the kinship matrix, use
	# /humgen/diabetes/users/ryank/software/EPACTS-3.2.6/bin/pEmmax kin-util --kinf ${LABEL}.biallelic.chr1-22.ALL.clean.epacts.kinf --outf test --dump
	cd ..

else
	echo "not a valid QC step"
fi
