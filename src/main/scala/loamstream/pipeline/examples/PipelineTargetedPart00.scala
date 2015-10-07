
package loamstream.pipeline.examples

/**
 * LoamStream
 * Created by oliverr on 10/6/2015.
 */
object PipelineTargetedPart00 {
  val string =
 """!title Targeted Sequencing

#To include this, must define:
# all keys required by common.cfg

lap_home=/home/unix/flannick/lap

# lap_home=/home/flannick/broad/lap

lap_trunk=$lap_home/trunk
lap_projects=$lap_home/projects
targeted_base_dir=/humgen/gsa-hpprojects/pfizer/projects/targeted/scratch/flannick
common_bin_dir=$lap_projects/common
targeted_bin_dir=$targeted_base_dir/bin

r_212_cmd=/broad/software/free/Linux/redhat_5_x86_64/pkgs/r_2.12.0/bin/R
r_cmd=R
r_215_cmd=/broad/software/free/Linux/redhat_5_x86_64/pkgs/r_2.15.1/bin/R

!include $lap_trunk/config/common.cfg
pdflatex_cmd=/broad/software/free/Linux/redhat_5_x86_64/pkgs/texlive-20110510/bin/x86_64-linux/pdflatex

#Pipeline specific stuff

#types
class project=Project with marker,seq_batch,sample_qc_filter,var_qc_filter,project_sample_subset,project_variant_subset,project_merge_subset
minor class project_sample_subset=Sample Subset parent project with marker_variant_subset
minor class project_variant_subset=Variant Subset parent project with marker_variant_subset
minor class project_merge_subset=Merge Subset parent project
minor class sample_qc_filter=Sample QC Filter parent project
minor class var_qc_filter=Variant QC Filter parent project
minor class marker=Marker parent project with marker_sample_subset,marker_variant_subset
minor class marker_sample_subset=Marker Sample Subset parent project_sample_subset consistent marker
minor class marker_variant_subset=Marker Variant Subset parent project_variant_subset consistent marker 
minor class seq_batch=Seq Batch parent project
class pheno=Phenotype parent project with pheno_qc_filter,pheno_variant_subset,pheno_sample_subset,burden,pheno_test
minor class pheno_value=Phenotype Value parent pheno
minor class pheno_variant_subset=Pheno Subset parent pheno consistent project_variant_subset 
minor class pheno_sample_subset=Pheno Sample Subset parent pheno consistent project_sample_subset 
minor class pheno_variant_qc_strata=Pheno QC Strata parent pheno with pheno_variant_qc_strata_variant_subset,pheno_variant_qc_pheno_strata
minor class pheno_variant_qc_strata_variant_subset=Pheno QC Strata Subset parent pheno_variant_qc_strata consistent project_variant_subset
minor class pheno_variant_qc_pheno_strata=Pheno QC Pheno Strata parent pheno with pheno_variant_qc_pheno_strata_variant_subset,pheno_variant_qc_strata
minor class pheno_variant_qc_pheno_strata_variant_subset=Pheno QC Pheno Strata Subset parent pheno_variant_qc_pheno_strata consistent project_variant_subset
minor class pheno_qc_filter=Pheno QC Filter parent pheno
class pheno_test=SV Test parent pheno with pheno_test_variant_subset
minor class pheno_test_variant_subset=SV Test Subset parent pheno_test consistent pheno_variant_subset,project_variant_subset 
class annot=Annot parent project with annot_var_qc_filter,annot_variant_subset
minor class annot_var_qc_filter=Annot Variant QC Filter parent annot
minor class annot_variant_subset=Annot Subset parent annot consistent project_variant_subset 
class burden=Burden parent pheno with burden_test consistent annot
minor class burden_variant_subset=Burden Subset parent burden consistent pheno_variant_subset,project_variant_subset,annot_variant_subset 
class burden_test=Burden Test parent burden with burden_variant_subset,burden_test_variant_subset
minor class burden_test_variant_subset=Burden Test Subset parent burden_test consistent burden_variant_subset,pheno_variant_subset,project_variant_subset,annot_variant_subset
minor class region=Region parent project
minor class locus=Locus parent pheno consistent region
class gene=Gene parent locus with gene_burden,variant consistent pheno_variant_subset,project_variant_subset
minor class gene_burden=Gene Burden parent gene consistent burden,annot with transcript_burden
minor class transcript_burden=Transcript Burden parent gene_burden
minor class variant=Variant parent gene
minor class call_set=Call Set parent project
minor class call_set_subset=Call Set Subset parent call_set with call_set_sample_subset
minor class call_set_sample_subset=Call Set Sample Subset parent call_set_subset
#minor class project_subset=Project Subset parent project
class sample=Sample parent project

#directories
sortable mkdir path projects_dir=$unix_out_dir/projects
sortable mkdir path project_dir=$projects_dir/@project class_level project
sortable mkdir path project_sample_subsets_dir=$project_dir/project_sample_subsets class_level project
sortable mkdir path project_sample_subset_dir=$project_sample_subsets_dir/@project_sample_subset class_level project_sample_subset
sortable mkdir path project_variant_subsets_dir=$project_dir/project_variant_subsets class_level project
sortable mkdir path project_variant_subset_dir=$project_variant_subsets_dir/@project_variant_subset class_level project_variant_subset
sortable mkdir path project_merge_subsets_dir=$project_dir/project_merge_subsets class_level project
sortable mkdir path project_merge_subset_dir=$project_merge_subsets_dir/@project_merge_subset class_level project_merge_subset
sortable mkdir path pheno_variant_qc_stratas_dir=$project_dir/pheno_variant_qc_stratas class_level project
sortable mkdir path pheno_variant_qc_strata_variant_subsets_dir=$project_dir/pheno_variant_qc_strata_variant_subsets class_level project
sortable mkdir path pheno_variant_qc_pheno_stratas_dir=$project_dir/pheno_variant_qc_pheno_stratas class_level project
sortable mkdir path pheno_variant_qc_pheno_strata_variant_subsets_dir=$project_dir/pheno_variant_qc_pheno_strata_variant_subsets class_level project
sortable mkdir path markers_dir=$project_dir/markers class_level project
sortable mkdir path marker_dir=$markers_dir/@marker class_level marker
sortable mkdir path marker_sample_subsets_dir=$project_sample_subset_dir/marker_sample_subsets class_level project_sample_subset
sortable mkdir path marker_sample_subset_dir=$marker_sample_subsets_dir/@marker_sample_subset class_level marker_sample_subset
sortable mkdir path marker_variant_subsets_dir=$project_variant_subset_dir/marker_variant_subsets class_level project_variant_subset
sortable mkdir path marker_variant_subset_dir=$marker_variant_subsets_dir/@marker_variant_subset class_level marker_variant_subset
sortable mkdir path call_sets_dir=$project_dir/call_sets class_level project
sortable mkdir path call_set_dir=$call_sets_dir/@call_set class_level call_set
sortable mkdir path call_set_subsets_dir=$call_set_dir/call_set_subsets_dir class_level call_set
sortable mkdir path call_set_sample_subsets_dir=$call_set_dir/call_set_sample_subsets_dir class_level call_set
#sortable mkdir path project_subsets_dir=$project_dir/project_subsets_dir class_level project
sortable mkdir path seq_batches_dir=$project_dir/seq_batches class_level project
sortable mkdir path seq_batch_dir=$seq_batches_dir/@seq_batch class_level seq_batch
sortable mkdir path phenos_dir=$project_dir/phenos class_level project
sortable mkdir path pheno_dir=$phenos_dir/@pheno class_level pheno
sortable mkdir path pheno_variant_subsets_dir=$pheno_dir/pheno_variant_subsets class_level pheno
sortable mkdir path pheno_variant_subset_dir=$pheno_variant_subsets_dir/@pheno_variant_subset class_level pheno_variant_subset
sortable mkdir path pheno_sample_subsets_dir=$pheno_dir/pheno_sample_subsets class_level pheno
sortable mkdir path pheno_sample_subset_dir=$pheno_sample_subsets_dir/@pheno_sample_subset class_level pheno_sample_subset
sortable mkdir path pheno_tests_dir=$pheno_dir/pheno_tests class_level pheno
sortable mkdir path pheno_test_dir=$pheno_tests_dir/@pheno_test class_level pheno_test
sortable mkdir path pheno_test_variant_subsets_dir=$pheno_test_dir/pheno_test_variant_subsets class_level pheno_test
sortable mkdir path pheno_test_variant_subset_dir=$pheno_test_variant_subsets_dir/@pheno_test_variant_subset class_level pheno_test_variant_subset
sortable mkdir path annots_dir=$project_dir/annots class_level project
sortable mkdir path annot_dir=$annots_dir/@annot class_level annot
sortable mkdir path annot_variant_subsets_dir=$annot_dir/annot_variant_subsets class_level annot
sortable mkdir path annot_variant_subset_dir=$annot_variant_subsets_dir/@annot_variant_subset class_level annot_variant_subset
sortable mkdir path burdens_dir=$pheno_dir/burdens class_level pheno
sortable mkdir path burden_dir=$burdens_dir/@burden class_level burden
sortable mkdir path burden_variant_subsets_dir=$burden_dir/burden_variant_subsets class_level burden
sortable mkdir path burden_variant_subset_dir=$burden_variant_subsets_dir/@burden_variant_subset class_level burden_variant_subset
sortable mkdir path burden_tests_dir=$burden_dir/burden_tests class_level burden
sortable mkdir path burden_test_dir=$burden_tests_dir/@burden_test class_level burden_test
sortable mkdir path burden_test_variant_subsets_dir=$burden_test_dir/burden_test_variant_subsets class_level burden_test
sortable mkdir path burden_test_variant_subset_dir=$burden_test_variant_subsets_dir/@burden_test_variant_subset class_level burden_test_variant_subset
sortable mkdir path regions_dir=$project_dir/regions class_level project
sortable mkdir path region_dir=$regions_dir/@region class_level region
sortable mkdir path loci_dir=$pheno_dir/loci class_level pheno
sortable mkdir path locus_dir=$loci_dir/@locus class_level locus
sortable mkdir path genes_dir=$pheno_dir/genes class_level pheno
sortable mkdir path gene_dir=$genes_dir/@gene class_level gene
sortable mkdir path gene_burdens_dir=$gene_dir/gene_burdens class_level gene
sortable mkdir path gene_burden_dir=$gene_burdens_dir/@gene_burden class_level gene_burden
sortable mkdir path transcript_burdens_dir=$gene_burden_dir/transcript_burdens class_level gene_burden
sortable mkdir path transcript_burden_dir=$transcript_burdens_dir/@transcript_burden class_level transcript_burden
sortable mkdir path variants_dir=$gene_dir/variants class_level gene
sortable mkdir path variant_dir=$variants_dir/@variant class_level variant
sortable mkdir path samples_dir=$project_dir/samples class_level project
sortable mkdir path sample_dir=$samples_dir/@sample class_level sample

#command classes/memory
cmd_class epstopdf_cmd_class=epstopdf env_mod TEMP:$tmp_dir
cmd_class python_cmd_class=python env_mod PYTHONPATH:$lib_dir/python
#cmd_class javac_cmd_class=javac umask_mod 002 
vstats_plot_mem=2000
pseq_gstats_mem=4000
pseq_pathway_mem=4000
pseq_gene_mem=4000
pseq_mem=2000
epacts_vassoc_mem=2000
epacts_eigen_mem=2000
recode_call_set_vcf_mem=5000
clean_vcf_mem=2000
epacts_gassoc_mem=2000
#cmd_class pseq_cmd_class=pseq env_mod LD_LIBRARY_PATH:$plinkseq_share_lib_dir

epacts_share_dir=$lib_dir/epacts/EPACTS-3.2.4/lib
cmd_class epacts_cmd_class=epacts env_mod R_LIBS_USER:$epacts_share_dir
cmd_class gstats_cmd_class=g-stats rusage_mod $pseq_gstats_mem
samtools_mem=4000
cmd_class samtools_cmd_class=samtools rusage_mod $samtools_mem
impute2_mem=2000
cmd_class impute2_cmd_class=$impute2_cmd rusage_mod $impute2_mem
cmd_class vep_cmd_class=$vep_cmd env_mod PATH:$tabix_dir,PERL5LIB:$vep_plugin_dir

mds_mem=2000
cmd_class mds_plot_cmd_class=mds-plot rusage_mod $mds_mem
cmd_class smart_pca_cmd_class=$smart_pca_cmd env_mod PATH:$eigensoft_dir
cmd_class multiallelic_qc_cmd_class=multiallelic_QC_metrics.R env_mod R_LIBS_USER:$lib_dir/pseq/pseq-0.08.2

hidden_output_class vcf_class=.vcf.gz update_ext tbi

merge_vcf_mem=3000
initial_vcf_mem=3000
initial_merge_vcf_mem=2000
missed_sites_mem=2000
recessive_full_hwe_mem=4000
project_plink_mem=2000
score_test_mem=2000
project_plinkseq_bfile_mem=2000
project_sample_subset_plinkseq_bfile_mem=2000
all_marker_mem=2000
genome_pdf_mem=2000
load_sample_db_mem=2000
pheno_genome_mem=2000
kin_mem=2000
vep_mem=2000
trait_vassoc_annot_mem=2000

smart_pca_mem=2000

snpeff_heap=4g
gatk_heap=3g

#IMPORTANT PROPERTIES/KEYS

#END IMPORTANT PROPERTIES/KEYS

#external files/directories

#pseq stuff
pseq_exons_loc_group=genes-unmerged
pseq_genes_loc_group=genes

plinkseq_local_home=$lib_dir/pseq
plinkseq_r_home=$plinkseq_local_home/R
plinkseq_rplinkseq_lib=$plinkseq_local_home/R/Rplinkseq/lib
plinkseq_global_home=/psych/genetics/pseq
#plinkseq_resources_dir=$plinkseq_global_home/resources
#plinkseq_share_lib_dir=$plinkseq_global_home/share/lib
#plinkseq_share_lib_dir=$plinkseq_local_home/share/lib
#pseq_cmd=$plinkseq_global_home/client/pseq
#pseq_cmd=$plinkseq_local_home/client/pseq
pseq_dir=$lib_dir/pseq/pseq-0.08.2
pseq_cmd=$pseq_dir/client/pseq

plinkseq_okay_err='conflicting type'

epacts_cmd=$lib_dir/epacts/EPACTS-3.2.4/bin/epacts
#epacts_old_cmd=$lib_dir/epacts/epacts2.01/epacts

mantra_cmd=$lib_dir/mantra/MANTRA_software/mantra.v2.1
dmatcal_cmd=$lib_dir/mantra/MANTRA_software/dmatcal

metasoft_cmd=java -jar $lib_dir/metasoft/Metasoft.jar
metasoft_pvalue_table=$lib_dir/metasoft/HanEskinPvalueTable.txt

metal_cmd=$lib_dir/metal/generic-metal/metal

#heng stuff
samtools_dir=$lib_dir/samtools/samtools-0.1.12a
bcftools_cmd=$samtools_dir/bcftools/bcftools
samtools_cmd=$samtools_dir/samtools
tabix_dir=/broad/software/free/Linux/redhat_5_x86_64/pkgs/tabix/tabix_0.2.2/bin
tabix_cmd=$tabix_dir/tabix
bgzip_cmd=$tabix_dir/bgzip

fetch_ucsc_cmd=sleep 15 && python $lib_dir/fetch_ucsc/fetch_ucsc.py

#CMD: perl -e 'print "chr1:109698512-109698676\nchr1:109686159-109686303"' | xargs -i ../lib/samtools/samtools-0.1.12a/samtools mpileup -r {} -C50 -DSug -f /seq/references/Homo_sapiens_assembly18/v0/Homo_sapiens_assembly18.fasta -b test.bam.list > a.bcf

score_seq_cmd=$lib_dir/score_seq/SCORE-Seq

#to use a different hg build, define :
hg_build=19
ensembl_version=74

plink_chrX=23
plink_chrY=24
plink_chrXY=25
plink_chrM=26

plink_male=1
plink_female=2

chrX=X
chrY=Y
chrXY=XY
chrM=MT

chrX_par=60001-2699520 154931044-155260560

snpeff_db_dir=$snpeff_dir/db
snpeff_genome=GRCh37.74
snpeff_dbsnp=$snpeff_db_dir/dbSnp.vcf
#snpeff_gwas_cat=$snpeff_db_dir/gwascatalog.txt
snpeff_dbnsfp=$snpeff_db_dir/dbNSFP2.4.txt.gz
dbnsfp_cols_start=9
dbnsfp_cols_ignore=SLR

ensembl_reference=$lib_dir/ensembl/reference/hg19/Homo_sapiens_assembly19.fasta 

hg_refflat=hg${hg_build}_refFlat
#dbsnp_vcf_file=/humgen/gsa-hpprojects/GATK/bundle/current/hg$hg_build/dbsnp_132.hg$hg_build.vcf
dbsnp_vcf_file=/humgen/gsa-hpprojects/GATK/bundle/current/b37/dbsnp_137.b37.vcf
reference_file=/seq/references/Homo_sapiens_assembly19/v1/Homo_sapiens_assembly19.fasta 
plinkseq_resources_dir=$plinkseq_local_home/resources/hg$hg_build

#plinkseq_refdb=$plinkseq_resources_dir/refdb
#plinkseq_seqdb=$plinkseq_resources_dir/seqdb
#plinkseq_ref_locdb_file=$plinkseq_resources_dir/locdb
#plinkseq_pph2db_file=$plinkseq_resources_dir/pph2.db
#Near as I can tell, pph2 does not depend on hg build
#when Shaun updates hg19 with link to pph2.db, uncomment above
pph2_whess_file=$lib_dir/pph2/whess/hg$hg_build/pph2.all.transcripts.bed.gz
genetic_map_file=/fg/software/Tagger/assocplot/genetic_map_chr@1.txt
#For the strand file the coordinates dont matter --- just the strands
genetic_strand_file=/fg/software/Tagger/assocplot/known_genes_build35_050307_chr@{1}.txt
#end reference defines
conservation_dir=$lib_dir/conservation/hg$hg_build

lib_dir=$targeted_base_dir/lib

gatk_home=$lib_dir/gatk
gatk_dir=$gatk_home/dist
gatk_jar=$gatk_dir/GenomeAnalysisTK.jar
gatk_dt=BY_SAMPLE
gatk_dcov=600
gatk_cmd_no_info_no_heap=java -Xmx@2 -jar $gatk_jar -R $reference_file -dt $gatk_dt -dcov $gatk_dcov -U LENIENT_VCF_PROCESSING -T @1
gatk_cmd_no_info=$gatk_cmd_no_info_no_heap(@1,$gatk_heap)
gatk_cmd_no_interval_no_heap=$gatk_cmd_no_info_no_heap(@1,@2)
gatk_cmd_no_interval=$gatk_cmd_no_info(@1)
gatk_cmd_no_heap=$gatk_cmd_no_interval_no_heap(@1,@2) !{input,-L,project_expanded_interval_list_file,if_prop=expand_targets,allow_empty=1} !{input,-L,project_interval_list_file,unless_prop=expand_targets,allow_empty=1}
gatk_cmd=$gatk_cmd_no_interval(@1) !{input,-L,project_expanded_interval_list_file,if_prop=expand_targets,allow_empty=1} !{input,-L,project_interval_list_file,unless_prop=expand_targets,allow_empty=1} 
gatk_cmd_dbsnp=$gatk_cmd(@1) --dbsnp $dbsnp_vcf_file 
gatk_cmd_dbsnp_no_heap=$gatk_cmd_no_heap(@1,@2) --dbsnp $dbsnp_vcf_file

queue_dir=$gatk_dir
queue_jar=$queue_dir/Queue.jar
sg_combine_scala_script=$targeted_bin_dir/SGCombine.scala

germline_dir=$lib_dir/germline
"""
}
    