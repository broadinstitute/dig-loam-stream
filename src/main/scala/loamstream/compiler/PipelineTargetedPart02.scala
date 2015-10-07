
package loamstream.compiler

/**
 * LoamStream
 * Created by oliverr on 10/6/2015.
 */
object PipelineTargetedPart02 {
  val string =
 """##====================

#PSEQ specific
prop make_two_sided=scalar #for pseq, set to TRUE to run case and control and then combine
prop analytic=scalar default 0 #for pseq, do not use permutations for p-values
#max frac missing genotypes to include in burden tests
#If value is too high, and if fix_null is true, may lose power
#i.e. .2
prop max_assoc_null=scalar
prop min_test_p_missing=scalar #max p_missing to include in burden tests
prop min_test_p_hwe=scalar #max p_hwe to include in burden tests
prop phenos_for_p_hwe=list #compute p_hwe across all of these and exclude if missing in any
prop custom_burden_test_exclude_filters=list #custom filters from vstats file to exclude variants


##SELECTION OF INTERESTING VARIANTS & GENES
#==========================================
prop max_interesting_variant_p_value=scalar default 0 #any single variant with p-value less than this is interesting, regardless of gene
prop min_interesting_variant_p_missing_value=scalar default .05 #must have p-missing greater than this  to be interesting
prop max_interesting_variant_maf=scalar default .05 #must have maf less than this to be interesting
prop min_pheno_interesting_mac=scalar default 3 #minimum minor allele count for any variant (or combination of variants) to consider a gene interesting
prop min_pheno_gene_interesting_mac=scalar default 2 #minimum minor allele count for any variant (or combination of variants) within an intereting gene to be considered interesting
prop interesting_variant_gene_combinations=list #mark genes as interesting if there are multiple variants:; each entry is comma delimited and have p-value,number that must be lower


prop max_interesting_gene_p_value=scalar default 0 #require this p-value from a test for a gene to be interesting
prop max_interesting_gene_variant_p_value=scalar default 0 #within an interesting gene, variants with p-values below this are interesting
prop min_interesting_gene_variant_p_missing_value=scalar default 0 #within an already interesting gene, must have p-missing greater than this  to be interesting
prop all_variants_interesting=scalar #mark all variants as interesting in genes identified through this burden

prop extra_interesting_genes=list #manual interesting genes
prop extra_interesting_variants=list #manual interesting variants


#Set all regions to be interesting
prop all_regions_interesting=scalar
prop all_loci_interesting=scalar



#ADDITIONAL ANNOTATIONS FOR PLINKSEQ, VCF
##====================

1kg_label=g1k
dbsnp_label=dbSNP130
db_mask=ref.req=$1kg_label,$dbsnp_label
nondb_mask=ref.ex=$1kg_label,$dbsnp_label

vcf_mq0_annot=MQ0
vcf_het_ad_annot=HET_AD
vcf_het_ab_annot=HET_AB




#external databases

impute2_cmd=$lib_dir/impute/impute2/impute2
snptest_cmd=$lib_dir/impute/snptest/snptest

eigensoft_dir=$lib_dir/eigensoft/bin
smart_pca_cmd=perl $eigensoft_dir/smartpca.perl

gcta_cmd=$lib_dir/gcta/gcta

#--------
#display categories
#--------

cat cat_project_slide_data=null disp "For Slides" class_level project

cat cat_project_slide_master_data=null disp "Master" parent cat_project_slide_data
cat cat_project_slide_coverage_data=null disp "Coverage" parent cat_project_slide_data
cat cat_project_slide_sites_data=null disp "Sites" parent cat_project_slide_data
cat cat_project_slide_qc_data=null disp "QC Data" parent cat_project_slide_data
cat cat_project_slide_qc_ind_data=null disp "Samples" parent cat_project_slide_qc_data
#cat cat_project_slide_qc_ind_box_data=null disp "Box Plots" parent cat_project_slide_qc_ind_data
cat cat_project_slide_qc_ind_hist_data=null disp "Hist Plots" parent cat_project_slide_qc_ind_data
cat cat_project_slide_qc_ind_minus_data=null disp "Qc -" parent cat_project_slide_qc_ind_data
cat cat_project_slide_qc_var_data=null disp "Variants" parent cat_project_slide_qc_data

cat cat_project_meta_data=null disp "Meta Data" class_level project
!!expand:,:project,parentt:project,meta_data:project_variant_subset,variant_data! \
cat cat_project_target_data=null disp "Target Data" parent cat_project_parentt
cat cat_project_picard_data=null disp "Picard Data" parent cat_project_meta_data
cat cat_project_extra_sample_data=null disp "Extra Sample Data" parent cat_project_meta_data
cat cat_project_failure_data=null disp "Failures" parent cat_project_meta_data
cat cat_project_region_data=null disp "Regions" parent cat_project_meta_data
cat cat_project_subset_meta_data=null disp "Subset meta data" parent cat_project_meta_data

!!expand:project:project:project_merge_subset! \
cat cat_project_variant_data=null disp "Variants" class_level project

!!expand:type:variant:sample! \
cat cat_project_type_subset_all_variant_data=null disp "All variant data" class_level project_type_subset

!!expand:type:variant:sample! \
cat cat_project_type_subset_variant_data=null disp "Variants" parent cat_project_type_subset_all_variant_data

!|expand:project:project:project_merge_subset:project_variant_subset| \
cat cat_project_all_call_data=null disp "All" parent cat_project_variant_data
!!expand:project:project:project_sample_subset:project_variant_subset:project_merge_subset! \
cat cat_project_snp_call_data=null disp "SNPs" parent cat_project_variant_data
!!expand:project:project:project_sample_subset:project_variant_subset:project_merge_subset! \
cat cat_project_indel_call_data=null disp "Indels" parent cat_project_variant_data
!!expand:project:project:project_sample_subset:project_variant_subset:project_merge_subset! \
cat cat_project_multiallelic_call_data=null disp "Multiallelics" parent cat_project_variant_data
!!expand:project:project:project_variant_subset! \
cat cat_project_annot_data=null disp "Annotations" parent cat_project_variant_data
!!expand:project:project:project_variant_subset! \
cat cat_project_mask_data=null disp "Masks" parent cat_project_variant_data

!|expand:project:project:project_merge_subset:project_variant_subset| \
cat cat_project_all_variant_call_data=null disp "Variants" parent cat_project_all_call_data
!!expand:project:project:project_sample_subset:project_variant_subset:project_merge_subset! \
cat cat_project_snp_variant_call_data=null disp "Variants" parent cat_project_snp_call_data
!!expand:project:project:project_sample_subset:project_variant_subset:project_merge_subset! \
cat cat_project_indel_variant_call_data=null disp "Variants" parent cat_project_indel_call_data
!!expand:project:project:project_sample_subset:project_variant_subset:project_merge_subset! \
cat cat_project_multiallelic_variant_call_data=null disp "Variants" parent cat_project_multiallelic_call_data

#cat cat_project_samtools_call_data=null disp "Samtools" parent cat_project_call_data
cat cat_project_call_all_bulk_properties_data=null disp "Bulk Properties" parent cat_project_snp_call_data
cat cat_project_call_titv_bulk_properties_data=null disp "TiTv" parent cat_project_call_all_bulk_properties_data
cat cat_project_call_theta_bulk_properties_data=null disp "Theta" parent cat_project_call_all_bulk_properties_data

cat cat_project_plinkseq_data=null disp "Plink/Seq" class_level project
!!expand:type:variant:sample! \
cat cat_project_type_subset_plinkseq_data=null disp "Plink/Seq" parent cat_project_type_subset_all_variant_data

!!expand:projectt:project:project_sample_subset:project_variant_subset! \
cat cat_projectt_plinkseq_project_data=null disp "Project Information" parent cat_projectt_plinkseq_data
!!expand:projectt:project:project_sample_subset:project_variant_subset! \
cat cat_projectt_plinkseq_clean_project_data=null disp "Clean Project Information" parent cat_projectt_plinkseq_data
#!!expand:projectt:project:project_sample_subset:project_variant_subset! \
#cat cat_projectt_plinkseq_indel_project_data=null disp "Indel Project Information" parent cat_projectt_plinkseq_data
#cat cat_project_plinkseq_samtools_clean_project_data=null disp "Samtools Clean Project Information" parent cat_project_plinkseq_data


!!expand:type:variant:sample! \
cat cat_project_type_subset_qc_data=null disp "QC" class_level project_type_subset

cat cat_project_qc_ind_data=null disp "Individual QC" class_level project

!!expand:type:variant:sample! \
cat cat_project_type_subset_qc_ind_data=null disp "Individual QC" parent cat_project_type_subset_qc_data


cat cat_project_qc_ind_data=null disp "Individual QC" class_level project

!!expand:project:project:project_sample_subset! \
!!expand:,:type,tname:raw,Raw:qc_pass,QC Pass:qc_plus,QC +! \
cat cat_project_type_istats_data=null disp "tname i-stats" parent cat_project_qc_ind_data

!!expand:project:project:project_variant_subset:project_sample_subset! \
cat cat_project_plink_output_data=null disp "Plink Outputs" parent cat_project_qc_ind_data

!!expand:project:project:project_variant_subset! \
cat cat_project_plink_qc_pass_output_data=null disp "QC Pass" parent cat_project_plink_output_data
!!expand:project:project:project_variant_subset! \
cat cat_project_plink_qc_plus_output_data=null disp "QC Plus" parent cat_project_plink_output_data

!!expand:project:project:project_sample_subset! \
cat cat_project_qc_ind_stats_data=null disp "Stats" parent cat_project_qc_ind_data

cat cat_project_qc_ind_plots_data=null disp "Plots" parent cat_project_qc_ind_data
cat cat_project_sample_qc_filter_data=null disp "Exclude" parent cat_project_qc_ind_data

!!expand:project:project! \
cat cat_project_qc_loc_data=null disp "Loci QC" class_level project

cat cat_project_variant_subset_qc_loc_data=null disp "Loci QC" parent cat_project_variant_subset_qc_data

!!expand:project:project:project_variant_subset! \
cat cat_project_qc_var_data=null disp "Variants" parent cat_project_qc_loc_data

!!expand:project:project:project_variant_subset! \
!!expand:,:type,tname:raw,Raw:qc_pass,QC Pass:qc_plus,QC +! cat cat_project_type_var_stats_data=null disp "tname v-stats" parent cat_project_qc_ind_data
!!expand:project:project:project_variant_subset! \
cat cat_project_qc_var_stats_data=null disp "Stats" parent cat_project_qc_var_data
cat cat_project_qc_var_plots_data=null disp "Plots" parent cat_project_qc_var_data

cat cat_project_qc_var_exclude_data=null disp "Exclude" parent cat_project_qc_var_data

!!expand:project:project:project_variant_subset! \
cat cat_project_qc_gene_data=null disp "Genes" parent cat_project_qc_loc_data
!!expand:project:project:project_variant_subset! \
cat cat_project_qc_gene_stats_data=null disp "Stats" parent cat_project_qc_gene_data
cat cat_project_qc_gene_plots_data=null disp "Plots" parent cat_project_qc_gene_data
cat cat_project_qc_gene_gc_plots_data=null disp "GC" parent cat_project_qc_gene_plots_data

cat cat_project_coverage_data=null disp "Coverage" class_level project
cat cat_project_sample_coverage_data=null disp "Samples" parent cat_project_coverage_data
cat cat_project_ind_sample_coverage_data=null disp "Individual" parent cat_project_sample_coverage_data
cat cat_project_sample_cum_coverage_data=null disp "Cumulative" parent cat_project_sample_coverage_data

!!expand:,:type,Type:gene,Gene:exon,Exon:bait,Bait! \
cat cat_project_type_coverage_data=null disp "Types" parent cat_project_coverage_data

!!expand:type:gene:exon:bait! \
cat cat_project_type_dist_coverage_data=null disp "Sample distribution" parent cat_project_type_coverage_data

!!expand:type:gene:exon:bait! \
cat cat_project_alphabetical_type_dist_coverage_data=null disp "Alphabetical" parent cat_project_type_coverage_data

cat cat_project_lowest_gene_dist_coverage_data=null disp "Lowest" parent cat_project_gene_coverage_data

!!expand:type:gene:exon:bait! \
cat cat_project_type_cum_coverage_data=null disp "Cumulative \\#types covered" parent cat_project_type_coverage_data

!!expand:whichlevel:sample:pheno! \
cat cat_whichlevel_qc_filter_data=null disp "QC Filter" class_level whichlevel_qc_filter
!!expand:whichlevel:sample:pheno! \
cat cat_whichlevel_qc_filter_trait_data=null disp "Trait filter" parent cat_whichlevel_qc_filter_data
!!expand:whichlevel:sample:pheno! \
cat cat_whichlevel_qc_filter_metric_data=null disp "Metric filter" parent cat_whichlevel_qc_filter_data

cat cat_var_qc_filter_data=null disp "QC Filter" class_level var_qc_filter

!!expand:pheno_variant_qc_strata:pheno_variant_qc_strata:pheno_variant_qc_strata_variant_subset! \
cat cat_pheno_variant_qc_strata_data=null disp "QC Strata" class_level pheno_variant_qc_strata
!!expand:pheno_variant_qc_pheno_strata:pheno_variant_qc_pheno_strata:pheno_variant_qc_pheno_strata_variant_subset! \
cat cat_pheno_variant_qc_pheno_strata_data=null disp "QC Strata" class_level pheno_variant_qc_pheno_strata

cat cat_pheno_qc_filter_data=null disp "QC Filter" class_level pheno_qc_filter

!!expand:marker:marker:marker_sample_subset:marker_variant_subset! \
cat cat_marker_data=null disp "Marker Data" class_level marker
cat cat_marker_all_genotype_data=null disp "All Genotypes" parent cat_marker_data
cat cat_marker_initial_all_genotype_data=null disp "Initial" parent cat_marker_all_genotype_data
cat cat_marker_filtered_all_genotype_data=null disp "Filtered" parent cat_marker_all_genotype_data
cat cat_marker_filtered_for_merge_all_genotype_data=null disp "Filtered for merging" parent cat_marker_all_genotype_data
cat cat_marker_sample_genotype_data=null disp "Project Sample Genotypes" parent cat_marker_data
!!expand:marker:marker:marker_sample_subset:marker_variant_subset! \
cat cat_marker_concordance_data=null disp "Concordance" parent cat_marker_data
cat cat_marker_meta_data=null disp "Meta" parent cat_marker_data

cat cat_project_plink_data=null disp "Plink Data" class_level project

!!expand:type:variant:sample! \
cat cat_project_type_subset_plink_data=null disp "Plink Data" parent cat_project_type_subset_all_variant_data

!!expand:project:project:project_sample_subset:project_variant_subset! \
cat cat_project_plink_seq_data=null disp "Seq" parent cat_project_plink_data
cat cat_project_plink_seq_meta_data=null disp "Meta" parent cat_project_plink_seq_data
!!expand:project:project:project_sample_subset:project_variant_subset! \
cat cat_project_plink_seq_qc_pass_data=null disp "QC Pass" parent cat_project_plink_seq_data
!!expand:project:project:project_sample_subset:project_variant_subset! \
cat cat_project_plink_seq_qc_plus_data=null disp "QC Plus" parent cat_project_plink_seq_data

cat cat_project_plink_marker_data=null disp "Marker" parent cat_project_plink_data
cat cat_project_plink_all_marker_data=null disp "All Samples" parent cat_project_plink_marker_data
cat cat_project_plink_all_marker_meta_data=null disp "Meta" parent cat_project_plink_all_marker_data
cat cat_project_plink_all_marker_ped_data=null disp "Ped" parent cat_project_plink_all_marker_data
cat cat_project_plink_sample_marker_data=null disp "Project Samples" parent cat_project_plink_marker_data

cat cat_project_plink_sample_marker_meta_data=null disp "Meta" parent cat_project_plink_sample_marker_data
cat cat_project_plink_sample_marker_ped_data=null disp "Ped" parent cat_project_plink_sample_marker_data
cat cat_project_plink_sample_marker_pruned_ped_data=null disp "Pruned Ped" parent cat_project_plink_sample_marker_data

cat cat_project_plink_sample_non_seq_plink_data=null disp "Non seq" parent cat_project_plink_sample_marker_data
cat cat_project_plink_sample_non_seq_meta_data=null disp "Meta" parent cat_project_plink_sample_non_seq_plink_data
cat cat_project_plink_sample_non_seq_ped_data=null disp "Ped" parent cat_project_plink_sample_non_seq_plink_data

cat cat_project_plink_extra_marker_data=null disp "Extra Samples" parent cat_project_plink_marker_data
cat cat_project_plink_extra_marker_ped_data=null disp "Ped" parent cat_project_plink_extra_marker_data

cat cat_project_plink_combined_data=null disp "Combined" parent cat_project_plink_data
cat cat_project_plink_sample_combined_data=null disp "Project Samples" parent cat_project_plink_combined_data
cat cat_project_plink_sample_combined_ped_data=null disp "Ped" parent cat_project_plink_sample_combined_data

cat cat_project_ibd_data=null disp "IBD Data" class_level project
cat cat_project_sample_subset_ibd_data=null disp "IBD Data" parent cat_project_sample_subset_qc_data

cat cat_project_all_ibd_data=null disp "All Samples" parent cat_project_ibd_data
cat cat_project_sample_ibd_data=null disp "Project Samples" parent cat_project_ibd_data
cat cat_project_sample_subset_sample_ibd_data=null disp "Project Samples" parent cat_project_sample_subset_ibd_data

!!expand:,:soft,Soft:plink,PLINK:eigen,EIGENSTRAT! \
cat cat_project_all_soft_ibd_data=null disp "Soft" parent cat_project_all_ibd_data

!!expand:,:soft,Soft:plink,PLINK:eigen,EIGENSTRAT! \
!!expand:project:project:project_sample_subset! \
cat cat_project_sample_soft_ibd_data=null disp "Soft" parent cat_project_sample_ibd_data

!!expand:,:soft,Soft:epacts,EPACTS! \
cat cat_project_sample_soft_ibd_data=null disp "Soft" parent cat_project_sample_ibd_data

cat cat_project_region_exclude_ibd_data=null disp "Exclude Regions" parent cat_project_ibd_data

!!expand:project:project:pheno! \
cat cat_project_sample_subset_coverage_data=null disp "Coverage Data" parent cat_project_sample_subset_qc_data

cat cat_seq_batch_picard_data=null disp "Picard Data" class_level seq_batch

meta constant cat_pheno_qt=@pheno_qt disp "QT" class_level pheno

cat cat_pheno_slide_data=null disp "For Slides" class_level pheno
cat cat_pheno_slide_master_data=null disp "Master" parent cat_pheno_slide_data
cat cat_pheno_slide_coverage_data=null disp "Coverage" parent cat_pheno_slide_data
"""
}
    