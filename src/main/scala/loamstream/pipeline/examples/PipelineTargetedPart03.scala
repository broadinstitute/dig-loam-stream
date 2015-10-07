
package loamstream.pipeline.examples

/**
 * LoamStream
 * Created by oliverr on 10/6/2015.
 */
object PipelineTargetedPart03 {
  val string =
 """#cat cat_pheno_slide_dirty_data=null disp "Dirty Data" parent cat_pheno_slide_data
cat cat_pheno_slide_sample_qc_data=null disp "Sample QC" parent cat_pheno_slide_data
#cat cat_pheno_slide_variants_data=null disp "Variants" parent cat_pheno_slide_data
#cat cat_pheno_slide_genes_data=null disp "Genes" parent cat_pheno_slide_data
#cat cat_pheno_dirty_variant_assoc_data=null disp "Variant associations" parent cat_pheno_slide_dirty_data
#cat cat_pheno_dirty_all_variant_assoc_data=null disp "All Sites" parent cat_pheno_dirty_variant_assoc_data
#cat cat_pheno_dirty_qc_pass_variant_assoc_data=null disp "QC Pass" parent cat_pheno_dirty_variant_assoc_data

#cat cat_pheno_dirty_gene_assoc_data=null disp "Gene associations" parent cat_pheno_slide_dirty_data
#cat cat_pheno_dirty_gene_assoc_data=null disp "Gene associations" parent cat_pheno_slide_dirty_data
#cat cat_pheno_dirty_all_gene_assoc_data=null disp "All Sites" parent cat_pheno_dirty_gene_assoc_data
#cat cat_pheno_dirty_qc_pass_gene_assoc_data=null disp "QC Pass" parent cat_pheno_dirty_gene_assoc_data

cat cat_pheno_meta_info=null disp "Meta Info" class_level pheno
cat cat_pheno_sample_info=null disp "Sample Info" parent cat_pheno_meta_info
cat cat_pheno_meta_data=null disp "Meta Data" parent cat_pheno_sample_info
cat cat_pheno_passed_meta_data=null disp "Passed" parent cat_pheno_meta_data
cat cat_pheno_failed_meta_data=null disp "Failed" parent cat_pheno_meta_data
cat cat_pheno_all_meta_data=null disp "All" parent cat_pheno_meta_data
cat cat_pheno_sample_coverage_data=null disp "Coverage Data" parent cat_pheno_sample_info
cat cat_pheno_interesting_info=null disp "Interesting Genes & Variants" parent cat_pheno_meta_info
cat cat_pheno_trait_info=null disp "Trait Info" parent cat_pheno_meta_info
cat cat_pheno_test_info=null disp "Test Info" parent cat_pheno_meta_info
cat cat_pheno_subset_info=null disp "Variant Subsets" parent cat_pheno_meta_info

!!expand:pheno:pheno:pheno_sample_subset! \
cat cat_pheno_qc_data=null disp "QC Data" class_level pheno
!!expand:pheno:pheno:pheno_sample_subset! \
cat cat_pheno_sample_qc_data=null disp "Sample QC" parent cat_pheno_qc_data

cat cat_pheno_sstats_qc_data=null disp "Pre Seq" parent cat_pheno_sample_qc_data
cat cat_pheno_sstats_qc_stats_data=null disp "Stats" parent cat_pheno_sstats_qc_data
cat cat_pheno_sstats_qc_plot_data=null disp "Plots" parent cat_pheno_sstats_qc_data

!!expand:pheno:pheno:pheno_sample_subset! \
cat cat_pheno_popgen_qc_data=null disp "Popgen" parent cat_pheno_sample_qc_data
!!expand:pheno:pheno:pheno_sample_subset! \
cat cat_pheno_popgen_qc_stats_data=null disp "Stats" parent cat_pheno_popgen_qc_data
cat cat_pheno_popgen_qc_plot_data=null disp "Plots" parent cat_pheno_popgen_qc_data

!!expand:pheno:pheno:pheno_sample_subset! \
cat cat_pheno_ibd_qc_data=null disp "IBD" parent cat_pheno_sample_qc_data
!!expand:pheno:pheno:pheno_sample_subset! \
cat cat_pheno_ibd_qc_stats_data=null disp "Stats" parent cat_pheno_ibd_qc_data
cat cat_pheno_ibd_qc_plot_data=null disp "Plots" parent cat_pheno_ibd_qc_data

!!expand:pheno:pheno:pheno_sample_subset! \
cat cat_pheno_covar_qc_data=null disp "Covar" parent cat_pheno_sample_qc_data
!!expand:pheno:pheno:pheno_sample_subset! \
cat cat_pheno_covar_qc_stats_data=null disp "Stats" parent cat_pheno_covar_qc_data
cat cat_pheno_covar_qc_plot_data=null disp "Plots" parent cat_pheno_covar_qc_data

cat cat_pheno_cluster_qc_data=null disp "Cluster" parent cat_pheno_sample_qc_data
cat cat_pheno_cluster_qc_stats_data=null disp "Stats" parent cat_pheno_cluster_qc_data
cat cat_pheno_cluster_qc_plot_data=null disp "Plots" parent cat_pheno_cluster_qc_data

cat cat_pheno_variant_subset_meta_data=null disp "Meta Data" parent cat_pheno_variant_subset_genotype_data
cat cat_pheno_variant_subset_qc_data=null disp "QC Data" parent cat_pheno_variant_subset_genotype_data

cat cat_pheno_genotype_data=null disp "Genotype Data" class_level pheno
cat cat_pheno_variant_subset_genotype_data=null disp "Pheno Subset Data" class_level pheno_variant_subset
!!expand:pheno:pheno:pheno_variant_subset! \
cat cat_pheno_plinkseq_data=null disp "Plink/Seq Data" parent cat_pheno_genotype_data
cat cat_pheno_plinkseq_phe_data=null disp "Phe" parent cat_pheno_plinkseq_data
!!expand:,:type,Type:all,All:unrelated,Unrelated! \
cat cat_pheno_plinkseq_type_include_data=null disp "Type Include" parent cat_pheno_plinkseq_data
!!expand:pheno:pheno:pheno_variant_subset! \
cat cat_pheno_plinkseq_raw_data=null disp "Raw" parent cat_pheno_plinkseq_data
!!expand:pheno:pheno:pheno_variant_subset! \
cat cat_pheno_plinkseq_clean_data=null disp "Clean" parent cat_pheno_plinkseq_data
!!expand:pheno:pheno:pheno_variant_subset! \
cat cat_pheno_plinkseq_strat_data=null disp "Strat" parent cat_pheno_plinkseq_data
!!expand:pheno:pheno:pheno_variant_subset! \
cat cat_pheno_plink_data=null disp "Plink Data" parent cat_pheno_genotype_data
cat cat_pheno_plink_phe_data=null disp "Phe" parent cat_pheno_plink_data
!!expand:type:all:unrelated! \
cat cat_pheno_plink_type_include_data=null disp "Include" parent cat_pheno_plink_data
cat cat_pheno_epacts_data=null disp "EPACTS Data" parent cat_pheno_genotype_data
cat cat_pheno_epacts_phe_data=null disp "Phe" parent cat_pheno_epacts_data

!!expand:pheno:pheno:pheno_variant_subset! \
cat cat_pheno_plink_seq_data=null disp "Seq" parent cat_pheno_plink_data
cat cat_pheno_plink_pruned_marker_data=null disp "Pruned Marker" parent cat_pheno_plink_data
cat cat_pheno_plink_combined_data=null disp "Combined" parent cat_pheno_plink_data

!!expand:pheno:pheno:pheno_variant_subset! \
cat cat_pheno_variant_qc_data=null disp "Variant QC" parent cat_pheno_qc_data
!!expand:pheno:pheno:pheno_variant_subset! \
cat cat_pheno_variant_qc_stats_data=null disp "Variant Stats" parent cat_pheno_variant_qc_data
!!expand:pheno:pheno:pheno_variant_subset! \
cat cat_pheno_qc_gene_stats_data=null disp "Gene Stats" parent cat_pheno_variant_qc_data
!!expand:pheno:pheno:pheno_variant_subset! \
cat cat_pheno_variant_qc_plink_data=null disp "Plink" parent cat_pheno_variant_qc_data

cat cat_pheno_assoc_data=null disp "Associations" class_level pheno
cat cat_pheno_variant_assoc_data=null disp "Variant associations" parent cat_pheno_assoc_data
cat cat_pheno_variant_subset_variant_assoc_data=null disp "Variant associations" parent cat_pheno_variant_subset_genotype_data
cat cat_pheno_gene_assoc_data=null disp "Gene associations" parent cat_pheno_assoc_data
cat cat_pheno_pathway_assoc_data=null disp "Pathway associations" parent cat_pheno_assoc_data
cat cat_pheno_variant_imputation_data=null disp "Variant imputation" parent cat_pheno_assoc_data

cat cat_pheno_fine_mapping_data=null disp "Fine mapping" class_level pheno
cat cat_pheno_top_hits_data=null disp "Top Hits" parent cat_pheno_fine_mapping_data

!!expand:pheno_test:pheno_test:pheno_test_variant_subset! \
cat cat_pheno_test_data=null disp "SV test files" class_level pheno_test

!!expand:,:annot,Annot:annot,Annot:annot_variant_subset,Annot Subset! \
cat cat_annot_data=null disp "Annot Data" class_level annot
!!expand:annotl:annot:annot_variant_subset! \
cat cat_annotl_var_data=null disp "Variants to use in this annot" parent cat_annotl_data
cat cat_annot_subset_info=null disp "Variant Subsets" parent cat_annot_data

cat cat_annot_var_qc_filter_data=null disp "QC Filter" class_level annot_var_qc_filter

!!expand:,:burden,Burden:burden,Burden:burden_variant_subset,Burden Subset! \
cat cat_burden_data=null disp "Burden Data" class_level burden
cat cat_burden_slide_master_data=null disp "Master" parent cat_burden_data
cat cat_burden_info_data=null disp "Info" parent cat_burden_data
!!expand:burden:burden:burden_variant_subset! \
cat cat_burden_association_data=null disp "Output of association results" parent cat_burden_data
!!expand:burden:burden:burden_variant_subset! \
cat cat_burden_association_display_data=null disp "Display results" parent cat_burden_association_data
cat cat_burden_association_test_data=null disp "Test results" parent cat_burden_association_data
cat cat_burden_interesting_data=null disp "Interesting gene/variant information" parent cat_burden_data
!!expand:burdenl:burden:burden_variant_subset! \
cat cat_burdenl_var_data=null disp "Variants to use in this burden" parent cat_burdenl_data
cat cat_burden_summary_data=null disp "Summary of association results" parent cat_burden_data
!!expand:burden:burden:annot! \
cat cat_burden_pathway_association_data=null disp "Output of pathway association results" parent cat_burden_data
#cat cat_burden_detail_data=null disp "Detailed Analysis" class_level burden
#cat cat_burden_recessive_test_data=null disp "Variant recessive test" parent cat_burden_detail_data
#cat cat_burden_haplotype_burden_data=null disp "Variant haplotype burden" parent cat_burden_detail_data
cat cat_burden_subset_info=null disp "Variant Subsets" parent cat_burden_data

!!expand:,:burden_test,Burden:burden_test,Burden Test:burden_test_variant_subset,Burden Test Subset! \
cat cat_burden_test_data=null disp "Burden Data" class_level burden_test
cat cat_burden_test_info_data=null disp "Info" parent cat_burden_test_data

cat cat_burden_test_samp_data=null disp "Samples to use in this burden test" parent cat_burden_test_data
!!expand:burden_test:burden_test:burden_test_variant_subset! \
cat cat_burden_test_input_association_data=null disp "Input to association results" parent cat_burden_test_data
!!expand:burden_test:burden_test:burden_test_variant_subset! \
cat cat_burden_test_association_data=null disp "Output of association results" parent cat_burden_test_data
cat cat_burden_test_pathway_association_data=null disp "Output of pathway association results" parent cat_burden_test_data

meta constant cat_variant_consequence=@consequence disp "Consequence" class_level variant
meta constant cat_variant_protein_change=@proteinchange disp "Change" class_level variant
#meta constant cat_variant_var_annot=@var_annot disp "$disp_for_var_annot" class_level variant
meta constant cat_variant_ncase=@ncase disp "Ncase" class_level variant
meta constant cat_variant_ncontrol=@ncontrol disp "Ncontrol" class_level variant
cat cat_variant_data=null disp "Variant Data" class_level variant
cat cat_variant_slide_master_data=null disp "Master" parent cat_variant_data
cat cat_variant_meta_data=null disp "Meta data" parent cat_variant_data
cat cat_variant_qc_data=null disp "QC data" parent cat_variant_data
cat cat_variant_plot_data=null disp "Plot data" parent cat_variant_data
cat cat_variant_ld_data=null disp "LD data" parent cat_variant_data
cat cat_variant_haplotype_analysis_data=null disp "Haplotype Analysis" parent cat_variant_data
cat cat_variant_imputation_data=null disp "Imputation" parent cat_variant_data
cat cat_variant_hap_imputation_data=null disp "Haplotype" parent cat_variant_imputation_data
cat cat_variant_traditional_imputation_data=null disp "Traditional" parent cat_variant_imputation_data

#region/locus files

!!expand:rorl:region:locus! \
meta constant cat_rorl_range=@rorl_range disp "Range" class_level rorl

cat cat_locus_summary_data=null disp "Summary" class_level locus
cat cat_locus_slide_master_data=null disp "Master" parent cat_locus_summary_data
cat cat_locus_coverage_data=null disp "Coverage" parent cat_locus_summary_data
cat cat_locus_association_data=null disp "Association" parent cat_locus_summary_data
cat cat_locus_marker_association_data=null disp "Marker Association" parent cat_locus_summary_data
cat cat_locus_haplotype_burden_data=null disp "Haplotype Burden" parent cat_locus_summary_data

!!expand:rorl:region:locus! \
cat cat_rorl_ped_data=null disp "Ped data" class_level rorl
!!expand:rorl:region:locus! \
cat cat_rorl_meta_data=null disp "Meta data" parent cat_rorl_ped_data
!!expand:rorl:region:locus! \
cat cat_rorl_ped_marker_data=null disp "Markers" parent cat_rorl_ped_data
!!expand:rorl:region:locus! \
cat cat_rorl_ped_all_marker_data=null disp "All" parent cat_rorl_ped_marker_data
!!expand:rorl:region:locus! \
cat cat_rorl_ped_common_marker_data=null disp "Common" parent cat_rorl_ped_marker_data
!!expand:rorl:region:locus! \
cat cat_rorl_ped_seq_data=null disp "Sequence" parent cat_rorl_ped_data
!!expand:rorl:region:locus! \
cat cat_rorl_ped_all_seq_data=null disp "All" parent cat_rorl_ped_seq_data
!!expand:rorl:region:locus! \
cat cat_rorl_ped_non_marker_seq_data=null disp "Non Marker" parent cat_rorl_ped_seq_data
!!expand:rorl:region:locus! \
cat cat_rorl_ped_combined_data=null disp "Combined" parent cat_rorl_ped_data

!!expand:rorl:region:locus! \
cat cat_rorl_other_data=null disp "Imputation data" class_level rorl
!!expand:rorl:region:locus! \
cat cat_rorl_beagle_data=null disp "Beagle" parent cat_rorl_other_data
!!expand:rorl:region! \
cat cat_rorl_beagle_input_data=null disp "Input" parent cat_rorl_beagle_data
!!expand:rorl:region:locus! \
cat cat_rorl_beagle_output_data=null disp "Output" parent cat_rorl_beagle_data

!!expand:rorl:region:locus! \
cat cat_rorl_impute2_data=null disp "Impute2" parent cat_rorl_other_data
!!expand:rorl:region:locus! \
cat cat_rorl_impute2_input_data=null disp "Input" parent cat_rorl_impute2_data
!!expand:rorl:region:locus! \
cat cat_rorl_impute2_output_data=null disp "Output" parent cat_rorl_impute2_data
!!expand:rorl:locus! \
cat cat_rorl_impute2_association_data=null disp "Association" parent cat_rorl_impute2_data

cat cat_region_marker_association_data=null disp "Marker Association" parent cat_region_other_data


cat cat_gene_summary_data=null disp "Summaries" class_level gene
cat cat_gene_slide_master_data=null disp "Master" parent cat_gene_summary_data
cat cat_gene_plot_plink_data=null disp "Sequence Plink Data" parent cat_gene_summary_data
#cat cat_gene_plot_data=null disp "Plots" parent cat_gene_summary_data
cat cat_gene_meta_data=null disp "Meta data" parent cat_gene_summary_data
cat cat_gene_annot_data=null disp "Annotations" parent cat_gene_summary_data
cat cat_gene_qc_data=null disp "QC Data" parent cat_gene_summary_data
cat cat_gene_ld_data=null disp "LD Data" parent cat_gene_summary_data
cat cat_gene_all_ld_data=null disp "Marker + Seq" parent cat_gene_ld_data
cat cat_gene_seq_ld_data=null disp "Seq" parent cat_gene_ld_data
cat cat_gene_assoc_data=null disp "Association Data" parent cat_gene_summary_data
cat cat_gene_imputation_assoc_data=null disp "Imputation" parent cat_gene_assoc_data
cat cat_gene_seq_assoc_data=null disp "Sequence" parent cat_gene_assoc_data

cat cat_gene_burden_data=null disp "Gene Burden Data" class_level gene_burden
cat cat_transcript_burden_data=null disp "Transcript Burden Data" class_level transcript_burden

cat cat_call_set_sequence_data=null disp "Sequence Data" class_level call_set
#cat cat_call_set_picard_data=null disp "Picard Data" parent cat_call_set_sequence_data
cat cat_call_set_call_data=null disp "Calls" parent cat_call_set_sequence_data
cat cat_call_set_meta_call_data=null disp "Meta" parent cat_call_set_call_data
cat cat_call_set_variant_call_data=null disp "Variants" parent cat_call_set_call_data
cat cat_call_set_all_call_data=null disp "All" parent cat_call_set_call_data

#cat cat_call_set_unfiltered_call_data=null disp "Unfiltered" parent cat_call_set_call_data

#cat cat_call_set_coverage_data=null disp "Coverage" class_level call_set
#cat cat_call_set_sample_coverage_data=null disp "Samples" parent cat_call_set_coverage_data
#cat cat_call_set_gene_coverage_data=null disp "Genes" parent cat_call_set_coverage_data
#cat cat_call_set_gene_dist_coverage_data=null disp "Sample distribution" parent cat_call_set_gene_coverage_data
#cat cat_call_set_gene_cum_coverage_data=null disp "Cumulative \#genes covered" parent cat_call_set_gene_coverage_data

#cat cat_project_subset_call_data=null disp "Calls" class_level project_subset
#cat cat_project_subset_samtools_data=null disp "Samtools" parent cat_project_subset_call_data

!!expand:call_set_subset:call_set_subset:call_set_sample_subset! \
cat cat_call_set_subset_call_data=null disp "Calls" class_level call_set_subset

cat cat_call_set_subset_variant_call_data=null disp "Input files" parent cat_call_set_subset_call_data

!!expand:call_set_subset:call_set_subset:call_set_sample_subset! \
cat cat_call_set_subset_missed_variants_data=null disp "Missed Variants" parent cat_call_set_subset_call_data
cat cat_sample_sequence_data=null disp "Sequence Data" class_level sample
meta constant cat_sample_failed=@failed disp "Failed" class_level sample
cat cat_sample_read_data=null disp "Read Data" parent cat_sample_sequence_data
cat cat_sample_coverage_data=null disp "Coverage" parent cat_sample_sequence_data

###BEGIN Uncomment this section to have files generated by Sample_UnifiedGenotyperToEval
#cat cat_sample_single_sample_snp_call_data=null disp "Single Sample SNP Calls" parent cat_sample_sequence_data
#cat cat_sample_meta_call_data=null disp "Meta" parent cat_sample_single_sample_snp_call_data
#cat cat_sample_all_call_data=null disp "All" parent cat_sample_single_sample_snp_call_data
#cat cat_sample_filtered_call_data=null disp "Filtered" parent cat_sample_single_sample_snp_call_data
#cat cat_sample_unfiltered_call_data=null disp "Unfiltered" parent cat_sample_single_sample_snp_call_data
###BEGIN Uncomment this section to have files generated by Sample_UnifiedGenotyperToEval

##BEGIN Uncomment to get files for indels
#cat cat_sample_indel_call_data=null disp "Indel Calls" parent cat_sample_sequence_data
##END Uncomment to get files for indels

#--------
#files
#--------

#project
minor file path project_interval_list_file=@project.interval_list dir project_dir disp ".interval_list" parent cat_project_target_data class_level project comment "Interval list of all regions in study"

minor file path project_expanded_interval_list_file=@project.expanded.interval_list dir project_dir disp ".expanded.interval_list" parent cat_project_target_data class_level project comment "Interval list of all regions in study -- expanded by $expand_interval_size bp"

file path project_chr_map_file=@project.chr.map.txt dir project_dir disp ".chr.map.txt" parent cat_project_target_data class_level project comment "File to use to map input chromsome names to output chromosome names"

minor file path project_target_length_file=@project.target.length dir project_dir disp ".target.length" parent cat_project_target_data class_level project comment "The length of the target region"
!!expand:project:project:project_variant_subset! \
minor table nohead file path project_genes_gtf_file=@project.genes.gtf dir project_dir disp ".genes.gtf" parent cat_project_target_data class_level project comment "GTF file of all genes in study"

minor table nohead file path project_variant_subset_region_file=@project_variant_subset.regions dir project_variant_subset_dir disp ".regions" parent cat_project_variant_subset_target_data class_level project_variant_subset comment "Regions in gtf file to use for this subset" 

table nohead file path project_bait_target_file=@project.bait.targets dir project_dir disp ".bait.targets" parent cat_project_target_data class_level project comment "All baits in study: has bait, chr, start, stop"
table nohead file path project_exon_target_file=@project.exon.targets dir project_dir disp ".exon.targets" parent cat_project_target_data class_level project comment "All exons in study: has gene, exon, chr, start, stop"
table nohead file path project_expanded_exon_target_file=@project.expanded.exon.targets dir project_dir disp ".expanded.exon.targets" parent cat_project_target_data class_level project comment "All exons in study: has gene, exon, chr, start, stop  -- expanded by $expand_interval_size bp"
table nohead file path project_variant_gene_annotation_file=@project.variant.gene.annot dir project_dir disp ".variant.gene.annot" parent cat_project_target_data class_level project comment "For each variant, lists the gene it lies in"

table nohead file path project_gene_target_file=@project.gene.targets dir project_dir disp ".gene.targets" parent cat_project_target_data class_level project comment "All genes in study: has name, chr, start, stop"

table nohead file path project_transcript_target_file=@project.transcript.targets dir project_dir disp ".transcript.targets" parent cat_project_target_data class_level project comment "All transcripts in study (for use solely in partitioning regions up)"

table nohead file path project_closest_gene_file=@project.closest.gene dir project_dir disp ".closest.gene" parent cat_project_target_data class_level project comment "Lists which regions are closest to each gene"
"""
}
    