
package loamstream.pipeline.examples

/**
 * LoamStream
 * Created by oliverr on 10/6/2015.
 */
object PipelineTargetedPart29 {
  val string =
 """ $mpjhf(seq_batch,seq_batch_picard_pdf_file,allow_empty=1) \
 $mpjhf(pheno,pheno_slide_failures_bar_pdf_file,if_prop=not_trait\,allow_empty=1) \
 $mpjhf(pheno,pheno_slide_failures_pdf_file,if_prop=not_trait\,allow_empty=1) \
 $mpjhf(pheno,pheno_slide_cross_failures_pdf_file,if_prop=not_trait\,allow_empty=1) \
 $mpjh(project,project_sample_cum_coverage_pdf_file) \
 $mpjhf(pheno,pheno_sample_coverage_pdf_file,if_prop=not_trait\,allow_empty=1) \
 $mpjh(project,project_slide_genes_pdf_file) \
 $mpjh(project,project_gene_cum_coverage_pdf_file) \
 $mpjhf(project,project_gene_dist_coverage_pdf_file,unless_prop=whole_exome\,allow_empty=1) \
 $mpjhf(project,project_lowest_gene_dist_coverage_pdf_file,unless_prop=whole_exome\,allow_empty=1) \
 $mpjh(project,project_gene_gc_pdf_file) \
 $mpjh(project,project_slide_exons_pdf_file) \
 $mpjh(project,project_exon_cum_coverage_pdf_file) \
 $mpjh(project,project_slide_baits_pdf_file) \
 $mpjh(project,project_bait_cum_coverage_pdf_file) \
 $mpjh(project,project_slide_vcounts_pdf_file) \
 $mpjh(project,project_slide_titv_pdf_file) \
 $mpjh(project,project_titv_ac_pdf_file) \
 $mpjh(project,project_slide_theta_pdf_file) \
 $mpjh(project,project_slide_ref_theta_pdf_file) \
 $mpjhf(marker,marker_positive_control_genotype_concordance_sum_pdf_file,if_prop=marker\,allow_empty=1) \
 $mpjh(project,project_sstats_initial_pdf_file) \
 $mpjhf(pheno,pheno_all_sstats_pdf_file,if_prop=qc_trait) \
 $mpjhf(pheno,pheno_slide_all_sstats_pdf_file,if_prop=qc_trait) \
 $mpjh(project,project_sstats_highlighted_pdf_file) \
 $mpjhf(pheno,pheno_highlighted_sstats_pdf_file,if_prop=qc_trait) \
 $mpjh(project,project_slide_sstats_highlighted_pdf_file) \
 $mpjh(project,project_slide_seq_qc_failures_pdf_file) \
 $mpjh(project,project_sstats_final_pdf_file) \
 $mpjh(project,project_slide_sstats_final_pdf_file) \
 $mpjhf(pheno,pheno_top_mds_pdf_file,if_prop=not_trait\,allow_empty=1) \
 $mpjhf(pheno,pheno_all_mds_pdf_file,if_prop=not_trait\,allow_empty=1) \
 $mpjh(project,project_vstats_initial_pdf_file) \
 $mpjh(project,project_vstats_highlighted_pdf_file) \
 $mpjh(project,project_vstats_final_pdf_file) \
 $mpjh(project,project_slide_var_qc_failures_pdf_file) \
 $mpjh(project,project_slide_var_pass_counts_pdf_file) \
 $mpjh(project,project_slide_sample_var_pass_counts_pdf_file) \
 $mpjh(project,project_gstats_pdf_file) \
 $mpjh(project,project_gstats_highlighted_pdf_file) \
 > !{output,,project_slide_master_ps_file} class_level project

local cmd make_project_slide_master_pdf_file=epstopdf !{input,,project_slide_master_ps_file} !{output,--outfile=,project_slide_master_pdf_file} class_level project

local cmd make_burden_slide_master_ps_file=$smart_cut_cmd \
 $mpjh(burden,burden_title_slide_pdf_file) \
 $mpjhf(burden,pheno_pheno_test_info_pdf_file,unless_prop=no_var_qq\,unless_prop=no_var_filter\,or_if_prop=1\,allow_empty=1) \
 $mpjhf(burden,burden_common_vassoc_qq_pdf_file,unless_prop=no_var_qq\,allow_empty=1) \
 $mpjhf(burden,burden_uncommon_vassoc_qq_pdf_file,unless_prop=no_var_qq\,allow_empty=1) \
 $mpjhf(burden,burden_rare_vassoc_qq_pdf_file,unless_prop=no_var_qq\,allow_empty=1) \
 $mpjhf(burden,burden_slide_common_vassoc_pdf_file,unless_prop=no_var_filter\,allow_empty=1) \
 $mpjhf(burden,burden_slide_common_vassoc_meta_trait_pdf_file,unless_prop=no_var_filter\,if_prop=meta_trait_inv\,allow_empty=1) \
 $mpjhf(burden,burden_slide_vassoc_pdf_file,unless_prop=no_var_filter\,allow_empty=1) \
 $mpjhf(burden,burden_slide_vassoc_meta_trait_pdf_file,unless_prop=no_var_filter\,if_prop=meta_trait_inv\,allow_empty=1) \
 $mpjhf(burden,burden_slide_unique_pdf_file,unless_prop=no_var_filter\,allow_empty=1) \
 $mpjhf(burden,burden_slide_unique_meta_trait_pdf_file,unless_prop=no_var_filter\,if_prop=meta_trait_inv\,allow_empty=1) \
 $mpjhf(burden,burden_burden_test_info_pdf_file,if_prop=burden_test\,allow_empty=1) \
 $mpjhf(burden,burden_gassoc_qq_pdf_file,if_prop=burden_test\,unless_prop=no_gene_qq\,allow_empty=1) \
 $mpjhf(burden,burden_slide_gassoc_pdf_file,if_prop=burden_test\,allow_empty=1) \
 > !{output,,burden_slide_master_ps_file} class_level burden

local cmd make_burden_slide_master_pdf_file=epstopdf !{input,,burden_slide_master_ps_file} !{output,--outfile=,burden_slide_master_pdf_file} class_level burden

local cmd make_pheno_slide_master_pdf_file=epstopdf !{input,,pheno_slide_master_ps_file} !{output,--outfile=,pheno_slide_master_pdf_file} class_level pheno


local cmd make_pheno_slide_master_qc_ps_file=$smart_cut_cmd \
 $mpjh(pheno,pheno_qc_title_slide_pdf_file) \
 $mpjh(pheno,pheno_slide_failures_bar_pdf_file) \
 $mpjh(pheno,pheno_slide_failures_pdf_file) \
 $mpjhf(pheno,pheno_slide_cross_failures_pdf_file,unless_prop=is_cross_classification\,allow_empty=1) \
 $mpjhf(pheno,pheno_trait_hist_pdf_file,if_prop=pheno_qt\,allow_empty=1) \
 $mpjh(pheno,pheno_sample_coverage_pdf_file) \
 $mpjh(pheno,pheno_slide_filtered_sstats_pdf_file) \
 $mpjh(pheno,pheno_top_mds_pdf_file) \
 $mpjh(pheno,pheno_all_mds_pdf_file) \
 $mpjh(pheno,pheno_all_related_pdf_file) \
 $mpjh(pheno,pheno_unrelated_related_pdf_file) \
 $mpjh(pheno,pheno_popgen_all_pdf_file) \
 $mpjh(pheno,pheno_popgen_highlighted_pdf_file) \
 $mpjh(pheno,pheno_slide_popgen_qc_failures_pdf_file) \
 $mpjh(pheno,pheno_popgen_final_pdf_file) \
 $mpjh(pheno,pheno_popgen_unrelated_pdf_file) \
 $mpjh(pheno,pheno_slide_popgen_sstats_pdf_file) \
 $mpjh(pheno,pheno_genome_pdf_file) \
 $mpjhf(pheno,pheno_cluster_assign_mds_pdf_file,allow_empty=1) \
 $mpjhf(pheno,pheno_cluster_stats_pdf_file,allow_empty=1) \
 > !{output,,pheno_slide_master_qc_ps_file} class_level pheno skip_if not_trait

local cmd make_pheno_slide_master_assoc_ps_file=$smart_cut_cmd \
 $mpjh(pheno,pheno_assoc_title_slide_pdf_file) \
 $mph(burden,burden_slide_master_ps_file) \
 $mpjh(pheno,pheno_interesting_counts_pdf_file) \
 > !{output,,pheno_slide_master_assoc_ps_file} class_level pheno skip_if not_trait

local cmd make_pheno_slide_master_ps_file=$smart_cut_cmd \
 $mpjh(pheno,pheno_title_slide_pdf_file) \
 $mph(pheno,pheno_slide_master_qc_ps_file) \
 $mph(pheno,pheno_slide_master_assoc_ps_file) \
 > !{output,,pheno_slide_master_ps_file} class_level pheno skip_if not_trait


local cmd make_pheno_slide_master_pdf_file=epstopdf !{input,,pheno_slide_master_ps_file} !{output,--outfile=,pheno_slide_master_pdf_file} class_level pheno

local cmd make_pheno_slide_master_genes_ps_file=$smart_cut_cmd \
 $mpjh(pheno,pheno_gene_title_slide_pdf_file) \
 $mph(gene,gene_slide_master_sum_ps_file) \
 > !{output,,pheno_slide_master_genes_ps_file} class_level pheno skip_if not_trait

!!expand:titletype:qc:assoc:genes! \
local cmd make_pheno_slide_master_titletype_pdf_file=epstopdf !{input,,pheno_slide_master_titletype_ps_file} !{output,--outfile=,pheno_slide_master_titletype_pdf_file} class_level pheno


local cmd make_locus_slide_master_ps_file=$smart_cut_cmd \
 $mpjh(locus,locus_title_slide_pdf_file) \
 $mpjh(locus,locus_fancy_marker_plot_pdf_file) \
 $mpjh(locus,locus_fancy_plot_pdf_file) \
 $mpjh(locus,locus_var_coverage_pdf_file) \
 $mpjhf(gene,gene_var_pdf_file,allow_empty=1) \
 > !{output,,locus_slide_master_ps_file} class_level locus

local cmd make_locus_slide_master_pdf_file=epstopdf !{input,,locus_slide_master_ps_file} !{output,--outfile=,locus_slide_master_pdf_file} class_level locus

local cmd make_gene_slide_master_ps_file=$smart_cut_cmd \
 $mpjh(gene,gene_title_slide_pdf_file) \
 $mpjh(locus,locus_fancy_marker_plot_pdf_file) \
 $mpjh(gene,gene_ucsc_pdf_file) \
 $mpjh(gene,gene_qq_pdf_file) \
 $mpjh(gene,gene_var_pdf_file) \
 $mpjhf(gene_burden,gene_burden_vassoc_pdf_file,unless_prop=no_var_filter) \
 $mpjhf(gene,gene_meta_trait_associated_pdf_file,if_prop=meta_trait_inv\,allow_empty=1) \
 $mpjhf(gene,gene_all_trait_associated_pdf_file,if_prop=related_traits\,allow_empty=1) \
 $mpjh(gene,gene_gassoc_pdf_file) \
 $mpjh(project,project_additional_slide_pdf_file) \ 
 $mpjh(gene,gene_var_counts_pdf_file) \
 $mpjh(gene,gene_var_coverage_pdf_file) \
 $mpjh(gene,gene_qc_metrics_pdf_file) \
 $mpjh(gene,gene_all_variants_pdf_file) \
 $mpjh(gene,gene_all_data_pdf_file) \
 $mphf(variant,variant_slide_master_ps_file,allow_empty=1) \
 > !{output,,gene_slide_master_ps_file} class_level gene

local cmd make_gene_slide_master_pdf_file=epstopdf !{input,,gene_slide_master_ps_file} !{output,--outfile=,gene_slide_master_pdf_file} class_level gene

local cmd make_gene_burden_slide_master_ps_file=$smart_cut_cmd \
 $mpjhf(gene_burden,gene_burden_vassoc_pdf_file,unless_prop=no_var_filter\,allow_empty=1) \
 $mpjhf(transcript_burden,transcript_burden_pheno_pdf_file,if_prop=burden_test\,allow_empty=1) \
 $mpjhf(transcript_burden,transcript_burden_all_pheno_pdf_file,if_prop=related_traits\,if_prop=burden_test\,allow_empty=1) \
 > !{output,,gene_burden_slide_master_ps_file} class_level gene_burden skip_if and,no_var_filter,no_var_qq,no_burden_test

local cmd make_gene_burden_slide_master_pdf_file=epstopdf !{input,,gene_burden_slide_master_ps_file} !{output,--outfile=,gene_burden_slide_master_pdf_file} class_level gene_burden

local cmd make_gene_slide_master_sum_ps_file=$smart_cut_cmd \
 $mpjh(gene,gene_title_slide_pdf_file) \
 $mpjh(gene,gene_var_pdf_file) \
 > !{output,,gene_slide_master_sum_ps_file} class_level gene

local cmd make_gene_slide_master_sum_pdf_file=epstopdf !{input,,gene_slide_master_sum_ps_file} !{output,--outfile=,gene_slide_master_sum_pdf_file} class_level gene


local cmd make_variant_slide_master_ps_file=$smart_cut_cmd \
 $mpjh(variant,variant_title_slide_pdf_file) \
 $mpjh(variant,variant_ucsc_pdf_file) \
 $mpjh(variant,variant_pheno_pdf_file) \
 $mpjhf(variant,variant_all_pheno_pdf_file,if_prop=related_traits\,allow_empty=1) \
 $mpjh(variant,variant_slide_sstats_pdf_file) \
 $mpjh(variant,variant_sstats_pdf_file) \
 $mpjh(variant,variant_igv_pdf_file) \
 $mpjh(variant,variant_top_mds_pdf_file) \
 $mpjh(variant,variant_all_mds_pdf_file) \
 $mpjh(variant,variant_genome_pdf_file) \
 $mpjhf(variant,variant_r2_pdf_file,if_prop=nind:gt:1\,allow_empty=1) \
 $mpjhf(variant,variant_r2_table_pdf_file,if_prop=nind:gt:1\,allow_empty=1) \
 $mpjhf(variant,variant_haplotype_analysis_info_pdf_file,if_prop=nind:gt:1\,allow_empty=1) \
 $mpjhf(variant,variant_haplotype_analysis_pdf_file,if_prop=nind:gt:1\,allow_empty=1) \
 $mpjhf(variant,variant_haplotype_analysis_longest_pdf_file,if_prop=nind:gt:1\,allow_empty=1) \
 $mpjhf(variant,variant_hap_threshold_vassoc_pdf_file,if_prop=nind:gt:1\,allow_empty=1) \
 $mpjhf(variant,variant_traditional_threshold_vassoc_pdf_file,if_prop=nind:gt:1\,allow_empty=1) \
 > !{output,,variant_slide_master_ps_file} class_level variant

local cmd make_variant_slide_master_pdf_file=epstopdf !{input,,variant_slide_master_ps_file} !{output,--outfile=,variant_slide_master_pdf_file} class_level variant
"""
}
    