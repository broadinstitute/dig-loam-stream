
package loamstream.pipeline.examples

/**
 * LoamStream
 * Created by oliverr on 10/6/2015.
 */
object PipelineTargetedPart06 {
  val string =
 """
file path project_variant_exclude_file=@project.variant.exclude dir project_dir disp ".variant.exclude" parent cat_project_qc_var_exclude_data class_level project comment "Variants excluded because they are outliers on one or more measures"

table file path project_extended_variant_exclude_detail_file=@project.extended.variant.exclude.detail dir project_dir disp ".extended.variant.exclude.detail" parent cat_project_qc_var_exclude_data class_level project comment "Variants excluded because they are outliers on one or more measures --- with information about the metrics that caused them to fail (extended filters)"

file path project_extended_variant_custom_exclude_file=@project.extended.variant.custom.exclude dir project_dir disp ".extended.variant.custom.exclude" parent cat_project_qc_var_exclude_data class_level project comment "Specify a file with (additional) custom extended exclusions"

file path project_extended_variant_exclude_file=@project.extended.variant.exclude dir project_dir disp ".extended.variant.exclude" parent cat_project_qc_var_exclude_data class_level project comment "Variants excluded because they are outliers on one or more measures (extended filters)"

table file path project_extended_strict_variant_exclude_detail_file=@project.extended.strict.variant.exclude.detail dir project_dir disp ".extended.strict.variant.exclude.detail" parent cat_project_qc_var_exclude_data class_level project comment "Variants excluded because they are outliers on one or more measures --- with information about the metrics that caused them to fail (extended strict filters)"

file path project_extended_strict_variant_custom_exclude_file=@project.extended.strict.variant.custom.exclude dir project_dir disp ".extended.strict.variant.custom.exclude" parent cat_project_qc_var_exclude_data class_level project comment "Specify a file with (additional) custom extended strict exclusions"

file path project_extended_strict_variant_exclude_file=@project.extended.strict.variant.exclude dir project_dir disp ".extended.strict.variant.exclude" parent cat_project_qc_var_exclude_data class_level project comment "Variants excluded because they are outliers on one or more measures (extended strict filters)"


!!expand:project:project:project_merge_subset! \
file path project_snp_indel_overlap_file=@project.snp.indel.overlap dir project_dir disp ".snp.indel.overlap" parent cat_project_all_variant_call_data class_level project comment "Variant positions that are both SNPs and indels"

!!expand:,:_vstats,.vstats:_vstats,.vstats:_extended_vstats,.extended.vstats! \
major file path project_vstats_initial_pdf_file=@project.vstats.initial.pdf dir project_dir disp ".initial.pdf" parent cat_project_qc_var_plots_data class_level project comment "Plot of various variant metrics across all variants --- hist plots"
!!expand:,:_vstats,.vstats:_vstats,.vstats:_extended_vstats,.extended.vstats! \
major file path project_vstats_highlighted_pdf_file=@project.vstats.highlighted.pdf dir project_dir disp ".highlighted.pdf" parent cat_project_qc_var_plots_data class_level project comment "Plot of various variant metrics across all variants with outlier variants highlighted --- hist plots"
!!expand:,:_vstats,.vstats:_vstats,.vstats:_extended_vstats,.extended.vstats! \
major file path project_vstats_final_pdf_file=@project.vstats.final.pdf dir project_dir disp ".final.pdf" parent cat_project_qc_var_plots_data class_level project comment "Plot of various variant metrics across all variants with outlier variants removed --- hist plots"

file table path project_plinkseq_gene_locstats_file=@project.locstats dir project_dir disp ".locstats" parent cat_project_qc_gene_stats_data class_level project comment "Dumped by Plink/Seq loc-stats"

file table path project_plinkseq_exon_locstats_file=@project.exon.locstats dir project_dir disp ".exon.locstats" parent cat_project_qc_gene_stats_data class_level project comment "Dumped by Plink/Seq loc-stats"

!!expand:,:vartype,ext:,:_syn,.syn:_ns,.ns! \
!!expand:,:stattype,dtype:gstats,genes:estats,exons! \
!!expand:project:project:project_variant_subset:pheno:pheno_variant_subset! \
file table path project_plinkseq_qc_passvartype_stattype_file=@project.qc_passext.stattype dir project_dir disp ".qc_passext.stattype" parent cat_project_qc_gene_stats_data class_level project comment "Dumped by Plink/Seq dtype g-stats on qc pass variants"

!!expand:project:project:project_variant_subset! \
file table path project_plinkseq_filtered_gstats_file=@project.filtered.gstats dir project_dir disp ".filtered.gstats" parent cat_project_qc_gene_stats_data class_level project comment "Dumped by Plink/Seq g-stats; uses only qc fail variants"

file table path project_gstats_file=@project.gstats dir project_dir disp ".gstats" parent cat_project_qc_gene_stats_data class_level project comment "All metrics for genes in project"

major table file path project_gene_outlier_file=@project.gene.outliers.csv dir project_dir disp ".outliers.csv" parent cat_project_qc_gene_stats_data class_level project comment "Outlier values from all gne statistics"

table file path project_gene_exclude_detail_file=@project.gene.exclude.detail dir project_dir disp ".gene.exclude.detail" parent cat_project_qc_gene_stats_data class_level project comment "Genes that are outliers on one or more measures --- with information about the metrics that caused them to fail"

major file path project_gstats_pdf_file=@project.gstats.pdf dir project_dir disp ".pdf" parent cat_project_qc_gene_plots_data class_level project comment "Plot of various gene metrics across all genes --- hist plot"
major file path project_gstats_highlighted_pdf_file=@project.gstats.highlighted.pdf dir project_dir disp ".highlighted.pdf" parent cat_project_qc_gene_plots_data class_level project comment "Plot of various variant metrics across all genes with outlier genes highlighted --- hist plot"

major file path project_gene_gc_pdf_file=@project.gene.gc.pdf dir project_dir disp ".pdf" parent cat_project_qc_gene_gc_plots_data class_level project comment "Plot of various gene metrics as a function of GC content"

#qc filter properties
major table file path sample_qc_filter_trait_exclude_file=@project.@sample_qc_filter.trait.exclude dir project_dir disp ".trait.exclude" parent cat_sample_qc_filter_trait_data class_level sample_qc_filter comment "Filter samples based on a trait"

major table file path sample_qc_filter_metric_exclude_file=@project.@sample_qc_filter.sstats.exclude dir project_dir disp ".metric.exclude" parent cat_sample_qc_filter_metric_data class_level sample_qc_filter comment "Filter samples based on a metric"

major table file path sample_qc_filter_exclude_file=@project.@sample_qc_filter.exclude dir project_dir disp ".exclude" parent cat_sample_qc_filter_metric_data class_level sample_qc_filter comment "Filter samples based on intersection of metrics"


major table file path var_qc_filter_exclude_file=@project.@var_qc_filter.exclude dir project_dir disp ".exclude" parent cat_var_qc_filter_data class_level var_qc_filter comment "Filter variants based on intersection of metrics"

major table file path var_qc_filter_modifier_exclude_file=@project.@var_qc_filter.modifier.exclude dir project_dir disp ".modifier.exclude" parent cat_var_qc_filter_data class_level var_qc_filter comment "Restricted list of variants to intersect with var_qc_filter_exclude_file"

major table file path annot_var_qc_filter_exclude_file=@annot.@annot_var_qc_filter.exclude dir annot_dir disp ".exclude" parent cat_annot_var_qc_filter_data class_level annot_var_qc_filter comment "Filter variants based on intersection of metrics"

#files to compute variant QC metrics (potentially) stratified by a separate trait

!!expand:pheno_variant_qc_strata:pheno_variant_qc_strata:pheno_variant_qc_pheno_strata! \
major table file path pheno_variant_qc_strata_sample_include_file=@pheno.@pheno_variant_qc_strata.sample.include dir pheno_variant_qc_stratas_dir disp ".sample.include" parent cat_pheno_variant_qc_strata_data class_level pheno_variant_qc_strata comment "Samples to run variant metrics on (plink include file)"

meta_table file path pheno_variant_qc_strata_pheno_variant_qc_strata_variant_subset_meta_file=@pheno.@pheno_variant_qc_strata.variant_subset.meta dir pheno_variant_qc_stratas_dir disp ".variant_subset.meta" parent cat_pheno_variant_qc_strata_data class_level pheno_variant_qc_strata comment "Load in variant subsets" meta_level pheno_variant_qc_strata_variant_subset

major table file path pheno_variant_qc_pheno_strata_sample_phe_file=@pheno.@pheno_variant_qc_pheno_strata.sample.phe dir pheno_variant_qc_pheno_stratas_dir disp ".sample.phe" parent cat_pheno_variant_qc_pheno_strata_data class_level pheno_variant_qc_pheno_strata comment "Samples to run variant metrics on (plink include file), with phenotype"

meta_table file path pheno_variant_qc_pheno_strata_pheno_variant_qc_pheno_strata_variant_subset_meta_file=@pheno.@pheno_variant_qc_pheno_strata.variant_subset.meta dir pheno_variant_qc_pheno_stratas_dir disp ".variant_subset.meta" parent cat_pheno_variant_qc_pheno_strata_data class_level pheno_variant_qc_pheno_strata comment "Load in variant subsets" meta_level pheno_variant_qc_pheno_strata_variant_subset

!!expand:pheno_variant_qc_strata:pheno_variant_qc_strata:pheno_variant_qc_strata_variant_subset! \
!!expand:type:lmiss:hwe:frq:het! \
major table file path pheno_variant_qc_strata_type_file=@pheno.@pheno_variant_qc_strata.type dir pheno_variant_qc_stratas_dir disp ".type" parent cat_pheno_variant_qc_strata_data class_level pheno_variant_qc_strata comment "Output of --type command"

!!expand:pheno_variant_qc_strata:pheno_variant_qc_strata:pheno_variant_qc_strata_variant_subset! \
!!expand:type:lmiss:hwe:frq:het! \
major table file path pheno_variant_qc_strata_type_log_file=@pheno.@pheno_variant_qc_strata.type.log dir pheno_variant_qc_stratas_dir disp ".type.log" parent cat_pheno_variant_qc_strata_data class_level pheno_variant_qc_strata comment "Log file for output of --type command"

!!expand:pheno_variant_qc_strata:pheno_variant_qc_strata:pheno_variant_qc_strata_variant_subset! \
!!expand:type:lmiss! \
major table file path pheno_variant_qc_strata_unthresholded_type_file=@pheno.@pheno_variant_qc_strata.unthresholded.type dir pheno_variant_qc_stratas_dir disp ".unthresholded.type" parent cat_pheno_variant_qc_strata_data class_level pheno_variant_qc_strata comment "Output of --type command on unthresholded PLINK file"

!!expand:pheno_variant_qc_strata:pheno_variant_qc_strata:pheno_variant_qc_strata_variant_subset! \
!!expand:type:lmiss! \
major table file path pheno_variant_qc_strata_unthresholded_type_log_file=@pheno.@pheno_variant_qc_strata.unthresholded.type.log dir pheno_variant_qc_stratas_dir disp ".unthresholded.type.log" parent cat_pheno_variant_qc_strata_data class_level pheno_variant_qc_strata comment "Log file for output of --type command on unthresholded PLINK file"

!!expand:,:_unthresholded,.unthresholded:_unthresholded,.unthresholded:,! \
!!expand:pheno_variant_qc_strata:pheno_variant_qc_strata:pheno_variant_qc_strata_variant_subset! \
major table file path pheno_variant_qc_strata_multiallelic_unthresholded_stats_file=@pheno.@pheno_variant_qc_strata.multiallelic.unthresholded.stats dir pheno_variant_qc_stratas_dir disp ".unthresholded.multiallelic.stats" parent cat_pheno_variant_qc_strata_data class_level pheno_variant_qc_strata comment "Statistics on multiallelics"

!!expand:pheno_variant_qc_strata:pheno_variant_qc_strata:pheno_variant_qc_strata_variant_subset! \
major table file path pheno_variant_qc_strata_vstats_file=@pheno.@pheno_variant_qc_strata.vstats dir pheno_variant_qc_stratas_dir disp ".vstats" parent cat_pheno_variant_qc_strata_data class_level pheno_variant_qc_strata comment "Summary of variant stats"

!!expand:pheno_variant_qc_pheno_strata:pheno_variant_qc_pheno_strata:pheno_variant_qc_pheno_strata_variant_subset! \
!!expand:type:p_missing! \
major table file path pheno_variant_qc_pheno_strata_type_file=@pheno.@pheno_variant_qc_pheno_strata.type dir pheno_variant_qc_pheno_stratas_dir disp ".type" parent cat_pheno_variant_qc_pheno_strata_data class_level pheno_variant_qc_pheno_strata comment "Output of --type command"

!!expand:pheno_variant_qc_pheno_strata:pheno_variant_qc_pheno_strata:pheno_variant_qc_pheno_strata_variant_subset! \
!!expand:type:p_missing! \
major table file path pheno_variant_qc_pheno_strata_type_log_file=@pheno.@pheno_variant_qc_pheno_strata.type.log dir pheno_variant_qc_pheno_stratas_dir disp ".type.log" parent cat_pheno_variant_qc_pheno_strata_data class_level pheno_variant_qc_pheno_strata comment "Log file for output of --type command"

!!expand:pheno_variant_qc_pheno_strata:pheno_variant_qc_pheno_strata:pheno_variant_qc_pheno_strata_variant_subset! \
!!expand:type:p_missing! \
major table file path pheno_variant_qc_pheno_strata_unthresholded_type_file=@pheno.@pheno_variant_qc_pheno_strata.unthresholded.type dir pheno_variant_qc_pheno_stratas_dir disp ".unthresholded.type" parent cat_pheno_variant_qc_pheno_strata_data class_level pheno_variant_qc_pheno_strata comment "Output of --type command on unthresholded PLINK file"
!!expand:pheno_variant_qc_pheno_strata:pheno_variant_qc_pheno_strata:pheno_variant_qc_pheno_strata_variant_subset! \
!!expand:type:p_missing! \
major table file path pheno_variant_qc_pheno_strata_unthresholded_type_log_file=@pheno.@pheno_variant_qc_pheno_strata.unthresholded.type.log dir pheno_variant_qc_pheno_stratas_dir disp ".unthresholded.type.log" parent cat_pheno_variant_qc_pheno_strata_data class_level pheno_variant_qc_pheno_strata comment "Log file for output of --type command for unthresholded file"

!!expand:,:_unthresholded,.unthresholded:_unthresholded,.unthresholded:,! \
!!expand:pheno_variant_qc_pheno_strata:pheno_variant_qc_pheno_strata:pheno_variant_qc_pheno_strata_variant_subset! \
major table file path pheno_variant_qc_pheno_strata_multiallelic_unthresholded_stats_file=@pheno.@pheno_variant_qc_pheno_strata.multiallelic.unthresholded.stats dir pheno_variant_qc_pheno_stratas_dir disp ".unthresholded.multiallelic.stats" parent cat_pheno_variant_qc_pheno_strata_data class_level pheno_variant_qc_pheno_strata comment "Additional statistics on multiallelics"

!!expand:pheno_variant_qc_pheno_strata:pheno_variant_qc_pheno_strata:pheno_variant_qc_pheno_strata_variant_subset! \
major table file path pheno_variant_qc_pheno_strata_vstats_file=@pheno.@pheno_variant_qc_pheno_strata.vstats dir pheno_variant_qc_pheno_stratas_dir disp ".vstats" parent cat_pheno_variant_qc_pheno_strata_data class_level pheno_variant_qc_pheno_strata comment "Summary of variant stats"

major table file path pheno_qc_filter_trait_exclude_file=@pheno.@pheno_qc_filter.trait.exclude dir pheno_dir disp ".trait.exclude" parent cat_pheno_qc_filter_trait_data class_level pheno_qc_filter comment "Filter samples at pheno level based on a trait"

major table file path pheno_qc_filter_metric_exclude_file=@pheno.@pheno_qc_filter.sstats.exclude dir pheno_dir disp ".metric.exclude" parent cat_pheno_qc_filter_metric_data class_level pheno_qc_filter comment "Filter samples at pheno level based on a metric"

major table file path pheno_qc_filter_exclude_file=@pheno.@pheno_qc_filter.exclude dir pheno_dir disp ".exclude" parent cat_pheno_qc_filter_metric_data class_level pheno_qc_filter comment "Filter samples at pheno level based on intersection of metrics"

!!expand:marker:marker:marker_variant_subset! \
marker_initial_plink_file=@marker.initial dir marker_dir class_level marker
!!expand:,:ext:bed:bim:fam! \
file path marker_initial_ext_file=$marker_initial_plink_file.ext dir marker_dir disp ".initial.ext" parent cat_marker_initial_all_genotype_data class_level marker comment "Marker ext file: specified by user"

file path marker_sample_keep_file=$marker_initial_plink_file.keep dir marker_dir disp ".keep" parent cat_marker_initial_all_genotype_data class_level marker comment "Samples to keep for any use of marker file"
file path marker_snp_exclude_file=$marker_initial_plink_file.exclude dir marker_dir disp ".exclude" parent cat_marker_initial_all_genotype_data class_level marker comment "SNPs to exclude for any use of marker file"

!!expand:marker:marker:marker_variant_subset! \
marker_initial_filtered_plink_file=@marker.initial.filtered dir marker_dir class_level marker
!!expand:,:ext:bed:bim:fam! \
file path marker_initial_filtered_ext_file=$marker_initial_filtered_plink_file.ext dir marker_dir disp ".initial.ext" parent cat_marker_initial_all_genotype_data class_level marker comment "Marker ext file: after any custom filters; used in analyses downstream"
file path marker_initial_filtered_make_bed_log_file=$marker_initial_filtered_plink_file.make_bed.log dir marker_dir disp ".make_bed.log" parent cat_marker_initial_all_genotype_data class_level marker comment "Log file for make bed"

file path marker_strand_discordant_file=$marker_initial_filtered_plink_file.strand.discordant dir marker_dir disp ".strand.discordant" parent cat_marker_initial_all_genotype_data class_level marker comment "SNPs that have discordant alelles between sequence data and marker data"
file path marker_project_non_overlap_file=$marker_initial_filtered_plink_file.project.non_overlap dir marker_dir disp ".project.non_overlap" parent cat_marker_initial_all_genotype_data class_level marker comment "SNPs that are not present in the project VCF file --- used for efficient site discordance checking"
file path marker_initial_corrected_bim_file=$marker_initial_filtered_plink_file.corrected.bim dir marker_dir disp ".initial.corrected.bim" parent cat_marker_initial_all_genotype_data class_level marker comment "Initial marker bim file, with strand corrected to match sequence data"
!!expand:marker:marker:marker_variant_subset! \
file path marker_initial_diff_file=$marker_initial_filtered_plink_file.diff dir marker_dir disp ".diff" parent cat_marker_concordance_data class_level marker comment "Discordant genotypes between sequence and marker"
!!expand:marker:marker:marker_variant_subset! \
file path marker_initial_diff_log_file=$marker_initial_filtered_plink_file.diff.log dir marker_dir disp ".diff.log" parent cat_marker_concordance_data class_level marker comment "Log file for diff cmd"
!!expand:marker:marker:marker_variant_subset! \
file path marker_initial_snp_pct_discordant_file=$marker_initial_filtered_plink_file.snp.pct_discordant dir marker_dir disp ".snp.pct_discordant" parent cat_marker_concordance_data class_level marker comment "For each SNP, list of fraction samples discordant between marker and seq"
file path marker_snp_discordant_file=$marker_initial_filtered_plink_file.snp.discordant dir marker_dir disp ".snp.discordant" parent cat_marker_initial_all_genotype_data class_level marker comment "SNPs that have high discordances between marker and seq data"
file path marker_snp_flipped_file=$marker_initial_filtered_plink_file.snp.flipped dir marker_dir disp ".snp.flipped" parent cat_marker_initial_all_genotype_data class_level marker comment "SNPs that appear to be flipped (discordances near zero)"
file path marker_initial_snp_corrected_bim_file=$marker_initial_filtered_plink_file.snp_corrected.bim dir marker_dir disp ".initial.snp_corrected.bim" parent cat_marker_initial_all_genotype_data class_level marker comment "Initial marker bim file, with SNPs corrected to reduce seq/marker discordances"
marker_filtered_plink_file=@marker.filtered dir marker_dir class_level marker
!!expand:,:ext:bed:bim:fam! \
file path marker_filtered_ext_file=$marker_filtered_plink_file.ext dir marker_dir disp ".filtered.ext" parent cat_marker_filtered_all_genotype_data class_level marker comment "Filtered marker ext file"
file path marker_filtered_make_bed_log_file=$marker_filtered_plink_file.make_bed.log dir marker_dir disp ".make_bed.log" parent cat_marker_filtered_all_genotype_data class_level marker comment "Log file for make bed cmd"
file path marker_project_sample_include_file=@marker.project.sample.list dir marker_dir disp ".project.sample.list" parent cat_marker_sample_genotype_data class_level marker comment "List of marker samples in the project who are also in sample keep file"

file path marker_filtered_frq_file=$marker_filtered_plink_file.frq dir marker_dir disp ".filtered.frq" parent cat_marker_filtered_all_genotype_data class_level marker comment "Filtered marker frq file"
file path marker_filtered_frq_log_file=$marker_filtered_plink_file.frq.log dir marker_dir disp ".frq.log" parent cat_marker_filtered_all_genotype_data class_level marker comment "Log file for frq cmd"

marker_filtered_for_merge_plink_file=@marker.filtered.for_merge dir marker_dir class_level marker
file path marker_filtered_for_merge_exclude_file=$marker_filtered_for_merge_plink_file.exclude dir marker_dir disp ".filtered.for_merge.exclude" parent cat_marker_filtered_for_merge_all_genotype_data class_level marker comment "SNPs to remove for merging (SNPs with contrasting alleles removed)"
file path marker_filtered_for_merge_initial_bim_file=$marker_filtered_for_merge_plink_file.initial.bim dir marker_dir disp ".initial.bim" parent cat_marker_filtered_for_merge_all_genotype_data class_level marker comment "Same SNPs as filtered, but with IDs/ref/alt adjusted"

!!expand:,:ext:bed:bim:fam! \
file path marker_filtered_for_merge_ext_file=$marker_filtered_for_merge_plink_file.ext dir marker_dir disp ".filtered.for_merge.ext" parent cat_marker_filtered_for_merge_all_genotype_data class_level marker comment "Filtered marker ext file (for merging: ambiguous SNPs removed)"
file path marker_filtered_for_merge_make_bed_log_file=$marker_filtered_for_merge_plink_file.make_bed.log dir marker_dir disp ".make_bed.log" parent cat_marker_filtered_for_merge_all_genotype_data class_level marker comment "Log file for make bed cmd"

#file path marker_filtered_for_merge_ld_file=$marker_filtered_for_merge_plink_file.ld dir marker_dir disp ".filtered.for_merge.ld" parent cat_marker_filtered_for_merge_all_genotype_data class_level marker comment "Filtered marker ld file"
#file path marker_filtered_for_merge_ld_log_file=$marker_filtered_for_merge_plink_file.ld.log dir marker_dir disp ".ld.log" parent cat_marker_filtered_for_merge_all_genotype_data class_level marker comment "Log file for ld cmd"

marker_sample_strand_filtered_overlap_plink_file=@marker.sample.strand.filtered.overlap dir marker_dir class_level marker
!!expand:,:ext:bed:bim:fam! \
file path marker_sample_strand_filtered_overlap_ext_file=$marker_sample_strand_filtered_overlap_plink_file.ext dir marker_dir disp ".sample.strand.filtered.overlap.ext" parent cat_marker_sample_genotype_data class_level marker comment "Plink marker ext file for project samples; removed discordant strand SNPs"
file path marker_sample_strand_filtered_overlap_make_bed_log_file=$marker_sample_strand_filtered_overlap_plink_file.make_bed.log dir marker_dir disp ".make_bed.log" parent cat_marker_sample_genotype_data class_level marker comment "Log file for make bed cmd"
!!expand:marker:marker:marker_variant_subset! \

marker_sample_filtered_plink_file=@marker.sample.filtered dir marker_dir class_level marker
file path marker_sample_filtered_snp_include_file=$marker_sample_filtered_plink_file.snp.include dir marker_dir disp ".sample.filtered.snp.include" parent cat_marker_sample_genotype_data class_level marker comment "List of SNPs to go into making VCF file"
file path marker_sample_filtered_vcf_file=$marker_sample_filtered_plink_file.vcf dir marker_dir disp ".sample.filtered.vcf" parent cat_marker_sample_genotype_data class_level marker comment "Filtered marker VCF file for project samples only; removed discordant strand SNPs and discordant SNPs"
file path marker_sample_filtered_recode_vcf_log_file=$marker_sample_filtered_plink_file.recode_vcf.log dir marker_dir disp ".sample.recode_vcf.log" parent cat_marker_sample_genotype_data class_level marker comment "Log file for recode vcf cmd for project samples only"
file path marker_sample_filtered_fixed_vcf_file=$marker_sample_filtered_plink_file.fixed.vcf dir marker_dir disp ".sample.filtered.fixed.vcf" parent cat_marker_sample_genotype_data class_level marker comment "Fixed IDs in VCF file"
!!expand:marker:marker:marker_sample_subset! \
major file path marker_positive_control_eval_file=@marker.positive_control.eval dir marker_dir disp ".eval" parent cat_marker_concordance_data class_level marker comment "Eval statistics on calls"

table file path marker_positive_control_genotype_concordance_file=@marker.positive_control.genotype.concordance.csv dir marker_dir disp ".csv" parent cat_marker_concordance_data class_level marker comment "Reformatted plink diff file"

marker_positive_control_genotype_concordance_sum_trunk=@marker.positive_control.genotype.marker.sum

file path marker_positive_control_genotype_concordance_sum_tex_file=$marker_positive_control_genotype_concordance_sum_trunk.tex dir marker_dir disp ".sum.tex" parent cat_marker_concordance_data trunk $marker_positive_control_genotype_concordance_sum_trunk class_level marker comment "Summary of overall marker for all samples --- tex file"

major file path marker_positive_control_genotype_concordance_sum_pdf_file=$marker_positive_control_genotype_concordance_sum_trunk.pdf dir marker_dir disp ".sum.pdf" parent cat_marker_concordance_data class_level marker comment "Summary of overall marker for all samples --- pdf file"

table file path marker_sample_concordance_file=@marker.sample.concordance.tsv dir marker_dir disp ".concordance.tsv" parent cat_marker_concordance_data class_level marker comment "Overall concordance for each sample"

table file path marker_sample_full_concordance_file=@marker.sample.full.marker.tsv dir marker_dir disp ".full.concordance.tsv" parent cat_marker_concordance_data class_level marker comment "Concordance for all samples --- NA inserted for those missing"

table nohead file path marker_sample_exclude_detail_file=@marker.sample.exclude.detail dir marker_dir disp ".sample.exclude.detail" parent cat_marker_concordance_data class_level marker comment "Samples excluded due to low concordances"

!!expand:stype:sample:variant! \
meta_table file path marker_marker_stype_subset_meta_file=@marker.marker_stype_subset.meta dir marker_dir disp ".marker_stype_subset.meta" parent cat_marker_meta_data class_level marker comment "Meta file to load in marker_stype_subsets" meta_level marker_stype_subset

!!expand:,:project,type:project,all:project,sample:project_sample_subset,sample:project,extra! \
project_type_marker_plink_file=@project.type.marker dir project_dir class_level project

file path project_all_marker_all_merge_list_file=@project.all.marker.all.merge.list dir project_dir disp ".all.merge.list" parent cat_project_plink_all_marker_meta_data class_level project comment "List of all marker files"

file path project_all_marker_plink_list_file=@project.all.marker.plink.list dir project_dir disp ".plink.list" parent cat_project_plink_all_marker_meta_data class_level project comment "List of plink trunks of marker files for merging --- have this to make sure the first file passed into the merge is always the first file in the merge list"

file path project_all_marker_merge_list_file=@project.all.marker.merge.list dir project_dir disp ".merge.list" parent cat_project_plink_all_marker_meta_data class_level project comment "List of marker files for merging"

!!expand:,:type,description:all,all:sample,project:extra,non-project! \
!!expand:,:fileext:bed:bim:fam! \
file path project_type_marker_fileext_file=$project_type_marker_plink_file.fileext dir project_dir disp ".fileext" parent cat_project_plink_type_marker_ped_data class_level project comment "Marker fileext file for description samples"

!!expand:,:type:all:sample:extra! \
file path project_type_marker_make_bed_log_file=$project_type_marker_plink_file.make_bed.log dir project_dir disp ".make_bed.log" parent cat_project_plink_type_marker_ped_data class_level project comment "Log file for make bed cmd"

project_extra_marker_pruned_plink_file=@project.extra.marker.pruned dir project_dir class_level project

!!expand:,:fileext:bed:bim:fam! \
file path project_extra_marker_pruned_fileext_file=$project_extra_marker_pruned_plink_file.fileext dir project_dir disp ".fileext" parent cat_project_plink_extra_marker_ped_data class_level project comment "Ready for merging for for_pca file (if add_for_pca)"

file path project_extra_marker_pruned_make_bed_log_file=$project_extra_marker_pruned_plink_file.make_bed.log dir project_dir disp ".make_bed.log" parent cat_project_plink_extra_marker_ped_data class_level project comment "Log file for make bed cmd"

#file path project_all_marker_ld_file=$project_all_marker_plink_file.ld dir project_dir disp ".ld" parent cat_project_plink_all_marker_meta_data class_level project comment "R2 values for all marker SNPs after merge"
#file path project_all_marker_ld_log_file=$project_all_marker_plink_file.ld.log dir project_dir disp ".ld.log" parent cat_project_plink_all_marker_meta_data class_level project comment "Log file for R2 calculation"

project_sample_marker_pruned_plink_file=@project.sample.marker.pruned dir project_dir class_level project
!!expand:,:fileext:bed:bim:fam! \
file path project_sample_marker_pruned_fileext_file=$project_sample_marker_pruned_plink_file.fileext dir project_dir disp ".pruned.fileext" parent cat_project_plink_sample_marker_pruned_ped_data class_level project comment "Marker fileext file for project samples, LD pruned"

file path project_sample_marker_pruned_make_bed_log_file=$project_sample_marker_plink_file.pruned.make_bed.log dir project_dir disp ".pruned.make_bed.log" parent cat_project_plink_sample_marker_pruned_ped_data class_level project comment "Log file for make bed cmd"

!!expand:,:keyt,extt,descript:initial_,initial.,-- individuals not necessarily sorted:,,! \
file path project_sample_marker_pruned_keytvcf_file=$project_sample_marker_pruned_plink_file.exttvcf.gz dir project_dir disp ".pruned.exttvcf.gz" parent cat_project_plink_sample_marker_pruned_ped_data class_level project comment "Marker vcf file for project samples, LD pruned descript"

file path project_sample_marker_pruned_recode_vcf_log_file=$project_sample_marker_pruned_plink_file.pruned.recode_vcf.log dir project_dir disp ".pruned.recode_vcf.log" parent cat_project_plink_sample_marker_pruned_ped_data class_level project comment "Log file for make bed cmd"

project_sample_for_pca_plink_file=@project.sample.for.pca dir project_dir class_level project

!!expand:,:fileext:bed:bim:fam! \
file path project_sample_for_pca_fileext_file=$project_sample_for_pca_plink_file.fileext dir project_dir disp ".for_pca.fileext" parent cat_project_plink_sample_marker_pruned_ped_data class_level project comment "Marker fileext file for project samples, LD pruned"

file path project_sample_for_pca_make_bed_log_file=$project_sample_for_pca_plink_file.pruned.make_bed.log dir project_dir disp ".for_pca.make_bed.log" parent cat_project_plink_sample_marker_pruned_ped_data class_level project comment "Log file for make bed cmd"


!!expand:,:num:1:2! \
file path project_sample_subset_sample_marker_genome_listnum_file=$project_sample_subset_sample_marker_plink_file.genome.listnum dir project_sample_subset_dir disp ".genome.listnum" parent cat_project_sample_subset_sample_plink_ibd_data class_level project_sample_subset comment "List of samples to run --genome for"

file path project_sample_marker_seq_snp_file=@project.sample.marker.seq.snps dir project_dir disp ".marker.seq.snps" parent cat_project_plink_sample_non_seq_meta_data class_level project comment "SNPs in both marker file and sequence data"

project_sample_non_seq_plink_file=@project.sample.non_seq dir project_dir class_level project
!!expand:,:fileext:bed:bim:fam! \
file path project_sample_non_seq_fileext_file=$project_sample_non_seq_plink_file.fileext dir project_dir disp ".fileext" parent cat_project_plink_sample_non_seq_ped_data class_level project comment "Marker + Seq fileext file for project samples"
file path project_sample_non_seq_make_bed_log_file=$project_sample_non_seq_plink_file.make_bed.log dir project_dir disp ".make_bed.log" parent cat_project_plink_sample_non_seq_ped_data class_level project comment "Marker + Seq make_bed log file"

project_sample_combined_plink_file=@project.sample.combined dir project_dir class_level project
!!expand:,:fileext:bed:bim:fam! \
file path project_sample_combined_fileext_file=$project_sample_combined_plink_file.fileext dir project_dir disp ".fileext" parent cat_project_plink_sample_combined_ped_data class_level project comment "Marker + Seq fileext file for project samples"

file path project_sample_combined_merge_log_file=$project_sample_combined_plink_file.merge.log dir project_dir disp ".merge.log" parent cat_project_plink_sample_combined_ped_data class_level project comment "Marker + Seq merge log file"

#file path project_all_marker_sample_keep_file=$project_all_marker_plink_file.keep dir project_dir disp ".keep" parent cat_project_all_marker_data class_level project comment "Samples to keep for any use of marker file"

#file path project_all_marker_snp_exclude_file=$project_all_marker_plink_file.exclude dir project_dir disp ".exclude" parent cat_project_all_marker_data class_level project comment "SNPs to exclude for any use of marker file"

file path project_all_marker_pheno_file=$project_all_marker_plink_file.pheno.tsv dir project_dir disp ".pheno.tsv" parent cat_project_plink_all_marker_meta_data class_level project comment "Phenotype file for all samples in the marker file"

file table path project_all_marker_assoc_covars_file=$project_all_marker_plink_file.assoc.covars dir project_dir disp ".assoc.covars.tsv" parent cat_project_plink_all_marker_meta_data class_level project comment "Covariates for any association testing in all marker file"

file path project_all_marker_assoc_sample_keep_file=$project_all_marker_plink_file.assoc.keep dir project_dir disp ".assoc.keep" parent cat_project_plink_all_marker_meta_data class_level project comment "Samples to keep for any association testing in all marker file"

file path project_sample_marker_input_frq_file=$project_sample_marker_plink_file.input.frq dir project_dir disp ".input.frq" parent cat_project_sample_plink_ibd_data class_level project comment "Use these frequencies for sites to go into all marker calculations"

!!expand:,:allorsample:all:sample! \
file path project_allorsample_marker_ld_prune_restrict_file=$project_allorsample_marker_plink_file.prune.restrict dir project_dir disp ".prune.restrict" parent cat_project_allorsample_plink_ibd_data class_level project comment "Restrict LD pruning to these sites"

!!expand:,:allorsample:all:sample! \
!!expand:,:type:in:in_range:out:log! \
file path project_allorsample_marker_ld_prune_type_file=$project_allorsample_marker_plink_file.prune.type dir project_dir disp ".prune.type" parent cat_project_allorsample_plink_ibd_data class_level project comment "Result of LD pruning --- prune.type file"

file path project_sample_marker_keep_file=$project_sample_marker_plink_file.keep dir project_dir disp ".keep" parent cat_project_plink_sample_marker_meta_data class_level project comment "List sequenced individuals to keep in the sample marker file"

table file path project_sample_marker_frq_file=$project_sample_marker_plink_file.frq dir project_dir disp ".frq" parent cat_project_sample_plink_ibd_data class_level project comment "Frequencies for all variants from marker file"

minor file path project_sample_marker_frq_log_file=$project_sample_marker_frq_file.log dir project_dir disp ".frq.log" parent cat_project_sample_plink_ibd_data class_level project comment "Log file for frq command"

!!expand:,:project,type,description:project,all,all:project,sample,project:project_sample_subset,sample,project_sample_subset! \
table file path project_type_marker_genome_file=$project_type_marker_plink_file.genome.gz dir project_dir disp ".genome" parent cat_project_type_plink_ibd_data class_level project comment "Genome file for description samples"
!!expand:,:project,type,description:project,all,all:project,sample,project:project_sample_subset,sample,project_sample_subset! \
file path project_type_marker_genome_log_file=$project_type_marker_plink_file.genome.log dir project_dir disp ".genome.log" parent cat_project_type_plink_ibd_data class_level project comment "Log file for genome cmd"

table nohead path file project_duplicate_exclude_custom_rank_file=@project.duplicate.exclude.custom.rank dir project_dir disp ".duplicate.exclude.custom.rank" parent cat_project_sample_plink_ibd_data class_level project comment "Custom rank samples to exclude: higher number means preferentially include"

table file path project_duplicate_exclude_rank_file=@project.duplicate.exclude.rank dir project_dir disp ".duplicate.exclude.rank" parent cat_project_sample_plink_ibd_data class_level project comment "Rank samples to include based on duplicates"

table file path project_duplicate_exclude_file=@project.duplicate.exclude dir project_dir disp ".duplicate.exclude" parent cat_project_sample_plink_ibd_data class_level project comment "Samples excluded due to outliers according to duplicate"

mkdir path project_epacts_dir=$project_dir/epacts class_level project chmod 777
"""
}
    