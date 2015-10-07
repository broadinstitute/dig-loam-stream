
package loamstream.compiler

/**
 * LoamStream
 * Created by oliverr on 10/6/2015.
 */
object PipelineTargetedPart05 {
  val string =
 """#minor file path project_plinkseq_samtools_clean_db_done_file=db.done dir project_plinkseq_samtools_clean_project_out_dir disp "db.done" parent cat_project_plinkseq_samtools_clean_project_data class_level project comment "Signifies that samtools clean db was done"


!!expand:qctype:qc_pass:qc_plus! \
file path project_qctype_all_merge_list_file=@project.qctype.all.merge.list dir project_dir disp ".all.merge.list" parent cat_project_plink_seq_qctype_data class_level project comment "List of all marker files"

!!expand:qctype:qc_pass:qc_plus! \
file path project_qctype_plink_list_file=@project.qctype.plink.list dir project_dir disp ".plink.list" parent cat_project_plink_seq_qctype_data class_level project comment "List of plink trunks of marker files for merging --- have this to make sure the first file passed into the merge is always the first file in the merge list"

!!expand:qctype:qc_pass:qc_plus! \
file path project_qctype_merge_list_file=@project.qctype.merge.list dir project_dir disp ".merge.list" parent cat_project_plink_seq_qctype_data class_level project comment "List of marker files for merging"

!!expand:project:project:project_sample_subset:project_variant_subset! \
!!expand:qctype:qc_pass:qc_plus! \
path project_plinkseq_qctype_plink_file=@project.qctype dir project_dir

!!expand:project:project:project_sample_subset:project_variant_subset! \
!!expand:qctype:qc_pass:qc_plus! \
!!expand:,:keytype,ext:tped,tped:tfam,tfam:renamed_tfam,renamed.tfam:bed,bed:bim,bim:fam,fam:with_sex_fam,with.sex.fam! \
file path project_plinkseq_qctype_keytype_file=@project.qctype.ext dir project_dir disp ".ext" parent cat_project_plink_seq_qctype_data class_level project comment "ext file format for qctype vcf file"

!!expand:project:project:project_sample_subset:project_variant_subset! \
!!expand:qctype:qc_pass:qc_plus! \
minor file path project_plinkseq_qctype_make_bed_log_file=@project.qctype.make_bed.log dir project_dir disp ".make_bed.log" parent cat_project_plink_seq_qctype_data class_level project comment "Log file for make-bed command"


!!expand:project:project:project_variant_subset! \
path project_plinkseq_qc_pass_unthresholded_plink_file=@project.qc_pass.unthresholded dir project_dir

!!expand:project:project:project_variant_subset! \
!!expand:,:keytype,ext:tped,tped:tfam,tfam:renamed_tfam,renamed.tfam:bed,bed:bim,bim:fam,fam:with_sex_fam,with.sex.fam! \
file path project_plinkseq_qc_pass_unthresholded_keytype_file=@project.qc_pass.unthresholded.ext dir project_dir disp ".unthresholded.ext" parent cat_project_plink_seq_qc_pass_data class_level project comment "ext file format for qc_pass vcf file; no threshold based on genotype quality"

!!expand:project:project:project_variant_subset! \
minor file path project_plinkseq_qc_pass_unthresholded_make_bed_log_file=@project.qc_pass.unthresholded.make_bed.log dir project_dir disp ".unthresholded.make_bed.log" parent cat_project_plink_seq_qc_pass_data class_level project comment "Log file for make-bed command for unthresholded file"



table file path project_seq_iid_fid_map_file=@project.seq.iid.fid.map dir project_dir disp ".iid.fid.map" parent cat_project_plink_seq_meta_data class_level project comment "Map from iid to fid for all samples with sequence data in the project"

!!expand:project:project:project_variant_subset! \
!!expand:qctype:qc_pass:qc_plus! \
table file path project_plinkseq_qctype_frq_file=$project_plinkseq_qctype_plink_file.frq dir project_dir disp ".frq" parent cat_project_plink_qctype_output_data class_level project comment "Frequency for all samples computed based on qctype plink file"

!!expand:qctype:qc_pass:qc_plus! \
!!expand:project:project:project_variant_subset! \
minor file path project_plinkseq_qctype_frq_log_file=$project_plinkseq_qctype_frq_file.log dir project_dir disp ".frq.log" parent cat_project_plink_qctype_output_data class_level project comment "Log file for frq command"

!!expand:project:project:project_variant_subset! \
table file path project_multiallelic_frq_file=@project.multiallelic.frq dir project_dir disp ".multiallelic.frq" parent cat_project_plink_qc_pass_output_data class_level project comment "Frequency for QC+ samples computed based on QC pass plink file at multialellic variants"

!!expand:qctype:qc_pass:qc_plus! \
!!expand:project:project:project_variant_subset! \
table file path project_plinkseq_qctype_combined_frq_file=$project_plinkseq_qctype_plink_file.combined.frq dir project_dir disp ".combined.frq" parent cat_project_plink_qctype_output_data class_level project comment "Combined frq file for single variants and multiallelics"


!!expand:qc_plus:qc_plus:qc_pass! \
!!expand:project:project:project_variant_subset! \
table file path project_plinkseq_qc_plus_strata_frq_file=$project_plinkseq_qc_plus_plink_file.strata.frq dir project_dir disp ".strata.frq" parent cat_project_plink_qc_plus_output_data class_level project comment "Frequency for all samples computed based on qc_plus plink file, stratified by specified maf_strata_trait"

!!expand:qc_plus:qc_plus:qc_pass! \
!!expand:project:project:project_variant_subset! \
minor file path project_plinkseq_qc_plus_strata_frq_log_file=$project_plinkseq_qc_plus_strata_frq_file.log dir project_dir disp ".strata.frq.log" parent cat_project_plink_qc_plus_output_data class_level project comment "Log file for strata frq command"

table file path project_multiallelic_strata_frq_file=@project.multiallelic.strata.frq dir project_dir disp ".multiallelic.strata.frq" parent cat_project_plink_qc_pass_output_data class_level project comment "Frequency for QC+ samples computed based on qc_pass plink file, stratified by specified maf_strata_trait"

!!expand:qc_plus:qc_plus:qc_pass! \
!!expand:project:project:project_variant_subset! \
table file path project_plinkseq_qc_plus_combined_strata_frq_file=$project_plinkseq_qc_plus_plink_file.combined.strata.frq dir project_dir disp ".combined.strata.frq" parent cat_project_plink_qc_plus_output_data class_level project comment "Combined strata.frq files for single variants and multialellics"

!!expand:qc_plus:qc_plus:qc_pass! \
!!expand:project:project:project_variant_subset! \
table file path project_plinkseq_qc_plus_strata_frq_summary_file=$project_plinkseq_qc_plus_plink_file.strata.frq.summary dir project_dir disp ".qc_plus.strata.frq.summary" parent cat_project_plink_qc_plus_output_data class_level project comment "Summaries of frequencies across strata"

!!expand:project:project:project_variant_subset! \
minor file path project_plinkseq_qc_plus_frq_log_file=$project_plinkseq_qc_plus_frq_file.log dir project_dir disp ".frq.log" parent cat_project_plink_qc_plus_output_data class_level project comment "Log file for frq command"

!!expand:project:project:project_variant_subset! \
!!expand:qctype:qc_pass:qc_plus! \
table file path project_plinkseq_qctype_counts_file=$project_plinkseq_qctype_plink_file.counts dir project_dir disp ".counts" parent cat_project_plink_qctype_output_data class_level project comment "Minor allele counts for all samples computed based on qctype plink file"

!!expand:qctype:qc_pass:qc_plus! \
table file path project_plinkseq_qctype_sing_reg_list_file=$project_plinkseq_qctype_plink_file.sing.reg.list dir project_dir disp ".sing.reg.list" parent cat_project_plink_qctype_output_data class_level project comment "Singleton variants"

table file path project_plinkseq_qc_pass_doub_reg_list_file=$project_plinkseq_qc_pass_plink_file.doub.reg.list dir project_dir disp ".doub.reg.list" parent cat_project_plink_qc_pass_output_data class_level project comment "Doubleton variants"

!!expand:,:_frequency,.frequency,Frequency:,,all:_high,.high,common:_low,.low,low frequency! \
!!expand:project:project:project_sample_subset! \
table file path project_plinkseq_frequency_het_file=$project_plinkseq_qc_pass_plink_file.frequency.het dir project_dir disp ".frequency.het" parent cat_project_plink_output_data class_level project comment "Heterozygosity for all samples computed based on qc_pass plink file (Frequency SNPs)"

!!expand:,:_frequency,.frequency,Frequency:,,all:_high,.high,common! \
!!expand:project:project:project_sample_subset! \
minor file path project_plinkseq_frequency_het_log_file=$project_plinkseq_frequency_het_file.log dir project_dir disp ".frequency.het.log" parent cat_project_plink_output_data class_level project comment "Log file for het command for Frequency SNPs"

!!expand:project:project:project_sample_subset! \
table file path project_plinkseq_sexcheck_file=$project_plinkseq_qc_pass_plink_file.sexcheck dir project_dir disp ".sexcheck" parent cat_project_plink_output_data class_level project comment "Sex check for all samples computed based on qc_pass plink file"

!!expand:project:project:project_sample_subset! \
minor file path project_plinkseq_sexcheck_log_file=$project_plinkseq_sexcheck_file.log dir project_dir disp ".sexcheck.log" parent cat_project_plink_output_data class_level project comment "Log file for sex check command"

!!expand:project:project:project_sample_subset! \
!!expand:,:maskkey,masktype,maskdescrip:all,,all:syn,syn.,synonymous:ns,ns.,non-synonymous:nonsense,nonsense.,nonsense:noncoding,noncoding.,noncoding! \
!!expand:,:tname,description:raw,were called:qc_pass,pass filters:qc_plus,pass QC! \ 
 table file path project_plinkseq_tname_maskkey_istats_file=@project.tname.masktypeistats dir project_dir disp ".tname.masktypeistats" parent cat_project_tname_istats_data class_level project comment "Dumped by Plink/Seq i-stats using maskdescrip variants that description"

!!expand:project:project:project_sample_subset! \
!!expand:,:tname,description:qc_pass,pass filters:qc_plus,pass QC! \ 
table file path project_plinkseq_tname_sing_istats_file=@project.tname.sing.istats dir project_dir disp ".tname.sing.istats" parent cat_project_tname_istats_data class_level project comment "Dumped by Plink/Seq i-stats using singleton variants that description"

!!expand:project:project:project_sample_subset! \
 table file path project_plinkseq_qc_pass_doub_istats_file=@project.qc_pass.doub.istats dir project_dir disp ".qc_pass.doub" parent cat_project_qc_pass_istats_data class_level project comment "Dumped by Plink/Seq i-stats using doubleton variants that pass filters"

!!expand:project:project:project_sample_subset! \
 table file path project_plinkseq_qc_pass_snp_istats_file=@project.qc_pass.snp.istats dir project_dir disp ".qc_pass.snp" parent cat_project_qc_pass_istats_data class_level project comment "Dumped by Plink/Seq i-stats using SNP variants that pass filters"

!!expand:project:project:project_sample_subset! \
 table file path project_plinkseq_qc_pass_indel_istats_file=@project.qc_pass.indel.istats dir project_dir disp ".qc_pass.indel" parent cat_project_qc_pass_istats_data class_level project comment "Dumped by Plink/Seq i-stats using indel variants that pass filters"

!!expand:project:project:project_sample_subset! \
 table file path project_plinkseq_qc_pass_multiallelic_istats_file=@project.qc_pass.multiallelic.istats dir project_dir disp ".qc_pass.multiallelic" parent cat_project_qc_pass_istats_data class_level project comment "Dumped by Plink/Seq i-stats using multiallelic variants that pass filters"

!!expand:project:project:project_sample_subset! \
table file path project_plinkseq_filtered_istats_file=@project.filtered.istats dir project_dir disp ".filtered.istats" parent cat_project_qc_ind_stats_data class_level project comment "Dumped by Plink/Seq i-stats using variants that fail filters"

!!expand:,:type:snps:indels:multiallelics! \
!!expand:,:project,cattouse:project,cat_project_qc_var_stats_data:project_variant_subset,cat_project_variant_subset_qc_var_stats_data! \
table file path project_plinkseq_type_vmatrix_file=@project.type.vmatrix dir project_dir disp ".type.vmatrix" parent cattouse class_level project comment "Dumped by Plink/Seq v-matrix using type that pass filters; no quality threshold"

!!expand:project:project:project_sample_subset! \
!!expand:metatype:gq:dp:ab:sing_ab! \
table file path project_plinkseq_metatype_vmetamatrix_file=@project.metatype.vmetamatrix dir project_dir disp ".metatype.vmetamatrix" parent cat_project_qc_ind_stats_data class_level project comment "Dumped by Plink/Seq v-meta-matrix --name metatype using variants that pass filters"

!!expand:project:project:project_sample_subset! \
!!expand:metatype:dp! \
table file path project_plinkseq_metatype_qcfail_vmetamatrix_file=@project.metatype.qcfail.vmetamatrix dir project_dir disp ".metatype.qcfail.vmetamatrix" parent cat_project_qc_ind_stats_data class_level project comment "Dumped by Plink/Seq v-meta-matrix --name metatype using variants that failed filters"

!!expand:project:project:project_sample_subset! \
table file path project_plinkseq_qcfail_vmatrix_file=@project.qcfail.vmatrix dir project_dir disp ".qcfail.vmatrix" parent cat_project_qc_ind_stats_data class_level project comment "Dumped by Plink/Seq v-matrix using variants that fail filters"

!!expand:project:project:project_sample_subset! \
table file path project_plinkseq_extra_istats_file=@project.extra.istats dir project_dir disp ".extra.istats" parent cat_project_qc_ind_stats_data class_level project comment "Additional sample statistics to append to i-stats"

table file project_sample_custom_exclude_file=@project.sample.custom.exclude dir project_dir disp ".sample.custom.exclude" parent cat_project_qc_ind_stats_data class_level project comment "Custom list of samples to exclude"

#table file path project_unfiltered_marker_istats_file=@project.unfiltered.marker.istats dir project_dir disp ".marker.istats" parent cat_project_qc_ind_stats_data class_level project comment "Sample statistics based on marker file --- no samples removed"

major table file path project_sstats_file=@project.sstats dir project_dir disp ".sstats" parent cat_project_qc_ind_stats_data class_level project comment "All relevant sample statistics"

major table file path project_traits_file=@project.traits dir project_dir disp ".traits" parent cat_project_qc_ind_stats_data class_level project comment "All relevant sample traits"

major table file path project_sample_outlier_file=@project.sample.outliers.csv dir project_dir disp ".outliers.csv" parent cat_project_sample_qc_filter_data class_level project comment "Outlier values from all sample statistics"

major file path project_sstats_initial_pdf_file=@project.sstats.initial.pdf dir project_dir disp ".sstats.pdf" parent cat_project_qc_ind_plots_data class_level project comment "Plot of various sample metrics across all samples --- hist plots"
major file path project_sstats_highlighted_pdf_file=@project.sstats.highlighted.pdf dir project_dir disp ".highlighted.pdf" parent cat_project_qc_ind_plots_data class_level project comment "Plot of various sample metrics across all samples with outlier samples highlighted --- hist plot"

major file path project_sstats_final_pdf_file=@project.sstats.final.pdf dir project_dir disp ".final.pdf" parent cat_project_qc_ind_plots_data class_level project comment "Plot of various sample metrics across all samples with outlier samples removed --- hist plot"

!!expand:ext:ps:pdf! \
major file path project_pheno_sstats_initial_ext_file=@project.pheno.sstats.initial.ext dir project_dir disp ".pheno.sstats.ext" parent cat_project_qc_ind_plots_data class_level project comment "Plot of various sample metrics across all samples stratified by phenotype --- hist plots"

!!expand:ext:ps:pdf! \
major file path project_pheno_sstats_highlighted_ext_file=@project.pheno.sstats.highlighted.ext dir project_dir disp ".pheno.highlighted.ext" parent cat_project_qc_ind_plots_data class_level project comment "Plot of various sample metrics across all samples stratified by phenotype with outlier samples highlighted --- hist plot"

!!expand:ext:ps:pdf! \
major file path project_pheno_sstats_final_ext_file=@project.pheno.sstats.final.ext dir project_dir disp ".pheno.final.ext" parent cat_project_qc_ind_plots_data class_level project comment "Plot of various sample metrics across all samples stratified by phenotype with outlier samples removed --- hist plot"

major file path project_sstats_extreme_pdf_file=@project.sstats.extreme.pdf dir project_dir disp ".extreme.pdf" parent cat_project_qc_ind_plots_data class_level project comment "Plot of various sample metrics across all samples with outlier samples removed --- highlighting extreme samples"

table file path project_sample_exclude_detail_file=@project.sample.exclude.detail dir project_dir disp ".sample.exclude.detail" parent cat_project_sample_qc_filter_data class_level project comment "Samples excluded because they are outliers on one or more measures --- with information about the metrics that caused them to fail"

!!expand:type:seq! \
file path project_sample_type_exclude_file=@project.sample.type.exclude dir project_dir disp ".sample.type.exclude" parent cat_project_sample_qc_filter_data class_level project comment "Samples excluded because they are outliers on one or more type measures"

file path project_sample_exclude_file=@project.sample.exclude dir project_dir disp ".sample.exclude" parent cat_project_sample_qc_filter_data class_level project comment "Samples excluded because they are outliers on one or more seq measures"

file path project_sample_include_file=@project.sample.include dir project_dir disp ".sample.include" parent cat_project_sample_qc_filter_data class_level project comment "Samples includeded because they are outliers on zero measures"

file path project_plink_sample_exclude_file=@project.plink.sample.exclude dir project_dir disp ".plink.sample.exclude" parent cat_project_sample_qc_filter_data class_level project comment "Samples excluded augmented with family ids in the plink tfam file"

project_slide_vcounts_trunk=@project.slide.vcounts
file path project_slide_vcounts_tex_file=$project_slide_vcounts_trunk.tex dir project_dir disp ".vcounts.tex" parent cat_project_slide_sites_data class_level project trunk $project_slide_vcounts_trunk comment "Bulk number of variant sites -- tex file"
file path project_slide_vcounts_pdf_file=$project_slide_vcounts_trunk.pdf dir project_dir disp ".vcounts.pdf" parent cat_project_slide_sites_data class_level project comment "Bulk number of variant sites -- pdf file"

project_slide_var_pass_counts_trunk=@project.slide.var.pass.counts
file path project_slide_var_pass_counts_tex_file=$project_slide_var_pass_counts_trunk.tex dir project_dir disp ".var.pass.counts.tex" parent cat_project_slide_sites_data class_level project trunk $project_slide_var_pass_counts_trunk comment "Number of variants passing various filters -- tex file"
file path project_slide_var_pass_counts_pdf_file=$project_slide_var_pass_counts_trunk.pdf dir project_dir disp ".var.pass.counts.pdf" parent cat_project_slide_sites_data class_level project comment "Number of variants passing various filters -- pdf file"

project_slide_sample_var_pass_counts_trunk=@project.slide.sample.var.pass.counts
file path project_slide_sample_var_pass_counts_tex_file=$project_slide_sample_var_pass_counts_trunk.tex dir project_dir disp ".sample.var.pass.counts.tex" parent cat_project_slide_sites_data class_level project trunk $project_slide_sample_var_pass_counts_trunk comment "Number of variants per sample passing various filters -- tex file"
file path project_slide_sample_var_pass_counts_pdf_file=$project_slide_sample_var_pass_counts_trunk.pdf dir project_dir disp ".sample.var.pass.counts.pdf" parent cat_project_slide_sites_data class_level project comment "Number of variants per sample passing various filters -- pdf file"



project_slide_titv_trunk=@project.slide.titv
file path project_slide_titv_tex_file=$project_slide_titv_trunk.tex dir project_dir disp ".titv.tex" parent cat_project_slide_sites_data class_level project trunk $project_slide_titv_trunk comment "Ti/Tv table for variants -- tex file"
file path project_slide_titv_pdf_file=$project_slide_titv_trunk.pdf dir project_dir disp ".titv.pdf" parent cat_project_slide_sites_data class_level project comment "Ti/Tv table for variants -- pdf file"

project_slide_ref_theta_trunk=@project.slide.ref.theta
file path project_slide_ref_theta_tex_file=$project_slide_ref_theta_trunk.tex dir project_dir disp ".ref.theta.tex" parent cat_project_slide_sites_data class_level project trunk $project_slide_ref_theta_trunk comment "Known theta table for variants -- tex file"
file path project_slide_ref_theta_pdf_file=$project_slide_ref_theta_trunk.pdf dir project_dir disp ".ref.theta.pdf" parent cat_project_slide_sites_data class_level project comment "Known theta table for variants -- pdf file"

project_slide_theta_trunk=@project.slide.theta
file path project_slide_theta_tex_file=$project_slide_theta_trunk.tex dir project_dir disp ".theta.tex" parent cat_project_slide_sites_data class_level project trunk $project_slide_theta_trunk comment "Theta table for variants -- tex file"
file path project_slide_theta_pdf_file=$project_slide_theta_trunk.pdf dir project_dir disp ".theta.pdf" parent cat_project_slide_sites_data class_level project comment "Theta table for variants -- pdf file"

minor file path project_slide_master_ps_file=@project.slide.master.ps dir project_dir disp ".ps" parent cat_project_slide_master_data class_level project comment "Master join file of all relevant slides --- ps file"
major file path project_slide_master_pdf_file=@project.slide.master.pdf dir project_dir disp ".pdf" parent cat_project_slide_master_data class_level project comment "Master join file of all relevant slides --- pdf file"

!!expand:,:ext,majmin:tex,minor:pdf,minor! \
majmin file path project_title_slide_ext_file=@project.title.ext dir project_dir disp "title.ext" trunk @project.title parent cat_project_slide_master_data class_level project comment "Master title slide --- ext file"

!!expand:,:ext,majmin:tex,minor:pdf,major! \
majmin file path project_additional_slide_ext_file=@project.additional.ext dir project_dir disp "additional.ext" trunk @project.additional parent cat_project_slide_master_data class_level project comment "Separator for additional slide section --- ext file"

project_slide_failures_trunk=@project.slide.failures
file path project_slide_failures_tex_file=$project_slide_failures_trunk.tex dir project_dir disp ".failures.tex" parent cat_project_slide_coverage_data class_level project trunk $project_slide_failures_trunk comment "Sample failure table -- tex file"
major file path project_slide_failures_pdf_file=$project_slide_failures_trunk.pdf dir project_dir disp ".failures.pdf" parent cat_project_slide_coverage_data class_level project comment "Sample failure table -- pdf file"

!!expand:type:gene:exon:bait! \
project_slide_types_trunk=@project.slide.types
!!expand:,:type,Type:gene,Gene:exon,Exon:bait,Bait! \
file path project_slide_types_tex_file=$project_slide_types_trunk.tex dir project_dir disp ".types.tex" parent cat_project_slide_coverage_data class_level project trunk $project_slide_types_trunk comment "Type coverage table -- tex file"

!!expand:type:gene:exon:bait! \
file path project_slide_types_pdf_file=$project_slide_types_trunk.pdf dir project_dir disp ".types.pdf" parent cat_project_slide_coverage_data class_level project comment "Type coverage table -- pdf file"

file path project_slide_sstats_initial_pdf_file=@project.slide.sstats.initial.pdf dir project_dir disp ".initial.pdf" parent cat_project_slide_qc_ind_hist_data class_level project comment "Shows all sample metrics that ultimately went into filtering out samples --- hist plot"

file path project_slide_sstats_highlighted_pdf_file=@project.slide.sstats.highlighted.pdf dir project_dir disp ".highlighted.pdf" parent cat_project_slide_qc_ind_hist_data class_level project comment "Shows all sample metrics that ultimately went into filtering out samples, with samples excluded based on each metric separately highlighted --- hist plot"

file path project_slide_sstats_final_pdf_file=@project.slide.sstats.final.pdf dir project_dir disp ".final.pdf" parent cat_project_slide_qc_ind_hist_data class_level project comment "Shows all sample metrics that ultimately went into filtering out samples, with only passed samples present --- plot"

file path project_slide_sstats_extreme_pdf_file=@project.slide.sstats.extreme.pdf dir project_dir disp ".extreme.pdf" parent cat_project_slide_qc_ind_hist_data class_level project comment "Shows select sample metrics, with only passed samples present --- extremes highlighted"

!!expand:type:seq! \
project_slide_type_qc_failures_trunk=@project.slide.type.qc_failures

!!expand:type:seq! \
file path project_slide_type_qc_failures_tex_file=$project_slide_type_qc_failures_trunk.tex dir project_dir disp ".type.failures.tex" parent cat_project_slide_qc_ind_minus_data class_level project trunk $project_slide_type_qc_failures_trunk comment "Sample type qc failure table -- tex file"

!!expand:type:seq! \
file path project_slide_type_qc_failures_pdf_file=$project_slide_type_qc_failures_trunk.pdf dir project_dir disp ".type.failures.pdf" parent cat_project_slide_qc_ind_minus_data class_level project comment "Sample type qc failure table -- pdf file"

project_slide_var_qc_failures_trunk=@project.slide.var.qc_failures
file path project_slide_var_qc_failures_tex_file=$project_slide_var_qc_failures_trunk.tex dir project_dir disp ".failures.tex" parent cat_project_slide_qc_var_data class_level project trunk $project_slide_var_qc_failures_trunk comment "Variant qc failure table -- tex file"
file path project_slide_var_qc_failures_pdf_file=$project_slide_var_qc_failures_trunk.pdf dir project_dir disp ".failures.pdf" parent cat_project_slide_qc_var_data class_level project comment "Variant qc failure table -- pdf file"


!!expand:project:project:project_variant_subset! \
!!expand:,:maskkey,masktype,maskdescrip:all,,all:syn,syn.,synonymous:ns,ns.,non-synonymous:nonsense,nonsense.,nonsense:noncoding,noncoding.,noncoding! \
!!expand:,:tname,description:raw,were called:qc_pass,pass filters:qc_plus,pass QC! \ 
table nohead file path project_tname_maskkey_vcounts_file=@project.tname.masktypevcounts dir project_dir disp ".tname.masktypevcounts" parent cat_project_tname_var_stats_data class_level project comment "Dumped by pseq v-stats using maskdescrip variants that description"

!!expand:,:maskkey,masktype,maskdescrip:all,,all:syn,syn.,synonymous:ns,ns.,non-synonymous:nonsense,nonsense.,nonsense:noncoding,noncoding.,noncoding! \

!!expand:project:project:project_variant_subset! \
!!expand:,:tname,description:qc_plus,pass QC! \ 
!!expand:,:tstrat,tdescrip:all,over all samples:strata,stratified by maf_strata_trait! \ 
table nohead file path project_tname_annotated_tstrat_pre_vcounts_file=@project.tname.annotated.tstrat.pre.vcounts dir project_dir disp ".tname.annotated.tstrat.pre.vcounts" parent cat_project_tname_var_stats_data class_level project comment "Helper file for the number of variants that description for each annotation tdescrip"

!!expand:project:project:project_variant_subset! \
!!expand:,:tname,description:qc_plus,pass QC! \ 
!!expand:,:tstrat,tdescrip:all,over all samples:strata,stratified by maf_strata_trait! \ 
table nohead file path project_tname_annotated_tstrat_vcounts_file=@project.tname.annotated.tstrat.vcounts dir project_dir disp ".tname.annotated.tstrat.vcounts" parent cat_project_tname_var_stats_data class_level project comment "The number of variants that description for each annotation tdescrip"

!!expand:project:project:project_variant_subset! \
!!expand:,:tname,description:qc_plus,pass QC! \ 
!!expand:,:tstrat,tdescrip:all,over all samples:strata,stratified by maf_strata_trait! \ 
table nohead file path project_tname_annotated_tstrat_sample_vcounts_file=@project.tname.annotated.tstrat.sample.vcounts dir project_dir disp ".tname.annotated.tstrat.sample.vcounts" parent cat_project_tname_var_stats_data class_level project comment "The number of variants per sample that description for each annotation tdescrip"

!!expand:project:project:project_variant_subset! \
table file path project_mean_alt_gq_file=@project.mean_alt_gq.dat dir project_dir disp ".mean_alt_gq.dat" parent cat_project_qc_var_stats_data class_level project comment "Mean GQ at alternate calls"

!!expand:meta:gq:dp! \
!!expand:project:project:project_variant_subset! \
table file path project_mean_meta_file=@project.mean_meta.dat dir project_dir disp ".mean_meta.dat" parent cat_project_qc_var_stats_data class_level project comment "Mean GQ "

!!expand:project:project:project_variant_subset! \
table file path project_dev_dp_file=@project.dev_dp.dat dir project_dir disp ".dev_dp.dat" parent cat_project_qc_var_stats_data class_level project comment "Dev GQ"

!!expand:project:project:project_variant_subset! \
table file path project_vstats_file=@project.vstats dir project_dir disp ".vstats" parent cat_project_qc_var_stats_data class_level project comment "All relevant variant statistics"

!!expand:project:project:project_variant_subset! \
table file path project_extended_vstats_file=@project.extended.vstats dir project_dir disp ".extended.vstats" parent cat_project_qc_var_stats_data class_level project comment "All relevant variant statistics"

!!expand:,:typeu,typed:,:snps_,snps.:indels_,indels.:multiallelics_,multiallelics.! \
!!expand:project:project:project_variant_subset! \
table file path project_typeuvfreq_file=@project.typedvfreq dir project_dir disp ".typedvfreq" parent cat_project_qc_var_stats_data class_level project comment "Dumped by pseq vfreq; descript"

#!!expand:project:project:project_variant_subset! \
#table file path project_vfreq_file=@project.vfreq dir project_dir disp ".vfreq" parent cat_project_qc_var_stats_data class_level project comment "Dumped by pseq vfreq; descript"

table file path project_custom_vstats_annot_file=@project.custom.vstats.annot dir project_dir disp ".custom.vstats.annot" parent cat_project_qc_var_stats_data class_level project comment "Custom file to use to add annotations to vstats"

major table file path project_variant_outlier_file=@project.variant.outliers.csv dir project_dir disp ".outliers.csv" parent cat_project_qc_var_stats_data class_level project comment "Outlier values from all variant statistics"

table file path project_variant_exclude_detail_file=@project.variant.exclude.detail dir project_dir disp ".variant.exclude.detail" parent cat_project_qc_var_exclude_data class_level project comment "Variants excluded because they are outliers on one or more measures --- with information about the metrics that caused them to fail"

file path project_variant_custom_exclude_file=@project.variant.custom.exclude dir project_dir disp ".variant.custom.exclude" parent cat_project_qc_var_exclude_data class_level project comment "Specify a file with (additional) custom exclusions"
"""
}
    