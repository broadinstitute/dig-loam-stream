
package loamstream.pipeline.examples

/**
 * LoamStream
 * Created by oliverr on 10/6/2015.
 */
object PipelineTargetedPart11 {
  val string =
 """
file major path transcript_burden_qq_pdf_file=@transcript_burden.qq.pdf dir transcript_burden_dir disp ".qq.pdf" parent cat_transcript_burden_data class_level transcript_burden comment "QQ plot for this transcript & burden mask"

nohead table file path transcript_burden_annot_variant_list_file=@transcript_burden.annot.variant.list dir transcript_burden_dir disp ".annot.variant.list" parent cat_transcript_burden_data class_level transcript_burden comment "List of variant ids this burden based solely on annotations"

nohead table file path transcript_burden_all_variant_list_file=@transcript_burden.all.variant.list dir transcript_burden_dir disp ".all.variant.list" parent cat_transcript_burden_data class_level transcript_burden comment "List of variant ids this burden"

nohead onecol table file path transcript_burden_reg_list_file=@transcript_burden.reg.list dir transcript_burden_dir disp ".reg.list" parent cat_transcript_burden_data class_level transcript_burden comment "List of regions for this burden"

table file path transcript_burden_gassoc_file=@transcript_burden.gassoc dir transcript_burden_dir disp ".gassoc" parent cat_transcript_burden_data class_level transcript_burden comment "Case / control variant counts for this burden"

table file path transcript_burden_sample_list_file=@transcript_burden.sample.list dir transcript_burden_dir disp ".sample.list" parent cat_transcript_burden_data class_level transcript_burden comment "List of samples who have variants for this burden"

table file path transcript_burden_sample_without_list_file=@transcript_burden.sample.without.list dir transcript_burden_dir disp ".sample.without.list" parent cat_transcript_burden_data class_level transcript_burden comment "List of samples who do not have variants for this burden"

major file path transcript_burden_pheno_pdf_file=@transcript_burden.pheno.pdf dir transcript_burden_dir disp ".pheno.pdf" parent cat_transcript_burden_data class_level transcript_burden comment "Plot of phenotypes for samples with and without variant"

major file path transcript_burden_all_pheno_pdf_file=@transcript_burden.all_pheno.pdf dir transcript_burden_dir disp ".all_pheno.pdf" parent cat_transcript_burden_data class_level transcript_burden comment "Plot of all related phenotypes for samples with and without variant"

#variant files

minor file variant_meta_info_file=@variant.meta.info dir variant_dir disp ".meta.info" parent cat_variant_meta_data class_level variant comment "Information about this variant"

table nohead onecol file variant_sample_list_file=@variant.sample.list dir variant_dir disp ".sample.list" parent cat_variant_meta_data class_level variant comment "List of project samples with variant"

table nohead onecol file variant_sample_without_list_file=@variant.sample.without.list dir variant_dir disp ".sample.without.list" parent cat_variant_meta_data class_level variant comment "List of project samples without variant"

file path variant_mds_dat_file=@variant.mds.dat dir variant_dir disp ".mds.dat" parent cat_variant_qc_data class_level variant comment "MDS Plot, highlighting samples with and without variant --- dat file"

!!expand:tval:top:all! \
major file path variant_tval_mds_pdf_file=@variant.tval.mds.pdf dir variant_dir disp ".tval.mds.pdf" parent cat_variant_qc_data class_level variant comment "Plot of tval MDS values, highlighting samples with and without variant --- pdf file"

file path variant_pheno_pdf_file=@variant.pheno.pdf dir variant_dir disp ".pheno.pdf" parent cat_variant_qc_data class_level variant comment "Comparison of trait values for samples with and without variant"

file path variant_all_pheno_pdf_file=@variant.all_pheno.pdf dir variant_dir disp ".all_pheno.pdf" parent cat_variant_qc_data class_level variant comment "Comparison of all trait values for samples with and without variant"

file path variant_sstats_highlight_file=@variant.sstats.highlight dir variant_dir disp ".sstats.highlight" parent cat_variant_qc_data class_level variant comment "Samples with variant to highlight"

!!expand:,:slidetype,slideext:,:slide_,slide.! \
file path variant_slidetypesstats_pdf_file=@variant.slideextsstats.pdf dir variant_dir disp ".slideextsstats.pdf" parent cat_variant_qc_data class_level variant comment "QC stats for samples with variant"

!!expand:,:ext,weblevel:tex,:pdf,major! \
file weblevel path variant_qc_metrics_ext_file=@variant.qc_metrics.ext dir variant_dir trunk @variant.qc_metrics disp ".qc_metrics.ext" parent cat_variant_qc_data class_level variant comment "QC metric data for variant --- ext file"

!!expand:,:ext,weblevel:tex,:pdf,major! \
file weblevel path variant_qc_metrics_ext_file=@variant.qc_metrics.ext dir variant_dir trunk @variant.qc_metrics disp ".qc_metrics.ext" parent cat_variant_qc_data class_level variant comment "QC metric data for variant --- ext file"

major file path variant_genome_pdf_file=@variant.genome.pdf dir variant_dir disp ".genome.pdf" parent cat_variant_qc_data class_level variant comment "Plot of IBD sharing stratified by variant"

!!expand:ext:png:pdf! \
major file path variant_igv_ext_file=@variant.igv.ext dir variant_dir disp ".igv.ext" parent cat_variant_qc_data class_level variant comment "IGV snapshot of variant --- ext file"

minor file path variant_ucsc_regions_file=@variant.ucsc.regions dir variant_dir disp ".ucsc.regions" parent cat_variant_plot_data class_level variant comment "Region information for UCSC screenshot fetching"

minor file path variant_ucsc_pdf_file=@variant.ucsc.pdf dir gene_dir disp ".ucsc.pdf" parent cat_variant_plot_data class_level variant comment "Region information for UCSC screenshot fetching"

variant_r2_trunk=@variant.r2 dir variant_dir
table file path variant_r2_ld_file=$variant_r2_trunk.ld dir variant_dir disp ".r2.ld" parent cat_variant_ld_data class_level variant comment "R2 values between this variant and all others at the locus"
file path variant_r2_ld_log_file=$variant_r2_trunk.log dir variant_dir disp ".r2.log" parent cat_variant_ld_data class_level variant comment "Log file for computation of R2 values between this variant and all others at the locus"

table file path variant_r2_annotated_ld_file=$variant_r2_trunk.annotated.ld dir variant_dir disp ".r2.annotated.ld" parent cat_variant_ld_data class_level variant comment "R2 values between this variant and all others at the locus --- annotated with information about which marker is where"

major file path variant_r2_pdf_file=@variant.r2.pdf dir variant_dir disp ".r2.pdf" parent cat_variant_ld_data class_level variant comment "Plot of R2 values between this variant and all others at the locus"

!!expand:ext:tex:pdf! \
file path variant_r2_table_ext_file=@variant.r2.table.ext dir variant_dir trunk @variant.r2.table  disp ".table.r2.ext" parent cat_variant_ld_data class_level variant comment "List of LD with other variants in this gene"

file path variant_beagle_phased_file=@variant.bgl.phased dir variant_dir disp ".bgl.phased" parent cat_variant_haplotype_analysis_data class_level variant comment "Beagle output formatted for input into variant haplotype analysis"

file path variant_haplotype_analysis_file=@variant.hap.analysis.txt dir variant_dir disp ".txt" parent cat_variant_haplotype_analysis_data class_level variant comment "Output results for haplotype analysis"

!!expand:,:ext,weblevel:tex,:pdf,major! \
weblevel file path variant_haplotype_analysis_info_ext_file=@variant.hap.analysis.info.ext trunk @variant.hap.analysis.info dir variant_dir disp ".info.ext" parent cat_variant_haplotype_analysis_data class_level variant comment "Output results for haplotype analysis --- ext file"

file path variant_best_hap_file=@variant.best.hap dir variant_dir disp ".best.hap" parent cat_variant_haplotype_analysis_data class_level variant comment "Best haplotype as determined from haplotype analysis"

table major file path variant_haplotype_analysis_freq_file=@variant.hap.freq.dat dir variant_dir disp ".freq.dat" parent cat_variant_haplotype_analysis_data class_level variant comment "List of case/control counts and frequencies by haplotype position"

table file path variant_haplotype_analysis_initial_seg_file=@variant.hap.initial.seg.dat dir variant_dir disp ".initial.seg.dat" parent cat_variant_haplotype_analysis_data class_level variant comment "Segmental sharing between individuals with haplotype and remaining individuals"

table file path variant_haplotype_analysis_seg_file=@variant.hap.seg.dat dir variant_dir disp ".seg.dat" parent cat_variant_haplotype_analysis_data class_level variant comment "Segmental sharing between individuals with haplotype and remaining individuals --- converted to format used by PLINK"


major file path variant_haplotype_analysis_pdf_file=@variant.hap.analysis.pdf dir variant_dir disp ".pdf" parent cat_variant_haplotype_analysis_data class_level variant comment "PDF plot of haplotype sharing around variant + case control ratio of haplotype"

major file path variant_haplotype_analysis_longest_pdf_file=@variant.hap.analysis.longest.pdf dir variant_dir disp ".longest.pdf" parent cat_variant_haplotype_analysis_data class_level variant comment "PDF plot of haplotype sharing around variant + case control ratio of haplotype --- showing samples with longest sharing colored by phenotype"

table file path variant_hap_posterior_file=@variant.hap.posterior.tsv dir variant_dir disp ".hap.posterior" parent cat_variant_hap_imputation_data class_level variant comment "Posterior probabilities that each haplotype carries the variant"

!!expand:impute_type:hap:traditional! \
table file path variant_impute_type_count_posterior_file=@variant.impute_type.count.posterior.tsv dir variant_dir disp ".count.posterior" parent cat_variant_impute_type_imputation_data class_level variant comment "Posterior probabilities that each person carries at least 1 or 2 alleles --- determined by impute_type imputation method"

!!expand:impute_type:hap:traditional! \
table file path variant_impute_type_threshold_vassoc_file=@variant.impute_type.threshold.vassoc dir variant_dir disp ".threshold.vassoc" parent cat_variant_impute_type_imputation_data class_level variant comment "Association score for variant --- one value for each threshold as determined by impute_type imputation method"

!!expand:impute_type:hap:traditional! \
table file path variant_impute_type_summary_file=@variant.impute_type.summary dir variant_dir disp ".summary" parent cat_variant_impute_type_imputation_data class_level variant comment "Summary of imputation results; impute_type method"

!!expand:impute_type:hap:traditional! \
major file path variant_impute_type_threshold_vassoc_pdf_file=@variant.impute_type.threshold.vassoc.pdf dir variant_dir disp ".threshold.vassoc.pdf" parent cat_variant_impute_type_imputation_data class_level variant comment "Plot of association score for variant at each threshold as determined by impute_type imputation method"

minor file path variant_slide_master_ps_file=@variant.slide.master.ps dir variant_dir disp ".ps" parent cat_variant_slide_master_data class_level variant comment "Master join file of all relevant slides --- ps file"
major file path variant_slide_master_pdf_file=@variant.slide.master.pdf dir variant_dir disp ".pdf" parent cat_variant_slide_master_data class_level variant comment "Master join file of all relevant slides --- pdf file"

!!expand:,:ext,majmin:tex,minor:pdf,minor! \
majmin file path variant_title_slide_ext_file=@variant.title.ext dir variant_dir disp "title.ext" trunk @variant.title parent cat_variant_slide_master_data class_level variant comment "Title slide for variant --- ext file"

#call_set
#table file path call_set_picard_metrics_file=@call_set.picard.metrics.csv dir call_set_dir disp ".metrics.csv" parent cat_call_set_picard_data class_level call_set comment "Picard metrics for samples on this call_set" skip_re "Solexa Picard|2010"

minor file path call_set_sample_vcf_id_to_sample_id_file=@call_set.sample.vcf_id.to.sample_id dir call_set_dir disp ".sample.vcf_id.to.sample_id" parent cat_call_set_meta_call_data class_level call_set comment "First column: id in VCF file; second column: id in project"

minor file path call_set_sample_list_file=@call_set.sample.list dir call_set_dir disp ".sample.list" parent cat_call_set_meta_call_data class_level call_set comment "List of samples for calling at call_set level"

minor file path call_set_bam_list_file=@call_set.bam.list dir call_set_dir disp ".bam.list" parent cat_call_set_meta_call_data class_level call_set comment "List of bams for calling at call_set level"

minor file path call_set_sample_subset_bam_list_file=@call_set.@call_set_subset.@call_set_sample_subset.bam.list dir call_set_sample_subsets_dir disp ".bam.list" parent cat_call_set_sample_subset_missed_variants_data class_level call_set_sample_subset comment "List of bams for calling at call_set_sample_subset level"

!!expand:,:call_set,mydir:call_set,call_set_dir:call_set_subset,call_set_subsets_dir! \
call_set_plink_file=@call_set dir mydir

!!expand:,:call_set,mydir:call_set,call_set_dir:call_set_subset,call_set_subsets_dir! \
call_set_temp_plink_file=@call_set.tmp dir mydir

minor file path call_set_subset_bim_keep_file=@call_set.@call_set_subset.bim_keep dir call_set_subsets_dir disp ".bim.keep" parent cat_call_set_subset_variant_call_data class_level call_set_subset comment "List of variants to keep when subsetting bim file"

!!expand:,:call_set,mydir:call_set,call_set_dir:call_set_subset,call_set_subsets_dir! \
!!expand:ext:bed:bim:fam! \
table doubcom major file path call_set_ext_file=$call_set_plink_file.ext dir mydir disp ".ext" parent cat_call_set_variant_call_data class_level call_set comment "Initial ext file for this call_set"

table file path call_set_extract_file=$call_set_plink_file.extract dir call_set_dir disp ".extract" parent cat_call_set_variant_call_data class_level call_set comment "Specify this to remove markers at the very start of the VCF generation process"

!!expand:,:call_set,mydir:call_set,call_set_dir:call_set_subset,call_set_subsets_dir! \
!!expand:ext:ref_alt:keep! \
table doubcom major file path call_set_ext_file=$call_set_plink_file.ext dir mydir disp ".ext" parent cat_call_set_variant_call_data class_level call_set comment "Initial ext file for this call_set"

!!expand:,:call_set,mydir:call_set,call_set_dir:call_set_subset,call_set_subsets_dir! \
table doubcom minor file path call_set_recode_vcf_log_file=$call_set_plink_file.vcf.log dir mydir disp ".vcf.log" parent cat_call_set_variant_call_data class_level call_set comment "Log file for conversion from bed/bim/fam file"

!!expand:,:call_set,mydir:call_set,call_set_dir:call_set_subset,call_set_subsets_dir! \
table doubcom major file path call_set_vcf_file=@call_set.vcf dir mydir disp ".vcf" parent cat_call_set_variant_call_data class_level call_set comment "Initial vcf file for this call_set (uncompressed)"

!!expand:,:type,ext:,:_compressed,.gz! \
table doubcom major file path call_set_from_plinktype_vcf_file=@call_set.from_plink.vcfext dir call_set_dir disp ".from_plink.vcfext" parent cat_call_set_variant_call_data class_level call_set comment "Initial vcf file for this call_set; specify rather than vcf_file compressed_vcf_file if want to do processing for plink input on it"

!!expand:,:call_set,mydir:call_set,call_set_dir:call_set_subset,call_set_subsets_dir! \
table doubcom major file path call_set_compressed_vcf_file=@call_set.vcf@zip_vcf dir mydir disp ".compressed.vcf" parent cat_call_set_variant_call_data class_level call_set comment "Vcf file for this call_set --- compressed if desired (specify as initial if compressed)"

!!expand:,:call_set,mydir:call_set,call_set_dir:call_set_subset,call_set_subsets_dir! \
table doubcom major file path call_set_compressed_vcf_index_file=@call_set.vcf@zip_vcf.tbi dir mydir disp "compressed.vcf.tbi" parent cat_call_set_variant_call_data class_level call_set comment "Index for compressed vcf file"

table doubcom major file path call_set_dis_removed_vcf_file=@call_set.dis_removed.vcf@zip_vcf dir call_set_dir disp ".dis_removed.vcf" parent cat_call_set_variant_call_data class_level call_set comment "Annotated vcf file for this call_set; has sites with different alleles than at the project level removed"

minor file path call_set_variant_site_vcf_file=@call_set.variant.site.vcf dir call_set_dir disp ".variant.site.vcf" parent cat_call_set_variant_call_data class_level call_set comment "A list of sites called on this call_set -- in vcf format -- used for tracking the alleles"

minor file path call_set_variant_annot_site_vcf_file=@call_set.variant.annot.vcf dir call_set_dir disp ".variant.annot.vcf" parent cat_call_set_variant_call_data class_level call_set comment "A list of sites called on this call_set -- after discordances are removed -- together with original annotations"

meta_table file path call_set_call_set_subsets_meta_file=@call_set.call_set_subsets.meta dir call_set_dir disp ".call_set_subets.meta" parent cat_call_set_meta_call_data class_level call_set comment "The call set subsets to use" meta_level call_set_subset

minor file path call_set_missed_variants_site_vcf_file=@call_set.missed_variants.site.vcf dir call_set_dir disp ".missed_variants.site.vcf" parent cat_call_set_variant_call_data class_level call_set comment "A list of variants called on other call_sets but not this call_set"


#minor file path call_set_maf_file=@call_set.maf dir call_set_dir disp ".maf" parent cat_call_set_all_call_data class_level call_set comment ".maf file output by firehose"
#table file path call_set_maf_annotated_file=@call_set.maf.annotated dir call_set_dir disp ".maf.annotated" parent cat_call_set_all_call_data class_level call_set comment ".maf.annotated file output by firehose"
#table doubcom file path call_set_vcf_file=@call_set.vcf dir call_set_dir disp ".vcf" parent cat_call_set_all_call_data class_level call_set comment "Variant calls for all samples on this call_set; via multi-sample caller"

#major file path call_set_filtered_eval_file=@call_set.filtered.eval dir call_set_dir disp ".eval" parent cat_call_set_filtered_call_data class_level call_set comment ".filtered.eval file generated by firehose"
#file path call_set_filtered_eval_interesting_sites_file=@call_set.filtered.eval.interesting_sites dir call_set_dir disp ".eval.interesting_sites" parent cat_call_set_filtered_call_data class_level call_set comment ".interesting_sites file generated by firehose"
#table doubcom file path call_set_filtered_vcf_file=@call_set.filtered.vcf dir call_set_dir disp ".filtered.vcf" parent cat_call_set_filtered_call_data class_level call_set comment ".filtered.vcf file generated by firehose"

table doubcom file path call_set_missed_sites_vcf_file=@call_set.missed_sites.vcf@zip_vcf dir call_set_dir disp ".missed_sites.vcf" parent cat_call_set_all_call_data class_level call_set comment "Variant calls for all samples on this call_set at variants called on another call_set but not this one"

table doubcom file path call_set_all_sites_vcf_file=@call_set.all_sites.vcf@zip_vcf dir call_set_dir disp ".all_sites.vcf" parent cat_call_set_all_call_data class_level call_set comment "Variant calls for all samples on this call_set, including at variants called on other call_sets"

#table doubcom file path call_set_ab_annotated_vcf_file=@call_set.ab_annotated.vcf@zip_vcf dir call_set_dir disp ".ab_annotated.vcf" parent cat_call_set_all_call_data class_level call_set comment "SNP calls for all samples on this call_set, annotated with AB by sample"

#table doubcom file path call_set_ab_annotated_site_vcf_file=@call_set.ab_annotated.site.vcf dir call_set_dir disp ".ab_annotated.site.vcf" parent cat_call_set_all_call_data class_level call_set comment "SNP calls for all samples on this call_set, annotated with AB by sample --- projected to only sites/filters"

table doubcom file path call_set_all_sites_site_vcf_file=@call_set.all_sites.site.vcf dir call_set_dir disp ".all_sites.site.vcf" parent cat_call_set_all_call_data class_level call_set comment "SNP calls for all samples on this call_set, annotated with AB by sample --- projected to only sites/filters"


#minor file path call_set_unfiltered_eval_file=@call_set.unfiltered.eval dir call_set_dir disp ".eval" parent cat_call_set_unfiltered_call_data class_level call_set comment ".unfiltered.eval file generated by firehose"
#minor file path call_set_unfiltered_eval_interesting_sites_file=@call_set.unfiltered.eval.interesting_sites dir call_set_dir disp ".eval.interesting_sites" parent cat_call_set_unfiltered_call_data class_level call_set comment ".unfiltered.interesting_sites file generated by firehose"

#minor file call_set_sample_coverage_stats_file_list=@call_set.sample.coverage.stats.list disp ".sample.list" dir call_set_dir parent cat_call_set_sample_coverage_data class_level call_set comment "List of sample coverage stats file for use in generating call_set sample coverage dat file"

#table file path call_set_sample_coverage_dat_file=@call_set.coverage.csv dir call_set_dir disp ".csv" parent cat_call_set_sample_coverage_data class_level call_set comment "Coverage statistics for each sample on this call_set"
#major file path call_set_sample_coverage_pdf_file=@call_set.coverage.pdf dir call_set_dir disp ".pdf" parent cat_call_set_sample_coverage_data class_level call_set comment "Plot of coverage for each sample on this call_set"
#minor file call_set_sample_gene_coverage_stats_file_list=@call_set.sample.gene.coverage.stats.list disp ".sample.list" dir call_set_dir parent cat_call_set_gene_dist_coverage_data class_level call_set comment "List of sample gene coverage stats file for use in generating call_set gene dist coverage dat file"

#table file path call_set_gene_dist_coverage_dat_file=@call_set.gene.coverage.csv dir call_set_dir disp "csv" parent cat_call_set_gene_dist_coverage_data class_level call_set comment "Coverage for each sample on this call_set stratified by gene"
#major file path call_set_gene_dist_coverage_pdf_file=@call_set.gene.coverage.pdf dir call_set_dir disp "pdf" parent cat_call_set_gene_dist_coverage_data class_level call_set comment "Plot of coverage for each sample on this call_set stratified by gene"

#table nohead file path call_set_gene_cum_coverage_dat_file=@call_set.gene.cum.coverage.csv dir call_set_dir disp ".csv" parent cat_call_set_gene_cum_coverage_data class_level call_set comment "Cumulative distribution of gene coverage for samples on this call_set"
#file path call_set_gene_cum_coverage_pdf_file=@call_set.gene.cum.coverage.pdf dir call_set_dir disp ".pdf" parent cat_call_set_gene_cum_coverage_data class_level call_set comment "Plot of cumulative distribution of gene coverage for samples on this call_set"

#project_subset

#minor file project_subset_interval_list_file=@project_subset.interval_list dir project_subsets_dir disp ".interval_list" parent cat_project_subset_samtools_data class_level project_subset comment "Interval list at which to call sites"

#table doubcom file path project_subset_samtools_vcf_file=@project_subset.samtools.vcf dir project_subsets_dir disp ".samtools.vcf" parent cat_project_subset_samtools_data class_level project_subset comment "Output of samtools calls"

#call_set_subset

file path call_set_subset_missed_variants_site_vcf_file=@call_set_subset.missed_variants.site.vcf dir call_set_subsets_dir disp ".missed_variants.site.vcf" parent cat_call_set_subset_missed_variants_data class_level call_set_subset comment "A list of variants called on other call_sets but not this call_set --- subsetted for efficiency"

#minor table doubcom file path call_set_subset_missed_sites_temp_vcf_file=@call_set_subset.missed_sites.temp.vcf dir call_set_subsets_dir disp ".temp.missed_sites.vcf" parent cat_call_set_subset_missed_sites_data class_level call_set_subset comment "Variant calls for all samples on this call_set at sites called on another call_set but not this one --- file before pruning out buggy sites without genotypes (for a short time output by the GATK) --- subsetted for efficiency"

minor table doubcom file path call_set_subset_missed_variants_vcf_file=@call_set_subset.missed_variants.vcf@zip_vcf dir call_set_subsets_dir disp ".missed_variants.vcf" parent cat_call_set_subset_missed_variants_data class_level call_set_subset comment "Variant calls for all samples on this call_set at variants called on another call_set but not this one --- subsetted for efficiency"

minor table doubcom file path call_set_sample_subset_missed_variants_vcf_file=@call_set_subset.@call_set_sample_subset.missed_variants.vcf@zip_vcf dir call_set_sample_subsets_dir disp ".missed_variants.vcf" parent cat_call_set_sample_subset_missed_variants_data class_level call_set_sample_subset comment "Variant calls for all samples on this call_set at variants called on another call_set but not this one --- subsetted for efficiency"

#sample
#To link to			return "$igv_http?sessionURL=$xml_attachment_path&user=$igv_user$locus_text";

igv_http=http://www.broadinstitute.org/igv/dynsession/igv.jnlp
igv_user=lap
igv_url=$igv_http
igv_port=13161


convert_paths major file path sample_bam_file=@sample.bam dir sample_dir disp ".bam" parent cat_sample_read_data class_level sample comment "Bam file for this sample" goto_url $igv_http?sessionURL=*sample_bam_xml_file&user=$igv_user
file path sample_bai_file=@sample.bam.bai dir sample_dir disp ".bai" parent cat_sample_read_data class_level sample comment "Bam index file for this sample"
file path sample_bam_xml_file=@sample.bam.xml dir sample_dir disp ".xml" parent cat_sample_read_data class_level sample comment "Xml file for loading bam in igv"

#No one has this anymore (did Firehose change?)
#minor file path sample_bam_blacklist_file=@sample.bam.blacklist.txt dir sample_dir disp ".blacklist.txt" parent cat_sample_read_data class_level sample comment ".bam.blacklist file generated by firehose"
#file path sample_indel_lods_file=@sample.indel.lods dir sample_dir disp ".indel.lods" parent cat_sample_read_data class_level sample comment "Indel lods file for this sample generated by firehose"
#file path sample_indel_stats_file=@sample.indel.stats dir sample_dir disp ".indel.stats" parent cat_sample_read_data class_level sample comment "Indel stats file for this sample generated by firehose"
#minor file path sample_merged_intervals_stats_file=@sample.merged.intervals dir sample_dir disp ".merged.intervals" parent cat_sample_read_data class_level sample comment ".merged.intervals file generated by firehose"

table file path sample_coverage_file=@sample.coverage.txt.gz dir sample_dir disp ".coverage.txt.gz" parent cat_sample_coverage_data class_level sample comment "Coverage at each base for this sample; generated by DepthOfCoverageWalker"
file path sample_coverage_stats_file=@sample.coverage.stats.csv dir sample_dir disp ".stats.csv" parent cat_sample_coverage_data class_level sample comment "Coverage statistics for this sample across all bases"
!!expand:type:gene:exon:bait! \
table nohead file path sample_type_coverage_stats_file=@sample.type.coverage.stats.csv dir sample_dir disp ".type.stats.csv" parent cat_sample_coverage_data class_level sample comment "Coverage statistics for this sample across all bases; stratified by gene"

major file path sample_gene_coverage_pdf_file=@sample.gene.coverage.pdf dir sample_dir disp ".gene.pdf" parent cat_sample_coverage_data class_level sample comment "Plot of this sample's coverage at each gene" 

###BEGIN Uncomment this section to have files generated by Sample_UnifiedGenotyperToEval
#minor file path sample_bam_list_file=@sample.bam.list dir sample_dir disp ".bam.list" parent cat_sample_meta_call_data class_level sample comment "List of bams for calling SNPs only at this sample"
#minor file path sample_snp_blacklist_file=@sample.snp.blacklist.list dir sample_dir disp ".blacklist.list" parent cat_sample_meta_call_data class_level sample comment ".snp.blacklist.list file generated by firehose"

#minor file path sample_maf_file=@sample.maf dir sample_dir disp ".maf" parent cat_sample_all_call_data class_level sample comment ".maf file generated by firehose"
#table minor file path sample_maf_annotated_file=@sample.maf.annotated dir sample_dir disp ".maf.annotated" parent cat_sample_all_call_data class_level sample comment ".maf.annotated file generated by firehose"
#table doubcom file path sample_vcf_file=@sample.vcf dir sample_dir disp ".vcf" parent cat_sample_all_call_data class_level sample comment "Calls made by firehose using single-sample caller for only this sample"

#major file path sample_filtered_eval_file=@sample.filtered.eval dir sample_dir disp ".eval" parent cat_sample_filtered_call_data class_level sample comment ".filtered.eval file at calls for this sample"
#file path sample_filtered_eval_interesting_sites_file=@sample.filtered.eval.interesting_sites dir sample_dir disp ".eval.interesting_sites" parent cat_sample_filtered_call_data class_level sample comment ".filtered.interesting_sites file at calls for this sample"
#table doubcom major file path sample_filtered_maf_annotated_vcf_file=@sample.filtered.maf_annotated.vcf dir sample_dir disp ".maf_annotated.vcf" parent cat_sample_filtered_call_data class_level sample comment "Annotated vcf file for calls for this sample"
#table doubcom file path sample_filtered_vcf_file=@sample.filtered.vcf dir sample_dir disp ".filtered.vcf" parent cat_sample_filtered_call_data class_level sample comment "Filtered vcf file for calls for this sample"

#minor file path sample_unfiltered_eval_file=@sample.unfiltered.eval dir sample_dir disp ".eval" parent cat_sample_unfiltered_call_data class_level sample "unfiltered.eval file generated by firehose"
#minor file path sample_unfiltered_eval_interesting_sites_file=@sample.unfiltered.eval.interesting_sites dir sample_dir disp ".eval.interesting_sites" parent cat_sample_unfiltered_call_data class_level sample comment "unfiltered.interesting_sites file generated by firehose"
###END Uncomment this section to have files generated by Sample_UnifiedGenotyperToEval

##BEGIN Uncomment to get files for indels
#minor file path sample_indel_blacklist_file=@sample.indel.blacklist.list dir sample_dir disp ".blacklist.txt" parent cat_sample_indel_call_data class_level sample comment "indel.blacklist.list file generated by firehose"

#table nohead file path sample_indels_bed_file=@sample.indels.bed dir sample_dir disp ".indels.bed" parent cat_sample_indel_call_data class_level sample skip_re REDUCE comment "indels.bed file generated by firehose"
#table nohead file path sample_filtered_indels_bed_file=@sample.filtered_indels.bed dir sample_dir disp ".filtered_indels.bed" parent cat_sample_indel_call_data class_level sample skip_re REDUCE comment "Filtered indels.bed file generated by firehose"
#major table nohead file path sample_indels_verbose_bed_file=@sample.indels.verbose.bed dir sample_dir disp ".indels.verbose.bed" parent cat_sample_indel_call_data class_level sample skip_re REDUCE comment "Verbose indels file generated by firehose"
##END Uncomment to get files for indels

#merged calls
#major table doubcom file sample_project_vcf_file=@sample.project.vcf dir sample_dir disp ".vcf" parent cat_sample_multi_sample_snp_call_data class_level sample comment "Calls from project (multi-sample) calls projected to this sample"

#--------
#commands
#--------

null_cmd=a=1

#meta cmds

expand_interval_size=50
expand_helper=awk -v OFS="\t" '@3 {\$@1 -= $expand_interval_size; if (\$@1 < 1) {\$@1 = 1} \$@2 += $expand_interval_size} {print}'

prop expand_coverage_targets=scalar default 0
prop expand_targets=scalar default 1
prop expand_intervals=scalar default 1

local cmd make_project_expanded_interval_list_file=$expand_helper(2,3,\$1 !~ /^@/) < !{input,,project_interval_list_file} > !{output,,project_expanded_interval_list_file} class_level project run_if and,(or,expand_coverage_targets,expand_targets),expand_intervals

local cmd cp_project_expanded_interval_list_file=cp !{input,,project_interval_list_file} !{output,,project_expanded_interval_list_file} class_level project skip_if and,(or,expand_coverage_targets,expand_targets),expand_intervals

local cmd make_project_target_length_file=perl $targeted_bin_dir/get_target_length.pl < !{input,,project_expanded_interval_list_file,if_prop=expand_targets,allow_empty=1} !{input,,project_interval_list_file,unless_prop=expand_targets,allow_empty=1} > !{output,,project_target_length_file} class_level project

local cmd make_project_expanded_exon_target_file=$expand_helper(4,5,) < !{input,,project_exon_target_file} > !{output,,project_expanded_exon_target_file} class_level project run_if expand_targets

local cmd cp_project_expanded_exon_target_file=cp !{input,,project_exon_target_file} !{output,,project_expanded_exon_target_file} class_level project skip_if expand_targets

short cmd make_project_variant_gene_annotation_file=$smart_cut_cmd !{input,--file,project_full_var_annot_file} --in-delim $tab --select-col 1,1,'$vep_id_annot SNPEFF_Gene_name' --require-col-match --exact --exclude-row 1,1 | awk -v OFS="\t" -F"\t" '\$2 == "$annot_missing_field" {\$2 = "$outside_gene_name"} {print}' > !{output,,project_variant_gene_annotation_file} class_level project

local cmd make_project_gene_target_file=perl $targeted_bin_dir/exon_to_gene_target.pl < !{input,,project_expanded_exon_target_file} > !{output,,project_gene_target_file} class_level project

vep_target_chunk_size=1000
#short cmd make_project_merge_subset_pos_transcript_file=n=`$combine_vcfs_for_annotation(project_merge_subset,) | wc -l | awk '{print int(\$1/$vep_target_chunk_size)+1}'` && for i in `seq 1 \$n`; do s=$((\$i * $vep_target_chunk_size)); ($combine_vcfs_for_annotation(project_merge_subset,) | head -n\$s | tail -n$vep_target_chunk_size | $vep_cmd --offline -o STDOUT --dir $ensembl_cache_dir | fgrep -v '\#\#'; done | awk 'NR == 1 || \$1 !~ /\#/' | $smart_cut_cmd --in-delim $tab --exclude-row 0,1 --select-col 0,1 --exact --require-col-match --select-col 0,1,$vep_trans_annot | sed 's/:/\t/' > !{output,,project_merge_subset_pos_transcript_file} class_level project_merge_subset


pos_transcript_helper=awk -F"\t" -v OFS="\t" '!/^\#/ {\$3=\$1":"\$2} {print}' | $vep_cmd --offline -o STDOUT --dir $ensembl_cache_dir | fgrep -v '\#\#' | $smart_cut_cmd --in-delim $tab --exclude-row 0,1 --select-col 0,1 --exact --require-col-match --select-col 0,1,$vep_trans_annot | sed 's/:/\t/'
"""
}
    