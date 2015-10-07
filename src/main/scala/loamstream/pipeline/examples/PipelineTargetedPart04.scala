
package loamstream.pipeline.examples

/**
 * LoamStream
 * Created by oliverr on 10/6/2015.
 */
object PipelineTargetedPart04 {
  val string =
 """
table nohead file path project_disjoint_gene_target_file=@project.disjoint.gene.targets dir project_dir disp ".disjoint.gene.targets" parent cat_project_target_data class_level project comment "All genes in study: has name, chr, start, stop; flattened to have all regions disjoint"

!!expand:project:project:project_variant_subset! \
table file path project_transcript_gene_map_file=@project.transcript.gene.map dir project_dir disp ".transcript.gene.map" parent cat_project_annot_data class_level project comment "Full list of all transcripts for each gene"

table file path project_transcript_gene_alias_file=@project.transcript.gene.alias dir project_dir disp ".transcript.gene.alias" parent cat_project_annot_data class_level project comment "Full list of all transcripts for each gene"

table nohead file path project_gene_interesting_transcript_file=@project.gene.interesting.transcript dir project_dir disp ".gene.interesting.transcript" parent cat_project_target_data class_level project comment "Externally specified: list of valid transcripts to count as interesting for a gene; space delimited with first col gene, second transcript"

table file path project_picard_file=@project.picard.metrics.tsv dir project_dir disp ".picard.metrics.tsv" parent cat_project_picard_data class_level project comment "Dump of all picard metrics generated for this project"
#file path project_picard_pdf_file=@project.picard.metrics.pdf dir project_dir disp ".picard.metrics.pdf" parent cat_project_picard_data class_level project comment "Plot of select picard metrics for all values in the picard file --- may not correspond one to one with samples"

table file path project_extra_sample_info_file=@project.extra.sample.info dir project_dir disp ".info" parent cat_project_extra_sample_data class_level project comment "Extra sample information --- should be tab delimited, with a header, first column id, each other column desired metrics"


file path project_passed_sample_list_file=@project.passed.samples dir project_dir disp ".passed.samples" parent cat_project_failure_data class_level project comment "List of passed samples"
file path project_failed_sample_list_file=@project.failed.samples dir project_dir disp ".failed.samples" parent cat_project_failure_data class_level project comment "List of failed samples"
table file path project_sample_failure_status_file=@project.samples.failure.status dir project_dir disp ".failure.status" parent cat_project_failure_data class_level project comment "Columns for each fail/pass designation"
#contains information about the regions in the project
table nohead file path project_region_list_file=@project.region.list dir project_dir disp ".region.list" parent cat_project_region_data class_level project comment "List of all regions in the target file"

table nohead file path project_all_regions_dat_file=@project.all_regions.dat dir project_dir disp ".all_regions.dat" parent cat_project_region_data class_level project comment "Dat file listing all regions for this project"

table nohead file path project_interesting_regions_dat_file=@project.interesting_regions.dat dir project_dir disp ".interesting_regions.dat" parent cat_project_region_data class_level project comment "Dat file listing regions that are interesting for this project"

meta_table file path project_interesting_regions_meta_file=@project.interesting_regions.meta dir project_dir disp ".interesting_regions.meta" parent cat_project_region_data class_level project comment "Meta file to specify interesting regions for this project" meta_level region


#contains the variants called by the UG
minor file path project_sample_vcf_id_to_sample_id_file=@project.sample.vcf_id.to.sample_id dir project_dir disp ".sample.vcf_id.to.sample_id" parent cat_project_extra_sample_data class_level project comment "First column: id in VCF file; second column: id in project"

minor file path project_sample_to_call_set_file=@project.sample.to.call_set dir project_dir disp ".sample.to.call_set" parent cat_project_extra_sample_data class_level project comment "First column: id in VCF file; second column: potential call sets; absent samples can come from any call set"

minor file path project_genetic_sex_sexcheck_file=@project.genetic_sex.sexcheck dir project_dir disp ".genetic_sex.sexcheck" parent cat_project_extra_sample_data class_level project comment "Output of sexcheck on genetic sex file"

minor file path project_genetic_sex_sexcheck_log_file=@project.genetic_sex.sexcheck.log dir project_dir disp ".genetic_sex.sexcheck.log" parent cat_project_extra_sample_data class_level project comment "Log file for sexcheck on genetic sex file"

minor file path project_sample_genetic_sex_file=@project.sample.genetic_sex dir project_dir disp ".sample.genetic_sex" parent cat_project_extra_sample_data class_level project comment "Sample ID and genetically determined sex from VCF file"

file path project_sample_bam_list_file=@project.sample.bam.list dir project_dir disp ".sample.bam.list" parent cat_project_extra_sample_data class_level project comment "First column: sample; second column: bam file"

file path project_bam_list_file=@project.bam.list dir project_dir disp ".bam.list" parent cat_project_extra_sample_data class_level project comment "Bam list to feed into calling at all passed samples in project"

file path project_subset_vars_file=@project.subset.vars dir project_dir disp ".subset.vars" parent cat_project_subset_meta_data class_level project comment "Cached list of variants for variant subset"

!!expand:stype:sample:variant! \
meta_table file path project_project_stype_subset_meta_file=@project.project_stype_subset.meta dir project_dir disp ".project_stype_subset.meta" parent cat_project_subset_meta_data class_level project comment "Meta file to load in project_stype_subsets" meta_level project_stype_subset

file path project_project_variant_subset_pheno_expand_subset_file=@project.project_variant_subset.pheno_expand_subset dir project_dir disp ".pheno_expand_subset" parent cat_project_subset_meta_data class_level project comment "Represents the number of pheno subsets each project variant subset should be expanded to" 

meta_table file path project_project_merge_subset_meta_file=@project.project_merge_subset.meta dir project_dir disp ".project_merge_subset.meta" parent cat_project_subset_meta_data class_level project comment "Meta file to load in project_merge_subsets" meta_level project_merge_subset

!!expand:,:type,cattouse:variant,all:snp,snp:indel,indel:multiallelic,multiallelic! \
file path project_type_site_vcf_file=@project.type.site.vcf dir project_dir disp ".type.site.vcf" parent cat_project_cattouse_variant_call_data class_level project comment "An interval list containing all types called at any call_set -- in vcf format -- used for tracking the alleles"

!!expand:project:project:project_merge_subset! \
!!expand:,:type,cattouse:snp,snp:indel,indel! \
file path project_type_id_site_vcf_file=@project.type.id.site.vcf dir project_dir disp ".type.id.site.vcf" parent cat_project_cattouse_variant_call_data class_level project comment "Contains ids and alleles in final VCF; distinct from project_type_site_vcf_file because the ids may change after remerging"

file path nohead table project_variant_subset_var_keep_file=@project_variant_subset.var.keep dir project_variant_subset_dir disp ".var.keep" parent cat_project_variant_subset_all_variant_call_data class_level project_variant_subset comment "List of variants to keep for this subset"

file path nohead table project_variant_subset_interval_list_file=@project_variant_subset.interval_list dir project_variant_subset_dir disp ".interval_list" parent cat_project_variant_subset_all_variant_call_data class_level project_variant_subset comment "Intervals to keep for this subset"

file path project_variant_subset_variant_site_vcf_file=@project_variant_subset.variant.site.vcf dir project_variant_subset_dir disp ".variant.site.vcf" parent cat_project_variant_subset_all_variant_call_data class_level project_variant_subset comment "An interval list containing all variants called at any call_set -- in vcf format -- used for tracking the alleles"

!!expand:project:project:project_variant_subset! \
file path project_clean_all_variant_site_vcf_file=@project.clean.all.variant.site.vcf dir project_dir disp ".variant.site.vcf" parent cat_project_all_variant_call_data class_level project comment "An interval list containing all variants in clean all vcf -- in vcf format -- used for tracking the alleles"

!!expand:,:keyext,fileext:,:_clean,.clean:_indel,.indel:_clean_indel,.clean.indel:_multiallelic,.multiallelic:_clean_multiallelic,.clean.multiallelic! \
file path project_project_merge_subsetkeyext_vcf_list_file=@projectfileext.vcf.list dir project_dir disp "fileext.vcf.list" parent cat_project_all_variant_call_data class_level project comment "List of fileext VCFs to filter"

!!expand:,:keyext,fileext:,:_clean,.clean:_indel,.indel:_clean_indel,.clean.indel:_multiallelic,.multiallelic:_clean_multiallelic,.clean.multiallelic! \
file path project_variant_subset_project_merge_subsetkeyext_vcf_list_file=@project_variant_subsetfileext.vcf.list dir project_variant_subset_dir disp "fileext.vcf.list" parent cat_project_variant_subset_all_variant_call_data class_level project_variant_subset comment "List of fileext VCFs to filter"

!|expand:project:project:project_merge_subset| \
file path project_variant_site_interval_list_file=@project.variant.site.interval_list dir project_dir disp ".variant.site.interval_list" parent cat_project_all_variant_call_data class_level project comment "An interval list containing all types called at any call_set -- in interval list format -- used for tracking the locations"

file path project_merge_subset_call_set_vcf_list_file=@project_merge_subset.call_set.vcf.list dir project_merge_subset_dir disp ".call_set.vcf.list" parent cat_project_merge_subset_all_variant_call_data class_level project_merge_subset comment "List of call sets to use for merge"

#minor file path doubcom table project_initial_sites_vcf_file=@project.initial.sites.vcf dir project_dir disp ".initial.sites.vcf" parent cat_project_variant_call_data class_level project comment "The sites that were called in any batch; merged with intersection. Difference between this and initial_vcf is that this merge is only over batches in which variant was called, rather than all batches"

!|expand:project:project:project_merge_subset| \
file path doubcom table project_merged_vcf_file=@project.merged.vcf@zip_vcf dir project_dir disp ".merged.vcf" parent cat_project_all_variant_call_data class_level project comment "Output of raw CombineVariants step on call set VCF files"

!|expand:project:project:project_merge_subset| \
path project_for_genetic_sex_plink_file=@project.for_genetic_sex dir project_dir

!|expand:ext:tped:tfam| \
!|expand:project:project:project_merge_subset| \
file path doubcom table project_for_genetic_sex_ext_file=$project_for_genetic_sex_plink_file.ext dir project_dir disp ".for_genetic_sex.ext" parent cat_project_all_variant_call_data class_level project comment "Variants on the X chromosome to be used for genetic sex determination; ext file"

#!|expand:project:project:project_merge_subset| \
#file path doubcom table project_initial_annotated_site_vcf_file=@project.initial.annotated.vcf dir project_dir disp ".initial.annotated.vcf" parent cat_project_all_variant_call_data class_level project comment "VCF file with annotations --- not yet processed to make final initial file "

#!|expand:project:project:project_merge_subset| \
#file path doubcom table project_annotated_vcf_file=@project.annotated.vcf@zip_vcf dir project_dir disp ".annotated.vcf" parent cat_project_all_variant_call_data class_level project comment "VCF file with annotations --- processed to make final initial file "

#!|expand:project:project:project_merge_subset| \
#!!expand:,:type,ext:snp,:indel,.indel! \
#file path doubcom table project_initial_type_vcf_file=@project.initialext.vcf@zip_vcf dir project_dir disp ".initialext.vcf" parent cat_project_type_variant_call_data class_level project comment "All type calls for the project. Generated via multisample caller by firehose for each call_set and then merged"

#!|expand:project:project:project_merge_subset| \
#!!expand:,:type,ext:snp,:indel,.indel! \
#file path doubcom table project_subsetted_type_vcf_file=@project.subsettedext.vcf@zip_vcf dir project_dir disp ".subsetted.vcf" parent cat_project_type_variant_call_data class_level project comment "Initial type VCF file subsetted to include only passed samples present in the project"

file path onecol nohead table project_sample_subset_samp_keep_file=@project_sample_subset.samp.keep dir project_sample_subset_dir disp ".samp.keep" parent cat_project_sample_subset_snp_variant_call_data class_level project_sample_subset comment "List of samples to keep for this subset"

file path onecol nohead table project_sample_subset_samp_exclude_file=@project_sample_subset.samp.exclude dir project_sample_subset_dir disp ".samp.exclude" parent cat_project_sample_subset_snp_variant_call_data class_level project_sample_subset comment "List of samples to exclude for this subset"

file path onecol nohead table project_sample_subset_clean_samp_keep_file=@project_sample_subset.clean.samp.keep dir project_sample_subset_dir disp ".clean.samp.keep" parent cat_project_sample_subset_snp_variant_call_data class_level project_sample_subset comment "List of samples that are in this subset for the clean file"

!!expand:project:project:project_sample_subset:project_variant_subset:project_merge_subset! \
major file path doubcom table project_vcf_file=@project.vcf@zip_vcf dir project_dir disp ".vcf" parent cat_project_snp_variant_call_data class_level project comment "VCF file for project; initial file with extra degeneracy annotation"

!!expand:project:project:project_sample_subset:project_variant_subset:project_merge_subset! \
major file path doubcom table project_indel_vcf_file=@project.indel.vcf@zip_vcf dir project_dir disp ".indel.vcf" parent cat_project_indel_variant_call_data class_level project comment "Indel VCF file for project"

!!expand:project:project:project_sample_subset:project_variant_subset:project_merge_subset! \
major file path doubcom table project_multiallelic_vcf_file=@project.multiallelic.vcf@zip_vcf dir project_dir disp ".multiallelic.vcf" parent cat_project_multiallelic_variant_call_data class_level project comment "Multiallelic VCF file for project"

!!expand:project:project:project_merge_subset! \
minor file path table project_pos_transcript_file=@project.pos.transcript dir project_dir disp ".pos.transcript" parent cat_project_all_variant_call_data class_level project comment "Annotation of each variant position with transcript"

#!!expand:projectt:project:project_sample_subset:project_variant_subset! \
#constant projectt_indel_project_exon_browser=Open disp "Exon Browser" parent cat_projectt_plinkseq_indel_project_data goto_url ${plinkseq_exome_browser_url}?proj=*projectt_plinkseq_indel_project_file&loc=$pseq_genes_loc_group comment "Link to Plink/Seq exon browser for indel projectt variants"
!!expand:projectt:project:project_sample_subset:project_variant_subset! \
#table nohead file path projectt_plinkseq_indel_project_file=@projectt.plinkseq.indel.project dir projectt_dir disp ".plinkseq.indel.project" parent cat_projectt_plinkseq_indel_project_data class_level projectt comment "Plink/Seq indel project file"
#!!expand:projectt:project:project_sample_subset:project_variant_subset! \
#mkdir path projectt_plinkseq_indel_project_out_dir=*{projectt_plinkseq_indel_project_file}_out class_level projectt chmod 777
#!!expand:projectt:project:project_sample_subset:project_variant_subset! \
#mkdir path projectt_plinkseq_indel_project_temp_dir=*{projectt_plinkseq_indel_project_file}_temp class_level projectt chmod 777
#!!expand:projectt:project:project_sample_subset:project_variant_subset! \
#file path projectt_plinkseq_indel_vardb_file=vardb dir projectt_plinkseq_indel_project_out_dir disp "vardb" parent cat_projectt_plinkseq_indel_project_data class_level projectt comment "Indel Plink/Seq vardb file"
#!!expand:projectt:project:project_sample_subset:project_variant_subset! \
#file path projectt_plinkseq_indel_inddb_file=inddb dir projectt_plinkseq_indel_project_out_dir disp "inddb" parent cat_projectt_plinkseq_indel_project_data class_level projectt comment "Indel Plink/Seq inddb file"

#!!expand:projectt:project:project_sample_subset:project_variant_subset! \
#file path projectt_plinkseq_indel_project_done_file=.$projectt_plinkseq_indel_project_file.done dir projectt_dir disp "project.done" parent cat_projectt_plinkseq_indel_project_data class_level projectt comment "Signifies that indel project was done"
#!!expand:projectt:project:project_sample_subset:project_variant_subset! \
#file path projectt_plinkseq_indel_db_done_file=db.done dir projectt_plinkseq_indel_project_out_dir disp "db.done" parent cat_projectt_plinkseq_indel_project_data class_level projectt comment "Signifies that indel db was done"
#!!expand:projectt:project:project_sample_subset:project_variant_subset! \
#file path projectt_plinkseq_indel_temp_done_file=temp.done dir projectt_plinkseq_indel_project_temp_dir disp "temp.done" parent cat_projectt_plinkseq_indel_project_data class_level projectt comment "Signifies that indel temp dir was made"

#major file path doubcom table project_samtools_vcf_file=@project.samtools.vcf dir project_dir disp ".samtools.vcf" parent cat_project_samtools_call_data class_level project comment "VCF file for project --- generated by SAMTOOLS"

#minor file path doubcom table project_plinkseq_vcf_file=@project.plinkseq.vcf dir project_dir disp ".plinkseq.vcf" parent cat_project_call_data class_level project comment "VCF file for project in format Plinkseq can read; bug in reading 4.0"

!!expand:project:project:project_sample_subset:project_variant_subset:project_merge_subset! \
major file path doubcom table project_clean_vcf_file=@project.clean.vcf@zip_vcf dir project_dir disp ".clean.vcf" parent cat_project_snp_variant_call_data class_level project comment "Clean VCF file for project; outlier samples and variants removed"

!!expand:project:project:project_sample_subset:project_variant_subset:project_merge_subset! \
major file path doubcom table project_clean_indel_vcf_file=@project.clean.indel.vcf@zip_vcf dir project_dir disp ".clean.indel.vcf" parent cat_project_indel_variant_call_data class_level project comment "Clean Indel VCF file for project"

!!expand:project:project:project_sample_subset:project_variant_subset:project_merge_subset! \
major file path doubcom table project_clean_multiallelic_vcf_file=@project.clean.multiallelic.vcf@zip_vcf dir project_dir disp ".clean.multiallelic.vcf" parent cat_project_multiallelic_variant_call_data class_level project comment "Clean multiallelic VCF file for project"

!!expand:project:project:project_variant_subset:project_merge_subset! \
major file path doubcom table project_clean_all_vcf_file=@project.clean.all.vcf.gz dir project_dir disp ".clean.all.vcf" parent cat_project_all_variant_call_data class_level project comment "Clean VCF file with all variants for project"

#major file path doubcom table project_samtools_clean_vcf_file=@project.samtools.clean.vcf dir project_dir disp ".samtools.clean.vcf" parent cat_project_samtools_call_data class_level project comment "VCF file for project with QC- samples removed --- generated by SAMTOOLS"

!!expand:project:project:project_variant_subset! \
file path doubcom table project_gene_variant_file=@project.gene.variant.tsv dir project_dir disp ".gene.variant.tsv" parent cat_project_all_variant_call_data class_level project comment "List of gene, variant for each variant in vcf file"

!!expand:project:project:project_variant_subset! \
file path doubcom table project_clean_gene_variant_file=@project.clean.gene.variant.tsv dir project_dir disp ".clean.gene.variant.tsv" parent cat_project_all_variant_call_data class_level project comment "List of gene, variant for each variant in clean vcf file"

minor file path doubcom table project_variant_subset_gene_burden_size_file=@project_variant_subset.gene_burden.size dir project_variant_subset_dir disp ".gene_burden.size" parent cat_project_variant_subset_annot_data class_level project_variant_subset comment "Helper file listing the 'size' of this variant subset for use in splitting later at the pheno level"


#minor file path doubcom table project_clean_plinkseq_vcf_file=@project.clean.plinkseq.vcf dir project_dir disp ".clean.plinkseq.vcf" parent cat_project_call_data class_level project comment "Clean VCF file for project; outlier samples and variants removed; format Plinkseq can read"

major file path project_vcf_eval_file=@project.eval dir project_dir disp ".eval" parent cat_project_call_all_bulk_properties_data class_level project comment "Eval statistics on project variants" child_order -1

table file path project_theta_eval_file=@project.eval.theta.csv dir project_dir disp ".theta.csv" parent cat_project_call_theta_bulk_properties_data class_level project comment "Estimates of theta based on variant and genotype calls"

#!!expand:,:ext,weblevel:dat,table:pdf,major! \
#!!expand:qc_type:qc_pass! \
#weblevel file path project_qc_type_theta_ac_ext_file=@project.qc_type.theta.ac.ext dir project_dir disp ".qc_type.theta.ac.ext" parent cat_project_call_theta_bulk_properties_data class_level project comment "Plot of theta estimate for each allele count --- ext file for qc_type variants"

table file path project_titv_eval_file=@project.eval.titv.csv dir project_dir disp ".titv.csv" parent cat_project_call_titv_bulk_properties_data class_level project comment "Estimates of transition transversion ratio based on variant calls"

table file path project_titv_ac_file=@project.titv.ac.csv dir project_dir disp ".titv.ac.csv" parent cat_project_call_titv_bulk_properties_data class_level project comment "TiTv by allele count"
table file path project_titv_ac_binned_file=@project.titv.ac.binned.csv dir project_dir disp ".titv.ac.binned.csv" parent cat_project_call_titv_bulk_properties_data class_level project comment "TiTv by allele count; binned to have at least $num_per_titv_ac_bin variants per bin"
major file path project_titv_ac_pdf_file=@project.titv.ac.pdf dir project_dir disp ".titv.ac.pdf" parent cat_project_call_titv_bulk_properties_data class_level project comment "TiTv by allele count; binned to have at least $num_per_titv_ac_bin variants per bin; pdf file"

!!expand:project:project:project_variant_subset! \
table file path project_snpeff_file=@project.snpeff.tsv dir project_dir disp ".snpeff" parent cat_project_annot_data class_level project comment "List of SNPEff annotations for all variants"

!!expand:project:project:project_variant_subset! \
table file path project_snpsift_file=@project.snpsift.tsv dir project_dir disp ".snpsift" parent cat_project_annot_data class_level project comment "List of SNPSift annotations for all variants"

!!expand:project:project:project_variant_subset! \
table file path project_chaos_vcf_file=@project.chaos.vcf dir project_dir disp ".chaos.vcf" parent cat_project_annot_data class_level project comment "List of Chaos annotations for all variants, in VCF format"

!!expand:project:project:project_variant_subset! \
table file path project_chaos_file=@project.chaos dir project_dir disp ".chaos" parent cat_project_annot_data class_level project comment "List of Chaos annotations for all variants, extracted from VCF"

!!expand:project:project:project_variant_subset! \
table file path project_vep_file=@project.vep.tsv dir project_dir disp ".vep" parent cat_project_annot_data class_level project comment "List of Variant Effect Predictor annotations for all variants"

!!expand:project:project:project_variant_subset! \
table file path project_parsed_vep_file=@project.parsed_vep.tsv dir project_dir disp ".parsed_vep" parent cat_project_annot_data class_level project comment "List of Variant Effect Predictor annotations for all variants; parsed for better formatting"

!!expand:project:project:project_variant_subset! \
table file path project_var_transcript_file=@project.var_transcript.tsv dir project_dir disp ".var_transcript.tsv" parent cat_project_annot_data class_level project comment "Transcript to use for each variant (global annotations)"

!!expand:project:project:project_variant_subset! \
table file path project_pph2_pos_file=@project.pph2.pos.tsv dir project_dir disp ".pph2.pos" parent cat_project_annot_data class_level project comment "List of PPH2 scores for all locations that contain variants"

!!expand:project:project:project_variant_subset! \
table file path project_pph2_file=@project.pph2.tsv dir project_dir disp ".pph2" parent cat_project_annot_data class_level project comment "List of PPH2 scores for all variants"

table file path project_reduced_trans_file=@project.reduced.trans dir project_dir disp ".reduced.trans" parent cat_project_annot_data class_level project comment "List of transcripts to restrict annotations to (specify this directly)"

table file path project_effect_size_map_file=@project.effect_size.map dir project_dir disp ".effect_size.map" parent cat_project_annot_data class_level project comment "Numerical mapping to effect size; format is TAG (one of VEP, SNPEFF, CHAOS), an comma delimited entry of effects to match exactly, and a number (smaller is more impactful). Specify this directly."

!!expand:project:project:project_variant_subset! \
table file path project_custom_var_annot_file=@project.custom_var_annot.tsv dir project_dir disp ".custom_var_annot" parent cat_project_annot_data class_level project comment "List of custom annotations (if specified)"

!!expand:project:project:project_variant_subset! \
table file path project_custom_trans_annot_file=@project.custom_trans_annot.tsv dir project_dir disp ".custom_trans_annot" parent cat_project_annot_data class_level project comment "List of custom annotations (with transcript key added)"

!!expand:project:project:project_variant_subset! \
table file path project_complete_full_annot_file=@project.complete.full_annot.tsv dir project_dir disp ".complete.full_annot" parent cat_project_annot_data class_level project comment "Full list of all combined annotations"

!!expand:project:project:project_variant_subset! \
table file path project_full_annot_file=@project.full_annot.tsv dir project_dir disp ".full_annot" parent cat_project_annot_data class_level project comment "Full list of all combined annotations; subset to allowable transcripts"

!!expand:project:project:project_variant_subset! \
table file path project_full_var_annot_file=@project.full_var_annot.tsv dir project_dir disp ".full_var_annot" parent cat_project_annot_data class_level project comment "Full list of all combined annotations, with most severe annotation chosen for each variant"

!!expand:project:project:project_variant_subset! \
!!expand:,:maskkey,masktype,maskdescrip:syn,syn.,synonymous:ns,ns.,non-synonymous:nonsense,nonsense.,nonsense:coding,coding.,coding! \
 table file path project_plinkseq_maskkey_reg_file=@project.masktypereg dir project_dir disp ".masktypereg" parent cat_project_mask_data class_level project comment "Mask for Plink/Seq i-stats using maskdescrip variants"


#!!expand:project:project:project_variant_subset! \
#table file path project_annot_pseq_file=@project.annot.pseq dir project_dir disp ".annot.pseq" parent cat_project_annot_data class_level project comment "Annotations converted into Plink/SEQ format"

#plinkseq data
!!expand:projectt:project:project_sample_subset:project_variant_subset! \
constant projectt_project_exon_browser=Open disp "Exon Browser" parent cat_projectt_plinkseq_project_data goto_url ${plinkseq_exome_browser_url}?proj=*projectt_plinkseq_project_file&loc=$pseq_genes_loc_group comment "Link to Plink/Seq exon browser for projectt variants"
!!expand:projectt:project:project_sample_subset:project_variant_subset! \
table nohead file path projectt_plinkseq_project_file=@projectt.plinkseq.project.pseq dir projectt_dir disp ".plinkseq.project" parent cat_projectt_plinkseq_project_data class_level projectt comment "Plink/Seq project file"
!!expand:projectt:project:project_sample_subset! \
nohead table file path projectt_plinkseq_ind_info_file=@projectt.ind.info dir projectt_dir disp ".ind.info" parent cat_projectt_plinkseq_project_data class_level projectt comment "Ind file for import into Plink/Seq"

!!expand:projectt:project:project_sample_subset:project_variant_subset! \
mkdir path projectt_plinkseq_project_out_dir=*{projectt_plinkseq_project_file}_out class_level projectt chmod 777
!!expand:projectt:project:project_sample_subset:project_variant_subset! \
mkdir path projectt_plinkseq_project_temp_dir=*{projectt_plinkseq_project_file}_temp class_level projectt chmod 777
!!expand:projectt:project:project_sample_subset:project_variant_subset! \
file path projectt_plinkseq_vardb_file=vardb dir projectt_plinkseq_project_out_dir disp "vardb" parent cat_projectt_plinkseq_project_data class_level projectt comment "Plink/Seq vardb file"
!!expand:projectt:project:project_sample_subset:project_variant_subset! \
file path projectt_plinkseq_inddb_file=inddb dir projectt_plinkseq_project_out_dir disp "inddb" parent cat_projectt_plinkseq_project_data class_level projectt comment "Plink/Seq inddb file"
!!expand:projectt:project:project_sample_subset:project_variant_subset! \
file path projectt_plinkseq_project_done_file=.$projectt_plinkseq_project_file.done dir projectt_dir disp "project.done" parent cat_projectt_plinkseq_project_data class_level projectt comment "Signifies the varproject was built"
!!expand:projectt:project:project_sample_subset:project_variant_subset! \
ignore_md5 file path projectt_plinkseq_db_done_file=db.done dir projectt_plinkseq_project_out_dir disp "db.done" parent cat_projectt_plinkseq_project_data class_level projectt comment "Signifies the vardb was built"
!!expand:projectt:project:project_sample_subset:project_variant_subset! \
file path projectt_plinkseq_temp_done_file=temp.done dir projectt_plinkseq_project_temp_dir disp "temp.done" parent cat_projectt_plinkseq_project_data class_level projectt comment "Signifies the temp dir was made"
!!expand:projectt:project:project_variant_subset! \
file path projectt_plinkseq_locdb_file=locdb dir projectt_dir disp "locdb" parent cat_projectt_plinkseq_project_data class_level projectt comment "Plink/Seq locdb file"

file path project_plinkseq_seqdb_file=seqdb dir project_dir disp "seqdb" parent cat_project_plinkseq_project_data class_level project comment "Plink/Seq seqdb file"

#table nohead file path project_generic_locset_file=@project.generic.locset.list dir project_dir disp "generic.locset.list" parent cat_project_plinkseq_project_data class_level project comment "List of locsets for pathways; generic list applicable to any project"

table nohead file path project_custom_locset_file=@project.custom.locset.list dir project_dir disp "custom.locset.list" parent cat_project_plinkseq_project_data class_level project comment "List of locsets custom to this project"

!!expand:pathwayburdentype:custom! \
table nohead file path project_pathway_pathwayburdentype_flat_locset_file=@project.pathwayburdentype.flat.locset.list dir project_dir disp "pathwayburdentype.flat.locset.list" parent cat_project_plinkseq_project_data class_level project comment "List of locsets pathwayburdentype to this project --- one row per gene set"

!!expand:projectt:project:project_sample_subset:project_variant_subset! \
constant projectt_clean_project_exon_browser=Open disp "Exon Browser" parent cat_projectt_plinkseq_clean_project_data goto_url ${plinkseq_exome_browser_url}?proj=*projectt_plinkseq_clean_project_file&loc=$pseq_genes_loc_group comment "Link to Plink/Seq exon browser for clean projectt variants"
!!expand:projectt:project:project_sample_subset:project_variant_subset! \
table nohead file path projectt_plinkseq_clean_project_file=@projectt.plinkseq.clean.project.pseq dir projectt_dir disp ".plinkseq.clean.project" parent cat_projectt_plinkseq_clean_project_data class_level projectt comment "Plink/Seq clean project file"
!!expand:projectt:project:project_sample_subset:project_variant_subset! \
mkdir path projectt_plinkseq_clean_project_out_dir=*{projectt_plinkseq_clean_project_file}_out class_level projectt chmod 777
!!expand:projectt:project:project_sample_subset:project_variant_subset! \
mkdir path projectt_plinkseq_clean_project_temp_dir=*{projectt_plinkseq_clean_project_file}_temp class_level projectt chmod 777
!!expand:projectt:project:project_sample_subset:project_variant_subset! \
file path projectt_plinkseq_clean_vardb_file=vardb dir projectt_plinkseq_clean_project_out_dir disp "vardb" parent cat_projectt_plinkseq_clean_project_data class_level projectt comment "Clean Plink/Seq vardb file"
!!expand:projectt:project:project_sample_subset:project_variant_subset! \
file path projectt_plinkseq_clean_inddb_file=inddb dir projectt_plinkseq_clean_project_out_dir disp "inddb" parent cat_projectt_plinkseq_clean_project_data class_level projectt comment "Clean Plink/Seq inddb file"
!!expand:projectt:project:project_sample_subset:project_variant_subset! \
file path projectt_plinkseq_clean_project_done_file=.$projectt_plinkseq_clean_project_file.done dir projectt_dir disp "project.done" parent cat_projectt_plinkseq_clean_project_data class_level projectt comment "Signifies that clean project was done"
!!expand:projectt:project:project_sample_subset:project_variant_subset! \
ignore_md5 file path projectt_plinkseq_clean_db_done_file=db.done dir projectt_plinkseq_clean_project_out_dir disp "db.done" parent cat_projectt_plinkseq_clean_project_data class_level projectt comment "Signifies that clean db was done"
!!expand:projectt:project:project_sample_subset:project_variant_subset! \
file path projectt_plinkseq_clean_temp_done_file=temp.done dir projectt_plinkseq_clean_project_temp_dir disp "temp.done" parent cat_projectt_plinkseq_clean_project_data class_level projectt comment "Signifies that clean temp dir was made"

#constant samtools_clean_project_exon_browser=Open disp "Exon Browser" parent cat_project_plinkseq_samtools_clean_project_data goto_url ${plinkseq_exome_browser_url}?proj=*project_plinkseq_samtools_clean_project_file&loc=$pseq_genes_loc_group comment "Link to Plink/Seq exon browser for samtools clean project variants"
#table nohead file path project_plinkseq_samtools_clean_project_file=@project.plinkseq.samtools.clean.project dir project_dir disp ".plinkseq.samtools.clean.project" parent cat_project_plinkseq_samtools_clean_project_data class_level project comment "Plink/Seq samtools clean project file"
#mkdir path project_plinkseq_samtools_clean_project_out_dir=*{project_plinkseq_samtools_clean_project_file}_out class_level project chmod 777
#mkdir path project_plinkseq_samtools_clean_project_temp_dir=*{project_plinkseq_samtools_clean_project_file}_temp class_level project chmod 777
#minor file path project_plinkseq_samtools_clean_vardb_file=vardb dir project_plinkseq_samtools_clean_project_out_dir disp "vardb" parent cat_project_plinkseq_samtools_clean_project_data class_level project comment "Samtools clean Plink/Seq vardb file"
#minor file path project_plinkseq_samtools_clean_inddb_file=inddb dir project_plinkseq_samtools_clean_project_out_dir disp "inddb" parent cat_project_plinkseq_samtools_clean_project_data class_level project comment "Samtools clean Plink/Seq inddb file"
"""
}
    