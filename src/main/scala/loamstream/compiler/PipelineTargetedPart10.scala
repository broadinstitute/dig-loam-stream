
package loamstream.compiler

/**
 * LoamStream
 * Created by oliverr on 10/6/2015.
 */
object PipelineTargetedPart10 {
  val string =
 """
!!expand:,:filetype,weblevel:tex,minor:pdf,major! \
weblevel file path burden_slide_common_vassoc_meta_trait_filetype_file=$burden_slide_common_vassoc_meta_trait_trunk.filetype trunk burden_slide_common_vassoc_meta_trait_trunk dir burden_dir disp ".slide.common.vassoc.meta_trait.filetype" parent cat_burden_summary_data class_level burden comment "List of top associated variants under this burden grouping --- filetype file"

burden_slide_vassoc_meta_trait_trunk=@burden.slide.vassoc.meta_trait

!!expand:,:filetype,weblevel:tex,minor:pdf,major! \
weblevel file path burden_slide_vassoc_meta_trait_filetype_file=$burden_slide_vassoc_meta_trait_trunk.filetype trunk burden_slide_vassoc_meta_trait_trunk dir burden_dir disp ".slide.vassoc.meta_trait.filetype" parent cat_burden_summary_data class_level burden comment "List of top associated variants under this burden grouping --- filetype file"

burden_slide_unique_meta_trait_trunk=@burden.slide.unique.meta_trait

!!expand:,:filetype,weblevel:tex,minor:pdf,major! \
weblevel file path burden_slide_unique_meta_trait_filetype_file=$burden_slide_unique_meta_trait_trunk.filetype trunk burden_slide_unique_meta_trait_trunk dir burden_dir disp ".slide.unique.meta_trait.filetype" parent cat_burden_summary_data class_level burden comment "List of top associated variants under this burden grouping --- filetype file"

burden_slide_gassoc_trunk=@burden.slide.gassoc
!!expand:,:filetype,weblevel:tex,minor:pdf,major! \
weblevel file path burden_slide_gassoc_filetype_file=$burden_slide_gassoc_trunk.filetype trunk burden_slide_gassoc_trunk dir burden_dir disp ".slide.gassoc.filetype" parent cat_burden_summary_data class_level burden comment "List of top associated genes under this burden grouping --- filetype file"

#table file path burden_sample_list_file=@burden.sample.list dir burden_dir disp ".sample.list" parent cat_burden_summary_data class_level burden comment "List of samples who have variants for this burden"

#table file path burden_sample_without_list_file=@burden.sample.without.list dir burden_dir disp ".sample.without.list" parent cat_burden_summary_data class_level burden comment "List of samples who do not have variants for this burden"

#major file path burden_pheno_pdf_file=@burden.pheno.pdf dir burden_dir disp ".pheno.pdf" parent cat_burden_summary_data class_level burden comment "Plot of phenotypes for samples with and without variant"

!!expand:pathwayburdentype:custom! \
major table path file burden_pathway_pathwayburdentype_gassoc_file=@burden.pathway.pathwayburdentype.gassoc dir burden_dir disp ".pathwayburdentype.gassoc" parent cat_burden_pathway_association_data class_level burden comment "Pathway level pathwayburdentype associations"

!!expand:pathwayburdentype:custom! \
doubcom major table path file burden_test_pathway_pathwayburdentype_gassoc_file=@burden_test.pathway.pathwayburdentype.gassoc dir burden_test_dir disp ".pathwayburdentype.gassoc" parent cat_burden_test_pathway_association_data class_level burden_test comment "Pathway level pathwayburdentype associations"

meta_table file path annot_annot_variant_subset_meta_file=@annot.annot_variant_subset.meta dir annot_dir disp ".annot_variant_subset.meta" parent cat_annot_subset_info class_level annot comment "Meta file to load in annot_variant_subsets" meta_level annot_variant_subset

meta_table file path burden_burden_variant_subset_meta_file=@burden.burden_variant_subset.meta dir burden_dir disp ".burden_variant_subset.meta" parent cat_burden_subset_info class_level burden comment "Meta file to load in burden_variant_subsets" meta_level burden_variant_subset

meta_table file path burden_burden_test_variant_subset_meta_file=@burden.burden_test_variant_subset.meta dir burden_dir disp ".burden_test_variant_subset.meta" parent cat_burden_subset_info class_level burden comment "Meta file to load in burden_test_variant_subsets" meta_level burden_test_variant_subset


#!!expand:,:name,ext,weblevel:locus_level,locus.level,table:sum,sum,table:pdf,pdf,:rv,rv,table:recessive,recessive,! \
#weblevel file path burden_haplotype_burden_name_file=@burden.hap.burden.ext dir burden_dir disp ".hap.burden.ext" parent cat_burden_haplotype_burden_data class_level burden comment "Tabular output of haplotype specific burden calculations --- name file"

#region or locus files


!!expand:rorl:region:locus! \
path rorl_all_marker_plink_file=@rorl.all.marker dir rorl_dir
!!expand:rorl:region:locus! \
!!expand:ext:bed:bim:fam:tfam:tped! \
file path rorl_all_marker_ext_file=$rorl_all_marker_plink_file.ext dir rorl_dir disp ".ext" parent cat_rorl_ped_all_marker_data class_level rorl comment "Entire project marker file projected to contain only SNPs around this rorl (filtering snps with low CR) --- ext file"
!!expand:rorl:region! \
file path rorl_all_marker_make_bed_log_file=${rorl_all_marker_plink_file}.make_bed.log dir rorl_dir disp ".make_bed.log" parent cat_rorl_ped_all_marker_data class_level rorl comment "Log file for construction of rorl_all_marker_plink_data"
!!expand:rorl:region! \
file path rorl_all_marker_recode_log_file=${rorl_all_marker_plink_file}.recode.log dir rorl_dir disp ".recode.log" parent cat_rorl_ped_all_marker_data class_level rorl comment "Log file for construction of rorl_all_marker_plink_data"


file path region_all_marker_freq_file=${region_all_marker_plink_file}.frq dir region_dir disp ".frq" parent cat_region_ped_all_marker_data class_level region comment "Frequencies for SNPs"
file path region_all_marker_freq_log_file=${region_all_marker_plink_file}.frq.log dir region_dir disp ".frq.log" parent cat_region_ped_all_marker_data class_level region comment "Frequencies for SNPs"
file path region_rare_marker_snp_list=@region.rare.snp.list dir region_dir disp ".rare.snp.list" parent cat_region_ped_common_marker_data class_level region comment "List of rare SNPs"

!!expand:rorl:region:locus! \
path rorl_common_marker_plink_file=@rorl.common.marker dir rorl_dir
!!expand:rorl:region:locus! \
!!expand:ext:tped:tfam! \
file path rorl_common_marker_ext_file=$rorl_common_marker_plink_file.ext dir rorl_dir disp ".ext" parent cat_rorl_ped_common_marker_data class_level rorl comment "Filtering out rare SNPs from all_marker_file --- ext file"
!!expand:rorl:region! \
file path rorl_common_marker_recode_log_file=${rorl_common_marker_plink_file}.recode.log dir rorl_dir disp ".recode.log" parent cat_rorl_ped_common_marker_data class_level rorl comment "Log file for construction of rorl_common_marker_plink_data"

!!expand:rorl:region:locus! \
path rorl_all_seq_plink_file=@rorl.all.seq dir rorl_dir
!!expand:rorl:region:locus! \
!!expand:ext:bed:bim:fam:tfam:tped! \
file path rorl_all_seq_ext_file=$rorl_all_seq_plink_file.ext dir rorl_dir disp ".ext" parent cat_rorl_ped_all_seq_data class_level rorl comment "All SNPs called in sequencing --- ext file"
!!expand:rorl:region! \
file path rorl_all_seq_make_bed_log_file=${rorl_all_seq_plink_file}.make_bed.log dir rorl_dir disp ".make_bed.log" parent cat_rorl_ped_all_seq_data class_level rorl comment "Log file for construction of rorl_all_seq_plink_data"
!!expand:rorl:region! \
file path rorl_all_seq_recode_log_file=${rorl_all_seq_plink_file}.recode.log dir rorl_dir disp ".recode.log" parent cat_rorl_ped_all_seq_data class_level rorl comment "Log file for construction of rorl_all_seq_plink_data"


!!expand:rorl:region:locus! \
path rorl_non_marker_seq_plink_file=@rorl.non.marker.seq dir rorl_dir
!!expand:rorl:region:locus! \
!!expand:ext:bed:bim:fam! \
file path rorl_non_marker_seq_ext_file=$rorl_non_marker_seq_plink_file.ext dir rorl_dir disp ".ext" parent cat_rorl_ped_non_marker_seq_data class_level rorl comment "SNPs called in sequencing but not in marker file --- ext file"
!!expand:rorl:region! \
file path rorl_non_marker_seq_make_bed_log_file=${rorl_non_marker_seq_plink_file}.make_bed.log dir rorl_dir disp ".make_bed.log" parent cat_rorl_ped_non_marker_seq_data class_level rorl comment "Log file for construction of non_markerseq_plink_data"
!!expand:rorl:region:locus! \
file path rorl_duplicate_marker_names_file=@rorl.duplicate.marker.names dir rorl_dir disp ".duplicate.marker.names" parent cat_rorl_ped_non_marker_seq_data class_level rorl comment "List of variants that occur in both marker and non marker plink files"

!!expand:rorl:region:locus! \
path rorl_combined_plink_file=@rorl.combined dir rorl_dir
!!expand:rorl:region:locus! \
!!expand:ext:tped:tfam:ped:map! \
file path rorl_combined_ext_file=$rorl_combined_plink_file.ext dir rorl_dir disp ".ext" parent cat_rorl_ped_combined_data class_level rorl comment "Merged marker and seq data --- if conflicts, taking marker files"
!!expand:rorl:region:locus! \
file path rorl_combined_haploview_ped_file=$rorl_combined_plink_file.haploview.ped dir rorl_dir disp ".haploview.ped" parent cat_rorl_ped_combined_data class_level rorl comment "Switch pheno from -9 to 0 to respect haploview conventions"

!!expand:rorl:region:locus! \
table nohead file path rorl_haploview_info_file=@rorl.haploview.info dir rorl_dir disp ".haploview.info" parent cat_rorl_marker_association_data class_level rorl comment "Haploview info file for this rorl"
!!expand:rorl:region:locus! \
rorl_haploview_ld_trunk=@rorl.haploview dir rorl_dir

!!expand:rorl:region:locus! \
table file path rorl_haploview_ld_file=$rorl_haploview_ld_trunk.LD dir rorl_dir disp ".haploview.LD" parent cat_rorl_marker_association_data class_level rorl comment "Haploview LD values for between each pair of variants"


!!expand:rorl:region! \
file path rorl_combined_merge_log_file=${rorl_combined_plink_file}.merge.log dir rorl_dir disp ".merge.log" parent cat_rorl_ped_combined_data class_level rorl comment "Log file for construction of rorl_combined_plink_data"

!!expand:rorl:region! \
file path rorl_combined_transpose_log_file=${rorl_combined_plink_file}.transpose.log dir rorl_dir disp ".transpose.log" parent cat_rorl_ped_combined_data class_level rorl comment "Log file for construction of rorl_combined_plink_data"


!!expand:rorl:region:locus! \
minor file path rorl_range_info_file=@rorl.range.info dir rorl_dir disp ".range.info" parent cat_rorl_meta_data class_level rorl comment "The range of the rorl --- for use in ensuring that files are outdated if range ever changes"

!!expand:rorl:region! \
table path file rorl_beagle_input_file=@rorl.bgl dir rorl_dir disp ".bgl" parent cat_rorl_beagle_input_data class_level rorl comment "Input file for beagle"

!!expand:rorl:region:locus! \
!!expand:,:compression,ext,tabletype:_gz,.gz,:,,table! \
!!expand:btype:phased:gprobs! \
file tabletype path rorl_beagle_btypecompression_file=@rorl.bgl.btypeext dir rorl_dir disp ".bgl.btypeext" parent cat_rorl_beagle_output_data class_level rorl comment "Output beagle file: btypeext"

!!expand:rorl:region:locus! \
file path rorl_beagle_phased_gz_log_file=@rorl.bgl.phased.gz.log dir rorl_dir disp ".bgl.phased.gz.log" parent cat_rorl_beagle_output_data class_level rorl comment "Log file for beagle run"

!!expand:rorl:region:locus! \
file table nohead path rorl_beagle_r2_file=@rorl.bgl.r2 dir rorl_dir disp ".bgl.r2" parent cat_rorl_beagle_output_data class_level rorl comment "Output beagle file: r2"

!!expand:rorl:region:locus! \
!!expand:,:name,ext,keymod:ref_genotype,ref.gens,nohead:strand,strand,nohead:genotype,gens,nohead! \
table keymod file path rorl_combined_name_file=$rorl_combined_plink_file.ext dir rorl_dir disp ".ext" parent cat_rorl_impute2_input_data class_level rorl comment "Input for IMPUTE2 --- .ext file"

!!expand:rorl:region:locus! \
!!expand:,:name,ext,keymod:,,table nohead:_info,_info,table:_info_by_sample,_info_by_sample,table:_summary,_summary,:_warnings,_warnings,! \
file keymod path rorl_combined_impute2name_file=$rorl_combined_plink_file.impute2ext dir rorl_dir disp ".impute2ext" parent cat_rorl_impute2_output_data class_level rorl comment "Output of IMPUTE2 --- impute2ext file"

!!expand:rorl:region:locus! \
file table nohead path rorl_combined_merged_impute2_file=$rorl_combined_plink_file.merged.impute2 dir rorl_dir disp ".merged.impute2" parent cat_rorl_impute2_output_data class_level rorl comment "Merged output of impute2 with gens file; if a marker SNP was excluded from imputation because it was rare, it is in this file with its gens counts"


!!expand:rorl:locus! \
!!expand:,:name,ext,keymod:ref_sample,ref.sample,:sample,sample,! \
table keymod file path rorl_combined_name_file=$rorl_combined_plink_file.ext dir rorl_dir disp ".ext" parent cat_rorl_impute2_association_data class_level rorl comment "Input for IMPUTE2 --- .ext file"

table file path locus_combined_snptest_out_file=$locus_combined_plink_file.snptest.out dir locus_dir disp ".snptest.out" parent cat_locus_impute2_association_data class_level locus comment "Output of SNPTEST on all variants"

file path locus_combined_snptest_log_file=$locus_combined_plink_file.snptest.log dir locus_dir disp ".snptest.log" parent cat_locus_impute2_association_data class_level locus comment "Log output of SNPTEST on all variants"

minor file path locus_slide_master_ps_file=@locus.slide.master.ps dir locus_dir disp ".ps" parent cat_locus_slide_master_data class_level locus comment "Master join file of all relevant slides --- ps file"
major file path locus_slide_master_pdf_file=@locus.slide.master.pdf dir locus_dir disp ".pdf" parent cat_locus_slide_master_data class_level locus comment "Master join file of all relevant slides --- pdf file"

!!expand:,:ext,majmin:tex,minor:pdf,minor! \
majmin file path locus_title_slide_ext_file=@locus.title.ext dir locus_dir disp "title.ext" trunk @locus.title parent cat_locus_slide_master_data class_level locus comment "Master title slide --- ext file"


!!expand:curpheno:case:control! \
table file path locus_curpheno_position_coverage_file=@locus.curpheno.position.coverage.tsv dir locus_dir disp "curpheno.position.coverage.csv" parent cat_locus_coverage_data class_level locus comment "Coverage stats by position for curphenos"

table file path locus_exon_target_file=@locus.exon.target.list dir locus_dir disp ".exon.target.list" parent cat_locus_coverage_data class_level locus comment "List of all exons in the locus"

table file path locus_gene_target_file=@locus.gene.target.list dir locus_dir disp ".gene.target.list" parent cat_locus_coverage_data class_level locus comment "List of all genes in the locus"

table file path locus_var_counts_dat_file=@locus.var.counts.dat dir locus_dir disp ".var.counts.dat" parent cat_locus_association_data class_level locus comment "List of all variants in the locus with associatin and maf info"

table file path locus_burden_dat_file=@locus.burden.dat dir locus_dir disp ".burden.dat" parent cat_locus_association_data class_level locus comment "List of all burden results for all genes in the locus"

file major path locus_var_coverage_pdf_file=@locus.var.coverage.pdf dir locus_dir disp ".var.coverage.pdf" parent cat_locus_association_data class_level locus comment "Case and control coverage data + variants --- pdf file"

table file path locus_combined_association_scores_file=$locus_combined_plink_file.association.scores dir locus_dir disp ".association.scores" parent cat_locus_marker_association_data class_level locus comment "Association scores for all SNPs in marker plink file --- from pheno_all_marker_snp_pvalues_file if available, otherwise from snptest"

file path locus_top_common_marker_snp_file=@locus.top.marker.snp dir locus_dir disp ".top.marker.snp" parent cat_locus_marker_association_data class_level locus comment "The marker SNP that had the top association score among all marker SNPs"

file path locus_top_common_marker_pos_file=@locus.top.marker.pos dir locus_dir disp ".top.marker.pos" parent cat_locus_marker_association_data class_level locus comment "The marker POS that had the top association score among all marker SNPs"

locus_r2_trunk=@locus.r2 dir locus_dir
table file path locus_r2_ld_file=$locus_r2_trunk.ld dir locus_dir disp ".r2.ld" parent cat_locus_marker_association_data class_level locus comment "R2 values between top marker variant at this locus and all other variants at the locus"
file path locus_r2_ld_log_file=$locus_r2_trunk.log dir locus_dir disp ".r2.log" parent cat_locus_marker_association_data class_level locus comment "Log file for computation of R2 values between top variant at this locus and all other variants at the locus"

table file path locus_haploview_marker_ld_file=$locus_haploview_ld_trunk.marker.LD dir locus_dir disp ".haploview.marker.LD" parent cat_locus_marker_association_data class_level locus comment "Haploview LD values for between each variant and marker"

table file path locus_fancy_plot_dat_file=@locus.fancy.plot.dat dir locus_dir disp ".fancy.plot.dat" parent cat_locus_marker_association_data class_level locus comment "Input for fancy plotting at this locus"

!!expand:,:keytype,exttype,txttype:_marker,.marker, -- markers only:,,! \
file path locus_fancykeytype_plot_pdf_file=@locus.fancyexttype.plot.pdf dir locus_dir disp ".fancyexttype.plot.pdf" parent cat_locus_marker_association_data class_level locus comment "Fancy plot at this locustxttype"

table file path locus_strat_index_snp_file=@locus.strat.index.snp dir locus_dir disp ".strat.index.snp" parent cat_locus_haplotype_burden_data class_level locus comment "Stratified rare variant dosages for every variant"

table file path locus_index_geno_counts_file=@locus.index.geno.counts dir locus_dir disp ".index.geno.counts" parent cat_locus_haplotype_burden_data class_level locus comment "Counts of individuals stratified by index genotype"

table file path locus_strat_self_snp_file=@locus.strat.self.snp dir locus_dir disp ".strat.self.snp" parent cat_locus_haplotype_burden_data class_level locus comment "Count of individuals stratified by each SNP"

table file path locus_est_variance_explained_file=@locus.est.var.explained.txt dir locus_dir disp ".est.var.explained.txt" parent cat_locus_haplotype_burden_data class_level locus comment "Estimate of variance explained by each snp"

table file path locus_est_variance_explained_annot_file=@locus.est.var.explained.annot.txt dir locus_dir disp ".est.var.explained.annot.txt" parent cat_locus_haplotype_burden_data class_level locus comment "Estimate of variance explained by each snp --- annotated with additional information about each snp"

table file path locus_conditional_or_file=@locus.conditional.or.tsv dir locus_dir disp ".conditional.or.tsv" parent cat_locus_haplotype_burden_data class_level locus comment "Estimate of odds ratios after different variants are removed"

file path locus_conditional_or_pdf_file=@locus.conditional.or.pdf dir locus_dir disp ".conditional.or.pdf" parent cat_locus_haplotype_burden_data class_level locus comment "Estimate of odds ratios after different variants are removed --- pdf file"



#gene files

gene_pheno_seq_plink_file=@gene.pheno.seq dir gene_dir
!!expand:,:name,ext,keymod:bed,bed,:bim,bim,table:fam,fam,table! \
file keymod path gene_pheno_seq_name_file=$gene_pheno_seq_plink_file.ext dir gene_dir disp ".ext" parent cat_gene_plot_plink_data class_level gene comment "All Variants in gene from sequencing, in QC/Popgen/Strat+ samples --- ext file"
file path gene_pheno_seq_make_bed_log_file=${gene_pheno_seq_plink_file}.make_bed.log dir gene_dir disp ".make_bed.log" parent cat_gene_plot_plink_data class_level gene comment "Log file from projection of variants to this gene; only includes popgen/strat passed samples"

#table file path gene_vars_file=@gene.vars dir gene_dir disp ".vars" parent cat_gene_plot_data class_level gene comment "List of variants in gene --- for plotting gene"

#table file path gene_group_file=@gene.group dir gene_dir disp ".group" parent cat_gene_plot_data class_level gene comment "Grouping of variants in gene --- for plotting gene"

#major file path gene_vars_pdf_file=@gene.vars.pdf dir gene_dir disp ".vars.pdf" parent cat_gene_plot_data class_level gene comment "Plot of all variants in gene"

file path table onecol gene_variant_list_file=@gene.variant.list dir gene_dir disp ".variant.list" parent cat_gene_meta_data class_level gene comment "Variants in this gene"

file path table gene_all_annot_file=@gene.all_annot.tsv dir gene_dir disp ".all_annot.tsv" parent cat_gene_meta_data class_level gene comment "Annotations in all transcripts for this gene"

meta_table file path table gene_transcript_burdens_meta_file=@gene.transcript_burdens.meta dir gene_dir disp ".transcript_burdens.meta" parent cat_gene_annot_data class_level gene comment "Transcripts for this gene" meta_level transcript_burden

minor file path gene_ucsc_regions_file=@gene.ucsc.regions dir gene_dir disp ".ucsc.regions" parent cat_gene_annot_data class_level gene comment "Region information for UCSC screenshot fetching"

minor file path gene_ucsc_pdf_file=@gene.ucsc.pdf dir gene_dir disp ".ucsc.pdf" parent cat_gene_annot_data class_level gene comment "Region information for UCSC screenshot fetching"

!!expand:curpheno:case:control! \
table file path gene_curpheno_position_coverage_file=@gene.curpheno.position.coverage.tsv dir gene_dir disp "curpheno.position.coverage.csv" parent cat_gene_qc_data class_level gene comment "Coverage stats by position for curphenos"

file path gene_exon_target_file=@gene.exon.target.list dir gene_dir disp ".exon.target.list" parent cat_gene_qc_data class_level gene comment "Exon target file for this gene"

file major path gene_var_coverage_pdf_file=@gene.var.coverage.pdf dir gene_dir disp ".var.coverage.pdf" parent cat_gene_qc_data class_level gene comment "Case and control coverage data + variants --- pdf file"

!!expand:,:ext,weblevel:tex,:pdf,major! \
file weblevel path gene_qc_metrics_ext_file=@gene.qc_metrics.ext dir gene_dir trunk @gene.qc_metrics disp ".qc_metrics.ext" parent cat_gene_qc_data class_level gene comment "QC metric data for gene --- ext file"

path gene_all_ld_plink_file=@gene.all.ld dir gene_dir
!!expand:,:ext:ped:map:info! \
table nohead file path gene_all_ld_ext_file=$gene_all_ld_plink_file.ext dir gene_dir disp ".ext" parent cat_gene_all_ld_data class_level gene comment "Markers from Seq + LD, projected to those close to gene"
file path gene_all_ld_recode_log_file=${gene_all_ld_plink_file}.recode.log dir gene_dir disp ".recode.log" parent cat_gene_all_ld_data class_level gene comment "Log file from creation of gene_all_ld_plink_file"

path gene_all_ld_png_trunk=@gene.all dir gene_dir
#!!expand:,:ext,EXT:png,PNG:pdf,pdf! \
#major file path gene_all_ld_ext_file=$gene_all_ld_png_trunk.LD.EXT dir gene_dir disp ".ext" trunk @gene.all.ld parent cat_gene_all_ld_data class_level gene comment "Haploview plot of LD at all variants in region"


path gene_seq_ld_plink_file=@gene.seq.ld dir gene_dir
!!expand:ext:ped:map:info! \
table nohead file path gene_seq_ld_ext_file=$gene_seq_ld_plink_file.ext dir gene_dir disp ".ext" parent cat_gene_seq_ld_data class_level gene comment "Markers from Seq + LD, projected to those close to gene"
file path gene_seq_ld_recode_log_file=${gene_seq_ld_plink_file}.recode.log dir gene_dir disp ".recode.log" parent cat_gene_seq_ld_data class_level gene comment "Log file from creation of gene_seq_ld_plink_file"
path gene_seq_ld_png_trunk=@gene.seq dir gene_dir
#!!expand:,:ext,EXT:png,PNG:pdf,pdf! \
#major file path gene_seq_ld_ext_file=$gene_seq_ld_png_trunk.LD.EXT dir gene_dir disp ".ext" trunk @gene.seq.ld  parent cat_gene_seq_ld_data class_level gene comment "Haploview plot of LD at all sequence variants"

file major path gene_var_pdf_file=@gene.var.pdf dir gene_dir disp ".var.pdf" parent cat_gene_seq_assoc_data class_level gene comment "Case and control variants --- pdf file"

file major path gene_qq_pdf_file=@gene.qq.pdf dir gene_dir disp ".qq.pdf" parent cat_gene_seq_assoc_data class_level gene comment "QQ plot of variants in gene"

!!expand:,:typeofvar,ext,weblevel:,dat,table:rare_,dat,table:,pdf,major! \
file weblevel path gene_typeofvarvar_counts_ext_file=@gene.typeofvarvar_counts.ext dir gene_dir disp ".typeofvarvar_counts.ext" parent cat_gene_seq_assoc_data class_level gene comment "Associated typeofvarvariant counts --- ext file"

#!!expand:,:ext,weblevel:tex,:pdf,major! \
#file weblevel path gene_associated_ext_file=@gene.associated.ext dir gene_dir trunk @gene.associated disp ".associated.ext" parent cat_gene_seq_assoc_data class_level gene comment "Top associated variants --- ext file"

!!expand:ctype:all:meta! \
!!expand:,:ext,weblevel:tex,:pdf,major! \
file weblevel path gene_ctype_trait_associated_ext_file=@gene.ctype_trait.associated.ext dir gene_dir trunk @gene.ctype_trait.associated disp ".ctype_trait.associated.ext" parent cat_gene_seq_assoc_data class_level gene comment "Top associated variants, with ctype trait information --- ext file"


!!expand:,:ext,weblevel:tex,:pdf,major! \
file weblevel path gene_all_variants_ext_file=@gene.all.variants.ext dir gene_dir trunk @gene.all.variants disp ".all.variants.ext" parent cat_gene_seq_assoc_data class_level gene comment "All variants (whether or not passing QC), but filtered samples --- ext file"

!!expand:,:ext,weblevel:tex,:pdf,major! \
file weblevel path gene_all_data_ext_file=@gene.all.data.ext dir gene_dir trunk @gene.all.data disp ".all.data.ext" parent cat_gene_seq_assoc_data class_level gene comment "All variants and samples (whether or not passing QC) --- ext file"

!!expand:,:ext,weblevel:tex,:pdf,major! \
file weblevel path gene_gassoc_ext_file=@gene.gassoc.ext dir gene_dir trunk @gene.gassoc disp ".gassoc.ext" parent cat_gene_seq_assoc_data class_level gene comment "Counts of variants in each burden --- ext file"

minor file path gene_slide_master_ps_file=@gene.slide.master.ps dir gene_dir disp ".ps" parent cat_gene_slide_master_data class_level gene comment "Master join file of all relevant slides --- ps file"
major file path gene_slide_master_pdf_file=@gene.slide.master.pdf dir gene_dir disp ".pdf" parent cat_gene_slide_master_data class_level gene comment "Master join file of all relevant slides --- pdf file"

minor file path gene_slide_master_sum_ps_file=@gene.slide.master.sum.ps dir gene_dir disp ".sum.ps" parent cat_gene_slide_master_data class_level gene comment "Master join file of all relevant summary slides --- ps file"
major file path gene_slide_master_sum_pdf_file=@gene.slide.master.sum.pdf dir gene_dir disp ".sum.pdf" parent cat_gene_slide_master_data class_level gene comment "Master join file of all relevant summary slides --- pdf file"


!!expand:,:ext,majmin:tex,minor:pdf,minor! \
majmin file path gene_title_slide_ext_file=@gene.title.ext dir gene_dir disp ".title.ext" trunk @gene.title parent cat_gene_slide_master_data class_level gene comment "Master title slide --- ext file"

!!expand:impute_type:hap:traditional! \
table file path gene_impute_type_summary_file=@gene.impute_type.summary dir gene_dir disp ".impute_type.summary" parent cat_gene_imputation_assoc_data class_level gene comment "Summary of imputation results for all variants; impute_type method"

#gene burden and transcript burden files

!!expand:ext:ps:pdf! \
minor file path gene_burden_slide_master_ext_file=@gene_burden.slide.master.ext dir gene_burden_dir disp ".ext" parent cat_gene_burden_data class_level gene_burden comment "Master join file of all relevant slides --- ext file"

file path table gene_burden_transcript_burdens_dat_file=@gene_burden.transcript_burdens.dat dir gene_burden_dir disp ".transcript_burdens.dat" parent cat_gene_burden_data class_level gene_burden comment "Transcripts for this gene burden"

table file path transcript_burden_vassoc_annot_file=@transcript_burden.vassoc.annot dir transcript_burden_dir disp ".vassoc.annot" parent cat_transcript_burden_data class_level transcript_burden comment "Top associated variants --- with annotations specific to this transcript"

!!expand:,:ext,weblevel:tex,:pdf,major! \
file weblevel path gene_burden_vassoc_ext_file=@gene_burden.vassoc.ext dir gene_burden_dir trunk @gene_burden.vassoc disp ".vassoc.ext" parent cat_gene_burden_data class_level gene_burden comment "Top associated variants --- ext file"
"""
}
    