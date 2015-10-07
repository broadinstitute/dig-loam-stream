
package loamstream.compiler

/**
 * LoamStream
 * Created by oliverr on 10/6/2015.
 */
object PipelineTargetedPart08 {
  val string =
 """major file path pheno_type_related_pdf_file=@pheno.type.related.pdf dir pheno_dir disp ".type.related.pdf" parent cat_pheno_ibd_qc_plot_data class_level pheno comment "Histogram of relatedness counts among Type samples"

!!expand:,:type,cattouse:all,popgen:highlighted,popgen:final,popgen:unrelated,popgen! \
major file path pheno_popgen_type_pdf_file=@pheno.popgen.type.pdf dir pheno_dir disp ".popgen.type.pdf" parent cat_pheno_cattouse_qc_plot_data class_level pheno comment "Plot of popgen sample metrics across all samples --- type plots"

!!expand:type:popgen! \
pheno_slide_type_qc_failures_trunk=@pheno.slide.type.qc_failures

!!expand:type:popgen! \
file path pheno_slide_type_qc_failures_tex_file=$pheno_slide_type_qc_failures_trunk.tex dir pheno_dir disp ".type.failures.tex" parent cat_pheno_popgen_qc_plot_data class_level pheno trunk $pheno_slide_type_qc_failures_trunk comment "Sample type qc failure table -- tex file"

!!expand:type:popgen! \
file path pheno_slide_type_qc_failures_pdf_file=$pheno_slide_type_qc_failures_trunk.pdf dir pheno_dir disp ".type.failures.pdf" parent cat_pheno_popgen_qc_plot_data class_level pheno comment "Sample type qc failure table -- pdf file"

table file path pheno_with_genome_file=@pheno.with.genome dir pheno_dir disp ".with.genome" parent cat_pheno_ibd_qc_stats_data class_level pheno comment "Helper file: pheno non missing file projected to those samples in the project genome file"

!!expand:pheno:pheno:pheno_sample_subset! \
pheno_genome_trunk=@pheno dir pheno_dir

!!expand:pheno:pheno:pheno_sample_subset! \
table file path pheno_genome_file=$pheno_genome_trunk.genome.gz dir pheno_dir disp ".genome" parent cat_pheno_ibd_qc_stats_data class_level pheno comment "Project level genome values projected to this phenotype --- only includes QC+ samples"

table file path pheno_sample_subset_genome_for_cat_file=$pheno_sample_subset_genome_trunk.genome.for_cat.gz dir pheno_sample_subset_dir disp ".genome.for_cat" parent cat_pheno_sample_subset_ibd_qc_stats_data class_level pheno_sample_subset comment "Pheno sample subset genome file projected so when catted together have unique pairs"

table file path pheno_annotated_genome_file=$pheno_genome_trunk.annotated.genome.gz dir pheno_dir disp ".annotated.genome" parent cat_pheno_ibd_qc_stats_data class_level pheno comment "Project level genome values projected to this phenotype --- only includes QC+ samples"

!!expand:pheno:pheno:pheno_sample_subset! \
table file path pheno_genome_log_file=$pheno_genome_trunk.genome.log dir pheno_dir disp ".genome.log" parent cat_pheno_ibd_qc_stats_data class_level pheno comment "Log file for genome command"

mkdir path pheno_epacts_dir=$pheno_dir/epacts class_level pheno chmod 777
table path file pheno_epacts_ready_file=@pheno.epacts.ready dir pheno_epacts_dir disp ".epacts.ready"  class_level pheno comment "Dummy file to indicate ready to run epacts"

pheno_epacts_trunk=@pheno.epacts dir pheno_epacts_dir

table file path pheno_kinship_file=$pheno_epacts_trunk.kinf dir pheno_dir disp ".kinf" parent cat_pheno_ibd_qc_stats_data class_level pheno comment "Kinship matrix for samples --- computed by EMMAX"

!!expand:,:_type,.type,Type:_all,.all,all:_unrelated,.unrelated,unrelated! \
file path pheno_type_related_dat_file=@pheno.type.related.dat dir pheno_dir disp ".type.related.dat" parent cat_pheno_ibd_qc_stats_data class_level pheno comment "Dat file for histogram of relatedness counts among Type samples"

!!expand:pheno:pheno:pheno_sample_subset! \
pheno_smart_pca_trunk=@pheno.pca dir pheno_dir

!!expand:pheno:pheno:pheno_sample_subset! \
!!expand:,:filetype,Filetype:evec,Evec:eval,Eval:weights,Weight:log,Log:fam,Fam! \
file path pheno_smart_pca_filetype_file=$pheno_smart_pca_trunk.filetype dir pheno_dir disp ".pca.filetype" parent cat_pheno_covar_qc_stats_data class_level pheno comment "Filetype file for smartpca"

#!!expand:pheno:pheno:pheno_sample_subset! \
#file path pheno_smart_pca_log_file=@pheno.pca.log dir pheno_dir disp ".pca.log" parent cat_pheno_covar_qc_stats_data class_level pheno comment "Par file for smartpca"

table file path pheno_custom_mds_file=@pheno.custom.mds dir pheno_dir disp ".custom.mds" parent cat_pheno_covar_qc_stats_data class_level pheno comment "Specify custom PCs to use"

!!expand:pheno:pheno:pheno_sample_subset! \
pheno_mds_trunk=@pheno dir pheno_dir
!!expand:pheno:pheno:pheno_sample_subset! \
table file path pheno_mds_file=$pheno_mds_trunk.mds dir pheno_dir disp ".mds" parent cat_pheno_covar_qc_stats_data class_level pheno comment "Recomputation of mds file using only samples at this pheno level"

file path pheno_mds_log_file=$pheno_mds_trunk.mds.log dir pheno_dir disp ".mds.log" parent cat_pheno_covar_qc_stats_data class_level pheno comment "Recomputation of mds file using only samples at this pheno level -- log file"

table file path pheno_annotated_mds_file=@pheno.annotated.mds dir pheno_dir disp ".annotated.mds" parent cat_pheno_covar_qc_stats_data class_level pheno comment "Mds file annotated with phenotype"

table file nohead path pheno_plink_match_file=@pheno.plink.match dir pheno_dir disp ".plink.match" parent cat_pheno_cluster_qc_stats_data class_level pheno comment "Match file for loading into plink"

!!expand:clusterlevel:0:1:2:3! \
table nohead file path pheno_sample_marker_clusterclusterlevel_file=$pheno_sample_marker_plink_file.clusterclusterlevel dir pheno_dir disp ".clusterclusterlevel" parent cat_pheno_cluster_qc_stats_data class_level pheno comment "Cluster clusterlevel file for samples in project; using this phenotype for any case/control matching"
file path pheno_sample_marker_cluster_log_file=$pheno_sample_marker_plink_file.cluster.log dir pheno_dir disp ".cluster.log" parent cat_pheno_cluster_qc_stats_data class_level pheno comment "Log file for cluster cmd"

file path pheno_cluster_avg_stats_file=@pheno.cluster.avg.stats dir pheno_dir disp ".cluster.avg.stats" parent cat_pheno_cluster_qc_stats_data class_level pheno comment "Stats on strata clusters that come from averages over samples" 

table file path pheno_cluster_stats_file=@pheno.cluster.stats dir pheno_dir disp ".cluster.stats" parent cat_pheno_cluster_qc_stats_data class_level pheno comment "Stats on strata clusters" 

!!expand:tval:top! \
major file path pheno_tval_project_mds_pdf_file=@pheno.tval.project.mds.pdf dir pheno_dir disp ".tval.project.mds.pdf" parent cat_pheno_covar_qc_plot_data class_level pheno comment "Plot of tval MDS values for all samples -- made from project level PCA"


!!expand:tval:top:all! \
major file path pheno_tval_mds_pdf_file=@pheno.tval.mds.pdf dir pheno_dir disp ".tval.mds.pdf" parent cat_pheno_covar_qc_plot_data class_level pheno comment "Plot of tval MDS values for popgen+ samples"

major file path pheno_genome_pdf_file=@pheno.genome.pdf dir pheno_dir disp ".genome.pdf" parent cat_pheno_ibd_qc_plot_data class_level pheno comment "Plot of IBD sharing stratified by phenotype"

major file path pheno_cluster_assign_mds_pdf_file=@pheno.cluster.assign.mds.pdf dir pheno_dir disp ".cluster.assign.mds.pdf" parent cat_pheno_cluster_qc_plot_data class_level pheno comment "Plot of MDS values for samples, samples in a common cluster connected"

major file path pheno_cluster_stats_pdf_file=@pheno.cluster.stats.pdf dir pheno_dir disp ".cluster.stats.pdf" parent cat_pheno_cluster_qc_plot_data class_level pheno comment "Plot of stats for clusters"

#post strat

#!!expand:filetype:covar:cluster! \
#onecol table nohead file path pheno_filetype_exclude_file=@pheno.filetype.exclude dir pheno_dir disp ".filetype.exclude" parent cat_pheno_post_strat_qc_stats_data class_level pheno comment "Samples to exclude during filetype evaluation"

!!expand:pheno:pheno:pheno_variant_subset! \
pheno_plink_file=@pheno dir pheno_dir
!!expand:pheno:pheno:pheno_variant_subset! \
file path pheno_bed_file=@pheno.bed dir pheno_dir disp ".bed" parent cat_pheno_plink_seq_data class_level pheno comment "Bed file format for qc_plus vcf file; phenotypes specific to this trait"
!!expand:pheno:pheno:pheno_variant_subset! \
nohead table file path pheno_fam_file=@pheno.fam dir pheno_dir disp ".fam" parent cat_pheno_plink_seq_data class_level pheno comment "Fam file format for qc_plus vcf file; phenotypes specific to this trait"
!!expand:pheno:pheno:pheno_variant_subset! \
nohead table file path pheno_bim_file=@pheno.bim dir pheno_dir disp ".bim" parent cat_pheno_plink_seq_data class_level pheno comment "Bim file format for qc_plus vcf file; phenotypes specific to this trait"

!!expand:pheno:pheno:pheno_variant_subset! \
minor file path pheno_make_bed_log_file=@pheno.make_bed.log dir pheno_dir disp ".make_bed.log" parent cat_pheno_plink_seq_data class_level pheno comment "Log file for make-bed command"

pheno_sample_marker_plink_file=@pheno.sample.marker dir pheno_dir
pheno_sample_pruned_marker_plink_file=@pheno.sample.pruned.marker dir pheno_dir

major file path doubcom table pheno_variant_subset_clean_all_vcf_file=@pheno_variant_subset.clean.all.vcf.gz dir pheno_variant_subset_dir disp ".clean.all.vcf" parent cat_pheno_variant_subset_plink_seq_data class_level pheno_variant_subset comment "Clean VCF file with all variants for pheno_variant_subset"

!!expand:,:type:in:in_range:out:log! \
file path pheno_sample_marker_ld_prune_type_file=$pheno_sample_marker_plink_file.prune.type dir pheno_dir disp ".prune.type" parent cat_pheno_plink_pruned_marker_data class_level pheno comment "Result of LD pruning at pheno level --- prune.type file"

!!expand:ext:bed:bim:fam! \
file path pheno_sample_pruned_marker_ext_file=$pheno_sample_pruned_marker_plink_file.ext dir pheno_dir disp ".ext" parent cat_pheno_plink_pruned_marker_data class_level pheno comment "Already LD pruned filtered sample marker plink file, projected to include only samples with non missing phenotype --- ext file"

minor file path pheno_sample_pruned_marker_make_bed_log_file=$pheno_sample_pruned_marker_plink_file.make_bed.log dir pheno_dir disp ".make_bed.log" parent cat_pheno_plink_pruned_marker_data class_level pheno comment "Log file for make-bed command"

!!expand:,:keyt,extt,descript:initial_,initial.,-- individuals not necessarily sorted:,,! \
file path pheno_sample_marker_pruned_keytvcf_file=$pheno_sample_pruned_marker_plink_file.exttvcf.gz dir pheno_dir disp ".pruned.exttvcf.gz" parent cat_pheno_plink_pruned_marker_data class_level pheno comment "Marker vcf file for pheno samples, LD pruned descript"

file path pheno_sample_pruned_marker_recode_vcf_log_file=$pheno_sample_pruned_marker_plink_file.pruned.recode_vcf.log dir pheno_dir disp ".pruned.recode_vcf.log" parent cat_pheno_plink_pruned_marker_data class_level pheno comment "Log file for make bed cmd"

pheno_sample_for_pca_plink_file=@pheno.for.pca dir pheno_dir

file path pheno_sample_for_pca_map_file=$pheno_sample_for_pca_plink_file.map dir pheno_dir disp ".for_pca.map" parent cat_pheno_plink_pruned_marker_data class_level pheno comment "Map from ID in file to FID/IID"

!!expand:ext:bed:bim:fam! \
file path pheno_sample_for_pca_ext_file=$pheno_sample_for_pca_plink_file.ext dir pheno_dir disp ".for_pca.ext" parent cat_pheno_plink_pruned_marker_data class_level pheno comment "Use this file for PCA; could contain additional samples if requested outside of project --- ext file"

minor file path pheno_sample_for_pca_make_bed_log_file=$pheno_sample_for_pca_plink_file.make_bed.log dir pheno_dir disp ".make_bed.log" parent cat_pheno_plink_pruned_marker_data class_level pheno comment "Log file for make-bed command"


pheno_sample_combined_plink_file=@pheno.sample.combined dir pheno_dir
!!expand:ext:bed:bim:fam! \
file path pheno_sample_combined_ext_file=$pheno_sample_combined_plink_file.ext dir pheno_dir disp ".ext" parent cat_pheno_plink_combined_data class_level pheno comment "Combined seq + marker data, projected to include only samples for this phenotype --- ext file"

minor file path pheno_sample_combined_make_bed_log_file=$pheno_sample_combined_plink_file.make_bed.log dir pheno_dir disp ".make_bed.log" parent cat_pheno_plink_combined_data class_level pheno comment "Log file for make-bed command"


table file path pheno_all_marker_top_hits_file=@pheno.marker.top.hits dir pheno_dir disp ".top.hits" parent cat_pheno_top_hits_data class_level pheno comment "List of variants to use as top hits in downstream plots"

table file path pheno_top_hits_snp_ids_file=@pheno.top.hits.snp.ids dir pheno_dir disp ".snp.ids" parent cat_pheno_top_hits_data class_level pheno comment "List of variants in marker or seq file"

pheno_top_hits_ld_trunk=@pheno.top.hits dir pheno_dir
table file path pheno_top_hits_ld_file=$pheno_top_hits_ld_trunk.ld dir pheno_dir disp ".ld" parent cat_pheno_top_hits_data class_level pheno comment "List of variants in LD with top hits"

minor file path pheno_top_hits_ld_log_file=@pheno.top.hits.ld.log dir pheno_dir disp ".ld.log" parent cat_pheno_top_hits_data class_level pheno comment "List of variants in LD with top hits -- log file"

table file path pheno_top_hits_ld_chr_pos_list=$pheno_top_hits_ld_trunk.ld.chr_pos.list dir pheno_dir disp ".ld.chr_pos.list" parent cat_pheno_top_hits_data class_level pheno comment "List of chr pos in LD with top hits"

table file path pheno_top_hits_ld_seq_var_list=$pheno_top_hits_ld_trunk.ld.seq_var.list dir pheno_dir disp ".ld.seq_var.list" parent cat_pheno_top_hits_data class_level pheno comment "List of seq variant ids in LD with top hits"

!!expand:pheno:pheno:pheno_variant_subset! \
table file path pheno_test_missing_file=$pheno_plink_file.missing dir pheno_dir disp ".missing" parent cat_pheno_variant_qc_plink_data class_level pheno comment "Pheno specific missingness test"

!!expand:pheno:pheno:pheno_variant_subset! \
minor file path pheno_test_missing_log_file=$pheno_test_missing_file.log dir pheno_dir disp ".missing.log" parent cat_pheno_variant_qc_plink_data class_level pheno comment "Pheno specific missingness test --- log file"

!!expand:pheno:pheno:pheno_variant_subset! \
table file path pheno_hwe_file=$pheno_plink_file.hwe dir pheno_dir disp ".hwe" parent cat_pheno_variant_qc_plink_data class_level pheno comment "Pheno specific hwe test"

!!expand:pheno:pheno:pheno_variant_subset! \
minor file path pheno_hwe_log_file=$pheno_hwe_file.log dir pheno_dir disp ".hwe.log" parent cat_pheno_variant_qc_plink_data class_level pheno comment "Pheno specific hwe test --- log file"

!!expand:male:male:female! \
!!expand:pheno:pheno:pheno_variant_subset! \
pheno_male_plink_trunk=@pheno.male

!!expand:male:male:female! \
!!expand:pheno:pheno:pheno_variant_subset! \
table file path pheno_male_hwe_file=$pheno_male_plink_trunk.hwe dir pheno_dir disp ".male.hwe" parent cat_pheno_variant_qc_plink_data class_level pheno comment "Pheno specific hwe test for males"

!!expand:male:male:female! \
!!expand:pheno:pheno:pheno_variant_subset! \
minor file path pheno_male_hwe_log_file=$pheno_male_plink_trunk.hwe.log dir pheno_dir disp ".male.hwe.log" parent cat_pheno_variant_qc_plink_data class_level pheno comment "Pheno specific hwe test for males --- log file"

!!expand:pheno:pheno:pheno_variant_subset! \
table file path pheno_lmiss_file=$pheno_plink_file.lmiss dir pheno_dir disp ".lmiss" parent cat_pheno_variant_qc_plink_data class_level pheno comment "Pheno specific lmiss test"

!!expand:pheno:pheno:pheno_variant_subset! \
minor file path pheno_lmiss_log_file=$pheno_lmiss_file.log dir pheno_dir disp ".lmiss.log" parent cat_pheno_variant_qc_plink_data class_level pheno comment "Pheno specific lmiss test --- log file"

!!expand:pheno:pheno:pheno_variant_subset! \
table file path pheno_frq_file=$pheno_plink_file.frq dir pheno_dir disp ".frq" parent cat_pheno_variant_qc_plink_data class_level pheno comment "Pheno specific frq test"

!!expand:pheno:pheno:pheno_variant_subset! \
minor file path pheno_frq_log_file=$pheno_frq_file.log dir pheno_dir disp ".frq.log" parent cat_pheno_variant_qc_plink_data class_level pheno comment "Pheno specific frq test --- log file"

!!expand:pheno:pheno:pheno_variant_subset! \
table file path pheno_sex_frq_file=$pheno_plink_file.sex.frq dir pheno_dir disp ".sex.frq" parent cat_pheno_variant_qc_plink_data class_level pheno comment "Pheno specific frq test stratified by sex"

!!expand:pheno:pheno:pheno_variant_subset! \
minor file path pheno_sex_frq_log_file=$pheno_sex_frq_file.log dir pheno_dir disp ".sex.frq.log" parent cat_pheno_variant_qc_plink_data class_level pheno comment "Pheno specific frq test stratified by sex  --- log file"

!!expand:pheno:pheno:pheno_variant_subset! \
table file path pheno_multiallelic_frq_file=$pheno_plink_file.multiallelic.frq dir pheno_dir disp ".multialellic.frq" parent cat_pheno_variant_qc_plink_data class_level pheno comment "Pheno specific frq test for multialellics"

!!expand:pheno:pheno:pheno_variant_subset! \
table file path pheno_multiallelic_group_frq_file=$pheno_plink_file.multiallelic.group.frq dir pheno_dir disp ".multialellic.group.frq" parent cat_pheno_variant_qc_plink_data class_level pheno comment "Pheno specific p_missing test for multialellics"

!!expand:pheno:pheno:pheno_variant_subset! \
table file path pheno_combined_frq_file=$pheno_plink_file.combined.frq dir pheno_dir disp ".combined.frq" parent cat_pheno_variant_qc_plink_data class_level pheno comment "Pheno specific frq test, combined over multiallelics and biallelics"

table file path pheno_high_test_missing_file=$pheno_plink_file.high.missing dir pheno_dir disp ".high.missing" parent cat_pheno_variant_qc_plink_data class_level pheno comment "List of variants with low test missing p-values"

#file path pheno_strata_vdist_file=@pheno.strata.vdist dir pheno_dir disp ".strata.vdist" parent cat_pheno_variant_qc_stats_data class_level pheno comment "Dump of plinkseq v-dist -- stratified by cluster"

#file path pheno_vdist_file=@pheno.vdist dir pheno_dir disp ".vdist" parent cat_pheno_variant_qc_stats_data class_level pheno comment "Dump of plinkseq v-dist"

!!expand:pheno:pheno:pheno_variant_subset! \
table file path pheno_counts_file=@pheno.counts dir pheno_dir disp ".counts" parent cat_pheno_variant_qc_stats_data class_level pheno comment "Dump of plinkseq counts"

!!expand:pheno:pheno:pheno_variant_subset! \
table file path pheno_variant_qc_strata_vstats_summary_file=@pheno.variant.qc_strata.vstats.summary dir pheno_dir disp ".qc_strata.vstats.summary" parent cat_pheno_variant_qc_stats_data class_level pheno comment "Summary of statistics for qc_strata"

!!expand:phenol:pheno:pheno_variant_subset! \
table file path phenol_variant_qc_pheno_strata_vstats_summary_file=@phenol.variant.qc_pheno_strata.vstats.summary dir phenol_dir disp ".qc_pheno_strata.vstats.summary" parent cat_phenol_variant_qc_stats_data class_level phenol comment "Summary of statistics for qc_pheno_strata"

minor file path pheno_slide_master_ps_file=@pheno.slide.master.ps dir pheno_dir disp ".ps" parent cat_pheno_slide_master_data class_level pheno comment "Master join file of all relevant slides --- ps file"
major file path pheno_slide_master_pdf_file=@pheno.slide.master.pdf dir pheno_dir disp ".pdf" parent cat_pheno_slide_master_data class_level pheno comment "Master join file of all relevant slides --- pdf file"

minor file path pheno_slide_master_qc_ps_file=@pheno.slide.master.qc.ps dir pheno_dir disp ".qc.ps" parent cat_pheno_slide_master_data class_level pheno comment "Master join file of all relevant QC slides --- ps file"
major file path pheno_slide_master_qc_pdf_file=@pheno.slide.master.qc.pdf dir pheno_dir disp ".qc.pdf" parent cat_pheno_slide_master_data class_level pheno comment "Master join file of all relevant QC slides --- pdf file"

minor file path pheno_slide_master_assoc_ps_file=@pheno.slide.master.assoc.ps dir pheno_dir disp ".assoc.ps" parent cat_pheno_slide_master_data class_level pheno comment "Master join file of all relevant association slides --- ps file"
major file path pheno_slide_master_assoc_pdf_file=@pheno.slide.master.assoc.pdf dir pheno_dir disp ".assoc.pdf" parent cat_pheno_slide_master_data class_level pheno comment "Master join file of all relevant association slides --- pdf file"

minor file path pheno_slide_master_genes_ps_file=@pheno.slide.master.genes.ps dir pheno_dir disp ".genes.ps" parent cat_pheno_slide_master_data class_level pheno comment "Master join file of all relevant gene slides --- ps file"
major file path pheno_slide_master_genes_pdf_file=@pheno.slide.master.genes.pdf dir pheno_dir disp ".genes.pdf" parent cat_pheno_slide_master_data class_level pheno comment "Master join file of all relevant gene slides --- pdf file"

!!expand:,:ext,majmin:tex,minor:pdf,minor! \
majmin file path pheno_title_slide_ext_file=@pheno.title.ext dir pheno_dir disp "title.ext" trunk @pheno.title parent cat_pheno_slide_master_data class_level pheno comment "Master title slide --- ext file"

!!expand:titletype:qc:assoc:gene! \
!!expand:,:ext,majmin:tex,minor:pdf,minor! \
majmin file path pheno_titletype_title_slide_ext_file=@pheno.titletype.title.ext dir pheno_dir disp "titletype.title.ext" trunk @pheno.titletype.title parent cat_pheno_slide_master_data class_level pheno comment "Master titletype title slide --- ext file"


#pheno_slide_nonsense_variants_trunk=@pheno.slide.nonsense
#!!expand:filetype:tex:pdf! minor file path pheno_slide_nonsense_variants_filetype_file=$pheno_slide_nonsense_variants_trunk.filetype trunk pheno_slide_nonsense_variants_trunk dir pheno_dir disp ".nonsense.filetype" parent cat_pheno_slide_variants_data class_level pheno comment "List of nonsense variants --- filetype file"

#!!expand:variant_type:all:ns:nondb:pph! \
#!!expand:,:filetype,major_level:tex,minor:pdf,major! \
#major_level file path pheno_slide_variant_type_associated_variants_filetype_file=@pheno.slide.variant_type.associated.filetype trunk @pheno.slide.variant_type.associated dir pheno_dir disp ".variant_type.associated.filetype" parent cat_pheno_slide_variants_data class_level pheno comment "List of associated variant_type variants --- filetype file"

#!!expand:variant_type:all:ns:nondb:pph:nonsense! \
#!!expand:,:filetype,major_level:tex,minor:pdf,major! \
#major_level file path pheno_slide_variant_type_associated_genes_filetype_file=@pheno.slide.variant_type.associated.genes.filetype trunk @pheno.slide.variant_type.associated.genes dir pheno_dir disp ".variant_type.associated.filetype" parent cat_pheno_slide_genes_data class_level pheno comment "List of associated variant_type genes --- filetype file"

pheno_slide_failures_trunk=@pheno.slide.failures
file path pheno_slide_failures_tex_file=$pheno_slide_failures_trunk.tex dir pheno_dir disp ".failures.tex" parent cat_pheno_slide_coverage_data class_level pheno trunk $pheno_slide_failures_trunk comment "Sample failure table -- tex file"
major file path pheno_slide_failures_pdf_file=$pheno_slide_failures_trunk.pdf dir pheno_dir disp ".failures.pdf" parent cat_pheno_slide_coverage_data class_level pheno comment "Sample failure table -- pdf file"

major file path pheno_slide_failures_dat_file=$pheno_slide_failures_trunk.dat dir pheno_dir disp ".failures.dat" parent cat_pheno_slide_coverage_data class_level pheno comment "Sample failure table -- dat file"

major file path pheno_slide_failures_bar_pdf_file=$pheno_slide_failures_trunk.bar.pdf dir pheno_dir disp ".failures.bar.pdf" parent cat_pheno_slide_coverage_data class_level pheno comment "Sample failure table -- bar plot"

pheno_slide_cross_failures_trunk=@pheno.slide.cross.failures
file path pheno_slide_cross_failures_tex_file=$pheno_slide_cross_failures_trunk.tex dir pheno_dir disp ".cross.failures.tex" parent cat_pheno_slide_coverage_data class_level pheno trunk $pheno_slide_cross_failures_trunk comment "Sample failure table stratified by cross classification phenotypes -- tex file"
file path pheno_slide_cross_failures_pdf_file=$pheno_slide_cross_failures_trunk.pdf dir pheno_dir disp ".cross.failures.pdf" parent cat_pheno_slide_coverage_data class_level pheno comment "Sample failure table stratified by cross classification phenotypes -- pdf file"

!!expand:curpheno:case:control! \
table file path pheno_curpheno_sample_coverage_file_list=@pheno.curpheno.sample.coverage.list dir pheno_dir disp "curpheno.sample.coverage.list" parent cat_pheno_sample_coverage_data class_level pheno comment "List of sample coverage file for curphenos"

table file path pheno_sample_coverage_dat_file=@pheno.coverage.csv dir pheno_dir disp ".csv" parent cat_pheno_sample_coverage_data class_level pheno comment "Coverage for each sample stratified by phenotype value"
major file path pheno_sample_coverage_pdf_file=@pheno.coverage.pdf dir pheno_dir disp ".pdf" parent cat_pheno_sample_coverage_data class_level pheno comment "Plot of coverage for each sample stratified by phenotype"

table nohead onecol file path pheno_interesting_genes_dat_file=@pheno.interesting_genes.dat dir pheno_dir disp ".interesting_genes.dat" parent cat_pheno_interesting_info class_level pheno comment "Dat file listing genes that are interesting for this phenotype"

table nohead file path pheno_gene_sort_values_file=@pheno.gene.sort.values dir pheno_dir disp ".gene.sort.values" parent cat_pheno_interesting_info class_level pheno comment "Gene Sort positions"

meta_table file path pheno_interesting_genes_meta_file=@pheno.interesting_genes.meta dir pheno_dir disp ".interesting_genes.meta" parent cat_pheno_interesting_info class_level pheno comment "Meta file to specify interesting genes for this phenotype" meta_level gene

!!expand:dat:dat:reg! \
table nohead onecol file path pheno_interesting_variants_dat_file=@pheno.interesting_variants.dat dir pheno_dir disp ".interesting_variants.dat" parent cat_pheno_interesting_info class_level pheno comment "Dat file listing interesting variants"

file path pheno_interesting_variants_pre_meta_file=@pheno.interesting_variants.pre_meta dir pheno_dir disp ".interesting_variants.pre_meta" parent cat_pheno_interesting_info class_level pheno comment "Information for meta file to specify interesting variants for this phenotype" 

meta_table file path pheno_interesting_variants_meta_file=@pheno.interesting_variants.meta dir pheno_dir disp ".interesting_variants.meta" parent cat_pheno_interesting_info class_level pheno comment "Meta file to specify interesting variants for this phenotype" meta_level variant

!!expand:ext:tex:pdf! \
file path pheno_interesting_counts_ext_file=@pheno.interesting_counts.ext dir pheno_dir disp "interesting_counts.ext" trunk @pheno.interesting_counts parent cat_pheno_interesting_info class_level pheno comment "Counts of interesting genes/variants by burden level"

!!expand:ext:tex:pdf! \
file path pheno_pheno_test_info_ext_file=@pheno.pheno_test.info.ext dir pheno_dir disp "pheno_test.info.ext" trunk @pheno.pheno_test.info parent cat_pheno_test_info class_level pheno comment "Information on which tests were run"

table file path pheno_all_marker_snp_pvalues_file=@pheno.marker.snp.pvalues dir pheno_dir disp ".snp.pvalues" parent cat_pheno_interesting_info class_level pheno comment "List of p-values for a subset of the SNPs in the master marker file"

file path pheno_marker_initial_pheno_file=@pheno.marker.initial.pheno dir pheno_dir disp ".marker.initial.pheno" parent cat_pheno_interesting_info class_level pheno comment "Specifies phenotypes for samples in the marker file; if specified takes precedence over project_all_marker_pheno_file "

file path pheno_all_marker_assoc_sample_keep_file=@pheno.marker.assoc.keep dir pheno_dir disp ".assoc.keep" parent cat_pheno_interesting_info class_level pheno comment "Samples to keep for any association testing in all marker file; if specified takes precedence over project_all_marker_assoc_sample_keep_file"

file path pheno_marker_pheno_file=@pheno.marker.pheno dir pheno_dir disp ".marker.pheno" parent cat_pheno_interesting_info class_level pheno comment "Specifies phenotypes for samples in the marker file"

table nohead onecol file path pheno_interesting_loci_dat_file=@pheno.interesting_loci.dat dir pheno_dir disp ".interesting_loci.dat" parent cat_pheno_interesting_info class_level pheno comment "Dat file listing loci that are interesting for this phenotype"

table nohead file path pheno_loci_sort_values_file=@pheno.loci.sort.values dir pheno_dir disp ".loci.sort.values" parent cat_pheno_interesting_info class_level pheno comment "Loci sort positions"

meta_table file path pheno_interesting_loci_meta_file=@pheno.interesting_loci.meta dir pheno_dir disp ".interesting_loci.meta" parent cat_pheno_interesting_info class_level pheno comment "Meta file to specify interesting loci for this phenotype" meta_level locus

minor file path pheno_is_trait_file=@pheno.is_trait dir pheno_dir disp ".is_trait" parent cat_pheno_trait_info class_level pheno comment "Placeholder file to indicate this is a trait"

minor file path pheno_ucsc_browser_file=@pheno.ucsc.browser dir pheno_dir disp ".ucsc.browser" parent cat_pheno_trait_info class_level pheno comment "Browser information for UCSC screenshot fetching"

"""
}
    