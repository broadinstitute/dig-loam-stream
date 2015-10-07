package loamstream.pipeline.examples

/**
 * LoamStream
 * Created by oliverr on 10/6/2015.
 */
object PipelineMtaPart1 {
  val string: String =
    """!title Analysis for MTA paper

lap_home=/home/unix/flannick/lap

#====================
#CLASSES

class project=Project with project_subset
minor class project_subset=Project Subset parent project
class mask=Mask parent project
class gene=Gene parent project
class special_gene_list=Special Gene List parent project
class simulation=Simulation parent project with simulation_batch
class simulation_batch=Simulation Batch parent simulation
class burden_test=Burden Test parent mask

#CLASSES
#====================

#====================
#DIRECTORIES
sortable mkdir path projects_dir=$unix_out_dir/projects
sortable mkdir path project_dir=$projects_dir/@project class_level project
sortable mkdir path project_subsets_dir=$project_dir/project_subsets class_level project
sortable mkdir path project_subset_dir=$project_subsets_dir/@project_subset class_level project_subset
sortable mkdir path masks_dir=$project_dir/masks class_level project
sortable mkdir path mask_dir=$masks_dir/@mask class_level mask
sortable mkdir path genes_dir=$project_dir/genes class_level project
sortable mkdir path gene_dir=$genes_dir/@gene class_level mask
sortable mkdir path simulations_dir=$project_dir/simulations class_level project
sortable mkdir path simulation_dir=$simulations_dir/@simulation class_level simulation
sortable mkdir path simulation_batches_dir=$simulation_dir/simulation_batches class_level simulation
sortable mkdir path simulation_batch_dir=$simulation_batches_dir class_level simulation
sortable mkdir path burden_tests_dir=$mask_dir/burden_tests class_level mask
sortable mkdir path burden_test_dir=$burden_tests_dir/@burden_test class_level burden_test

#DIRECTORIES
#====================


#====================
#CATEGORIES
cat cat_project_figure_data=null disp "Figures" class_level project
cat cat_project_annotation_data=null disp "Annotation data" class_level project
cat cat_project_mask_data=null disp "Mask data" class_level project
cat cat_project_simulation_data=null disp "Simulation data" class_level project

cat cat_project_subset_data=null disp "Project Subset Data" class_level project_subset

cat cat_mask_figure_data=null disp "Figures" class_level mask
cat cat_mask_data=null disp "Mask Data" class_level mask

cat cat_gene_figure_data=null disp "Figures" class_level gene
cat cat_gene_data=null disp "Gene Data" class_level gene

cat cat_special_gene_list_figure_data=null disp "Figures" class_level special_gene_list
cat cat_special_gene_list_data=null disp "Special Gene List Data" class_level special_gene_list

cat cat_simulation_data=null disp "Simulations Data" class_level simulation
cat cat_simulation_input_data=null disp "Input data" parent cat_simulation_data
cat cat_simulation_sim_data=null disp "Simulations" parent cat_simulation_data
cat cat_simulation_agg_sim_data=null disp "Aggregated" parent cat_simulation_data
cat cat_simulation_bar_data=null disp "Bar plot data" parent cat_simulation_data
cat cat_simulation_val_plot_data=null disp "Validation plots" parent cat_simulation_data
cat cat_simulation_bar_plot_data=null disp "Bar plots" parent cat_simulation_data
meta constant cat_n_samples=@n_samples disp "Npop" class_level simulation
meta constant cat_n_cases=@n_cases disp "Ncase" class_level simulation
meta constant cat_one_sided=@one_sided disp "One Sided" class_level simulation

cat cat_simulation_batch_data=null disp "Simulation Batch Data" class_level simulation_batch

cat cat_burden_test_data=null disp "Burden Test Data" class_level burden_test

#CATEGORIES
#====================

#====================
#UTILS

lap_trunk=$lap_home/trunk
lap_projects=$lap_home/projects
targeted_base_dir=/humgen/gsa-hpprojects/pfizer/projects/targeted/scratch/flannick
common_bin_dir=$lap_projects/common
targeted_bin_dir=$targeted_base_dir/bin
lib_dir=$targeted_base_dir/lib
bin_dir=$lap_projects/mta/bin

r_cmd=/broad/software/free/Linux/redhat_5_x86_64/pkgs/r_2.15.1/bin/R
r_script_cmd=$r_cmd -f @1 --slave --vanilla --args

draw_matrix_plot_cmd=$r_script_cmd($common_bin_dir/draw_matrix_plot.R)
draw_box_plot_cmd=$r_script_cmd($common_bin_dir/draw_box_plot.R)
draw_hist_plot_cmd=$r_script_cmd($common_bin_dir/draw_hist_plot.R)
draw_bar_plot_cmd=$r_script_cmd($common_bin_dir/draw_bar_plot.R)
draw_qq_plot_cmd=$r_script_cmd($common_bin_dir/draw_qq_plot.R)
draw_line_plot_cmd=$r_script_cmd($common_bin_dir/draw_line_plot.R)

!include $lap_trunk/config/common.cfg
pdflatex_cmd=/broad/software/free/Linux/redhat_5_x86_64/pkgs/texlive-20110510/bin/x86_64-linux/pdflatex

#vep_dir=$lib_dir/ensembl/variant_effect_predictor_new/ensembl-tools-release-76/scripts/variant_effect_predictor
vep_dir=$lib_dir/ensembl/variant_effect_predictor
vep_plugin_dir=$vep_dir/VEP_plugins
vep_condel_config_dir=$vep_plugin_dir/config/Condel/config
loftee_human_ancestor_path=$vep_plugin_dir/loftee-master/human_ancestor.fa.rz
ensembl_cache_dir=$lib_dir/ensembl/cache
ensembl_reference=$lib_dir/ensembl/reference/hg19/Homo_sapiens_assembly19.fasta

vep_cmd=perl $vep_dir/variant_effect_predictor.pl

snpeff_dir=$lib_dir/snpEff/snpEff
snpeff_jar=$snpeff_dir/snpEff.jar
snpsift_dir=$lib_dir/snpEff/snpEff
snpsift_jar=$snpsift_dir/SnpSift.jar
snpeff_config=$snpeff_dir/snpEff.config
snpeff_heap=4g
snpeff_db_dir=$snpeff_dir/db
snpeff_genome=GRCh37.74
snpeff_dbsnp=$snpeff_db_dir/dbSnp.vcf
snpeff_dbnsfp=$snpeff_db_dir/dbNSFP2.4.txt.gz
dbnsfp_cols_start=9
dbnsfp_cols_ignore=SLR


vep_custom_tabix=$conservation_dir/29way.omega.v2.allchr.bed.gz $conservation_dir/gerp.allchr.bed.gz $conservation_dir/phyloP.allchr.bed.gz $conservation_dir/1kg.20101123.snps_indels_sv.sites.bed.gz
vep_custom_names=29_mammals_omega GERP_UCSC_RS PhyloP 1000G

vep_custom_tabix=$conservation_dir/29way.omega.v2.allchr.bed.gz $conservation_dir/gerp.allchr.bed.gz
vep_custom_names=29_mammals_omega GERP_UCSC_RS

tabix_dir=/broad/software/free/Linux/redhat_5_x86_64/pkgs/tabix/tabix_0.2.2/bin
tabix_cmd=$tabix_dir/tabix
bgzip_cmd=$tabix_dir/bgzip

pseq_dir=$lib_dir/pseq/pseq-0.08.2
pseq_cmd=$pseq_dir/client/pseq

conditional_exec_cmd=perl $common_bin_dir/conditional_exec.pl
smart_join_cmd=perl $common_bin_dir/smart_join.pl
smart_cut_cmd=perl $common_bin_dir/smart_cut.pl
bin_values_cmd=perl $common_bin_dir/bin_values.pl
add_function_cmd=perl $common_bin_dir/add_function.pl
add_header_cmd=perl $common_bin_dir/add_header.pl
transpose_cmd=perl $common_bin_dir/transpose.pl
table_to_beamer_cmd=perl $common_bin_dir/table_to_beamer.pl
text_to_beamer_cmd=perl $common_bin_dir/text_to_beamer.pl
format_columns_cmd=perl $common_bin_dir/format_columns.pl
vcf_utils_cmd=perl $targeted_bin_dir/vcf_utils.pl
sync_ref_alt_cmd=perl $targeted_bin_dir/sync_ref_alt.pl
tped_to_bed_cmd=perl $targeted_bin_dir/tped_to_bed.pl

table_sum_stats_cmd=perl $common_bin_dir/table_sum_stats.pl
transpose_cmd=perl $common_bin_dir/transpose.pl

transcript_fig_cmd=perl $bin_dir/make_transcript_fig.pl

#UTILS
#====================

#====================
#PARAMETERS

variant_group_mem=8000

#PARAMETERS
#====================

#====================
#CONSTANTS

vep_id_annot=Uploaded_variation
vep_trans_annot=Feature
vep_gene_annot=Gene
vep_ccds_annot=CCDS
vep_type_annot=Consequence
vep_loc_annot=Location
vep_canonical_annot=CANONICAL
vep_protein_change_annot=Protein_change
vep_codon_change_annot=Codons
vep_type_synonymous_annot=synonymous_variant
vep_type_missense_annot=missense_variant
vep_type_nonsense_annot=stop_gained
vep_type_readthrough_annot=stop_lost

synonymous_mask=$vep_type_annot,eq:$vep_type_synonymous_annot
missense_mask=$vep_type_annot,eq:$vep_type_missense_annot
nonsense_mask=$vep_type_annot,'eq:$vep_type_nonsense_annot eq:$vep_type_readthrough_annot'
noncoding_mask=$vep_protein_change_annot,eq:$vep_missing_field
coding_mask=$vep_protein_change_annot,ne:$vep_missing_field

vep_consequence_annot=Consequence
vep_consequence_rank=transcript_ablation splice_donor_variant splice_acceptor_variant stop_gained frameshift_variant stop_lost initiator_codon_variant inframe_insertion inframe_deletion missense_variant transcript_amplification splice_region_variant incomplete_terminal_codon_variant synonymous_variant stop_retained_variant coding_sequence_variant mature_miRNA_variant 5_prime_UTR_variant 3_prime_UTR_variant intron_variant NMD_transcript_variant non_coding_exon_variant nc_transcript_variant upstream_gene_variant downstream_gene_variant TFBS_ablation TFBS_amplification TF_binding_site_variant regulatory_region_variant regulatory_region_ablation regulatory_region_amplification feature_elongation feature_truncation intergenic_variant

vep_id_col=1
vep_trans_col=5
vep_gene_col=4
vep_ccds_col=14

#CONSTANTS
#====================


#====================
#FILES

path file project_variant_vcf_file=@project.variant.vcf.gz dir project_dir disp ".variant.vcf.gz" parent cat_project_annotation_data class_level project comment "The VCF file containing the variants used for annotation as well as sample genotypes"

path file project_variant_vcf_index_file=@project.variant.vcf.gz.tbi dir project_dir disp ".variant.vcf.gz.tbi" parent cat_project_annotation_data class_level project comment "Tabix index file for this vcf file"

path file project_extended_variant_exclude_file=@project.extended.variant.exclude dir project_dir disp ".extended.variant.exclude" parent cat_project_annotation_data class_level project comment "Exclude file for burden tests"

path file project_extended_chrpos_exclude_file=@project.extended.chrpos.exclude dir project_dir disp ".extended.chrpos.exclude" parent cat_project_annotation_data class_level project comment "Exclude file for burden tests (chromosome:position format)"

path file project_variant_site_vcf_file=@project.variant.site.vcf dir project_dir disp ".variant.site.vcf" parent cat_project_annotation_data class_level project comment "The VCF file containing the variants used for annotation; sites only"

table path file project_intervals_file=@project.intervals dir project_dir disp ".intervals" parent cat_project_annotation_data class_level project comment "List of intervals, one for each subset"

meta_table path file project_subset_meta_file=@project.project_subset.meta dir project_dir disp ".project_subset.meta" parent cat_project_annotation_data class_level project comment "Meta file to load project subsets" meta_level project_subset

path file project_gencode_gtf_gz_file=@project.gencode.gtf.gz dir project_dir disp ".gencode.gtf.gz" parent cat_project_annotation_data class_level project comment "The raw GTF input file from GENCODE"

table path file project_gene_appris_file=@project.gene.appris.txt dir project_dir disp ".gene.appris.txt" parent cat_project_annotation_data class_level project comment "Appris information for all genes; external file"

table path file project_subset_vep_file=@project_subset.vep.tsv dir project_subset_dir disp ".vep.tsv" parent cat_project_subset_data class_level project_subset comment "The output of running the VEP on this subset of variants"

table path file project_subset_snpsift_file=@project_subset.snpsift dir project_subset_dir disp ".snpsift" parent cat_project_subset_data class_level project_subset comment "The output of running the SnpSIFT on this subset of variants"

#path file project_subset_vep_summary_file=@project_subset.vep.summary.html dir project_subset_dir disp ".vep.summary.html" parent cat_project_subset_data class_level project_subset comment "The summary file dumped by running the VEP on this subset of variants"

table path file project_vep_file=@project.vep.tsv dir project_dir disp ".vep.tsv" parent cat_project_annotation_data class_level project comment "The output of running the VEP on all variants"

table path file project_snpsift_file=@project.snpsift dir project_dir disp ".snpsift" parent cat_project_annotation_data class_level project comment "The output of running the SnpSIFT on all variants"

table path file project_maf_file=@project.maf.txt dir project_dir disp ".maf.txt" parent cat_project_annotation_data class_level project comment "The minor allele frequency for each variant"

table path file project_annot_file=@project.annot.txt dir project_dir disp ".annot.txt" parent cat_project_annotation_data class_level project comment "All annotations for each variant"

table path file project_gene_name_map_file=@project.gene.map dir project_dir disp ".gene.map" parent cat_project_annotation_data class_level project comment "A map from ENSEMBL gene ID to gene name"

table path file project_gene_canonical_file=@project.gene.canonical.txt dir project_dir disp ".gene.canonical.txt" parent cat_project_annotation_data class_level project comment "Map from gene to the canonical transcript, only includes genes with at least one variant in the canonical transcript"

!!expand:large:large:medium:small! \
table path file project_large_transcript_gene_file=@project.large.transcript.gene.txt dir project_dir disp ".large_transcript.gene.txt" parent cat_project_annotation_data class_level project comment "List of all transcripts to use, with parent gene name"

!!expand:large:large:medium:small! \
table path file project_large_annot_file=@project.large_annot.tsv dir project_dir disp ".large_annot.tsv" parent cat_project_annotation_data class_level project comment "List of annotations for large set of transcripts"

table path file project_num_gene_transcripts_text_file=@project.num.gene.transcripts.txt dir project_dir disp ".num.gene.transcripts.txt" parent cat_project_annotation_data class_level project comment "Number of transcripts per gene, dat file"

path file project_num_gene_transcripts_pdf_file=@project.num.gene.transcripts.pdf dir project_dir disp ".num.gene.transcripts.pdf" parent cat_project_figure_data class_level project comment "Number of transcripts per gene, pdf file"

table path file project_num_variant_transcripts_text_file=@project.num.variant.transcripts.txt dir project_dir disp ".num.variant.transcripts.txt" parent cat_project_mask_data class_level project comment "Number of transcripts per variant, dat file"

!!expand:large:large:medium:small! \
table path file project_num_variant_large_transcripts_text_file=@project.num.variant.large.transcripts.txt dir project_dir disp ".num.variant.large.transcripts.txt" parent cat_project_mask_data class_level project comment "Number of transcripts in large set per variant, dat file"

!!expand:common:common:lowfreq:rare! \
path file project_num_common_variant_transcripts_pdf_file=@project.num.common.variant.transcripts.pdf dir project_dir disp ".num.common.variant.transcripts.pdf" parent cat_project_figure_data class_level project comment "Number of transcripts per common variant, pdf file"

!!expand:large:large:medium:small! \
path file project_num_variant_maf_large_transcripts_pdf_file=@project.num.variant.maf.large.transcripts.pdf dir project_dir disp ".num.variant.maf.large.transcripts.pdf" parent cat_project_figure_data class_level project comment "Number of transcripts in large set per variant, stratified by frequency"

!!expand:large:large:medium:small! \
table path file mask_large_setid_file=@mask.large.setid dir mask_dir disp ".large.setid" parent cat_mask_data class_level mask comment "SetID file for this mask for large set of transcripts"

!!expand:large:large:medium:small! \
table path file mask_large_set_chrpos_file=@mask.large.set.chrpos dir mask_dir disp ".large.set.chrpos" parent cat_mask_data class_level mask comment "Chr:pos file for this mask for large set of transcripts; includes Non-pathogenic transcript"

!!expand:large:large:medium:small! \
table path file mask_num_variant_large_transcripts_text_file=@mask.num.variant.large.transcripts.txt dir mask_dir disp ".num.variant.large.transcripts.txt" parent cat_mask_data class_level mask comment "Number of transcripts in large set per variant, dat file"

!!expand:large:large:medium:small! \
table path file mask_num_any_all_variant_large_transcripts_text_file=@mask.num.any.all.large.txt dir mask_dir disp ".num.any.all.large.txt" parent cat_mask_data class_level mask comment "Aggregate number of variants in who change membership in mask based on transcript in large set"

!!expand:large:large:medium:small! \
table path file mask_gene_size_large_transcripts_text_file=@mask.gene.size.large.transcripts.txt dir mask_dir disp ".gene.size.large.transcripts.txt" parent cat_mask_data class_level mask comment "Number of variants in each transcript according to this mask"

!!expand:large:large:medium:small! \
table path file mask_gene_size_large_most_del_text_file=@mask.gene.size.large.most_del.txt dir mask_dir disp ".gene.size.large.most_del.txt" parent cat_mask_data class_level mask comment "Number of variants in mask according to most and least deleterious annotations"

!!expand:large:large:medium:small! \
table path file mask_gene_size_stats_large_transcripts_text_file=@mask.gene.size.stats.large.transcripts.txt dir mask_dir disp ".gene.size.stats.large.transcripts.txt" parent cat_mask_data class_level mask comment "Statistics on change in number of variants for different use of transcripts in large set according to this mask"

table path file mask_gene_size_stats_transcripts_text_file=@mask.gene.size.stats.transcripts.txt dir mask_dir disp ".gene.size.stats.transcripts.txt" parent cat_mask_data class_level mask comment "Statistics on change in number of variants for different use of transcripts in all sets according to this mask"

!!expand:common:common:lowfreq:rare! \
path file project_num_common_variant_transcripts_pdf_file=@project.num.common.variant.transcripts.pdf dir project_dir disp ".num.common.variant.transcripts.pdf" parent cat_project_figure_data class_level project comment "Number of transcripts per common variant, pdf file"


!!expand:common:all:common:lowfreq:rare! \
!!expand:large:large:medium:small! \
table path file project_num_any_all_common_variant_large_transcripts_text_file=@project.num.any.all.common.large.txt dir project_dir disp ".num.any.all.common.large.txt" parent cat_project_mask_data class_level project comment "Aggregate number of common variants who change membership in all masks based on transcript in large set"

!!expand:pdf:tex:pdf! \
path file project_num_any_all_all_variant_pdf_file=@project.num.any.all.all.pdf dir project_dir disp ".num.any.all.all.pdf" parent cat_project_figure_data trunk @project.num.any.all.all class_level project comment "Table of number of variants in each mask who change membership for each set of transcripts"

!!expand:large:large:medium:small! \
table path file project_gene_size_stats_large_transcripts_text_file=@project.gene.size.stats.large.transcripts.txt dir project_dir disp ".gene.size.stats.large.transcripts.txt" parent cat_project_mask_data class_level project comment "Statistics on change in number of variants for different use of transcripts in large set according to all masks"

#!!expand:,:canonical,Canonical:max,max vs. min:canonical,max vs. canonical:appris,max vs. appris:quantile,25th vs. 75th quantile:max_mean,max vs. mean:mean_min,mean vs. min:canonical_mean,canonical vs. mean:max_med,max vs. median:med_min,median vs. min:canonical_med,canonical vs. median! \

!!expand:,:canonical,Canonical:max_mean,max vs. mean:max_med,max vs. median! \
path file mask_gene_size_canonical_transcripts_pdf_file=@mask.gene.size.canonical.transcripts.pdf dir mask_dir disp ".gene.size.canonical.transcripts.pdf" parent cat_mask_figure_data class_level mask comment "Change in number of transcripts in this mask according to Canonical criteria"

#!!expand:,:canonical,Canonical:max,max vs. min:canonical,max vs. canonical:appris,max vs. appris:quantile,25th vs. 75th quantile:max_mean,max vs. mean:mean_min,mean vs. min:canonical_mean,canonical vs. mean:max_med,max vs. median:med_min,median vs. min:canonical_med,canonical vs. median! \

!!expand:,:canonical,Canonical:max_mean,max vs. mean:max_med,max vs. median! \
path file mask_gene_size_canonical_transcripts_scatter_pdf_file=@mask.gene.size.canonical.transcripts.scatter.pdf dir mask_dir disp ".gene.size.canonical.transcripts.scatter.pdf" parent cat_mask_figure_data class_level mask comment "Scatter plot of number of transcripts in this mask for Canonical transcripts"

!!expand:large:large:medium:small! \
table path file mask_gene_size_strat_large_transcripts_text_file=@mask.gene.size.strat.large.transcripts.txt dir mask_dir disp ".gene.size.strat.large.transcripts.txt" parent cat_mask_data class_level mask comment "Numbers of variants in each gene for different fold increases in large transcript set"

!!expand:large:large:medium:small! \
path file mask_gene_size_strat_large_transcripts_pdf_file=@mask.gene.size.strat.large.transcripts.pdf dir mask_dir disp ".gene.size.strat.large.transcripts.pdf" parent cat_mask_figure_data class_level mask comment "Box plots of the numbers of variants in each gene for different fold increases in large transcript set"

!!expand:,:small_,small.:,:small_,small.! \
!!expand:,:lambdav,freqv:.5,.001:4,.001:.5,.01:4,.01! \
path file mask_analytical_sample_size_lambdalambdav_freqfreqv_small_pdf_file=@mask.analytical.sample.size.lambdalambdav.freqfreqv.small.pdf dir mask_dir disp ".analytical.sample.size.lambdalambdav.freqfreqv.small.pdf" parent cat_mask_figure_data class_level mask comment "Plot of sample size lost based on analytical power calculations, lambda=lambdav, freq=freqv"

#!!expand:,:canonical,Canonical:max,max vs. min:canonical,max vs. canonical:appris,max vs. appris:quantile,25th vs. 75th quantile:max_mean,max vs. mean:mean_min,mean vs. min:canonical_mean,canonical vs. mean:max_med,max vs. median:med_min,median vs. min:canonical_med,canonical vs. median! \

!!expand:,:canonical,Canonical:max_mean,max vs. mean:max_med,max vs. median! \
!!expand:large:large:medium:small! \
path file project_gene_size_canonical_large_transcripts_pdf_file=@project.gene.size.canonical.large.transcripts.pdf dir project_dir disp ".gene.size.canonical.large.transcripts.pdf" parent cat_project_figure_data class_level project comment "Change in number of transcripts in all mask according to Canonical criteria applied across large transcript set"

!!expand:,:canonical,Canonical:max_mean,max vs. mean:max_med,max vs. median! \
!!expand:large:large:medium:small! \
path file project_gene_size_canonical_large_transcripts_sum_text_file=@project.gene.size.canonical.large.transcripts.sum.txt trunk @project.gene.size.canonical.large.transcripts.sum dir project_dir disp ".gene.size.canonical.large.transcripts.sum.txt" parent cat_project_mask_data class_level project comment "Change in number of transcripts in all mask according to Canonical criteria averaged across large transcript set"

!!expand:,:canonical,Canonical:max_mean,max vs. mean:max_med,max vs. median! \
!!expand:large:large:medium:small! \
!!expand:pdf:tex:pdf! \
path file project_gene_size_canonical_large_transcripts_sum_pdf_file=@project.gene.size.canonical.large.transcripts.sum.pdf trunk @project.gene.size.canonical.large.transcripts.sum dir project_dir disp ".gene.size.canonical.large.transcripts.sum.pdf" parent cat_project_figure_data class_level project comment "Change in number of transcripts in all mask according to Canonical criteria averaged across large transcript set"

!!expand:,:canonical,Canonical:max_mean,max vs. mean:max_med,max vs. median! \
!!expand:pdf:tex:pdf! \
path file project_gene_size_canonical_transcripts_sum_pdf_file=@project.gene.size.canonical.transcripts.sum.pdf trunk @project.gene.size.canonical.transcripts.sum dir project_dir disp ".gene.size.canonical.transcripts.sum.pdf" parent cat_project_figure_data class_level project comment "Change in number of transcripts in all mask according to Canonical criteria averaged across all transcript sets"

!!expand:large:large:medium:small! \
table path file special_gene_list_large_stats_txt_file=@special_gene_list.large.stats.txt dir project_dir disp ".large.stats.txt" parent cat_special_gene_list_data class_level special_gene_list comment "Statistics subset down to the list of special genes for large transcript list"

!!expand:large:large:medium:small! \
!!expand:pdf:tex:pdf! \
path file special_gene_list_large_stats_pdf_file=@special_gene_list.large.stats.pdf trunk @special_gene_list.large.stats dir project_dir disp ".large.stats.pdf" parent cat_special_gene_list_figure_data class_level special_gene_list comment "Statistics subset down to the list of special genes for large transcript list"

!!expand:large:large:medium:small! \
table path file gene_plot_large_transcript_file=@gene.plot.large.transcript.txt dir gene_dir disp ".plot.large.transcript.txt" parent cat_gene_data class_level gene comment "Transcript file to use for plotting the gene with large transcripts"

!!expand:large:large:medium:small! \
table path file gene_plot_large_variant_file=@gene.plot.large.variant.txt dir gene_dir disp ".plot.large.variant.txt" parent cat_gene_data class_level gene comment "Variant file to use for plotting the gene with large transcripts"

!!expand:large:large:medium:small! \
!!expand:pdf:tex:pdf! \
path file gene_large_transcripts_pdf_file=@gene.large.transcripts.pdf trunk @gene.large.transcripts dir gene_dir disp ".large.transcripts.pdf" parent cat_gene_figure_data class_level gene comment "Plot of large transcripts for the gene"

!!expand:large:large:medium:small! \
table path file project_large_transcript_exons_file=@project.large.transcript.exons dir project_dir disp ".large.transcript.exons" parent cat_project_annotation_data class_level project comment "List of exons for each transcript"

!!expand:large:large:medium:small! \
path file project_large_gene_exons_file=@project.large.gene.exons dir project_dir disp ".large.gene.exons" parent cat_project_simulation_data class_level project comment "Number of exons for each gene"

!!expand:transcripts_per_gene:transcripts_per_gene:exons_per_transcript:exons_per_gene:exon_length! \
!!expand:large:large:medium:small! \
path file project_large_transcripts_per_gene_dist_file=@project.large.transcripts_per_gene_dist.txt dir project_dir disp ".large.transcripts_per_gene_dist.txt" parent cat_project_simulation_data class_level project comment "Distribution of transcripts per gene for large set"

!!expand:large:large:medium:small! \
path file mask_large_variants_per_gene_dist_file=@mask.large.variants_per_gene_dist.txt dir mask_dir disp ".large.variants_per_gene_dist.txt" parent cat_mask_data class_level mask comment "Distribution of variants per gene for large set"

table path file simulation_maf_effect_size_file=@simulation.maf_effect_size.txt dir simulation_dir disp ".maf_effect_size.txt" parent cat_simulation_input_data class_level simulation comment "Specify file of maf/effect size samples"

table path file simulation_batch_intercept_data_file=@simulation_batch.intercept.data dir simulation_batch_dir disp ".intercept.data" parent cat_simulation_batch_data class_level simulation_batch comment "Data from running simulations to learn intercept for log(p) scaling"

!!expand:large:large:medium:small! \
table path file simulation_batch_large_null_data_file=@simulation_batch.large.null.data dir simulation_batch_dir disp ".large.null.data" parent cat_simulation_batch_data class_level simulation_batch comment "Data from running simulations to learn null distribution for large set of transcripts"

!!expand:large:large:medium:small! \
table path file simulation_batch_large_simulations_output_file=@simulation_batch.large.sim.out dir simulation_batch_dir disp ".large.sim.out" parent cat_simulation_batch_data class_level simulation_batch comment "Output of simulations for large set"

meta_table path file simulation_simulation_batch_meta_file=@simulation.simulation_batch.meta dir simulation_dir disp ".simulation_batch.meta" parent cat_simulation_input_data class_level simulation comment "Meta file for simulation batches" meta_level simulation_batch

table path file simulation_intercept_data_file=@simulation.intercept.data dir simulation_dir disp ".intercept.data" parent cat_simulation_sim_data class_level simulation comment "Data from running simulations to learn intercept for log(p) scaling"

!!expand:burden:burden:skat:skat_perm! \
!!expand:large:large:medium:small! \
table path file simulation_large_burden_null_data_file=@simulation.large.burden.null.data dir simulation_dir disp ".large.burden.null.data" parent cat_simulation_sim_data class_level simulation comment "Data from running simulations to learn null distribution for burden test"

path file simulation_intercept_pdf_file=@simulation.intercept.pdf dir simulation_dir disp ".intercept.pdf" parent cat_simulation_val_plot_data class_level simulation comment "Plot of intercept values for log(p) scaling"

path file simulation_intercept_file=@simulation.intercept dir simulation_dir disp ".intercept" parent cat_simulation_sim_data class_level simulation comment "Intercept for log(p) scaling"

!!expand:large:large:medium:small! \
table path file simulation_large_simulations_output_file=@simulation.large.sim.out dir simulation_dir disp ".large.sim.out" parent cat_simulation_sim_data class_level simulation comment "Output of simulations for large set, per simulation statistics"

path file simulation_num_gene_transcripts_pdf_file=@simulation.num.gene.transcripts.pdf dir simulation_dir disp ".num.gene.transcripts.pdf" parent cat_simulation_val_plot_data class_level simulation comment "Number of transcripts per gene"

path file simulation_gene_size_transcripts_pdf_file=@simulation.gene.size.transcripts.pdf dir simulation_dir disp ".gene.size.transcripts.pdf" parent cat_simulation_val_plot_data class_level simulation comment "Change in number of transcripts from this simulation between pathogenic and collapse criteria"

!!expand:large:large:medium:small! \
table path file simulation_large_simulations_aggregated_output_file=@simulation.large.sim.aggregated.out dir simulation_dir disp ".large.sim.aggregated.out" parent cat_simulation_agg_sim_data class_level simulation comment "Output of simulations for large set, aggregated statistics"

!!expand:,:varexp,varianceexplained:varexp,variance explained:fold_increase,fold increase:ntrans,number of transcripts:nvar,number of variants:percentile,percentiles:reduced_percentile,reduced percentiles:top_20,top 20%! \
!!expand:large:large:medium:small! \
table path file simulation_large_simulations_varexp_aggregated_output_file=@simulation.large.sim.varexp.aggregated.out dir simulation_dir disp ".large.sim.varexp.aggregated.out" parent cat_simulation_agg_sim_data class_level simulation comment "Output of simulations for large set, aggregated statistics over varianceexplained"

!!expand:large:large:medium:small! \
table path file simulation_large_rel_bar_dat_file=@simulation.large.rel.bar.dat dir simulation_dir disp ".large.rel.bar.dat" parent cat_simulation_bar_data class_level simulation comment "Relative change in sample size between different criteria for large set of transcripts"

!!expand:,:varexp,varianceexplained:varexp,variance explained:fold_increase,fold increase:ntrans,number of transcripts:nvar,number of variants:percentile,percentiles:reduced_percentile,reduced percentiles:top_20,top 20%! \
!!expand:large:large:medium:small! \
table path file simulation_large_varexp_rel_bar_dat_file=@simulation.large.varexp.rel.bar.dat dir simulation_dir disp ".large.varexp.rel.bar.dat" parent cat_simulation_bar_data class_level simulation comment "Relative change in sample size between different criteria for large set of transcripts, stratified by varianceexplained"

!!expand:,:path_collapse,path_and_collapse:path_collapse,pathogenic and collapse:all_burden,all possible burden:burden_skat,all burden and SKAT:bonf,bonferroni:skat,just SKAT:skat_perm,SKAT with permutations:rand_path,trade-off between random and pathogenic:rand_path_skat,trade-off between random and pathogenic SKAT! \
!!expand:large:large:medium:small! \
path file simulation_large_path_collapse_rel_bar_pdf_file=@simulation.large.path_collapse.rel.bar.pdf dir simulation_dir disp ".large.path_collapse.rel.bar.pdf" parent cat_simulation_bar_plot_data class_level simulation comment "Relative change in sample size between path_and_collapse criteria for large set of transcripts"

!!expand:,:no_ylab_,no.ylab.:,:no_ylab_,no.ylab.! \
!!expand:,:path_collapse,path_and_collapse:path_collapse,pathogenic and collapse:all_burden,all possible burden:burden_skat,all burden and SKAT:bonf,bonferroni:skat,just SKAT:skat_perm,SKAT with permutations:rand_path,trade-off between random and pathogenic:rand_path_skat,trade-off between random and pathogenic SKAT! \
path file simulation_path_collapse_no_ylab_rel_bar_pdf_file=@simulation.path_collapse.no.ylab.rel.bar.pdf dir simulation_dir disp ".path_collapse.no.ylab.rel.bar.pdf" parent cat_simulation_bar_plot_data class_level simulation comment "Relative change in sample size between path_and_collapse criteria for all sets of transcripts"

!!expand:,:path_collapse,path_and_collapse:rand_path,trade-off between random and pathogenic:rand_path_skat,trade-off between random and pathogenic SKAT! \
!!expand:large:large:medium:small! \
path file simulation_large_path_collapse_tradeoff_pdf_file=@simulation.large.path_collapse.tradeoff.pdf dir simulation_dir disp ".large.path_collapse.tradeoff.pdf" parent cat_simulation_bar_plot_data class_level simulation comment "Trade-off between path_and_collapse criteria for large set of transcripts"

!!expand:,:no_ylab_,no.ylab.:,:no_ylab_,no.ylab.! \
!!expand:,:path_collapse,path_and_collapse:rand_path,trade-off between random and pathogenic:rand_path_skat,trade-off between random and pathogenic SKAT! \
path file simulation_path_collapse_no_ylab_tradeoff_pdf_file=@simulation.path_collapse.no.ylab.tradeoff.pdf dir simulation_dir disp ".path_collapse.no.ylab.tradeoff.pdf" parent cat_simulation_bar_plot_data class_level simulation comment "Trade-off between path_and_collapse criteria for all sets of transcripts"

!!expand:,:varexp,varianceexplained:varexp,variance explained:fold_increase,fold increase:ntrans,number of transcripts:nvar,number of variants:percentile,percentiles:reduced_percentile,reduced percentiles:top_20,top 20%! \
!!expand:,:path_collapse,path_and_collapse:path_collapse,pathogenic and collapse:all_burden,all possible burden:burden_skat,all burden and SKAT:bonf,bonferroni:skat,just SKAT:skat_perm,SKAT with permutations:rand_path,random and pathogenic:rand_path_skat,random and pathogenic SKAT! \
!!expand:large:large:medium:small! \
path file simulation_large_varexp_path_collapse_rel_bar_pdf_file=@simulation.large.varexp.path_collapse.rel.bar.pdf dir simulation_dir disp ".large.varexp.path_collapse.rel.bar.pdf" parent cat_simulation_bar_plot_data class_level simulation comment "Relative change in sample size between path_and_collapse criteria for large set of transcripts, stratified by varianceexplained"

!!expand:,:no_ylab_,no.ylab.:,:no_ylab_,no.ylab.! \
!!expand:,:varexp,varianceexplained:varexp,variance explained:fold_increase,fold increase:ntrans,number of transcripts:nvar,number of variants:percentile,percentiles:reduced_percentile,reduced percentiles:top_20,top 20%! \
!!expand:,:path_collapse,path_and_collapse:path_collapse,pathogenic and collapse:all_burden,all possible burden:burden_skat,all burden and SKAT:bonf,bonferroni:skat,just SKAT:skat_perm,SKAT with permutations:rand_path,random and pathogenic:rand_path_skat,random and pathogenic SKAT! \
path file simulation_varexp_path_collapse_no_ylab_rel_bar_pdf_file=@simulation.varexp.path_collapse.no.ylab.rel.bar.pdf dir simulation_dir disp ".varexp.path_collapse.no.ylab.rel.bar.pdf" parent cat_simulation_bar_plot_data class_level simulation comment "Relative change in sample size between path_and_collapse criteria for all setts of transcripts, stratified by varianceexplained"

!!expand:,:varexp,varianceexplained:varexp,variance explained:fold_increase,fold increase:ntrans,number of transcripts:nvar,number of variants:percentile,percentiles:reduced_percentile,reduced percentiles:top_20,top 20%! \
!!expand:,:path_collapse,path_and_collapse:rand_path,random and pathogenic:rand_path_skat,random and pathogenic SKAT! \
!!expand:large:large:medium:small! \
path file simulation_large_varexp_path_collapse_tradeoff_pdf_file=@simulation.large.varexp.path_collapse.tradeoff.pdf dir simulation_dir disp ".large.varexp.path_collapse.tradeoff.pdf" parent cat_simulation_bar_plot_data class_level simulation comment "Trade-off in sample size between path_and_collapse criteria for large set of transcripts, stratified by varianceexplained"

path file burden_test_ped_file=@burden_test.ped dir project_dir disp ".ped" parent cat_burden_test_data class_level burden_test comment "EPACTS ped file format containing the phenotypes as well as any covariates"

nohead table path file burden_test_gene_path_trans_list_file=@burden_test.gene.path.trans.list dir burden_test_dir disp ".gene.path.trans.list" parent cat_burden_test_data class_level burden_test comment "A file with two columns, first is gene to test, second is pathogenic transcript"

!!expand:large:large:medium:small! \
nohead table path file burden_test_large_initial_set_chrpos_file=@burden_test.large.initial.set.chrpos dir burden_test_dir disp ".large.initial.set.chrpos" parent cat_burden_test_data class_level burden_test comment "Set chr/pos file consisting only of genes for this burden test"

!!expand:large:large:medium:small! \
nohead table path file burden_test_large_set_chrpos_file=@burden_test.large.set.chrpos dir burden_test_dir disp ".large.set.chrpos" parent cat_burden_test_data class_level burden_test comment "Set chr/pos file consisting only of genes for this burden test; includes Non-pathogenic transcript"

!!expand:large:large:medium:small! \
table path file burden_test_large_gassoc_file=@burden_test.large.gassoc dir burden_test_dir disp ".large.gassoc" parent cat_burden_test_data class_level burden_test comment "Output of running the burden test"

!!expand:large:large:medium:small! \
table path file burden_test_large_flat_gassoc_file=@burden_test.large.flat.gassoc dir burden_test_dir disp ".large.flat.gassoc" parent cat_burden_test_data class_level burden_test comment "Processed output of gassoc file to have separate columns for each test"

!!expand:large:large:medium:small! \
table path file burden_test_large_path_non_sign_comp_file=@burden_test.large.path.non.sign.comp.txt dir burden_test_dir disp ".large.path.non.sign.comp.txt" parent cat_burden_test_data class_level burden_test comment "Statistics on the number of variants in correct direction between pathogenic and non-pathogenic transcript for large transcript set"

#FILES
#====================


#====================
#COMMANDS

prop num_project_subsets=scalar

local cmd make_project_variant_vcf_index_file=$tabix_cmd -f -p vcf !{input,,project_variant_vcf_file} class_level project

cmd make_project_extended_chrpos_exclude_file=$smart_join_cmd --in-delim $tab !{input,--file,project_extended_variant_exclude_file} --exec "cat !{input,,project_large_annot_file} | awk -F\"\\t\" -v OFS=\"\\t\" 'NR == 1 {for(i=1;i<=NF;i++) {m[\\$i]=i}} NR > 1 {print \\$m[\"VAR\"],\\$m[\"Location\"]}' | sort -u" --extra 2 --fill 2 --fill-value NA | cut -f2 | awk '\$1 != "NA"' | sed 's/:/\t/' > !{output,,project_extended_chrpos_exclude_file} class_level project

local cmd make_project_variant_site_vcf_file=zcat !{input,,project_variant_vcf_file} | cut -f1-8 > !{output,,project_variant_site_vcf_file} class_level project

local cmd make_project_intervals_file=cat !{input,,project_variant_site_vcf_file} | perl -ne 'BEGIN {\$n=`cat !{input,,project_variant_site_vcf_file} | fgrep -v \\\# | wc -l`; \$num_per = int(\$n / !{prop,,project,num_project_subsets} + 0.5); \$l=0} unless (/^\\#/) {chomp; @f = split(/\t/); if (!defined \$chrom) {\$chrom = \$f[0]; \$pos = \$f[1]} \$l++; if (\$f[0] ne \$chrom) {print "\$chrom:\$pos "; \$chrom=\$f[0]; \$pos=\$f[1]} if (\$l % \$num_per == 0) {print "\$chrom:\${pos}-\$f[1]\n"; \$pos=\$f[1]+1;}} END { unless (\$l % \$num_per) {print "\$chrom:\${pos}-\$f[1]\n"} }' > !{output,,project_intervals_file} class_level project

prop subset_interval=scalar

local cmd make_project_subset_meta_file=cat !{input,,project_intervals_file} | awk '{n="!{prop,,project}_subset_"NR; print n,"class project_subset"; print n,"parent","!{prop,,project}"; print n,"sort",NR; print n,"subset_interval",\$0}' > !{output,,project_subset_meta_file} class_level project

#--all_refseq --merged !{output,--stats_file,project_subset_vep_summary_file}

subset_vcf_file=(zcat !{input,,project_variant_vcf_file} | $vcf_utils_cmd --print-header | cut -f1-8; $tabix_cmd !{input,,project_variant_vcf_file} !{prop,,project_subset,subset_interval} | cut -f1-8)

short cmd make_project_subset_vep_file=$subset_vcf_file | $vep_cmd --offline -o STDOUT --dir $ensembl_cache_dir  --polyphen b --sift b --ccds --canonical --regulatory --domains flags --fields Uploaded_variation,Location,Allele,Gene,Feature,Feature_type,Consequence,cDNA_position,CDS_position,Protein_position,Amino_acids,Codons,Existing_variation,SOURCE,CCDS,CANONICAL,HGNC,ENSP,DOMAINS,MOTIF_NAME,MOTIF_POS,HIGH_INF_POS,MOTIF_SCORE_CHANGE,SIFT,PolyPhen > !{output,,project_subset_vep_file} class_level project_subset

snpsift_exe_helper=java -Xmx$snpeff_heap -jar $snpsift_jar

short cmd make_project_subset_snpsift_file=cols=`zcat $snpeff_dbnsfp | head -n1 | cut -f${dbnsfp_cols_start}- | sed 's/\t/\n/g' | egrep -v '$dbnsfp_cols_ignore' | tr '\n' ' '`; \
  $subset_vcf_file | cut -f1-8 \
      | $snpsift_exe_helper dbnsfp -v $snpeff_dbnsfp -f `echo \$cols | sed 's/\s\s*/,/g'` - \
      | $vcf_utils_cmd --print-annots | $smart_cut_cmd --tab-delim --select-col 0,1,ID --select-col 0,1,"\$cols" \
  > !{output,,project_subset_snpsift_file} class_level project_subset

    """

}

