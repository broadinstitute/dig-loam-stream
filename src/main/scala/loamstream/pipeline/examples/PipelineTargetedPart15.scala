
package loamstream.pipeline.examples

/**
 * LoamStream
 * Created by oliverr on 10/6/2015.
 */
object PipelineTargetedPart15 {
  val string =
 """frac_above_threshold_mean=$summary_mean(${frac_above_threshold})

#local cmd make_call_set_gene_cum_coverage_dat_file=$table_sum_stats_cmd --out-delim , --summaries --has-header --col $frac_above_threshold --group-col 2 --label "!{prop,,call_set,disp}" --print-header --in-delim $coverage_dat_delim < !{input,,call_set_gene_dist_coverage_dat_file} | $make_cum_dat_cmd --nbins $num_cum_bins --min 0 --max 1 --header 1 --col $frac_above_threshold_mean --in-delim $coverage_dat_delim --out-delim $coverage_dat_delim > !{output,,call_set_gene_cum_coverage_dat_file} class_level call_set skip_if failed

#local cmd make_call_set_sample_coverage_pdf_file=$draw_coverage_bar_plot_cmd(!{input\,\,call_set_sample_coverage_dat_file},!{output\,\,call_set_sample_coverage_pdf_file},'Coverage across passed samples -- !{prop\,\,call_set\,disp}') class_level call_set skip_if failed
#local cmd make_call_set_gene_dist_coverage_pdf_file=$draw_coverage_box_plot_cmd(!{input\,\,call_set_gene_dist_coverage_dat_file},!{output\,\,call_set_gene_dist_coverage_pdf_file},'Coverage across genes -- passed !{prop\,\,call_set\,disp} samples') class_level call_set skip_if or,whole_exome,failed
#local cmd make_call_set_gene_cum_coverage_pdf_file=$draw_line_plot_cmd !{input,,call_set_gene_cum_coverage_dat_file} !{output,,call_set_gene_cum_coverage_pdf_file} 'Distribution of gene coverage -- passed !{prop,,call_set,disp} samples' 'Fraction of bases >= ${threshold}x' 'Fraction of genes above' 1 2 sep=$coverage_dat_delim decreasing=TRUE class_level call_set skip_if failed

local cmd make_project_sample_coverage_dat_file=$table_sum_stats_threshold_cmd --label sample --col 2 --only-print-header > !{output,,project_sample_coverage_dat_file} && xargs cat < !{input,,project_sample_coverage_stats_file_list} !{input,sample_coverage_stats_file,unless_prop=failed} >> !{output,,project_sample_coverage_dat_file} class_level project

meta_table cmd make_project_failed_sample_list_file=!{prop,,sample,if_prop=failed} !{output,project_failed_sample_list_file} class_level project skip_if no_sample_tracking
meta_table cmd make_project_passed_sample_list_file=!{prop,,sample,unless_prop=failed} !{output,project_passed_sample_list_file} class_level project skip_if no_sample_tracking

local cmd make_project_empty_failed_sample_list_file=rm -f !{output,,project_failed_sample_list_file} && touch !{output,,project_failed_sample_list_file} class_level project run_if no_sample_tracking
local cmd make_project_untracked_passed_sample_list_file=$smart_cut_cmd --ignore-status $broken_pipe_status !{raw,--exec,call_set,"zcat *call_set_compressed_vcf_file | $vcf_utils_cmd --samp-map *call_set_sample_vcf_id_to_sample_id_file --print-samps",if_prop=zip_vcf:eq:.gz,allow_empty=1}!{raw,--exec,call_set,"cat *call_set_compressed_vcf_file | $vcf_utils_cmd --samp-map *call_set_sample_vcf_id_to_sample_id_file --print-samps",unless_prop=zip_vcf:eq:.gz,allow_empty=1} !{input,call_set_sample_vcf_id_to_sample_id_file} !{input,call_set_compressed_vcf_file} | sort -u > !{output,,project_passed_sample_list_file} class_level project run_if no_sample_tracking

total_status=Tot
failed_status=Low Cov
passed_status=Cov Met
qc_fail_status=Rem Seq
popgen_fail_status=Rem Pop
popgen_pass_status=Inc Pop
covar_pass_status=Inc Covar
cluster_pass_status=Inc Cluster
qc_pass_status=QC Plus
related_fail_status=Rel

total_status_long=Attempted
failed_status_long=Coverage Not Met
passed_status_long=Coverage Met
qc_fail_status_long=Removed Seq
popgen_fail_status_long=Removed Popgen
popgen_pass_status_long=Included Popgen
covar_pass_status_long=Included Covar
cluster_pass_status_long=Included Cluster
qc_pass_status_long=QC Plus
related_fail_status_long=Related


local cmd make_project_sample_failure_status_file=$smart_cut_cmd --in-delim $tab --exec "sed 's/$/\t1\t0\t0/' !{input,,project_failed_sample_list_file}" --exec "sort !{input,,project_passed_sample_list_file} !{input,,project_sample_seq_exclude_file} | uniq -d | sed 's/$/\t0\t1\t0/' " --exec "sed 's/$/\t0\t0\t1/' !{input,,project_sample_include_file}" | sed '1 s/^/Sample\t$failed_status\t$qc_fail_status\t$qc_pass_status\n/' > !{output,,project_sample_failure_status_file} class_level project

local cmd make_project_sample_cum_coverage_dat_file=$make_cum_dat_cmd --nbins $num_cum_bins --min 0 --max 1 --header 1 --col $frac_above_threshold --in-delim $coverage_dat_delim --out-delim $coverage_dat_delim < !{input,,project_sample_coverage_dat_file} > !{output,,project_sample_cum_coverage_dat_file} class_level project
local cmd make_project_sample_cum_coverage_pdf_file=$draw_line_plot_cmd !{input,,project_sample_cum_coverage_dat_file} !{output,,project_sample_cum_coverage_pdf_file} 'Distribution of sample coverage -- passed samples' 'Fraction of bases >= ${threshold}x' 'Fraction of samples above' 1 2 sep=$coverage_dat_delim decreasing=TRUE class_level project

!!expand:type:gene:exon:bait! \
cmd meta_table make_project_sample_type_coverage_stats_file_list=!{input,,sample_type_coverage_stats_file,unless_prop=failed} !{output,project_sample_type_coverage_stats_file_list} class_level project skip_if no_coverage
cmd meta_table make_project_sample_coverage_file_list=!{prop,,sample}\t!{input,,sample_coverage_file,unless_prop=failed} !{output,project_sample_coverage_file_list} class_level project skip_if no_coverage

!!expand:,:type,group_cols,val_col:gene,1,4:exon,1 --group-col 2,5:bait,1,4! \
local cmd make_project_type_dist_coverage_dat_file=$table_sum_stats_threshold_cmd --only-print-header --label sample --group-col group_cols --col val_col > !{output,,project_type_dist_coverage_dat_file} && xargs cat < !{input,,project_sample_type_coverage_stats_file_list} !{input,sample_type_coverage_stats_file,unless_prop=failed} >> !{output,,project_type_dist_coverage_dat_file} class_level project skip_if no_coverage 
local cmd make_project_type_gene_coverage_pdf_file=$draw_box_plot_cmd(!{input\,\,project_gene_dist_coverage_dat_file} !{output\,\,project_gene_dist_coverage_pdf_file} 'Coverage across genes -- all passed samples' '' '% bases >= ${threshold}x' $frac_above_threshold label=1 sep=$coverage_dat_delim) class_level project skip_if or,whole_exome,no_coverage

!!expand:,:type,group_col:gene,1:exon,1 --group-col 2:bait,1! \
local cmd make_project_type_dist_mean_coverage_dat_file=cat !{input,,project_type_dist_coverage_dat_file} | $table_sum_stats_cmd --out-delim , --summaries --has-header --print-header --col $frac_above_threshold --group-col group_col --in-delim $coverage_dat_delim > !{output,,project_type_dist_mean_coverage_dat_file} class_level project skip_if no_coverage

!!expand:type:gene:exon:bait! \
local cmd make_project_type_cum_coverage_dat_file=cat !{input,,project_type_dist_mean_coverage_dat_file} | $make_cum_dat_cmd --nbins $num_cum_bins --min 0 --header 1 --max 1 --col $frac_above_threshold_mean --in-delim $coverage_dat_delim --out-delim $coverage_dat_delim > !{output,,project_type_cum_coverage_dat_file} class_level project skip_if no_coverage

!!expand:type:gene:exon:bait! \
local cmd make_project_type_cum_coverage_pdf_file=$draw_line_plot_cmd !{input,,project_type_cum_coverage_dat_file} !{output,,project_type_cum_coverage_pdf_file} 'Distribution of type coverage -- all passed samples' 'Average per sample fraction of bases >= ${threshold}x' 'Fraction of type above' 1 2 sep=$coverage_dat_delim decreasing=TRUE class_level project skip_if no_coverage

local cmd make_project_lowest_gene_dist_coverage_dat_file=$smart_join_cmd --exec "$smart_cut_cmd !{input,--file,project_gene_dist_mean_coverage_dat_file} --in-delim , --select-col 1,1 --select-col 1,1,$frac_above_threshold_mean" !{input,--file,project_gene_dist_coverage_dat_file} --arg-delim : --in-delim 1:, --in-delim 2:, --multiple 2 --header 1 --out-delim ,  > !{output,,project_lowest_gene_dist_coverage_dat_file} class_level project skip_if no_coverage
local cmd make_project_lowest_gene_dist_coverage_pdf_file=$draw_box_plot_cmd(!{input\,\,project_lowest_gene_dist_coverage_dat_file} !{output\,\,project_lowest_gene_dist_coverage_pdf_file} 'Coverage across genes -- all passed samples' '' '% bases >= ${threshold}x' $frac_above_threshold label=1 sep=$coverage_dat_delim order=$frac_above_threshold_mean) class_level project skip_if or,whole_exome,no_coverage

#gs -dNOPAUSE -sDEVICE=pdfwrite -sOUTPUTFILE=finalcombinedpdf.pdf -dBATCH 1.pdf 2.pdf 3.pdf


prop no_sample_tracking=scalar

#plinkseq cmds
local cmd make_plinkseq_ind_info_file=cat !{input,,project_passed_sample_list_file} | sed 's/$/\t.\t.\t.\t.\t./' > !{output,,project_plinkseq_ind_info_file} class_level project 

local cmd make_project_sample_subset_plinkseq_ind_info_file=awk -v OFS="\t" -F "\t" 'NR >= !{prop,,project_sample_subset,samp_start} && NR < !{prop,,project_sample_subset,samp_end} {print}' < !{input,,project_plinkseq_ind_info_file} > !{output,,project_sample_subset_plinkseq_ind_info_file} class_level project_sample_subset

prop pheno_qt=scalar default 0
prop pheno_type=scalar
prop pheno_missing=scalar
prop pheno_description=scalar
prop no_strat_phenos=scalar


max_region_length=8500000
min_region_length=0
prop region_buffer=scalar default 5000

local cmd make_project_region_list_file=sort -k2,2 -k3,3n !{input,,project_gene_target_file} !{input,,project_transcript_target_file} | uniq -f1 | perl $targeted_bin_dir/get_regions_from_gene_interval_list.pl $max_region_length $min_region_length !{prop,,project,region_buffer} --require-disjoint > !{output,,project_region_list_file} class_level project

local cmd make_project_all_regions_dat_file=$interesting_region_or_loci_dat_helper("cat !{input,,project_region_list_file} | cut -f1 | sort -u") > !{output,,project_all_regions_dat_file} class_level project run_if all_regions_interesting

local cmd make_project_interesting_regions_dat_file=$smart_cut_cmd --in-delim $tab !{raw,,pheno,--exec "$optional_cat(*pheno_interesting_loci_dat_file)",unless_prop=not_trait} !{input,pheno_interesting_loci_dat_file,optional=1} | cat - !{input,,project_all_regions_dat_file,if_prop=all_regions_interesting,allow_empty=1} | sort -u > !{output,,project_interesting_regions_dat_file} class_level project with pheno

prop chrom=scalar
prop ref_chrom=scalar
prop region_range=scalar
prop region_start=scalar
prop region_end=scalar

local cmd make_project_interesting_regions_meta_file=$interesting_regions_or_loci_helper(region,project,project_interesting_regions_dat_file,project_interesting_regions_meta_file,,) class_level project with pheno

local cmd make_project_seq_iid_fid_map_file=awk '{print \$1,\$1,".","."}' !{input,,project_passed_sample_list_file} | $rename_seq_tfam_file | awk -v OFS="\t" '{print \$2,\$1}' | sed '1 s/^/IID\tFID\n/' > !{output,,project_seq_iid_fid_map_file} class_level project 

sample_list_to_plink_sample_list=$smart_join_cmd --in-delim $tab --exec "awk 'NR > (1 - @2)' !{input,,project_seq_iid_fid_map_file}" @1 --extra 1 --out-delim $tab --header @2 | awk -v OFS="\t" '{t=\$2; \$2=\$1; \$1=t} {print}'

#plinkseq_resources_helper=--resources $plinkseq_resources_dir --seqdb $plinkseq_seqdb --refdb $plinkseq_refdb
plinkseq_resources_helper=!{input,--seqdb,project_plinkseq_seqdb_file}

short cmd make_project_plinkseq_seqdb_file=rm -f !{output,,project_plinkseq_seqdb_file} && $pseq_no_project_cmd seq-load !{output,--seqdb,project_plinkseq_seqdb_file} --file $reference_file --name hg${hg_build} --description Broad --format build=hg${hg_build} repeat-mode=lower class_level project

!!expand:type:project:project_variant_subset! \
short cmd make_type_plinkseq_locdb_file=rm -f !{output,,type_plinkseq_locdb_file} && $pseq_no_project_cmd loc-load !{output,--locdb,type_plinkseq_locdb_file} --group $pseq_genes_loc_group !{input,--file,type_genes_gtf_file} --keep-unmerged $pseq_genes_loc_group class_level type

pseq_generic_locset_name=generic
pseq_custom_locset_name=custom

pseq_generic_loc_group=$pseq_genes_loc_group
pseq_custom_loc_group=$pseq_genes_loc_group

!!expand:pathwayburdentype:custom! \
local cmd make_project_pathway_pathwayburdentype_flat_locset_file=sort -k2 !{input,,project_pathwayburdentype_locset_file} | perl -ne '@a = split; \$a{\$a[1]}{\$a[0]} = 1; END {foreach \$a (sort keys %a) {print \$a . "\t" . join("|", keys %{\$a{\$a}}) . "\n"}}' > !{output,,project_pathway_pathwayburdentype_flat_locset_file} class_level project with pheno

!!expand:,:projectt,projectl:project,project:project_sample_subset,project:project_variant_subset,project_variant_subset! \
local cmd make_projectt_plinkseq_project_file=rm -f !{raw,,projectt,*projectt_plinkseq_vardb_file} !{raw,,projectt,*projectt_plinkseq_inddb_file} && $pseq_cmd !{output,,projectt_plinkseq_project_file} new-project !{raw,--vcf,projectt,*projectt_vcf_file} !{raw,--vcf,projectt,*projectt_indel_vcf_file} !{raw,--vcf,projectt,*projectt_multiallelic_vcf_file} $plinkseq_resources_helper --scratch !{raw,,projectt,$projectt_plinkseq_project_temp_dir} !{raw,--locdb,projectl,*projectl_plinkseq_locdb_file} && touch !{output,,projectt_plinkseq_project_done_file} class_level projectt

pseq_snp_tag=snps
pseq_indel_tag=indels
pseq_multiallelic_tag=multiallelics

!|expand:;:projectt;projectl;projecti;projecte;extracmd;after;shortt:\
project;project;project;;load-vcf;run_with project_sample_subset,project_variant_subset;:\
project_sample_subset;project;project_sample_subset;_project_sample_subset;load-vcf;rusage_mod $load_sample_db_mem;short:\
project_variant_subset;project_variant_subset;project;_project_variant_subset;load-vcf;;short| \
shortt cmd make_projectt_plinkseq_project_dbs=rm -rf !{raw,,projectt,$projectt_plinkseq_project_out_dir/*} && rm -rf !{raw,,projectt,$projectt_plinkseq_project_temp_dir/*} && $pseq_project_cmdprojecte load-vcf !{input,projectt_vcf_file} !{input,projectt_indel_vcf_file} !{input,projectt_multiallelic_vcf_file} !{output,projectt_plinkseq_vardb_file} && $pseq_project_cmdprojecte tag-file --id 1 --name $pseq_snp_tag && $pseq_project_cmdprojecte tag-file --id 2 --name $pseq_indel_tag && $pseq_project_cmdprojecte tag-file --id 3 --name $pseq_multiallelic_tag && $pseq_project_cmdprojecte load-pedigree !{input,--file,projecti_plinkseq_ind_info_file} !{output,projectt_plinkseq_inddb_file} && touch !{output,,projectt_plinkseq_db_done_file} !{output,,projectt_plinkseq_temp_done_file} !{input,projectl_plinkseq_locdb_file} class_level projectt after

#!!expand:,:projectt,projectl:project,project:project_sample_subset,project:project_variant_subset,project_variant_subset! \
#local cmd make_projectt_plinkseq_indel_project_file=$pseq_cmd !{output,,projectt_plinkseq_indel_project_file} new-project !{raw,--vcf,projectt,*projectt_indel_vcf_file} $plinkseq_resources_helper --scratch !{raw,,projectt,$projectt_plinkseq_indel_project_temp_dir} !{raw,--locdb,projectl,*projectl_plinkseq_locdb_file} && touch !{output,,projectt_plinkseq_indel_project_done_file} class_level projectt

#!|expand:;:projectt;projectl;projecti;projecte;after;shortt:project;project;project;;run_with project_sample_subset,project_variant_subset;:project_sample_subset;project;project_sample_subset;_project_sample_subset;;short:project_variant_subset;project_variant_subset;project;_project_variant_subset;;short| \
#shortt cmd make_projectt_plinkseq_indel_project_dbs=rm -rf !{raw,,projectt,$projectt_plinkseq_indel_project_out_dir/*} && rm -rf !{raw,,projectt,$projectt_plinkseq_indel_project_temp_dir/*} && $pseq_indel_project_cmdprojecte load-vcf !{input,projectt_indel_vcf_file} !{output,projectt_plinkseq_indel_vardb_file} && $pseq_indel_project_cmdprojecte load-pedigree !{input,--file,projecti_plinkseq_ind_info_file} !{output,projectt_plinkseq_indel_inddb_file} && touch !{output,,projectt_plinkseq_indel_db_done_file} !{output,,projectt_plinkseq_indel_temp_done_file} !{input,projectl_plinkseq_locdb_file} after class_level projectt 

!!expand:,:projectt,projectl:project,project:project_sample_subset,project:project_variant_subset,project_variant_subset! \
projectt_clean_project_helper=$plinkseq_resources_helper --scratch !{raw,,projectt,$projectt_plinkseq_clean_project_temp_dir} !{raw,--locdb,projectl,*projectl_plinkseq_locdb_file} && touch !{output,,projectt_plinkseq_clean_project_done_file}

!!expand:,:projectt,projectl:project,project:project_sample_subset,project:project_variant_subset,project_variant_subset! \
local cmd make_projectt_plinkseq_clean_project_file=rm -f !{output,,projectt_plinkseq_clean_project_file} && rm -f !{raw,,projectt,*projectt_plinkseq_clean_vardb_file} !{raw,,projectt,*projectt_plinkseq_clean_inddb_file} && $pseq_cmd !{output,,projectt_plinkseq_clean_project_file} new-project !{raw,--vcf,projectt,*projectt_clean_vcf_file} !{raw,--vcf,projectt,*projectt_clean_indel_vcf_file} !{raw,--vcf,projectt,*projectt_clean_multiallelic_vcf_file} $projectt_clean_project_helper class_level projectt 

!!expand:,:projectt,projectl:project,project:project_sample_subset,project:project_variant_subset,project_variant_subset! \
local cmd ln_projectt_plinkseq_clean_project_dbs=rm -rf !{raw,,projectt,$projectt_plinkseq_clean_project_out_dir/*} && rm -rf !{raw,,projectt,$projectt_plinkseq_clean_project_temp_dir/*} && rm -f !{output,,projectt_plinkseq_clean_vardb_file} !{output,,projectt_plinkseq_clean_inddb_file} && ln -s !{input,,projectt_plinkseq_vardb_file} !{output,,projectt_plinkseq_clean_vardb_file} && ln -s !{input,,projectt_plinkseq_inddb_file} !{output,,projectt_plinkseq_clean_inddb_file} && touch !{output,,projectt_plinkseq_clean_db_done_file} !{output,,projectt_plinkseq_clean_temp_done_file} !{input,projectl_plinkseq_locdb_file} class_level projectt skip_if or,sample_qc_filter,var_qc_filter,project_variant_custom_exclude_file,project_sample_custom_exclude_file

!|expand:;:projectt;projectl;projecti;projecte;after;shortt:project;project;project;;run_with project_sample_subset,project_variant_subset;:project_sample_subset;project;project_sample_subset;_project_sample_subset;;short:project_variant_subset;project_variant_subset;project;_project_variant_subset;;short| \
shortt cmd make_projectt_plinkseq_clean_project_dbs=rm -rf !{raw,,projectt,$projectt_plinkseq_clean_project_out_dir/*} && rm -rf !{raw,,projectt,$projectt_plinkseq_clean_project_temp_dir/*} && rm -f !{output,,projectt_plinkseq_clean_vardb_file} !{output,,projectt_plinkseq_clean_inddb_file} && $pseq_clean_project_cmdprojecte load-vcf !{input,projectt_clean_vcf_file} !{input,projectt_clean_indel_vcf_file} !{input,projectt_clean_multiallelic_vcf_file} !{output,projectt_plinkseq_clean_vardb_file} && $pseq_clean_project_cmdprojecte tag-file --id 1 --name $pseq_snp_tag && $pseq_clean_project_cmdprojecte tag-file --id 2 --name $pseq_indel_tag && $pseq_clean_project_cmdprojecte tag-file --id 3 --name $pseq_multiallelic_tag && $pseq_clean_project_cmdprojecte load-pedigree !{input,--file,projecti_plinkseq_ind_info_file} !{output,projectt_plinkseq_clean_inddb_file} && touch !{output,,projectt_plinkseq_clean_db_done_file} !{output,,projectt_plinkseq_clean_temp_done_file} !{input,projectl_plinkseq_locdb_file} after class_level projectt run_if or,sample_qc_filter,var_qc_filter,project_variant_custom_exclude_file,project_sample_custom_exclude_file

#local cmd make_plinkseq_samtools_clean_project_file=$pseq_cmd !{output,,project_plinkseq_samtools_clean_project_file} new-project !{raw,--vcf,project,*project_samtools_clean_vcf_file} $plinkseq_resources_helper --scratch !{raw,,project,$project_plinkseq_samtools_clean_project_temp_dir} !{input,--locdb,project_plinkseq_locdb_file} class_level project

#cmd make_plinkseq_samtools_clean_project_dbs=rm -rf !{raw,,project,$project_plinkseq_samtools_clean_project_out_dir/*} && rm -rf !{raw,,project,$project_plinkseq_samtools_clean_project_temp_dir/*} && $pseq_samtools_clean_project_cmd load-vcf !{input,project_samtools_clean_vcf_file} !{output,project_plinkseq_samtools_clean_vardb_file} && $pseq_samtools_clean_project_cmd load-pedigree !{input,--file,project_plinkseq_ind_info_file} && $pseq_samtools_clean_project_cmd load-pheno !{input,--file,project_plinkseq_phe_file} !{output,project_plinkseq_samtools_clean_inddb_file} && touch !{output,,project_plinkseq_samtools_clean_db_done_file} class_level project

prop gq_crit=scalar default GQ:ge:20
gq_crit_helper=!{prop;;project;gq_crit}
gq_mask=--mask geno=$gq_crit_helper

#should ultimately change no_project_cmd to not depend on any project file, but I think right now pseq does not like this
pseq_no_project_cmd=$pseq_cmd . 

!!expand:,:projectt,projecte:project,:project_sample_subset,_project_sample_subset:project_variant_subset,_project_variant_subset! \
pseq_project_cmdprojecte=$pseq_cmd !{raw,,projectt,*projectt_plinkseq_project_file} !{input,projectt_plinkseq_project_done_file}

#!!expand:,:projectt,projecte:project,:project_sample_subset,_project_sample_subset:project_variant_subset,_project_variant_subset! \
#pseq_indel_project_cmdprojecte=$pseq_cmd !{raw,,projectt,*projectt_plinkseq_indel_project_file} !{input,projectt_plinkseq_indel_project_done_file}

!!expand:,:projectt,projecte:project,:project_sample_subset,_project_sample_subset:project_variant_subset,_project_variant_subset! \
pseq_clean_project_cmdprojecte=$pseq_cmd !{raw,,projectt,*projectt_plinkseq_clean_project_file} !{input,projectt_plinkseq_clean_project_done_file}
#pseq_samtools_clean_project_cmd=$pseq_cmd !{input,,project_plinkseq_samtools_clean_project_file}

!!expand:,:projectt,projecte,projectl:project,,project:project_sample_subset,_project_sample_subset,project:project_variant_subset,_project_variant_subset,project_variant_subset! \
pseq_analysis_cmdprojecte=$pseq_project_cmdprojecte !{input,projectt_plinkseq_db_done_file} !{input,projectl_plinkseq_locdb_file}

vcf_pass=PASS

!!expand:projecte::_project_sample_subset:_project_variant_subset! \
pseq_qc_pass_no_gq_mask_analysis_cmdprojecte=$pseq_analysis_cmdprojecte @1 --mask filter=$vcf_pass
!!expand:projecte::_project_sample_subset:_project_variant_subset! \
pseq_qc_pass_analysis_cmdprojecte=$pseq_qc_pass_no_gq_mask_analysis_cmdprojecte(@1) $gq_mask
!!expand:projecte::_project_sample_subset:_project_variant_subset! \
pseq_raw_analysis_cmdprojecte=$pseq_analysis_cmdprojecte @1 $gq_mask
!!expand:projecte::_project_sample_subset:_project_variant_subset! \
pseq_filtered_analysis_cmdprojecte=$pseq_analysis_cmdprojecte @1 --mask filter.ex=$vcf_pass $gq_mask

list_to_mask=tr '\n' ',' | sed 's/,$//'
filter_samples_mask=--mask indiv.ex=@!{input,,project_sample_exclude_file}
!!expand:projecte::_project_sample_subset:_project_variant_subset! \
pseq_filter_samples_analysis_cmdprojecte=$pseq_qc_pass_analysis_cmdprojecte(@1) $filter_samples_mask
!!expand:projecte::_project_sample_subset:_project_variant_subset! \
pseq_filter_only_samples_no_gq_mask_analysis_cmdprojecte=$pseq_analysis_cmdprojecte @1 $filter_samples_mask 
!!expand:projecte::_project_sample_subset:_project_variant_subset! \
pseq_filter_only_samples_analysis_cmdprojecte=$pseq_filter_only_samples_no_gq_mask_analysis_cmdprojecte(@1) $gq_mask
!!expand:,:projectt,projecte,projectl:project,,project:project_sample_subset,_project_sample_subset,project:project_variant_subset,_project_variant_subset,project_variant_subset! \
pseq_qc_plus_no_mask_analysis_cmdprojecte=$pseq_clean_project_cmdprojecte !{input,projectt_plinkseq_clean_db_done_file} !{input,projectl_plinkseq_locdb_file} @1
!!expand:,:projectt,projecte,projectl:project,,project:project_sample_subset,_project_sample_subset,project:project_variant_subset,_project_variant_subset,project_variant_subset! \
pseq_qc_plus_analysis_cmdprojecte=$pseq_qc_plus_no_mask_analysis_cmdprojecte(@1) $gq_mask 

#pseq_filter_variants_analysis_cmd=$pseq_filter_samples_analysis_cmd(@1) --mask var.ex=`tr '\n' ',' < !{input,,project_variant_exclude_file} | sed 's/,$//'`

sqlite_error=^SQLITE\|^Created.\*database:
catch_sqlite_errors=sed 's/\($sqlite_error\)/\#\1/g'
remove_sqlite_errors=perl -ne 'print unless /$sqlite_error/'

!!expand:;:projectt;projecte;skipif;shortt:project;;skip_if or,num_samp_subsets,num_var_subsets;:project_sample_subset;_project_sample_subset;;short:project_variant_subset;_project_variant_subset;;short! \
!!expand:;:qctype;runif:qc_pass;:qc_plus;run_if or,sample_qc_filter,var_qc_filter,project_variant_custom_exclude_file,project_sample_custom_exclude_file! \
shortt cmd make_projectt_plinkseq_qctype_plink_file=rm -f !{output,,projectt_plinkseq_qctype_tfam_file} !{output,,projectt_plinkseq_qctype_tped_file} && $pseq_qctype_analysis_cmdprojecte(write-ped) !{raw,--name,projectt,*projectt_plinkseq_qctype_plink_file} !{output,projectt_plinkseq_qctype_tped_file} !{output,projectt_plinkseq_qctype_tfam_file} class_level projectt skipif runif

!!expand:;:projectt;projecte;skipif;shortt:project;;skip_if num_var_subsets;:project_variant_subset;_project_variant_subset;;short! \
shortt cmd make_projectt_plinkseq_qc_pass_unthresholded_plink_file=rm -f !{output,,projectt_plinkseq_qc_pass_unthresholded_tfam_file} !{output,,projectt_plinkseq_qc_pass_unthresholded_tped_file} && $pseq_qc_pass_no_gq_mask_analysis_cmdprojecte(write-ped) !{raw,--name,projectt,*projectt_plinkseq_qc_pass_unthresholded_plink_file} !{output,projectt_plinkseq_qc_pass_unthresholded_tped_file} !{output,projectt_plinkseq_qc_pass_unthresholded_tfam_file} class_level projectt skipif 

!!expand:;:projectt;projecte;skipif;shortt:project;;skip_if or,num_samp_subsets,num_var_subsets;:project_sample_subset;_project_sample_subset;;short:project_variant_subset;_project_variant_subset;;short! \
local cmd ln_projectt_plinkseq_qc_plus_plink_file=rm -f !{output,,projectt_plinkseq_qc_plus_tfam_file} !{output,,projectt_plinkseq_qc_plus_tped_file} && ln -s !{input,,projectt_plinkseq_qc_pass_tped_file} !{output,,projectt_plinkseq_qc_plus_tped_file} && ln -s !{input,,projectt_plinkseq_qc_pass_tfam_file} !{output,,projectt_plinkseq_qc_plus_tfam_file} class_level projectt skipif run_if and,!sample_qc_filter,!var_qc_filter,!project_variant_custom_exclude_file,!project_sample_custom_exclude_file

#this cannot use FID IID map because the renamed tfam is used to generate the FID IID map
rename_seq_tfam_file=perl -ne 'BEGIN {@files=qw(!{input,,marker_initial_filtered_fam_file,if_prop=marker_initial_fam_file,unless_prop=no_marker,allow_empty=1} !{raw,,project,/dev/null}); foreach \$file (@files) {open IN, \$file or die "Cannot read \$file\n"; while (<IN>) {@a = split; die "Expected two columns" unless scalar @a >= 2; die "Multiple families for ID \$a[1]" if defined \$id2fam{\$a[1]} && \$id2fam{\$a[1]} ne \$a[0]; \$id2fam{\$a[1]} = \$a[0]; \$id2pat{\$a[1]} = \$a[2]; \$id2mat{\$a[1]} = \$a[3]} close IN}} @a = split; \$a[1] = \$a[0] if \$a[1] eq "."; if (exists \$id2fam{\$a[1]}) {\$a[0] = \$id2fam{\$a[1]}; \$a[2] = \$id2pat{\$a[1]}; \$a[3] = \$id2mat{\$a[1]};} print join("\t", @a); print "\n"'

add_sex=perl -ne 'BEGIN {\$file = "!{input,,pheno_non_missing_sample_pheno_file,if_prop=pheno:eq:sex,max=1}"; open IN, \$file or die "Cannot read \$file\n"; while (<IN>) {@a = split; die "Expected two columns" unless scalar @a == 2; die "Multiple sex values for ID \$a[1]" if defined \$id2sex{\$a[0]} && \$id2sex{\$a[0]} ne \$a[1]; \$id2sex{\$a[0]} = \$a[1];} close IN} @a = split; \$a[4] = \$id2sex{\$a[1]} if defined \$id2sex{\$a[1]}; print join("\t", @a); print "\n"'

!!expand:;:project;skipif:project;skip_if or,num_samp_subsets,num_var_subsets:project_sample_subset;:project_variant_subset;! \
!!expand:qctype:qc_pass:qc_plus! \
local cmd make_project_plinkseq_qctype_renamed_tfam_file=$rename_seq_tfam_file < !{input,,project_plinkseq_qctype_tfam_file} > !{output,,project_plinkseq_qctype_renamed_tfam_file} class_level project skipif

!!expand:;:project;skipif:project;skip_if or,num_var_subsets:project_variant_subset;! \
!!expand:qctype:qc_pass_unthresholded! \
local cmd make_project_plinkseq_qctype_renamed_tfam_file=$rename_seq_tfam_file < !{input,,project_plinkseq_qctype_tfam_file} > !{output,,project_plinkseq_qctype_renamed_tfam_file} class_level project skipif

!!expand:;:project;skipif:project;skip_if or,num_samp_subsets,num_var_subsets:project_sample_subset;:project_variant_subset;! \
!!expand:qctype:qc_pass:qc_plus! \
local cmd make_project_plinkseq_qctype_with_sex_tfam_file=cat !{input,,project_plinkseq_qctype_fam_file} | $add_sex > !{output,,project_plinkseq_qctype_with_sex_fam_file} class_level project skipif run_if pheno;pheno:eq:sex

!!expand:qctype:qc_pass:qc_pass_unthresholded:qc_plus! \
cmd cat_project_plinkseq_qctype_renamed_tfam_file=n=`paste -d@ !{input,,project_variant_subset_plinkseq_qctype_renamed_tfam_file,sort_prop=project_variant_subset} | sed "s/\s\s*//g" | sed "s/@/ /g" | $transpose_cmd | sort | uniq | wc -l` && if [[ \$n != 1 ]]; then perl -e 'die "Tfam files did not math at variant subset level\n"'; fi && cp !{input,,project_variant_subset_plinkseq_qctype_renamed_tfam_file,limit=1} !{output,,project_plinkseq_qctype_renamed_tfam_file} class_level project run_if num_var_subsets

!|expand:;:qctype;skipif:qc_pass;:qc_pass_unthresholded;:qc_plus;skip_if and,!sample_qc_filter,!var_qc_filter,!project_variant_custom_exclude_file,!project_sample_custom_exclude_file| \
cmd cat_project_plinkseq_qctype_tped_file=rm -f !{output,,project_plinkseq_qctype_tped_file} && cat !{input,,project_variant_subset_plinkseq_qctype_tped_file,sort_prop=project_variant_subset} > !{output,,project_plinkseq_qctype_tped_file} class_level project run_if num_var_subsets

!!expand:project:project:project_sample_subset:project_variant_subset! \
!!expand:qctype:qc_pass:qc_pass_unthresholded:qc_plus! \
project_plinkseq_qctype_plink_bfile_helper=rm -f !{output,,project_plinkseq_qctype_bed_file} !{output,,project_plinkseq_qctype_fam_file} !{output,,project_plinkseq_qctype_bim_file} && $plink_cmd !{input,--tped,project_plinkseq_qctype_tped_file} !{input,--tfam,project_plinkseq_qctype_renamed_tfam_file} --make-bed !{raw,--out,project,*project_plinkseq_qctype_plink_file} !{output,project_plinkseq_qctype_bed_file} !{output,project_plinkseq_qctype_fam_file} !{output,project_plinkseq_qctype_bim_file} && $plink_mv_log_cmd(!{raw;;project;*project_plinkseq_qctype_plink_file},!{output;;project_plinkseq_qctype_make_bed_log_file;optional=1})

!|expand:;:project;skipif;shortt;rusagemod:project;skip_if and,num_samp_subsets,!num_var_subsets;;rusage_mod $project_plinkseq_bfile_mem:project_sample_subset;skip_if fast_split;short;rusage_mod $project_sample_subset_plinkseq_bfile_mem:project_variant_subset;skip_if fast_split;short;| \
!!expand:;:qctype;exrunif:qc_pass;:qc_plus;run_if and,(or,sample_qc_filter,var_qc_filter,project_variant_custom_exclude_file,project_sample_custom_exclude_file)! \
shortt cmd make_project_plinkseq_qctype_plink_bfile=$project_plinkseq_qctype_plink_bfile_helper class_level project skipif exrunif rusagemod

!|expand:;:project;skipif;shortt;rusagemod:project;skip_if !num_var_subsets;;rusage_mod $project_plinkseq_bfile_mem:project_variant_subset;skip_if fast_split;short;| \
!!expand:;:qctype;exrunif:qc_pass_unthresholded;! \
shortt cmd make_project_plinkseq_qctype_plink_bfile=$project_plinkseq_qctype_plink_bfile_helper class_level project skipif exrunif rusagemod

!|expand:;:projectt;skipif;shortt:project_sample_subset;;short:project_variant_subset;;short| \
!!expand:;:qctype;exrunif:qc_pass;:qc_plus;run_if and,(or,sample_qc_filter,var_qc_filter,project_variant_custom_exclude_file,project_sample_custom_exclude_file)! \
shortt cmd make_fast_projectt_plinkseq_qctype_plink_bfile=$tped_to_bed_cmd !{output,--write-bim,projectt_plinkseq_qctype_bim_file} !{input,,projectt_plinkseq_qctype_tped_file} > !{output,,projectt_plinkseq_qctype_bed_file} class_level projectt skip_if !fast_split exrunif

!|expand:;:projectt;skipif;shortt:project_variant_subset;;short| \
!!expand:;:qctype;exrunif:qc_pass_unthresholded;! \
shortt cmd make_fast_projectt_plinkseq_qctype_plink_bfile=$tped_to_bed_cmd !{output,--write-bim,projectt_plinkseq_qctype_bim_file} !{input,,projectt_plinkseq_qctype_tped_file} > !{output,,projectt_plinkseq_qctype_bed_file} class_level projectt skip_if !fast_split exrunif

!|expand:;:projectt;skipif;shortt:project_sample_subset;;short:project_variant_subset;;short| \
!!expand:;:qctype;exrunif:qc_pass;:qc_plus;run_if and,(or,sample_qc_filter,var_qc_filter,project_variant_custom_exclude_file,project_sample_custom_exclude_file)! \
local cmd make_fast_projectt_plinkseq_qctype_plink_bim_file=cp !{input,,projectt_plinkseq_qctype_renamed_tfam_file} !{output,,projectt_plinkseq_qctype_fam_file} class_level projectt skip_if !fast_split exrunif

!|expand:;:projectt;skipif;shortt:project_variant_subset;;short| \
!!expand:;:qctype;exrunif:qc_pass_unthresholded;! \
local cmd make_fast_projectt_plinkseq_qctype_plink_bim_file=cp !{input,,projectt_plinkseq_qctype_renamed_tfam_file} !{output,,projectt_plinkseq_qctype_fam_file} class_level projectt skip_if !fast_split exrunif

!|expand:;:projectt;skipif;shortt:project;skip_if and,num_samp_subsets,!num_var_subsets;:project_sample_subset;;short:project_variant_subset;;short| \
local cmd ln_projectt_plinkseq_qc_plus_plink_bfile=rm -f !{output,,projectt_plinkseq_qc_plus_bed_file} !{output,,projectt_plinkseq_qc_plus_fam_file} !{output,,projectt_plinkseq_qc_plus_bim_file} && ln -s !{input,,projectt_plinkseq_qc_pass_bed_file} !{output,,projectt_plinkseq_qc_plus_bed_file} && ln -s !{input,,projectt_plinkseq_qc_pass_fam_file} !{output,,projectt_plinkseq_qc_plus_fam_file} && ln -s !{input,,projectt_plinkseq_qc_pass_bim_file} !{output,,projectt_plinkseq_qc_plus_bim_file} class_level projectt skipif run_if and,!sample_qc_filter,!var_qc_filter,!project_variant_custom_exclude_file,!project_sample_custom_exclude_file

!!expand:qctype:qc_pass:qc_plus! \
meta_table cmd make_project_qctype_plink_list_file=!{input,--bed,project_sample_subset_plinkseq_qctype_bed_file}\t!{input,--bim,project_sample_subset_plinkseq_qctype_bim_file}\t!{input,--fam,project_sample_subset_plinkseq_qctype_fam_file} !{output,project_qctype_plink_list_file} class_level project run_if and,num_samp_subsets,!num_var_subsets

!!expand:qctype:qc_pass:qc_plus! \
meta_table cmd make_project_qctype_all_merge_list_file=!{input,,project_sample_subset_plinkseq_qctype_bed_file}\t!{input,,project_sample_subset_plinkseq_qctype_bim_file}\t!{input,,project_sample_subset_plinkseq_qctype_fam_file} !{output,project_qctype_all_merge_list_file} class_level project run_if and,num_samp_subsets,!num_var_subsets

!!expand:qctype:qc_pass:qc_plus! \
local cmd make_project_qctype_merge_list_file=tail -n+2 !{input,,project_qctype_all_merge_list_file} > !{output,,project_qctype_merge_list_file} class_level project run_if and,num_samp_subsets,!num_var_subsets

!|expand:;:qctype;skipif:qc_pass;:qc_plus;skip_if and,sample_qc_filter,var_qc_filter,project_variant_custom_exclude_file,project_sample_custom_exclude_file| \
cmd make_project_qctype_plink_file=rm -f !{output,,project_plinkseq_qctype_bed_file} !{output,,project_plinkseq_qctype_bim_file} !{output,,project_plinkseq_qctype_fam_file} && $plink_cmd `head -n1 !{input,,project_qctype_plink_list_file}` !{input,--merge-list,project_qctype_merge_list_file} !{input,project_sample_subset_plinkseq_qctype_bed_file} !{input,project_sample_subset_plinkseq_qctype_bim_file} !{input,project_sample_subset_plinkseq_qctype_fam_file} --make-bed $plink_out_bed_helper(project_plinkseq_qctype) && $plink_mv_log_cmd(!{raw\,\,project\,*project_plinkseq_qctype_plink_file},!{output\,\,project_plinkseq_qctype_make_bed_log_file}) class_level project run_if and,num_samp_subsets,!num_var_subsets rusage_mod $project_plink_mem

!!expand:qctype:qc_pass:qc_plus! \
!!expand:,:projectt,projecte:project,:project_sample_subset,_project_sample_subset:project_variant_subset,_project_variant_subset! \
plink_qctype_bed_cmdprojecte=$plink_cmd !{raw,--bfile,projectt,*projectt_plinkseq_qctype_plink_file} !{input,projectt_plinkseq_qctype_bed_file} !{input,projectt_plinkseq_qctype_bim_file} !{input,projectt_plinkseq_qctype_fam_file}

!!expand:qctype:qc_pass_unthresholded! \
!!expand:,:projectt,projecte:project,:project_variant_subset,_project_variant_subset! \
plink_qctype_bed_cmdprojecte=$plink_cmd !{raw,--bfile,projectt,*projectt_plinkseq_qctype_plink_file} !{input,projectt_plinkseq_qctype_bed_file} !{input,projectt_plinkseq_qctype_bim_file} !{input,projectt_plinkseq_qctype_fam_file}

!!expand:,:phenol:pheno:pheno_variant_subset! \
plink_phenol_bed_cmd=$plink_cmd !{raw,--bfile,phenol,*phenol_plink_file} !{input,phenol_bed_file} !{input,phenol_bim_file} !{input,phenol_fam_file}

plink_analysis_helper_template=!{raw,--out,@5,*@1} --@2 !{output,@4} && mv !{raw,,@5,*@{1}.@3} !{output,,@1}

!!expand:,:projectt,projecte:project,:project_sample_subset,_project_sample_subset:project_variant_subset,_project_variant_subset! \
plink_analysis_helperprojecte=$plink_analysis_helper_template(@1,@2,@3,@4,projectt)

#args are:
# 1. The output file
# 2. The analysis
"""
}
    