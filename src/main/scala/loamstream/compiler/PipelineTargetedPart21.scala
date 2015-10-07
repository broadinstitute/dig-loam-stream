
package loamstream.compiler

/**
 * LoamStream
 * Created by oliverr on 10/6/2015.
 */
object PipelineTargetedPart21 {
  val string =
 """local cmd make_pheno_epacts_type_ped_file=$epacts_ped_helper(basic_type,) > !{output,,pheno_epacts_type_ped_file} class_level pheno skip_if not_trait

!!expand:type:all:unrelated! \
local cmd make_pheno_epacts_popgen_type_ped_file=$epacts_ped_helper(popgen_type,) > !{output,,pheno_epacts_popgen_type_ped_file} class_level pheno skip_if not_trait

!!expand:type:all:unrelated! \
local cmd make_pheno_epacts_covar_type_ped_file=$epacts_ped_helper(covar_type,!{input:--file:pheno_plink_covar_file}) > !{output,,pheno_epacts_covar_type_ped_file} class_level pheno skip_if not_trait

local cmd make_pheno_plink_match_file=$sample_list_to_plink_sample_list(--exec "grep -v \\\#\\\# !{input\,\,pheno_plinkseq_strata_phe_file} | sed '1 s/^\\#//' | $smart_cut_cmd --in-delim $tab --vec-delim : --select-col 0\,1 --select-col 0\,1\,'!{prop\,\,pheno\,strata_traits\,sep=:}'",1) > !{output,,pheno_plink_match_file} class_level pheno run_if strata_traits

ibs_cluster_pheno=ibs_cluster
ibs_cluster_missing=$plink_missing
ibs_cluster_height=0
local cmd make_pheno_plinkseq_cluster_phe_file=$smart_join_cmd !{input,--file,project_sample_include_file} --exec "$smart_join_cmd --exec \"awk 'NF > 1 {f=NF-$ibs_cluster_height; print \\\\$2,\\\\$f}' < !{input,,pheno_sample_marker_cluster2_file}\" --exec \"sed 's/$/ $ibs_cluster_missing/' !{input,,project_sample_include_file}\" --merge" --extra 2 --out-delim $tab | sed '1 s/^/\#\#$ibs_cluster_pheno,Integer,$ibs_cluster_missing,"IBS cluster"\n\#ID\t$ibs_cluster_pheno\n/' > !{output,,pheno_plinkseq_cluster_phe_file} class_level pheno

subset_mds_file=sed 's/^\s*//' !{input,,pheno_mds_file} | sed 's/\s\s*/\t/g' | cut -f2,4-

local cmd make_pheno_plinkseq_covar_phe_file=$smart_join_cmd --out-delim $tab --exec "$subset_mds_file | tail -n+2" --exec "cat !{input,,project_sample_include_file} | perl -ne 'chomp; print; print \" \"; print join(\" \", map {$ibs_cluster_missing} (1..!{prop,,pheno,num_mds_calc})); print \"\n\"'" --merge | $smart_cut_cmd --exec "$subset_mds_file" --out-delim $tab --select-row 1,1 | sed '1 s/^\S\S*/\#ID/' | perl -lpe 'print join("\n", map {"\#\#C\$_,Float,$ibs_cluster_missing,\"MDS value \$_\""} (1..!{prop,,pheno,num_mds_calc})) unless \$i++' > !{output,,pheno_plinkseq_covar_phe_file} class_level pheno

convert_pheno_plink_phe=cut -f2- !{input,,pheno_plink_phe_file} !{raw,,pheno,| sed 's/$case_pheno_helper_int(\)$/1temp/' | sed 's/$control_pheno_helper_int(\)$/0/' | sed 's/temp$//' | sed '1 s/^/ID\t@pheno\n/',unless_prop=pheno_qt,allow_empty=1}

#local cmd make_pheno_score_seq_phe_covar_file=c=$get_covar_traits(:)`$get_mds_cols(!{prop\\,\\,pheno\\,num_mds_covar},:)` && $smart_join_cmd --in-delim $tab --header 1 --rest-extra 1 --exec "sed '1 s/^/ID\n/' !{input,,pheno_sample_covar_include_file}" --exec "$convert_pheno_plink_phe" --exec "cut -f2- !{input,,pheno_plink_covar_file}" | $smart_cut_cmd --in-delim $tab --select-col 0,1 --select-col 0,1,!{prop,,pheno}:\$c --vec-delim : --require-col-match --exact | tail -n+2 > !{output,,pheno_score_seq_phe_covar_file} class_level pheno run_if or,strata_traits,strat_covar skip_if not_trait

#local cmd make_pheno_score_seq_phe_file=$smart_join_cmd --in-delim $tab --header 1 --rest-extra 1 --exec "sed '1 s/^/ID\n/' !{input,,pheno_sample_popgen_include_file}" --exec "$convert_pheno_plink_phe" | tail -n+2 > !{output,,pheno_score_seq_phe_file} class_level pheno skip_if not_trait

#!!expand:phetype:phe:phe_covar! \
#local cmd make_pheno_score_seq_reverse_phetype_file=awk -F"\t" -v OFS="\t" '{\$2 = 1 - \$2} {print}' !{input,,pheno_score_seq_phetype_file} > !{output,,pheno_score_seq_reverse_phetype_file} class_level pheno skip_if pheno_qt

#!!expand:filetype:covar:cluster! \
#local cmd make_pheno_filetype_exclude_file=sort !{input,,pheno_sample_popgen_all_include_file} !{input,,pheno_sample_filetype_all_include_file} !{input,,pheno_sample_filetype_all_include_file} | uniq -u > !{output,,pheno_filetype_exclude_file} class_level pheno

#ISSUE right now is that PSEQ seems to prepend chr in front of every variant
#To get around this, we assume that the variants either all have chr or none do

!!expand:,:shortt,phenol,projectl,extrarunif:,pheno,project,:short,pheno_variant_subset,project_variant_subset,run_if num_var_subsets! \
local cmd make_phenol_plinkseq_project_file=$pseq_cmd !{output,,phenol_plinkseq_project_file} new-project !{raw,--vardb,projectl,*projectl_plinkseq_vardb_file} $plinkseq_resources_helper --scratch !{raw,,phenol,$phenol_plinkseq_project_temp_dir} !{raw,--locdb,projectl,*projectl_plinkseq_locdb_file} && touch !{output,,phenol_plinkseq_project_done_file} class_level phenol

#DID THIS BREAK?
#removed loading of this file
#!{input,,pheno_plinkseq_strata_phe_file,if_prop=strata_traits,if_prop=covar_traits,if_prop=extra_traits,or_if_prop=1,allow_empty=1}

!!expand:,:shortt,phenol,projectl,extrarunif:,pheno,project,:short,pheno_variant_subset,project_variant_subset,run_if num_var_subsets! \
shortt cmd make_phenol_plinkseq_project_dbs=rm -rf !{raw,,phenol,$phenol_plinkseq_project_out_dir/*} && rm -rf !{raw,,phenol,$phenol_plinkseq_project_temp_dir/*} && $pseq_phenol_project_cmd load-pedigree !{input,--file,project_plinkseq_ind_info_file} !{output,phenol_plinkseq_inddb_file} && touch !{output,,phenol_plinkseq_db_done_file} !{output,,phenol_plinkseq_temp_done_file} && $pseq_phenol_project_cmd load-pheno --file !{input,,pheno_plinkseq_phe_file} !{input,projectl_plinkseq_db_done_file} class_level phenol run_with pheno_variant_subset

!!expand:,:phenol,projectl,extrarunif:pheno,project,:pheno_variant_subset,project_variant_subset,run_if num_var_subsets! \
local cmd make_phenol_plinkseq_clean_project_file=$pseq_cmd !{output,,phenol_plinkseq_clean_project_file} new-project !{raw,--vardb,projectl,*projectl_plinkseq_clean_vardb_file} $plinkseq_resources_helper --scratch !{raw,,phenol,$phenol_plinkseq_clean_project_temp_dir} !{raw,--locdb,projectl,*projectl_plinkseq_locdb_file} && touch !{output,,phenol_plinkseq_clean_project_done_file} class_level phenol extrarunif

#DID THIS BREAK?
#removed loading of this file
#!{input,,pheno_plinkseq_strata_phe_file,if_prop=strata_traits,if_prop=covar_traits,if_prop=extra_traits,or_if_prop=1,allow_empty=1}

!!expand:,:shortt,phenol,projectl,extrarunif:,pheno,project,:short,pheno_variant_subset,project_variant_subset,run_if num_var_subsets! \
shortt cmd make_phenol_plinkseq_clean_project_dbs=rm -rf !{raw,,phenol,$phenol_plinkseq_clean_project_out_dir/*} && rm -rf !{raw,,phenol,$phenol_plinkseq_clean_project_temp_dir/*} && $pseq_phenol_clean_project_cmd load-pedigree !{input,--file,project_plinkseq_ind_info_file} !{output,phenol_plinkseq_clean_inddb_file} && touch !{output,,phenol_plinkseq_clean_db_done_file} !{output,,phenol_plinkseq_clean_temp_done_file} && $pseq_phenol_clean_project_cmd load-pheno --file !{input,,pheno_plinkseq_phe_file} !{input,projectl_plinkseq_clean_db_done_file} class_level phenol extrarunif

!!expand:,:phenol,projectl,extrarunif:pheno,project,:pheno_variant_subset,project_variant_subset,run_if num_var_subsets! \
local cmd make_phenol_plinkseq_strat_project_file=$pseq_cmd !{output,,phenol_plinkseq_strat_project_file} new-project !{raw,--vardb,projectl,*projectl_plinkseq_clean_vardb_file} $plinkseq_resources_helper --scratch !{raw,,phenol,$phenol_plinkseq_strat_project_temp_dir} !{raw,--locdb,projectl,*projectl_plinkseq_locdb_file} && touch !{output,,phenol_plinkseq_strat_project_done_file} class_level phenol extrarunif

!|expand:,:shortt,phenol,projectl,extrarunif:,pheno,project,:short,pheno_variant_subset,project_variant_subset,skip_if !num_var_subsets bsub_batch 50 | \
shortt cmd make_phenol_plinkseq_strat_project_dbs=rm -rf !{raw,,phenol,$phenol_plinkseq_strat_project_out_dir/*} && rm -rf !{raw,,phenol,$phenol_plinkseq_strat_project_temp_dir/*} && $pseq_phenol_strat_project_cmd load-pedigree !{input,--file,project_plinkseq_ind_info_file} !{output,phenol_plinkseq_strat_inddb_file} && touch !{output,,phenol_plinkseq_strat_db_done_file} !{output,,phenol_plinkseq_strat_temp_done_file} \
  && $pseq_phenol_strat_project_cmd load-pheno --file !{input,,pheno_plinkseq_phe_file} !{input,,pheno_plinkseq_strata_phe_file,if_prop=strata_traits,if_prop=covar_traits,if_prop=extra_traits,or_if_prop=1,allow_empty=1} !{input,,pheno_plinkseq_covar_phe_file} !{input,,pheno_plinkseq_cluster_phe_file} \
  !{input,pheno_is_trait_file} !{input,phenol_plinkseq_clean_db_done_file} class_level phenol

marker_nn_helper=$smart_cut_cmd !{input,--file,@2} --exact --select-col 1,1,IID --select-col 1,1,Z --select-row 1,1 --select-row 1,1,NN,@1 --out-delim $tab | sed '1 s/^.*/IID\tZ_IBS_NN@1/'

min_related_outlier=.05

custom_exclude_header=CUST_EX

!|expand:;:type;toexclude;skipif:\
all;!{input\,\,project_sample_exclude_file} !{input\,\,pheno_missing_sample_info_file};skip_if not_trait:\
unrelated;!{input\,\,project_sample_exclude_file} !{input\,\,pheno_missing_sample_info_file} !{input\,\,pheno_popgen_exclude_file} !{input\,\,pheno_basic_related_exclude_file};skip_if not_trait| \
cmd make_pheno_sample_marker_type_nearest_file=$sample_list_to_plink_sample_list(--exec "cat toexclude | cut -f1  | sort -u",0) | $plink_marker_helper !{input,--read-genome,pheno_genome_file} --remove /dev/stdin --neighbour $min_neighbor !{prop,,pheno,max_neighbor} !{raw,--out,pheno,*pheno_sample_marker_type_nearest_trunk} !{output,pheno_sample_marker_type_nearest_file} && $plink_mv_log_cmd(!{raw\,\,pheno\,*pheno_sample_marker_type_nearest_trunk},!{output\,\,pheno_sample_marker_type_nearest_log_file}) class_level pheno skipif

add_indicator_helper=$smart_join_cmd --exec \"cat !{input,,@1} !{input,,@2} | sort | uniq -d | sed 's/$/\t1/'\" --exec \"sed 's/$/\t0/' !{input,,@1}\" --merge | sed '1 s/^/ID\t@3\n/'

!@expand:;:phenol;shortt;extrainclude;runif:pheno;;;run_if or,!num_samp_subsets,!parallelize_genome:pheno_sample_subset;short;| cat - !{input,,project_sample_subset_clean_samp_keep_file} | sort | uniq -d;run_if and,num_samp_subsets,parallelize_genome@ \
!+expand@;@typekey;typefile;toinclude\
@all;pheno_sample_marker_all_nearest_file;!{input::project_sample_include_file}\
@unrelated;pheno_sample_marker_unrelated_nearest_file;!{input::pheno_sample_popgen_unrelated_include_file}\
+ \
shortt cmd make_phenol_typekey_popgen_istats_file=$smart_join_cmd \ 
  --exec "$marker_nn_helper(1,typefile) | cat - !{input,,pheno_sample_basic_all_include_file} | cut -f1 | sort | uniq -d extrainclude | sed '1 s/^/ID\n/'" \
	--exec "$marker_nn_helper(1,typefile)" \
	--exec "$marker_nn_helper(2,typefile)" \
	--exec "$marker_nn_helper(3,typefile)" \
	--exec "$marker_nn_helper(4,typefile)" \
	--exec "$marker_nn_helper(5,typefile)" \
  --exec "$smart_cut_cmd !{input,--file,typefile} --exact --select-col 1,1,IID --select-col 1,1,PROP_DIFF --select-row 1,1 --select-row 1,1,NN,!{prop,,pheno,max_neighbor}" \
	--exec "$fill_file_helper(pheno_project_mds_file,pheno_sample_basic_all_include_file,NA,1,1,1,1) | cut -f1,4-" \
  --exec "$smart_cut_cmd --out-delim $tab --exec \"cat toinclude !{input,,pheno_sample_basic_typekey_include_file} | sort | uniq -d | $filter_sample_genome_file(-,phenol_genome_file)\" --exclude-row 1,1 --select-col 1,1,IID1 --select-col 1,1,IID2 --select-row 1,1,PI_HAT,ge:$min_related_outlier | sed 's/\t/\n/' | sort | uniq -c | $smart_cut_cmd --select-col 0,'1 2' --out-delim $tab | $smart_cut_cmd --stdin-first --in-delim $tab --exec \"sed 's/^/0\t/' !{input,,project_sample_include_file}\" | sort -s -k2,2 | uniq -f1 | $smart_cut_cmd --in-delim $tab --select-col 0,'2 1' | sed '1 s/^/ID\tNUM_CLOSE\n/'" \
  --exec "$smart_cut_cmd --exec \"cat toinclude !{input,,pheno_sample_basic_typekey_include_file} | sort | uniq -d | $filter_sample_genome_file(-,phenol_genome_file)\" --exec \"cat toinclude !{input,,pheno_sample_basic_typekey_include_file} | sort | uniq -d | $filter_sample_genome_file(-,phenol_genome_file)\" --select-col 1,1,IID1 --select-col 1,1,PI_HAT --exclude-row 2,1 --select-col 2,1,IID2 --select-col 2,1,PI_HAT | $table_sum_stats_cmd --out-delim $tab --group-col IID1 --col PI_HAT --summaries --has-header --print-header | $smart_cut_cmd --select-col 0,1 --select-col 0,1,PI_HAT_max --require-col-match | tail -n+2 | sed '1 s/^/ID\tMAX_PI_HAT\n/'" \
	--exec "$smart_cut_cmd --in-delim , --out-delim $tab !{input,--file,project_sstats_file} --select-col 1,1,'ID SEX_CHECK'" \
  --exec "$add_indicator_helper(pheno_sample_basic_typekey_include_file,pheno_mds_exclude_file,MDS_EXCLUDE)" \
  --exec "$add_indicator_helper(pheno_sample_basic_typekey_include_file,pheno_additional_popgen_exclude_file\,if_prop=pheno_additional_popgen_exclude_file\,allow_empty=1,$custom_exclude_header)" \
  --out-delim , --header 1 --rest-extra 1 | sed '1 s/^[^,]*,/ID,/' > !{output,,phenol_typekey_popgen_istats_file} class_level phenol skip_if not_trait runif

local cmd make_pheno_popgen_outlier_file=$write_outlier_table_cmd !{input,,pheno_all_popgen_istats_file} !{output,,pheno_popgen_outlier_file} -ID ID sep=, out.sep=, class_level pheno skip_if not_trait

popgen_highlight_columns=$sample_highlight_columns
popgen_highlight_info=id.col=1 highlight.list.file=!{input,,pheno_popgen_exclude_detail_file} $popgen_highlight_columns
popgen_highlight_info=id.col=1 highlight.list.file=!{input,,pheno_popgen_exclude_detail_file} $popgen_highlight_columns

popgen_pdf_helper=$smart_join_cmd --exec "$get_non_missing_with_header" !{input,--file,pheno_@{3}_popgen_istats_file} --arg-delim : --in-delim 1:$tab --in-delim 2:$coverage_dat_delim --header 1 --out-delim $coverage_dat_delim --extra 1 | $draw_box_plot_cmd(/dev/stdin !{output\,\,@1} '!{prop\,\,pheno\,disp} QC+ Sample Popgen Information@4' '' 'Sample Values' -1\,$display_col_name\,$order_col_name label=$display_col_name order=$order_col_name sep=\, @2)

!!expand:,:type,options,which_marker_istats,extra_title\
:all,,all,\
:highlighted,$popgen_highlight_info,all,\
:final,$popgen_highlight_info exclude.highlighted=TRUE,all, -- after popgen filters; IBD not recomputed\
:unrelated,,unrelated, -- unrelated samples; IBD recomputed\
! local cmd make_pheno_popgen_type_pdf_file=$popgen_pdf_helper(pheno_popgen_type_pdf_file, options,which_marker_istats,extra_title) class_level pheno skip_if not_trait

exclude_popgen_outlier=$exclude_outlier_raw(pheno_popgen_outlier_file,@1,@2,@3,\,)

local cmd make_pheno_popgen_exclude_detail_file=\ 
	$exclude_popgen_outlier($custom_exclude_header,VALUE,eq:1) \
	!{raw,|,pheno_qc_filter,cat - *pheno_qc_filter_exclude_file,if_prop=pheno_qc_filter,allow_empty=1} !{input,pheno_qc_filter_exclude_file,if_prop=pheno_qc_filter,allow_empty=1} \
	| cut -d, -f1-2 | sort -u | $smart_cut_cmd --file !{input,,pheno_popgen_outlier_file} --select-row 1,1 --select-col 1,1-2 --in-delim , > !{output,,pheno_popgen_exclude_detail_file} class_level pheno skip_if not_trait

prop prune_all_related=scalar
prop prune_by_call_rate=scalar default 1

local cmd make_pheno_related_exclude_rank_file=$smart_join_cmd --out-delim $tab !{input,--file,project_passed_sample_list_file} --rest-extra 1 !{input,--file,pheno_related_exclude_custom_rank_file,if_prop=pheno_related_exclude_custom_rank_file,allow_empty=1} !{raw,,pheno,--fill 2 --fill-value NA,if_prop=pheno_related_exclude_custom_rank_file,allow_empty=1} --exec "$smart_join_cmd --exec \"sed 's/$/\t1/' !{input,,pheno_sample_popgen_all_include_file}\" --exec \"sed 's/$/\t0/' !{input,,project_passed_sample_list_file}\" --merge" !{raw;;pheno;--exec "$smart_cut_cmd --in-delim , --out-delim $tab --file *project_sstats_file --select-col 1,1,'ID RATE' --exclude-row 1,1";if_prop=prune_by_call_rate;allow_empty=1} !{input;project_sstats_file;if_prop=prune_by_call_rate;allow_empty=1} > !{output,,pheno_related_exclude_rank_file} class_level pheno skip_if and,pheno_related_exclude_custom_rank_file,!prune_by_call_rate 

short cmd make_pheno_basic_related_exclude_file=\ 
        $filter_sample_genome_file(!{input::pheno_sample_basic_all_include_file} !{raw::pheno:| cat - *pheno_custom_related_exclude_file *pheno_custom_related_exclude_file | sort | uniq -u:if_prop=pheno_custom_related_exclude_file:allow_empty=1},pheno_genome_file) | perl $targeted_bin_dir/prune_most_ibd.pl !{input,--rank-file,pheno_related_exclude_rank_file} !{prop,,pheno,max_related} !{raw::pheno:| cat - *pheno_custom_related_exclude_file | sort -u:if_prop=pheno_custom_related_exclude_file:allow_empty=1} !{input:pheno_custom_related_exclude_file:if_prop=pheno_custom_related_exclude_file:allow_empty=1} > !{output,,pheno_basic_related_exclude_file} class_level pheno skip_if not_trait run_if !prune_all_related

#!@expand:,:type,extrainc,extraex:\
#popgen,pheno_sample_non_missing_unrelated_include_file,pheno_non_missing_related_exclude_file:\
#covar,pheno_sample_popgen_unrelated_include_file,pheno_popgen_related_exclude_file:\
#cluster,pheno_sample_popgen_unrelated_include_file,pheno_popgen_related_exclude_file@\
#short cmd make_pheno_type_related_exclude_file=\ 
#        $filter_sample_genome_file(!{input::pheno_sample_type_all_include_file} !{input::extrainc} | sort | uniq -d !{raw::pheno:| cat - *pheno_custom_related_exclude_file *pheno_custom_related_exclude_file | sort -u:if_prop=pheno_custom_related_exclude_file:allow_empty=1},pheno_genome_file) | perl $targeted_bin_dir/prune_most_ibd.pl !{input,--rank-file,pheno_related_exclude_rank_file,if_prop=pheno_related_exclude_rank_file,allow_empty=1} !{prop,,pheno,max_related} | $smart_cut_cmd !{input,--file,extraex} | sort -u !{raw::pheno:| cat - *pheno_custom_related_exclude_file | sort -u:if_prop=pheno_custom_related_exclude_file:allow_empty=1} !{input:pheno_custom_related_exclude_file:if_prop=pheno_custom_related_exclude_file:allow_empty=1} > !{output,,pheno_type_related_exclude_file} class_level pheno skip_if not_trait run_if !prune_all_related

#!!expand:,:type:non_missing:popgen:covar:cluster! \
#local cmd make_all_pheno_type_related_exclude_file=\ 
#        $filter_sample_genome_file(!{input::pheno_sample_type_all_include_file},pheno_genome_file) | perl $targeted_bin_dir/prune_most_ibd.pl !{input,--rank-file,pheno_related_exclude_rank_file,if_prop=pheno_related_exclude_rank_file,allow_empty=1} !{prop,,pheno,max_related} > !{output,,pheno_type_related_exclude_file} class_level pheno skip_if not_trait run_if !prune_all_related

filter_sample_genome_file=cat @1 | cut -d, -f1 | perl $targeted_bin_dir/filter_genome_file.pl !{input,,@2}

min_notable_pi_hat=.1

!!expand:,:_type:_all:_unrelated! \
short cmd make_pheno_type_related_dat_file=$filter_sample_genome_file(!{input::pheno_sample_basic_type_include_file},pheno_genome_file) | $smart_cut_cmd --select-col 0,1,IID1 --select-row 0,1 --select-col 0,1,PI_HAT --select-row 0,1,PI_HAT,gt:$min_notable_pi_hat > !{output,,pheno_type_related_dat_file} class_level pheno skip_if not_trait bsub_batch 5

!!expand:,:_type:_all:_unrelated! \
local cmd make_pheno_type_related_pdf_file=$draw_hist_plot_cmd("!{input,,pheno_type_related_dat_file} !{output,,pheno_type_related_pdf_file} PI_HAT 'Relatedness among QC+ !{prop,,pheno,disp} samples: PI_HAT > $min_notable_pi_hat' 'PI_HAT' x.min=$min_notable_pi_hat") class_level pheno skip_if not_trait

local cmd make_pheno_mds_exclude_file=eval `echo "!{prop,,pheno,mds_exclude_filters,missing_key=default_mds_exclude_filters,sep=;}" | sed 's/;/\n/g' | sed 's/\(\S\S*\)/ --select-row 1,1,\1/g' | sed 's;^;$smart_cut_cmd --tab-delim  --and-row-all --exclude-row 1,1 --exact !{input,--file,pheno_project_mds_file};' | sed '1! s/^/\| /' | tr '\n' ' '` | cut -f1 | $smart_cut_cmd --exec "tail -qn+2 !{input,,pheno_project_mds_file} !{input,,pheno_project_mds_file} | cut -f1 | cat - !{input,,pheno_sample_non_missing_all_include_file} | sort | uniq -u" | sort -u  > !{output,,pheno_mds_exclude_file} class_level pheno skip_if not_trait

local cmd make_pheno_basic_exclude_file=cat !{input,,pheno_mds_exclude_file} !{input,,project_duplicate_exclude_file} | sort -u > !{output,,pheno_basic_exclude_file} class_level pheno skip_if not_trait with project

local cmd make_empty_pheno_basic_exclude_file=echo > !{output,,pheno_basic_exclude_file} class_level pheno run_if not_trait with project

local cmd make_pheno_popgen_exclude_file=$detail_to_exclude("!{input,,pheno_popgen_exclude_detail_file}") > !{output,,pheno_popgen_exclude_file} class_level pheno skip_if not_trait

local cmd make_pheno_sample_non_missing_all_include_file=cut -f1 !{input,,pheno_non_missing_sample_info_file} | sort - !{input,,project_sample_include_file} | uniq -d > !{output,,pheno_sample_non_missing_all_include_file} class_level pheno with project

local cmd make_pheno_sample_basic_all_include_file=sort !{input,,pheno_sample_non_missing_all_include_file} !{input,,pheno_basic_exclude_file} !{input,,pheno_basic_exclude_file} | uniq -u > !{output,,pheno_sample_basic_all_include_file} class_level pheno with project

local cmd make_pheno_sample_popgen_all_include_file=sort !{input,,pheno_sample_basic_all_include_file} !{input,,pheno_popgen_exclude_file} !{input,,pheno_popgen_exclude_file} | uniq -u > !{output,,pheno_sample_popgen_all_include_file} class_level pheno

!!expand:filetype:covar:cluster!\
filetype_phe_file_helper_no_remove=grep -v \\\#\\\# !{input,,pheno_plinkseq_filetype_phe_file} | sed '1 s/^\\#//' 

!!expand:filetype:covar:cluster!\
filetype_phe_file_helper=$filetype_phe_file_helper_no_remove | perl -lane '(print && next) unless \\$i++; \\$skip = 0; foreach \\$f (@F) {\\$skip = 1 if \\$f == $ibs_cluster_missing} print unless \\$skip'

local cmd make_pheno_sample_cluster_all_include_file=$smart_join_cmd --out-delim $tab --col 1,2 --exec "$cluster_phe_file_helper | tail -n+2" --multiple 1 --extra 1 --in-delim 1,$tab --exec "$smart_cut_cmd --in-delim $tab !{input,--file,pheno_cluster_stats_file} --exclude-row 1,1 --select-row 1,1,PHENO_DEV,gt:0 --select-row 1,1,SIZE,gt:1 --and-row-all --select-col 1,1" | cut -f2 | sort - !{input,,pheno_sample_popgen_all_include_file} | uniq -d > !{output,,pheno_sample_cluster_all_include_file} class_level pheno

local cmd make_pheno_sample_covar_all_include_file=$smart_cut_cmd --exec "$covar_phe_file_helper" --in-delim $tab --select-col 1,1 --exclude-row 1,1 | sort - !{input,,pheno_sample_popgen_all_include_file} | uniq -d > !{output,,pheno_sample_covar_all_include_file} class_level pheno

!!expand:,:filetype,intersectwith:non_missing,pheno_sample_non_missing_all_include_file:basic,pheno_sample_basic_all_include_file:popgen,pheno_sample_non_missing_unrelated_include_file:covar,pheno_sample_popgen_unrelated_include_file:cluster,pheno_sample_popgen_unrelated_include_file! \
local cmd make_pheno_sample_filetype_unrelated_include_file=sort !{input,,pheno_sample_filetype_all_include_file} !{input,,pheno_basic_related_exclude_file} !{input,,pheno_basic_related_exclude_file} | uniq -u | sort - !{input,,intersectwith} | uniq -d > !{output,,pheno_sample_filetype_unrelated_include_file} class_level pheno

!!expand:alltype:all:unrelated! \
!!expand:filetype:non_missing:basic:popgen:covar:cluster! \
local cmd make_pheno_sample_plink_filetype_alltype_include_file=$sample_list_to_plink_sample_list("!{input,--file,pheno_sample_filetype_alltype_include_file}",0) > !{output,,pheno_sample_plink_filetype_alltype_include_file} class_level pheno

qc_plus_col=3
local cmd make_pheno_sample_failure_status_file=perl -lne 'BEGIN {open IN, "!{input,,pheno_popgen_exclude_file}"; while (<IN>) {chomp; \$ibd_failed{\$_} = 1} close IN; open IN, "!{input,,pheno_basic_related_exclude_file}"; while (<IN>) {chomp; \$related_failed{\$_} = 1} close IN; open IN, "!{input,,pheno_sample_covar_unrelated_include_file}"; while (<IN>) {chomp; \$covar_passed{\$_} = 1} close IN; open IN, "!{input,,pheno_sample_cluster_unrelated_include_file}"; while (<IN>) {chomp; \$cluster_passed{\$_} = 1} close IN; } chomp; @F = split("\t"); if (\$first) {if (\$ibd_failed{\$F[0]}) {push @F, 1; \$F[$qc_plus_col] = 0} else {push @F, 0} if (\$related_failed{\$F[0]}) {push @F, 1; \$F[$qc_plus_col] = 0} else {push @F, 0;} if (\$covar_passed{\$F[0]}) {push @F, 1;} else {push @F, 0;} if (\$cluster_passed{\$F[0]}) {push @F, 1} else {push @F, 0;} } else {\$first = 1; push @F, "$popgen_fail_status\t$related_fail_status\t$covar_pass_status\t$cluster_pass_status"} print join("\t", @F)' < !{input,,project_sample_failure_status_file} > !{output,,pheno_sample_failure_status_file} class_level pheno skip_if not_trait

local cmd make_pheno_sample_failure_status_file=perl -lne 'BEGIN {open IN, "!{input,,pheno_popgen_exclude_file}"; while (<IN>) {chomp; \$ibd_failed{\$_} = 1} close IN; open IN, "!{input,,pheno_basic_related_exclude_file}"; while (<IN>) {chomp; \$related_failed{\$_} = 1} close IN; open IN, "!{input,,pheno_sample_covar_unrelated_include_file}"; while (<IN>) {chomp; \$covar_passed{\$_} = 1} close IN; open IN, "!{input,,pheno_sample_cluster_unrelated_include_file}"; while (<IN>) {chomp; \$cluster_passed{\$_} = 1} close IN; } chomp; @F = split("\t"); if (\$first) {if (\$ibd_failed{\$F[0]}) {push @F, 1; \$F[$qc_plus_col] = 0} else {push @F, 0} if (\$related_failed{\$F[0]}) {push @F, 1; \$F[$qc_plus_col] = 0} else {push @F, 0;} if (\$covar_passed{\$F[0]}) {push @F, 1;} else {push @F, 0;} if (\$cluster_passed{\$F[0]}) {push @F, 1} else {push @F, 0;} } else {\$first = 1; push @F, "$popgen_fail_status\t$related_fail_status\t$covar_pass_status\t$cluster_pass_status"} print join("\t", @F)' < !{input,,project_sample_failure_status_file} > !{output,,pheno_sample_failure_status_file} class_level pheno skip_if not_trait

prop rank_prop=scalar

rank_prop_header=rank

get_non_missing_with_header=$add_header_cmd !{input,,pheno_sample_info_header_file} --from-file < !{input,,pheno_non_missing_sample_info_file} | sed 's/\\\#//1' | cut -f1,3,4

pheno_qc_dat_helper=$smart_join_cmd --exec "$get_non_missing_with_header" !{input,--file,@1} --arg-delim : --in-delim 1:$tab --in-delim 2:$coverage_dat_delim --header 1 --out-delim $coverage_dat_delim --extra 2 class_level pheno
pheno_qc_pdf_helper=$draw_box_plot_cmd(!{input\,\,@1} !{output\,\,@2} @3 $frac_above_threshold label=2 sep=$coverage_dat_delim @4) class_level pheno

!|expand:;:qttype;pheno_select;extrarunif:;!{prop,,pheno},!{prop,,pheno,curpheno_pheno,missing_key=default_curpheno_pheno};skip_if or,not_trait,pheno_qt,no_coverage:_qt;$pheno_half,$default_curpheno_pheno;run_if pheno_qt skip_if or,not_trait,no_coverage| \
!!expand:curpheno:case:control! \
local cmd make_phenoqttype_curpheno_sample_coverage_file_list=$smart_join_cmd --exec "$smart_cut_cmd !{input,--file,pheno_all_sample_info_file} --in-delim $tab --select-col 1,1 --select-row 1,1,pheno_select --exclude-row 1,1" !{input,--file,project_sample_coverage_file_list} --extra 2 --out-delim $tab | cut -f2 > !{output,,pheno_curpheno_sample_coverage_file_list} class_level pheno extrarunif

prop cross_classification=scalar
downstream_cross_helper=sed '1 s/\t\(\S\S*\)\t\($display_col_name\)/\t\2\t\1/g' | $smart_cut_cmd --in-delim $tab --exclude-col 0,1,$display_col_name | $smart_cut_cmd --in-delim $tab --select-col 0,1 --select-col 0,1,!{prop,,pheno,if_prop=pheno:eq:@cross_classification,if_prop=project:eq:@project,all_instances=1,sep=:} --vec-delim : 

local cmd make_pheno_cross_classification_phe_file=$smart_join_cmd --exec "$smart_cut_cmd !{input,--file,pheno_sequenced_all_sample_info_file} --in-delim $tab --select-col 1,1" !{raw,--exec,pheno,"$smart_cut_cmd --file *pheno_sequenced_all_sample_info_file --in-delim $tab --select-col 1\,1 --select-col 1\,2 --select-col 1\,1\,$display_col_name",if_prop=pheno:eq:@cross_classification,if_prop=project:eq:@project,all_instances=1,allow_empty=1} !{input,pheno_sequenced_all_sample_info_file,if_prop=pheno:eq:@cross_classification,if_prop=project:eq:@project,all_instances=1,allow_empty=1} --in-delim $tab --out-delim $tab | $downstream_cross_helper > !{output,,pheno_cross_classification_phe_file} class_level pheno run_if cross_classification

local cmd make_pheno_sample_coverage_dat_file=$pheno_qc_dat_helper(project_sample_coverage_dat_file) > !{output,,pheno_sample_coverage_dat_file} class_level pheno skip_if no_coverage
local cmd make_pheno_sample_coverage_pdf_file=$pheno_qc_pdf_helper(pheno_sample_coverage_dat_file,pheno_sample_coverage_pdf_file,'Coverage by !{prop\\,\\,pheno\\,disp}' '' '% bases >= ${threshold}x',order=$order_col_name) class_level pheno skip_if no_coverage

!|expand:;:shortt;phenol;projectl;extrakeep;extrarunif;rusageadd:;pheno;project;;;rusage_mod $project_plink_mem:short;pheno_variant_subset;project_variant_subset;!{input,--extract,pheno_variant_subset_var_keep_file};run_if num_var_subsets bsub_batch 3;| \
shortt cmd make_phenol_plink_files=$plink_cmd $plink_in_bed_helper(projectl_plinkseq_qc_plus) --make-bed !{input,--pheno,pheno_plink_phe_file} !{input,--keep,pheno_sample_plink_basic_all_include_file} extrakeep $plink_out_bed_helper(phenol) && $plink_mv_log_cmd(!{raw\,\,phenol\,*phenol_plink_file},!{output\,\,phenol_make_bed_log_file}) class_level phenol skip_if not_trait extrarunif rusageadd run_with pheno_sample_subset

short cmd ln_pheno_variant_subset_clean_all_vcf_file=ln -s !{input,,project_variant_subset_clean_all_vcf_file} !{output,,pheno_variant_subset_clean_all_vcf_file} && !{raw,,pheno_variant_subset,$run_tabix_cmd(pheno_variant_subset_clean_all_vcf_file)} class_level pheno_variant_subset run_if expand_pheno_subsets:eq:1 bsub_batch 10

short cmd make_pheno_variant_subset_clean_all_vcf_file=zcat !{input,,project_variant_subset_clean_all_vcf_file} | $vcf_utils_cmd !{input,--var-keep,pheno_variant_subset_var_keep_file} | $bgzip_cmd > !{output,,pheno_variant_subset_clean_all_vcf_file} && !{raw,,pheno_variant_subset,$run_tabix_cmd(pheno_variant_subset_clean_all_vcf_file)} class_level pheno_variant_subset run_if expand_pheno_subsets:ne:1 bsub_batch 2

cmd make_pheno_sample_combined_plink_files=$plink_cmd $plink_in_bed_helper(project_sample_combined) --make-bed !{input,--keep,pheno_sample_plink_basic_all_include_file} !{input,--pheno,pheno_plink_phe_file} $plink_out_bed_helper(pheno_sample_combined) && $plink_mv_log_cmd(!{raw\,\,pheno\,*pheno_sample_combined_plink_file},!{output\,\,pheno_sample_combined_make_bed_log_file}) class_level pheno skip_if not_trait

cmd make_pheno_sample_pruned_marker_plink_files=$plink_marker_helper !{input,--pheno,pheno_plink_phe_file} !{input,--keep,pheno_sample_plink_basic_all_include_file} --make-bed $plink_out_bed_helper(pheno_sample_pruned_marker) && $plink_mv_log_cmd(!{raw\,\,pheno\,*pheno_sample_pruned_marker_plink_file},!{output\,\,pheno_sample_pruned_marker_make_bed_log_file}) class_level pheno skip_if not_trait run_if and,!recompute_pca,!recompute_genome

cmd make_new_pheno_sample_pruned_marker_plink_files=$plink_cmd !{input,--pheno,pheno_plink_phe_file} !{input,--keep,pheno_sample_plink_basic_all_include_file} $pheno_marker_filters(sample) $plink_in_bed_helper(project_sample_marker) --make-bed $plink_out_bed_helper(pheno_sample_pruned_marker) && $plink_mv_log_cmd(!{raw\,\,pheno\,*pheno_sample_pruned_marker_plink_file},!{output\,\,pheno_sample_pruned_marker_make_bed_log_file}) class_level pheno skip_if not_trait run_if or,recompute_pca,recompute_genome rusage_mod $project_plink_mem

local cmd make_pheno_additional_popgen_plink_exclude_file=$sample_list_to_plink_sample_list("!{input,--file,pheno_additional_popgen_exclude_file}",0) > !{output,,pheno_additional_popgen_plink_exclude_file} class_level pheno run_if pheno_additional_popgen_exclude_file

short cmd make_pheno_sample_for_pca_plink_files=$make_for_pca_helper(pheno_sample_pruned_marker,pheno,!{input;--keep;pheno_sample_plink_popgen_all_include_file}) class_level pheno run_if and,marker;add_for_pca,!pheno_additional_popgen_exclude_file

short cmd make_pheno_sample_for_pca_with_filter_plink_files=$make_for_pca_helper(pheno_sample_pruned_marker,pheno,!{input;--remove;pheno_additional_popgen_plink_exclude_file} !{input;--keep;pheno_sample_plink_popgen_all_include_file}) class_level pheno run_if and,marker;add_for_pca,pheno_additional_popgen_exclude_file

short cmd filter_pheno_sample_for_pca_plink_files=$plink_cmd $plink_in_bed_helper(pheno_sample_pruned_marker) !{input;--keep;pheno_sample_plink_popgen_all_include_file} --make-bed $plink_out_bed_helper(pheno_sample_for_pca) && $plink_mv_log_cmd(!{raw\,\,pheno\,*pheno_sample_for_pca_plink_file},!{output\,\,pheno_sample_for_pca_make_bed_log_file}) class_level pheno run_if and,!marker;add_for_pca

local cmd make_pheno_top_hits_snp_ids_file=$smart_cut_cmd !{input,--file,project_sample_combined_bim_file} --exec "$smart_cut_cmd !{input,--file,pheno_all_marker_top_hits_file} --select-col 1,1,'CHROM POS' | sed 's/$/ 1/' | tail -n+2" --select-col 1,'1 4 2' | sort -ns -k1,1 -k2,2 | awk '{print \$3,\$1,\$2}' | uniq -d -f1 | awk '{print \$1}' > !{output,,pheno_top_hits_snp_ids_file} class_level pheno run_if and,!not_trait,pheno_all_marker_top_hits_file

top_hit_r2_threshold=.2
cmd make_pheno_top_hits_ld_file=$plink_cmd $plink_in_bed_helper(pheno_sample_combined) !{input,--ld-snp-list,pheno_top_hits_snp_ids_file} --ld-window-kb 10000 --ld-window 99999 --ld-window-r2 $top_hit_r2_threshold !{raw,--out,pheno,*pheno_top_hits_ld_trunk} !{output,pheno_top_hits_ld_file} && $plink_mv_log_cmd(!{raw\,\,pheno\,*pheno_top_hits_ld_trunk},!{output\,\,pheno_top_hits_ld_log_file}) class_level pheno run_if and,!not_trait,pheno_all_marker_top_hits_file

local cmd make_pheno_top_hits_ld_chr_pos_list=$smart_join_cmd --exec "$smart_cut_cmd !{input,--file,pheno_top_hits_ld_file} !{input,--file,pheno_top_hits_ld_file} --select-col 1,1,SNP_A --select-col 2,1,SNP_B --exclude-row 1,1 --exclude-row 2,1 | sort -u" --exec "awk '{print \\$2,\\$1,\\$4}' !{input,,project_sample_combined_bim_file}" --out-delim $tab --extra 2 | cut -f2- > !{output,,pheno_top_hits_ld_chr_pos_list} class_level pheno run_if and,!not_trait,pheno_all_marker_top_hits_file

local cmd make_pheno_top_hits_ld_seq_var_list=$smart_cut_cmd !{input,--file,project_clean_gene_variant_file} --exec "sed 's/$/\t1/' !{input,,pheno_top_hits_ld_chr_pos_list}" --in-delim $tab --select-col 1,1,'CHROM POS ID' | sort -sn -k1,1 -k2,2 | $smart_cut_cmd --in-delim $tab --select-col 0,'3 1 2' | uniq -d -f1 | cut -f1 > !{output,,pheno_top_hits_ld_seq_var_list} class_level pheno run_if and,!not_trait,pheno_all_marker_top_hits_file

!|expand:,:shortt,phenol,extrarunif:,pheno,run_if !num_var_subsets:short,pheno_variant_subset,run_if num_var_subsets bsub_batch 20| \
shortt cmd make_phenol_test_missing_file=$plink_analysis_phenol_bed_cmd(phenol_test_missing_file,test-missing --allow-no-sex,missing,phenol_test_missing_log_file) class_level phenol skip_if or,not_trait,pheno_qt extrarunif

!|expand:,:shortt,phenol,extrarunif:,pheno,run_if !num_var_subsets:short,pheno_variant_subset,run_if num_var_subsets| \
shortt cmd make_phenol_hwe_file=$plink_analysis_phenol_bed_cmd(phenol_hwe_file,hardy --allow-no-sex,hwe,phenol_hwe_log_file) class_level phenol skip_if not_trait extrarunif

!!expand:male:male:female! \
!|expand:,:shortt,phenol,extrarunif:,pheno,run_if !num_var_subsets:short,pheno_variant_subset,run_if num_var_subsets bsub_batch 20| \
shortt cmd make_phenol_male_hwe_file=$plink_analysis_phenol_bed_cmd(phenol_male_hwe_file,hardy --allow-no-sex !{input;--filter;pheno_plink_phe_file;instance_level=pheno;if_prop=project:eq:@project;all_instances=1;if_prop=pheno:eq:sex} $plink_male,hwe,phenol_male_hwe_log_file) class_level phenol skip_if not_trait extrarunif

!|expand:,:shortt,phenol,extrarunif:,pheno,run_if !num_var_subsets:short,pheno_variant_subset,run_if num_var_subsets| \
shortt cmd make_phenol_lmiss_file=$plink_analysis_phenol_bed_cmd(phenol_lmiss_file,missing --allow-no-sex,lmiss,phenol_lmiss_log_file) class_level phenol skip_if not_trait extrarunif

!|expand:,:shortt,phenol,extrarunif:,pheno,run_if !num_var_subsets:short,pheno_variant_subset,run_if num_var_subsets| \
shortt cmd make_phenol_frq_file=$plink_analysis_phenol_bed_cmd(phenol_frq_file,freq --nonfounders --allow-no-sex,frq,phenol_frq_log_file) class_level phenol skip_if not_trait extrarunif

!|expand:,:shortt,phenol,projectt,extrarunif:,pheno,project,run_if !num_var_subsets:short,pheno_variant_subset,project_variant_subset,run_if num_var_subsets bsub_batch 20| \
shortt cmd make_phenol_sex_frq_file=$plink_analysis_phenol_bed_cmd(phenol_sex_frq_file,freq --nonfounders --allow-no-sex !{input;--within;pheno_plink_phe_file;instance_level=pheno;if_prop=project:eq:@project;all_instances=1;if_prop=pheno:eq:sex},frq.strat,phenol_sex_frq_log_file) class_level phenol skip_if not_trait extrarunif

!|expand:,:phenol,projectt:pheno,project:pheno_variant_subset,project_variant_subset| \
phenol_multiallelic_qc_helper=$multiallelic_qc_cmd !{input,,pheno_sample_basic_all_include_file} -spl_field:1 -out:!{output,,@1} -condition_on:$thresholded_nalt_field -var_mask:"file=$pseq_multiallelic_tag reg.ex=$chrX; reg=$chrX" -pseq_project:!{input,,projectt_plinkseq_clean_project_file} !{input,-pheno_file:,pheno_non_missing_sample_pheno_file,if_prop=pheno:eq:sex,allow_empty=1,all_instances=1,if_prop=project:eq:\@project,max=1,instance_level=pheno,sep=} !{raw,,pheno,-pheno_field:2 -pheno_id_field:1 -pheno_sel:eq$plink_female,if_prop=pheno:eq:sex,allow_empty=1,all_instances=1,if_prop=project:eq:\@project,max=1} -chrX_code:$chrX

!|expand:,:shortt,phenol,projectt,extrarunif:,pheno,project,run_if !num_var_subsets:short,pheno_variant_subset,project_variant_subset,run_if num_var_subsets| \
shortt cmd make_phenol_multiallelic_frq_file=$phenol_multiallelic_qc_helper(phenol_multiallelic_frq_file) class_level phenol skip_if not_trait extrarunif

!|expand:,:shortt,phenol,projectt,extrarunif:,pheno,project,run_if !num_var_subsets:short,pheno_variant_subset,project_variant_subset,run_if num_var_subsets| \
shortt cmd make_phenol_multiallelic_group_frq_file=$phenol_multiallelic_qc_helper(phenol_multiallelic_group_frq_file) -m_group_file:!{input,,pheno_plink_phe_file,unless_prop=pheno_qt,allow_empty=1} !{raw,,pheno,-m_group_field:3 -m_id_field:2 -m_group_missing:$plink_missing,unless_prop=pheno_qt,allow_empty=1} -m_group_affected=$case_pheno_helper class_level phenol skip_if not_trait extrarunif

!|expand:,:shortt,phenol,projectt,extrarunif:,pheno,project,run_if !num_var_subsets:short,pheno_variant_subset,project_variant_subset,run_if num_var_subsets| \
shortt cmd make_phenol_combined_combined_frq_file=$combined_frq_helper(phenol_frq_file,phenol_multiallelic_frq_file) > !{output,,phenol_combined_frq_file} class_level phenol skip_if not_trait extrarunif

local cmd make_pheno_high_test_missing_file=$smart_join_cmd --in-delim $tab --exec "$smart_cut_cmd !{input,--file,pheno_test_missing_file} --select-col 1,1,SNP --exact --select-row 1,1 --select-row 1,1,P,lt:!{prop,,pheno,min_clean_p_missing}" --exec "$smart_cut_cmd !{input,--file,project_clean_gene_variant_file} --in-delim $tab --select-col 1,1,'ID CHROM POS'" --header 1 --extra 2 | $smart_cut_cmd --in-delim $tab --exclude-row 0,1 > !{output,,pheno_high_test_missing_file} class_level pheno skip_if or,not_trait,pheno_qt

get_covar_traits=`($smart_cut_cmd --file /dev/null !{input,--file,pheno_indicator_list_file,if_prop=pheno:eq:\@covar_traits,unless_prop=pheno_qt,if_prop=project:eq:\@project,instance_level=pheno,all_instances=1,allow_empty=1} --exclude-row .,1 | sort - !{input,,@{2}_variable_covars_file} | uniq -d && perl -e 'print qq(!{prop,,pheno,if_prop=pheno:eq:\@covar_traits,if_prop=project:eq:\@project,if_prop=pheno_qt,all_instances=1,allow_empty=1,sep=\n}!{raw,,@2,\n,if_prop=manual_covar_traits,allow_empty=1}!{prop,,@2,manual_covar_traits,if_prop=manual_covar_traits,allow_empty=1,sep=\n})') | awk 'NF > 0' | perl -pe 's/\n/@1/' | sed 's/@1$//'`

add_cluster=--strata !{raw,,project,$ibs_cluster_pheno}
add_strata=$add_cluster!{raw,,@1,\,,if_prop=strata_traits,allow_empty=1}!{prop,,@1,strata_traits,if_prop=strata_traits,sep=\,,allow_empty=1}

add_covar=--covar $get_covar_traits( ,@1)!{raw::@1: :if_prop=strat_covar:if_prop=covar_traits:allow_empty=1}!{raw::@1: :if_prop=strat_covar:if_prop=manual_covar_traits:unless_prop=covar_traits:allow_empty=1}`!{raw::@1:$get_mds_cols(\@num_mds_covar, ):if_prop=strat_covar:allow_empty=1}`

qt_plinkseq_phenotype_selector_raw=!{prop,@1,pheno,unless_prop=pheno_qt,allow_empty=1}!{raw,@1,pheno,$pheno_extreme_for_raw,if_prop=pheno_qt,allow_empty=1}
qt_plinkseq_phenotype_selector=$qt_plinkseq_phenotype_selector_raw(--phenotype)

#!|expand:;:keyext;extracmd;extrarunif:\
#strata_;$add_cluster;run_if or,run_cluster,strata_traits:\
#;;|\
#cmd make_pheno_keyextvdist_file=$pseq_qc_plus_analysis_cmd_pheno_cluster(v-dist) $qt_plinkseq_phenotype_selector !{input,pheno_is_trait_file} extracmd > !{output,,pheno_keyextvdist_file} class_level pheno extrarunif run_with pheno_variant_subset,burden_variant_subset

!|expand:,:shortt,phenol,extrarunif:,pheno,run_if !num_var_subsets:short,pheno_variant_subset,run_if num_var_subsets| \
shortt cmd make_phenol_counts_file=$pseq_qc_plus_all_assoc_cmd_phenol(counts) $qt_plinkseq_phenotype_selector > !{output,,phenol_counts_file} class_level phenol skip_if not_trait extrarunif

local cmd make_pheno_sstats_file=$pheno_qc_dat_helper(project_sstats_file) > !{output,,pheno_sstats_file} class_level pheno with project

with_genome_delim=,

short cmd make_pheno_with_genome_file=$smart_join_cmd --exec "$smart_cut_cmd --exec 'zcat !{input,,project_sample_marker_genome_file}' --select-col 1,1,IID1 --select-col 1,1,IID2 --exclude-row 1,1 --out-delim $tab | sed 's/\t/\n/g' | sort -u | $smart_cut_cmd !{input,--file,pheno_non_missing_sample_info_file} --in-delim $tab --select-col 1,1 | sort | uniq -d | cat - !{input,,project_sample_include_file} | sort | uniq -d | sed '1 s/^/ID\n/'" --exec "$get_non_missing_with_header" --in-delim $tab --out-delim $with_genome_delim $tab --header 1 --extra 2 | tail -n+2 > !{output,,pheno_with_genome_file} class_level pheno skip_if recompute_genome run_with pheno_variant_subset

prop recompute_genome=scalar default 0

#Assumes order cols are always numeric
restart_mem short cmd make_filter_pheno_genome_file=$filter_sample_genome_file(!{input::pheno_with_genome_file},project_sample_marker_genome_file) | gzip -c > !{output,,pheno_genome_file} class_level pheno skip_if or,recompute_genome,(and,num_samp_subsets,parallelize_genome)

#restart_mem short cmd make_pheno_annotated_genome_file=$smart_join_cmd --exec "while read a; do while read b; do echo \\$a$with_genome_delim\\$b; done < !{input,,pheno_with_genome_file}; done < !{input,,pheno_with_genome_file} | sed '1 s/^/ID1$with_genome_delim${display_col_name}1$with_genome_delim${order_col_name}1${with_genome_delim}ID2$with_genome_delim${display_col_name}2$with_genome_delim${order_col_name}2\n/' | perl -ne 'chomp; @cols = split(/$with_genome_delim/); @out = (); push @out, \\$cols[0]; push @out, \\$cols[3]; @scols = sort(@cols[2,5]); \\$lineno++; if (\\$lineno == 1 || join(\":\", @scols) == join(\":\", @cols[2,5])) {@slice1 = (1,4); @slice2 = (2,5)} else {@slice1 = (4,1); @slice2 = (5,2)} push @out, join(\"/\", @cols[@slice1]); push @out, join(\"\", @cols[@slice2]); print join(\"$with_genome_delim\", @out); print \"\n\"' | sed 's/,/\t/g' | sed 's/^\(\S*\)\(\s*\)\(\S*\)/\1:\3\2\1\2\3/'" --exec "zcat !{input,,pheno_genome_file} | $smart_cut_cmd --exclude-col 0,1,'FID1 FID2' --out-delim $tab | sed 's/^\(\S*\)\(\s*\)\(\S*\)/\1:\3/'" --extra 1 --in-delim $tab --header 1 --out-delim $tab | cut -f2- | sed '1 s/${display_col_name}1\/${display_col_name}2/${display_col_name}/' | sed '1 s/${order_col_name}1${order_col_name}2/${order_col_name}/' | gzip -c > !{output,,pheno_annotated_genome_file} class_level pheno rusage_mod $pheno_genome_mem run_with pheno_variant_subset 


#Different than for project
"""
}
    