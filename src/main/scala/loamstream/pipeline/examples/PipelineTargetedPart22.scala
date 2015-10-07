
package loamstream.pipeline.examples

/**
 * LoamStream
 * Created by oliverr on 10/6/2015.
 */
object PipelineTargetedPart22 {
  val string =
 """#Compute between people at this level and everyone else
!|expand:;:shortt;phenol;runif;exflags:;pheno;run_if or,!num_samp_subsets,!parallelize_genome;:short;pheno_sample_subset;run_if and,num_samp_subsets,parallelize_genome;!{input,--read-freq,pheno_frq_file} --genome-lists !{input,,project_sample_subset_sample_marker_genome_list1_file} !{input,,pheno_sample_pruned_marker_fam_file}| \
shortt cmd make_phenol_genome_file=$plink_cmd $plink_in_bed_helper(pheno_sample_pruned_marker) --genome --Z-genome exflags !{raw,--out,phenol,*phenol_genome_trunk} !{output,phenol_genome_file} && $plink_mv_log_cmd(!{raw\,\,phenol\,*phenol_genome_trunk},!{output\,\,phenol_genome_log_file}) class_level phenol runif skip_if !recompute_genome

short cmd make_subset_pheno_sample_subset_genome_file=awk '{print \$2}' !{input::project_sample_subset_sample_marker_genome_list1_file} | cat - !{input,,pheno_sample_basic_all_include_file} | sort | uniq -d | $filter_sample_genome_file(-,project_sample_marker_genome_file) '' EITHER !{input,,pheno_sample_non_missing_all_include_file} | gzip -c > !{output,,pheno_sample_subset_genome_file} class_level pheno_sample_subset skip_if or,recompute_genome,!parallelize_genome

short cmd make_pheno_sample_subset_genome_for_cat_file=$smart_join_cmd --exec "zcat !{input,,pheno_sample_subset_genome_file} | awk 'NR > 1 {print \\$1,\\$2}' | sort -u | cat !{input,,project_sample_subset_sample_marker_genome_list1_file} - | sort | uniq -d | $add_header_cmd 'IID1 FID1' " --exec "zcat !{input,,pheno_sample_subset_genome_file}" --header 1 --col 1 --col 2 --extra 2 --multiple 2 | awk '{s=\$1; \$1=\$2; \$2=s} {print}' | gzip -c > !{output,,pheno_sample_subset_genome_for_cat_file} class_level pheno_sample_subset run_if parallelize_genome bsub_batch 5

short cmd make_cat_pheno_genome_file=zcat !{input,,pheno_sample_subset_genome_for_cat_file,sort_prop=pheno_sample_subset} | awk 'NR==1 {a=\$1; print} NR > 1 && \$1 != a {print}' | gzip -c > !{output,,pheno_genome_file} class_level pheno run_if and,num_samp_subsets,parallelize_genome skip_if not_trait

local cmd make_pheno_project_mds_file=$smart_join_cmd --exec "$smart_cut_cmd !{input,--file,project_sample_marker_mds_file} --select-col 1,1,IID --exclude-row 1,1 | $smart_cut_cmd !{input,--file,pheno_non_missing_sample_info_file} --in-delim $tab --select-col 1,1 | sort | uniq -d | cat - !{input,,project_sample_include_file} | sort | uniq -d  | sed '1 s/^/ID\n/'" --exec "$get_non_missing_with_header" --exec "$smart_cut_cmd !{input,--file,project_sample_marker_mds_file} --exclude-col 1,1,FID --exclude-col 1,1,SOL --out-delim $tab"  --in-delim $tab --header 1 --out-delim $tab --extra 2 --extra 3 > !{output,,pheno_project_mds_file} class_level pheno with project

smart_pca_num_outlier_iter=5
smart_pca_num_outlier_evec=10
smart_pca_outlier_sigma_thresh=6
smart_pca_qtmode=!{raw,,pheno,0,unless_prop=pheno_qt,allow_empty=1}!{raw,,pheno,1,if_prop=pheno_qt,allow_empty=1}

prop recompute_pca=scalar default 0
prop remove_pca_outliers=scalar default 1 #when running PCA with eigenstrat, remove outliers

#meta_table cmd make_pheno_smart_pca_par_file=!{raw,genotypename:,pheno,*pheno_sample_pruned_marker_bed_file}\n!{raw,snpname:,pheno,*pheno_sample_popgen_pruned_marker_bim_file}\n!{raw,indivname:,pheno,*pheno_sample_popgen_pruned_marker_fam_file}\n!{raw,evecoutname:,pheno,*pheno_smart_pca_evec_file}\n!{raw,evaloutname:,pheno,*pheno_smart_pca_eval_file}\naltnormstyle: NO\nnumoutevec: !{prop,,pheno,num_mds_calc}\nnumoutlieriter: !{raw,,pheno,$smart_pca_num_outlier_iter,if_prop=remove_pca_outliers,allow_empty=1}!{raw,,pheno,0,unless_prop=remove_pca_outliers,allow_empty=1}\nnumoutlierevec: $smart_pca_num_outlier_evec\noutliersignmathresh: $smart_pca_outlier_sigma_thresh\nqtmode: $smart_pca_qtmode !{output,pheno_smart_pca_par_file} class_level pheno run_if and,use_eigenstrat,!pheno_custom_mds_file

#cmd make_pheno_smart_pca_out_files=$smart_pca_cmd !{input,-p,pheno_smart_pca_par_file} > !{output,,pheno_smart_pca_log_file} !{input,pheno_sample_popgen_pruned_marker_bed_file} !{input,pheno_sample_popgen_pruned_marker_bim_file} !{input,pheno_sample_popgen_pruned_marker_fam_file} !{output,pheno_smart_pca_evec_file} !{output,pheno_smart_pca_eval_file} class_level pheno run_if and,use_eigenstrat,!pheno_custom_mds_file

short cmd make_pheno_sample_marker_pruned_initial_vcf_file=$marker_pruned_initial_vcf_helper(pheno,pheno_sample_pruned_marker) class_level pheno rusage_mod $project_plink_mem run_if or,recompute_genome,recompute_pca skip_if not_trait

#need to do this since marker may not be sorted same way as VCF

short cmd make_pheno_sample_marker_pruned_vcf_file=$marker_pruned_vcf_helper(pheno) class_level pheno run_if and,use_marker_for_sample_ibd,!not_trait skip_if and,!recompute_genome,!recompute_pca

short cmd ln_pheno_sample_marker_pruned_vcf_file=ln -s !{input,,pheno_sample_marker_pruned_initial_vcf_file} !{output,,pheno_sample_marker_pruned_vcf_file} && !{raw,,pheno,$run_tabix_cmd(pheno_sample_marker_pruned_vcf_file)} class_level pheno skip_if or,use_marker_for_sample_ibd,not_trait run_if and,(or,recompute_genome,recompute_pca)

short cmd ln_from_project_pheno_sample_marker_pruned_vcf_file=ln -s !{input,,project_sample_marker_pruned_vcf_file} !{output,,pheno_sample_marker_pruned_vcf_file} && !{raw,,pheno,$run_tabix_cmd(pheno_sample_marker_pruned_vcf_file)} class_level pheno skip_if or,use_marker_for_sample_ibd,recompute_genome,recompute_pca,not_trait

local cmd make_pheno_epacts_ready_file=touch !{output,,pheno_epacts_ready_file} class_level pheno

cmd make_pheno_kinship_file=$kinship_helper(pheno) class_level pheno rusage_mod $kin_mem run_if recompute_genome skip_if not_trait run_with pheno_sample_subset

local cmd ln_pheno_kinship_file=ln -s !{input,,project_kinship_file} !{output,,pheno_kinship_file} class_level pheno rusage_mod $kin_mem skip_if or,recompute_genome,not_trait

local cmd make_pheno_smart_pca_fam_file=$pheno_smart_pca_fam_file_helper(pheno_sample_for_pca_fam_file) | cat !{input,,pheno_basic_related_exclude_file} - | awk 'NF == 1 {m[\$1]=1} NF == 6 {if (m[\$2] ) {\$6=$pca_ignore_pop} } NF == 6 {print}' | $smart_pca_remap_helper(0,2) > !{output,,pheno_smart_pca_fam_file} class_level pheno run_if recompute_pca 

local cmd make_pheno_sample_subset_smart_pca_fam_file=$project_sample_subset_smart_pca_fam_file_helper(pheno_smart_pca_fam_file) > !{output,,pheno_sample_subset_smart_pca_fam_file} class_level pheno_sample_subset run_if and,recompute_pca,parallelize_pca

!|expand:;:shortt;phenos;exrunif:;pheno;skip_if and,num_samp_subsets,parallelize_pca:short;pheno_sample_subset;skip_if !parallelize_pca| \
shortt cmd make_phenos_smart_pca_out_files=$smart_pca_cmd !{input,-i,pheno_sample_for_pca_bed_file} !{input,-a,pheno_sample_for_pca_bim_file} !{input,-b,phenos_smart_pca_fam_file} !{input,-w,project_sample_marker_smart_pca_poplist_file} !{prop,-k,pheno,num_mds_calc} !{raw,-o,phenos,*phenos_smart_pca_trunk} !{output,phenos_smart_pca_evec_file} !{output,-e,phenos_smart_pca_eval_file} !{raw,-p,phenos,*phenos_smart_pca_trunk} !{output,-g,phenos_smart_pca_weights_file} !{output,-l,phenos_smart_pca_log_file} -m !{raw,,pheno,$smart_pca_num_outlier_iter,if_prop=remove_pca_outliers,allow_empty=1}!{raw,,pheno,0,unless_prop=remove_pca_outliers,allow_empty=1} -t $smart_pca_num_outlier_evec -s $smart_pca_outlier_sigma_thresh -q $smart_pca_qtmode class_level phenos run_if and,!pheno_custom_mds_file,recompute_pca exrunif

parse_pheno_mds_helper=$smart_join_cmd --in-delim $tab --exec "cat !{input,,@{1}_smart_pca_fam_file} | awk '{print \\$2}' | $smart_cut_cmd --exec '$smart_cut_cmd !{input,--file,@{1}_smart_pca_evec_file} --select-col 1,1 --exclude-row 1,1' --in-delim : --select-col 1,2 | sort | uniq -d | $smart_pca_remap_helper_int(2,0,0, ,\) | cat - !{input,,@2} | sort | uniq -d | sed '1 s/^/IID\n/'" --exec "$evec_to_mds_helper_int(@{1}_smart_pca_evec_file,@1,\) | $smart_pca_remap_helper_int(2,0,1,\\t,\)" --extra 2 --col 2,2 --header 1 | sed 's/^\(\S\S*\)\(\s\s*\)\(\S\S*\)/\3\2\1/'

short cmd parse_pheno_sample_subset_mds_file=$parse_pheno_mds_helper(pheno_sample_subset,project_sample_subset_samp_keep_file) > !{output,,pheno_sample_subset_mds_file} class_level pheno_sample_subset run_if and,parallelize_pca,!pheno_custom_mds_file,recompute_pca

local cmd parse_pheno_mds_file=$parse_pheno_mds_helper(pheno,project_passed_sample_list_file) > !{output,,pheno_mds_file} class_level pheno run_if and,!pheno_custom_mds_file,recompute_pca,(or,!parallelize_pca,!num_samp_subsets)

local cmd cp_pheno_mds_file=$smart_join_cmd --exec "tail -n+2 !{input,,project_sample_marker_mds_file} | cat - !{input,,pheno_sample_plink_basic_all_include_file} | awk '{print \\$1,\\$2}' | sort | uniq -d | sed '1 s/^/FID IID\n/'" !{input,--file,project_sample_marker_mds_file} --col 1 --col 2 --header 1 --extra 2 > !{output,,pheno_mds_file} class_level pheno run_if and,!pheno_custom_mds_file,!recompute_pca

local cmd convert_pheno_custom_mds_file=$smart_join_cmd --exec "$smart_cut_cmd !{input,--file,pheno_custom_mds_file} --select-col 1,'1 2' --exclude-row 1,1 | cat - !{input,,pheno_sample_plink_basic_all_include_file} | sed 's/\s\s*/\t/g' | sort | uniq -d | sed '1 s/^/FID\tIID\n/'  " !{input,--file,pheno_custom_mds_file} --header 1 --extra 2 --col 1 --col 2 > !{output,,pheno_mds_file} run_if pheno_custom_mds_file class_level pheno

local cmd make_pheno_annotated_mds_file=$smart_join_cmd --exec "$smart_cut_cmd !{input,--file,pheno_sample_plink_basic_all_include_file} --select-col 1,2 !{input,--file,pheno_mds_file} --select-col 2,2 | sort | uniq -d | sed '1 s/^/ID\n/'" --exec "$get_non_missing_with_header" --exec "$smart_cut_cmd !{input,--file,pheno_mds_file} --exclude-col 1,1,FID --exclude-col 1,1,SOL --out-delim $tab"  --in-delim $tab --header 1 --out-delim $tab --extra 2 --extra 3 > !{output,,pheno_annotated_mds_file} class_level pheno

prop mds_exclude_filters=list
default_mds_exclude_filters=C1,lt:0 C1,gt:0

prop max_strata_cluster_size=scalar
default_max_strata_cluster_size=100
prop no_strata_cc=scalar

prop cluster_ppc_value=scalar

default_cluster_ppc_value=.01

cmd make_pheno_sample_marker_cluster_files=$sample_list_to_plink_sample_list("!{input,--file,pheno_sample_basic_all_include_file}",0) | $plink_cmd $plink_in_bed_helper(pheno_sample_pruned_marker) !{input,--pheno,pheno_plink_phe_file} --keep /dev/stdin !{input,--read-genome,pheno_genome_file} --cluster !{prop,--ppc,pheno,cluster_ppc_value,missing_key=default_cluster_ppc_value} !{raw,,pheno,--cc,unless_prop=no_strata_cc,unless_prop=pheno_qt,allow_empty=1} !{input,--match,pheno_plink_match_file,if_prop=strata_traits,allow_empty=1} !{prop,--mc,pheno,max_strata_cluster_size,missing_key=default_max_strata_cluster_size} !{raw,--out,pheno,*pheno_sample_marker_plink_file} !{output,pheno_sample_marker_cluster0_file} !{output,pheno_sample_marker_cluster1_file} !{output,pheno_sample_marker_cluster2_file} !{output,pheno_sample_marker_cluster3_file}  && $plink_mv_log_cmd(!{raw\,\,pheno\,*pheno_sample_marker_plink_file},!{output\,\,pheno_sample_marker_cluster_log_file}) class_level pheno run_with pheno_variant_subset run_if run_cluster

get_samples_to_cluster=$sample_list_to_plink_sample_list(--exec "$smart_join_cmd --exec \\"cut -f1 !{input\,\,pheno_plinkseq_strata_phe_file} | tail -n+2 | cat - !{input::pheno_sample_basic_all_include_file} | sort | uniq -d | sed '1 s/^/IID\n/'\\" !{input:--file:pheno_plinkseq_strata_phe_file} --header 1 --comment \\\#\\\# --extra 2",1)
get_all_samples_to_cluster=$sample_list_to_plink_sample_list(!{input:--file:pheno_sample_basic_all_include_file},1)
local cmd make_pheno_dummy_strata_sample_marker_cluster_files=$get_samples_to_cluster | $smart_cut_cmd --select-col 0,1,'FID IID !{prop,,pheno,strata_traits}' --exclude-row 0,1 | perl -lane 'print "\$F[0]\t\$F[1]\t" . join("", @F[2..\$\#F])' | awk -v OFS="\t" 'BEGIN {i=1} !m[\$3] {m[\$3] = i++} {print \$1,\$2,m[\$3]}' > !{output,,pheno_sample_marker_cluster2_file} class_level pheno run_if and,!run_cluster,strata_traits
local cmd make_pheno_dummy_sample_marker_cluster_files=$get_all_samples_to_cluster | $smart_cut_cmd --select-col 0,1,'FID IID' --exclude-row 0,1 | awk -v OFS="\t" '{print \$1,\$2,1}' > !{output,,pheno_sample_marker_cluster2_file} class_level pheno run_if and,!run_cluster,!strata_traits

#vdist_parse_helper=egrep '^-|@1' !{input,,pheno_cluster_vdist_file} | tail -n+3 | sed 's/^-.\*=\s*\(\S\S*\).*/\1/' | sed 's/.\*chi-sq\s*\S*\s*=\s*\(\S\S*\)/\1\n/' | sed 's/.*\sp\s\s*=\s*\(\S\S*\).*/\1/' | tr '\n' '\t' | sed 's/\(\S\S*\)\s\s*\(\S\S*\)\s\s*\(\S\S*\)\s\s*/\1\t\2\t\3\n/g' | sed '1 s/^/ID\tCHI_SQ_@2\tP_@2\n/' | $add_function_cmd --in-delim $tab --header 1 --col1 P_@2 --type minus_log --val-header LOG_P_@2 | cut -f1,2,4


local cmd make_pheno_cluster_avg_stats_file=$smart_join_cmd --in-delim $tab --exec "$cluster_phe_file_helper | tail -n+2 | cut -f1 | cat - !{input,,pheno_all_popgen_istats_file} | cut -d, -f1 | sort | uniq -d | sed '1 s/^/ID\n/'" --exec "$cluster_phe_file_helper" --exec "$smart_cut_cmd !{input,--file,pheno_all_popgen_istats_file} --select-col 1,1 --select-col 1,1,'C1 C2' --in-delim , --out-delim $tab" --extra 2 --extra 3 --header 1  | $table_sum_stats_cmd --in-delim $tab --out-delim $tab --group-col $ibs_cluster_pheno --col C1 --col C2 --summaries --has-header --print-header | $smart_cut_cmd --in-delim $tab --select-col 0,1 --select-col 0,1,$table_sum_stats_stddev | sed '1 s/$table_sum_stats_stddev/DEV/g' > !{output,,pheno_cluster_avg_stats_file} class_level pheno 

local cmd make_pheno_cluster_stats_file=$smart_join_cmd --in-delim $tab !{input,--file,pheno_cluster_avg_stats_file} --exec "$smart_join_cmd --comment \\\# --extra 2 --out-delim $tab !{input,--file,pheno_plinkseq_cluster_phe_file} --exec \"$smart_cut_cmd !{input,--file,pheno_all_sample_info_file} --select-col 1,1 --select-col 1,1,!{prop,,pheno}\" | cut -f2- | awk 'NR == 1 || \\$1 != $ibs_cluster_missing' | $table_sum_stats_cmd --in-delim $tab --out-delim $tab --group-col 1 --col 2 --summaries --print-header | $smart_cut_cmd --in-delim $tab --exclude-row 0,1 --select-col 0,1  --select-col 0,1,$table_sum_stats_stddev | sed '1 s/^/ID\tPHENO_DEV\n/'" --exec "$cluster_phe_file_helper | tail -n+2 | cut -f2 | sort | uniq -c | sed 's/^\s*//' | sed 's/\s\s*/\t/g' | sed '1 s/^/SIZE\tID\n/'" --extra 2 --extra 3 --col 3,2 --header 1 > !{output,,pheno_cluster_stats_file} class_level pheno 

local cmd make_pheno_cluster_stats_pdf_file=$draw_box_plot_cmd(!{input\,\,pheno_cluster_stats_file} !{output\,\,pheno_cluster_stats_pdf_file} 'Sample cluster properties: !{prop\,\,pheno\,disp}' '' 'Cluster Values' -1) class_level pheno 


local cmd make_pheno_trait_hist_pdf_file=$smart_join_cmd --in-delim $tab --exec "sed '1 s/^/ID\n/' !{input,,pheno_sample_non_missing_all_include_file}" !{input,--file,pheno_sequenced_all_sample_info_file} --rest-extra 1 --header 1 --out-delim , | $draw_hist_plot_cmd("/dev/stdin !{output,,pheno_trait_hist_pdf_file} !{prop,,pheno} '!{prop,,pheno,disp} QC+ sample values' '!{prop,,pheno,disp}' sep=, do.gaussian=T") class_level pheno run_if pheno_qt

pheno_sstats_pdf_helper=$draw_box_plot_cmd(!{input::pheno_sstats_file} !{output::@1} '@2 sample QC properties: by !{prop::pheno:disp}' '' 'Sample Values' @3 sep=\, @4 max.plot.points=$max_sstats_points max.highlighted.points=$max_sstats_highlighted_points)
pheno_all_sstats_pdf_helper=$pheno_sstats_pdf_helper(@1,@2,-1\\,$display_col_name\\,$order_col_name\\,NVAR,@3)
pheno_slide_sstats_pdf_helper=$pheno_sstats_pdf_helper(@1,@2,"$all_major_sstats_cols",@3)

pheno_highlight_helper=$sample_highlight_columns_int id.col=1


!!expand:,:slideornone,slideorall:slide_,slide:,all! \
!|expand@;@shortt;type;tname;extra;which;runifvalue\
    @short;slideornoneall;All;;slideorall;\ 
    @short;slideornonehighlighted;All;highlight.list.file=!{input::project_sample_exclude_detail_file} $pheno_highlight_helper highlight.label.col=2;slideorall;\
    @local;slideornonefiltered;QC+;highlight.list.file=!{input::project_sample_exclude_detail_file} $pheno_highlight_helper highlight.label.col=2 exclude.highlighted=TRUE;slideorall;\
    @short;slideornoneextreme;QC+;highlight.list.file=!{input::project_sample_exclude_detail_file} $pheno_highlight_helper highlight.label.col=2 exclude.highlighted=TRUE extreme.highlight.col="$all_extreme_highlight_cols" extreme.quantile=$extreme_quantile ;slideorall;\
    @short;slideornonepopgen;QC/Popgen+;highlight.list.file=!{input::pheno_sample_popgen_all_include_file} $pheno_highlight_helper highlight.header=FALSE include.highlighted=TRUE;slideorall;skip_if not_trait\
| \
shortt cmd make_pheno_type_sstats_pdf_file=$pheno_which_sstats_pdf_helper(pheno_type_sstats_pdf_file,tname,label=$display_col_name order=$order_col_name extra) class_level pheno runifvalue with project

mds_color=color.brewer=Paired

mds_cex=.3


!|expand@;@type;colsarg@top;C1,C2| \
local cmd make_pheno_type_project_mds_pdf_file=$draw_matrix_plot_cmd !{input,,pheno_project_mds_file} !{output,,pheno_type_project_mds_pdf_file} 'Individual Project MDS Values' colsarg sep=$tab cex=$mds_cex class_level pheno with project

!|expand@;@type;colsarg@top;C1,C2@all;`$get_mds_cols(!{prop::pheno:num_mds_plot},\\,,)`| \
local cmd make_pheno_type_mds_pdf_file=$draw_matrix_plot_cmd !{input,,pheno_annotated_mds_file} !{output,,pheno_type_mds_pdf_file} 'Individual MDS Values: !{prop,,pheno,disp}' colsarg color.col=$display_col_name sep=$tab cex=$mds_cex $mds_color class_level pheno 


local cmd make_pheno_cluster_assign_mds_pdf_file=$smart_join_cmd --exec "$smart_cut_cmd --in-delim $tab !{input,--file,pheno_sample_popgen_all_include_file} !{input,--file,pheno_annotated_mds_file} --select-col 2,1 --exclude-row 2,1 | sort | uniq -d | sort - !{input,,pheno_sample_cluster_all_include_file} | uniq -d | sed '1 s/^/ID\n/'" !{input,--file,pheno_annotated_mds_file} --exec "$cluster_phe_file_helper_no_remove" --header 1 --extra 2 --extra 3 --in-delim $tab | $draw_matrix_plot_cmd /dev/stdin !{output,,pheno_cluster_assign_mds_pdf_file} 'Cluster assignments for QC/Popgen+ samples: !{prop,,pheno,disp}' C1,C2 color.col=$display_col_name connect.col=$ibs_cluster_pheno sep=$tab cex=$mds_cex $mds_color class_level pheno 

short cmd make_pheno_genome_pdf_file=zcat !{input,,pheno_annotated_genome_file} | $draw_box_plot_cmd(/dev/stdin !{output::pheno_genome_pdf_file} 'IBD sharing by !{prop::pheno:disp} ---: QC/Popgen+ samples' '' 'PI_HAT' PI_HAT sep=$tab title.newline=: label=$display_col_name order=$order_col_name max.plot.points=1000) class_level pheno skip_if not_trait rusage_mod $genome_pdf_mem

nperm_gassoc=!{raw::burden_test:-1:unless_prop=analytic:allow_empty=1}!{raw::burden_test:0:if_prop=analytic:allow_empty=1}
nperm_pathway=$nperm_gassoc

!!expand:,:assocl,burdenl:assoc,burden:assoc_variant_subset,burden_variant_subset! \
assocl_regex_mask=--mask !{raw,,burdenl,reg.ex\=@,if_prop=min_test_p_missing,if_prop=min_test_p_hwe,if_prop=max_assoc_null,if_prop=custom_burden_test_exclude_filters,or_if_prop=1,allow_empty=1}!{input,,burdenl_regex_file,if_prop=min_test_p_missing,if_prop=min_test_p_hwe,if_prop=max_assoc_null,if_prop=custom_burden_test_exclude_filters,or_if_prop=1,allow_empty=1}

vassoc=v-assoc
assoc_raw=assoc 
!!expand:,:assocl:assoc:assoc_variant_subset! \
assocl=$assoc_raw --tests !{prop::burden_test:test_name} !{prop::burden_test:test_options:if_prop=test_options:allow_empty=1} !{raw,,burden_test,--fix-null,if_prop=fix_null,unless_prop=analytic,allow_empty=1} $assocl_regex_mask

#!{raw,--mask,burden_test,null.prop\=0-@max_assoc_null,unless_prop=analytic,allow_empty=1}

!!expand:alltype:all:unrelated! \
!!expand:filetype:non_missing:basic:popgen:covar:cluster! \
pheno_filetype_alltype_include_mask=--mask indiv=@!{input,,pheno_sample_filetype_alltype_include_file}

!!expand:pheno:pheno:pheno_variant_subset! \
pseq_pheno_project_cmd=$pseq_cmd !{raw,,pheno,*pheno_plinkseq_project_file} !{input,pheno_plinkseq_project_done_file}

!|expand:;:pheno;projectt;includemask:pheno;project;:pheno_variant_subset;project_variant_subset;--mask reg.req=@!{input,,pheno_variant_subset_chr_pos_keep_file}| \
pseq_pheno_analysis_cmd=$pseq_pheno_project_cmd !{input,pheno_plinkseq_db_done_file} !{input,projectt_plinkseq_locdb_file}

!!expand:alltype:all:unrelated! \
!!expand:phenol:pheno:pheno_variant_subset! \
pseq_alltype_analysis_cmd_phenol=$pseq_phenol_analysis_cmd @1 $pheno_basic_alltype_include_mask

!!expand:alltype:all:unrelated! \
!!expand:phenol:pheno:pheno_variant_subset! \
pseq_filter_only_samples_alltype_analysis_cmd_phenol=$pseq_phenol_analysis_cmd @1 $pheno_popgen_alltype_include_mask

!!expand:phenol:pheno:pheno_variant_subset! \
pseq_phenol_clean_project_cmd=$pseq_cmd !{raw,,phenol,*phenol_plinkseq_clean_project_file} !{input,phenol_plinkseq_clean_project_done_file} 

!!expand:,:qc_plus,clean_:qc_plus,clean_:qc_pass,! \
!!expand:alltype:all:unrelated! \
!|expand:;:phenol;projectl;includemask:pheno;project;:pheno_variant_subset;project_variant_subset;--mask reg.req=@!{input,,pheno_variant_subset_chr_pos_keep_file}| \
pseq_qc_plus_alltype_analysis_cmd_phenol=$pseq_phenol_clean_project_cmd !{input,phenol_plinkseq_clean_db_done_file} !{input,projectl_plinkseq_locdb_file} @1 $pheno_popgen_alltype_include_mask includemask $gq_mask

!!expand:phenol:pheno:pheno_variant_subset! \
pseq_phenol_strat_project_cmd=$pseq_cmd !{raw,,phenol,*phenol_plinkseq_strat_project_file} !{input,phenol_plinkseq_strat_project_done_file}
!!expand:,:phenol,projectl:pheno,project:pheno_variant_subset,project_variant_subset! \
pseq_phenol_strat_depends=!{input,phenol_plinkseq_strat_db_done_file} !{input,projectl_plinkseq_locdb_file}

!!expand:alltype:all:unrelated! \
!|expand:;:phenol;projectl;includemask:pheno;project;:pheno_variant_subset;project_variant_subset;--mask reg.req=@!{input,,pheno_variant_subset_chr_pos_keep_file}| \
pseq_qc_plus_alltype_analysis_cmd_phenol_covar=$pseq_phenol_strat_project_cmd $pseq_phenol_strat_depends @1 $pheno_covar_alltype_include_mask includemask $gq_mask

!!expand:alltype:all:unrelated! \
!|expand:;:phenol;projectl;includemask:pheno;project;:pheno_variant_subset;project_variant_subset;--mask reg.req=@!{input,,pheno_variant_subset_chr_pos_keep_file}| \
pseq_qc_plus_alltype_analysis_cmd_phenol_cluster=$pseq_phenol_strat_project_cmd $pseq_phenol_strat_depends @1 $pheno_cluster_alltype_include_mask includemask $gq_mask

!!expand:qc_plus:qc_plus:qc_pass! \
!!expand:alltype:all:unrelated! \
!!expand:phenol:pheno:pheno_variant_subset! \
pseq_qc_plus_alltype_assoc_cmd_phenol=$pseq_qc_plus_alltype_analysis_cmd_phenol(@1) 

!!expand:alltype:all:unrelated! \
!!expand:phenol:pheno:pheno_variant_subset! \
pseq_qc_plus_alltype_assoc_cmd_phenol_cluster=$pseq_qc_plus_alltype_analysis_cmd_phenol_cluster(@1)

!!expand:alltype:all:unrelated! \
!!expand:phenol:pheno:pheno_variant_subset! \
pseq_qc_plus_alltype_assoc_cmd_phenol_covar=$pseq_qc_plus_alltype_analysis_cmd_phenol_covar(@1)

assoc_fix_bug_int=awk -F @1"\t@1" 'NF > 1'
assoc_fix_bug=$assoc_fix_bug_int()

pheno_vassoc_helper_int=$qt_plinkseq_phenotype_selector --perm @1 | $assoc_fix_bug_int(@2)
pheno_vassoc_helper=$pheno_vassoc_helper_int(@1,)
prop not_trait=scalar

!|expand:,:phenol,projectl:pheno,project:pheno_variant_subset,project_variant_subset| \
phenol_vassoc_counts_merge_helper=$pseq_qc_plus_unrelated_analysis_cmd_phenol(v-matrix) $show_id @1 | cut -f1 | sed 's/$/\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA/'

neff_helper=$add_function_cmd --in-delim $tab --header 1 --val1 1 --col2 OBSA  --type divide --val-header OBSA_INV | $add_function_cmd --in-delim $tab --header 1 --val1 1 --col2 OBSU --type divide --val-header OBSU_INV | $add_function_cmd --in-delim $tab --header 1 --col1 OBSA_INV --col2 OBSU_INV --type add --val-header OBS_INV | $add_function_cmd --in-delim $tab --header 1 --val1 4 --col2 OBS_INV --type divide --val-header NEFF --add-at OBSA | $format_columns_cmd --in-delim $tab --header 1 --number-format NEFF,%d | $smart_cut_cmd --tab-delim --exclude-col 0,1,'OBSA_INV OBSU_INV OBS_INV' --exact


!|expand:,:shortt,phenol,projectl,exrunif:,pheno,project,skip_if num_var_subsets:short,pheno_variant_subset,project_variant_subset,skip_if !num_var_subsets| \
shortt cmd make_phenol_vassoc_counts_file=rm -f !{output,,phenol_vassoc_counts_file} && $smart_join_cmd --in-delim $tab --exec "$smart_join_cmd --in-delim $tab --header 1 --extra 2 --col 1,1 --col 1,3 --col 1,4 --col 2,1 --col 2,2 --col 2,3 --ignore-err $plinkseq_okay_err --exec \"$pseq_qc_plus_all_analysis_cmd_phenol($vassoc) $show_id $pheno_vassoc_helper_int(0,\\\) | $smart_cut_cmd --tab-delim --select-col 0,1,'VAR REF ALT SAMPLES FILTER MAF HWE REFA HETA HOMA REFU HETU HOMU P OR' --exact --require-col-match | sed '1 s/^/ID\t/' | sed '1! s/^\([^:][^:]*\):\([^:\.][^:\.]*\)/\1:\2\t\1:\2/'\" --exec \"$smart_cut_cmd --in-delim $tab !{input,--file,phenol_counts_file} --require-col-match --select-col 1,1,'VAR REF/ALT CNTA CNTU TOTA TOTU' | sed 's;/;\t;' | sed '1 s/TOT/OBS/g' | sed '1 s/CNT/MIN/g'\" | $smart_cut_cmd --tab-delim --exact --require-col-match --select-col 0,1,'VAR REF ALT SAMPLES FILTER MAF HWE MINA MINU OBSA OBSU REFA HETA HOMA REFU HETU HOMU P OR' | awk -F\"\t\" -v OFS=\"\t\" 'NR > 1 {\\$10=\\$10/2; \\$11=\\$11/2;} {print}'" --exec "$phenol_vassoc_counts_merge_helper(--mask file=$pseq_snp_tag)" --exec "$phenol_vassoc_counts_merge_helper(--mask file=$pseq_indel_tag)" --exec "$phenol_vassoc_counts_merge_helper(--mask file=$pseq_multiallelic_tag)" --merge --header 1  --ignore-err $plinkseq_okay_err | $neff_helper > !{output,,phenol_vassoc_counts_file} !{input,pheno_is_trait_file} class_level phenol exrunif run_if !not_trait

#strat_cluster
#strata_traits
#strat_covar
#num_mds_covar
#covar_traits
#run_raw
#test_tag
#test_software
#test_name

#min_clean_p_missing
#min_clean_geno
#min_clean_hwe

#fix_null

#make_two_sided
#analytic
#max_assoc_null
#min_test_p_missing
#min_test_hwe

!%expand:;:type;samples:;`cat @5 | wc -l`:_meta;NA% \
record_phenotype_test_info=(echo Test$tab!{raw,,pheno_test,\@test_tag,uc=1} && \
	 echo Software$tab!{raw,,pheno_test,\@test_software} && \
	 echo Test name${tab}@1 !{prop;;pheno_test;test_options;if_prop=test_options;allow_empty=1} && \
	 echo Samples${tab}samples && \
	 echo Covariates${tab}@2 && \
	 echo Clustering${tab}@3 && \
	 echo GT cutoff${tab}@4 && \
	 echo HWE cutoff$tab!{raw,,pheno_test,\@min_clean_hwe} && \
	 echo CR cutoff$tab!{raw,,pheno_test,\@min_clean_geno} && \
	 echo P missing cutoff$tab!{raw,,pheno_test,\@min_clean_p_missing,unless_prop=pheno_qt,allow_empty=1}!{raw,,pheno_test,NA,if_prop=pheno_qt,allow_empty=1} && \
	 echo PI_HAT cutoff$tab!{raw,,pheno_test,\@max_related,unless_prop=include_related,allow_empty=1}!{raw,,pheno_test,1,if_prop=include_related,allow_empty=1} \
	 )> !{output,,pheno_test_info_file}

local cmd make_pheno_pheno_test_info_tex_file=$smart_cut_cmd --in-delim $tab !{input,--file,pheno_test_info_file} --select-col 1,1 --select-col .,2 --paste | $table_to_beamer_cmd --header-rows 1 --header-cols 1 --in-delim $tab --title "Single variant tests" --auto-dim --font-size 8pt > !{output,,pheno_pheno_test_info_tex_file} class_level pheno run_if pheno_test
local cmd make_pheno_pheno_test_info_pdf_file=$run_latex_cmd(pheno_pheno_test_info_tex_file,pheno_pheno_test_info_pdf_file) class_level pheno

#score test commands

#this excludes individuals

!!expand:pheno_test:pheno_test:burden_test! \
local cmd make_pheno_test_pseq_sample_include_aux_file=sort -u !{input,,pheno_test_sample_include_file} | sed 's/\(\S*\S*\)/\1\n\1/' | cat - !{input,,pheno_sample_popgen_unrelated_include_file,unless_prop=include_related,allow_empty=1} !{input,,pheno_sample_popgen_all_include_file,if_prop=include_related,allow_empty=1} | sort | uniq -u  > !{output,,pheno_test_sample_include_aux_file} class_level pheno_test run_if and,pheno_test_sample_include_file,(or,test_software:eq:R,test_software:eq:pseq)

!!expand:pheno_test:pheno_test:burden_test! \
local cmd make_pheno_test_alternate_pheno_file=ln -s !{input,,pheno_non_missing_sample_pheno_file,if_prop=pheno:eq:@alternate_pheno,if_prop=project:eq:@project,all_instances=1,instance_level=pheno} !{output,,pheno_test_alternate_pheno_file} class_level pheno_test run_if alternate_pheno

!!expand:pheno_test:pheno_test:burden_test! \
local cmd make_pheno_test_extra_covars_file=$smart_join_cmd --exec "cat !{input,,pheno_non_missing_sample_pheno_file,if_prop=pheno:eq:@extra_covar_traits,if_prop=project:eq:@project,all_instances=1,allow_empty=1,instance_level=pheno} | cut -f1 | sort | uniq -c | sort -nrk1 | awk 'NR == 1 {n=\\$1} \\$1 == n {print \\$2}' | sed '1 s/^/\\#ID\n/'" !{input,--file,pheno_plinkseq_phe_file_body,if_prop=pheno:eq:@extra_covar_traits,if_prop=project:eq:@project,all_instances=1,allow_empty=1,instance_level=pheno} !{input,--file,pheno_plinkseq_indicator_phe_file_body,if_prop=pheno:eq:@extra_covar_traits,unless_prop=pheno_qt,if_prop=project:eq:@project,all_instances=1,allow_empty=1,instance_level=pheno} --header 1 --rest-extra 1 --in-delim $tab --out-delim $tab > !{output,,pheno_test_extra_covars_file} class_level pheno_test run_if extra_covar_traits

!!expand:pheno_test:pheno_test:burden_test! \
local cmd make_pheno_test_variable_covars_file=$get_variable_covars(pheno_test_extra_covars_file,2) > !{output,,pheno_test_variable_covars_file} class_level pheno_test run_if extra_covar_traits

!!expand:pheno_test:pheno_test:burden_test! \
local cmd ln_pheno_test_variable_covars_file=ln -s !{input,,pheno_variable_covars_file} !{output,,pheno_test_variable_covars_file} class_level pheno_test skip_if extra_covar_traits

!!expand:pheno_test:pheno_test:burden_test! \
pheno_test_covars_aux_helper=$smart_join_cmd --header 1 --exec "tail -qn+2 !{input,,@1} | awk '{print \\$2}' | cat !{input,,pheno_test_extra_covars_file} - | tail -n+2 | awk '{print \\$1}' | sort | uniq -d | sed '1 s/^/ID\n/'" !{input,--file,@1} !{input,--file,pheno_test_extra_covars_file} --col 2,2 --rest-extra 1 | awk '{t=\$1;\$1=\$2;\$2=t} NR == 1 {\$2="IID"} {print}' > !{output,,pheno_test_covars_aux_file}

!!expand:pheno_test:pheno_test:burden_test! \
pheno_test_alternate_pheno_covars_aux_helper=$smart_join_cmd --header 1 --exec "tail -qn+2 !{input,,@1} !{input,,@2} | awk '{print \\$2}' | sort -u | cat !{input,,pheno_test_extra_covars_file,if_prop=extra_covar_traits,allow_empty=1} - | awk '{print \\$1}' | sort | !{raw,,pheno_test,uniq -d |,if_prop=extra_covar_traits,allow_empty=1} $smart_cut_cmd --exec 'cut -f1 !{input,,pheno_test_alternate_pheno_file} | cut -f1 | sort -u' | sort | uniq -d | sed '1 s/^/ID\n/'" --exec "cat !{input,,@1} | sed 's/\s\s*/\t/g' @5 | cut -f1-@3" --exec "sed '1 s/^/ID\t!{prop,,pheno_test,alternate_pheno}\n/' !{input,,pheno_test_alternate_pheno_file}" --exec "sed 's/\s\s*/\t/g' !{input,,@2} | cut -f2,@4-" !{input,--file,pheno_test_extra_covars_file,if_prop=extra_covar_traits,allow_empty=1} --col 2,2 --rest-extra 1 | awk '{t=\$1;\$1=\$2;\$2=t} NR == 1 {\$2="IID"} {print}' > !{output,,pheno_test_covars_aux_file}

!!expand:pheno_test:pheno_test:burden_test! \
local cmd make_pheno_test_R_covars_aux_file=$pheno_test_covars_aux_helper(pheno_plink_covar_file) class_level pheno_test skip_if or,!extra_covar_traits,alternate_pheno run_if or,test_software:eq:R,test_software:eq:plink

!!expand:pheno_test:pheno_test:burden_test! \
local cmd make_pheno_test_R_alternate_phe_covars_aux_file=$pheno_test_alternate_pheno_covars_aux_helper(pheno_plink_phe_file,pheno_plink_covar_file,2,3,| sed '1 s/^/FID\tIID\n/') class_level pheno_test skip_if !alternate_pheno run_if or,test_software:eq:R,test_software:eq:plink

pheno_score_test_helper=$score_test_cmd /dev/stdin -out:!{output,,@1} -pheno:!{input,,pheno_plink_alternate_phe_file,unless_prop=alternate_pheno,allow_empty=1}!{input,,pheno_test_covars_aux_file,if_prop=alternate_pheno,allow_empty=1} -plink:FALSE -model:model1 !{input,pheno_is_trait_file} 

!!expand:pheno_test:pheno_test:burden_test! \
pheno_test_indiv_ex_helper=!{raw,,pheno_test,--mask indiv.ex\=@,if_prop=pheno_test_sample_include_file,allow_empty=1}!{input,,pheno_test_sample_include_aux_file,if_prop=pheno_test_sample_include_file,if_prop=alternate_pheno,or_if_prop=1,allow_empty=1}

!|expand:;:alltype;allrunif:all;include_related:unrelated;!include_related| \
!|expand:,:shortt,pheno_testl,phenol,exrunif:,pheno_test,pheno,num_var_subsets:short,pheno_test_variant_subset,pheno_variant_subset,!num_var_subsets| \
restart_mem shortt cmd make_pheno_testl_r_score_raw_alltype_vassoc_file=$pseq_qc_plus_alltype_analysis_cmd_phenol(v-matrix) $pheno_test_indiv_ex_helper $show_id | $pheno_score_test_helper(pheno_testl_vassoc_file) -phesel:!{prop,,pheno,unless_prop=alternate_pheno,allow_empty=1}!{prop,,pheno_test,alternate_pheno,if_prop=alternate_pheno,allow_empty=1} -convert.pheno:TRUE !{prop;;pheno_test;test_options;if_prop=test_options;allow_empty=1} class_level pheno_testl run_if and,run_raw,test_software:eq:R,test_name:eq:score,allrunif skip_if or,pheno_qt,exrunif rusage_mod $score_test_mem

!|expand:;:alltype;allrunif:all;include_related:unrelated;!include_related| \
local cmd make_pheno_test_r_score_raw_alltype_info_file=$record_pheno_test_info(!{prop;;pheno_test;test_name;sep=\,},no,no,$gq_crit_helper,!{input;;pheno_sample_popgen_alltype_include_file} !{input;;pheno_test_sample_include_aux_file;if_prop=pheno_test_sample_include_file;if_prop=alternate_pheno;or_if_prop=1;allow_empty=1} !{input;;pheno_test_sample_include_aux_file;if_prop=pheno_test_sample_include_file;if_prop=alternate_pheno;or_if_prop=1;allow_empty=1} | sort | uniq -u) class_level pheno_test run_if and,run_raw,test_software:eq:R,test_name:eq:score,allrunif skip_if pheno_qt 

get_plink_covars=$get_covar_traits(\,,@1)!{raw::@1:,:if_prop=covar_traits:if_prop=strat_covar:allow_empty=1}!{raw::@1:,:if_prop=manual_covar_traits:unless_prop=covar_traits:if_prop=strat_covar:allow_empty=1}`!{raw::@1:$get_mds_cols(\@num_mds_covar,\\,):if_prop=strat_covar:allow_empty=1}`
get_disp_covars=$get_covar_traits(\,,@1)!{raw::@1:,:if_prop=covar_traits:if_prop=manual_covar_traits:or_if_prop=1:allow_empty=1}!{raw::@1:\@num_mds_covar:if_prop=strat_covar:allow_empty=1}!{raw::@1:no:unless_prop=strat_covar:allow_empty=1} PCs
get_disp_covars_pheno_test=$get_disp_covars(pheno_test)
get_disp_covars_burden_test=$get_disp_covars(burden_test)

!|expand:;:alltype;allrunif:all;include_related:unrelated;!include_related| \
!|expand:,:shortt,pheno_testl,phenol,exrunif:,pheno_test,pheno,!num_var_subsets:short,pheno_test_variant_subset,pheno_variant_subset,num_var_subsets| \
restart_mem shortt cmd make_pheno_testl_r_score_strata_alltype_vassoc_file=c=$get_plink_covars(pheno_test) && $pseq_qc_plus_alltype_analysis_cmd_phenol_covar(v-matrix) $pheno_test_indiv_ex_helper $show_id | $pheno_score_test_helper(pheno_testl_vassoc_file) -phesel:!{prop,,pheno,unless_prop=alternate_pheno,allow_empty=1}!{prop,,pheno_test,alternate_pheno,if_prop=alternate_pheno,allow_empty=1} -convert.pheno:TRUE -cov:!{input,,pheno_plink_covar_file,unless_prop=extra_covar_traits,allow_empty=1}!{input,,pheno_test_covars_aux_file,if_prop=extra_covar_traits,allow_empty=1} -covsel:\$c !{prop;;pheno_test;test_options;if_prop=test_options;allow_empty=1} class_level pheno_testl run_if and,!run_raw,(or,strat_covar,covar_traits),test_software:eq:R,test_name:eq:score,exrunif,allrunif skip_if pheno_qt rusage_mod $score_test_mem

!|expand:;:alltype;allrunif:all;include_related:unrelated;!include_related| \
local cmd make_pheno_test_r_score_strata_alltype_info_file=$record_pheno_test_info(!{prop;;pheno_test;test_name;sep=\,},$get_disp_covars_pheno_test,no,$gq_crit_helper,!{input;pheno_test_covars_aux_file;if_prop=extra_covar_traits;allow_empty=1} !{raw;;pheno_test;*pheno_test_covars_aux_file | awk '{print \$2}' | cat -;if_prop=extra_covar_traits;allow_empty=1} !{input;;pheno_sample_covar_alltype_include_file} !{raw;;pheno_test;| sort | uniq -d | cat -;if_prop=extra_covar_traits;allow_empty=1} !{input;;pheno_test_sample_include_aux_file;if_prop=pheno_test_sample_include_file;if_prop=alternate_pheno;or_if_prop=1;allow_empty=1} !{input;;pheno_test_sample_include_aux_file;if_prop=pheno_test_sample_include_file;if_prop=alternate_pheno;or_if_prop=1;allow_empty=1} | sort | uniq -u) class_level pheno_test run_if and,!run_raw,(or,strat_covar,covar_traits),test_software:eq:R,test_name:eq:score,allrunif skip_if pheno_qt

!|expand:;:alltype;allrunif:all;include_related:unrelated;!include_related| \
!|expand:,:shortt,pheno_testl,whichvcf,phenol,projectl,exskipif:,pheno_test,project_clean_all_vcf_file,pheno,project,skip_if num_var_subsets:short,pheno_test_variant_subset,pheno_variant_subset_clean_all_vcf_file,pheno_variant_subset,project_variant_subset,skip_if !num_var_subsets| \
shortt cmd make_pheno_testl_r_score_alltype_small_vassoc_file=$smart_join_cmd --in-delim $tab --header 1 --exec "$pseq_qc_plus_alltype_analysis_cmd_phenol_covar(v-freq) $show_id | $smart_cut_cmd --in-delim $tab --select-col 0,1,'VAR MAF REF ALT' --exact --require-col-match" --ignore-err $plinkseq_okay_err !{input,--file,pheno_testl_vassoc_file} --exec "$smart_cut_cmd --in-delim $tab !{input,--file,phenol_vassoc_counts_file} --select-col 1,1,'VAR NEFF'" --rest-extra 1 | sed '1 s/^/CHR:POS:/' | $parse_out_id | $smart_cut_cmd --tab-delim --exact --require-col-match --select-col 0,1 --select-col 0,1,'$n_col_disp NEFF MAF REF ALT beta SEbeta PrChi' | $add_function_cmd --in-delim $tab --header 1 --col1 beta --type exp --val-header $or_col_disp --add-at beta | sed '1 s/\tPrChi/\t$p_col_disp/' | sed '1 s/\tNEFF/\t$neff_col_disp/' | sed '1 s/\tMAF/\t$maf_col_disp/' | sed '1 s/\tREF/\t$ref_col_disp/' | sed '1 s/\tALT/\t$alt_col_disp/' | sed '1 s/\tSEbeta/\t$se_col_disp/' | sed '1 s/\tbeta/\t$beta_col_disp/' | sed '1 s/^\S\S*/$id_col_disp/' | $replace_nan(NA) > !{output,,pheno_testl_small_vassoc_file} class_level pheno_testl run_if and,test_software:eq:R,test_name:eq:score,allrunif exskipif
"""
}
    