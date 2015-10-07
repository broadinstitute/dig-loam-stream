
package loamstream.compiler

/**
 * LoamStream
 * Created by oliverr on 10/6/2015.
 */
object PipelineTargetedPart17 {
  val string =
 """
short cmd make_project_sstats_final_pdf_file=$sstats_pdf_helper(project_sstats_final_pdf_file, $sample_highlight_info exclude.highlighted=TRUE ,QC+ Sample Properties) class_level project
short cmd make_project_sstats_extreme_pdf_file=$sstats_pdf_helper(project_sstats_extreme_pdf_file, $sample_highlight_info exclude.highlighted=TRUE extreme.highlight.col="$all_extreme_highlight_cols" extreme.quantile=$extreme_quantile ,QC+ Sample Properties) class_level project

get_filtered_cols=`$smart_cut_cmd !{input,--file,project_sample_exclude_detail_file} --in-delim , --select-col 1,1,MEASURE --exclude-row 1,1 | sort -u | tr '\n' '\,' | sed 's/\,\$//' | sed 's/^\(.\)/,\1/'`

prop major_sstats_cols=list
default_major_sstats_cols=$default_major_sstats_cols_no_coverage,PCT_BASES_${threshold}x
default_major_sstats_cols_no_coverage=NMIN,SING,HET,HET_HOM,RATE,MEAN_AB,QCFAIL
additional_major_sstats_cols=!{raw,,marker,\,@disp,uc=1,if_prop=is_fingerprint,sep=,allow_empty=1}
all_major_sstats_cols=!{prop,,project,major_sstats_cols,missing_key=default_major_sstats_cols,sep=\,,unless_prop=no_coverage,allow_empty=1}!{prop,,project,major_sstats_cols,missing_key=default_major_sstats_cols_no_coverage,sep=\,,if_prop=no_coverage,allow_empty=1}$additional_major_sstats_cols

prop extreme_highlight_cols=list
all_extreme_highlight_cols=!{prop,,project,extreme_highlight_cols,missing_key=default_major_sstats_cols_no_coverage,sep=\,}


max_sstats_points=1000
max_sstats_highlighted_points=200


slide_sstats_pdf_helper=$draw_box_plot_cmd(!{input\,\,project_sstats_file} !{output\,\,@1} '@3' '' 'Sample Values' "$all_major_sstats_cols$get_filtered_cols" sep=\,@2 max.plot.points=$max_sstats_points max.highlighted.points=$max_sstats_highlighted_points)

slide_vcounts_num_name=Num
slide_vcounts_cut_vcounts_helper=$smart_cut_cmd !{input,--file,@1} --and-row-all --exact --select-row 1,1,@2 --out-delim $tab --require-col-match  | sed 's/@2/$slide_vcounts_num_name @3/'

slide_vcounts_convert_stats_summary_helper=$smart_cut_cmd --in-delim $tab --select-col 0,1,$summary_mean(@1) --select-col 0,1,$summary_dev(@1) --exclude-row 0,1 --require-col-match | $format_columns_cmd --in-delim $tab --number-format @3 --commify 1 --commify 2 | sed 's/\t/ \\$\\\\\\pm\\$ /' | sed 's/^/$slide_vcounts_num_name @2\t/'

slide_vcounts_sum_istats_helper=$table_sum_stats_cmd --summaries --has-header --col @2 --in-delim $tab --out-delim $tab --print-header < !{input,,@1} | $slide_vcounts_convert_stats_summary_helper(@2,@3,@4)
slide_vcounts_sum_istats_helper_int=$slide_vcounts_sum_istats_helper(@1,@2,@3,%.1f)

dbsnp_per_sample_helper=$add_pct_dbsnp_cmd($tab) < !{input,,@1} | $table_sum_stats_cmd --summaries --has-header --col PCT_DBSNP --in-delim $tab --out-delim $tab --print-header | $slide_vcounts_convert_stats_summary_helper(PCT_DBSNP,% dbSNP,%.1f --percentage 1 --percentage 2) | sed 's/^Num\s*//'

#dbsnp fraction now not in pseq anymore
#			 --exec \"$smart_cut_cmd !{input,--file,project_qc_pass_all_vcounts_file} --select-row 1,1,^db[Ss][Nn][Pp].\*PCT$ --and-row-all --select-row 1,2,ALL --exclude-col 1,2 --out-delim $tab | $format_columns_cmd --in-delim $tab --number-format %.1f --percentage 2 | sed 's/db[Ss][Nn][Pp].\*PCT/% dbSNP/'\" \
#			 --exec \"$dbsnp_per_sample_helper(project_plinkseq_qc_pass_all_istats_file)\" \


local cmd make_project_slide_vcounts_tex_file=$smart_cut_cmd \
--exec \
 "$smart_cut_cmd --in-delim $tab --out-delim $tab \
 --exec \"$slide_vcounts_cut_vcounts_helper(project_qc_pass_all_vcounts_file,NVAR,all)\" \
 --exec \"$slide_vcounts_cut_vcounts_helper(project_qc_pass_ns_vcounts_file,NVAR,missense)\" \
 --exec \"$slide_vcounts_cut_vcounts_helper(project_qc_pass_nonsense_vcounts_file,NVAR,nonsense)\" \
 --exec \"$slide_vcounts_cut_vcounts_helper(project_qc_pass_syn_vcounts_file,NVAR,synonymous)\" \
 --exec \"$slide_vcounts_cut_vcounts_helper(project_qc_pass_noncoding_vcounts_file,NVAR,noncoding)\" \
 --exec \"$slide_vcounts_cut_vcounts_helper(project_qc_pass_all_vcounts_file,SING,singletons)\" \
 " \
--exec \
 "$smart_cut_cmd --in-delim $tab --out-delim $tab \
 --exec \"$slide_vcounts_sum_istats_helper_int(project_plinkseq_qc_pass_all_istats_file,NMIN,all)\" \
 --exec \"$slide_vcounts_sum_istats_helper_int(project_plinkseq_qc_pass_ns_istats_file,NMIN,missense)\" \
 --exec \"$slide_vcounts_sum_istats_helper_int(project_plinkseq_qc_pass_nonsense_istats_file,NMIN,nonsense)\" \
 --exec \"$slide_vcounts_sum_istats_helper_int(project_plinkseq_qc_pass_syn_istats_file,NMIN,synonymous)\" \
 --exec \"$slide_vcounts_sum_istats_helper_int(project_plinkseq_qc_pass_noncoding_istats_file,NMIN,noncoding)\" \
 --exec \"$slide_vcounts_sum_istats_helper_int(project_plinkseq_qc_pass_sing_istats_file,NMIN,singletons)\" \
 | $smart_cut_cmd --in-delim $tab --exclude-col 0,1 " \
--paste --in-delim $tab --out-delim $tab  \
| $format_columns_cmd --in-delim $tab --commify 2 | sed '1 s/^/Variant Class\tQC Pass Sites\tPer Sample\n/' | $table_to_beamer_cmd --in-delim $tab --header-cols 1 --header-rows 1 --title "Variant counts"  > !{output,,project_slide_vcounts_tex_file} class_level project

local cmd make_project_slide_vcounts_pdf_file=$run_latex_cmd(project_slide_vcounts_tex_file,project_slide_vcounts_pdf_file) class_level project


slide_var_pass_counts_helper=$smart_cut_cmd !{input,--file,@1} --and-row-all --exact --select-row 1,1,@2 --out-delim $tab | sed 's/@2/$slide_vcounts_num_name @3/'

#slide_var_pass_raw_counts_helper=$pseq_analysis_cmd v-stats @1 | $slide_var_pass_counts_helper(@2,@3)
#slide_var_pass_qc_pass_counts_helper=$pseq_qc_pass_analysis_cmd(v-stats) @1 | $slide_var_pass_counts_helper(@2,@3)
#slide_var_pass_clean_counts_helper=$pseq_qc_plus_analysis_cmd(v-stats) @1 | $slide_var_pass_counts_helper(@2,@3)

local cmd make_project_slide_var_pass_counts_tex_file=$smart_cut_cmd \
--exec \
 "$smart_cut_cmd --in-delim $tab --out-delim $tab \
 --exec \"$slide_var_pass_counts_helper(project_raw_all_vcounts_file,NVAR,all)\" \
 --exec \"$slide_var_pass_counts_helper(project_raw_ns_vcounts_file,NVAR,missense)\" \
 --exec \"$slide_var_pass_counts_helper(project_raw_nonsense_vcounts_file,NVAR,nonsense)\" \
 --exec \"$slide_var_pass_counts_helper(project_raw_syn_vcounts_file,NVAR,synonymous)\" \
 --exec \"$slide_var_pass_counts_helper(project_raw_noncoding_vcounts_file,NVAR,noncoding)\" \
 --exec \"$slide_var_pass_counts_helper(project_raw_all_vcounts_file,SING,singletons)\" \
 " \
--exec \
 "$smart_cut_cmd --in-delim $tab --out-delim $tab \
 --exec \"$slide_var_pass_counts_helper(project_qc_pass_all_vcounts_file,NVAR,all)\" \
 --exec \"$slide_var_pass_counts_helper(project_qc_pass_ns_vcounts_file,NVAR,missense)\" \
 --exec \"$slide_var_pass_counts_helper(project_qc_pass_nonsense_vcounts_file,NVAR,nonsense)\" \
 --exec \"$slide_var_pass_counts_helper(project_qc_pass_syn_vcounts_file,NVAR,synonymous)\" \
 --exec \"$slide_var_pass_counts_helper(project_qc_pass_noncoding_vcounts_file,NVAR,noncoding)\" \
 --exec \"$slide_var_pass_counts_helper(project_qc_pass_all_vcounts_file,SING,singletons)\" \
  | $smart_cut_cmd --in-delim $tab --exclude-col 0,1 \
 " \
--exec \
 "$smart_cut_cmd --in-delim $tab --out-delim $tab \
 --exec \"$slide_var_pass_counts_helper(project_qc_plus_all_vcounts_file,NVAR,all)\" \
 --exec \"$slide_var_pass_counts_helper(project_qc_plus_ns_vcounts_file,NVAR,missense)\" \
 --exec \"$slide_var_pass_counts_helper(project_qc_plus_nonsense_vcounts_file,NVAR,nonsense)\" \
 --exec \"$slide_var_pass_counts_helper(project_qc_plus_syn_vcounts_file,NVAR,synonymous)\" \
 --exec \"$slide_var_pass_counts_helper(project_qc_plus_noncoding_vcounts_file,NVAR,noncoding)\" \
 --exec \"$slide_var_pass_counts_helper(project_qc_plus_all_vcounts_file,SING,singletons)\" \
  | $smart_cut_cmd --in-delim $tab --exclude-col 0,1 \
 " \
--paste --in-delim $tab --out-delim $tab  \
| $format_columns_cmd --in-delim $tab --commify 2 --commify 3 --commify 4 | sed '1 s/^/Variant Class\tRaw\tQC Pass\tQC+\n/' | $table_to_beamer_cmd --in-delim $tab --header-cols 1 --header-rows 1 --title "Variant counts by filtration level"  > !{output,,project_slide_var_pass_counts_tex_file} class_level project

local cmd make_project_slide_var_pass_counts_pdf_file=$run_latex_cmd(project_slide_var_pass_counts_tex_file,project_slide_var_pass_counts_pdf_file) class_level project


#dbsnp for now is defunct
#			 --exec \"$dbsnp_per_sample_helper(project_plinkseq_raw_all_istats_file)\" " \


#right now not computing SINGLETONS for RAW
#because when we do this at sample subset level, need count information project wide
#and don't have this for raw because no plink project built for raw

local cmd make_project_slide_sample_var_pass_counts_tex_file=$smart_cut_cmd \
--exec \
 "$smart_cut_cmd --in-delim $tab --out-delim $tab \
 --exec \"$slide_vcounts_sum_istats_helper_int(project_plinkseq_raw_all_istats_file,NMIN,all)\" \
 --exec \"$slide_vcounts_sum_istats_helper_int(project_plinkseq_raw_ns_istats_file,NMIN,missense)\" \
 --exec \"$slide_vcounts_sum_istats_helper_int(project_plinkseq_raw_nonsense_istats_file,NMIN,nonsense)\" \
 --exec \"$slide_vcounts_sum_istats_helper_int(project_plinkseq_raw_syn_istats_file,NMIN,synonymous)\" \
 --exec \"$slide_vcounts_sum_istats_helper_int(project_plinkseq_raw_noncoding_istats_file,NMIN,noncoding)\" \
 --exec \"$slide_vcounts_sum_istats_helper_int(project_plinkseq_raw_all_istats_file,SING,singletons) | sed 's/\t..*/\tNA/'\" " \
--exec \
 "$smart_cut_cmd --in-delim $tab --out-delim $tab \
 --exec \"$slide_vcounts_sum_istats_helper_int(project_plinkseq_qc_pass_all_istats_file,NMIN,all)\" \
 --exec \"$slide_vcounts_sum_istats_helper_int(project_plinkseq_qc_pass_ns_istats_file,NMIN,missense)\" \
 --exec \"$slide_vcounts_sum_istats_helper_int(project_plinkseq_qc_pass_nonsense_istats_file,NMIN,nonsense)\" \
 --exec \"$slide_vcounts_sum_istats_helper_int(project_plinkseq_qc_pass_syn_istats_file,NMIN,synonymous)\" \
 --exec \"$slide_vcounts_sum_istats_helper_int(project_plinkseq_qc_pass_noncoding_istats_file,NMIN,noncoding)\" \
 --exec \"$slide_vcounts_sum_istats_helper_int(project_plinkseq_qc_pass_sing_istats_file,NMIN,singletons)\" \
 | $smart_cut_cmd --in-delim $tab --exclude-col 0,1 " \
--exec \
 "$smart_cut_cmd --in-delim $tab --out-delim $tab \
 --exec \"$slide_vcounts_sum_istats_helper_int(project_plinkseq_qc_plus_all_istats_file,NMIN,all)\" \
 --exec \"$slide_vcounts_sum_istats_helper_int(project_plinkseq_qc_plus_ns_istats_file,NMIN,missense)\" \
 --exec \"$slide_vcounts_sum_istats_helper_int(project_plinkseq_qc_plus_nonsense_istats_file,NMIN,nonsense)\" \
 --exec \"$slide_vcounts_sum_istats_helper_int(project_plinkseq_qc_plus_syn_istats_file,NMIN,synonymous)\" \
 --exec \"$slide_vcounts_sum_istats_helper_int(project_plinkseq_qc_plus_noncoding_istats_file,NMIN,noncoding)\" \
 --exec \"$slide_vcounts_sum_istats_helper_int(project_plinkseq_qc_plus_sing_istats_file,NMIN,singletons)\" \
 | $smart_cut_cmd --in-delim $tab --exclude-col 0,1 " \
--paste --in-delim $tab --out-delim $tab  \
| sed '1 s/^/Variant Class\tRaw\tQC Pass\tQC +\n/' | $table_to_beamer_cmd --font-size 8pt --in-delim $tab --header-cols 1 --header-rows 1 --title "Variant counts per sample by filtration level"  > !{output,,project_slide_sample_var_pass_counts_tex_file} class_level project

local cmd make_project_slide_sample_var_pass_counts_pdf_file=$run_latex_cmd(project_slide_sample_var_pass_counts_tex_file,project_slide_sample_var_pass_counts_pdf_file) class_level project


no_jexl_name=none
tex_degenerate_selection='$no_jexl_name:$two_fold_degenerate_name:$four_fold_degenerate_name:$non_degenerate_name'
project_slide_titv_helper=$smart_cut_cmd !{input,--file,project_titv_eval_file} --in-delim , --vec-delim : --select-row 1,1,JexlExpression,$tex_degenerate_selection --select-row 1,1 --select-col 1,1,JexlExpression --select-col 1,1,'^tiTvRatio$' --select-col 1,1,Novelty | $smart_cut_cmd --in-delim , --select-row 0,1,Novelty,@1 --select-row 0,1 --exclude-col 0,1,Novelty --exclude-row 0,1

#local cmd make_project_slide_titv_tex_file=$smart_join_cmd \
#--exec "perl -e 'print \"none,1\n$non_degenerate_name,2\n$two_fold_degenerate_name,3\n$four_fold_degenerate_name,4\n\"'" \
#--exec "$project_slide_titv_helper(all)" \
#--exec "$project_slide_titv_helper(known)" \
#--exec "$project_slide_titv_helper(novel)" \
#--arg-delim : --in-delim , --out-delim , | sort -t, -nk2 | $smart_cut_cmd --in-delim , --exclude-col 0,2 | sed 's/^none/All/' | sed '1 s/^/QC Pass Variants,All Sites,Known Sites,Novel Sites\n/' | $format_columns_cmd --in-delim , --number-format %.3f | $table_to_beamer_cmd --in-delim , --header-rows 1 --header-cols 1 --title "Ti/Tv ratios" > !{output,,project_slide_titv_tex_file} class_level project

local cmd make_project_slide_titv_pdf_file=$run_latex_cmd(project_slide_titv_tex_file,project_slide_titv_pdf_file) class_level project


project_slide_theta_helper=$smart_cut_cmd !{input,--file,project_theta_eval_file} --in-delim , --vec-delim : --select-row 1,1,JexlExpression,$tex_degenerate_selection --select-row 1,1 --select-col 1,1,JexlExpression --select-col 1,1,'^totalHet:^thetaRegionNumSites$' --select-col 1,1,Novelty | $smart_cut_cmd --in-delim , --select-row 0,1,Novelty,all --select-row 0,1 --exclude-col 0,1,Novelty 

perl_get_target_length=`cat !{input,,project_target_length_file} | sed 's/\n//'`
perl_get_num_with_degeneracy=$perl_get_target_length \* `perl $targeted_bin_dir/get_fraction_with_degeneracy.pl @1`


#local cmd make_project_slide_theta_tex_file=$smart_join_cmd \
#--exec "perl -e 'print \"type,order\nnone,1\n$non_degenerate_name,2\n$two_fold_degenerate_name,3\n$four_fold_degenerate_name,4\n\"'" \
#--exec "$project_slide_theta_helper" \
#--exec "perl -e 'print \"type,num_bases\nnone,\" . $perl_get_target_length . \"\n$non_degenerate_name,\" . $perl_get_num_with_degeneracy(1) . \"\n$two_fold_degenerate_name,\" . ($perl_get_num_with_degeneracy(2) + $perl_get_num_with_degeneracy(3)) . \"\n$four_fold_degenerate_name,\" . $perl_get_num_with_degeneracy(4) . \"\n\"'" \
#--arg-delim : --in-delim , --out-delim , --header 1 | sort -t, -nk2 | $smart_cut_cmd --in-delim , --exclude-col 0,2 | sed 's/^none/All/' | $add_function_cmd --in-delim , --col1 totalHet --col2 num_bases --type divide --header 1 --val-header pi | $add_function_cmd --in-delim , --col1 thetaRegionNumSites --col2 num_bases --type divide --header 1 --val-header theta | $smart_cut_cmd --in-delim , --select-col 0,1 --select-col 0,1,^pi$ --select-col 0,1,^theta$ | sed '1 s/type/QC Pass Variants/' | sed '1 s/pi/\$\\pi\$/' | sed '1 s/theta/\$\\theta\$/' | awk -v OFS=, -F, '{print \$1,\$3,\$2}' | $format_columns_cmd --in-delim , --number-format %.2e | $table_to_beamer_cmd --in-delim , --header-rows 1 --header-cols 1 --title "Estimates of population level mutation rate" > !{output,,project_slide_theta_tex_file} class_level project

local cmd make_project_slide_theta_pdf_file=$run_latex_cmd(project_slide_theta_tex_file,project_slide_theta_pdf_file) class_level project


local cmd make_project_slide_ref_theta_tex_file=perl -e 'print "Cargill et. al. Variants\t\\$\\theta (\\times 10^{-4})\\$\t\\$\\pi (\\times 10^{-4})\\$\nAll\t5.43\\$\\pm\\$1.36\t5.00\\$\\pm\\$2.38\n$non_degenerate_name\t3.66\\$\\pm\\$0.92\t2.93\\$\\pm\\$1.39\n$two_fold_degenerate_name\t6.85\\$\\pm\\$1.72\t6.27\\$\\pm\\$2.98\n$four_fold_degenerate_name\t9.73\\$\\pm\\$2.46\t11.18\\$\\pm\\$5.31\n"' | $table_to_beamer_cmd --in-delim $tab --header-rows 1 --header-cols 1 --title "Reference estimate of population level mutation rate" > !{output,,project_slide_ref_theta_tex_file} class_level project

local cmd make_project_slide_ref_theta_pdf_file=$run_latex_cmd(project_slide_ref_theta_tex_file,project_slide_ref_theta_pdf_file) class_level project


slide_failures_helper=`cat !{input,,@1} | wc -l` # !{input,,@1} | awk '\''{print \\$1}'\''`
local cmd make_project_slide_failures_tex_file=perl -e '\$failed = $slide_failures_helper(project_failed_sample_list_file); chomp(\$failed); \$passed = $slide_failures_helper(project_passed_sample_list_file); chomp(\$failed); chomp(\$passed); \$qc_fail = $slide_failures_helper(project_sample_seq_exclude_file); chomp(\$qc_fail); \$qc_pass = \$passed - \$qc_fail; chomp(\$passed); \$tot = \$passed + \$failed; \$pct_passed = \$passed / \$tot; \$pct_failed = \$failed / \$tot; \$pct_qc_fail = \$qc_fail / \$tot; \$pct_qc_pass = \$qc_pass / \$tot; print "$total_status_long\t\$tot\t\n$failed_status_long\t\$failed\t\$pct_failed\n$passed_status_long\t\$passed\t\$pct_passed\n$qc_fail_status_long\t\$qc_fail\t\$pct_qc_fail\n$qc_pass_status_long\t\$qc_pass\t\$pct_qc_pass\n"' | $format_columns_cmd --in-delim $tab --out-delim $tab --percentage 3 --number-format 2,%d --number-format 3,%.1f | sed '1! s/\t\(\S*\)$/ (\1)/' | sed '1 s/\t$//' | sed '1 s/^/Samples...\tNumber\n/' | $table_to_beamer_cmd --in-delim $tab --header-rows 1 --header-cols 1 --title "Project wide sample status" > !{output,,project_slide_failures_tex_file} class_level project

local cmd make_project_slide_failures_pdf_file=$run_latex_cmd(project_slide_failures_tex_file,project_slide_failures_pdf_file) class_level project

qc_failures_dummy_col=VALUE

!+expand:;:classlevel;type;extratitle;input_cmd;input_file;extra_run_if:\
pheno;popgen; -- !{prop,,pheno,disp};--exec \"sed '1 s/$/\,$qc_failures_dummy_col/' !{input,,pheno_popgen_exclude_detail_file} | sed '1! s/$/\,0/'\";pheno_popgen_exclude_detail_file;:\
project;seq;;!{input,--file,project_sample_exclude_detail_file};project_sample_exclude_detail_file;+ \
local cmd make_classlevel_slide_type_qc_failures_tex_file=$smart_cut_cmd --exec "$smart_cut_cmd --in-delim , input_cmd --select-col 1,1-3 | $table_sum_stats_cmd --in-delim , --has-header --group-col MEASURE --col $qc_failures_dummy_col --totals --print-header" --exec "tail -n+2 !{input,,input_file} | cut -d, -f1 | sort -u | wc -l | sed 's/^/Total,/'" --select-col 1,1 --select-col 1,1,$summary_num(${qc_failures_dummy_col}) --in-delim , --exclude-row 1,1 | $add_header_cmd "Metric,Samples Removed" | $table_to_beamer_cmd --in-delim , --header-rows 1 --footer-rows 1 --auto-dim --title "Sample QC statisticsextratitle" > !{output,,classlevel_slide_type_qc_failures_tex_file} class_level classlevel extra_run_if

!|expand:;:classlevel;type;extra_run_if:project;seq;:pheno;popgen;skip_if not_trait| \
local cmd make_classlevel_slide_type_qc_failures_pdf_file=$run_latex_cmd(classlevel_slide_type_qc_failures_tex_file,classlevel_slide_type_qc_failures_pdf_file) class_level classlevel extra_run_if

local cmd make_project_slide_var_qc_failures_tex_file=$smart_cut_cmd --exec "cat !{input,,project_variant_exclude_detail_file} | $table_sum_stats_cmd --in-delim $vstats_delim --has-header --group-col MEASURE --col $qc_failures_dummy_col --totals --print-header" --exec "cat !{input,,project_variant_exclude_file} | wc -l | sed 's/^/Total,/'" --select-col 1,1 --select-col 1,1,$summary_num(${qc_failures_dummy_col}) --in-delim , --exclude-row 1,1 --out-delim $tab | $format_columns_cmd --in-delim $tab --commify 2 | sed '1 s/^/Metric\tVariants Removed\n/' | $table_to_beamer_cmd --in-delim $tab --header-rows 1 --footer-rows 1 --title "Variant QC statistics" > !{output,,project_slide_var_qc_failures_tex_file} class_level project
local cmd make_project_slide_var_qc_failures_pdf_file=$run_latex_cmd(project_slide_var_qc_failures_tex_file,project_slide_var_qc_failures_pdf_file) class_level project

slide_genes_helper_above=$slide_genes_helper(@1,@2,${threshold_num_above}@1,${threshold_frac_above}@1,@3,@4)
slide_genes_helper_lte=$slide_genes_helper(@1,@2,${threshold_num_lte}@1,${threshold_frac_lte}@1,@3,@4)

slide_genes_helper=$table_sum_stats_cmd --has-header --col $frac_above_threshold --in-delim , --print-header --summaries --group-col @5 < !{input,,project_@{6}_dist_coverage_dat_file} | $table_sum_stats_cmd --threshold @1 --label @2 --has-header --col $frac_above_threshold_mean --in-delim , --print-header | $smart_cut_cmd --in-delim , --exclude-row 0,1 --select-col 0,1 --select-col 0,1,@3 --select-col 0,1,@4

!!expand:,:type,Type,group_col:gene,Gene,1:exon,Exon,1 --group-col 2:bait,Bait,1! \
local cmd make_project_slide_types_tex_file=$smart_cut_cmd --in-delim , --exec "$slide_genes_helper_above(.99,latex_gt99%,group_col,type)" --exec "$slide_genes_helper_above(.95,latex_gt95%,group_col,type)" --exec "$slide_genes_helper_above(.8,latex_gt80%,group_col,type)" --exec "$slide_genes_helper_above(.5,latex_gt50%,group_col,type)" --exec "$slide_genes_helper_above(.25,latex_gt25%,group_col,type)" --exec "$slide_genes_helper_lte(.25,latex_lt25%,group_col,type)" --exec "$slide_genes_helper_lte(.10,latex_lt10%,group_col,type)" --exec "$slide_genes_helper_lte(.05,latex_lt5%,group_col,type)" | sed 's/^latex_gt/\\$>\\$/' | sed 's/^latex_lt/\\$<\\$/' | sed 's/^\([^,]*\),/\1 bases \\$>\\$${threshold}x,/' | sed '1 s/^/Types with...,\\# types,\% types\n/' | $format_columns_cmd --in-delim , --commify 2 --number-format 3,%.1f --percentage 3 --out-delim $tab | $table_to_beamer_cmd --in-delim $tab --header-cols 1 --header-rows 1 --title "Type coverage statistics" > !{output,,project_slide_types_tex_file} class_level project

!!expand:type:gene:exon:bait! \
local cmd make_project_slide_types_pdf_file=$run_latex_cmd(project_slide_types_tex_file,project_slide_types_pdf_file) class_level project

short cmd make_project_slide_sstats_initial_pdf_file=$slide_sstats_pdf_helper(project_slide_sstats_initial_pdf_file, ,Sample QC Properties) class_level project
short cmd make_project_slide_sstats_highlighted_pdf_file=$slide_sstats_pdf_helper(project_slide_sstats_highlighted_pdf_file, $sample_highlight_info ,Sample Outliers) class_level project
short cmd make_project_slide_sstats_final_pdf_file=$slide_sstats_pdf_helper(project_slide_sstats_final_pdf_file, $sample_highlight_info exclude.highlighted=TRUE ,QC+ Sample Properties) class_level project
short cmd make_project_slide_sstats_extreme_pdf_file=$slide_sstats_pdf_helper(project_slide_sstats_extreme_pdf_file, $sample_highlight_info exclude.highlighted=TRUE extreme.highlight.col="$all_extreme_highlight_cols" extreme.quantile=$extreme_quantile ,QC+ Sample Properties) class_level project

exclude_outlier_raw=$smart_cut_cmd !{input,--file,@1} --select-row 1,1,MEASURE,@2 --select-row 1,1,@3,@4 --and-row-all --in-delim @5 --exact
exclude_outlier_iqr=$exclude_outlier_raw(@1,@2,IQR_DIST,@3,\,)

#properties for filtering
default_concordance_filter_type=VALUE
prop concordance_filter_type=scalar
default_concordance_filter_threshold=le:.8
prop concordance_filter_threshold=scalar

prop max_duplicate=scalar default .9
prop max_related=scalar default .2
#end properties for filtering

exclude_sample_outlier_iqr=$exclude_outlier_iqr(project_sample_outlier_file,@1,@2,\,)
exclude_sample_outlier=$exclude_outlier_raw(project_sample_outlier_file,@1,@2,@3,\,)

prop filter_trait=scalar
prop filter_trait_op=scalar
prop filter_trait_value=scalar

prop filter_metric=scalar
prop filter_metric_type=scalar
prop filter_metric_op=scalar
prop filter_metric_value=scalar

#only apply this as an extended filter (to marker file and to SV/burden tests that request it)
prop is_extended_filter=scalar
prop is_extended_strict_filter=scalar

prop modifier_filter_metric=scalar
#prop modifier_filter_metric_type=scalar
prop modifier_filter_metric_op=scalar
prop modifier_filter_metric_value=scalar

!!expand:whichlevel:sample:pheno! \
local cmd make_whichlevel_qc_filter_trait_exclude_file=$smart_cut_cmd --in-delim $tab !{input,--file,project_traits_file} --exclude-row 1,1 --select-row 1,1,!{prop,,whichlevel_qc_filter,filter_trait},!{prop,,whichlevel_qc_filter,filter_trait_op,missing=eq}:!{prop,,whichlevel_qc_filter,filter_trait_value} --select-col 1,1 --select-col 1,1,!{prop,,whichlevel_qc_filter,filter_trait} > !{output,,whichlevel_qc_filter_trait_exclude_file} class_level whichlevel_qc_filter run_if and,filter_trait,filter_trait_value

!!expand:,:whichlevel,whichexclude:sample,sample:pheno,popgen! \
local cmd make_whichlevel_qc_filter_metric_exclude_file=$exclude_whichexclude_outlier(!{prop\\,\\,whichlevel_qc_filter\\,filter_metric},!{prop\\,\\,whichlevel_qc_filter\\,filter_metric_type},!{prop\\,\\,whichlevel_qc_filter\\,filter_metric_op\\,missing=ge}:!{prop\\,\\,whichlevel_qc_filter\\,filter_metric_value\\,missing=1.5},\,) > !{output,,whichlevel_qc_filter_metric_exclude_file} class_level whichlevel_qc_filter run_if filter_metric

!!expand:whichlevel:sample:pheno! \
local cmd make_whichlevel_qc_filter_exclude_file=$smart_join_cmd --arg-delim : --in-delim , --out-delim , --exec "$smart_cut_cmd !{input,--file,whichlevel_qc_filter_trait_exclude_file} --in-delim $tab --select-col 1,1 | $smart_cut_cmd !{input,--file,whichlevel_qc_filter_metric_exclude_file} --in-delim ,  --select-col 1,1 | sort | uniq -d" !{input,--file,whichlevel_qc_filter_metric_exclude_file} --extra 2 > !{output,,whichlevel_qc_filter_exclude_file} class_level whichlevel_qc_filter run_if and,filter_trait,filter_trait_value

!!expand:whichlevel:sample:pheno! \
local cmd cp_whichlevel_qc_filter_exclude_file=cp !{input,,whichlevel_qc_filter_metric_exclude_file} !{output,,whichlevel_qc_filter_exclude_file} class_level whichlevel_qc_filter skip_if and,filter_trait,filter_trait_value

#add for pca mode: assume samples are disjoint, only use them to add for PCA
prop add_for_pca=scalar
#is fingerprint mode: assume samples have overlaps; compute concordance
prop is_fingerprint=scalar
local cmd make_marker_sample_exclude_detail_file=$exclude_sample_outlier(!{prop\\,\\,marker\\,disp\\,uc=1},!{prop\\,\\,marker\\,concordance_filter_type\\,missing_key=default_concordance_filter_type},!{prop\\,\\,marker\\,concordance_filter_threshold\\,missing_key=default_concordance_filter_threshold}) > !{output,,marker_sample_exclude_detail_file} class_level marker run_if is_fingerprint

local cmd make_project_sample_exclude_detail_file=\ 
  cat /dev/null !{input,,marker_sample_exclude_detail_file,if_prop=is_fingerprint,allow_empty=1}  \
	| $exclude_sample_outlier($custom_exclude_header,VALUE,eq:1) \
	!{raw,|,sample_qc_filter,cat - *sample_qc_filter_exclude_file,if_prop=sample_qc_filter,allow_empty=1} !{input,sample_qc_filter_exclude_file,if_prop=sample_qc_filter,allow_empty=1} \
	| sort -u | $smart_cut_cmd --file !{input,,project_sample_outlier_file} --select-row 1,1 > !{output,,project_sample_exclude_detail_file} class_level project

detail_to_exclude=cat @1 | cut -d, -f1  | tail -n+2 | sort -u

!!expand:,:type,input_exclude_detail_file,extra_run_if:seq,project_sample_exclude_detail_file,! \
local cmd make_project_sample_type_exclude_file=$detail_to_exclude("!{input,,input_exclude_detail_file}") > !{output,,project_sample_type_exclude_file} class_level project extra_run_if

local cmd make_project_sample_exclude_file=cat !{input,,project_sample_seq_exclude_file} | sort -u > !{output,,project_sample_exclude_file} class_level project

local cmd make_project_sample_include_file=sort !{input,,project_passed_sample_list_file} !{input,,project_sample_exclude_file} !{input,,project_sample_exclude_file} | uniq -u > !{output,,project_sample_include_file} class_level project

local cmd make_project_plink_sample_exclude_file=$sample_list_to_plink_sample_list("!{input,--file,project_sample_exclude_file}",0) > !{output,,project_plink_sample_exclude_file} class_level project

!!expand:,:projectt,projecte,skipif,shortt:project,,skip_if num_var_subsets,:project_variant_subset,_project_variant_subset,,short! \
!|expand;,;maskkey,varmask;all,;syn,--mask reg.req=@!{input::project_plinkseq_syn_reg_file};ns,--mask reg.req=@!{input::project_plinkseq_ns_reg_file};nonsense,--mask reg.req=@!{input::project_plinkseq_nonsense_reg_file};noncoding,--mask reg.ex=@!{input::project_plinkseq_coding_reg_file}| \
!!expand:;:tname;runif:raw;:qc_pass;:qc_plus;run_if or,sample_qc_filter,var_qc_filter,project_variant_custom_exclude_file,project_sample_custom_exclude_file! \ 
shortt cmd make_projectt_tname_maskkey_vcounts_file=rm -f !{output,,projectt_tname_maskkey_vcounts_file} && $pseq_tname_analysis_cmdprojecte(v-stats) varmask > !{output,,projectt_tname_maskkey_vcounts_file}  class_level projectt runif skipif

!|expand:,:projectt,projecte,exrunif:project,,run_if !num_var_subsets:project_variant_subset,_project_variant_subset,| \
!!expand;,;maskkey;all;syn;ns;nonsense;noncoding! \
local cmd ln_projectt_qc_plus_maskkey_vcounts_file=rm -f !{output,,projectt_qc_plus_maskkey_vcounts_file} && ln -s !{input,,projectt_qc_pass_maskkey_vcounts_file} !{output,,projectt_qc_plus_maskkey_vcounts_file} class_level projectt skip_if or,sample_qc_filter,var_qc_filter,project_variant_custom_exclude_file,project_sample_custom_exclude_file exrunif

!!expand;,;maskkey,extrarunif;all,;syn,;ns,;nonsense,;noncoding,! \
!!expand:tname:raw:qc_pass:qc_plus! \ 
local cmd make_joined_tname_maskkey_vcounts_file=rm -f !{output,,project_tname_maskkey_vcounts_file} && $smart_join_cmd --out-delim $tab !{input,project_variant_subset_tname_maskkey_vcounts_file} !{raw::project_variant_subset:--exec "$smart_cut_cmd --file *project_variant_subset_tname_maskkey_vcounts_file --exclude-row 1,1,^FILTER"} | $transpose_cmd --in-delim $tab | $smart_cut_cmd --in-delim $tab --exact --require-col-match --select-col 0,1,'NVAR RATE MAC MAF SING MONO TITV TITV_S DP QUAL PASS PASS_S' | $bin_values_cmd --header 1 --min-per-bin -1 --in-delim $tab --num-col NVAR --sum-col NVAR --sum-col SING --sum-col MONO --sum-col DP --sum-col QUAL --mean-col RATE --mean-col MAC --mean-col MAF --mean-col TITV --mean-col TITV_S --mean-col PASS --mean-col PASS_S | $transpose_cmd --in-delim $tab > !{output,,project_tname_maskkey_vcounts_file} class_level project run_if num_var_subsets skip_if and,!sample_qc_filter,!var_qc_filter,!project_variant_custom_exclude_file,!project_sample_custom_exclude_file

low_freq_maf=0.05
rare_maf=0.005

prop one_annotation_per_variant=scalar default 1

"""
}
    