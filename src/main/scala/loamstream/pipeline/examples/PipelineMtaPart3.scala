package loamstream.pipeline.examples

/**
 * LoamStream
 * Created by oliverr on 10/6/2015.
 */
object PipelineMtaPart3 {
  val string =
    """prop ensembl_id=scalar

adjust_exons=awk -F\"\\t\" -v OFS=\"\\t\" 'BEGIN {g=\"\"; cs=@2; ce=@3} g == \"\" || g != \\$2 {s=@1; e=@1} \\$1 == \"UTR\" {g=\\$2; if (\\$cs > e) {s = \\$cs} if (\\$ce > e) {e = \\$ce}} \\$1 == \"exon\" && \\$2 == g {if (\\$cs < e) {\\$cs = e} if (\\$ce < e) {\\$ce = e}} {print}'
invert_exons=awk -F\"\\t\" -v OFS=\"\\t\" 'BEGIN {cs=@1; ce=@2} {\\$cs=-\\$cs; \\$ce=-\\$ce} {print}'

clean_exons=$adjust_exons(0,4,5) | $invert_exons(4,5) | $smart_cut_cmd --in-delim $tab --select-col 0,'1 2 3 5 4' | sort -k2,2 -k3,3 -k4,4g | $adjust_exons(-3e9,4,5) | $invert_exons(4,5) | $smart_cut_cmd --in-delim $tab --select-col 0,'1 2 3 5 4'

!!expand:large:large:medium:small! \
parse_large_transcripts_helper=$smart_join_cmd --in-delim $tab --exec "$smart_cut_cmd !{input,--file,project_large_transcript_gene_file} --select-col 1,@3 @1 | sort -u" --exec "zcat !{input,,project_gencode_gtf_gz_file} | fgrep -v \\\#\\\# | cut -f1,3,4,5,9 | cut -d\; -f1,2 | awk -v OFS=\"\\t\" '\\$2 == \"exon\" || \\$2 == \"UTR\" {print \\$2,\\$6,\\$@4,\\$1,\\$3,\\$4}' | sed 's/\(\"\|;\)//g' | sed 's/\.[0-9]*//g' | $smart_cut_cmd --in-delim $tab --select-col 0,'1 3-6' @2 | sort -u | sort -k2,2 -k3,3 -k4,4g | $clean_exons | $smart_cut_cmd --in-delim $tab --select-row 0,1,exon --select-col 0,2-5 | sort -k1,1 -k2,2 -k3,3 | awk -F\"\\t\" '\\$3 != \\$4'" --extra 2 --multiple 2

!!expand:large:large:medium:small! \
short cmd make_gene_plot_large_transcript_file=$parse_large_transcripts_helper(--select-row 1\,2\,!{prop::gene:ensembl_id},--select-row 0\,2\,!{prop::gene:ensembl_id},1,8) | cut -f1,3- > !{output,,gene_plot_large_transcript_file} class_level gene

!!expand:large:large:medium:small! \
short cmd make_gene_plot_large_variant_file=id="!{prop,,gene,ensembl_id}"; $smart_join_cmd --exec "fgrep -v \\\# !{input,,project_variant_site_vcf_file} | cut -f2-3" --col 1,2 --extra 1 --multiple 2 --exec "$smart_join_cmd !{raw;--exec;mask;\"$smart_cut_cmd --in-delim $tab --file *mask_large_setid_file --select-row 1,1,Gene,\$id --exact --require-col-match --select-col 1,1,'VAR Feature' | sed 's/$/\t@color\t@sort/'\"} --merge --col 1 --col 2" !{input,mask_large_setid_file} | cut -f2- | sort -nrk4 | cut -f1-3 | perl -ne '@f = split("\t"); push(@f,ucfirst(pop(@f))); print join("\t", @f)' > !{output,,gene_plot_large_variant_file} class_level gene

!!expand:large:large:medium:small! \
local cmd make_gene_large_transcripts_tex_file=$transcript_fig_cmd !{input,,gene_plot_large_transcript_file} !{input,,gene_plot_large_variant_file} > !{output,,gene_large_transcripts_tex_file} class_level gene

!!expand:large:large:medium:small! \
local cmd make_gene_large_transcripts_pdf_file=$run_latex_cmd(gene_large_transcripts_tex_file,gene_large_transcripts_pdf_file) class_level gene

count_to_list_helper=awk '{print \$1,1}' | $table_sum_stats_cmd --group-col 1 --totals --col 2 --out-delim $tab --print-header |  $smart_cut_cmd --in-delim $tab --select-col 0,1 --select-col 0,1,'num' --exact --exclude-row 0,1 | sort -gk1 | awk -F"\t" -v OFS="\t" 'BEGIN {n=0} {for (i=n+1;i<\$1;i++) {print i,0} n=\$1; if (n > 0) {print}}' | cut -f2 | $transpose_cmd --in-delim $tab --out-delim , | sed 's/^/[/' | sed 's/$/]/'

item_to_list_helper=sort | uniq -c | sort | $count_to_list_helper

!!expand:large:large:medium:small! \
short cmd make_project_large_transcripts_per_gene_dist_file=cut -f2 !{input,,project_large_transcript_gene_file} | $item_to_list_helper > !{output,,project_large_transcripts_per_gene_dist_file} class_level project

!!expand:large:large:medium:small! \
short cmd make_project_large_transcript_exons_file=$parse_large_transcripts_helper(,,1,8) > !{output,,project_large_transcript_exons_file} class_level project

!!expand:large:large:medium:small! \
short cmd make_project_large_exons_per_transcript_dist_file=$smart_join_cmd --exec "cat !{input,,project_large_transcript_exons_file} | cut -f1 | sort | uniq -c" --exec "$smart_join_cmd !{input,--file,project_large_transcript_gene_file} --exec \"cat !{input,,project_large_gene_exons_file} | cut -f1 | sort | uniq -c\" --col 2 --multiple 1" --col 2 | awk -F"\t" -v OFS="\t" '{print \$3,\$2/\$4}' | $table_sum_stats_cmd --group-col 1 --summaries --col 2 --out-delim $tab --print-header | $smart_cut_cmd --in-delim $tab --exclude-row 0,1 --select-col 0,1,mean | awk '{print int(100 * \$1 + .5)}' | $count_to_list_helper > !{output,,project_large_exons_per_transcript_dist_file} class_level project

!!expand:large:large:medium:small! \
short cmd make_project_large_gene_exons_file=$smart_cut_cmd --in-delim $tab --exec "$smart_join_cmd --in-delim $tab !{input,--file,project_large_transcript_gene_file} !{input,--file,project_large_transcript_exons_file} --extra 2 --multiple 2 | cut -f2- | sed 's/^/exon\t/' | $clean_exons" | cut -f2- | sort -u > !{output,,project_large_gene_exons_file} class_level project

!!expand:large:large:medium:small! \
short cmd make_project_large_exons_per_gene_dist_file=cat !{input,,project_large_gene_exons_file} | cut -f1 | $item_to_list_helper > !{output,,project_large_exons_per_gene_dist_file} class_level project

!!expand:large:large:medium:small! \
short cmd make_project_large_exon_length_dist_file=cut -f2-4 !{input,,project_large_transcript_exons_file} | sort -u | awk '{print \$3-\$2}' | $count_to_list_helper > !{output,,project_large_exon_length_dist_file} class_level project

!!expand:large:large:medium:small! \
short cmd make_mask_large_variants_per_gene_dist_file=cut -f2 !{input,,mask_gene_size_large_most_del_text_file} | tail -n+2 | $count_to_list_helper > !{output,,mask_large_variants_per_gene_dist_file} class_level mask


prop n_samples=scalar
prop n_cases=scalar
prop one_sided=scalar
prop path_frac=scalar

prop environ_var=scalar

prop effect_size_frac=scalar

prop pval=scalar default 1e-4
prop min_var_exp=scalar default 0.0005
prop max_var_exp=scalar default 0.1
prop max_var_var_exp=scalar default 0.001
prop var_exp_bins=list default "0.001 0.002 0.005 0.01"


prop min_skat_perms=scalar default 100
prop max_skat_perms=scalar default 1000

prop variants_per_gene_dist_mask=scalar

prop num_sims=scalar default 1000
prop num_per_batch=scalar default 5

local cmd make_simulation_simulation_batch_meta_file=num_batches=`perl -e 'print int(!{prop,,simulation,num_sims}/!{prop,,simulation,num_per_batch}+.5)'` && (echo "simulation_batch_{1..\$num_batches} class simulation_batch" && echo "!select:!{prop,,project} simulation_batch_{1..\$num_batches} parent !{prop,,simulation}") > !{output,,simulation_simulation_batch_meta_file} class_level simulation

!!expand:large:large:medium:small! \
large_ve_sim_params=!{prop,-pval,simulation,pval} !{prop,-environVar,simulation,environ_var,if_prop=environ_var,allow_empty=1} !{prop,-maxVarExp,simulation,max_var_exp,if_prop=max_var_exp,allow_empty=1} !{prop,-minVarExp,simulation,min_var_exp,allow_empty=1} !{prop,-maxVarVarExp,simulation,max_var_var_exp,if_prop=max_var_var_exp,allow_empty=1} !{raw,-oneSided,simulation,,if_prop=one_sided,allow_empty=1} -effectSizeVar 1 $large_ve_sim_dist_params !{input,-mafEffectSizeFile,simulation_maf_effect_size_file,if_prop=simulation_maf_effect_size_file,allow_empty=1} !{raw,,simulation,-effectSizeConst 1,if_prop=effect_size_frac,allow_empty=1} !{prop,-effectSizeFrac,simulation,effect_size_frac,if_prop=effect_size_frac,allow_empty=1} !{prop,-pathFrac,simulation,path_frac,if_prop=path_frac,allow_empty=1} !{raw,,simulation,-varExpBins '[,if_prop=var_exp_bins,allow_empty=1}!{prop;;simulation;var_exp_bins;sep=,;allow_empty=1}!{raw,,simulation,]',if_prop=var_exp_bins,allow_empty=1} !{input,-variants_per_gene_dist_file,mask_large_variants_per_gene_dist_file,if_prop=mask:eq:@variants_per_gene_dist_mask,allow_empty=1}

!!expand:large:large:medium:small! \
large_ve_sim_dist_params=!{input,-transcripts_per_gene_dist_file,project_large_transcripts_per_gene_dist_file} !{input,-exons_per_transcript_dist_file,project_large_exons_per_transcript_dist_file} !{input,-exons_per_gene_dist_file,project_large_exons_per_gene_dist_file} !{input,-exon_length_dist_file,project_large_exon_length_dist_file}

simulation_intercept_mem=2000

short cmd make_simulation_batch_intercept_data_file=python $bin_dir/veSim2.py -nSamples $((!{prop,,simulation,n_samples}\*2)) !{raw,,simulation,-nCases $((@n_cases\*2)),if_prop=n_cases,allow_empty=1} !{prop,-nRuns,simulation,num_per_batch} $large_ve_sim_params -onlyPath -downSample .1 -downSample .2 -downSample .3 -downSample .4 -downSample .5 -downSample .75 > !{output,,simulation_batch_intercept_data_file} class_level simulation_batch rusage_mod $simulation_intercept_mem

prop num_per_null_batch=scalar default 10

!!expand:large:large:medium:small! \
short cmd make_simulation_batch_large_null_data_file=python $bin_dir/veSim2.py !{prop,-nSamples,simulation,n_samples} !{prop,-nRuns,simulation,num_per_null_batch} !{prop,-nCases,simulation,n_cases,if_prop=n_cases,allow_empty=1} $large_ve_sim_dist_params -nullDist -environVar 1 !{prop,-minSkatPermute,simulation,min_skat_perms,if_prop=min_skat_perms} !{prop,-maxSkatPermute,simulation,max_skat_perms,if_prop=max_skat_perms} > !{output,,simulation_batch_large_null_data_file} class_level simulation_batch

process_aggregated_rows=awk 'FNR == 1 {p=0} p {print} \$1 == "\#N" {p=1}' !{input,,@1} | sed 's/^\\#//'

local cmd make_simulation_intercept_data_file=$process_aggregated_rows(simulation_batch_intercept_data_file) | awk -v OFS="\t" '\$2 == "Pathogenic" {print \$1,\$4}' | $table_sum_stats_cmd --out-delim $tab --col 2 --group-col 1 --summaries --print-header | $smart_cut_cmd --exclude-row 0,1 --select-col 0,1 --select-col 0,1,mean | sed '1 s/^/N\tlogP\n/' > !{output,,simulation_intercept_data_file} class_level simulation

local cmd make_simulation_intercept_files=$r_script_cmd($bin_dir/fit_logp_intercept.R) !{input,,simulation_intercept_data_file} !{output,,simulation_intercept_pdf_file} cex=1.1 height.scale=.8 | tail -n1 > !{output,,simulation_intercept_file} class_level simulation

prop exclude_tails=scalar default .01

!!expand:,:burden,Min_pval,Collapse_pval:burden,Min_pval,Collapse_pval:skat,Min_SKAT_pval,Collapse_SKAT_pval:skat_perm,Min_SKAT_perm_pval,Collapse_SKAT_perm_pval! \
!!expand:large:large:medium:small! \
local cmd make_simulation_large_burden_null_data_file=$process_simulation_batches(simulation_batch_large_null_data_file) | $table_sum_stats_cmd --in-delim $tab --out-delim $tab --has-header --col Min_pval --col Collapse_pval --col NumTranscripts --threshold 0.05 --threshold 0.01 --totals --print-header | $smart_cut_cmd --tab-delim --exact --require-col-match --select-col 0,1,Min_pval_num --select-col 0,1,NumTranscripts_tot --select-col 0,1,Min_pval_frac_lte_0.05 --select-col 0,1,Collapse_pval_frac_lte_0.05 --select-col 0,1,Min_pval_frac_lte_0.01 --select-col 0,1,Collapse_pval_frac_lte_0.01 | $add_function_cmd --in-delim $tab --header 1 --col1 Min_pval_frac_lte_0.05 --col2 Collapse_pval_frac_lte_0.05 --type divide --val-header 0.05_ratio | $add_function_cmd --in-delim $tab --header 1 --col1 Min_pval_frac_lte_0.01 --col2 Collapse_pval_frac_lte_0.01 --type divide --val-header 0.01_ratio | $add_function_cmd --in-delim $tab --header 1 --col1 NumTranscripts_tot --col2 Min_pval_num --type divide --val-header NumTranscripts_ratio > !{output,,simulation_large_burden_null_data_file} class_level simulation

#$process_aggregated_rows(simulation_batch_intercept_data_file) | awk -v OFS="\t" '\$2 == "Pathogenic" {print \$1,\$4}' | $table_sum_stats_cmd --out-delim $tab --col 2 --group-col 1 --summaries --print-header | $smart_cut_cmd --exclude-row 0,1 --select-col 0,1 --select-col 0,1,mean | sed '1 s/^/N\tlogP\n/' > !{output,,simulation_intercept_data_file} class_level simulation

!!expand:large:large:medium:small! \
short cmd make_simulation_batch_large_simulations_output_file=python $bin_dir/veSim2.py !{prop,-nSamples,simulation,n_samples} !{prop,-nRuns,simulation,num_per_batch} !{prop,-nCases,simulation,n_cases,if_prop=n_cases,allow_empty=1} -adjustLogP `cat !{input,,simulation_intercept_file}` $large_ve_sim_params !{prop,-minSkatPermute,simulation,min_skat_perms,if_prop=min_skat_perms} !{prop,-maxSkatPermute,simulation,max_skat_perms,if_prop=max_skat_perms} > !{output,,simulation_batch_large_simulations_output_file} class_level simulation_batch

process_simulation_batches=(fgrep -v \\# !{input,,@1,limit=1} | head -n1; awk 'FNR == 1 {p=0} \$1 ~ "Error" {p=-1} p > 0 && NF > 1 {print} \$1 !~ "^\#" {p++}' !{input,,@1} | fgrep -v \\# | awk -v OFS="\t" 'NF > 0 {print \$14/\$12,\$0}' | sort -gk1 | cut -f2-)

!!expand:large:large:medium:small! \
local cmd make_simulation_large_simulations_output_file=$process_simulation_batches(simulation_batch_large_simulations_output_file) > !{output,,simulation_large_simulations_output_file} class_level simulation

log_p_transform=$add_function_cmd --in-delim $tab --header 1 --col1 @{1}_pval --type minus_log --val-header TEMP_LOG | $add_function_cmd --in-delim $tab --header 1 --col1 TEMP_LOG --val2 `cat !{input,,simulation_intercept_file}` --type subtract --val-header @{1}_NCP | $smart_cut_cmd --in-delim $tab --exclude-col 0,1,'@{1}_pval TEMP_LOG' --exact

#relative_n_mean_transform=$add_function_cmd --in-delim $tab --header 1 --col1 @{1}_NCP_mean --col2 Pathogenic_NCP_mean --type divide --val-header @{1}_mean_relative_N | $add_function_cmd --in-delim $tab --header 1 --col1 @{1}_relative_N_num --type sqrt --val-header @{1}_relative_N_sqrt_num | $add_function_cmd --in-delim $tab --header 1 --col1 @{1}_relative_N_stddev --col2 @{1}_relative_N_sqrt_num --type divide --val-header @{1}_relative_N_stderr --add-at @{1}_relative_N_stddev | $smart_cut_cmd --in-delim $tab --exclude-col 0,1,@{1}_relative_N_sqrt_num | awk -F"\t" -v OFS="\t" 'NR == 1 {for (i=1;i<=NF;i++) {m[\$i]=i} print \$0,"@{1}_relative_N_jacknife","@{1}_relative_N_jacknife_stderr"} NR > 1 {g = \$m["@{1}_relative_N_num"]; mean = g * \$m["@{1}_mean_relative_N"] - (g - 1) * \$m["@{1}_relative_N_mean"]; var = 1} NR > 1 {print \$0,mean,var}'

#aggregate_sims_helper=$log_p_transform(MTA) | $log_p_transform(Bonf) | $log_p_transform(Collapse) | $log_p_transform(Pathogenic) | $log_p_transform(Random_Transcript) | $log_p_transform(Collapse_SKAT) | $log_p_transform(Pathogenic_SKAT) | $log_p_transform(Bonf_SKAT) | $table_sum_stats_cmd --in-delim $tab --out-delim $tab --has-header --group-col N @1 --col 'MTA_NCP' --col 'Bonf_NCP' --col 'Collapse_NCP' --col 'Pathogenic_NCP' --col 'Random_Transcript_NCP' --col 'Collapse_SKAT_NCP' --col 'Pathogenic_SKAT_NCP' --col 'Bonf_SKAT_NCP'  --summaries --totals --print-header | $smart_cut_cmd --in-delim $tab --exclude-col 0,1,'min max tot' | $relative_n_transform(MTA) | $relative_n_transform(Bonf) | $relative_n_transform(Collapse) | $relative_n_transform(Pathogenic) | $relative_n_transform(Random_Transcript) | $relative_n_transform(Collapse_SKAT) | $relative_n_transform(Pathogenic_SKAT) | $relative_n_transform(Bonf_SKAT)

#first_agg_number=40

#aggregate_sims_helper=$log_p_transform(MTA) | $log_p_transform(Bonf) | $log_p_transform(Collapse) | $log_p_transform(Pathogenic) | $log_p_transform(Random_Transcript) | $log_p_transform(Collapse_SKAT) | $log_p_transform(Pathogenic_SKAT) | $log_p_transform(Bonf_SKAT) | awk -F"\t" -v OFS="\t" 'NR == 1 {print "Row",\$0} NR > 1 {print NR % int(!{prop,,simulation,num_sims}/$first_agg_number),\$0;}' | $table_sum_stats_cmd --in-delim $tab --out-delim $tab --has-header --group-col N @1 --group-col Row --col 'MTA_NCP' --col 'Bonf_NCP' --col 'Collapse_NCP' --col 'Pathogenic_NCP' --col 'Random_Transcript_NCP' --col 'Collapse_SKAT_NCP' --col 'Pathogenic_SKAT_NCP' --col 'Bonf_SKAT_NCP' --summaries --print-header | $smart_cut_cmd --in-delim $tab --exclude-col 0,1,'Row stddev' | sed '1 s/_mean//g' | $relative_n_transform(MTA) | $relative_n_transform(Bonf) | $relative_n_transform(Collapse) | $relative_n_transform(Pathogenic) | $relative_n_transform(Random_Transcript) | $relative_n_transform(Collapse_SKAT) | $relative_n_transform(Pathogenic_SKAT) | $relative_n_transform(Bonf_SKAT) | $table_sum_stats_cmd --in-delim $tab --out-delim $tab --has-header --group-col N @1 --col 'MTA_NCP' --col 'Bonf_NCP' --col 'Collapse_NCP' --col 'Pathogenic_NCP' --col 'Random_Transcript_NCP' --col 'Collapse_SKAT_NCP' --col 'Pathogenic_SKAT_NCP' --col 'Bonf_SKAT_NCP' --col 'MTA_relative_N' --col 'Bonf_relative_N' --col 'Collapse_relative_N' --col 'Pathogenic_relative_N' --col 'Random_Transcript_relative_N' --col 'Collapse_SKAT_relative_N' --col 'Pathogenic_SKAT_relative_N' --col 'Bonf_SKAT_relative_N'  --summaries --totals --print-header | $smart_cut_cmd --in-delim $tab --exclude-col 0,1,'min max tot' | $relative_n_mean_transform(MTA) | $relative_n_mean_transform(Bonf) | $relative_n_mean_transform(Collapse) | $relative_n_mean_transform(Pathogenic) | $relative_n_mean_transform(Random_Transcript) | $relative_n_mean_transform(Collapse_SKAT) | $relative_n_mean_transform(Pathogenic_SKAT) | $relative_n_mean_transform(Bonf_SKAT)

#relative_n_mean_transform=$add_function_cmd --in-delim $tab --header 1 --col1 @{1}_NCP_mean --col2 Pathogenic_NCP_mean --type divide --val-header @{1}_mean_relative_N | $add_function_cmd --in-delim $tab --header 1 --col1 @{1}_relative_N_num --type sqrt --val-header @{1}_relative_N_sqrt_num | $add_function_cmd --in-delim $tab --header 1 --col1 @{1}_relative_N_stddev --col2 @{1}_relative_N_sqrt_num --type divide --val-header @{1}_relative_N_stderr --add-at @{1}_relative_N_stddev | $smart_cut_cmd --in-delim $tab --exclude-col 0,1,@{1}_relative_N_sqrt_num | awk -F"\t" -v OFS="\t" 'NR == 1 {for (i=1;i<=NF;i++) {m[\$i]=i} print \$0,"@{1}_relative_N_jacknife","@{1}_relative_N_jacknife_stderr"} NR > 1 {g = \$m["@{1}_relative_N_num"]; mean = g * \$m["@{1}_mean_relative_N"] - (g - 1) * \$m["@{1}_relative_N_mean"]; var = 1} NR > 1 {print \$0,mean,var}'

relative_n_transform=$add_function_cmd --in-delim $tab --header 1 --col1 @{1}_NCP_mean --col2 @{2}_NCP_mean --type divide --val-header @{1}_relative_N_@{2} | awk -F"\t" -v OFS="\t" 'NR == 1 {for (i=1;i<=NF;i++) {m[\$i]=i} print \$0,"@{1}_relative_N_@{2}_corr","@{1}_relative_N_@{2}_stderr"} NR > 1 {r = \$m["@{1}_relative_N_@2"]; n = \$m["@{1}_NCP_num"]; s_x = \$m["@{1}_NCP_stddev"]; s_y = \$m["@{2}_NCP_stddev"]; m_x = \$m["@{1}_NCP_mean"]; m_y = \$m["@{2}_NCP_mean"]; s_xy = \$m["@{1}_NCP_covariance_@{2}_NCP"]; r_corr = r + (1/n) * ((r * s_x * s_x - s_xy) / (m_x * m_x)); var = (1 / n) * ((s_y * s_y)/(m_x * m_x) + (m_y * m_y * s_x * s_x)/(m_x ** 4) - (2 * m_y * s_xy)/(m_x ** 3)); if (var < 0) {var = 0}} NR > 1 {print \$0,r_corr,sqrt(var)}'

aggregate_sims_helper=$log_p_transform(MTA) | $log_p_transform(Bonf) | $log_p_transform(Collapse) | $log_p_transform(Pathogenic) | $log_p_transform(Random_Transcript) | $log_p_transform(Collapse_SKAT) | $log_p_transform(Pathogenic_SKAT) | $log_p_transform(Bonf_SKAT) | $log_p_transform(Random_Transcript_SKAT) | $log_p_transform(Collapse_SKAT_perm) | $log_p_transform(Pathogenic_SKAT_perm) | $log_p_transform(Bonf_SKAT_perm) | $log_p_transform(MTA_SKAT_perm) | $table_sum_stats_cmd --in-delim $tab --out-delim $tab --has-header --group-col N @1 --col 'MTA_NCP' --col 'Bonf_NCP' --col 'Collapse_NCP' --col 'Pathogenic_NCP' --col 'Random_Transcript_NCP' --col 'Collapse_SKAT_NCP' --col 'Pathogenic_SKAT_NCP' --col 'Bonf_SKAT_NCP' --col 'Random_Transcript_SKAT_NCP' --col 'Collapse_SKAT_perm_NCP' --col 'Pathogenic_SKAT_perm_NCP' --col 'Bonf_SKAT_perm_NCP' --col 'MTA_SKAT_perm_NCP' --summaries --totals --covariance 'Pathogenic_NCP' --covariance 'Pathogenic_SKAT_NCP' --covariance 'Pathogenic_SKAT_perm_NCP' --print-header | $smart_cut_cmd --in-delim $tab --exclude-col 0,1,'tot min max' | $relative_n_transform(MTA,Pathogenic) | $relative_n_transform(Bonf,Pathogenic) | $relative_n_transform(Collapse,Pathogenic) | $relative_n_transform(Pathogenic,Pathogenic) | $relative_n_transform(Random_Transcript,Pathogenic) | $relative_n_transform(Collapse_SKAT,Pathogenic) | $relative_n_transform(Pathogenic_SKAT,Pathogenic) | $relative_n_transform(Bonf_SKAT,Pathogenic) | $relative_n_transform(Collapse_SKAT,Pathogenic_SKAT) | $relative_n_transform(Pathogenic_SKAT,Pathogenic_SKAT) | $relative_n_transform(Bonf_SKAT,Pathogenic_SKAT) | $relative_n_transform(Random_Transcript_SKAT,Pathogenic_SKAT) | $relative_n_transform(Collapse_SKAT_perm,Pathogenic_SKAT_perm) | $relative_n_transform(Pathogenic_SKAT_perm,Pathogenic_SKAT_perm) | $relative_n_transform(Bonf_SKAT_perm,Pathogenic_SKAT_perm) | $relative_n_transform(MTA_SKAT_perm,Pathogenic_SKAT_perm) | sed '1 s/Random_Transcript/Random/g'

#aggregate_sims_helper=$log_p_transform(Pathogenic) | $table_sum_stats_cmd --in-delim $tab --out-delim $tab --has-header --group-col N @1 --col 'Pathogenic_NCP' --summaries --totals --covariance 'Pathogenic_NCP' --print-header | $smart_cut_cmd --in-delim $tab --exclude-col 0,1,'tot min max' | $relative_n_transform(Pathogenic,Pathogenic)

!!expand:large:large:medium:small! \
truncate_simulation_large_simulations_output_file=cat !{input,,simulation_large_simulations_output_file} | awk 'NR <= 1 + (1 - !{prop,,simulation,exclude_tails}) * (!{prop,,simulation,num_sims})'


!!expand:large:large:medium:small! \
short cmd make_simulation_large_simulations_aggregated_output_file=$truncate_simulation_large_simulations_output_file | $aggregate_sims_helper() > !{output,,simulation_large_simulations_aggregated_output_file} class_level simulation

!!expand:large:large:medium:small! \
short cmd make_simulation_large_simulations_varexp_aggregated_output_file=$truncate_simulation_large_simulations_output_file | awk -F"\t" -v OFS="\t" '{p = 1} NR > 1 {if (\$2 > 0.002) {\$2="3:>0.002"} else if (\$2 > 0.001) {\$2="2:0.001-0.002"} else if (\$2 > 0) {\$2="1:<0.001"} else {p=0}} p {print}' | $aggregate_sims_helper(--group-col VarianceExplained) > !{output,,simulation_large_simulations_varexp_aggregated_output_file} class_level simulation

!!expand:large:large:medium:small! \
short cmd make_simulation_large_simulations_fold_increase_aggregated_output_file=$truncate_simulation_large_simulations_output_file | awk -F"\t" -v OFS="\t" 'NR == 1 {print \$0,"FoldIncrease"} NR > 1 {p=1; f = \$5/\$4} NR > 1 {if (f > 2.5) {f = ">2.5"} else if (f > 2) {f="2-2.5"} else if (f > 1.5) {f="1.5-2"} else if (f > 1) {f="1-1.5"} else if (f == 1) {p=0} if (p) {print \$0,f}}' | $aggregate_sims_helper(--group-col FoldIncrease) > !{output,,simulation_large_simulations_fold_increase_aggregated_output_file} class_level simulation

!!expand:large:large:medium:small! \
short cmd make_simulation_large_simulations_ntrans_aggregated_output_file=$truncate_simulation_large_simulations_output_file | awk -F"\t" -v OFS="\t" 'NR == 1 {print \$0,"NumTranscripts"} NR > 1 {p=1; f = \$3} NR > 1 {if (f > 5) {f = ">5"} else if (f > 3) {f="4-5"} else if (f > 2) {f="3"} else if (f > 1) {f="2"} else if (f == 1) {p=0} if (p) {print \$0,f}}' | $aggregate_sims_helper(--group-col NumTranscripts) > !{output,,simulation_large_simulations_ntrans_aggregated_output_file} class_level simulation

!!expand:large:large:medium:small! \
short cmd make_simulation_large_simulations_nvar_aggregated_output_file=$truncate_simulation_large_simulations_output_file | awk -F"\t" -v OFS="\t" 'NR == 1 {print \$0,"NumVariants"} NR > 1 {p=1; f = \$4} NR > 1 {if (f > 40) {f = "3:>40"} else if (f > 10) {f="2:10-40"} else if (f > 0) {f="1:<10"} if (p) {print \$0,f}}' | $aggregate_sims_helper(--group-col NumVariants) > !{output,,simulation_large_simulations_nvar_aggregated_output_file} class_level simulation

!!expand:large:large:medium:small! \
short cmd make_simulation_large_simulations_percentile_aggregated_output_file=$truncate_simulation_large_simulations_output_file | awk -F"\t" -v OFS="\t" 'NR == 1 {print -1,\$0} NR > 1 {if (\$13 == 1) {if (\$14 == 1) {r=1} else {r=100000}} else {r= -log(\$14)/-log(\$13)} print r,\$0}' | sort -k1,1g | cut -f2- | awk -F"\t" -v OFS="\t" -v nsim=`$truncate_simulation_large_simulations_output_file | wc -l` 'NR == 1 {print \$0,"Percentile"} NR > 1 {p=1; f = 1 - (NR / (nsim - 1))} NR > 1 {if (f <= .05) {f = "5:<5%"} else if (f <= .1) {f="4:10%-5%"} else if (f <= .2) {f="3:20%-10%"} else if (f <= .3) {f="2:30%-20%"} else if (f <= .5) {f="1:50%-30%"} else {p=0} if (p) {print \$0,f}}' | $aggregate_sims_helper(--group-col Percentile) > !{output,,simulation_large_simulations_percentile_aggregated_output_file} class_level simulation

!!expand:large:large:medium:small! \
short cmd make_simulation_large_simulations_reduced_percentile_aggregated_output_file=$truncate_simulation_large_simulations_output_file | awk -F"\t" -v OFS="\t" 'NR == 1 {print -1,\$0} NR > 1 {if (\$13 == 1) {if (\$14 == 1) {r=1} else {r=100000}} else {r= -log(\$14)/-log(\$13)} print r,\$0}' | sort -k1,1g | cut -f2- | awk -F"\t" -v OFS="\t" -v nsim=`$truncate_simulation_large_simulations_output_file | wc -l` 'NR == 1 {print \$0,"Percentile"} NR > 1 {p=1; f = 1 - (NR / (nsim - 1))} NR > 1 {if (f <= .1) {f = "3:<10%"} else if (f <= .25) {f="2:25%-10%"} else if (f <= .5) {f="1:50%-25%"} else {p=0} if (p) {print \$0,f}}' | $aggregate_sims_helper(--group-col Percentile) > !{output,,simulation_large_simulations_reduced_percentile_aggregated_output_file} class_level simulation

!!expand:large:large:medium:small! \
short cmd make_simulation_large_simulations_top_20_aggregated_output_file=$truncate_simulation_large_simulations_output_file | awk -F"\t" -v OFS="\t" 'NR == 1 {print -1,\$0} NR > 1 {if (\$13 == 1) {if (\$14 == 1) {r=1} else {r=100000}} else {r= -log(\$14)/-log(\$13)} print r,\$0}' | sort -k1,1g | cut -f2- | awk -F"\t" -v OFS="\t" -v nsim=`$truncate_simulation_large_simulations_output_file | wc -l` 'NR == 1 {print \$0,"Percentile"} NR > 1 {p=1; f = 1 - (NR / (nsim - 1))} NR > 1 {if (f <= .2) {f = "20%"} else {p=0} if (p) {print \$0,f}}' | $aggregate_sims_helper(--group-col Percentile) > !{output,,simulation_large_simulations_top_20_aggregated_output_file} class_level simulation

!!expand:large:large:medium:small! \
large_sim_gene_size_helper="$add_function_cmd --in-delim $tab --header 1 --col1 NumCollapseVariants --col2 NumPathVariants --type divide --val-header '$large_disp' --add-at 1 !{input,,simulation_large_simulations_output_file} | cut -f1 | awk -F\"\\t\" -v OFS=\"\\t\" '{print NR-1,\\$1}'"

local cmd make_simulation_gene_size_transcripts_pdf_file=$smart_join_cmd --header 1 --in-delim $tab --exec "awk 'FNR > 1 {print FNR-1}' !{input,,simulation_small_simulations_output_file} !{input,,simulation_medium_simulations_output_file} !{input,,simulation_large_simulations_output_file} | sort -nu | sed '1 s/^/Num\n/'" --exec $small_sim_gene_size_helper --exec $medium_sim_gene_size_helper --exec $large_sim_gene_size_helper --extra 2 --extra 3 --extra 4 --fill 2 --fill 3 --fill 4 | $draw_hist_plot_cmd /dev/stdin !{output,,simulation_gene_size_transcripts_pdf_file} 2,3,4 '' 'Fold-increase in variants between "most deleterious" and pathogenic' colors='$small_color,$medium_color,$large_color' sep=$tab alpha=.4 x.max=!{prop,,project,max_fold_increase} !{prop,breaks=,project,num_fold_breaks,if_prop=num_fold_breaks,allow_empty=1} overlay.density=T overlay.cumulative=T legend.pos=topright log=y inset=0,0.2 cex=1.1 height.scale=.8 class_level simulation

min_sims=5

rel_bar_dat_helper=names="Pathogenic Collapse Bonf MTA Random Pathogenic_SKAT Collapse_SKAT Bonf_SKAT Random_SKAT Pathogenic_SKAT_perm Collapse_SKAT_perm Bonf_SKAT_perm MTA_SKAT_perm"; eval $smart_cut_cmd --tab-delim --select-col .,1 --select-col .,2 @2 --exact --exclude-row .,1 `for f in \$names; do for g in Pathogenic Pathogenic_SKAT Pathogenic_SKAT_perm; do echo --exec "\"$smart_cut_cmd !{input,--file,@1} --tab-delim --exclude-row 1,1,MTA_NCP_num,le:$min_sims | sed 's/^/\$f\\\\\t\$g\\\\\t/'\""; done; done` `i=1; for f in \$names; do for g in Pathogenic Pathogenic_SKAT Pathogenic_SKAT_perm; do echo --select-col \$i,1,"'\${f}_relative_N_\${g} \${f}_relative_N_\${g}_stderr'"; i=$((\$i+1)); done; done` @3 | awk -F"\t" 'NF >= 4' | sed '1 s/^/Criteria\tRelative_to\tRelative_sample_size\tStd_Err\n/' | sed 's/Bonf/Bonferroni/g' | sed '1! s/_/ /g' | awk -F"\t" -v OFS="\t" '{se_col=NF; n_col=NF-1} NR > 1 {if (\$n_col < 0) (\$n_col = 0)} {print}'

!!expand:large:large:medium:small! \
local cmd make_simulation_large_rel_bar_dat_file=$rel_bar_dat_helper(simulation_large_simulations_aggregated_output_file,,) > !{output,,simulation_large_rel_bar_dat_file} class_level simulation

!!expand:,:varexp,VarianceExplained,Variance_Explained,addprocess:varexp,VarianceExplained,Variance_Explained,:fold_increase,FoldIncrease,Fold_Increase,:ntrans,NumTranscripts,Number_of_Transcripts,:nvar,NumVariants,Number_of_Variants,:percentile,Percentile,Percentile,:reduced_percentile,Percentile,Percentile,:top_20,Percentile,Percentile,! \
!!expand:large:large:medium:small! \
local cmd make_simulation_large_varexp_rel_bar_dat_file=$rel_bar_dat_helper(simulation_large_simulations_varexp_aggregated_output_file,--select-col .\,1\,VarianceExplained, | sort -k3\,3 -s) addprocess | sed 's/\t\([0-9]:\)/\t/' | sed '1 s/Relative_to/Relative_to\tVariance_Explained/' > !{output,,simulation_large_varexp_rel_bar_dat_file} class_level simulation

rel_bar_pdf_helper=$smart_cut_cmd --in-delim $tab !{input,--file,@1} --select-row 1,1 --select-row 1,1,"(`echo @3 | sed 's/:/|/g'`)" --select-row 1,2,'@4' --vec-delim : --and-row-all --exact | sed 's/Collapse/Most del./g' | sed 's/\s\s\*perm//' | $draw_bar_plot_cmd /dev/stdin !{output,,@2} "" "Relative sample size" Relative_sample_size Criteria se.col=Std_Err sep=$tab no.legend=T se.cap=T

!!expand:large:large:medium:small! \
!!expand;@;path_collapse@toselect@tocompare@runif;path_collapse@Pathogenic:Collapse@Pathogenic@;all_burden@Pathogenic:Collapse:Bonferroni:MTA@Pathogenic@;burden_skat@Pathogenic:Collapse:Pathogenic SKAT:Collapse SKAT@Pathogenic@;bonf@Pathogenic:Collapse:Bonferroni@Pathogenic@;skat@Pathogenic SKAT perm:Collapse SKAT perm:Bonferroni SKAT perm@Pathogenic SKAT perm@;skat_perm@Pathogenic SKAT perm:Collapse SKAT perm:Bonferroni SKAT perm:MTA SKAT perm@Pathogenic SKAT perm@run_if or,min_skat_perms,max_skat_perms;rand_path@Random:Collapse:Pathogenic@Pathogenic@;rand_path_skat@Random SKAT:Collapse SKAT:Pathogenic SKAT@Pathogenic SKAT@! \
local cmd make_simulation_large_path_collapse_rel_bar_pdf_file=$rel_bar_pdf_helper(simulation_large_rel_bar_dat_file,simulation_large_path_collapse_rel_bar_pdf_file,toselect,tocompare) colors=$large_color width.scale=.75 height.scale=.85 class_level simulation runif

all_rel_bar_pdf_exec_helper=$smart_cut_cmd --in-delim $tab !{input,--file,@1} --select-row 1,1 --select-row 1,1,\"(`echo @2 | sed 's/:/|/g'`)\" --select-row 1,2,'@3' --vec-delim : --and-row-all --exact | sed '1 s/Relative_sample_size/@4/' | sed '1 s/Std_Err/Std_Err_@4/'

#else if ( NR > 1 && new[\$m["Relative_to"]@5] {rel=\$m["Relative_to"]@5; \$m["\$large_disp"] *= (new[rel] / old_large[rel]); \$m["\$medium_disp"] *= (new[rel] / old_medium[rel]); \$m["\$small_disp"] *= (new[rel] / old_small[rel]); } {print}'

all_rel_bar_join_helper=$smart_join_cmd --col 1 --col 2 @4 --in-delim $tab --header 1 --extra 1 --extra 2 --fill 1 --fill 2 --exec "$all_rel_bar_pdf_exec_helper(simulation_small_@{1}rel_bar_dat_file,@2,@3,$small_disp)" --exec "$all_rel_bar_pdf_exec_helper(simulation_medium_@{1}rel_bar_dat_file,@2,@3,$medium_disp)" --exec "$all_rel_bar_pdf_exec_helper(simulation_large_@{1}rel_bar_dat_file,@2,@3,$large_disp) | awk -v OFS=\"\\t\" '{print \\$0,NR}'" | awk -F"\t" -v OFS="\t" '{print \$NF,\$0}' | sort -k1,1n | cut -f2- | rev | cut -f2- | rev | awk -F"\t" -v OFS="\t" 'NR == 1 {for (i=1;i<=NF;i++) {m[\$i]=i}} {if (NR > 1 && \$m["Relative_to"] == "Pathogenic" && (\$m["Criteria"] == "Pathogenic SKAT" || \$m["Criteria"] == "Pathogenic SKAT perm")) {t=\$m["Criteria"]@5; mean=(\$m["$large_disp"]+\$m["$medium_disp"]+\$m["$small_disp"])/3; old_large[t]=\$m["$large_disp"]; old_medium[t]=\$m["$medium_disp"]; old_small[t]=\$m["$small_disp"]; \$m["$large_disp"]=\$m["$medium_disp"]=\$m["$small_disp"]=new[t]=mean} else if ( NR > 1 && (\$m["Criteria"] ~ "SKAT" || \$m["Criteria"] ~ "SKAT perm")) {if (\$m["Criteria"] ~ "SKAT perm") { rel="Pathogenic SKAT perm"@5 } else { rel="Pathogenic SKAT"@5 } if (new[rel]) {\$m["$large_disp"] *= (new[rel] / old_large[rel]); \$m["$medium_disp"] *= (new[rel] / old_medium[rel]); \$m["$small_disp"] *= (new[rel] / old_small[rel]); }}} {print}' | sed 's/Collapse/Most del./g' | sed 's/\s\s\*perm//' | awk -F"\t" -v OFS="\t" 'NR == 1 {for (i=1;i<=NF;i++) {m[\$i]=i}} NR > 1 {if (\$m["$small_disp"] != "NA" && \$m["$large_disp"] != "NA") {\$m["$small_disp"]-=\$m["$large_disp"]} if (\$m["$medium_disp"] != "NA" && \$m["$large_disp"] != "NA") {\$m["$medium_disp"]-=\$m["$large_disp"]} if (\$m["$small_disp"] != "NA" && \$m["$medium_disp"] != "NA") {\$m["$small_disp"]-=\$m["$medium_disp"]}} {print}'


all_rel_bar_pdf_helper=$all_rel_bar_join_helper(@1,@2,@3,@4,@6) | $draw_bar_plot_cmd /dev/stdin !{output,,simulation_@{1}@{5}_rel_bar_pdf_file} "" "@7" "$small_disp,$medium_disp,$large_disp" Criteria sep=$tab colors="$large_color","$medium_color","$small_color" no.legend=T se.col="Std_Err_$small_disp,Std_Err_$medium_disp,Std_Err_$large_disp" se.cap=T no.bar=T pt.cex=1.5

!!expand:,:_no_ylab,ylabel:,Relative sample size:_no_ylab,! \
!!expand;@;path_collapse@toselect@tocompare@runif@widthscale@heightscale;path_collapse@Pathogenic:Collapse@Pathogenic@@.75@.85;all_burden@Pathogenic:Collapse:Bonferroni:MTA@Pathogenic@@.75@.85;burden_skat@Pathogenic:Collapse:Pathogenic SKAT:Collapse SKAT@Pathogenic@@.75@.96;bonf@Pathogenic:Collapse:Bonferroni@Pathogenic@@.75@.85;skat@Pathogenic SKAT perm:Collapse SKAT perm:Bonferroni SKAT perm@Pathogenic SKAT perm@@.75@.96;skat_perm@Pathogenic SKAT perm:Collapse SKAT perm:Bonferroni SKAT perm:MTA SKAT perm@Pathogenic SKAT perm@run_if or,min_skat_perms,max_skat_perms@.75@.96;rand_path@Random:Collapse:Pathogenic@Pathogenic@@.75@.85;rand_path_skat@Random SKAT:Collapse SKAT:Pathogenic SKAT@Pathogenic SKAT@@.75@.96! \
local cmd make_simulation_path_collapse_no_ylab_rel_bar_pdf_file=$all_rel_bar_pdf_helper(,toselect,tocompare,,path_collapse_no_ylab,,ylabel) width.scale=widthscale height.scale=heightscale class_level simulation runif

draw_tradeoff_pdf=$draw_matrix_plot_cmd /dev/stdin !{output,,@1} "" Accuracy,Relative_sample_size x.label="Accuracy of prediction of pathogenic transcript" y.label="@2" header=T text.label.col=Criteria sep=$tab no.legend=T color.col=Color order.col=Color connect.col=Line cex=1.4 axis.cex=1.25 height.scale=.8

!!expand:large:large:medium:small! \
large_tradeoff_pdf_helper=$smart_cut_cmd --in-delim $tab --select-row 0,1 --select-row 0,1,"(`echo @2 | sed 's/:/|/g'`)" --select-row 0,2,'@3' @4 --vec-delim : --and-row-all --exact | awk -F"\t" -v OFS="\t" 'NR == 1 {print "Line","Color","Accuracy",\$0} NR > 1 { if (\$1 ~ "Most del.") {print 0,1,.5,\$0} else {n=\$1; \$1="\"\""; if (n ~ "Pathogenic") {print "$large_color",1,1,\$0} else {print "$large_color",1,0,\$0}}}' | $draw_tradeoff_pdf(@1,Relative sample size) custom.color=$large_color

!!expand:large:large:medium:small! \
!!expand;@;path_collapse@toselect@tocompare@runif;rand_path@Random:Collapse:Pathogenic@Pathogenic@;rand_path_skat@Random SKAT:Collapse SKAT:Pathogenic SKAT@Pathogenic SKAT@! \
local cmd make_simulation_large_path_collapse_tradeoff_pdf_file=cat !{input,,simulation_large_rel_bar_dat_file} | $large_tradeoff_pdf_helper(simulation_large_path_collapse_tradeoff_pdf_file,toselect,tocompare,)  class_level simulation runif

#@4 becomes an argument, replaces Pathogenic in column 2
#Pathogenic gets
#Pass in tocompare

all_tradeoff_pdf_exec_helper=$smart_cut_cmd --in-delim $tab !{input,--file,@1} --select-row 1,1 --select-row 1,1,\"(`echo @2 | sed 's/:/|/g'`)\" --select-row 1,2,'@3' @4 --vec-delim : --and-row-all --exact | awk -v OFS=\"\\t\" -F\"\\t\" 'NR == 1 {print -1,\\$0} NR > 1 {v=10; if (\\$1 ~ \"Pathogenic\") {v=1} else if (\\$1 ~ \"Random\") {v=2} else if (\\$1 ~ \"Collapse\") {v=3} print v,\\$0}' | sort -gk1 | cut -f2- | awk -v OFS=\"\\t\" -F\"\\t\" 'NR == 1 {p=0; r=0; print \"Accuracy\",\\$0} NR > 1 {v=0; if (\\$1 ~ \"Pathogenic\") {p=\\$3; v=1} else if (\\$1 ~ \"Random\") {r=\\$3; v=0} else if (\\$1 ~ \"Collapse\" && p > 0 && r > 0) {if (\\$3 > r) {v=(\\$3 - r)/(p - r)} else {v=0; \\$3=r}} print v,\\$0}' | sed '1 s/^/Line\tColor\t/' | sed '1! s/^/@6\t@5\t/'

all_tradeoff_pdf_helper=$smart_cut_cmd --in-delim $tab --exec "$all_tradeoff_pdf_exec_helper(simulation_small_@{2}rel_bar_dat_file,@3,@4,@5,1,$small_color)" --exec "$all_tradeoff_pdf_exec_helper(simulation_medium_@{2}rel_bar_dat_file,@3,@4,@5,2,$medium_color)" --exec "$all_tradeoff_pdf_exec_helper(simulation_large_@{2}rel_bar_dat_file,@3,@4,@5,3,$large_color)" --exclude-row 2-3,1 | sed 's/Collapse/Most del./g' | sed 's/\s\s\*perm//' | awk -F"\t" -v OFS="\t" 'NR == 1 {print} NR > 1 { if (\$4 ~ "Most del.") {\$1=\$1"no"; print \$0} else {n=\$4; \$4="\"\""; if (n ~ "Pathogenic") {print \$0} else {print \$0}}}' | $draw_tradeoff_pdf(@1,@6) custom.color=$small_color custom.color=$medium_color custom.color=$large_color

!!expand:,:_no_ylab,ylabel:,Relative sample size:_no_ylab, ! \
!!expand;@;path_collapse@toselect@tocompare@runif;rand_path@Random:Collapse:Pathogenic@Pathogenic@;rand_path_skat@Random SKAT:Collapse SKAT:Pathogenic SKAT@Pathogenic SKAT@;rand_path_skat@Random SKAT:Collapse SKAT:Pathogenic SKAT@Pathogenic SKAT@! \
local cmd make_simulation_path_collapse_no_ylab_tradeoff_pdf_file=$all_tradeoff_pdf_helper(simulation_path_collapse_no_ylab_tradeoff_pdf_file,,toselect,tocompare,,ylabel) class_level simulation runif

!!expand:,:varexp,VarianceExplained,Variance_Explained:varexp,VarianceExplained,Variance_Explained:fold_increase,FoldIncrease,Fold_Increase:ntrans,NumTranscripts,Number_of_Transcripts:nvar,NumVariants,Number_of_Variants:percentile,Percentile,Percentile:reduced_percentile,Percentile,Percentile:top_20,Percentile,Percentile! \
!!expand:large:large:medium:small! \
!!expand;@;path_collapse@toselect@tocompare@runif;path_collapse@Pathogenic:Collapse@Pathogenic@;all_burden@Pathogenic:Collapse:Bonferroni:MTA@Pathogenic@;burden_skat@Pathogenic:Collapse:Pathogenic SKAT:Collapse SKAT@Pathogenic@;bonf@Pathogenic:Collapse:Bonferroni@Pathogenic@;skat@Pathogenic SKAT perm:Collapse SKAT perm:Bonferroni SKAT perm@Pathogenic SKAT perm@;skat_perm@Pathogenic SKAT perm:Collapse SKAT perm:Bonferroni SKAT perm:MTA SKAT perm@Pathogenic SKAT perm@run_if or,min_skat_perms,max_skat_perms;rand_path@Random:Collapse:Pathogenic@Pathogenic@;rand_path_skat@Random SKAT:Collapse SKAT:Pathogenic SKAT@Pathogenic SKAT@! \
local cmd make_simulation_large_varexp_path_collapse_rel_bar_pdf_file=$rel_bar_pdf_helper(simulation_large_varexp_rel_bar_dat_file,simulation_large_varexp_path_collapse_rel_bar_pdf_file,toselect,tocompare) group.col='Variance_Explained' colors=$large_color class_level simulation runif

!!expand:,:_no_ylab,ylabel:,Relative sample size:_no_ylab,! \
!!expand:,:varexp,VarianceExplained,Variance_Explained,widthscale:varexp,VarianceExplained,Variance_Explained,.8:fold_increase,FoldIncrease,Fold_Increase,.9:ntrans,NumTranscripts,Number_of_Transcripts,.8:nvar,NumVariants,Number_of_Variants,.8:percentile,Percentile,Percentile,1.22:reduced_percentile,Percentile,Percentile,.8:top_20,Percentile,Percentile,.75! \
!!expand;@;path_collapse@toselect@tocompare@runif@heightscale;path_collapse@Pathogenic:Collapse@Pathogenic@@.85;all_burden@Pathogenic:Collapse:Bonferroni:MTA@Pathogenic@@.85;burden_skat@Pathogenic:Collapse:Pathogenic SKAT:Collapse SKAT@Pathogenic@@.96;bonf@Pathogenic:Collapse:Bonferroni@Pathogenic@@.85;skat@Pathogenic SKAT perm:Collapse SKAT perm:Bonferroni SKAT perm@Pathogenic SKAT perm@@.96;skat_perm@Pathogenic SKAT perm:Collapse SKAT perm:Bonferroni SKAT perm:MTA SKAT perm@Pathogenic SKAT perm@run_if or,min_skat_perms,max_skat_perms@.96;rand_path@Random:Collapse:Pathogenic@Pathogenic@@.85;rand_path_skat@Random SKAT:Collapse SKAT:Pathogenic SKAT@Pathogenic SKAT@@.96! \
local cmd make_simulation_varexp_path_collapse_no_ylab_rel_bar_pdf_file=$all_rel_bar_pdf_helper(varexp_,toselect,tocompare,--col 3,path_collapse_no_ylab,\\":\\"\$m["VarianceExplained"],ylabel) group.col='Variance_Explained' width.scale=widthscale height.scale=heightscale class_level simulation runif

!!expand@;@varexp;VarianceExplained;Variance_Explained;widthscale;addselect@varexp;VarianceExplained;Variance_Explained;2;@fold_increase;FoldIncrease;Fold_Increase;1;--exclude-row 0\,1\,Fold_Increase\,eq:1@ntrans;NumTranscripts;Number_of_Transcripts;1;--exclude-row 0\,1\,Number_of_Transcripts\,eq:1@nvar;NumVariants;Number_of_Variants;1;@percentile;Percentile;Percentile;1;@reduced_percentile;Percentile;Percentile;1;@top_20;Percentile;Percentile;1;! \
!!expand:large:large:medium:small! \
!!expand;@;path_collapse@toselect@tocompare@runif;rand_path@Random:Collapse:Pathogenic@Pathogenic@;rand_path_skat@Random SKAT:Collapse SKAT:Pathogenic SKAT@Pathogenic SKAT@! \
local cmd make_simulation_large_varexp_path_collapse_tradeoff_pdf_file=cat !{input,,simulation_large_varexp_rel_bar_dat_file} | $large_tradeoff_pdf_helper(simulation_large_varexp_path_collapse_tradeoff_pdf_file,toselect,tocompare,addselect) group.col='Variance_Explained' class_level simulation runif

!!expand:large:large:medium:small! \
large_sim_num_transcripts_helper="$smart_cut_cmd --in-delim $tab !{input,--file,simulation_large_simulations_output_file} --select-col 1,1,NumTranscripts | sed '1 s/^/$large_disp\n/' | awk -F\"\\t\" -v OFS=\"\\t\" '{print NR-1,\\$1}'"

local cmd make_simulation_num_gene_transcripts_pdf_file=$smart_join_cmd --header 1 --in-delim $tab --exec "awk 'FNR > 1 {print FNR-1}' !{input,,simulation_small_simulations_output_file} !{input,,simulation_medium_simulations_output_file} !{input,,simulation_large_simulations_output_file} | sort -nu | sed '1 s/\S\S*/Num/'" --exec $small_sim_num_transcripts_helper --exec $medium_sim_num_transcripts_helper --exec $large_sim_num_transcripts_helper --extra 2 --extra 3 --extra 4 --fill 2 --fill 3 --fill 4 | $draw_hist_plot_cmd /dev/stdin !{output,,simulation_num_gene_transcripts_pdf_file} 2,3,4 '' 'Number of transcripts per gene' colors='$small_color,$medium_color,$large_color' sep=$tab alpha=.4 x.max=!{prop,,project,max_num_transcripts} breaks=25 overlay.density=T cex=1.1 height.scale=.8 class_level simulation

!!expand:large:large:medium:small! \
short cmd make_burden_test_large_initial_set_chrpos_file=$smart_join_cmd --in-delim $tab --exec "cut -f1 !{input,,burden_test_gene_path_trans_list_file}" --exec "$smart_join_cmd --in-delim $tab !{input,--file,project_gene_name_map_file} --exec \"tail -n+2 !{input,,mask_large_set_chrpos_file}\" --extra 1 --multiple 2 | cut -f2- | $smart_cut_cmd --in-delim $tab !{input,--file,mask_large_set_chrpos_file} --exclude-row 1,1" --extra 2 --multiple 2 --fill 2 | awk -F"\t" '\$2 != "NA"' | $smart_cut_cmd --in-delim $tab !{input,--file,project_extended_chrpos_exclude_file} | awk -F"\t" 'NF == 2 {m[\$1":"\$2]=1} NF > 2 && !m[\$3":"\$4] {print}' > !{output,,burden_test_large_initial_set_chrpos_file} class_level burden_test

!!expand:large:large:medium:small! \
local cmd make_burden_test_large_set_chrpos_file=cat !{input,,burden_test_large_initial_set_chrpos_file} | $smart_cut_cmd --in-delim $tab --exec "$smart_join_cmd --in-delim $tab --exec \"cut -f2 !{input,,burden_test_gene_path_trans_list_file} | sort -u\" !{input,--file,burden_test_large_initial_set_chrpos_file} --col 2,2 --extra 2 --fill 2 --multiple 2 | cut -f2,3-4 | cat - !{input,,burden_test_large_initial_set_chrpos_file} | awk -v OFS=\"\\t\" -F\"\\t\" 'NF == 3 {m[\\$1\":\"\\$2\":\"\\$3]=1} NF == 4 && m[\\$1\":\"\\$3\":\"\\$4] != 1 {m[\\$1\":\"\\$3\":\"\\$4] = 1; print \\$1,\"Non-Pathogenic\",\\$3,\\$4}'" > !{output,,burden_test_large_set_chrpos_file} class_level burden_test

gassoc_sig=1e-6

prop dichotomous=scalar

!!expand:large:large:medium:small! \
short cmd make_burden_test_large_gassoc_file=python $bin_dir/mta.py !{input,-vcf,project_variant_vcf_file} !{input,-w,burden_test_large_set_chrpos_file} !{input,-c,burden_test_ped_file} `for i in 1 2 3 4 5 6 7 8 9 10; do echo -cNames C\$i; done` !{input,-p,burden_test_ped_file} -pCol 7 -s $gassoc_sig !{raw,-l,burden_test,Logistic,if_prop=dichotomous,allow_empty=1} -pr 0.01 > !{output,,burden_test_large_gassoc_file} class_level burden_test

adjust_flat_gassoc_header=sed '1 s/AsymP/P/' | sed '1 s/BetaMax/BETA/' | sed '1 s/\t\(\S\S*\)/\t@{1}_\1/g'

!!expand:large:large:medium:small! \
filter_large_gassoc_file_helper=$smart_cut_cmd --in-delim $tab --exec \"sed 's/Bonferonni/Bonferroni/' !{input,,burden_test_large_gassoc_file}\" --select-col 1,1,'Gene AsymP BetaMax' --select-row 1,1 --select-row 1,1,Test,@1 | $adjust_flat_gassoc_header(@1)

!!expand:large:large:medium:small! \
local cmd make_burden_test_large_flat_gassoc_file=$smart_join_cmd --in-delim $tab --exec "$smart_join_cmd --in-delim $tab --exec \"cat !{input,,burden_test_gene_path_trans_list_file} | sed 's/\t/\tTranscript_/' | sed '1 s/^/Gene\tTranscript\tExpDir\tSource\n/'\" !{input,--file,burden_test_large_gassoc_file} --col 1 --col 2 --header 1 --extra 2 --fill 2 | $smart_cut_cmd --in-delim $tab --select-col 0,1,'Gene ExpDir Source AsymP BetaMax' | $adjust_flat_gassoc_header(Pathogenic)" --exec "$filter_large_gassoc_file_helper(Transcript_Non-Pathogenic) | sed '1 s/Transcript_//g'" --exec "$filter_large_gassoc_file_helper(Collapse)" --exec "$filter_large_gassoc_file_helper(Bonferroni)" --exec "$filter_large_gassoc_file_helper(MTA)" --header 1 --extra 2 --extra 3 --extra 4 --extra 5 --fill 2 --fill 3 --fill 4 --fill 5 > !{output,,burden_test_large_flat_gassoc_file} class_level burden_test

binom_test_helper=$smart_cut_cmd !{input,--file,burden_test_large_flat_gassoc_file} --in-delim $tab --select-col 1,1,'Pathogenic_ExpDir Pathogenic_BETA Non-Pathogenic_BETA' @1 | sed 's/nan/NA/g' | awk -F\"\\t\" -v OFS=\"\\t\" 'NR > 1 {for (i=2; i<=3; i++) {if (\\$i != \"NA\") { if (\\$1 == \"-\") {\\$i *= -1;} \\$i /= (\\$i > 0 ? \\$i : -\\$i)}}} {print \\$2,\\$3}' | $table_sum_stats_cmd --in-delim $tab --out-delim $tab --col Pathogenic_BETA --col Non-Pathogenic_BETA --threshold 0 --totals --print-header --has-header | $smart_cut_cmd --in-delim $tab --exact --require-col-match --select-col 0,1,'Pathogenic_BETA_num_above_0 Pathogenic_BETA_num_lte_0 Pathogenic_BETA_num Non-Pathogenic_BETA_num_above_0 Non-Pathogenic_BETA_num_lte_0 Non-Pathogenic_BETA_num' | $add_function_cmd --in-delim $tab --header 1 --col1 Pathogenic_BETA_num_above_0 --col2 Pathogenic_BETA_num --type binom_test --val-header Pathogenic_p_value --add-at Non-Pathogenic_BETA_num_above_0 | $add_function_cmd --in-delim $tab --header 1 --col1 Non-Pathogenic_BETA_num_above_0 --col2 Non-Pathogenic_BETA_num --type binom_test --val-header Non-Pathogenic_p_value | $smart_cut_cmd --in-delim $tab --select-col 0,1,'Pathogenic_BETA_num_above_0 Pathogenic_BETA_num_lte_0 Pathogenic_p_value Non-Pathogenic_BETA_num_above_0 Non-Pathogenic_BETA_num_lte_0 Non-Pathogenic_p_value' --exact --require-col-match | $format_columns_cmd --in-delim $tab --header 1 --number-format 'Pathogenic_p_value Non-Pathogenic_p_value','%.3f'

!!expand:large:large:medium:small! \
local cmd make_burden_test_large_path_non_sign_comp_file=$smart_cut_cmd --in-delim $tab --exec "$binom_test_helper() | sed '1 s/^/Genes\t/' | sed '1! s/^/All\t/'" --exec "$binom_test_helper(--select-row 1\,1 --select-row 1\,1\,Pathogenic_BETA\,gt:0) | tail -n+2 | sed 's/^/Pathogenic_BETA_gt_0\t/'" > !{output,,burden_test_large_path_non_sign_comp_file} class_level burden_test


#COMMANDS
#===================="""


}

