
package loamstream.pipeline.examples

/**
 * LoamStream
 * Created by oliverr on 10/6/2015.
 */
object PipelineTargetedPart19 {
  val string =
 """local cmd make_project_gene_gc_pdf_file=$draw_matrix_plot_cmd !{input,,project_gstats_file} !{output,,project_gene_gc_pdf_file} 'GC metrics' GC,PCT_BASES_${threshold}x,VAR_PER_BP sep=, exclude.outliers=10 do.ipairs=TRUE class_level project skip_if no_coverage

#Args
plink_bed_hidden_in_or_out=!{@1,@{2}_bed_file} !{@1,@{2}_bim_file} !{@1,@{2}_fam_file}
plink_in_or_out_bed_helper=!{raw,--@1,project,*@{2}_plink_file} $plink_bed_hidden_in_or_out(@3,@2)
plink_out_bed_helper=$plink_in_or_out_bed_helper(out,@1,output)
plink_in_bed_helper=$plink_in_or_out_bed_helper(bfile,@1,input)

marker_sync_ref_alt_helper=$sync_ref_alt_cmd !{input,--ref-vcf,project_snp_id_site_vcf_file} !{input,--ref-vcf,project_indel_id_site_vcf_file} !{input,--sync-bim,@1} --remove-discordant --remove-multiple-in-ref --remove-multiple-in-sync

short cmd make_marker_initial_filtered_plink_file=$plink_cmd !{input,--bed,marker_initial_bed_file} !{input,--bim,marker_initial_bim_file} !{input,--fam,marker_initial_fam_file} !{input,--keep,marker_sample_keep_file,if_prop=marker_sample_keep_file,allow_empty=1} !{input,--exclude,marker_snp_exclude_file,if_prop=marker_snp_exclude_file,allow_empty=1} --make-bed $plink_out_bed_helper(marker_initial_filtered) && $plink_mv_log_cmd(!{raw\,\,marker\,*marker_initial_filtered_plink_file},!{output\,\,marker_initial_filtered_make_bed_log_file}) class_level marker run_if or,marker_sample_keep_file,marker_snp_exclude_file

local cmd ln_marker_initial_filtered_plink_file=ln -s !{input,,marker_initial_bed_file} !{output,,marker_initial_filtered_bed_file} && ln -s !{input,,marker_initial_bim_file} !{output,,marker_initial_filtered_bim_file} && ln -s !{input,,marker_initial_fam_file} !{output,,marker_initial_filtered_fam_file} class_level marker skip_if or,marker_sample_keep_file,marker_snp_exclude_file class_level marker

short cmd make_marker_strand_discordant_file=$marker_sync_ref_alt_helper(marker_initial_filtered_bim_file) --write-excluded > !{output,,marker_strand_discordant_file} class_level marker

short cmd make_marker_project_non_overlap_file=cat !{input,,project_snp_id_site_vcf_file} !{input,,project_snp_id_site_vcf_file} !{input,,project_indel_id_site_vcf_file} !{input,,project_indel_id_site_vcf_file} | fgrep -v '\#' | awk -F"\t" '{print \$3,\$1,\$2}' | $smart_cut_cmd !{input,--file,marker_initial_filtered_bim_file} --select-col 1,'2 1 4' | sort -k2,2 -k3,3 | uniq -u -f1 | awk '{print \$1}' > !{output,,marker_project_non_overlap_file} class_level marker

short cmd make_marker_initial_corrected_bim_file=$marker_sync_ref_alt_helper(marker_initial_filtered_bim_file) --write-corrected-file > !{output,,marker_initial_corrected_bim_file} class_level marker

marker_initial_corrected_inputs=!{input,--bed,marker_initial_filtered_bed_file} !{input,--bim,marker_initial_corrected_bim_file} !{input,--fam,marker_initial_filtered_fam_file}
marker_initial_snp_corrected_inputs=!{input,--bed,marker_initial_filtered_bed_file} !{input,--bim,marker_initial_snp_corrected_bim_file} !{input,--fam,marker_initial_filtered_fam_file}

cmd make_marker_sample_strand_filtered_overlap_plink_files=cat !{input,,marker_project_non_overlap_file} !{input,,marker_strand_discordant_file} | sort -u | $plink_cmd $marker_initial_corrected_inputs !{input,--keep,marker_project_sample_include_file} --exclude /dev/stdin --make-bed $plink_out_bed_helper(marker_sample_strand_filtered_overlap) && $plink_mv_log_cmd(!{raw\,\,marker\,*marker_sample_strand_filtered_overlap_plink_file},!{output\,\,marker_sample_strand_filtered_overlap_make_bed_log_file}) class_level marker

!!expand:,:stype,skipif,shortt:,skip_if num_var_subsets,:_variant_subset,,short! \
shortt cmd make_markerstype_initial_diff_file=$plink_cmd $plink_in_bed_helper(marker_sample_strand_filtered_overlap) --bmerge !{input,,projectstype_plinkseq_qc_pass_bed_file} !{input,,projectstype_plinkseq_qc_pass_bim_file} !{input,,projectstype_plinkseq_qc_pass_fam_file} !{input,--keep,marker_project_sample_include_file} !{input,--exclude,marker_strand_discordant_file} --merge-mode 6 !{raw,--out,markerstype,*markerstype_initial_filtered_plink_file} !{output,markerstype_initial_diff_file} && $plink_mv_log_cmd(!{raw\,\,markerstype\,*markerstype_initial_filtered_plink_file},!{output\,\,markerstype_initial_diff_log_file}) class_level markerstype skipif run_if !add_for_pca

local cmd make_empty_marker_initial_diff_file=echo "SNP FID IID NEW OLD"  > !{output,,marker_initial_diff_file} class_level marker run_if add_for_pca

plink_geno_no_call=0/0

!!expand:,:vtype,skipif,shortt:,skip_if num_var_subsets,:_variant_subset,,short! \
shortt cmd make_markervtype_initial_snp_pct_discordant_file=f=`cat !{input,,marker_initial_filtered_fam_file} | awk '{print \$2}' | sort -u | cat - !{input,,project_passed_sample_list_file} | sort | uniq -d | wc -l` && perl -lape '\$r = reverse \$F[4]; s/$/ \$r/' !{input,,markervtype_initial_diff_file} | tail -n+2 | awk -v OFS="\t" '{missing=1; dis=0; swap=0} \$4 != "$plink_geno_no_call" && \$5 != "$plink_geno_no_call" {missing=0} \$4 != \$5 && !missing {dis=1} \$4 == \$6 && dis {swap=1} {print \$1,missing,dis,swap}' | $add_header_cmd SNP${tab}MISSING${tab}DIS${tab}SWAP | $table_sum_stats_cmd --in-delim $tab --out-delim $tab --has-header --group-col SNP --col MISSING --col DIS --col SWAP --totals --print-header | $smart_cut_cmd --in-delim $tab --select-col 0,1 --select-col 0,1,'$summary_tot(MISSING) $summary_tot(DIS) $summary_tot(SWAP)' | tail -n+2 | sed "s/\$/\t\$f/" | awk -v OFS="\t" '{t=\$5-\$2;} t > 0 {print \$1,\$3/t,(\$3 - \$4)/t} t == 0 {print \$1,0,0}' | $add_header_cmd SNP${tab}PCT_DIS${tab}PCT_DIS_SWAP  > !{output,,markervtype_initial_snp_pct_discordant_file} class_level markervtype skipif run_if !add_for_pca

local cmd make_empty_marker_initial_snp_pct_discordant_file=echo | $add_header_cmd SNP${tab}PCT_DIS${tab}PCT_DIS_SWAP  > !{output,,marker_initial_snp_pct_discordant_file} class_level marker run_if add_for_pca

cmd make_cat_marker_initial_snp_pct_discordant_file=(head -qn+1 !{input,,marker_variant_subset_initial_snp_pct_discordant_file,limit=1,sort_prop=marker_variant_subset} | head -n1 && tail -qn+2 !{input,,marker_variant_subset_initial_snp_pct_discordant_file,sort_prop=marker_variant_subset}) > !{output,,marker_initial_snp_pct_discordant_file} class_level marker run_if num_var_subsets skip_if add_for_pca

marker_snp_discordant_threshold=.1

!!expand:,:keytype,ineq:discordant,>:flipped,<=! \
local cmd make_marker_snp_keytype_file=awk 'NR > 1 && \$2 > $marker_snp_discordant_threshold && \$3 ineq $marker_snp_discordant_threshold {print \$1}' !{input,,marker_initial_snp_pct_discordant_file} > !{output,,marker_snp_keytype_file} class_level marker

local cmd make_marker_initial_snp_corrected_bim_file=$marker_sync_ref_alt_helper(marker_initial_corrected_bim_file) --write-corrected-file !{input,--flip-snp-list,marker_snp_flipped_file} > !{output,,marker_initial_snp_corrected_bim_file} class_level marker

local cmd make_marker_project_sample_include_file=cat !{input,,project_sample_marker_keep_file} !{input,marker_initial_fam_file,if_prop=add_for_pca,allow_empty=1} !{raw,,marker,| cat - *marker_initial_fam_file | sort -u,if_prop=add_for_pca,allow_empty=1} > !{output,,marker_project_sample_include_file} class_level marker

short cmd make_marker_filtered_plink_files=cat !{input,,marker_snp_discordant_file} !{input,,marker_strand_discordant_file} | $plink_cmd $marker_initial_snp_corrected_inputs --exclude /dev/stdin --make-bed $plink_out_bed_helper(marker_filtered) && $plink_mv_log_cmd(!{raw\,\,marker\,*marker_filtered_plink_file},!{output\,\,marker_filtered_make_bed_log_file}) class_level marker


marker_ld_helper=$plink_cmd $plink_in_bed_helper(@1) --r2 --ld-window-r2 .7 --ld-window-kb 1000 --ld-window 99999 !{raw,--out,@4,*@{1}_plink_file} !{output,@2} && $plink_mv_log_cmd(!{raw\,\,@4\,*@{1}_plink_file},!{output\,\,@3})

short cmd make_marker_filtered_frq_file=$plink_cmd $plink_in_bed_helper(marker_filtered) --freq !{output,--out,marker_filtered_frq_file} !{output,marker_filtered_frq_log_file} && mv !{output,,marker_filtered_frq_file}.frq !{output,,marker_filtered_frq_file} class_level marker

#short cmd make_marker_filtered_for_merge_ld_file=$marker_ld_helper(marker_filtered_for_merge,marker_filtered_for_merge_ld_file,marker_filtered_for_merge_ld_log_file,marker) class_level marker

prop max_freq_flip_maf=scalar default .35
prop remove_ambiguous=scalar

!!expand:,:ftype,cmdtype:initial_bim,write-corrected-file:exclude,write-excluded! \
short cmd make_marker_filtered_for_merge_ftype_file=$sync_ref_alt_cmd !{input,--sync-bim,marker_filtered_bim_file} --remove-discordant --cmdtype !{input,--ref-bim,marker_filtered_bim_file,all_instances=1,if_prop=project:eq:@project} !{input,--sync-freq,marker_filtered_frq_file} !{prop,--max-freq-flip-maf,marker,max_freq_flip_maf,if_prop=max_freq_flip_maf,allow_empty=1} !{raw,,marker,--remove-ambiguous,if_prop=remove_ambiguous,allow_empty=1} > !{output,,marker_filtered_for_merge_ftype_file} class_level marker


short cmd make_marker_filtered_for_merge_plink_files=$plink_cmd !{input,--bed,marker_filtered_bed_file} !{input,--bim,marker_filtered_for_merge_initial_bim_file} !{input,--fam,marker_filtered_fam_file} !{input,--exclude,marker_filtered_for_merge_exclude_file} --make-bed $plink_out_bed_helper(marker_filtered_for_merge) && $plink_mv_log_cmd(!{raw\,\,marker\,*marker_filtered_for_merge_plink_file},!{output\,\,marker_filtered_for_merge_make_bed_log_file}) class_level marker

#short cmd make_marker_filtered_for_merge_exclude_file=$sync_ref_alt_cmd !{input,--sync-bim,marker_filtered_bim_file} --write-excluded --remove-ambiguous | $smart_cut_cmd --exec "cat !{input,,marker_filtered_bim_file} | $smart_cut_cmd --select-col 0,2 |  > !{output,,marker_filtered_for_merge_exclude_file} class_level marker


short cmd make_marker_sample_filtered_snp_include_file=$add_gene_annot_cmd --outside-name $outside_gene_name !{input,--gene-file,project_expanded_exon_target_file} --gene-file-num-ids 2 --out-delim $tab --chr-col 1 --pos-col 4 < !{input,,marker_initial_snp_corrected_bim_file} | cut -f3 | sort - !{input,,marker_snp_discordant_file} !{input,,marker_strand_discordant_file} !{input,,marker_snp_discordant_file} !{input,,marker_strand_discordant_file} | uniq -u  > !{output,,marker_sample_filtered_snp_include_file} class_level marker

clean_geno=.1
clean_mind=0.1
clean_hwe=0.001
clean_filters=--hwe $clean_hwe --mind $clean_mind --geno $clean_geno

cmd make_marker_sample_filtered_vcf_file=$plink108_cmd $marker_initial_snp_corrected_inputs $clean_filters !{input,--keep,marker_project_sample_include_file} !{input,--extract,marker_sample_filtered_snp_include_file} --recode-vcf !{raw,--out,marker,*marker_sample_filtered_plink_file} !{output,marker_sample_filtered_vcf_file} && $plink_mv_log_cmd(!{raw\,\,marker\,*marker_sample_filtered_plink_file},!{output\,\,marker_sample_filtered_recode_vcf_log_file}) class_level marker

fix_vcf_file=perl -lane 'BEGIN {open IN, "!{input,,@1}" or die; while (<IN>) {\@cols = split; \$vcfid2id{"\$cols[0]_\$cols[1]"} = \$cols[1];}} \@cols = split; if (/^\\#CHROM/) {for (\$i=9;\$i<scalar(\@cols);\$i++) {\$new_id = \$vcfid2id{\$cols[\$i]}; die "No sample \$cols[\$i]" unless \$new_id; \$cols[\$i] = \$new_id}} print join("\t", \@cols)'

short cmd make_marker_sample_filtered_fixed_vcf_file=$fix_vcf_file(marker_initial_filtered_fam_file) < !{input,,marker_sample_filtered_vcf_file} | awk -F"\t" '/^\#/ || (\$4 ~ /[ACTG]/ && \$5 ~ /[ACTG]/)' | $vcf_utils_cmd !{input,--reference-vcf,project_snp_id_site_vcf_file} !{input,--reference-vcf,project_indel_id_site_vcf_file} !{input,--chr-pos-keep,marker_sample_filtered_vcf_file} --remove-non-biallelic --valid-ref-alt > !{output,,marker_sample_filtered_fixed_vcf_file} class_level marker

!!expand:stype::_sample_subset! \
markerstype_eval_helper=$gatk_cmd_no_interval(GenotypeConcordance) -moltenize -l OFF @1 !{output,-o,@2} !{input,--eval,projectstype_vcf_file} !{input,--comp,@3} 

!!expand:,:stype,runif,shortt:,skip_if num_samp_subsets,:_sample_subset,,short! \
shortt cmd make_markerstype_positive_control_eval_file=$markerstype_eval_helper(,markerstype_positive_control_eval_file,marker_sample_filtered_fixed_vcf_file) class_level markerstype runif run_if !add_for_pca

cmd make_cat_marker_positive_control_eval_file=(head -qn+1 !{input,,marker_sample_subset_positive_control_eval_file,limit=1,sort_prop=marker_sample_subset} | head -n1 && tail -qn+2 !{input,,marker_sample_subset_positive_control_eval_file,sort_prop=marker_sample_subset}) | grep -v "\\#" > !{output,,marker_positive_control_eval_file} class_level marker run_if num_samp_subsets run_if !add_for_pca

#eval_parser_cmd=perl $targeted_bin_dir/eval_table_to_delim_file.pl --out-delim ,

#local cmd make_marker_positive_control_genotype_concordance_file=$eval_parser_cmd --analysis-name "GenotypeConcordance" --table-name "detailedStats" < !{input,,marker_positive_control_eval_file} > !{output,,marker_positive_control_genotype_concordance_file} class_level marker

local cmd make_marker_positive_control_genotype_concordance_file=grep -v "\\#" !{input,,marker_positive_control_eval_file} > !{output,,marker_positive_control_genotype_concordance_file} class_level marker

row_col=6

subset_genotype_concordance_file=$smart_cut_cmd !{input,--file,marker_positive_control_genotype_concordance_file} | awk 'NR == 1 {print \"Sample\",\"variable\",\"value\"} NR > 1 {print \\$1,\"n_true_\"\\$3\"_called_\"\\$2,\\$4}'
process_genotype_concordance_file=$smart_cut_cmd --out-delim $tab --exec "$subset_genotype_concordance_file" --select-col 1,1,'Sample variable value' --select-row 1,1 --select-row 1,1,variable,n_true_HET_called_HET --select-row 1,1,variable,n_true_HOM_VAR_called_HOM_VAR --select-row 1,1,variable,n_true_HOM_REF_called_HOM_REF --select-row 1,1,variable,n_true_HET_called_HOM_REF --select-row 1,1,variable,n_true_HET_called_HOM_VAR --select-row 1,1,variable,n_true_HOM_VAR_called_REF --select-row 1,1,variable,n_true_HOM_VAR_called_HET --select-row 1,1,variable,n_true_HOM_REF_called_HOM_VAR --select-row 1,1,variable,n_true_HOM_REF_called_HET --exclude-row 1,1,Sample,^all$

short cmd make_marker_positive_control_genotype_concordance_sum_tex_file=$process_genotype_concordance_file | $table_sum_stats_cmd --has-header --summaries --col value --group-col variable --print-header --out-delim , | $smart_cut_cmd --in-delim , --select-col 0,1,'variable value_mean' | tail -n+2 | sed 's;n_true_HET_called_HET;Het/Het;' | sed 's;n_true_HET_called_HOM_REF;Het/Ref;' | sed 's;n_true_HET_called_HOM_VAR;Het/Hom;' | sed 's;n_true_HOM_REF_called_HET;Ref/Het;' | sed 's;n_true_HOM_REF_called_HOM_REF;Ref/Ref;' | sed 's;n_true_HOM_REF_called_HOM_VAR;Ref/Hom;' | sed 's;n_true_HOM_VAR_called_HET;Hom/Het;' | sed 's;n_true_HOM_VAR_called_HOM_VAR;Hom/Hom;' | $format_columns_cmd --in-delim , --number-format 2,"%.1f" | $add_header_cmd "!{prop,,marker,disp}/Seq,Avg Per Sample" | $table_to_beamer_cmd --row-emph 2 --row-emph 5 --row-emph 9 --in-delim , --header-rows 1 --header-cols 1 --title "!{prop,,marker,disp}" > !{output,,marker_positive_control_genotype_concordance_sum_tex_file} class_level marker

local cmd make_marker_positive_control_genotype_concordance_sum_pdf_file=$run_latex_cmd(marker_positive_control_genotype_concordance_sum_tex_file,marker_positive_control_genotype_concordance_sum_pdf_file) class_level marker

short cmd make_marker_sample_concordance_file=$process_genotype_concordance_file | sed 's/\(n_true_HET_called_HET\|n_true_HOM_VAR_called_HOM_VAR\|n_true_HOM_REF_called_HOM_REF\)\(\s\s*\)\(\S\S*\)$/\1\2\3\2\3\20/' | sed 's/\(n_true_HET_called_HOM_REF\|n_true_HET_called_HOM_VAR\|n_true_HOM_VAR_called_REF\|n_true_HOM_VAR_called_HET\|n_true_HOM_REF_called_HOM_VAR\|n_true_HOM_REF_called_HET\)\(\s\s*\)\(\S\S*\)$/\1\2\3\20\2\3/' | sed '1 s/$/ concordant discordant/' | $table_sum_stats_cmd --out-delim , --totals --col concordant --col discordant --has-header --group-col Sample --print-header | $smart_cut_cmd --in-delim , --select-col 0,1,'Sample concordant_tot discordant_tot' | $add_function_cmd --in-delim , --header 1 --type add --col1 concordant_tot --col2 discordant_tot --val-header "total" | $add_function_cmd --in-delim , --header 1 --type divide --col1 concordant_tot --col2 total --val-header "!{prop,,marker,disp,uc=1}" | $smart_cut_cmd --no-vec-delim --select-col 0,1,Sample --select-col 0,1,"!{prop,,marker,disp,uc=1}" --in-delim , --out-delim $tab > !{output,,marker_sample_concordance_file} class_level marker

local cmd make_marker_sample_full_concordance_file=$smart_cut_cmd --in-delim $tab --exec "tail -n+2 !{input,,marker_sample_concordance_file}" --exec "tail -n+2 !{input,,marker_sample_concordance_file}" | cut -f1 | sort - !{input,,project_passed_sample_list_file} | uniq -u | sed 's/$/\tNA/' | cat !{input,,marker_sample_concordance_file} - > !{output,,marker_sample_full_concordance_file} class_level marker
#files for merging the marker files

prop no_marker=scalar

meta_table cmd make_project_all_marker_plink_list_file=!{input,--bed,marker_filtered_for_merge_bed_file,unless_prop=no_marker,allow_empty=1}\t!{input,--bim,marker_filtered_for_merge_bim_file,unless_prop=no_marker,allow_empty=1}\t!{input,--fam,marker_filtered_for_merge_fam_file,unless_prop=no_marker,allow_empty=1} !{output,project_all_marker_plink_list_file} class_level project run_if marker

meta_table cmd make_project_all_marker_all_merge_list_file=!{input,,marker_filtered_for_merge_bed_file,unless_prop=no_marker,allow_empty=1}\t!{input,,marker_filtered_for_merge_bim_file,unless_prop=no_marker,allow_empty=1}\t!{input,,marker_filtered_for_merge_fam_file,unless_prop=no_marker,allow_empty=1} !{output,project_all_marker_all_merge_list_file,unless_prop=no_marker,allow_empty=1} class_level project run_if marker

local cmd make_project_all_marker_merge_list_file=tail -n+2 !{input,,project_all_marker_all_merge_list_file} > !{output,,project_all_marker_merge_list_file} class_level project run_if marker

cmd make_project_all_marker_plink_file=rm -f !{output,,project_all_marker_bed_file} !{output,,project_all_marker_bim_file} !{output,,project_all_marker_fam_file} && $plink_cmd `head -n1 !{input,,project_all_marker_plink_list_file}` !{input,--merge-list,project_all_marker_merge_list_file} !{input,marker_filtered_for_merge_bed_file} !{input,marker_filtered_for_merge_bim_file} !{input,marker_filtered_for_merge_fam_file} --make-bed $plink_out_bed_helper(project_all_marker) && $plink_mv_log_cmd(!{raw\,\,project\,*project_all_marker_plink_file},!{output\,\,project_all_marker_make_bed_log_file}) class_level project run_if marker rusage_mod $all_marker_mem

#short cmd make_project_all_marker_ld_file=$marker_ld_helper(project_all_marker,project_all_marker_ld_file,project_all_marker_ld_log_file,project) class_level project rusage_mod $all_marker_mem

local cmd make_project_sample_marker_keep_file=$sample_list_to_plink_sample_list(!{input\,--file\,project_passed_sample_list_file},0) > !{output,,project_sample_marker_keep_file} class_level project

prop use_marker_for_sample_ibd=scalar

!|expand:;:name;command;extrarunif:sample;keep;run_if and,marker,use_marker_for_sample_ibd,!project_sample_marker_bed_file,!project_sample_marker_bim_file,!project_sample_marker_fam_file:extra;remove;run_if marker| \
cmd make_project_name_marker_plink_file=rm -f !{output,,project_name_marker_bed_file} !{output,,project_name_marker_bim_file} !{output,,project_name_marker_fam_file} && $plink_cmd $plink_in_bed_helper(project_all_marker) --make-bed $plink_out_bed_helper(project_name_marker) !{input,--command,project_sample_marker_keep_file} && $plink_mv_log_cmd(!{raw\,\,project\,*project_name_marker_plink_file},!{output\,\,project_name_marker_make_bed_log_file}) class_level project extrarunif rusage_mod $all_marker_mem

prop exclude_indels_from_marker=scalar

cmd make_filtered_project_sample_marker_plink_file=rm -f !{output,,project_sample_marker_bed_file} !{output,,project_sample_marker_bim_file} !{output,,project_sample_marker_fam_file} && !{raw,,project,fgrep -v \\# *project_indel_id_site_vcf_file | cut -f3 |,if_prop=exclude_indels_from_marker,allow_empty=1} cat !{raw,,project,/dev/null,unless_prop=exclude_indels_from_marker,allow_empty=1} | $plink_cmd $plink_in_bed_helper(project_plinkseq_qc_plus) --make-bed $plink_out_bed_helper(project_sample_marker) --exclude /dev/stdin && $plink_mv_log_cmd(!{raw;;project;*project_sample_marker_plink_file},!{output;;project_sample_marker_make_bed_log_file}) class_level project rusage_mod $all_marker_mem skip_if or,use_marker_for_sample_ibd,project_sample_marker_bed_file run_if or,var_qc_filter;is_extended_filter,var_qc_filter;is_extended_strict_filter

local cmd ln_project_sample_marker_bed_file=rm -f !{output,,project_sample_marker_bed_file} && ln -s !{input,,project_plinkseq_qc_plus_bed_file} !{output,,project_sample_marker_bed_file} class_level project skip_if or,use_marker_for_sample_ibd,project_sample_marker_bed_file,var_qc_filter;is_extended_filter,var_qc_filter;is_extended_strict_filter

local cmd ln_project_sample_marker_bim_file=rm -f !{output,,project_sample_marker_bim_file} && ln -s !{input,,project_plinkseq_qc_plus_bim_file} !{output,,project_sample_marker_bim_file} class_level project skip_if or,use_marker_for_sample_ibd,project_sample_marker_bim_file,var_qc_filter;is_extended_filter,var_qc_filter;is_extended_strict_filter

local cmd ln_project_sample_marker_fam_file=rm -f !{output,,project_sample_marker_fam_file} && ln -s !{input,,project_plinkseq_qc_plus_fam_file} !{output,,project_sample_marker_fam_file} class_level project skip_if or,use_marker_for_sample_ibd,project_sample_marker_fam_file,var_qc_filter;is_extended_filter,var_qc_filter;is_extended_strict_filter

local cmd make_project_sample_marker_seq_snp_file=cat !{input,,project_sample_marker_bim_file} !{input,,project_plinkseq_qc_plus_bim_file} | awk '{print \$1,\$4,\$2}' | sort -ns -k1,1 -k2,2 | awk '{print \$3,\$1,\$2}' | uniq -d -f1 | awk '{print \$1}' > !{output,,project_sample_marker_seq_snp_file} class_level project

prop has_non_seq=scalar

cmd make_project_sample_non_seq_plink_files=$plink_cmd $plink_in_bed_helper(project_sample_marker) !{input,--exclude,project_sample_marker_seq_snp_file} --make-bed $plink_out_bed_helper(project_sample_non_seq) && $plink_mv_log_cmd(!{raw\,\,project\,*project_sample_non_seq_plink_file},!{output\,\,project_sample_non_seq_make_bed_log_file}) class_level project run_if and,marker,has_non_seq

cmd make_project_sample_combined_plink_files=rm -f !{output,,project_sample_combined_bed_file} !{output,,project_sample_combined_bim_file} !{output,,project_sample_combined_fam_file} && $plink_cmd $plink_in_bed_helper(project_sample_non_seq) !{input,--bmerge,project_plinkseq_qc_plus_bed_file}  !{input,,project_plinkseq_qc_plus_bim_file} !{input,,project_plinkseq_qc_plus_fam_file} --merge-mode 5 --make-bed $plink_out_bed_helper(project_sample_combined) && $plink_mv_log_cmd(!{raw\,\,project\,*project_sample_combined_plink_file},!{output\,\,project_sample_combined_merge_log_file}) class_level project run_if marker

local cmd ln_project_sample_combined_bed_file=rm -f !{output,,project_sample_combined_bed_file} && ln -s !{input,,project_plinkseq_qc_plus_bed_file} !{output,,project_sample_combined_bed_file} class_level project skip_if marker

local cmd ln_project_sample_combined_bim_file=rm -f !{output,,project_sample_combined_bim_file} && ln -s !{input,,project_plinkseq_qc_plus_bim_file} !{output,,project_sample_combined_bim_file} class_level project skip_if marker

local cmd ln_project_sample_combined_fam_file=rm -f !{output,,project_sample_combined_fam_file} && ln -s !{input,,project_plinkseq_qc_plus_fam_file} !{output,,project_sample_combined_fam_file} class_level project skip_if marker


prop marker_autosomes_only=scalar default 1

local cmd make_project_region_exclude_file=$smart_cut_cmd --file /dev/null !{input,--file,project_marker_custom_region_exclude_file,if_prop=project_marker_custom_region_exclude_file,allow_empty=1} !{raw,,project,--exec "echo X 1 1000000000 X && echo Y 1 1000000000 Y",if_prop=marker_autosomes_only,allow_empty=1} > !{output,,project_marker_region_exclude_file} class_level project

prop marker_maf=scalar default .05
prop marker_geno=scalar default .1
prop marker_hwe=scalar default .001

!!expand:projectl:project:pheno! \
projectl_marker_basic_filters=!{prop:--maf:projectl:marker_maf} !{prop:--geno:projectl:marker_geno} !{prop:--hwe:projectl:marker_hwe} --range !{input,--exclude,project_marker_region_exclude_file}

!!expand:projectl:project:pheno! \
projectl_prune_filters=!{input,--extract,projectl_@{1}_marker_ld_prune_in_range_file} --range !{input,--exclude,project_marker_region_exclude_file}

!!expand:project:project:pheno! \
project_marker_filters=$project_marker_basic_filters !{input,--extract,project_@{1}_marker_ld_prune_in_range_file}

prop ld_r2_value=scalar default .2

!!expand:project:project:pheno! \
project_ld_prune_indep_filter=--indep-pairwise 50 5 !{prop,,project,ld_r2_value}

short cmd make_project_sample_marker_input_frq_file=$smart_join_cmd !{input,--file,project_plinkseq_qc_plus_combined_frq_file} --exec "$smart_cut_cmd --out-delim $tab --in-delim , !{input,--file,project_plinkseq_qc_plus_strata_frq_summary_file} --select-col 1,1,SNP --select-col 1,1,MAF_!{prop,,project,marker_strat_maf_summary}" --header 1 --col 1,2 | $smart_cut_cmd --in-delim $tab --select-col 0,1,'CHR SNP A1 A2 MAF_!{prop,,project,marker_strat_maf_summary} NCHROBS' --exact --require-col-match > !{output,,project_sample_marker_input_frq_file} class_level project run_if and,maf_strata_trait,marker_strat_maf_summary

!|expand:;:projectl;type;extrakeep;runif:project;all;;:project;sample;!{input,--read-freq,project_sample_marker_input_frq_file,if_prop=maf_strata_trait,if_prop=marker_strat_maf_summary,allow_empty=1};:pheno;sample;!{input@--keep@pheno_sample_plink_basic_all_include_file} ;skip_if not_trait run_if or,recompute_pca,recompute_genome run_with pheno_variant_subset| \
cmd make_projectl_type_marker_ld_prune_files=$plink_cmd $projectl_marker_basic_filters $plink_in_bed_helper(project_type_marker) $projectl_ld_prune_indep_filter extrakeep !{input,--extract,project_type_marker_ld_prune_restrict_file,if_prop=project_type_marker_ld_prune_restrict_file,allow_empty=1} !{raw,--out,projectl,*projectl_type_marker_plink_file} !{output,projectl_type_marker_ld_prune_in_file} !{output,projectl_type_marker_ld_prune_out_file} && $plink_mv_log_cmd(!{raw\,\,projectl\,*projectl_type_marker_plink_file},!{output\,\,projectl_type_marker_ld_prune_log_file}) class_level projectl rusage_mod $project_plink_mem runif

!!expand:;:projectl;type;runif:project;all;:project;sample;:pheno;sample;run_if or,recompute_genome,recompute_pca! \
local cmd make_projectl_type_marker_ld_prune_in_range_file=$smart_join_cmd !{input,--file,projectl_type_marker_ld_prune_in_file} !{input,--file,project_type_marker_bim_file} --col 2,2 --extra 2 | awk '{print \$2,\$4,\$4,\$1}' > !{output,,projectl_type_marker_ld_prune_in_range_file} class_level projectl runif

short cmd make_project_sample_marker_pruned_plink_files=$plink_cmd $project_marker_filters(sample) $plink_in_bed_helper(project_sample_marker) !{input,--read-freq,project_sample_marker_input_frq_file,if_prop=maf_strata_trait,if_prop=marker_strat_maf_summary,allow_empty=1} --make-bed $plink_out_bed_helper(project_sample_marker_pruned) && $plink_mv_log_cmd(!{raw\,\,project\,*project_sample_marker_pruned_plink_file},!{output\,\,project_sample_marker_pruned_make_bed_log_file}) class_level project rusage_mod $project_plink_mem

#short cmd make_project_extra_marker_pruned_plink_files=cat !{input,,marker_initial_filtered_fam_file,if_prop=add_for_pca,allow_empty=1} | $plink_cmd $project_prune_filters(sample) --keep /dev/stdin $plink_in_bed_helper(project_extra_marker) --make-bed $plink_out_bed_helper(project_extra_marker_pruned) && $plink_mv_log_cmd(!{raw\,\,project\,*project_extra_marker_pruned_plink_file},!{output\,\,project_extra_marker_pruned_make_bed_log_file}) class_level project

make_for_pca_helper=$plink_cmd $plink_in_bed_helper(@{1}) !{input,--bmerge,project_extra_marker_pruned_bed_file} !{input,,project_extra_marker_pruned_bim_file} !{input,,project_extra_marker_pruned_fam_file} --merge-mode 4 @3 --make-bed $plink_out_bed_helper(@{2}_sample_for_pca) && $plink_mv_log_cmd(!{raw\,\,@{2}\,*@{2}_sample_for_pca_plink_file},!{output\,\,@{2}_sample_for_pca_make_bed_log_file})

ln_for_pca_helper=ln -s !{input,,@{1}_bed_file} !{output,,@{2}_sample_for_pca_bed_file} && ln -s !{input,,@{1}_bim_file} !{output,,@{2}_sample_for_pca_bim_file} && ln -s !{input,,@{1}_fam_file} !{output,,@{2}_sample_for_pca_fam_file}

short cmd make_project_sample_for_pca_plink_files=$make_for_pca_helper(project_sample_marker_pruned,project,) class_level project run_if marker;add_for_pca

local cmd ln_project_sample_for_pca_plink_files=$ln_for_pca_helper(project_sample_marker_pruned,project) class_level project skip_if marker;add_for_pca

marker_pruned_initial_vcf_helper=$plink108_cmd !{input,--bed,@{2}_bed_file} !{input,--bim,@{2}_bim_file} !{input,--fam,@{2}_fam_file} --recode-vcf !{raw,--out,@{1},*@{2}_plink_file} && $plink_mv_log_cmd(!{raw\,\,@{1}\,*@{2}_plink_file},!{output\,\,@{2}_recode_vcf_log_file}) && $fix_vcf_file(@{2}_fam_file) < !{raw,,@{1},*@{2}_plink_file.vcf} | $bgzip_cmd > !{output,,@{1}_sample_marker_pruned_initial_vcf_file} && !{raw,,@{1},$run_tabix_cmd(@{1}_sample_marker_pruned_initial_vcf_file)} && rm -f !{raw,,@{1},*@{2}_plink_file.vcf}

short cmd make_project_sample_marker_pruned_initial_vcf_file=$marker_pruned_initial_vcf_helper(project,project_sample_marker_pruned) class_level project rusage_mod $project_plink_mem

marker_pruned_vcf_helper=$gatk_cmd_no_interval(SelectVariants) !{input,--variant,@{1}_sample_marker_pruned_initial_vcf_file} !{output,-o,@{1}_sample_marker_pruned_vcf_file} && !{raw,,@{1},$run_tabix_cmd(@{1}_sample_marker_pruned_vcf_file)}

#need to do this since marker may not be sorted same way as VCF
short cmd make_project_sample_marker_pruned_vcf_file=$marker_pruned_vcf_helper(project) class_level project run_if use_marker_for_sample_ibd

short cmd ln_project_sample_marker_pruned_vcf_file=ln -s !{input,,project_sample_marker_pruned_initial_vcf_file} !{output,,project_sample_marker_pruned_vcf_file} && !{raw,,project,$run_tabix_cmd(project_sample_marker_pruned_vcf_file)} class_level project skip_if use_marker_for_sample_ibd

plink_marker_helper_int=@1 $plink_in_bed_helper(project_sample_marker_pruned)
plink_marker_helper=$plink_marker_helper_int($plink_cmd)
short cmd make_project_sample_marker_frq_file=$plink_marker_helper $plink_analysis_helper(project_sample_marker_frq_file,freq,frq,project_sample_marker_frq_log_file) class_level project


local cmd make_project_sample_subset_sample_marker_genome_list1_file=cat !{input,,project_sample_marker_fam_file} !{input,,project_sample_subset_plinkseq_qc_plus_fam_file} | awk '{print \$1,\$2}' | sort | uniq -d > !{output,,project_sample_subset_sample_marker_genome_list1_file} class_level project_sample_subset run_if parallelize_genome

local cmd make_project_sample_subset_sample_marker_genome_list2_file=cat !{input,,project_sample_subset_sample_marker_genome_list1_file,all_instances=1,if_prop=project_sample_subset:cmp_ge:@project_sample_subset} > !{output,,project_sample_subset_sample_marker_genome_list2_file} class_level project_sample_subset run_if parallelize_genome

prop do_all_genome=scalar

!@expand:;:shortt;projectl;type;runif;exflags;num_samples:;project;sample;run_if or,!num_samp_subsets,!parallelize_genome;;1:short;project_sample_subset;sample;run_if and,num_samp_subsets,parallelize_genome;!{input,--read-freq,project_sample_marker_frq_file} --genome-lists !{input,,project_sample_subset_sample_marker_genome_list1_file} !{input,,project_sample_subset_sample_marker_genome_list2_file};`cat !{input,,project_sample_subset_sample_marker_genome_list1_file} !{input,,project_sample_subset_sample_marker_genome_list2_file} | wc -l`@ \
shortt cmd make_projectl_type_marker_genome_file=if [[ num_samples -gt 0 ]]; then $plink_marker_helper --genome --Z-genome exflags !{raw,--out,projectl,*projectl_type_marker_plink_file} !{output,projectl_type_marker_genome_file} && $plink_mv_log_cmd(!{raw\,\,projectl\,*projectl_type_marker_plink_file},!{output\,\,projectl_type_marker_genome_log_file}); else echo "FID1 IID1 FID2 IID2 RT EZ Z0 1 Z2 PI_HAT PHE DST PPC RATIO" | gzip -c > !{output,,projectl_type_marker_genome_file} && echo > !{output,,projectl_type_marker_genome_log_file} ; fi class_level projectl runif

short cmd make_cat_project_sample_marker_genome_file=zcat !{input,,project_sample_subset_sample_marker_genome_file,sort_prop=project_sample_subset} | awk 'NR==1 {a=\$1; print} NR > 1 && \$1 != a {print}' | gzip -c > !{output,,project_sample_marker_genome_file} class_level project run_if and,num_samp_subsets,parallelize_genome

!!expand:,:project,duplicate:project,duplicate:pheno,related! \
local cmd make_project_custom_duplicate_exclude_rank_file=ln -s !{input,,project_duplicate_exclude_custom_rank_file,if_prop=project_duplicate_exclude_custom_rank_file,allow_empty=1} !{output,,project_duplicate_exclude_rank_file} class_level project run_if and,project_duplicate_exclude_custom_rank_file,!prune_by_call_rate 

local cmd make_project_call_rate_duplicate_exclude_rank_file=$smart_cut_cmd --in-delim , --out-delim $tab !{input,--file,project_sstats_file} --select-col 1,1,'ID RATE' --exclude-row 1,1 > !{output,,project_duplicate_exclude_rank_file} class_level project run_if and,!project_duplicate_exclude_custom_rank_file,prune_by_call_rate 

local cmd make_project_duplicate_exclude_rank_file=$smart_join_cmd --out-delim $tab !{input,--file,project_passed_sample_list_file} --rest-extra 1 --exec "$fill_file_helper(project_duplicate_exclude_custom_rank_file,project_passed_sample_list_file,NA,1,1,1,0)" --exec "$smart_cut_cmd --in-delim , --out-delim $tab !{input,--file,project_sstats_file} --select-col 1,1,'ID RATE' --exclude-row 1,1" > !{output,,project_duplicate_exclude_rank_file} class_level project run_if and,project_duplicate_exclude_custom_rank_file,prune_by_call_rate 

short cmd make_project_duplicate_exclude_file=zcat !{input::project_sample_marker_genome_file} | perl $targeted_bin_dir/prune_most_ibd.pl !{input,--rank-file,project_duplicate_exclude_rank_file,if_prop=project_duplicate_exclude_custom_rank_file,if_prop=prune_by_call_rate,or_if_prop=1,allow_empty=1} !{prop,,project,max_duplicate} > !{output,,project_duplicate_exclude_file} class_level project 


marker_merge_helper=$smart_join_cmd --exec \"@1\" --exec \"sed 's/\$/@2/' !{input,,project_passed_sample_list_file} @3\" --merge @4

local cmd make_project_epacts_ready_file=touch !{output,,project_epacts_ready_file} class_level project

kinship_helper=!{input,@{1}_epacts_ready_file} $epacts_cmd make-kin -restart !{input,--vcf,@{1}_sample_marker_pruned_vcf_file} --min-maf 0 --min-callrate 0 -unit 1000000000 -run 1 !{raw,--out,@{1},*@{1}_epacts_trunk} && mv !{raw,,@{1},*@{1}_epacts_trunk} !{output,,@{1}_kinship_file}

cmd make_project_kinship_file=$kinship_helper(project) class_level project rusage_mod $kin_mem 

prop parallelize_pca=scalar default 1
prop parallelize_genome=scalar default 1

prop max_num_samples_pcs=scalar default 2000
pca_compute_pop=10
pca_ignore_pop=1

local cmd make_project_sample_marker_smart_pca_map_file=cat !{input,,project_sample_marker_fam_file} | awk '{print \$1,\$2,NR,NR}' > !{output,,project_sample_marker_smart_pca_map_file} class_level project

smart_pca_remap_helper_int=perl -lne 'BEGIN {open IN, @5"!{input,,project_sample_marker_smart_pca_map_file}@5" or die @5"Cannot read !{input,,project_sample_marker_smart_pca_map_file}@5"; while (<IN>) {chomp; \@cols = split; @5\$m{@5\$cols[@1]}{@5\$cols[@1+@3]} = [@5\$cols[@2], @5\$cols[@2+@3]];}} chomp; \@cols = split; if (exists @5\$m{@5\$cols[0]}{@5\$cols[0+@3]}) {@5\$value = @5\$m{@5\$cols[0]}{@5\$cols[0+@3]}; @5\$cols[0] = @5\$value->[0]; @5\$cols[0+@3] = @5\$value->[0+@3];} print join(@5"@4@5", \@cols)'

smart_pca_remap_helper=$smart_pca_remap_helper_int(@1,@2,1, ,)

!!expand:projectl:project:pheno! \
projectl_smart_pca_fam_file_helper=ns=`cat !{input,,@1} | wc -l` && cat /dev/null !{input,,marker_initial_filtered_fam_file,if_prop=add_for_pca,allow_empty=1} !{input,,projectl_sample_for_pca_fam_file,unless_prop=parallelize_pca,allow_empty=1} | awk '{print \$2}' | cat - !{input,,@1} | awk -v n=\$ns 'NR == 1 {f=!{prop,,projectl,max_num_samples_pcs}/n} NF == 1 {m[\$1]=1} NF == 6 {if (rand() < f || m[\$2] ) {\$6=$pca_compute_pop} else {\$6 = $pca_ignore_pop}} NF == 6 {print}' 

project_sample_subset_smart_pca_fam_file_helper=cat !{input,,project_sample_subset_clean_samp_keep_file} | $smart_pca_remap_helper_int(1,3,0, ,) | cat - !{input,,@1} | awk 'NF == 1 {m[\$1]=1} NF == 6 && !m[\$2] && \$6 != $pca_compute_pop {\$6=-9} NF == 6 {print}'

local cmd make_project_sample_marker_smart_pca_fam_file=$project_smart_pca_fam_file_helper(project_sample_for_pca_fam_file) | $smart_pca_remap_helper(0,2) > !{output,,project_sample_marker_smart_pca_fam_file} class_level project

local cmd make_project_sample_subset_sample_marker_smart_pca_fam_file=$project_sample_subset_smart_pca_fam_file_helper(project_sample_marker_smart_pca_fam_file) > !{output,,project_sample_subset_sample_marker_smart_pca_fam_file} class_level project_sample_subset

!!expand:,:type:sample! \
local cmd make_project_type_marker_smart_pca_poplist_file=echo $pca_compute_pop > !{output,,project_type_marker_smart_pca_poplist_file} class_level project

!|expand:;:shortt;projects;type;exrunif;rusagemod:;project;sample;skip_if and,num_samp_subsets,parallelize_pca;rusage_mod $smart_pca_mem:short;project_sample_subset;sample;skip_if !parallelize_pca;| \
short cmd make_projects_type_marker_smart_pca_out_files=$smart_pca_cmd !{input,-i,project_type_for_pca_bed_file} !{input,-a,project_type_for_pca_bim_file} !{input,-b,projects_type_marker_smart_pca_fam_file} !{input,-w,project_sample_marker_smart_pca_poplist_file,if_prop=parallelize_pca,allow_empty=1} !{prop,-k,project,num_mds_calc} !{raw,-o,projects,*projects_type_marker_smart_pca_trunk} !{output,projects_type_marker_smart_pca_evec_file} !{output,-e,projects_type_marker_smart_pca_eval_file} !{raw,-p,projects,*projects_type_marker_smart_pca_trunk} !{output,-g,projects_type_marker_smart_pca_weights_file} !{output,-l,projects_type_marker_smart_pca_log_file} -m 0 -t $smart_pca_num_outlier_evec -s $smart_pca_outlier_sigma_thresh class_level projects exrunif

local cmd make_cat_project_sample_marker_smart_pca_eval_file=cp !{input,,project_sample_subset_sample_marker_smart_pca_eval_file,limit=1,sort_prop=project_sample_subset} !{output,,project_sample_marker_smart_pca_eval_file,sort_prop=project_sample_subset} class_level project run_if and,num_samp_subsets,parallelize_pca

evec_to_mds_helper_int=tail -n+2 !{input,,@1} | $smart_cut_cmd --exec @3"echo FID:IID `$get_mds_cols_int(!{prop\,\,@2\,num_mds_calc}, ,\)` 0@3" | sed 's/^\(\S\S*\):\(\S\S*\)\(\s\s*\)/\1\3\2\30\3/' | sed 's/\s\s*/\t/g' | rev | cut -f2- | rev
evec_to_mds_helper=$evec_to_mds_helper_int(@1,@2,,\)

parse_mds_helper=$smart_join_cmd --in-delim $tab --exec "cat !{input,,@{1}_sample_marker_smart_pca_fam_file} | $smart_pca_remap_helper_int(2,0,1, ,\) | awk '{print \\$2}' | cat - !{input,,@2} | sort | uniq -d | sed '1 s/^/IID\n/'" --exec "$evec_to_mds_helper_int(@{1}_sample_marker_smart_pca_evec_file,@1,\) | $smart_pca_remap_helper_int(2,0,1,\\t,\)" --extra 2 --col 2,2 --header 1 | sed 's/^\(\S\S*\)\(\s\s*\)\(\S\S*\)/\3\2\1/'

short cmd parse_project_sample_subset_sample_marker_mds_file=$parse_mds_helper(project_sample_subset,project_sample_subset_clean_samp_keep_file) > !{output,,project_sample_subset_sample_marker_mds_file} class_level project_sample_subset run_if parallelize_pca bsub_batch 10

short cmd parse_project_sample_marker_mds_file=$parse_mds_helper(project,project_passed_sample_list_file) > !{output,,project_sample_marker_mds_file} class_level project run_if or,!parallelize_pca,!num_samp_subsets

#local cmd make_project_strat_mds_file=$smart_join_cmd --exec "sed '/^\\#/d' !{input,,project_plinkseq_strat_phe_file} | $smart_cut_cmd !{input,--file,project_sample_marker_mds_file} --select-col 1,1,IID --exclude-row 1,1 --select-col 0,1 | sort | uniq -d | sed '1 s/^/ID\n/'" --exec "sed 's/^\\#//' !{input,,project_plinkseq_strat_phe_file} | grep -v \\\# | awk -v OFS=\"\t\" 'NR > 1 {if (\\$2==$ibs_cluster_missing) {\\$2=\"Missing\"} else {\\$2=\"Cluster \"\\$2}} {print}'" --exec "$smart_cut_cmd !{input,--file,project_sample_marker_mds_file} --exclude-col 1,1,FID --exclude-col 1,1,SOL" --in-delim 2,$tab --out-delim $tab --header 1 --extra 2 --extra 3 > !{output,,project_strat_mds_file} class_level project

#!!expand:;:type;colsarg:top;--select-col 1,1,'C1 C2':all;--exclude-col 1,1-3! \
#local cmd make_project_type_strat_mds_pdf_file=$draw_matrix_plot_cmd !{input,,project_strat_mds_file} !{output,,project_type_strat_mds_pdf_file} 'Sample MDS Values: by IBS cluster' `$smart_cut_cmd !{input,--file,project_sample_marker_mds_file} --select-row 1,1 colsarg --out-delim ,` color.col=$ibs_cluster_pheno sep=$tab cex=$mds_cex class_level project

min_neighbor=1
prop max_neighbor=scalar default 5

#pheno cmds

display_col_name=Display
order_col_name=Order

#keys for on-the-fly dichotomization of a quantitative trait

#for plots (everyone assigned a phenotype
qt_missing_disp=Missing
qt_bottom_disp=Below !{prop,,pheno,med_quantile,missing_key=default_med_quantile}q
qt_top_disp=Above !{prop,,pheno,med_quantile,missing_key=default_med_quantile}q

"""
}
    