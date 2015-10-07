
package loamstream.pipeline.examples

/**
 * LoamStream
 * Created by oliverr on 10/6/2015.
 */
object PipelineTargetedPart27 {
  val string =
 """
associated_genes_top_helper=$smart_cut_cmd --in-delim $tab !{input,--file,burden_flat_gassoc_file} --exclude-row 1,1 --select-col 1,1,@1 !{input,--file,burden_flat_gassoc_file} $associated_genes_tex_select_helper(2) --exclude-row 2,1 --paste | sort -gk1 | cut -f2- | awk '\$1 ~ /^[0-9\.\-e]+$/' | awk -F$tab '{k=\$2":"\$NF} !m[k] {print; m[k]=1}' | $head_cmd($num_slide_associated_gene)

format_gene_dists=perl -ne 'chomp; @a = split("\t"); \$dist_ind = \$\#a; %dists = (); @dists = split(";", \$a[\$dist_ind]); \$count_a = 0; \$count_u = 0; foreach \$dist (@dists) {next unless \$dist =~ /^([0-9]+)\/([0-9]+)(\(([0-9]+)\))?/; \$c=\$4; \$c = 1 unless defined \$c; \$obs_a = \$1 * \$c; \$obs_u = \$2 * \$c; \$count_a += \$obs_a; \$count_u += \$obs_u; \$tot = \$obs_a + \$obs_u; next unless \$tot > 0; \$frac_a = \$a[1] / (\$a[1] + \$a[2]); \$exp_a = \$tot * \$frac_a; \$exp_u = \$tot * (1 - \$frac_a); if (\$exp_a == 0 || \$exp_u == 0) {\$stat = 0} else {\$stat = ((\$obs_a - \$exp_a)\*\*2 / (\$exp_a) + (\$obs_u - \$exp_u)\*\*2 / (\$exp_u));} \$dists{\$dist} = \$stat} @new_dist = (); \$num = 0; foreach \$dist (sort {\$dists{\$b} <=> \$dists{\$a}} keys %dists) {last if ++\$num > $max_dist_per_gene; push @new_dist, \$dist} \$a[\$dist_ind] = join(";", @new_dist); \$a[\$dist_ind] .= "..." if \$num > $max_dist_per_gene; print join("\t", @a); print "\n"'

obs_mean_helper=f=`$table_sum_stats_cmd --in-delim $tab --out-delim $tab --col OBSA --col OBSU --has-header --print-header --summaries < !{input,,pheno_vassoc_annot_file} | $smart_cut_cmd --in-delim $tab --select-col 0,1,'$summary_mean(OBSA) $summary_mean(OBSU)' | tail -n+2`
short cmd make_burden_slide_gassoc_tex_file=$obs_mean_helper && $associated_genes_top_helper(P_MIN) | sort -gk1 | awk -v OFS="\t" '{tmp=\$2; \$2=\$1; \$1=tmp} {print}' | sed 's/^\(\S\S*\)/\1\t'"\$f"'/' | sort -t$tab -gk4 | cut -f4 --complement | $format_gene_dists | $smart_cut_cmd --in-delim $tab --exclude-col 0,'2 3' | $smart_cut_cmd --in-delim $tab --exec "$smart_cut_cmd --in-delim $tab !{input,--file,burden_flat_gassoc_file} --select-row 1,1 $associated_genes_tex_select_helper(1) | cut -f2-" | $format_columns_cmd --in-delim $tab --number-format %.3g | $table_to_beamer_cmd --allow-breaks --auto-dim --font-size 8pt --title "Most associated transcripts (1 per gene) --- !{prop,,burden,disp} variants: !{prop,,pheno,disp}" --in-delim $tab --header-rows 1 > !{output,,burden_slide_gassoc_tex_file} class_level burden run_if burden_test

#obs_mean_helper=$table_sum_stats_cmd --in-delim $tab --out-delim $tab --col @1 --has-header --print-header --summaries --group-col GENE < !{input,,pheno_vassoc_annot_file}
#local cmd make_burden_slide_gassoc_tex_file=$smart_join_cmd --in-delim $tab --header 1 --exec "$smart_cut_cmd --in-delim $tab --exec \"$obs_mean_helper(OBSA)\" --exec \"$obs_mean_helper(OBSU)\" --paste --select-col 1,1,'GENE $summary_mean(OBSA)' --select-col 2,1,$summary_mean(OBSU)" --exec "$smart_cut_cmd --in-delim $tab --exec \"$associated_genes_top_helper(P_MEAN)\" --exec \"$associated_genes_top_helper(P_MIN)\" --exec \"$associated_genes_top_helper(P_MEDIAN)\" | sort -u -gk1 | $smart_cut_cmd --in-delim $tab --exec \"$smart_cut_cmd --in-delim $tab !{input,--file,burden_flat_gassoc_file} --select-row 1,1 $associated_genes_tex_select_helper(1)\"" --extra 1 --col 2,2 | sort -t$tab -gk4 | cut -f4 --complement | $format_gene_dists | $smart_cut_cmd --in-delim $tab --exclude-col 0,1,'$summary_mean(OBSA) $summary_mean(OBSU)' | $format_columns_cmd --in-delim $tab --number-format %.3g | $table_to_beamer_cmd --allow-breaks --auto-dim --font-size 8pt --title "Most associated genes --- !{prop,,burden,disp} variants: !{prop,,pheno,disp}" --in-delim $tab --header-rows 1 > !{output,,burden_slide_gassoc_tex_file} class_level burden

local cmd make_burden_slide_gassoc_pdf_file=$run_latex_cmd(burden_slide_gassoc_tex_file,burden_slide_gassoc_pdf_file) class_level burden run_if burden_test

#cmd make_burden_haplotype_burden_files=$r_script_cmd($haplotype_burden_bin_dir/calc_haplotype_specific_burden.R) !{input,,pheno_haplotype_burden_input_list_file} !{input,locus_index_geno_counts_file} !{input,locus_strat_index_snp_file} !{input,locus_top_common_marker_pos_file} !{input,locus_all_seq_tped_file} !{input,locus_all_seq_tfam_file} !{input,locus_all_marker_tped_file} !{input,locus_all_marker_tfam_file} !{input,,pheno_marker_pheno_file} !{input,,burden_locdb_reg_file} !{output,,burden_haplotype_burden_locus_level_file} !{output,,burden_haplotype_burden_sum_file} !{output,,burden_haplotype_burden_pdf_file} !{output,,burden_haplotype_burden_rv_file} !{output,,burden_haplotype_burden_recessive_file} class_level burden run_if do_hap_burden skip_if pheno_qt


#region cmds

!!expand:rorl:region:locus! \
meta_table cmd make_rorl_range_info_file=!{prop,,rorl,rorl_range} !{output,rorl_range_info_file} class_level rorl

hap_geno=.05
hap_maf=.05

kb_range=--chr !{prop,,@1,chrom} --from-kb `perl -e 'print ((!{prop,,@1,@{1}_start} - @2) / 1000)'` --to-kb `perl -e 'print ((!{prop,,@1,@{1}_end} + @2) / 1000)'` 
cmd make_region_all_marker_plink_files=$plink_cmd $plink_in_bed_helper(project_all_marker) !{raw,--out,region,*region_all_marker_plink_file} !{output,region_all_marker_bed_file} !{output,region_all_marker_bim_file} !{output,region_all_marker_fam_file} --make-bed --geno $hap_geno $kb_range(region,1) && $plink_mv_log_cmd(!{raw\,\,region\,*region_all_marker_plink_file},!{output\,\,region_all_marker_make_bed_log_file}) !{input,region_range_info_file} class_level region

cmd make_region_all_marker_transposed_plink_files=$plink_cmd $plink_in_bed_helper(region_all_marker) !{raw,--out,region,*region_all_marker_plink_file} !{output,region_all_marker_tped_file} !{output,region_all_marker_tfam_file} --transpose --recode && $plink_mv_log_cmd(!{raw\,\,region\,*region_all_marker_plink_file},!{output\,\,region_all_marker_recode_log_file}) class_level region

#depends on tfam file in order to prevent running at same time and thus overwriting the log file
cmd make_region_all_marker_freq_file=$plink_cmd $plink_in_bed_helper(region_all_marker) --freq !{raw,--out,region,*region_all_marker_plink_file} !{output,region_all_marker_freq_file} && $plink_mv_log_cmd(!{raw\,\,region\,*region_all_marker_plink_file},!{output\,\,region_all_marker_freq_log_file}) !{input,region_all_marker_tfam_file} class_level region

local cmd make_region_rare_marker_snp_list=$smart_cut_cmd !{input,--file,region_all_marker_freq_file} --select-row 1,1,MAF,lt:$hap_maf --select-col 1,1,SNP > !{output,,region_rare_marker_snp_list} class_level region

cmd make_region_common_marker_transposed_plink_files=$plink_cmd $plink_in_bed_helper(region_all_marker) !{raw,--out,region,*region_common_marker_plink_file} !{output,region_common_marker_tped_file} !{output,region_common_marker_tfam_file} !{input,--exclude,region_rare_marker_snp_list} --transpose --recode && $plink_mv_log_cmd(!{raw\,\,region\,*region_common_marker_plink_file},!{output\,\,region_common_marker_recode_log_file}) class_level region

pseq_region_variant_cmd=$pseq_qc_plus_analysis_cmd(@1) --mask reg.req=!{prop,,region,ref_chrom}:!{prop,,region,region_start}..!{prop,,region,region_end} !{input,region_range_info_file} 

local cmd make_region_all_seq_plink_files=$plink_cmd $plink_in_bed_helper(project_plinkseq_qc_plus) $kb_range(region,1) --make-bed !{raw,--out,region,*region_all_seq_plink_file} !{output,region_all_seq_bim_file} !{output,region_all_seq_bed_file} !{output,region_all_seq_fam_file} && $plink_mv_log_cmd(!{raw\,\,region\,*region_all_seq_plink_file},!{output\,\,region_all_seq_make_bed_log_file}) class_level region

local cmd make_region_all_seq_transposed_plink_files=$plink_cmd $plink_in_bed_helper(region_all_seq) --recode --transpose !{raw,--out,region,*region_all_seq_plink_file} !{output,region_all_seq_tfam_file} !{output,region_all_seq_tped_file} && $plink_mv_log_cmd(!{raw\,\,region\,*region_all_seq_plink_file},!{output\,\,region_all_seq_recode_log_file}) class_level region

get_duplicated_variants=cat !{input,,region_all_seq_bim_file} !{input,,region_all_marker_bim_file} | awk '{print \$2,\$4}' | sort -sk2 | uniq -df1

#local cmd make_region_duplicate_marker_names_file=$get_duplicated_variants -D | perl -ne '@a = split; push @{\$h{\$a[1]}}, \$a[0]; END {foreach (keys %h) {print join("\t", @{\$h{\$_}}); print "\n"}}' > !{output,,region_duplicate_marker_names_file} class_level region

local cmd make_region_non_marker_seq_plink_file=$get_duplicated_variants | awk '{print \$1}' | $plink_cmd $plink_in_bed_helper(region_all_seq) --exclude /dev/stdin --make-bed !{raw,--out,region,*region_non_marker_seq_plink_file} !{output,region_non_marker_seq_bed_file} !{output,region_non_marker_seq_bim_file} !{output,region_non_marker_seq_fam_file} && $plink_mv_log_cmd(!{raw\,\,region\,*region_non_marker_seq_plink_file},!{output\,\,region_non_marker_seq_make_bed_log_file}) class_level region

local cmd make_region_combined_plink_files=$plink_cmd $plink_in_bed_helper(region_all_marker) !{input,--bmerge,region_non_marker_seq_bed_file} !{input,,region_non_marker_seq_bim_file} !{input,,region_non_marker_seq_fam_file} --transpose --recode !{raw,--out,region,*region_combined_plink_file} !{output,region_combined_tfam_file} !{output,region_combined_tped_file} && $plink_mv_log_cmd(!{raw\,\,region\,*region_combined_plink_file},!{output\,\,region_combined_merge_log_file}) class_level region

local cmd make_region_combined_plink_ped_files=$plink_cmd !{raw,--tfile,region,*region_combined_plink_file} !{input,region_combined_tfam_file} !{input,region_combined_tped_file} --recode !{raw,--out,region,*region_combined_plink_file} !{output,region_combined_ped_file} !{output,region_combined_map_file} && $plink_mv_log_cmd(!{raw\,\,region\,*region_combined_plink_file},!{output\,\,region_combined_transpose_log_file}) class_level region

local cmd make_region_combined_haploview_ped_file=awk '{\$6 = 0} {print}' !{input,,region_combined_ped_file} > !{output,,region_combined_haploview_ped_file} class_level region

combined_impute_helper=sort -nk4 !{input,,@{1}_@{2}_tped_file} | perl $targeted_bin_dir/plink_to_impute.pl --tped /dev/stdin !{input,--tfam,@{1}_@{2}_tfam_file} !{input,--sample-list,project_sample_marker_keep_file}

!|expand:;:cmdname;extra_cmd;whichplink:ref_;!{output,--out-strand,region_combined_strand_file};combined:;--exclude;all_marker| \
cmd make_region_combined_cmdnameimpute_files=$combined_impute_helper(region,whichplink) !{output,--out-genotype,region_combined_cmdnamegenotype_file} extra_cmd class_level region

Ne=11418
#set to zero b/c region start already includes a buffer
impute_window=0
cmd make_region_all_variant_impute2_file=$impute2_cmd -m $genetic_map_file("!{prop,,region,chrom}") !{input,-g_ref,region_combined_ref_genotype_file} !{input,-strand_g_ref,region_combined_strand_file} !{input,-g,region_combined_genotype_file} !{input,-strand_g,region_combined_strand_file} -int `perl -e 'print !{prop,,region,region_start} - $impute_window'` `perl -e 'print !{prop,,region,region_end} + $impute_window'` -Ne $Ne -allow_large_regions !{input,-exclude_snps_g,region_rare_marker_snp_list} !{input,-exclude_snps_g_ref,region_rare_marker_snp_list} -impute_excluded !{output,-o,region_combined_impute2_file} !{output,region_combined_impute2_info_file} !{output,region_combined_impute2_info_by_sample_file} !{output,region_combined_impute2_summary_file} !{input,region_range_info_file} !{output,region_combined_impute2_warnings_file} > /dev/null class_level region


cmd make_region_combined_merged_impute2_file=$smart_join_cmd !{input,--file,region_combined_impute2_file} !{input,--file,region_combined_genotype_file} --merge --col 3 | awk '{\$3=\$3" "\$1} {\$1=""} {print}' | sed 's/^\s*//' > !{output,,region_combined_merged_impute2_file} class_level region

beagle_window=0
cmd make_region_beagle_input_file=perl $targeted_bin_dir/make_hap_analysis_beagle_file.pl !{prop,--chr,region,chrom} !{prop,--start,region,region_start} !{prop,--end,region,region_end} --window $beagle_window !{input,--tfam,region_common_marker_tfam_file} !{input,--tped,region_common_marker_tped_file} > !{output,,region_beagle_input_file} !{input,region_range_info_file} class_level region

cmd make_region_beagle_output_files=java -Xmx$beagle_mem -jar $beagle_jar !{input,unphased=,region_beagle_input_file} !{output,out=,region_beagle_phased_gz_file} !{output,region_beagle_phased_gz_log_file} !{output,region_beagle_gprobs_gz_file} !{output,region_beagle_r2_file} nsamples=$nsamples omitprefix=$omitprefix missing=$beagle_missing class_level region

!!expand:btype:phased:gprobs! \
local cmd make_region_btype_file=gunzip -c !{input,,region_beagle_btype_gz_file} > !{output,,region_beagle_btype_file} class_level region

local cmd make_region_haploview_info_file=$smart_cut_cmd !{input,--file,region_combined_map_file} --select-col 1,'2 4' > !{output,,region_haploview_info_file} class_level region

cmd make_region_haploview_ld_file=rm -f !{output,,region_haploview_ld_file} && java -Xmx$haploview_mem -jar $haploview_jar -nogui !{input,-pedfile,region_combined_haploview_ped_file} !{input,-info,region_haploview_info_file} -dprime !{raw,-out,region,*region_haploview_ld_trunk} !{output,region_haploview_ld_file} -skipcheck && ls !{output,,region_haploview_ld_file} class_level region

#locus cmds

#to generate symbolic links

!!expand:filenamesuffix:\
all_marker_bed_file:\
all_marker_bim_file:\
all_marker_fam_file:\
all_marker_tfam_file:\
all_marker_tped_file:\
common_marker_tped_file:\
common_marker_tfam_file:\
all_seq_bed_file:\
all_seq_bim_file:\
all_seq_fam_file:\
all_seq_tfam_file:\
all_seq_tped_file:\
non_marker_seq_bed_file:\
non_marker_seq_bim_file:\
non_marker_seq_fam_file:\
duplicate_marker_names_file:\
combined_tped_file:\
combined_tfam_file:\
combined_ped_file:\
combined_map_file:\
combined_haploview_ped_file:\
haploview_info_file:\
haploview_ld_file:\
beagle_gprobs_gz_file:\
beagle_gprobs_file:\
beagle_phased_gz_file:\
beagle_phased_file:\
beagle_phased_gz_log_file:\
beagle_phased_file:\
beagle_r2_file:\
combined_ref_genotype_file:\
combined_strand_file:\
combined_genotype_file:\
combined_impute2_info_file:\
combined_impute2_info_by_sample_file:\
combined_impute2_summary_file:\
combined_impute2_warnings_file:\
combined_impute2_file:\
combined_merged_impute2_file!\
local cmd ln_locus_filenamesuffix=rm -f !{output,,locus_filenamesuffix} && ln -s !{input,,region_filenamesuffix,if_prop=region_range:eq:@locus_range} !{output,,locus_filenamesuffix}  !{input,locus_range_info_file} class_level locus

!|expand:;:cmdname;extra_cmd;whichplink:ref_;;combined:;--exclude;all_marker| \
cmd make_locus_combined_cmdnameimpute_sample_file=$combined_impute_helper(locus,whichplink) !{input,--pheno-file,pheno_marker_pheno_file,if_prop=project_all_marker_pheno_file,if_prop=pheno_marker_initial_pheno_file,or_if_prop=1,allow_empty=1} !{prop,--pheno-name,pheno} !{input,--extra-covars,project_all_marker_assoc_covars_file,if_prop=project_all_marker_assoc_covars_file,allow_empty=1} !{output,--out-sample,locus_combined_cmdnamesample_file} extra_cmd class_level locus

impute_method=score
prop use_covars=list
cmd make_locus_combined_snptest_out_file=$snptest_cmd -data !{input,,locus_combined_merged_impute2_file} !{input,,locus_combined_sample_file} !{output,-o,locus_combined_snptest_out_file} !{raw,-cov_names,locus,,if_prop=use_covars,allow_empty=1} !{prop,,locus,use_covars,if_prop=use_covars,allow_empty=1} !{raw,-cov_all,locus,,unless_prop=use_covars,allow_empty=1} -frequentist 1 -method $impute_method !{prop,-pheno,pheno} > !{output,,locus_combined_snptest_log_file} class_level locus

snptest_pvalue_col=!{prop,,pheno}_frequentist_add.\*score_pvalue

locus_chrom_selector=--select-row @1,1,@2,!{prop,,locus,chrom} --exact !{input,locus_range_info_file}
locus_pos_selector=--select-row @1,1,@2,ge:!{prop,,locus,locus_start} --select-row @1,1,@2,le:!{prop,,locus,locus_end} --and-row-all !{input,locus_range_info_file}
locus_range_selector=--select-row @1,1,@2,ge:`tail -n+2 !{input,,locus_gene_target_file} | sort -nk2 | head -n1 | awk '{print \$2 - $locus_range_buffer}'` --select-row @1,1,@2,le:`tail -n+2 !{input,,locus_gene_target_file} | sort -nrk2 | head -n1 | awk '{print \$2 + $locus_range_buffer}'` --and-row-all

!|expand:;:extraname;extraexec;skiporrunif:\
with_pvalues;--exec \"$smart_cut_cmd !{input,--file,pheno_all_marker_snp_pvalues_file} $locus_chrom_selector(1,CHROM) $locus_pos_selector(1,POS)  --exact --require-col-match --exclude-row 1,1 --select-col 1,1,'POS P'\";run_if:\
without_pvalues;;skip_if| \
cmd make_locus_combined_association_scores_file_extraname=$smart_join_cmd --in-delim $tab \
  --exec "$smart_cut_cmd --out-delim $tab !{input,--file,locus_all_marker_bim_file} --select-col 1,4 !{input,--file,locus_combined_snptest_out_file} --exclude-row 2,1 --select-col 2,1,pos --exact --require-col-match | sort | uniq -d" \
  --exec "$smart_cut_cmd --out-delim $tab !{input,--file,locus_combined_snptest_out_file} --exclude-row 1,1 --select-col 1,1,'pos rsid all_maf' --exact --require-col-match" \
  --exec "$smart_cut_cmd --out-delim $tab extraexec --exec \"$smart_cut_cmd !{input,--file,locus_combined_snptest_out_file} --select-col 1,1,'pos $snptest_pvalue_col' $locus_pos_selector(1,pos) --exact --require-col-match --exclude-row 1,1\" !{input,locus_range_info_file} | awk 'seen[\\$1] != 1 {print} {seen[\\$1] = 1}'" --extra 2 --extra 3 | sed '1 s/^/POS\tSNP\tMAF\tP\n/' | $smart_cut_cmd --in-delim $tab $locus_range_selector(0,POS) --select-row 0,1 > !{output,,locus_combined_association_scores_file} class_level locus skiporrunif pheno_all_marker_snp_pvalues_file

!!expand:curpheno:case:control! \
cmd make_locus_curpheno_position_coverage_file=(for f in `cat !{input,,pheno_curpheno_sample_coverage_file_list,is_list=1}`; do zcat \$f | tail -n+2; done) | sed 's/^chr//' | sed 's/^\(\S*\):\(\S\S*\)/\1\t\2/' | awk -v OFS="\t" '\$1 == !{prop,,locus,chrom} && \$2 >= !{prop,,locus,locus_start} && \$2 <= !{prop,,locus,locus_end} {print \$1,\$2,\$3}' | sort | $table_sum_stats_cmd --in-delim $tab --group-col 1 --group-col 2 --col 3 --summaries --threshold $threshold --out-delim $tab --print-header --groups-sorted  > !{output,,locus_curpheno_position_coverage_file} !{input,locus_range_info_file} class_level locus skip_if no_coverage

local cmd make_locus_exon_target_file=awk -v OFS="\t" -v chrom="!{prop,,locus,ref_chrom}" -v start=!{prop,,locus,locus_start} -v end=!{prop,,locus,locus_end} -F"\t" 'BEGIN {print "EXON","START","END"} \$3 == chrom && ((\$4 >= start && \$4 <= end) || (\$5 >= start && \$5 <= end)) {print \$2,\$4,\$5}' !{input,,project_expanded_exon_target_file} !{input,locus_range_info_file} > !{output,,locus_exon_target_file} class_level locus

local cmd make_locus_gene_target_file=awk -v OFS="\t" -v chrom="!{prop,,locus,ref_chrom}" -v start=!{prop,,locus,locus_start} -v end=!{prop,,locus,locus_end} -F"\t" 'BEGIN {print "GENE","START","STOP"} \$2 == chrom && ((\$3 >= start && \$3 <= end) || (\$3 >= start && \$3 <= end)) {print \$1,\$3,\$4}' !{input,,project_gene_target_file} !{input,locus_range_info_file} > !{output,,locus_gene_target_file} class_level locus

short cmd make_locus_var_counts_dat_file=$var_counts_dat_helper($locus_var_counts_selector) > !{output,,locus_var_counts_dat_file} class_level locus

local cmd make_locus_burden_dat_file=$smart_join_cmd --exec "$smart_cut_cmd --in-delim $tab !{input,--file,locus_gene_target_file} --exec \"$smart_cut_cmd --in-delim $tab !{input,--file,pheno_gassoc_file} --exclude-row 1,1 --select-col 1,1,GENE | sort -u\" --exclude-row 1,1 --select-col 1,1,GENE --exact --require-col-match | sort | uniq -d | sed '1 s/^/GENE\n/'" !{input,--file,locus_gene_target_file} !{input,--file,pheno_gassoc_file} --in-delim $tab --header 1 --col 3,2 --extra 2 --extra 3 --multiple 3 | awk '{print} END {if (NR == 0) {print "GENE\tSTART\tSTOP\tTEST\tVAR_GROUP\tP"}}' | $smart_cut_cmd --in-delim $tab --select-col 0,1,'GENE START STOP TEST VAR_GROUP P' --select-row 0,1,TEST,'!{prop,,burden_test,test_tag,missing_prop=test_name,uc=1}' --select-row 0,1 --exact --require-col-match > !{output,,locus_burden_dat_file} class_level locus

local cmd make_locus_var_coverage_pdf_file=$var_coverage_helper(locus,var_coverage,,) !{input,gene.position.file=,locus_gene_target_file} !{input,burden.file=,locus_burden_dat_file} num.shading=2 frac.spacing=.05 width=`perl -le '\$ngenes = \`cat !{input,,locus_gene_target_file} | cut -f1 | sort -u | wc -l\`; if (\$ngenes <= 2) {print 8} else {print 4 + 2 * \$ngenes}'`  class_level locus skip_if no_coverage

locus_range_buffer=250000

top_common_marker_maf=.05

!!expand:,:snporpos,colnum1,colnum2:snp,2,3:pos,1,2! \
!^expand:;:extraname;extraexec;skiporrunif:\
with_tophits;--exec "$smart_join_cmd --exec \"$smart_cut_cmd !{input,--file,pheno_all_marker_top_hits_file} $locus_chrom_selector(1,CHROM) --exclude-row 1,1 --and-row-all $locus_pos_selector(1,POS) --select-col 1,1,POS | $smart_cut_cmd !{input,--file,locus_combined_tped_file} --select-col 1,4 | sort | uniq -d\" --exec \"$smart_cut_cmd --out-delim $tab !{input,--file,locus_combined_tped_file} --select-col 1,'4 2'\" --extra 2 --in-delim $tab | cut -fcolnum1";run_if:\
without_tophits;;skip_if^ \
local cmd make_locus_top_common_marker_snporpos_file_extraname=$smart_cut_cmd extraexec --exec "$smart_cut_cmd !{input,--file,locus_combined_association_scores_file} --exclude-row 1,1 --select-row 1,1,MAF,ge:$top_common_marker_maf $locus_range_selector(1,POS) --select-col 1,1,'P POS SNP' --exact --require-col-match --out-delim $tab | sort -gk1 | cut -fcolnum2" | awk 'NR == 1' > !{output,,locus_top_common_marker_snporpos_file} class_level locus skiporrunif pheno_all_marker_top_hits_file


ld_window_params=--ld-window-r2 0 --ld-window 1000000000 --ld-window-kb 300000

local cmd make_locus_r2_ld_file=$plink_cmd !{raw,--tfile,locus,*locus_combined_plink_file} !{input,locus_combined_tped_file} !{input,locus_combined_tfam_file} --ld-snp `cat !{input,,locus_top_common_marker_snp_file}` --r2 $ld_window_params !{raw,--out,locus,*locus_r2_trunk} !{output,locus_r2_ld_file} !{output,locus_r2_ld_log_file} class_level locus

cmd make_locus_haploview_marker_ld_file=$smart_join_cmd !{input,--file,locus_top_common_marker_snp_file} --exec "$smart_cut_cmd !{input,--file,locus_haploview_ld_file} --exec \"awk -F $tab -v OFS=$tab '{temp=\\\\$1; \\\\$1=\\\\$2; \\\\$2=temp} {print}' < !{input,,locus_haploview_ld_file}\" --exclude-row 2,1 | $smart_cut_cmd --exclude-row 0,1 --vec-delim : --select-col 0,1,L1:L2:D.:r.. --exact --require-col-match" --extra 2 --multiple 2 | cut -f2- | cat - !{input,,locus_haploview_info_file} | awk -v OFS=$tab 'NF==2 {\$2="NA\tNA"} !map[\$1] {print} {map[\$1]=1}' | sed '1 s/^/SNP\tDprime\tr2\n/' > !{output,,locus_haploview_marker_ld_file} class_level locus

get_associated_marker_variants=$smart_cut_cmd !{input,--file,locus_combined_association_scores_file} --out-delim $tab --select-col 1,1,'POS P' --exact --require-col-match | $smart_cut_cmd --in-delim $tab --select-row 0,2,ge:0 --exclude-row 0,1

get_associated_seq_variants=cat !{input,,locus_var_counts_dat_file} | $smart_cut_cmd --in-delim $tab --select-col 0,1,'POS P' --exact --require-col-match --exclude-row 0,1

seq_type=Sequence
marker_type=Marker

locus_fancy_plot_dat_helper=$smart_join_cmd --in-delim $tab --header 1 --exec \"$smart_cut_cmd !{input,--file,locus_@{1}_bim_file} --select-col 1,4 --exec \\\"${@2} | cut -f1\\\" !{input,--file,locus_r2_ld_file} --select-col 3,1,'BP_B' | sort | uniq -c | awk '\\\\$1 == 3 {print \\\\$2}' | sed '1 s/^/POS\n/'\" --exec \"${@2} | sed 's/$/\t@3/' | sed '1 s/^/POS\tPVAL\tTYPE\n/'\" --exec \"$smart_cut_cmd !{input,--file,locus_r2_ld_file} --out-delim $tab --exact --require-col-match --select-col 1,1,'BP_B R2 SNP_B' | sed '1 s/SNP_B/SNP/' | sed '1 s/R2/RSQR/'\" --extra 2 --extra 3

local cmd make_locus_fancy_plot_dat_file=$smart_cut_cmd --in-delim $tab --exec "$locus_fancy_plot_dat_helper(all_marker,get_associated_marker_variants,$marker_type)" --exec "$locus_fancy_plot_dat_helper(non_marker_seq,get_associated_seq_variants,$seq_type) | tail -n+2" | sed 's/\(.*\)\(\s\s*\)\(\S\S*\)$/\3\2\1/' | $smart_cut_cmd --in-delim $tab --select-row 0,1 $locus_range_selector(0,POS) > !{output,,locus_fancy_plot_dat_file} class_level locus

!!expand:,:keytype,extra_cmd,snpperkb:_marker,,.2:,markers.type=$seq_type,2! \
local cmd make_locus_fancykeytype_plot_pdf_file=$draw_fancy_locus_plot_cmd !{input,in.file=,locus_fancy_plot_dat_file} !{output,out.file=,locus_fancykeytype_plot_pdf_file} snp=`cat !{input,,locus_top_common_marker_snp_file}` title=!{prop,,locus,disp} !{prop,chrom=,locus,chrom} range=8 recomb=$genetic_map_file(!{prop\,\,locus\,chrom}) !{input,gene.list=,locus_gene_target_file} gene.strand.list=$genetic_strand_file(!{prop\,\,locus\,chrom}) extra_cmd imputed.type=$marker_type snp.per.10kb=snpperkb !{input,locus_range_info_file} class_level locus

cmd make_locus_haplotype_burden_files=perl $haplotype_burden_bin_dir/make_index_stratify_file_from_peds.pl !{input,,locus_all_seq_tfam_file} !{input,,locus_all_seq_tped_file} !{input,,locus_all_marker_tfam_file} !{input,,locus_all_marker_tped_file} !{input,,locus_top_common_marker_pos_file} !{input,,pheno_marker_pheno_file} !{output,,locus_strat_index_snp_file} !{output,,locus_index_geno_counts_file} !{prop,,pheno,prevalence} class_level locus run_if and,prevalence,do_hap_burden

prop prevalence=scalar
cmd make_locus_strat_self_snp_file=perl $haplotype_burden_bin_dir/make_self_stratify_file_from_peds.pl !{input,,locus_all_seq_tfam_file} !{input,,locus_all_seq_tped_file} !{input,,locus_all_marker_tfam_file} !{input,,locus_all_marker_tped_file} !{input,,pheno_marker_pheno_file} !{output,,locus_strat_self_snp_file} !{prop,,pheno,prevalence} class_level locus run_if and,prevalence,do_hap_burden

local cmd make_locus_est_variance_explained_file=$r_script_cmd($haplotype_burden_bin_dir/est_var_explained_from_counts.R) !{input,,locus_strat_self_snp_file} !{input,,locus_top_common_marker_pos_file} !{output,,locus_est_variance_explained_file} !{prop,,pheno,prevalence} class_level locus run_if and,prevalence,do_hap_burden

#complicated command because output of haploview must be massaged three ways
#1. Need to get all ids in L1
#2. Need to add in lines for duplicated marker ids
#3. Need to add NAs for SNPs that were missing (failed haploview checks)
local cmd make_locus_est_variance_explained_annot_file=$smart_join_cmd --in-delim $tab --exec "cat !{input,,locus_est_variance_explained_file} | cut -f2- | $format_columns_cmd --in-delim $tab --number-format 1,%d --number-format %.4g" --exec "cut -f2- !{input,,locus_strat_index_snp_file}" --exec "$smart_join_cmd --exec \"sed '1 s/^/SNP\tPOS\n/' !{input,,locus_haploview_info_file}\" !{input,--file,locus_haploview_marker_ld_file} | cut -f2-" --exec "cut -f2 !{input,,locus_strat_index_snp_file}" --extra 1 --extra 3 --header 1 | awk -F"\t" -v OFS="\t" '{tmp = \$2; \$2 = \$1; \$1 = tmp} {print}' > !{output,,locus_est_variance_explained_annot_file} class_level locus

max_conditional_or_maf=.02
local cmd make_locus_conditional_or_file=python $haplotype_burden_bin_dir/calculate_conditional_or.py --index-pos `cat !{input,,locus_top_common_marker_pos_file}` --max-maf $max_conditional_or_maf !{input,--marker-tped,locus_all_marker_tped_file} !{input,--marker-tfam,locus_all_marker_tfam_file} !{input,--seq-tped,locus_all_seq_tped_file} !{input,--seq-tfam,locus_all_seq_tfam_file} !{input,--pheno-file,pheno_non_missing_sample_info_file} > !{output,,locus_conditional_or_file} class_level locus run_if do_hap_burden

or_header=OR
local cmd make_locus_conditional_or_pdf_file=awk 'NR == 1 {for(i = 1; i <= NF; i++) {fields[\$i] = i}} {cur=\$fields["$or_header"]; still_print=1;} NR == 2 {initial=cur} {if (still_print && NR <= 2 || (NR > 2 && (cur - prev) * (initial - 1) < 0)) {print; prev=cur} else {still_print=0}}' !{input,,locus_conditional_or_file} | $r_script_cmd($haplotype_burden_bin_dir/draw_conditional_or.R) /dev/stdin !{output,,locus_conditional_or_pdf_file} OR OR_LB OR_UB !{prop,,locus,disp} class_level locus run_if do_hap_burden


#gene cmds
prop gene_start=scalar
prop gene_end=scalar

local cmd make_gene_pheno_seq_plink_files=$plink_cmd $plink_in_bed_helper(pheno) $kb_range(gene, 1) --make-bed $plink_out_bed_helper(gene_pheno_seq) && $plink_mv_log_cmd(!{raw\,\,gene\,*gene_pheno_seq_plink_file},!{output\,\,gene_pheno_seq_make_bed_log_file}) class_level gene

ld_window=50000
local cmd make_gene_all_ld_plink_files=$plink_cmd !{input,--tped,locus_combined_tped_file} !{input,--tfam,locus_combined_tfam_file} !{input,--pheno,pheno_plink_alternate_phe_file,if_prop=pheno_qt,allow_empty=1} !{raw,--pheno-name,pheno,$pheno_half_for_raw,if_prop=pheno_qt,allow_empty=1} !{raw,--out,gene,*gene_all_ld_plink_file} !{output,gene_all_ld_ped_file} !{output,gene_all_ld_map_file} --recode $kb_range(gene,$ld_window) --output-missing-phenotype 0 && $plink_mv_log_cmd(!{raw\,\,gene\,*gene_all_ld_plink_file},!{output\,\,gene_all_ld_recode_log_file}) class_level gene

max_num_ld_plot=150
get_all_ld_haploview_snp_exclude_file=perl -lane '++\$num; print abs(\$F[3] - (!{prop,,gene,gene_start} + !{prop,,gene,gene_end}) / 2) . "\t\$num\t"' !{input,,gene_all_ld_map_file} | sort -nk1 | awk 'NR > $max_num_ld_plot {print \$2}' | tr '\n' ',' | sed 's/,$//' | sed 's/^/-excludeMarkers /'

local cmd make_gene_all_ld_info_file=perl -pe 'BEGIN {open IN, "!{input,,gene_seq_ld_info_file}" or die; while (<IN>) {chomp; @a = split; \$map{\$a[1]} = \$a[0]} } chomp; @a = split; if (\$map{\$a[3]}) {\$a[1] = "\*\*\$map{\$a[3]}\*\*"; \$_ = join(" ", @a)} \$_ .= "\n" ' < !{input,,gene_all_ld_map_file} | awk '{print \$2,\$4}' > !{output,,gene_all_ld_info_file} class_level gene

local cmd make_gene_seq_ld_plink_files=$plink_cmd $plink_in_bed_helper(gene_pheno_seq) !{input,--pheno,pheno_plink_alternate_phe_file,if_prop=pheno_qt,allow_empty=1} !{raw,--pheno-name,pheno,$pheno_half_for_raw,if_prop=pheno_qt,allow_empty=1} !{raw,--out,gene,*gene_seq_ld_plink_file} !{output,gene_seq_ld_ped_file} !{output,gene_seq_ld_map_file} --recode --output-missing-phenotype 0 && $plink_mv_log_cmd(!{raw\,\,gene\,*gene_seq_ld_plink_file},!{output\,\,gene_seq_ld_recode_log_file}) class_level gene

local cmd make_gene_seq_ld_info_file=$smart_join_cmd --in-delim $tab --rest-extra 1 --exec "$smart_cut_cmd --in-delim $tab !{input,--file,gene_seq_ld_map_file} !{input,--file,gene_var_counts_dat_file} --select-col 1,4 --select-col 2,2 | sort -u | $smart_cut_cmd --in-delim $tab !{input,--file,gene_seq_ld_map_file} --select-col 1,4 | sort | uniq -d" --exec "$smart_cut_cmd --out-delim $tab !{input,--file,gene_seq_ld_map_file} --select-col 1,4" --exec "$smart_join_cmd --exec \"$smart_cut_cmd --in-delim $tab !{input,--file,gene_var_counts_dat_file} --select-col 1,1,'POS ID MINA MINU' --exclude-row 1,1 --exact\" --exec \"$smart_cut_cmd --out-delim $tab !{input,--file,gene_seq_ld_map_file} --select-col 1,'4 2' | sed 's/$/\tNA\tNA/'\" --merge" | sed 's/^\(.*\)\s\s*\(\S\S*\)\s\s*\(\S\S*\)$/\1-Ca:\2-Ct:\3/' | $smart_cut_cmd --in-delim $tab --select-col 0,'2 1'  > !{output,,gene_seq_ld_info_file} class_level gene

!!expand:,:keytype,excludecmd:all,`$get_all_ld_haploview_snp_exclude_file`:seq,! \
haploview_keytype_cmd=java -Xmx$haploview_mem -jar $haploview_jar -nogui !{input,-pedfile,gene_keytype_ld_ped_file} !{input,-info,gene_keytype_ld_info_file} -@1 !{raw,-out,gene,*gene_keytype_ld_png_trunk} !{output,gene_keytype_ld_png_file} -skipcheck excludecmd

haploview_mem=1g
#!!expand:,:keytype,excludecmd:all,`$get_all_ld_haploview_snp_exclude_file`:seq,! \
#short cmd make_gene_keytype_ld_png_file=rm -f !{output,,gene_keytype_ld_png_file} && $open_vnc_display && (($haploview_keytype_cmd(png) && ls !{output,,gene_keytype_ld_png_file}) || ($haploview_keytype_cmd(compressedpng) && ls !{output,,gene_keytype_ld_png_file})); $close_vnc_display class_level gene

#!!expand:keytype:all:seq! \
#local cmd make_gene_keytype_ld_pdf_file=convert !{input,,gene_keytype_ld_png_file} !{output,,gene_keytype_ld_pdf_file} class_level gene

gene_impute_summary_helper=(head -q -n1 /dev/null !{input,,variant_@{1}_summary_file,allow_empty=1} | awk 'NR == 1' && tail -q -n+2 /dev/null !{input,,variant_@{1}_summary_file,allow_empty=1})

local cmd make_gene_hap_summary_file=$gene_impute_summary_helper(hap) > !{output,,gene_hap_summary_file} class_level gene

local cmd make_gene_traditional_summary_file=$smart_join_cmd --in-delim $tab --header 1 --exec "$gene_impute_summary_helper(traditional) | tail -n+2 | $smart_cut_cmd !{input,--file,locus_combined_snptest_out_file} --exclude-row 1,1 --out-delim $tab | cut -f3 | sort | uniq -d | sed '1 s/^/ID\n/'" --exec "$gene_impute_summary_helper(traditional)" --exec "$smart_cut_cmd --out-delim $tab !{input,--file,locus_combined_snptest_out_file}  --select-col 1,1,'rsid pos cases_maf controls_maf $snptest_pvalue_col info' --exact --require-col-match | sed '1 s/^.*$/SNP\tPOS\tF_CASE_SCORE\tF_CONTROL_SCORE\tP_SCORE\tINFO/'" --extra 2 --extra 3 --col 2,3 --col 3,2 | cut -f2- > !{output,,gene_traditional_summary_file} class_level gene

#cmd make_gene_vars_file=$smart_join_cmd --exec "$smart_cut_cmd --in-delim $tab !{input,--file,project_clean_gene_variant_file} --select-col 1,1,'ID GENE CHROM POS' --exact" --exec "cat !{input,,project_full_var_annot_file} | $smart_cut_cmd --select-col 0,1,'ID $vcf_type_annot' --exact" !{input,--file,project_pph2_file} --extra 1 --extra 3 --comment \\# --header 1 --out-delim $tab | $smart_cut_cmd --in-delim $tab --select-row 0,1,GENE,!{prop,,gene,external_id} --select-col 0,1,'ID CHROM POS $vcf_type_annot $pph2_class_annot' --exact --require-col-match | awk -v OFS="\t" -F"\t" '\$5 == 2 {\$5="possibly damaging"} \$5 == 1 {\$5="benign"} \$5 == 0 && \$4 == "$vcf_type_synonymous_annot" {\$5="NA"} \$5 == 0 && \$4 != "$vcf_type_synonymous_annot" {\$5="unknown"} \$5 == 3 || \$4=="$vcf_type_nonsense_annot" {\$5="probably damaging"} {print}' | sed '1 s/^/snp\tchrom\tposition\tvariant_type\tpolyphen\n/' > !{output,,gene_vars_file} class_level gene

#short cmd make_gene_group_file=$smart_cut_cmd --in-delim $tab !{input,--file,project_clean_gene_variant_file} --select-row 1,1,GENE,!{prop,,gene,external_id} --select-col 1,1,'ID GENE' --exact | sed '1 s/^/snp\tgroup\n/' > !{output,,gene_group_file} class_level gene

#local cmd make_gene_sample_rank_file=$smart_join_cmd --in-delim $tab --exec "$smart_cut_cmd !{input,--file,gene_pheno_seq_fam_file} --select-col 1,1-2 --out-delim $tab | sed '1 s/^/FID\tIID\n/'" !{input,--file,pheno_sample_rank_file} --col 1,2 --extra 2 --header 1 | $smart_cut_cmd --in-delim $tab --select-col 0,'2 1 3' > !{output,,gene_sample_rank_file} class_level gene

#local cmd make_gene_vars_pdf_file=$draw_gene_plot_cmd("!{input,bed.file=,gene_pheno_seq_bed_file} !{input,bim.file=,gene_pheno_seq_bim_file} !{input,fam.file=,gene_pheno_seq_fam_file} !{input,covar.file=,gene_sample_rank_file} pheno.name=$rank_prop_header !{input,vars.file=,gene_vars_file} !{input,group.file=,gene_group_file} !{raw,title=,pheno,'@disp'} !{output,out.file=,gene_vars_pdf_file}") class_level gene

ucsc_gene_buffer=100

local cmd make_gene_ucsc_regions_file=(echo !{prop,,gene,disp}, human, hg$hg_build, chr!{prop,,gene,chrom}, !{prop,,gene,gene_start}, !{prop,,gene,gene_end}, \"!{prop,,gene,disp}\", $ucsc_gene_buffer, $ucsc_gene_buffer) > !{output,,gene_ucsc_regions_file} class_level gene

local cmd make_gene_ucsc_pdf_file=$fetch_ucsc_cmd !{input,-r,gene_ucsc_regions_file} !{input,-t,pheno_ucsc_tracks_file} !{input,-u,pheno_ucsc_browser_file} !{output,--output-file,gene_ucsc_pdf_file} class_level gene

prop use_transcripts=list

short cmd make_gene_variant_list_file=$pseq_qc_plus_all_analysis_cmd_pheno_variant_subset(v-matrix) $show_id --mask $pseq_gene_mask $mono_ex_mask | $split_parsed_id | cut -f1-3 | tail -n+2 | $add_gene_annot_cmd --in-delim $tab --print-multiple --chr-col 1 --pos-col 2 --gene-file !{input,,project_expanded_exon_target_file} --out-delim $tab --gene-file-num-ids 2 --gene-id-join-char $tab | $smart_cut_cmd --in-delim $tab --select-row 0,1,!{prop,,gene,external_id} | cut -f3- | cut -f3 | sort -u > !{output,,gene_variant_list_file} class_level gene

short cmd make_gene_all_annot_file=$smart_join_cmd --in-delim $tab --header 1 --exec "cut -f1 !{input,,project_variant_subset_full_var_annot_file} | sort -u | cat - !{input,,gene_variant_list_file} | sort | uniq -d | sed '1 s/^/ID\n/'" --exec "cat !{input,,project_variant_subset_full_var_annot_file} | awk -F\"\t\" -v OFS=\"\t\" 'NR == 1 {for (i=1;i<=NF;i++) {m[\\$i]=i}} NR > 1 {\\$m[\"$vep_trans_annot\"]=\"$consensus_transcript\"} NR > 1 {print}' | $smart_cut_cmd --in-delim $tab !{input,--file,project_variant_subset_full_annot_file}" --extra 2 --multiple 2 > !{output,,gene_all_annot_file} class_level gene


#Take variants both in burden file and in the gene file; use these to join with
#The detail reg file with non CCDS transcripts that have transcripts in the all annot file

replace_consensus=awk -v OFS=\\\"\t\\\" -F\\\"\t\\\" '\\\\$1 == \\\"!{prop,,gene,external_id}\\\" {\\\\$1=\\\"$consensus_transcript\\\"} {print}'

#get_burden_transcripts=$smart_join_cmd --in-delim $tab --exec "cat !{input,,burden_all_variant_list_file} !{input,,gene_variant_list_file} | sort | uniq -d" --rest-extra 1 --multiple 2 --exec "$smart_join_cmd --in-delim $tab --exec \"$smart_cut_cmd --in-delim $tab !{input,--file,gene_all_annot_file} --select-col 1,1,$vep_trans_annot --select-row 1,1,$vep_trans_annot,eq:$consensus_transcript --select-row 1,1,$vep_ccds_annot,ne:$annot_missing_field --exclude-row 1,1 --exact --require-col-match | sort -u | $smart_cut_cmd --in-delim $tab --exec \\\"$smart_cut_cmd --in-delim $tab !{input,--file,burden_locdb_detail_reg_file} --select-col 1,1,ID --exclude-row 1,1 --exact --require-col-match | sort -u\\\" | sort | uniq -d\" --exec \"$smart_cut_cmd --in-delim $tab !{input,--file,burden_locdb_detail_reg_file} --select-col 1,1,'ID VAR' --exclude-row 1,1 --exact --require-col-match | $replace_consensus\" --extra 2 --multiple 2 | $smart_cut_cmd --in-delim $tab --select-col 0,'2 1'" | cut -f2 | sort -u

get_burden_transcripts=$smart_join_cmd --in-delim $tab --exec "cat !{input,,burden_all_variant_list_file} !{input,,gene_variant_list_file} | sort | uniq -d" --rest-extra 1 --multiple 2 --exec "$smart_cut_cmd --in-delim $tab !{input,--file,burden_locdb_detail_reg_file} --select-col 1,1,'VAR ID' --exclude-row 1,1 --exact --require-col-match" | cut -f2 | awk -v OFS="\t" -F"\t" '\$1 == "!{prop,,gene,external_id}" {\$1="$consensus_transcript"} {print}' | $smart_cut_cmd --in-delim $tab --exec "$smart_cut_cmd !{input,--file,gene_all_annot_file} --select-col 1,1,$vep_trans_annot --select-row 1,1,$vep_trans_annot,eq:$consensus_transcript --select-row 1,1,$vep_ccds_annot,ne:$annot_missing_field --exclude-row 1,1 --exact --require-col-match | sort -u" | sort | uniq -d

short cmd make_gene_burden_transcript_burdens_dat_file=$get_burden_transcripts | sed 's/\(.*\)/\1 !{prop,,gene_burden}/' > !{output,,gene_burden_transcript_burdens_dat_file} class_level gene_burden skip_if use_transcripts with transcript_burden 

short cmd make_custom_gene_transcript_burdens_dat_file=$get_burden_transcripts | $smart_cut_cmd --in-delim $tab --exec "perl -e '{print qq(!{prop,,gene,use_transcripts,sep=\n})}' | sort -u" | sort | uniq -d | sed 's/\(.*\)/\1 !{prop,,gene_burden}/' > !{output,,gene_burden_transcript_burdens_dat_file} class_level gene_burden run_if use_transcripts with transcript_burden

local cmd make_gene_transcript_burdens_meta_file=cat !{input,,gene_burden_transcript_burdens_dat_file} | sed 's/\(\S\S*\)\s\s*\(\S\S*\)/\1 \2 !{prop,,gene}_\1/' | sed 's/^\(\S\S*\)\s\s*\(\S\S*\)\s\s*\(\S\S*\)/\3 class transcript_burden\n!select:!{prop,,project}:\2 \3 parent !{prop,,gene}\n\3 disp \1\n\3 external_id \1/' > !{output,,gene_transcript_burdens_meta_file} class_level gene

vassoc_subset_helper=$smart_cut_cmd !{input,--file,pheno_vassoc_annot_file} --in-delim $tab --select-row 1,1,$vcf_type_annot,@1 --and-row-all | sed 's/$/\t@2/g'

max_gene_var_counts_maf=.1

gene_var_counts_selector=$smart_cut_cmd --in-delim $tab --select-row 0,1 --select-row 0,1,GENE,!{prop,,gene,external_id} --exact

locus_var_counts_selector=$smart_cut_cmd --in-delim $tab --select-row 0,1 --select-row 0,1,CHROM,!{prop,,locus,ref_chrom} $locus_pos_selector(0,POS) --and-row-all !{input,locus_range_info_file}

var_counts_nonsense_type=Nonsense
var_counts_missense_type=Missense
var_counts_synonymous_type=Synonymous
var_counts_other_type=Other


var_counts_types="$var_counts_synonymous_type,$var_counts_missense_type,$var_counts_other_type,$var_counts_nonsense_type"

var_counts_dat_helper=$smart_join_cmd --in-delim $tab --extra 1 \
   									--exec "$smart_cut_cmd --in-delim $tab !{input,--file,project_clean_gene_variant_file} --select-col 1,1,'ID' --exact --require-col-match" \
         --exec "$smart_cut_cmd --in-delim $tab --out-delim $tab \
				 --exec \"$smart_cut_cmd !{input,--file,pheno_vassoc_annot_file} --in-delim $tab --select-row 1,1 | sed 's/$/\tVAR_TYPE/'\" \
"""
}
    