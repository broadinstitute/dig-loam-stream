
package loamstream.compiler

/**
 * LoamStream
 * Created by oliverr on 10/6/2015.
 */
object PipelineTargetedPart16 {
  val string =
 """# 3. The extension appended
# 4. The log file

!!expand:qctype:qc_pass:qc_plus! \
!!expand:,:projectt,projecte:project,:project_sample_subset,_project_sample_subset:project_variant_subset,_project_variant_subset! \
plink_analysis_qctype_bed_cmdprojecte=$plink_qctype_bed_cmdprojecte $plink_analysis_helperprojecte(@1,@2,@3,@4)

!!expand:qctype:qc_pass_unthresholded! \
!!expand:,:projectt,projecte:project,:project_variant_subset,_project_variant_subset! \
plink_analysis_qctype_bed_cmdprojecte=$plink_qctype_bed_cmdprojecte $plink_analysis_helperprojecte(@1,@2,@3,@4)

!!expand:,:phenol:pheno:pheno_variant_subset! \
plink_analysis_phenol_bed_cmd=$plink_phenol_bed_cmd $plink_analysis_helper(@1,@2,@3,@4)

#plink_base_analysis_qc_pass_bed_cmd=$plink_qc_pass_bed_cmd !{raw,--out,project,*project_plinkseq_qc_pass_plink_file} --@1 @2 && $plink_mv_log_cmd(!{raw\,\,project\,*project_plinkseq_qc_pass_plink_file},!{output\,\,project_plinkseq_@{1}_log_file})
#plink_analysis_qc_pass_bed_cmd=$plink_base_analysis_qc_pass_bed_cmd("@1",!{output\,project_plinkseq_@{1}_file})

!!expand:qctype:qc_pass:qc_plus! \
!!expand:,:projectt,projecte:project,:project_variant_subset,_project_variant_subset! \
projectt_plinkseq_qctype_frq_file_helper=$plink_analysis_qctype_bed_cmdprojecte(projectt_plinkseq_qctype_frq_file,freq --nonfounders,frq,projectt_plinkseq_qctype_frq_log_file)

!!expand:qctype:qc_pass:qc_plus! \
!!expand:,:projectt,projecte:project,:project_variant_subset,_project_variant_subset! \
projectt_plinkseq_qctype_strata_frq_file_helper=$plink_analysis_qctype_bed_cmdprojecte(projectt_plinkseq_qctype_strata_frq_file,freq --nonfounders !{input;--remove;project_plink_sample_exclude_file} !{input;--within;pheno_plink_phe_file;if_prop=pheno:eq:@maf_strata_trait},frq.strat,projectt_plinkseq_qctype_strata_frq_log_file)

!!expand:qctype:qc_pass:qc_plus! \
!!expand:,:projectt,projecte:project,:project_variant_subset,_project_variant_subset! \
projectt_plinkseq_qctype_counts_file_helper=$smart_join_cmd --exec "awk -v OFS=$tab '{print \\$2,\\$1,\\$4}' !{input,,projectt_plinkseq_qctype_bim_file}" --exec "awk -v OFS=$tab 'NR > 1 {print \\$2,int(\\$5*\\$6+.5)}' !{input,,projectt_plinkseq_qctype_frq_file}" --in-delim $tab | sed '1 s/^/ID\tCHROM\tPOS\tCOUNT\n/' > !{output,,projectt_plinkseq_qctype_counts_file}

!!expand:,:frq,runif:frq,:strata_frq,run_if maf_strata_trait:counts,! \
!!expand:,:projectt,projecte,skipif:project,,skip_if num_var_subsets:project_variant_subset,_project_variant_subset,bsub_batch 20! \
short cmd make_projectt_plinkseq_qc_pass_frq_file=$projectt_plinkseq_qc_pass_frq_file_helper class_level projectt skipif runif

!!expand:,:projectt,projecte,skipif:project,,skip_if num_var_subsets:project_variant_subset,_project_variant_subset,! \
short cmd make_projectt_multiallelic_frq_file=$multiallelic_qc_cmd !{input,,project_sample_include_file} -spl_field:1 -out:!{output,,projectt_multiallelic_frq_file} -condition_on:$thresholded_nalt_field -var_mask:"file=$pseq_multiallelic_tag" -pseq_project:!{input,,projectt_plinkseq_project_file} class_level projectt skipif 

!!expand:,:frq,runif:frq,:strata_frq,run_if maf_strata_trait:counts,! \
short cmd make_project_plinkseq_qc_plus_frq_file=$project_plinkseq_qc_plus_frq_file_helper class_level project run_if or,sample_qc_filter,var_qc_filter,project_variant_custom_exclude_file,project_sample_custom_exclude_file skip_if num_var_subsets runif

!!expand:frq:frq:strata_frq! \
short cmd make_project_variant_subset_plinkseq_qc_plus_frq_file=if [[ `cat !{input,,project_variant_subset_plinkseq_qc_plus_bim_file} | wc -l` -gt 0 ]]; then $project_variant_subset_plinkseq_qc_plus_frq_file_helper; else head -n1 !{input,,project_variant_subset_plinkseq_qc_pass_frq_file} > !{output,,project_variant_subset_plinkseq_qc_plus_frq_file} && echo > !{output,,project_variant_subset_plinkseq_qc_plus_frq_log_file}; fi class_level project_variant_subset run_if or,sample_qc_filter,var_qc_filter,project_variant_custom_exclude_file,project_sample_custom_exclude_file

combined_frq_helper=$smart_join_cmd --col 2 --header 1 --exec "$smart_join_cmd --header 1 --exec \"$smart_cut_cmd !{input,--file,@1} !{input,--file,@2} --select-col 1,1,SNP --select-col 2,1 --exclude-row 1-2,1 | sort | uniq -d | sed '1 s/^/SNP\n/'\" !{input,--file,@2} !{input,--file,@1} --col 3,2 --rest-extra 1 | $smart_cut_cmd --in-delim $tab --exact --require-col-match --select-col 0,1,'CHR SNP A1 A2 aaf n_tot_samp' --empty-okay | sed '1 s/aaf/MAF/' | sed '1 s/n_tot_samp/NCHROBS/'" !{input,--file,@1} --merge | awk -F"\t" -v OFS="\t" '{t=\$1; \$1=\$2; \$2=t} {print}'

!!expand:,:projectt,projecte,skipif:project,,skip_if num_var_subsets:project_variant_subset,_project_variant_subset,bsub_batch 30! \
!!expand:;:qctype;runif:qc_pass;:qc_plus;run_if or,sample_qc_filter,var_qc_filter,project_variant_custom_exclude_file,project_sample_custom_exclude_file! \
short cmd make_projectt_plinkseq_qctype_combined_frq_file=$combined_frq_helper(projectt_plinkseq_qctype_frq_file,projectt_multiallelic_frq_file) > !{output,,projectt_plinkseq_qctype_combined_frq_file} class_level projectt runif skipif

!!expand:,:projectt,projecte,skipif:project,,skip_if num_var_subsets:project_variant_subset,_project_variant_subset,bsub_batch 30! \
!!expand:;:qctype;runif:qc_pass;:qc_plus;run_if or,sample_qc_filter,var_qc_filter,project_variant_custom_exclude_file,project_sample_custom_exclude_file! \
short cmd make_projectt_plinkseq_qctype_combined_strata_frq_file=$smart_join_cmd --merge --header 1 --col 2 --exec "$smart_join_cmd --header 1 --exec \"$smart_cut_cmd !{input,--file,projectt_plinkseq_qctype_strata_frq_file} !{input,--file,project_multiallelic_strata_frq_file} --select-col 1,1,'SNP CLST' --select-col 2,1,'ID Strata' --exclude-row 1-2,1 | sort | uniq -d | sed '1 s/^/SNP CLST\n/'\" --exec \"sed '1 s/MAF/MAF_1/' !{input,,projectt_plinkseq_qctype_strata_frq_file}\" !{input,--file,project_multiallelic_strata_frq_file} --col 1,1 --col 1,2 --col 2,2 --col 2,3 --col 3,1 --col 3,2 --rest-extra 1 | $smart_cut_cmd --in-delim $tab --exact --require-col-match --select-col 0,1,'CHR SNP CLST A1 A2 MAF N' | sed '1 s/\tN$/\tNCHROBS/' | $add_function_cmd --in-delim $tab --header 1 --col1 MAF --col2 NCHROBS --round --type multiply --val-header MAC --add-at NCHROBS" !{input,--file,projectt_plinkseq_qctype_strata_frq_file} | awk -F"\t" -v OFS="\t" '{t=\$1; \$1=\$2; \$2=t} {print}' > !{output,,projectt_plinkseq_qctype_combined_strata_frq_file} class_level projectt runif skipif

short cmd make_project_variant_subset_plinkseq_qc_plus_counts_file=if [[ `cat !{input,,project_variant_subset_plinkseq_qc_plus_bim_file} | wc -l` -gt 0 ]]; then $project_variant_subset_plinkseq_qc_plus_counts_file_helper; else head -n1 !{input,,project_variant_subset_plinkseq_qc_pass_counts_file} > !{output,,project_variant_subset_plinkseq_qc_plus_counts_file}; fi class_level project_variant_subset run_if or,sample_qc_filter,var_qc_filter,project_variant_custom_exclude_file,project_sample_custom_exclude_file

!!expand:frq:frq:strata_frq:counts! \
!!expand:,:projectt,skipif:project,skip_if num_var_subsets:project_variant_subset,! \
local cmd ln_projectt_plinkseq_qc_plus_frq_file=ln -s !{input,,projectt_plinkseq_qc_pass_frq_file} !{output,,projectt_plinkseq_qc_plus_frq_file} class_level projectt skipif run_if and,!sample_qc_filter,!var_qc_filter,!project_variant_custom_exclude_file,!project_sample_custom_exclude_file

!!expand:,:projectt,skipif:project,skip_if num_var_subsets:project_variant_subset,! \
!!expand:frq:frq:strata_frq! \
local cmd ln_projectt_plinkseq_qc_plus_combined_frq_file=ln -s !{input,,projectt_plinkseq_qc_pass_combined_frq_file} !{output,,projectt_plinkseq_qc_plus_combined_frq_file} class_level projectt run_if and,!sample_qc_filter,!var_qc_filter,!projectt_variant_custom_exclude_file,!projectt_sample_custom_exclude_file skipif

short cmd make_project_multiallelic_strata_frq_file=$smart_cut_cmd !{raw;;pheno_variant_qc_strata;--exec "sed '1! s/$/\t@meta_strata_value/' *pheno_variant_qc_strata_multiallelic_stats_file | sed '1 s/$/\t@pheno/' | $smart_cut_cmd --select-col 0,1,'id @pheno n_tot_samp aaf' --exclude-row 0,1";if_prop=pheno:eq:@maf_strata_trait} !{input,pheno_variant_qc_strata_multiallelic_stats_file,if_prop=pheno:eq:@maf_strata_trait} | $replace_nan(NA) | sed '1 s/^/ID\tStrata\tN\tMAF\n/g' > !{output,,project_multiallelic_strata_frq_file} class_level project run_if maf_strata_trait

!!expand:qc_plus:qc_plus:qc_pass! \
!!expand:,:projectt,projecte,skipif:project,,skip_if num_var_subsets:project_variant_subset,_project_variant_subset,bsub_batch 10! \
short cmd make_projectt_plinkseq_qc_plus_strata_frq_summary_file=$table_sum_stats_cmd !{input,,projectt_plinkseq_qc_plus_combined_strata_frq_file} --has-header --print-header --col MAF --group-col SNP --means --summaries > !{output,,projectt_plinkseq_qc_plus_strata_frq_summary_file} class_level projectt skipif run_if maf_strata_trait

!!expand:qctype:qc_pass:qc_plus! \
qctype_count_mask_helper=$smart_cut_cmd !{input,--file,project_plinkseq_qctype_counts_file} --in-delim $tab --exclude-row 1,1 --select-col 1,1,'CHROM POS' --select-row 1,1,COUNT,eq:@1 | sed 's/\s\s*/:/' |sed 's/^/chr/' | sed 's/^chrchr/chr/'

!!expand:qctype:qc_pass:qc_plus! \
short cmd make_project_plinkseq_qctype_sing_reg_list_file=$qctype_count_mask_helper(1) > !{output,,project_plinkseq_qctype_sing_reg_list_file} class_level project

short cmd make_project_plinkseq_qc_pass_doub_reg_list_file=$qc_pass_count_mask_helper(2) > !{output,,project_plinkseq_qc_pass_doub_reg_list_file} class_level project


het_freq_threshold=.01

read_freq_options=

!!expand:,:_frequency,flag:,:_high,--maf $het_freq_threshold! \
!!expand:,:projectt,projecte,runif,shortt:project,,skip_if num_samp_subsets,:project_sample_subset,_project_sample_subset,bsub_batch 10,short! \
shortt cmd make_projectt_plinkseq_frequency_het_file=$plink_analysis_qc_pass_bed_cmdprojecte(projectt_plinkseq_frequency_het_file,het flag --read-freq !{input::project_plinkseq_qc_pass_frq_file} $read_freq_options,het,projectt_plinkseq_frequency_het_log_file) class_level projectt runif rusage_mod $projectt_plinkseq_bfile_mem

!!expand:,:projectt,projecte,skipif,shortt:project,,skip_if num_samp_subsets,:project_sample_subset,_project_sample_subset,,short! \
shortt cmd make_projectt_plinkseq_sexcheck_file=$plink_cmd !{input,--bed,projectt_plinkseq_qc_pass_bed_file} !{input,--bim,projectt_plinkseq_qc_pass_bim_file} !{input,--fam,projectt_plinkseq_qc_pass_with_sex_fam_file} $plink_analysis_helperprojecte(projectt_plinkseq_sexcheck_file,check-sex,sexcheck,projectt_plinkseq_sexcheck_log_file) class_level projectt skipif run_if pheno;pheno:eq:sex rusage_mod $projectt_plinkseq_bfile_mem

!!expand:,:projectt,projecte,skipif,shortt:project,,skip_if num_samp_subsets,:project_sample_subset,_project_sample_subset,,short! \
local cmd make_dummy_projectt_plinkseq_sexcheck_file=awk '{print \$1,\$2,0,0,"PROBLEM","NA"}' !{input,,projectt_plinkseq_qc_pass_fam_file} | sed '1 s/^/FID IID PEDSEX SNPSEX STATUS F\n/' > !{output,,projectt_plinkseq_sexcheck_file} class_level projectt skipif run_if !pheno;pheno:eq:sex 

het_add_function_helper=$add_function_cmd --header 1 --col1 '@1)' --col2 '@{1}_HIGH' --type subtract --val-header '@{1}_LOW'

!!expand:,:projectt,projecte,runif,shortt:project,,skip_if num_samp_subsets,:project_sample_subset,_project_sample_subset,bsub_batch 10,short! \
shortt cmd make_projectt_plinkseq_low_het_file=$smart_join_cmd !{input,--file,projectt_plinkseq_het_file} --exec "sed '1 s/\(\S\S*\)/\1_HIGH/g' !{input,,projectt_plinkseq_high_het_file}" --header 1 --col 1 --col 2 | $add_function_cmd --header 1 --col1 'O(HOM)' --col2 'O(HOM)_HIGH' --type subtract --val-header 'O(HOM)_LOW' | $add_function_cmd --header 1 --col1 'E(HOM)' --col2 'E(HOM)_HIGH' --type subtract --val-header 'E(HOM)_LOW' | $add_function_cmd --header 1 --col1 'N(NM)' --col2 'N(NM)_HIGH' --type subtract --val-header 'N(NM)_LOW' | $smart_cut_cmd --exact --require-col-match --select-col 0,1,'FID IID O.HOM._LOW E.HOM._LOW N.NM._LOW' | sed '1 s/^/_LOW/g' > !{output,,projectt_plinkseq_low_het_file} class_level projectt runif

!!expand:,:projectt,projecte,skipif,shortt:project,,skip_if num_samp_subsets,:project_sample_subset,_project_sample_subset,,short! \
!|expand;,;maskkey,varmask;all,;syn,--mask reg.req=@!{input::project_plinkseq_syn_reg_file};ns,--mask reg.req=@!{input::project_plinkseq_ns_reg_file};nonsense,--mask reg.req=@!{input::project_plinkseq_nonsense_reg_file};noncoding,--mask reg.ex=@!{input::project_plinkseq_coding_reg_file}| \
!|expand:;:tname;runif:raw;:qc_pass;:qc_plus;run_if or,sample_qc_filter,var_qc_filter,project_variant_custom_exclude_file,project_sample_custom_exclude_file| \ 
shortt cmd make_projectt_plinkseq_tname_maskkey_istats_file=rm -f !{output,,projectt_plinkseq_tname_maskkey_istats_file} && $pseq_tname_analysis_cmdprojecte(i-stats) varmask > !{output,,projectt_plinkseq_tname_maskkey_istats_file} class_level projectt runif skipif

!!expand:,:projectt,projecte,skipif,shortt:project,,skip_if num_samp_subsets,:project_sample_subset,_project_sample_subset,,short! \
!!expand;maskkey;all;syn;ns;nonsense;noncoding;sing! \
local cmd ln_projectt_plinkseq_qc_plus_maskkey_istats_file=rm -f !{output,,projectt_plinkseq_qc_plus_maskkey_istats_file} && ln -s !{input,,projectt_plinkseq_qc_pass_maskkey_istats_file} !{output,,projectt_plinkseq_qc_plus_maskkey_istats_file} class_level projectt skipif run_if and,!sample_qc_filter,!var_qc_filter,!project_variant_custom_exclude_file,!project_sample_custom_exclude_file

!!expand:,:projectt,projecte,runif,shortt:project,,skip_if num_samp_subsets,:project_sample_subset,_project_sample_subset,,short! \
shortt cmd make_projectt_plinkseq_qc_pass_doub_istats_file=$pseq_qc_pass_analysis_cmdprojecte(i-stats) --mask reg.req=@!{input,,project_plinkseq_qc_pass_doub_reg_list_file} > !{output,,projectt_plinkseq_qc_pass_doub_istats_file} class_level projectt runif

!!expand:,:projectt,projecte,runif,shortt:project,,skip_if num_samp_subsets,:project_sample_subset,_project_sample_subset,,short! \
shortt cmd make_projectt_plinkseq_qc_pass_indel_istats_file=$pseq_qc_pass_analysis_cmdprojecte(i-stats) --mask file=$pseq_indel_tag > !{output,,projectt_plinkseq_qc_pass_indel_istats_file} class_level projectt runif

!!expand:,:projectt,projecte,runif,shortt:project,,skip_if num_samp_subsets,:project_sample_subset,_project_sample_subset,,short! \
shortt cmd make_projectt_plinkseq_qc_pass_multiallelic_istats_file=$pseq_qc_pass_analysis_cmdprojecte(i-stats) --mask file=$pseq_multiallelic_tag > !{output,,projectt_plinkseq_qc_pass_multiallelic_istats_file} class_level projectt runif

!!expand:,:projectt,projecte,runif,shortt:project,,skip_if num_samp_subsets,:project_sample_subset,_project_sample_subset,,short! \
shortt cmd make_projectt_plinkseq_qc_pass_snp_istats_file=$pseq_qc_pass_analysis_cmdprojecte(i-stats) --mask file=$pseq_snp_tag > !{output,,projectt_plinkseq_qc_pass_snp_istats_file} class_level projectt runif

!|expand:;:tname;runif:qc_pass;:qc_plus;run_if or,sample_qc_filter,var_qc_filter,project_variant_custom_exclude_file,project_sample_custom_exclude_file| \ 
!!expand:,:projectt,projecte,skipif,shortt:project,,skip_if num_samp_subsets,:project_sample_subset,_project_sample_subset,,short! \
shortt cmd make_projectt_plinkseq_tname_sing_istats_file=rm -f !{output,,projectt_plinkseq_tname_sing_istats_file} && $pseq_tname_analysis_cmdprojecte(i-stats) --mask reg.req=@!{input,,project_plinkseq_tname_sing_reg_list_file} > !{output,,projectt_plinkseq_tname_sing_istats_file} class_level projectt runif skipif

#cmd make_plinkseq_qc_pass_istats_file=$pseq_qc_pass_analysis_cmd(i-stats) > !{output,,project_plinkseq_qc_pass_istats_file} class_level project

!!expand:,:projectt,projecte,runif,shortt:project,,skip_if num_samp_subsets,:project_sample_subset,_project_sample_subset,,short! \
shortt cmd make_projectt_plinkseq_filtered_istats_file=$pseq_filtered_analysis_cmdprojecte(i-stats) > !{output,,projectt_plinkseq_filtered_istats_file} class_level projectt runif

prop no_filters=scalar

!!expand:,:projectt,projecte,runif,shortt:project,,skip_if num_samp_subsets,:project_sample_subset,_project_sample_subset,,short! \
!|expand:;:metatype;metaname;addmask:gq;GQ;:dp;DP;:ab;AB;:sing_ab;AB;--mask reg.req=@!{input,,project_plinkseq_qc_pass_sing_reg_list_file}| \
shortt cmd make_projectt_plinkseq_metatype_vmetamatrix_file=$pseq_qc_pass_analysis_cmdprojecte(v-meta-matrix) $show_id addmask --name metaname > !{output,,projectt_plinkseq_metatype_vmetamatrix_file} class_level projectt runif rusage_mod $pseq_mem

mem

!!expand:,:projectt,projecte,runif,shortt:project,,skip_if num_samp_subsets,:project_sample_subset,_project_sample_subset,,short! \
!!expand:,:metatype,metaname:dp,DP! \
shortt cmd make_projectt_plinkseq_metatype_qcfail_vmetamatrix_file=$pseq_filtered_analysis_cmdprojecte(v-meta-matrix) $show_id --name metaname > !{output,,projectt_plinkseq_metatype_qcfail_vmetamatrix_file} class_level projectt runif

!!expand:,:type,filet:snps,$pseq_snp_tag:indels,$pseq_indel_tag:multiallelics,$pseq_multiallelic_tag! \
!!expand:,:projectt,projecte,runif,shortt\
:project,,skip_if num_var_subsets,\
:project_variant_subset,_project_variant_subset,,short! \
shortt cmd make_projectt_plinkseq_type_vmatrix_file=$pseq_filter_only_samples_no_gq_mask_analysis_cmdprojecte(v-matrix) $show_id --mask file=filet $filter_samples_mask  > !{output,,projectt_plinkseq_type_vmatrix_file} class_level projectt runif

!!expand:,:projectt,projecte,runif,shortt\
:project,,skip_if num_samp_subsets,\
:project_sample_subset,_project_sample_subset,,short! \
shortt cmd make_projectt_plinkseq_qcfail_vmatrix_file=$pseq_filtered_analysis_cmdprojecte(v-matrix) $show_id > !{output,,projectt_plinkseq_qcfail_vmatrix_file} class_level projectt runif

summarize_sample_helper=perl $targeted_bin_dir/summarize_vmeta.pl --by-sample --type @1 --id-head ID --val-head @2 @3

!!expand:,:projectt,projecte:project,:project_sample_subset,_project_sample_subset! \
plinkseq_sample_meta_cmd_no_head_no_cacheprojecte=${pseq_@{1}_analysis_cmdprojecte}(v-meta-matrix) $show_id --name @2 | $summarize_sample_helper(@3,@4,@5)
!!expand:,:projectt,projecte:project,:project_sample_subset,_project_sample_subset! \
plinkseq_sample_meta_cmd_no_headprojecte=cat !{input,,projectt_plinkseq_@{1}_vmetamatrix_file} | $summarize_sample_helper(@2,@3,@4)
!!expand:,:projectt,projecte:project,:project_sample_subset,_project_sample_subset! \
plinkseq_sample_meta_cmdprojecte=$plinkseq_sample_meta_cmd_no_headprojecte(@1,@2,@{3}_@1,@4)
!!expand:,:projectt,projecte:project,:project_sample_subset,_project_sample_subset! \
plinkseq_mean_sample_meta_cmd_no_cacheprojecte=$plinkseq_sample_meta_cmd_no_head_no_cacheprojecte(@1,@2,mean,@3,@4)
!!expand:,:projectt,projecte:project,:project_sample_subset,_project_sample_subset! \
plinkseq_mean_sample_meta_cmdprojecte=$plinkseq_sample_meta_cmdprojecte(@1,mean,MEAN,)
!!expand:,:projectt,projecte:project,:project_sample_subset,_project_sample_subset! \
plinkseq_mean_sample_meta_cmd_no_headprojecte=$plinkseq_sample_meta_cmd_no_headprojecte(@1,mean,@2,@3)
!!expand:,:projectt,projecte:project,:project_sample_subset,_project_sample_subset! \
plinkseq_dev_sample_meta_cmd_no_headprojecte=$plinkseq_sample_meta_cmd_no_headprojecte(@1,dev,@2,@3)

!!expand:,:projectt,projecte:project,:project_sample_subset,_project_sample_subset:project_variant_subset,_project_variant_subset! \
plinkseq_variant_meta_nohead_cmdprojecte=$pseq_filter_only_samples_analysis_cmdprojecte(v-meta-matrix) $show_id --name @1 | perl $targeted_bin_dir/summarize_vmeta.pl --val-head @2

!!expand:,:projectt,projecte:project,:project_sample_subset,_project_sample_subset:project_variant_subset,_project_variant_subset! \
plinkseq_variant_meta_cmdprojecte=$plinkseq_variant_meta_nohead_cmdprojecte(@1 @2,@1)

!!expand:,:projectt,projecte:project,:project_sample_subset,_project_sample_subset! \


compute_het_cmd=$smart_cut_cmd !{input,--file,@1} --select-col 1,1,IID --select-col 1,1,O.HOM --select-col 1,1,NM --out-delim $tab | $add_function_cmd --col1 3 --col2 2 --header 1 --type subtract --in-delim $tab | $add_function_cmd --col1 4 --col2 3 --header 1 --type divide --in-delim $tab --val-header HET@2 | cut -f1,5

ab_col=MEAN_ABM50
min_ab_dp=100

!!expand:,:projectt,projecte:project,:project_sample_subset,_project_sample_subset! \
make_plinkseq_extra_istats_helperprojecte=$smart_join_cmd \
    --exec "$plinkseq_mean_sample_meta_cmd_no_headprojecte(gq,MEAN_GQ,) --out-delim $tab" \
    --exec "$plinkseq_dev_sample_meta_cmd_no_headprojecte(gq,DEV_GQ,) --out-delim $tab"  \
    --exec "$plinkseq_mean_sample_meta_cmd_no_headprojecte(dp,MEAN_DP,) --remap NA,0 --out-delim $tab" \
    --exec "$plinkseq_dev_sample_meta_cmd_no_headprojecte(dp,DEV_DP,) --remap NA,0 --out-delim $tab" \
    --exec "$plinkseq_mean_sample_meta_cmd_no_headprojecte(ab,MEAN_AB,) --out-delim $tab" \
    --exec "$plinkseq_mean_sample_meta_cmd_no_headprojecte(sing_ab,MEAN_SING_AB,) --out-delim $tab" \
    --exec "$plinkseq_mean_sample_meta_cmd_no_cacheprojecte(filtered,AB,M_ALT_QF_AB,!{input\\\,--ref-info\\\,projectt_plinkseq_qcfail_vmatrix_file} --ref-col-start 4 --ref-gt 0 !{input\\\,--ref-info\\\,projectt_plinkseq_dp_qcfail_vmetamatrix_file} --ref-col-start 2 --ref-gt $min_ab_dp) --out-delim $tab" \
    --exec "$plinkseq_sample_meta_cmd_no_headprojecte(ab,mean,$ab_col,--dist-from .5 !{input\\,--ref-info\\,projectt_plinkseq_dp_vmetamatrix_file} --ref-col-start 2 --ref-gt $min_ab_dp) --out-delim $tab" \
    --exec "$plinkseq_mean_sample_meta_cmd_no_cacheprojecte(filtered,AB,M_ALT_QF_ABM50,!{input\\\,--ref-info\\\,projectt_plinkseq_qcfail_vmatrix_file} --ref-col-start 4 --ref-gt 0 !{input\\\,--ref-info\\\,projectt_plinkseq_dp_qcfail_vmetamatrix_file} --ref-col-start 2 --ref-gt $min_ab_dp  --dist-from .5) --out-delim $tab" \
    --exec "$compute_het_cmd(projectt_plinkseq_het_file,)" \
    --exec "$compute_het_cmd(projectt_plinkseq_high_het_file,_HIGH)" \
    --exec "$compute_het_cmd(projectt_plinkseq_low_het_file,_LOW)" \
    --exec "cat !{input,,projectt_plinkseq_sexcheck_file} | awk -v OFS=$tab 'NR == 1 {print \"ID\",\"SEX_CHECK\"} NR > 1 && (\\$3 == 0 || \\$4 == 0) {print \\$2,\"NA\"} NR > 1 && \\$3 != 0 && \\$4 != 0 {if (\\$5 == \"PROBLEM\") {print \\$2,0} if (\\$5 != \"PROBLEM\") {print \\$2,1}}'" \
    @1 \
   --header 1 --in-delim $tab --arg-delim : --out-delim $tab --ignore-err $plinkseq_okay_err > !{output,,projectt_plinkseq_extra_istats_file} 
 
prop no_coverage=scalar default 1

!|expand:,:projectt,projecte,extrarunif,shortt:project,,run_if !num_samp_subsets,:project_sample_subset,_project_sample_subset,,short| \
shortt cmd make_plinkseq_extra_istats_fileprojecte=$make_plinkseq_extra_istats_helperprojecte(--exec "$smart_cut_cmd --in-delim $coverage_dat_delim --out-delim $tab --select-col 1\,1 --select-col 1\,1\,$frac_above_threshold !{input\,--file\,project_sample_coverage_dat_file} | sed '1 s/$frac_above_threshold/PCT_BASES_${threshold}x/'" --extra 14) class_level projectt skip_if no_coverage extrarunif

!!expand:,:projectt,projecte,extrarunif,shortt:project,,skip_if num_samp_subsets,:project_sample_subset,_project_sample_subset,,short! \
shortt cmd make_plinkseq_extra_istats_no_cov_fileprojecte=$make_plinkseq_extra_istats_helperprojecte() class_level projectt run_if no_coverage extrarunif

#to combine all of the istats files
!|expand:;:itype;exskipif\
:plinkseq_het_file;\
:plinkseq_low_het_file;\
:plinkseq_high_het_file;\
:plinkseq_sexcheck_file;\
:plinkseq_raw_all_istats_file;\
:plinkseq_qc_pass_all_istats_file;\
:plinkseq_qc_plus_all_istats_file;skip_if and,!sample_qc_filter,!var_qc_filter,!project_variant_custom_exclude_file,!project_sample_custom_exclude_file\
:plinkseq_raw_syn_istats_file;\
:plinkseq_qc_pass_syn_istats_file;\
:plinkseq_qc_plus_syn_istats_file;skip_if and,!sample_qc_filter,!var_qc_filter,!project_variant_custom_exclude_file,!project_sample_custom_exclude_file\
:plinkseq_raw_ns_istats_file;\
:plinkseq_qc_pass_ns_istats_file;\
:plinkseq_qc_plus_ns_istats_file;skip_if and,!sample_qc_filter,!var_qc_filter,!project_variant_custom_exclude_file,!project_sample_custom_exclude_file\
:plinkseq_raw_nonsense_istats_file;\
:plinkseq_qc_pass_nonsense_istats_file;\
:plinkseq_qc_plus_nonsense_istats_file;skip_if and,!sample_qc_filter,!var_qc_filter,!project_variant_custom_exclude_file,!project_sample_custom_exclude_file\
:plinkseq_raw_noncoding_istats_file;\
:plinkseq_qc_pass_noncoding_istats_file;\
:plinkseq_qc_plus_noncoding_istats_file;skip_if and,!sample_qc_filter,!var_qc_filter,!project_variant_custom_exclude_file,!project_sample_custom_exclude_file\
:plinkseq_qc_pass_sing_istats_file;\
:plinkseq_qc_plus_sing_istats_file;skip_if and,!sample_qc_filter,!var_qc_filter,!project_variant_custom_exclude_file,!project_sample_custom_exclude_file\
:plinkseq_qc_pass_doub_istats_file;\
:plinkseq_qc_pass_indel_istats_file;\
:plinkseq_qc_pass_multiallelic_istats_file;\
:plinkseq_qc_pass_snp_istats_file;\
:plinkseq_filtered_istats_file;\
:plinkseq_extra_istats_file;\
:sample_marker_mds_file;skip_if !parallelize_pca\
|\
local cmd make_cat_project_itype=rm -f !{output,,project_itype} && (head -qn+1 !{input,,project_sample_subset_itype,limit=1,sort_prop=project_sample_subset} | head -n1 && tail -qn+2 !{input,,project_sample_subset_itype,sort_prop=project_sample_subset}) > !{output,,project_itype} class_level project run_if num_samp_subsets exskipif

#These are costly to produce and big...so commenting out now
#!!expand:,:itype,ecol\
#:plinkseq_vmatrix_file,3\
#:plinkseq_gq_vmetamatrix_file,1\
#:plinkseq_dp_vmetamatrix_file,1\
#:plinkseq_ab_vmetamatrix_file,1\
#:plinkseq_sing_ab_vmetamatrix_file,1\
#:plinkseq_dp_qcfail_vmetamatrix_file,1\
#:plinkseq_qcfail_vmatrix_file,3\
#!\
#short cmd make_paste_project_itype=$smart_cut_cmd --in-delim $tab !{input,--file,project_sample_subset_itype} --paste --exclude-col .,1-ecol | $smart_cut_cmd --in-delim $tab !{input,--file,project_sample_subset_itype,limit=1} --select-col 1,1 --paste > !{output,,project_itype} class_level project run_if num_samp_subsets


#istats no longer dumps this by default
add_pct_dbsnp_cmd=$add_function_cmd --col1 DBSNP --col2 $nalt --type divide --header 1 --in-delim @1 --val-header PCT_DBSNP

local cmd make_project_sstats_file=$smart_join_cmd --exec "$smart_cut_cmd --in-delim $tab !{input,--file,project_plinkseq_qc_pass_all_istats_file} --exclude-col 1,1,PASS --exclude-col 1,1,SING --exclude-col 1,1,TITV" --exec "$smart_cut_cmd --in-delim $tab !{input,--file,project_plinkseq_qc_pass_snp_istats_file} --select-col 1,1 --select-col 1,1,TITV" --exec "cat !{input,,project_plinkseq_raw_all_istats_file} | $add_function_cmd --header 1 --in-delim $tab --type subtract  --col1 NMIN --col2 PASS --val-header QCFAIL | $add_function_cmd --header 1 --in-delim $tab --type divide  --col1 PASS --col2 NMIN --val-header FRAC_PASS | $smart_cut_cmd --in-delim $tab --select-col 0,1 --select-col 0,1,QCFAIL --select-col 0,1,'^FRAC_PASS$'" --exec "$smart_cut_cmd --in-delim $tab !{input,--file,project_plinkseq_qc_pass_sing_istats_file} --select-col 1,1 --select-col 1,1,NMIN --select-col 1,1,PASS_S --exact | $add_function_cmd --header 1 --in-delim $tab --type subtract  --col1 NMIN --col2 PASS_S --val-header QCFAIL_SING | $smart_cut_cmd --in-delim $tab --select-col 0,1 --select-col 0,1,NMIN --select-col 0,1,QCFAIL_SING --exclude-row 0,1 --exact | sed '1 s/^/ID\tSING\tQCFAIL_SING\n/'" --exec "$smart_cut_cmd --in-delim $tab !{input,--file,project_plinkseq_qc_pass_doub_istats_file} --select-col 1,1 --select-col 1,1,NMIN --exclude-row 1,1 | sed '1 s/^/ID\tDOUB\n/'" --exec "$smart_cut_cmd --in-delim $tab !{input,--file,project_plinkseq_qc_pass_snp_istats_file} --select-col 1,1 --select-col 1,1,NMIN --exclude-row 1,1 | sed '1 s/^/ID\tSNP\n/'" --exec "$smart_cut_cmd --in-delim $tab !{input,--file,project_plinkseq_qc_pass_indel_istats_file} --select-col 1,1 --select-col 1,1,NMIN --exclude-row 1,1 | sed '1 s/^/ID\tINDEL\n/'" --exec "$smart_cut_cmd --in-delim $tab !{input,--file,project_plinkseq_qc_pass_multiallelic_istats_file} --select-col 1,1 --select-col 1,1,NMIN --exclude-row 1,1 | sed '1 s/^/ID\tMULTI\n/'"  !{input,--file,project_plinkseq_extra_istats_file}  !{input,--file,project_extra_sample_info_file,if_prop=project_extra_sample_info_file,allow_empty=1} !{raw,,project,--fill 9,if_prop=project_extra_sample_info_file,allow_empty=1} !{input,--file,marker_sample_full_concordance_file,if_prop=is_fingerprint,allow_empty=1} !{raw,--exec,project,"$smart_join_cmd --in-delim $tab --exec \"sort -u *project_sample_custom_exclude_file | sed 's/$/\t1/'\" --exec \"cut -f1 *project_plinkseq_qc_pass_all_istats_file | sed 's/$/\t0/'\" --merge --in-delim $tab | sed '1 s/^/ID\t$custom_exclude_header\n/'",if_prop=project_sample_custom_exclude_file,allow_empty=1} !{input,project_sample_custom_exclude_file,if_prop=project_sample_custom_exclude_file,allow_empty=1}  --rest-extra 1 --header 1 --comment \\# --in-delim $tab --out-delim , | $add_function_cmd --header 1 --in-delim , --type subtract  --col1 NMIN --col2 NHET --val-header NHOM | $add_function_cmd --header 1 --in-delim , --type divide --col1 NHET --col2 NHOM --val-header HET_HOM > !{output,,project_sstats_file} class_level project

#use for sample or variant QC (stratification)
prop qc_trait=scalar

#use for stratifying maf
prop maf_strata_trait=scalar

#summary to use for marker frequency filters
prop marker_strat_maf_summary=scalar

#more detailed variant QC
#must specify qc_trait if want to be pulled into vstats file
prop qc_strata_trait=scalar

#compute popgen stats for the qc_strata_trait (more detailed metrics)
prop compute_popgen=scalar

#list of phenotypes to stratify on (for p_missing)
#that is, make this is qc_pheno_strata_trait as well
prop pheno_stratas=list

#for a specific pheno_variant_qc_pheno_strata instance, the phenotype to stratify on
#auto populated by pipeline
prop pheno_strata=scalar


local cmd make_project_traits_file=$smart_join_cmd !{raw,--exec,pheno,"$smart_cut_cmd --file *pheno_all_sample_info_file --in-delim $tab --select-col 1\,1\,'ID @pheno'",if_prop=qc_trait} --in-delim $tab --header 1 !{input,pheno_all_sample_info_file,if_prop=qc_trait} | sed '1 s/^\#//' > !{output,,project_traits_file} class_level project with sample_qc_filter

local cmd make_project_sample_outlier_file=$write_outlier_table_cmd !{input,,project_sstats_file} !{output,,project_sample_outlier_file} -ID ID sep=, out.sep=, class_level project

sstats_pdf_helper=$draw_box_plot_cmd(!{input\,\,project_sstats_file} !{output\,\,@1} '@3' '' 'Sample Values' -1\,NVAR sep=\, @2 max.plot.points=$max_sstats_points max.highlighted.points=$max_sstats_highlighted_points)
sample_highlight_columns_int=highlight.id.col=1 highlight.sep=,
sample_highlight_columns=$sample_highlight_columns_int highlight.label.col=2
sample_highlight_info=id.col=1 highlight.list.file=!{input,,project_sample_exclude_detail_file} $sample_highlight_columns
extreme_quantile=.01

short cmd make_project_sstats_initial_pdf_file=$sstats_pdf_helper(project_sstats_initial_pdf_file, ,Sample QC Properties) class_level project
short cmd make_project_sstats_highlighted_pdf_file=$sstats_pdf_helper(project_sstats_highlighted_pdf_file, $sample_highlight_info ,Sample Outliers) class_level project

!!expand:,:projecttype,phenotype:initial,all:highlighted,highlighted:final,filtered! \
local cmd make_project_pheno_sstats_projecttype_ps_file=$smart_cut_cmd $mpjhf(pheno,pheno_phenotype_sstats_pdf_file,if_prop=qc_trait) > !{output,,project_pheno_sstats_projecttype_ps_file} class_level project with pheno

!!expand:type:initial:highlighted:final! \
local cmd make_project_pheno_sstats_type_pdf_file=epstopdf !{input,,project_pheno_sstats_type_ps_file} !{output,--outfile=,project_pheno_sstats_type_pdf_file} class_level project with pheno
"""
}
    