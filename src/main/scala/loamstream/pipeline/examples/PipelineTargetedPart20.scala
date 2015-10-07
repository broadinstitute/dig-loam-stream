
package loamstream.pipeline.examples

/**
 * LoamStream
 * Created by oliverr on 10/6/2015.
 */
object PipelineTargetedPart20 {
  val string =
 """qt_missing_order=2
qt_bottom_order=1
qt_top_order=0

prop med_quantile=scalar
default_med_quantile=.5

#for associations (extremes)
prop low_quantile=scalar
default_low_quantile=.2
prop high_quantile=scalar
default_high_quantile=.8

qt_low_disp=Below !{prop,,pheno,low_quantile,missing_key=default_low_quantile}q
qt_high_disp=Above !{prop,,pheno,high_quantile,missing_key=default_high_quantile}q

#specify the phenotype to use in association analysis for the meta analysis
prop meta_trait=scalar
prop meta_trait_inv=list
#specify which phenotype to use to stratify samples
prop meta_strata=scalar
#specify the specific value of the meta_strata trait that samples must have
prop meta_strata_value=scalar

local cmd make_pheno_is_trait_file=touch !{output,,pheno_is_trait_file} class_level pheno skip_if not_trait

local cmd make_pheno_ucsc_tracks_file=(echo '[visual_options]' && echo && echo '[custom_tracks]' && echo && echo '[tracks]' && echo 'wgRna=hide' && echo 'wgEncodeReg=show' && echo 'cpgIslandExt=hide' && echo 'ensGene=hide' && echo 'mrna=show' && echo 'intronEst=hide' && echo 'mgcGenes=hide' && echo 'cons44way=show' && echo 'snp130=hide' && echo 'snpArray=hide' && echo 'refGene=hide' && echo 'wgEncodeRegMarkPromoter=full' && echo 'knownGene=full' && echo 'rmsk=hide' && echo 'phyloP46wayPlacental=show') > !{output,,pheno_ucsc_tracks_file} class_level pheno skip_if not_trait

local cmd make_pheno_ucsc_browser_file=(echo '[browser]' && echo 'ucsc_base_url = http://genome.ucsc.edu/cgi-bin/hgTracks?db=hg$hg_build' && echo 'username =' && echo 'query_interval = 0' && echo 'password =' && echo 'user-agent = Mechanize client to get screenshots from the UCSC browser. Home' && echo 'page: https://bitbucket.org/dalloliogm/ucsc-fetch' && echo "email = `whoami`@broadinstitute.org" && echo 'httpproxy =' && echo 'httproxy_port =' && echo 'httproxy_password =') > !{output,,pheno_ucsc_browser_file} class_level pheno skip_if not_trait

#!|expand:;:dott;tag;ext:.1;1;:.{1..!{prop,,pheno,expand_pheno_subsets}};{1..!{prop,,pheno,expand_pheno_subsets}};_mult:;1;_burden| \
#!|expand:;:dott;tag;ext:;1;_burden| \
#pheno_subset_meta_file_helperext=egrep "class @{3}_@{1}_subset$" !{input,,@{3}_@{3}_@{1}_subset_meta_file} | sed 's/^\(!{prop,,@{3}}_@{1}_subset_\(\S\S*\)\).*/\1 !{prop,,@{2}}_@{1}_subset_\2 \2/' | sed 's/^\(\S\S*\)\s\s*\(\S\S*\)\s\s*\(\S\S*\)/\2dott class @{2}_@{1}_subset\n\2dott parent !{prop,,@{2}}\n\2dott disp !{prop,,@{2},disp} @{1} subset \3dott\n\2dott consistent \1\n\2dott expand_subset tag/' > !{output,,@{2}_@{2}_@{1}_subset_meta_file}

#!|expand:;:tag;excomp:;eq:_mult;gt| \
#local cmd make_pheno_pheno_variant_subsettag_meta_file=$pheno_subset_meta_file_helpertag(variant,pheno,project) class_level pheno run_if and,num_var_subsets,expand_pheno_subsets:excomp:1 skip_if not_trait

local cmd make_pheno_pheno_variant_qc_strata_meta_file=cut -f2 !{input,,pheno_non_missing_sample_pheno_file} | sort -u !{raw::pheno:| $smart_cut_cmd --exclude-row 0,1,:if_prop=ignore_pheno_value_for_strata:allow_empty=1}!{prop::pheno:ignore_pheno_value_for_strata:sep= --exclude-row 0,1,:if_prop=ignore_pheno_value_for_strata:allow_empty=1} !{raw::pheno:| $smart_cut_cmd --select-row 0,1,:if_prop=only_pheno_value_for_strata:allow_empty=1}!{prop::pheno:only_pheno_value_for_strata:sep= --select-row 0,1,:if_prop=only_pheno_value_for_strata:allow_empty=1} | sed 's/\(..*\)/!{prop,,pheno}_strata_\1\t\1/' | sed 's/\([^\t][^\t]*\)\t\([^\t][^\t]*\)/\1 class pheno_variant_qc_strata\n!select:!{prop,,project} \1 parent !{prop,,pheno}\n\1 disp QC \2\n\1 meta_strata_value \2/' > !{output,,pheno_pheno_variant_qc_strata_meta_file} class_level pheno with project,pheno_variant_qc_strata run_if and,(or,qc_strata_trait,maf_strata_trait:eq:@pheno),!pheno_qt

local cmd make_empty_pheno_pheno_variant_qc_strata_meta_file=echo > !{output,,pheno_pheno_variant_qc_strata_meta_file} class_level pheno with project run_if or,(and,!qc_strata_trait,!maf_strata_trait:eq:@pheno),pheno_qt

qc_strata_subset_meta_file_helper=egrep "class project_@{1}_subset$" !{input,,project_project_@{1}_subset_meta_file} | sed 's/^\(!{prop,,project}_@{1}_subset_\(\S\S*\)\).*/\1 !{prop,,@{2}}_@{1}_subset_\2 \2/' | sed 's/^\(\S\S*\)\s\s*\(\S\S*\)\s\s*\(\S\S*\)/\2 class @{2}_@{1}_subset\n!select:!{prop,,project}:!{prop,,pheno} \2 parent !{prop,,@{2}}\n\2 disp !{prop,,@{2},disp} @{1} subset \3\n\2 sort \3\n\2 consistent \1@3/' > !{output,,@{2}_@{2}_@{1}_subset_meta_file}

local cmd make_pheno_variant_qc_strata_pheno_variant_qc_strata_variant_subset_meta_file=$qc_strata_subset_meta_file_helper(variant,pheno_variant_qc_strata,\n\2 var_subset_num \3) class_level pheno_variant_qc_strata run_if num_var_subsets

!!expand:pheno_variant_qc_strata:pheno_variant_qc_strata:pheno_variant_qc_pheno_strata! \
local cmd make_pheno_variant_qc_strata_sample_include_file=$smart_join_cmd --exec "$smart_cut_cmd --in-delim $tab !{input,--file,pheno_non_missing_sample_pheno_file} --select-row 1,2,eq:!{prop,,pheno_variant_qc_strata,meta_strata_value} --select-col 1,1 | sort -u | cat - !{input,,project_sample_include_file} | sort | uniq -d" !{input,--file,project_plinkseq_qc_pass_renamed_tfam_file} --col 2,2 --extra 2 | awk '{print \$2,\$1}' > !{output,,pheno_variant_qc_strata_sample_include_file} class_level pheno_variant_qc_strata

!|expand:,:pheno_variant_qc_stratat,projectt,projecte,skipif:pheno_variant_qc_strata,project,,skip_if num_var_subsets:pheno_variant_qc_strata_variant_subset,project_variant_subset,_project_variant_subset,skip_if !num_var_subsets bsub_batch 20| \
!!expand:,:typef,typea,typeo,_typeu,runif:lmiss,missing,lmiss,,:lmiss,missing,lmiss,_unthresholded,:hwe,hardy,hwe,,:frq,freq --nonfounders,frq,,run_if compute_popgen:het,het,het,,run_if compute_popgen! \
short cmd make_pheno_variant_qc_stratat_typeu_typef_file=$plink_qc_pass_typeu_bed_cmdprojecte $plink_analysis_helper_template(pheno_variant_qc_stratat_typeu_typef_file,typea --allow-no-sex !{input;--keep;pheno_variant_qc_strata_sample_include_file},typeo,pheno_variant_qc_stratat_typeu_typef_log_file,pheno_variant_qc_stratat) class_level pheno_variant_qc_stratat skipif runif


!!expand;@;_unthresholded@maskk;_unthresholded@;@-condition_on:$thresholded_nalt_field! \
!|expand:,:pheno_variant_qc_stratat,projectt:pheno_variant_qc_strata,project:pheno_variant_qc_strata_variant_subset,project_variant_subset| \
pheno_variant_qc_stratat_multiallelic_unthresholded_qc_helper1=$multiallelic_qc_cmd !{input,,@{1}_sample_include_file} -spl_field:2 -out:!{output,,@{2}_multiallelic_unthresholded_stats_file} maskk -var_mask:"file=$pseq_multiallelic_tag reg.ex=$chrX; reg=$chrX" -pseq_project:!{input,,projectt_plinkseq_project_file} !{input,-pheno_file:,pheno_non_missing_sample_pheno_file,if_prop=pheno:eq:sex,allow_empty=1,all_instances=1,if_prop=project:eq:\@project,max=1,instance_level=pheno,sep=} !{raw,,pheno,-pheno_field:2 -pheno_id_field:1 -pheno_sel:eq$plink_female,if_prop=pheno:eq:sex,allow_empty=1,all_instances=1,if_prop=project:eq:\@project,max=1} -chrX_code:$chrX 

!!expand:_unthresholded:_unthresholded:! \
!|expand:,:pheno_variant_qc_stratat,projectt:pheno_variant_qc_strata,project:pheno_variant_qc_strata_variant_subset,project_variant_subset| \
pheno_variant_qc_stratat_multiallelic_unthresholded_qc_helper=$pheno_variant_qc_stratat_multiallelic_unthresholded_qc_helper1(pheno_variant_qc_strata,pheno_variant_qc_stratat)

!!expand:_unthresholded:_unthresholded:! \
!|expand:,:pheno_variant_qc_pheno_stratat,pheno_variant_qc_stratat,projectt:pheno_variant_qc_pheno_strata,pheno_variant_qc_strata,project:pheno_variant_qc_pheno_strata_variant_subset,pheno_variant_qc_strata_variant_subset,project_variant_subset| \
pheno_variant_qc_pheno_stratat_multiallelic_unthresholded_qc_helper=$pheno_variant_qc_stratat_multiallelic_unthresholded_qc_helper1(pheno_variant_qc_pheno_strata,pheno_variant_qc_pheno_stratat) -m_group_file:!{input,,pheno_variant_qc_pheno_strata_sample_phe_file} -m_group_field:3 -m_id_field:2 -m_group_missing:$plink_missing

!!expand:_unthresholded:_unthresholded:! \
!|expand:,:pheno_variant_qc_stratat,projectt,projecte,skipif:pheno_variant_qc_strata,project,,skip_if num_var_subsets:pheno_variant_qc_strata_variant_subset,project_variant_subset,_project_variant_subset,skip_if !num_var_subsets bsub_batch 30| \
short cmd make_pheno_variant_qc_stratat_multiallelic_unthresholded_stats_file=$pheno_variant_qc_stratat_multiallelic_unthresholded_qc_helper class_level pheno_variant_qc_stratat skipif

local cmd make_pheno_variant_qc_pheno_strata_pheno_variant_qc_pheno_strata_variant_subset_meta_file=$qc_strata_subset_meta_file_helper(variant,pheno_variant_qc_pheno_strata,\n\2 var_subset_num \3) class_level pheno_variant_qc_pheno_strata run_if num_var_subsets

local cmd make_pheno_variant_qc_pheno_strata_sample_phe_file=$smart_join_cmd --exec "cat !{input,,pheno_variant_qc_pheno_strata_sample_include_file} !{input,,pheno_non_missing_sample_pheno_file,all_instances=1,if_prop=project:eq:@project,if_prop=pheno:eq:@pheno_strata,instance_level=pheno} | awk '{print \\$1}' | sort | uniq -d" !{input,--file,pheno_variant_qc_pheno_strata_sample_include_file} !{input,--file,pheno_non_missing_sample_pheno_file,all_instances=1,if_prop=project:eq:@project,if_prop=pheno:eq:@pheno_strata,instance_level=pheno} --rest-extra 1 --col 2,2 | awk '{print \$2,\$1,\$3}' > !{output,,pheno_variant_qc_pheno_strata_sample_phe_file} class_level pheno_variant_qc_pheno_strata

!!expand:,:pheno_variant_qc_pheno_stratat,projectt,projecte,skipif:pheno_variant_qc_pheno_strata,project,,skip_if num_var_subsets:pheno_variant_qc_pheno_strata_variant_subset,project_variant_subset,_project_variant_subset,run_if num_var_subsets bsub_batch 20! \
!!expand:,:typef,typea,typeo,_typeu:p_missing,test-missing,missing,:p_missing,test-missing,missing,_unthresholded! \
short cmd make_pheno_variant_qc_pheno_stratat_typeu_typef_file=$plink_qc_pass_typeu_bed_cmdprojecte $plink_analysis_helper_template(pheno_variant_qc_pheno_stratat_typeu_typef_file,typea --allow-no-sex !{input;--keep;pheno_variant_qc_pheno_strata_sample_include_file} !{input;--pheno;pheno_variant_qc_pheno_strata_sample_phe_file},typeo,pheno_variant_qc_pheno_stratat_typeu_typef_log_file,pheno_variant_qc_pheno_stratat) class_level pheno_variant_qc_pheno_stratat skipif

!!expand:,:_unthresholded:_unthresholded:! \
!!expand:,:pheno_variant_qc_pheno_stratat,projectt,projecte,skipif:pheno_variant_qc_pheno_strata,project,,skip_if num_var_subsets:pheno_variant_qc_pheno_strata_variant_subset,project_variant_subset,_project_variant_subset,run_if num_var_subsets bsub_batch 30! \
short cmd make_pheno_variant_qc_pheno_stratat_multiallelic_unthresholded_stats_file=$pheno_variant_qc_pheno_stratat_multiallelic_unthresholded_qc_helper class_level pheno_variant_qc_pheno_stratat skipif

!!expand:,:typef:lmiss:unthresholded_lmiss:hwe:multiallelic_stats:multiallelic_unthresholded_stats:vstats! \
short cmd cat_pheno_variant_qc_strata_typef_file=(head -qn+1 !{input,,pheno_variant_qc_strata_variant_subset_typef_file,limit=1,sort_prop=pheno_variant_qc_strata_variant_subset} | head -n1 && tail -qn+2 !{input,,pheno_variant_qc_strata_variant_subset_typef_file,sort_prop=pheno_variant_qc_strata_variant_subset}) > !{output,,pheno_variant_qc_strata_typef_file} class_level pheno_variant_qc_strata run_if num_var_subsets

!!expand:,:typef:p_missing:unthresholded_p_missing:multiallelic_stats:multiallelic_unthresholded_stats:vstats! \
short cmd cat_pheno_variant_qc_pheno_strata_typef_file=(head -qn+1 !{input,,pheno_variant_qc_pheno_strata_variant_subset_typef_file,limit=1,sort_prop=pheno_variant_qc_pheno_strata_variant_subset} | head -n1 && tail -qn+2 !{input,,pheno_variant_qc_pheno_strata_variant_subset_typef_file,sort_prop=pheno_variant_qc_pheno_strata_variant_subset}) > !{output,,pheno_variant_qc_pheno_strata_typef_file} class_level pheno_variant_qc_pheno_strata run_if num_var_subsets

!!expand:,:pheno_variant_qc_stratat:pheno_variant_qc_strata:pheno_variant_qc_strata_variant_subset! \
pheno_variant_qc_stratat_multiallelic_vstats_helper=$smart_cut_cmd --out-delim $tab !{input,--file,pheno_variant_qc_stratat_multiallelic@{1}_stats_file} --select-col 1,1,'SNP pval call_rate' --exact | $add_function_cmd --col1 pval --type minus_log --header 1 --val-header LOG_P_HWE@2 --in-delim $tab | $smart_cut_cmd --in-delim $tab --exclude-col 0,1,pval --exact | $add_function_cmd --val1 1 --col1 call_rate --type subtract --header 1 --val-header F_MISS@2 --in-delim $tab | $smart_cut_cmd --in-delim $tab --exclude-col 0,1,call_rate --exact

!!expand:,:pheno_variant_qc_stratat,projectt,projecte,skipif:pheno_variant_qc_strata,project,,skip_if num_var_subsets:pheno_variant_qc_strata_variant_subset,project_variant_subset,_project_variant_subset,run_if num_var_subsets bsub_batch 20! \
short cmd make_pheno_variant_qc_stratat_vstats_file=$smart_join_cmd --in-delim $tab --header 1 --merge --exec "$smart_join_cmd --in-delim $tab --header 1 --exec \"$smart_cut_cmd --out-delim $tab !{input,--file,pheno_variant_qc_stratat_multiallelic_stats_file} --select-col 1,1,'id pval call_rate' --exact --require-col-match | $add_function_cmd --col1 pval --type minus_log --header 1 --val-header LOG_P_HWE --in-delim $tab | $smart_cut_cmd --in-delim $tab --exclude-col 0,1,pval --exact | $add_function_cmd --val1 1 --col2 call_rate --type subtract --header 1 --val-header F_MISS --in-delim $tab | $smart_cut_cmd --in-delim $tab --exclude-col 0,1,call_rate --exact\" --exec \"$smart_cut_cmd --out-delim $tab !{input,--file,pheno_variant_qc_stratat_multiallelic_unthresholded_stats_file} --select-col 1,1,'id call_rate' --exact --require-col-match | $add_function_cmd --val1 1 --col2 call_rate --type subtract --header 1 --val-header F_MISS_UNTHRESH --in-delim $tab | $smart_cut_cmd --in-delim $tab --exclude-col 0,1,call_rate --exact\" | $smart_cut_cmd --in-delim $tab --select-col 0,1 --select-col 0,1,'LOG_P_HWE F_MISS F_MISS_UNTHRESH' --exact --require-col-match | sed '1 s/^\S\S*/SNP/' | sed '1 s/\t\(\S\S*\)/\t\1_!{prop,,pheno_variant_qc_strata,meta_strata_value}/g'" --exec "$smart_join_cmd --in-delim $tab --header 1 --exec \"$smart_cut_cmd --out-delim $tab !{input,--file,pheno_variant_qc_stratat_lmiss_file} --select-col 1,1,'SNP F_MISS' --exact\" --exec \"$smart_cut_cmd --out-delim $tab !{input,--file,pheno_variant_qc_stratat_hwe_file} --select-col 1,1,'SNP P' --select-row 1,1 --select-row 1,1,TEST,ALL --exact | $add_function_cmd --col1 P --type minus_log --header 1 --val-header LOG_P_HWE --in-delim $tab | $smart_cut_cmd --in-delim $tab --exclude-col 0,1,P --exact\" --exec \"$smart_cut_cmd --out-delim $tab !{input,--file,pheno_variant_qc_stratat_unthresholded_lmiss_file} --select-col 1,1,'SNP F_MISS' --exact | sed 's/F_MISS/F_MISS_UNTHRESH/'\" | $smart_cut_cmd --in-delim $tab --select-col 0,1 --select-col 0,1,'LOG_P_HWE F_MISS F_MISS_UNTHRESH' --exact --require-col-match | sed '1 s/\t\(\S\S*\)/\t\1_!{prop,,pheno_variant_qc_strata,meta_strata_value}/g'" --exec "cut -f1 !{input,,projectt_vfreq_file} | $parse_out_id | sed 's/$/\tNA\tNA\tNA/'" | $replace_nan(NA) > !{output,,pheno_variant_qc_stratat_vstats_file} class_level pheno_variant_qc_stratat skipif


!|expand%;%phenol;projectl;pheno_variant_qc_stratat;skipif;extraifprop%pheno;project;pheno_variant_qc_strata;skip_if num_var_subsets;%pheno_variant_subset;project_variant_subset;pheno_variant_qc_strata_variant_subset;skip_if !num_var_subsets bsub_batch 5 consistent_prop var_subset_num;,if_prop=var_subset_num:eq:@var_subset_num| \
short cmd make_phenol_variant_qc_strata_vstats_summary_file=(head -n1 !{input,,pheno_variant_qc_stratat_vstats_fileextraifprop,limit=1} && tail -qn+2 !{input,,pheno_variant_qc_stratat_vstats_file,unless_prop=meta_strata_value:eq:@ignore_pheno_value_for_strata_summaryextraifprop}) | $smart_cut_cmd --in-delim $tab --select-col 0,1,'SNP F_MISS_UNTHRESH F_MISS LOG_P_HWE' --require-col-match | sed '1 s/.*/SNP\tF_MISS_UNTHRESH\tF_MISS\tLOG_P_HWE/' | $table_sum_stats_cmd --in-delim $tab --out-delim $tab --has-header --print-header --col F_MISS --col F_MISS_UNTHRESH --col LOG_P_HWE --group-col SNP --summaries --na-output NA | $smart_cut_cmd --tab-delim --select-col 0,1,'SNP F_MISS_max F_MISS_UNTHRESH_max LOG_P_HWE_max' | sed '1 s/\t\(\S\S*\)/\t\1_!{prop,,pheno}/g' > !{output,,phenol_variant_qc_strata_vstats_summary_file} class_level phenol skipif run_if qc_strata_trait with projectl

!!expand:,:pheno_variant_qc_pheno_stratat,projectt,projecte,skipif:pheno_variant_qc_pheno_strata,project,,skip_if num_var_subsets:pheno_variant_qc_pheno_strata_variant_subset,project_variant_subset,_project_variant_subset,run_if num_var_subsets bsub_batch 20! \
short cmd make_pheno_variant_qc_pheno_stratat_vstats_file=$smart_join_cmd --in-delim $tab --header 1 --merge --exec "$smart_join_cmd --in-delim $tab --header 1 --exec \"$smart_cut_cmd --out-delim $tab !{input,--file,pheno_variant_qc_pheno_stratat_multiallelic_stats_file} --select-col 1,1,'id pval' --exact\" --exec \"$smart_cut_cmd --out-delim $tab !{input,--file,pheno_variant_qc_pheno_stratat_multiallelic_unthresholded_stats_file} --select-col 1,1,'id pval' --exact\" | sed '1 s/.*/SNP\tP_MISSING\tP_MISSING_UNTHRESH/' | sed '1 s/\t\(\S\S*\)/\t\1_!{prop,,pheno_variant_qc_pheno_strata,meta_strata_value}_!{prop,,pheno_variant_qc_pheno_strata,pheno_strata}/g'" --exec "$smart_join_cmd --in-delim $tab --header 1 --exec \"$smart_cut_cmd --out-delim $tab !{input,--file,pheno_variant_qc_pheno_stratat_p_missing_file} --select-col 1,1,'SNP P' --exact\" --exec \"$smart_cut_cmd --out-delim $tab !{input,--file,pheno_variant_qc_pheno_stratat_unthresholded_p_missing_file} --select-col 1,1,'SNP P' --exact\"" --exec "cut -f1 !{input,,projectt_vfreq_file} | $parse_out_id | sed 's/$/\tNA\tNA/'" | $replace_nan(NA) > !{output,,pheno_variant_qc_pheno_stratat_vstats_file} class_level pheno_variant_qc_pheno_stratat skipif

!|expand%;%phenol;pheno_variant_qc_pheno_stratat;runif;extraifprop%pheno;pheno_variant_qc_pheno_strata;run_if !num_var_subsets;%pheno_variant_subset;pheno_variant_qc_pheno_strata_variant_subset;run_if num_var_subsets bsub_batch 5 consistent_prop var_subset_num;,if_prop=var_subset_num:eq:@var_subset_num| \
short cmd make_phenol_variant_qc_pheno_strata_vstats_summary_file=(head -n1 !{input,,pheno_variant_qc_pheno_stratat_vstats_fileextraifprop,limit=1} && tail -qn+2 !{input,,pheno_variant_qc_pheno_stratat_vstats_fileextraifprop}) | $smart_cut_cmd --in-delim $tab --select-col 0,1,'SNP P_MISSING_UNTHRESH P_MISSING' --require-col-match | sed '1 s/.*/SNP\tP_MISSING_UNTHRESH\tP_MISSING/' | $table_sum_stats_cmd --in-delim $tab --out-delim $tab --has-header --print-header --col P_MISSING --col P_MISSING_UNTHRESH --group-col SNP --summaries --na-output NA | $smart_cut_cmd --tab-delim --select-col 0,1,'SNP P_MISSING_min P_MISSING_UNTHRESH_min' | sed '1 s/\t\(\S\S*\)/\t\1_!{prop,,pheno}/g' > !{output,,phenol_variant_qc_pheno_strata_vstats_summary_file} class_level phenol runif skip_if or,!qc_strata_trait,!pheno_stratas

prop ignore_pheno_value_for_strata=list
prop only_pheno_value_for_strata=list
prop ignore_pheno_value_for_strata_summary=list

local cmd make_pheno_pheno_variant_qc_pheno_strata_meta_file=for t in `$smart_cut_cmd --exec "echo !{prop,,pheno,pheno_stratas} | sed 's/\s\s*/\n/g' | sort -u" --exec "echo !{prop,,pheno,all_instances=1,if_prop=project:eq:@project,unless_prop=pheno_qt} | sed 's/\s\s*/\n/g'" | sort | uniq -d`; do cut -f2 !{input,,pheno_non_missing_sample_pheno_file} | sort -u !{raw::pheno:| $smart_cut_cmd --exclude-row 0,1,:if_prop=ignore_pheno_value_for_strata:allow_empty=1}!{prop::pheno:ignore_pheno_value_for_strata:sep= --exclude-row 0,1,:if_prop=ignore_pheno_value_for_strata:allow_empty=1} !{raw::pheno:| $smart_cut_cmd --select-row 0,1,:if_prop=only_pheno_value_for_strata:allow_empty=1}!{prop::pheno:only_pheno_value_for_strata:sep= --select-row 0,1,:if_prop=only_pheno_value_for_strata:allow_empty=1} | sed 's/\(..*\)/!{prop,,pheno}_'\$t'_strata_\1\t\1/' | sed 's/\([^\t][^\t]*\)\t\([^\t][^\t]*\)/\1 class pheno_variant_qc_pheno_strata\n!select:!{prop,,project} \1 parent !{prop,,pheno}\n\1 disp QC \2\n\1 meta_strata_value \2\n\1 pheno_strata '\$t'/'; done > !{output,,pheno_pheno_variant_qc_pheno_strata_meta_file} class_level pheno with project,pheno_variant_qc_pheno_strata run_if and,qc_strata_trait,!pheno_qt,pheno_stratas


local cmd make_empty_pheno_pheno_variant_qc_pheno_strata_meta_file=echo > !{output,,pheno_pheno_variant_qc_pheno_strata_meta_file} class_level pheno with project,pheno_variant_qc_pheno_strata run_if or,!qc_strata_trait,pheno_qt,!pheno_stratas

#local cmd make_pheno_pheno_variant_subset_meta_file=cat !{input,,project_project_variant_subset_pheno_expand_subset_file} | awk '{n = \$2} \$2 > 1 {n = "{1.."\$2"}"} {print \$1,n,\$2}' | sed 's/^\(\S\S\*_subset_\(\S\S*\)\)\s\s*\(\S\S*\)\s\s*\(\S\S*\)/\1 !{prop,,pheno}_variant_subset_\2.\3 \2 \3 \4/' | sed 's/\(\S\S*\)\s\s*\(\S\S*\)\s\s*\(\S\S*\)\s\s*\(\S\S*\)\s\s*\(\S\S*\)/\1 expand_pheno_subsets \5\n\2 class pheno_variant_subset\n!select:!{prop,,project} \2 parent !{prop,,pheno}\n\2 disp !{prop,,pheno,disp} variant subset \3.\4\n\2 consistent \1\n\2 var_subset_num \3\n\2 expand_subset_num \4/' > !{output,,pheno_pheno_variant_subset_meta_file} class_level pheno skip_if not_trait with pheno_variant_subset run_if num_var_subsets

local cmd make_pheno_pheno_variant_subset_meta_file=cat !{input,,project_project_variant_subset_pheno_expand_subset_file} | awk '{n = \$2} \$2 > 1 {n = "{1.."\$2"}"} {print \$1,n,\$2}' | sed 's/^\(\S\S\*_subset_\(\S\S*\)\)\s\s*\(\S\S*\)\s\s*\(\S\S*\)/\1 !{prop,,pheno}_variant_subset_\2.\3 \2 \3 \4/' | perl -lane 'BEGIN {@ex = qw(!{prop,,pheno,empty_clean_subsets,missing=}); map {\$ex{\$_}=1} @ex} print unless \$ex{\$F[2]}' | sed 's/\(\S\S*\)\s\s*\(\S\S*\)\s\s*\(\S\S*\)\s\s*\(\S\S*\)\s\s*\(\S\S*\)/\1 expand_pheno_subsets \5\n\2 class pheno_variant_subset\n!select:!{prop,,project} \2 parent !{prop,,pheno}\n\2 disp !{prop,,pheno,disp} variant subset \3.\4\n\2 consistent \1\n\2 var_subset_num \3\n\2 expand_subset_num \4/' > !{output,,pheno_pheno_variant_subset_meta_file} class_level pheno skip_if and,not_trait,!qc_strata_trait with pheno_variant_subset run_if and,num_var_subsets,project_variant_subset

local cmd make_empty_pheno_pheno_variant_subset_meta_file=echo > !{output,,pheno_pheno_variant_subset_meta_file} class_level pheno with pheno_variant_subset run_if or,(and,not_trait,!qc_strata_trait),!num_var_subsets,!project_variant_subset

local cmd make_pheno_pheno_sample_subset_meta_file=egrep "class project_sample_subset$" !{input,,project_project_sample_subset_meta_file} | sed 's/^\(!{prop,,project}_sample_subset_\(\S\S*\)\).*/\1 !{prop,,pheno}_sample_subset_\2 \2/' | sed 's/^\(\S\S*\)\s\s*\(\S\S*\)\s\s*\(\S\S*\)/\2 class pheno_sample_subset\n!select:!{prop,,project} \2 parent !{prop,,pheno}\n!select:!{prop,,project} \2 disp !{prop,,pheno,disp} sample subset \3\n\2 consistent \1/' > !{output,,pheno_pheno_sample_subset_meta_file} class_level pheno run_if or,num_samp_subsets,parallelize_genome,parallelize_pca skip_if not_trait

local cmd make_empty_pheno_pheno_sample_subset_meta_file=echo > !{output,,pheno_pheno_sample_subset_meta_file} class_level pheno skip_if not_trait with pheno_sample_subset run_if and,!num_samp_subsets,!parallelize_genome,!parallelize_pca

pheno_test_variant_subset_name=!{prop,,pheno}_test_variant_subset

local cmd make_pheno_pheno_test_variant_subset_meta_file=if [[ "!{prop;;pheno_test;sep=,;if_prop=num_var_subsets;unless_prop=test_software:eq:metal;unless_prop=test_software:eq:metasoft;allow_empty=1,sort=pheno_test}" ]]; then egrep "class pheno_variant_subset$" !{input,,pheno_pheno_variant_subset_meta_file} | sed 's/^\(!{prop,,pheno}_variant_subset_\([^\.][^\.]*\)\.\(\S\S*\)\).*/\1 ${pheno_test_variant_subset_name}_\2.\3 \2 \3/' | sed 's/^\(\S\S*\)\s\s*\(\S\S*\)\s\s*\(\S\S*\)\s\s*\(\S\S*\)/\2 class pheno_test_variant_subset\n!expand_all !select:!{prop;;project}:!{prop;;pheno} \2 parent {!{prop;;pheno_test;sep=,;if_prop=num_var_subsets;unless_prop=test_software:eq:metal;unless_prop=test_software:eq:metasoft;allow_empty=1}}\n\2 disp !{prop,,pheno,disp} test variant subset \3.\4\n\2 consistent \1\n\1 consistent \2\n\2 var_subset_num \3\n\2 expand_subset_num \4/' | sed 's/{\([^,\.}][^,\.}]*\)}/\1/g' > !{output,,pheno_pheno_test_variant_subset_meta_file} && awk '\$2 == "consistent"' !{input,,pheno_pheno_variant_subset_meta_file} | sed 's/^\(!{prop,,pheno}_variant_subset_\(\S\S*\)\)/${pheno_test_variant_subset_name}_\2/' >> !{output,,pheno_pheno_test_variant_subset_meta_file}; else echo > !{output,,pheno_pheno_test_variant_subset_meta_file}; fi class_level pheno run_if and,num_var_subsets,pheno_test skip_if not_trait with pheno_test_variant_subset

local cmd make_empty_pheno_pheno_test_variant_subset_meta_file=rm -f !{output,,pheno_pheno_test_variant_subset_meta_file} && touch !{output,,pheno_pheno_test_variant_subset_meta_file} class_level pheno run_if and,num_var_subsets,!pheno_test skip_if not_trait with pheno_test_variant_subset

burden_variant_subset_name=!{prop,,pheno}_!{prop,,burden}_variant_subset
burden_test_variant_subset_name=!{prop,,pheno}_!{prop,,burden}_test_variant_subset

filter_pheno_subsets_to_use=sed 's/^\(!{prop,,pheno}_variant_subset_\([^\.][^\.]*\)\.\(\S\S*\)\)\(.*\)/\1 \4 \2 \3/' !{raw::burden:| perl -pe 'END {\$ex \= "@exclude_subset"; \$ex \=~ s/([0-9]+)([^0-9]+)([0-9]+)(\s|$)/\1 \3\n/; print \$ex;}' | awk 'NF \=\= 2 {print "X","X","X",\$0} NF > 2 {print \$0}' | sort -s -k4,4 -k5,5 | uniq -f3 -u:if_prop=exclude_subset:allow_empty=1} | awk '{print \$1,\$2,\$3}'

local cmd make_burden_burden_variant_subset_meta_file=egrep "class pheno_variant_subset$" !{input,,pheno_pheno_variant_subset_meta_file} | $filter_pheno_subsets_to_use | sed 's/^\(!{prop,,pheno}_variant_subset_\([^\.][^\.]*\)\.\(\S\S*\)\).*/\1 ${burden_variant_subset_name}_\2.\3 \2 \3/' | sed 's/^\(\S\S*\)\s\s*\(\S\S*\)\s\s*\(\S\S*\)\s\s*\(\S\S*\)/\2 class burden_variant_subset\n!select:!{prop,,project}:!{prop,,pheno} \2 parent !{prop,,burden}\n\2 disp !{prop,,burden,disp} variant subset \3.\4\n\2 consistent \1\n\2 var_subset_num \3\n\2 expand_subset_num \4/' > !{output,,burden_burden_variant_subset_meta_file} && awk '\$2 == "consistent"' !{input,,pheno_pheno_variant_subset_meta_file} | $filter_pheno_subsets_to_use | sed 's/^\(!{prop,,pheno}_variant_subset_\([^\.][^\.]*\).\(\S\S*\)\)\s\s\*consistent\s\s*\(.*\)/${burden_variant_subset_name}_\2.\3 consistent \4\n${burden_variant_subset_name}_\2.\3 consistent ${annot_variant_subset_name}_\2/' !{input,annot_annot_variant_subset_meta_file,max=1} >> !{output,,burden_burden_variant_subset_meta_file} class_level burden run_if and,!only_for_interesting,num_var_subsets,(or,burden_maf,burden_mac_lb,burden_mac_ub,burden_test),!annot_genes,!annot_manual_gene_list_file,!annot_manual_gene_variant_list_file skip_if not_trait with burden_variant_subset

local cmd make_empty_burden_burden_variant_subset_meta_file=rm -f !{output,,burden_burden_variant_subset_meta_file} && touch !{output,,burden_burden_variant_subset_meta_file} class_level burden run_if or,only_for_interesting,!num_var_subsets,(and,!burden_maf,!burden_mac_lb,!burden_mac_ub,!burden_test),annot_genes,annot_manual_gene_list_file,annot_manual_gene_variant_list_file skip_if not_trait with burden_variant_subset

local cmd make_burden_burden_test_variant_subset_meta_file=egrep "class pheno_variant_subset$" !{input,,pheno_pheno_variant_subset_meta_file} | $filter_pheno_subsets_to_use | sed 's/^\(!{prop,,pheno}_variant_subset_\([^\.][^\.]*\)\.\(\S\S*\)\).*/\1 ${burden_variant_subset_name}_\2.\3 ${burden_test_variant_subset_name}_\2.\3 \2 \3/' | sed 's/^\(\S\S*\)\s\s*\(\S\S*\)\s\s*\(\S\S*\)\s\s*\(\S\S*\)\s\s*\(\S\S*\)/\3 class burden_test_variant_subset\n!select:!{prop,,project}:!{prop,,pheno}:!{prop,,burden} \3 parent !{prop,,burden_test,if_prop=test_software:ne:metal,sep=\,}\n\3 disp !{prop,,burden,disp} test variant subset \4.\5\n\3 consistent \1\n\3 consistent \2\n\3 var_subset_num \4\n\3 expand_subset_num \5/' | sed 's/\(parent\s\s*\)\(.*,.*\)/\1{\2}/' > !{output,,burden_burden_test_variant_subset_meta_file} && awk '\$2 == "consistent"' !{input,,pheno_pheno_variant_subset_meta_file} | $filter_pheno_subsets_to_use | sed 's/^\(!{prop,,pheno}_variant_subset_\([^\.][^\.]*\).\(\S\S*\)\)\s\s\*consistent\s\s*\(.*\)/${burden_test_variant_subset_name}_\2.\3 consistent \4\n${burden_test_variant_subset_name}_\2.\3 consistent ${annot_variant_subset_name}_\2/' !{input,annot_annot_variant_subset_meta_file,max=1} >> !{output,,burden_burden_test_variant_subset_meta_file} class_level burden run_if and,num_var_subsets,!only_for_interesting,(or,burden_maf,burden_mac_lb,burden_mac_ub,burden_test),burden_test,!annot_genes,!annot_manual_gene_list_file,!annot_manual_gene_variant_list_file skip_if not_trait with burden_test_variant_subset

local cmd make_empty_burden_burden_test_variant_subset_meta_file=rm -f !{output,,burden_burden_test_variant_subset_meta_file} && touch !{output,,burden_burden_test_variant_subset_meta_file} class_level burden run_if or,only_for_interesting,!num_var_subsets,(and,!burden_maf,!burden_mac_lb,!burden_mac_ub,!burden_test),!burden_test,annot_genes,annot_manual_gene_list_file,annot_manual_gene_variant_list_file skip_if not_trait with burden_test_variant_subset

local cmd make_pheno_variant_subset_region_file=sort -k1,1 -k2,2 -k3,3n !{input,,project_variant_subset_region_file} | awk 'NR == 1 {n=1} NR > 1 && r != \$1 {n++} n % !{prop,,project_variant_subset,expand_pheno_subsets} + 1 == !{prop,,pheno_variant_subset,expand_subset_num} {print} {r=\$1}' > !{output,,pheno_variant_subset_region_file} class_level pheno_variant_subset

#first, annotate var list with all regions at project_variant_subset
#then, annotate further with regions at pheno var subset
#keep a variant if outside project_variant_subset_region and if NR mod expand_subsets equals pheno_variant_subset index
#keep all variants inside pheno var subset regions

short cmd make_pheno_variant_subset_var_chr_pos_keep_file=$pseq_qc_plus_analysis_cmd_project_variant_subset(v-view) $show_id | cut -f1 | awk -F: -v OFS="\t" '{print \$1":"\$2,\$3}' | $add_gene_annot_cmd --in-delim $tab --outside-name $outside_gene_name --keep-outside --locus-col 1 --gene-file !{input,,project_variant_subset_region_file} --out-delim $tab | awk -v OFS="\t" -F"\t" '{print \$2,\$3,\$1,NR}' | $add_gene_annot_cmd --in-delim $tab --locus-col 1 --gene-file !{input,,pheno_variant_subset_region_file} --out-delim $tab --keep-outside | awk -v OFS="\t" -F"\t" '{if (\$4 == "$outside_gene_name") {if (\$5 % !{prop,,project_variant_subset,expand_pheno_subsets} + 1 == !{prop,,pheno_variant_subset,expand_subset_num}) {print}} else if (\$1 != "$outside_gene_name") {print}}' | cut -f2-3 > !{output,,pheno_variant_subset_var_chr_pos_keep_file} class_level pheno_variant_subset run_if expand_pheno_subsets:ne:1

local cmd make_copy_pheno_variant_subset_var_chr_pos_keep_file=$smart_cut_cmd --in-delim $tab !{input,--file,project_variant_subset_clean_gene_variant_file} --select-col 1,1,'CHROM POS ID' --exact --require-col-match | tail -n+2 | awk -v OFS="\t" '{print "chr"\$1":"\$2,\$3}' | sed 's/^chrchr/chr/' > !{output,,pheno_variant_subset_var_chr_pos_keep_file} class_level pheno_variant_subset run_if expand_pheno_subsets:eq:1 skip_if not_trait

!!expand:,:keeptype,cols:var,2:chr_pos,1! \
local cmd make_pheno_variant_subset_keeptype_keep_file=cat !{input,,pheno_variant_subset_var_chr_pos_keep_file} | cut -fcols > !{output,,pheno_variant_subset_keeptype_keep_file} class_level pheno_variant_subset

local cmd make_pheno_disp_order_file=$smart_cut_cmd --in-delim $tab !{input,--file,pheno_sequenced_all_sample_info_file} --select-col 1,1,$display_col_name --select-col 1,1,$order_col_name --exclude-row 1,1 | sort -t$tab -uk1 > !{output,,pheno_disp_order_file} class_level pheno

meta_table cmd make_pheno_sample_info_header_file=\#ID\t!{prop,,pheno}\t$display_col_name\t$order_col_name !{output,pheno_sample_info_header_file}  class_level pheno skip_if pheno_qt

meta_table cmd make_pheno_qt_sample_info_header_file=\#ID\t!{prop,,pheno}\t$display_col_name\t$order_col_name\t$pheno_half\t$pheno_extreme !{output,pheno_sample_info_header_file}  class_level pheno run_if pheno_qt

prop value=scalar

missing_sample_fields_helper=\t!{prop,,pheno,pheno_missing}\t!{prop,,pheno_value,disp,missing_prop=pheno_value,if_prop=pheno_value:eq:@pheno_missing}\t!{prop,,pheno_value,sort,if_prop=pheno_value:eq:@pheno_missing,missing=1}

missing_qt_sample_fields_helper=\t!{prop,,pheno,pheno_missing}\t$qt_missing_disp\t$qt_missing_order\t!{prop,,pheno,pheno_missing,missing=-9}\t!{prop,,pheno,pheno_missing,missing=-9}

!|expand:;:qttype;sedhelper;exrunif:;$missing_sample_fields_helper;skip_if pheno_qt:_qt;$missing_qt_sample_fields_helper;skip_if !pheno_qt| \
!!expand:,:ftype,gtype:,passed:failed_,failed! \
local cmd make_phenoqttype_ftypemissing_sample_info_file=cat !{input,,pheno_ftypenon_missing_sample_pheno_file} !{input,,pheno_ftypenon_missing_sample_pheno_file} | cut -f1 | cat - !{input,,project_gtype_sample_list_file} | sort | uniq -u | sed 's/$/sedhelper/' > !{output,,pheno_ftypemissing_sample_info_file} class_level pheno run_if and,!meta_strata,!meta_strata_value,!meta_trait exrunif


!|expand:;:qttype;sedhelper:;$missing_sample_fields_helper:_qt;$missing_qt_sample_fields_helper| \
meta_phenoqttype_missing_sample_helper=cat $meta_trait_file_helper(,@1) $meta_trait_file_helper(,@2) | cut -f1 | sort -u | cat - !{input,,pheno_sample_exclude_file} | sort | uniq -d | cat - $meta_trait_file_helper(,@1) | cut -f1 | sort -u | sed 's/$/sedhelper/' > !{output,,@1}

!|expand:;:qttype;exskipif:;skip_if pheno_qt:_qt;skip_if !pheno_qt| \
local cmd make_phenoqttype_meta_missing_sample_info_file=$meta_phenoqttype_missing_sample_helper(pheno_missing_sample_info_file,pheno_non_missing_sample_info_file) class_level pheno run_if or,meta_strata,meta_strata_value,meta_trait exskipif

!|expand:;:qttype;exskipif:;skip_if pheno_qt:_qt;skip_if !pheno_qt| \
local cmd make_phenoqttype_meta_failed_missing_sample_info_file=$meta_pheno_missing_sample_helper(pheno_failed_missing_sample_info_file,pheno_failed_non_missing_sample_info_file) class_level pheno run_if or,meta_strata,meta_strata_value,meta_trait exskipif


non_missing_sample_pheno_helper=!{prop,,sample,@{1}_prop=failed,if_prop=pheno:defined:deref:1}\t!{prop,,sample,pheno,deref=1,@{1}_prop=failed,if_prop=pheno:defined:deref:1}

!!expand:,:ktype,stype:keep,select-row:exclude,exclude-row! \
local cmd make_pheno_sample_ktype_file=$smart_cut_cmd --in-delim $tab !{input,--file,pheno_sequenced_all_sample_info_file,if_prop=pheno:eq:@meta_strata,if_prop=project:eq:@project,all_instances=1} --select-col 1,1,'\\#ID' --exclude-row 1,1 --stype 1,1,!{prop,,pheno,meta_strata},eq:!{prop,,pheno,meta_strata_value} --and-row-all --exact --require-col-match > !{output,,pheno_sample_ktype_file} class_level pheno run_if or,meta_strata,meta_strata_value,meta_trait

!!expand:,:ktype,stype:keep,select-row:exclude,exclude-row! \
local cmd make_pheno_non_meta_sample_keep_file=cut -f1 !{input,,pheno_non_missing_sample_pheno_file} > !{output,,pheno_sample_keep_file} class_level pheno skip_if or,meta_strata,meta_strata_value,meta_trait

!!expand:,:ktype,ftype:,passed:failed_,failed! \
local cmd cp_pheno_ktypenon_missing_sample_pheno_file=$smart_join_cmd --out-delim $tab --exec "$smart_cut_cmd !{input,--file,pheno_sample_pheno_file} --exclude-row 1,2,eq:!{prop,,pheno,pheno_missing} --select-col 1,1 | sort -u | $smart_cut_cmd --exec 'sort -u !{input,,project_ftype_sample_list_file}' | sort | uniq -d" !{input,--file,pheno_sample_pheno_file}  --rest-extra 1 > !{output,,pheno_ktypenon_missing_sample_pheno_file} class_level pheno run_if pheno_sample_pheno_file with project

meta_table cmd make_pheno_non_missing_sample_pheno_file=$non_missing_sample_pheno_helper(unless) !{output,pheno_non_missing_sample_pheno_file} class_level pheno skip_if or,meta_strata,meta_strata_value,meta_trait,pheno_sample_pheno_file,no_sample_tracking

#meta_table cmd make_pheno_non_missing_sample_pheno_file=$non_missing_sample_pheno_helper(unless) !{output,pheno_non_missing_sample_pheno_file} class_level pheno skip_if or,meta_strata,pheno_sample_pheno_file,no_sample_tracking

meta_trait_file_helper=!{input,@1,@2,if_prop=pheno:eq:\@meta_trait,if_prop=project:eq:\@project,all_instances=1}

meta_pheno_sample_helper_int=$smart_join_cmd --exec "cut -f1 $meta_trait_file_helper(,@1) | cat - !{input,,pheno_sample_keep_file} | sort | uniq -d @3" --exec "cat !{input,,pheno_sample_keep_file} @3" $meta_trait_file_helper(--file,@1) --rest-extra 1 --in-delim $tab @2 > !{output,,@1}

meta_pheno_sample_helper=$meta_pheno_sample_helper_int(@1,,)
meta_pheno_sample_helper_header=$meta_pheno_sample_helper_int(@1,--header 1,| sed '1 s/^/ID\n/')

local cmd make_pheno_meta_non_missing_sample_pheno_file=$meta_pheno_sample_helper(pheno_non_missing_sample_pheno_file) class_level pheno run_if or,meta_strata,meta_strata_value,meta_trait with project
local cmd make_pheno_meta_failed_non_missing_sample_pheno_file=$meta_pheno_sample_helper(pheno_failed_non_missing_sample_pheno_file) class_level pheno run_if or,meta_strata,meta_strata_value,meta_trait

failed_tag=Failed
passed_tag=Passed

local cmd make_pheno_quantile_file=$smart_cut_cmd --in-delim $tab --exec "sed 's/^/$passed_tag\t/' !{input,,pheno_non_missing_sample_pheno_file}" --exec "sed 's/^/$failed_tag\t/' !{input,,pheno_failed_non_missing_sample_pheno_file}" --exec "cat !{input,,pheno_non_missing_sample_pheno_file} !{input,,pheno_failed_non_missing_sample_pheno_file} | sed 's/^/$all_tag\t/'" | $table_sum_stats_cmd --out-delim $tab --group-col 1 --col 3 !{prop,--quantile,pheno,low_quantile,missing_key=default_low_quantile} !{prop,--quantile,pheno,med_quantile,missing_key=default_med_quantile} !{prop,--quantile,pheno,high_quantile,missing_key=default_high_quantile}  --print-header > !{output,,pheno_quantile_file} class_level pheno run_if pheno_qt

non_missing_sample_info_helper=$smart_join_cmd --in-delim $tab !{input,--file,@1} --multiple 1 --extra 2 --exec "perl -le 'print \"!{prop,,pheno_value,value,missing_prop=pheno_value,sort_prop=\@pheno_value,sep=\t}\n!{prop,,pheno_value,disp,sort_prop=\@pheno_value,missing_prop=pheno_value,sep=\t}\n!{prop,,pheno_value,sort,sort_prop=\@pheno_value,missing=1,sep=\t}\"' | $transpose_cmd --in-delim $tab" --col 1,2 | awk -F"\t" -v OFS="\t" '{s=\$1; \$1=\$2; \$2=s} {print}'

local cmd make_pheno_non_missing_sample_info_file=$non_missing_sample_info_helper(pheno_non_missing_sample_pheno_file) > !{output,,pheno_non_missing_sample_info_file} class_level pheno skip_if pheno_qt run_if and,!meta_strata,!meta_strata_value,!meta_trait with project
local cmd make_pheno_failed_non_missing_sample_info_file=$non_missing_sample_info_helper(pheno_failed_non_missing_sample_pheno_file) > !{output,,pheno_failed_non_missing_sample_info_file} class_level pheno skip_if pheno_qt run_if and,!meta_strata,!meta_strata_value,!meta_trait

local cmd make_pheno_meta_non_missing_sample_info_file=$meta_pheno_sample_helper(pheno_non_missing_sample_info_file) class_level pheno run_if or,meta_strata,meta_strata_value,meta_trait
local cmd make_pheno_meta_failed_non_missing_sample_info_file=$meta_pheno_sample_helper(pheno_failed_non_missing_sample_info_file) class_level pheno run_if or,meta_strata,meta_strata_value,meta_trait

fetch_quantile_helper=$smart_cut_cmd --in-delim $tab !{input,--file,pheno_quantile_file} --select-row 1,1,$all_tag --select-col 1,1,${table_sum_stats_quantile}!{prop,,pheno,low_quantile,missing_key=default_@{1}_quantile}

all_tag=All
pheno_half=!{prop,,pheno}_half
pheno_half_for_raw=@{pheno}_half
pheno_extreme=!{prop::pheno}_ext
pheno_extreme_for_raw=@{pheno}_ext

case_pheno_helper_int=!{prop@1,@1,pheno@1,case_pheno@1,missing_key@1=default_case_pheno}
control_pheno_helper_int=!{prop@1,@1,pheno@1,control_pheno@1,missing_key@1=default_control_pheno}
case_pheno_helper=$case_pheno_helper_int()
control_pheno_helper=$control_pheno_helper_int()

#low is control, top is case

qt_non_missing_sample_info_helper=low=`$fetch_quantile_helper(low)` && med=`$fetch_quantile_helper(med)` && high=`$fetch_quantile_helper(high)` && cat !{input,,pheno_@{1}_sample_pheno_file} | awk -F"\t" -v OFS="\t" '{\$4=$qt_missing_order;\$3="Missing";\$5=$default_missing_pheno} \$2 <= '\$med' {\$4=$qt_bottom_order;\$3="$qt_bottom_disp";\$5=$control_pheno_helper} \$2 > '\$med' {\$4=$qt_top_order;\$3="$qt_top_disp";\$5=$case_pheno_helper} {\$6=$default_missing_pheno} \$2 <= '\$low' {\$6=$control_pheno_helper} \$2 > '\$high' {\$6=$case_pheno_helper} {print}'

local cmd make_pheno_qt_non_missing_sample_info_file=$qt_non_missing_sample_info_helper(non_missing) > !{output,,pheno_non_missing_sample_info_file} class_level pheno run_if pheno_qt  with project
local cmd make_pheno_qt_failed_non_missing_sample_info_file=$qt_non_missing_sample_info_helper(failed_non_missing) > !{output,,pheno_failed_non_missing_sample_info_file} class_level pheno run_if pheno_qt

local cmd make_pheno_all_sample_info_file=cat !{input,,pheno_sample_info_header_file} !{input,,pheno_non_missing_sample_info_file} !{input,,pheno_missing_sample_info_file} > !{output,,pheno_all_sample_info_file} class_level pheno


open_pheno_all_sample_info_file=open IN, "!{input,,pheno_all_sample_info_file}" or die "Cannot read !{input,,pheno_all_sample_info_file}"; <IN>

local cmd make_pheno_indicator_list_file=sed 's/\\#\\#//' !{input,,pheno_plinkseq_indicator_phe_file_header1} | cut -d, -f1 | $fix_plink_covar_names() > !{output,,pheno_indicator_list_file} class_level pheno skip_if pheno_qt

indicator_missing_pheno=-9

local cmd make_pheno_plinkseq_indicator_phe_file_header1=cut -f1,2 !{input,,pheno_all_sample_info_file} | tail -n+2 | perl -ne 'chomp; @a = split("\t"); \$pheno = \$a[1]; \$value = "!{prop,,pheno}_\$pheno"; if (\$pheno ne "!{prop,,pheno,pheno_missing}" && !\$map{\$pheno}) {\$map{\$pheno} = 1; print "\\#\\#\$value,Integer,$indicator_missing_pheno,Indicator variable for !{prop,,pheno} value \$pheno\n"}' > !{output,,pheno_plinkseq_indicator_phe_file_header1} class_level pheno skip_if pheno_qt

local cmd make_pheno_plinkseq_indicator_phe_file_body=perl -e '$open_pheno_all_sample_info_file; @map = (); while (<IN>) {chomp; @a = split("\t"); \$pheno = \$a[1]; \$value = "!{prop,,pheno}_\$pheno"; if (\$pheno ne "!{prop,,pheno,pheno_missing}" && !exists \$map{\$pheno}) {push @map, \$value; \$map{\$pheno} = \$\#map;}} close IN; print "\#ID\t" . join("\t", @map) . "\n"; $open_pheno_all_sample_info_file; while (<IN>) {chomp; @a = split("\t"); \$pheno = \$a[1]; \$id = \$a[0]; @phenos = split("", 0 x scalar @map); if (\$pheno eq "!{prop,,pheno,pheno_missing}") {@phenos = map {$indicator_missing_pheno} @phenos} else {\$phenos[\$map{\$pheno}] = 1} print "\$id\t" . join("\t", @phenos) . "\n"}'  > !{output,,pheno_plinkseq_indicator_phe_file_body} class_level pheno skip_if pheno_qt

pheno_plinkseq_phe_file_header1_helper=\#\#!{prop,,pheno},!{prop,,pheno,pheno_type},!{prop,,pheno,pheno_missing},!{prop,,pheno,pheno_description}
meta_table cmd make_pheno_plinkseq_phe_file_header1=$pheno_plinkseq_phe_file_header1_helper !{output,pheno_plinkseq_phe_file_header1} class_level pheno skip_if pheno_qt

meta_table cmd make_pheno_qt_plinkseq_phe_file_header1=$pheno_plinkseq_phe_file_header1_helper\n\#\#$pheno_half,Integer,$default_missing_pheno,!{prop,,pheno,pheno_description} thresholded at median\n\#\#$pheno_extreme,Integer,$default_missing_pheno,!{prop,,pheno,pheno_description} thresholded below !{prop,,pheno,low_quantile,missing_key=default_low_quantile} or above !{prop,,pheno,high_quantile,missing_key=default_high_quantile} !{output,pheno_plinkseq_phe_file_header1} class_level pheno run_if pheno_qt

local cmd make_pheno_plinkseq_phe_file_body=cut -f1,2,5- !{input,,pheno_all_sample_info_file} > !{output,,pheno_plinkseq_phe_file_body} class_level pheno

local cmd make_pheno_plinkseq_phe_file=cat !{input,,pheno_plinkseq_phe_file_header1} !{input,,pheno_plinkseq_phe_file_body} > !{output,,pheno_plinkseq_phe_file} class_level pheno

local cmd make_pheno_plinkseq_strata_phe_file_header1=cat /dev/null !{input,,pheno_plinkseq_phe_file_header1,if_prop=pheno:eq:@strata_traits,if_prop=project:eq:@project,all_instances=1,allow_empty=1} !{input,,pheno_plinkseq_phe_file_header1,if_prop=pheno:eq:@covar_traits,unless_prop=pheno:eq:@strata_traits,if_prop=project:eq:@project,all_instances=1,allow_empty=1} !{input,,pheno_plinkseq_phe_file_header1,if_prop=pheno:eq:@extra_traits,unless_prop=pheno:eq:@strata_traits,unless_prop=pheno:eq:@covar_traits,if_prop=project:eq:@project,all_instances=1,allow_empty=1} !{input,,pheno_plinkseq_indicator_phe_file_header1,if_prop=pheno:eq:@strata_traits,unless_prop=pheno_qt,if_prop=project:eq:@project,all_instances=1,allow_empty=1} !{input,,pheno_plinkseq_indicator_phe_file_header1,if_prop=pheno:eq:@covar_traits,unless_prop=pheno:eq:@strata_traits,unless_prop=pheno_qt,if_prop=project:eq:@project,all_instances=1,allow_empty=1} !{input,,pheno_plinkseq_indicator_phe_file_header1,if_prop=pheno:eq:@extra_traits,unless_prop=pheno:eq:@covar_traits,unless_prop=pheno:eq:@strata_traits,unless_prop=pheno_qt,if_prop=project:eq:@project,all_instances=1,allow_empty=1} > !{output,,pheno_plinkseq_strata_phe_file_header1} class_level pheno run_if or,strata_traits,covar_traits,extra_traits

local cmd make_pheno_plinkseq_strata_phe_file_body=$smart_join_cmd !{raw,--exec,pheno,"cut -f1 *pheno_plinkseq_phe_file_body",if_prop=pheno:eq:@strata_traits,if_prop=project:eq:@project,all_instances=1,allow_empty=1} !{raw,--exec,pheno,"cut -f1 *pheno_plinkseq_phe_file_body",if_prop=pheno:eq:@covar_traits,unless_prop=pheno:eq:@strata_traits,if_prop=project:eq:@project,all_instances=1,allow_empty=1} !{raw,--exec,pheno,"cut -f1 *pheno_plinkseq_phe_file_body",if_prop=pheno:eq:@extra_traits,unless_prop=pheno:eq:@strata_traits,unless_prop=pheno:eq:@covar_traits,if_prop=project:eq:@project,all_instances=1,allow_empty=1} !{raw,--exec,pheno,"cut -f1 *pheno_plinkseq_indicator_phe_file_body",if_prop=pheno:eq:@strata_traits,unless_prop=pheno_qt,if_prop=project:eq:@project,all_instances=1,allow_empty=1} !{raw,--exec,pheno,"cut -f1 *pheno_plinkseq_indicator_phe_file_body",if_prop=pheno:eq:@covar_traits,unless_prop=pheno:eq:@strata_traits,unless_prop=pheno_qt,if_prop=project:eq:@project,all_instances=1,allow_empty=1} !{raw,--exec,pheno,"cut -f1 *pheno_plinkseq_indicator_phe_file_body",if_prop=pheno:eq:@extra_traits,unless_prop=pheno:eq:@covar_traits,unless_prop=pheno:eq:@strata_traits,unless_prop=pheno_qt,if_prop=project:eq:@project,all_instances=1,allow_empty=1} !{input,--file,pheno_plinkseq_phe_file_body,if_prop=pheno:eq:@strata_traits,if_prop=project:eq:@project,all_instances=1,allow_empty=1} !{input,--file,pheno_plinkseq_phe_file_body,if_prop=pheno:eq:@covar_traits,unless_prop=pheno:eq:@strata_traits,if_prop=project:eq:@project,all_instances=1,allow_empty=1} !{input,--file,pheno_plinkseq_phe_file_body,if_prop=pheno:eq:@extra_traits,unless_prop=pheno:eq:@strata_traits,unless_prop=pheno:eq:@covar_traits,if_prop=project:eq:@project,all_instances=1,allow_empty=1} !{input,--file,pheno_plinkseq_indicator_phe_file_body,if_prop=pheno:eq:@strata_traits,unless_prop=pheno_qt,if_prop=project:eq:@project,all_instances=1,allow_empty=1} !{input,--file,pheno_plinkseq_indicator_phe_file_body,if_prop=pheno:eq:@covar_traits,unless_prop=pheno:eq:@strata_traits,unless_prop=pheno_qt,if_prop=project:eq:@project,all_instances=1,allow_empty=1} !{input,--file,pheno_plinkseq_indicator_phe_file_body,if_prop=pheno:eq:@extra_traits,unless_prop=pheno:eq:@covar_traits,unless_prop=pheno:eq:@strata_traits,unless_prop=pheno_qt,if_prop=project:eq:@project,all_instances=1,allow_empty=1} --header 1 --in-delim $tab --out-delim $tab > !{output,,pheno_plinkseq_strata_phe_file_body} class_level pheno run_if or,strata_traits,covar_traits,extra_traits

local cmd make_pheno_plinkseq_strata_phe_file=cat !{input,,pheno_plinkseq_strata_phe_file_header1} !{input,,pheno_plinkseq_strata_phe_file_body} > !{output,,pheno_plinkseq_strata_phe_file} class_level pheno run_if or,strata_traits,covar_traits

local cmd make_pheno_sequenced_all_sample_info_file=cat !{input,,pheno_all_sample_info_file} !{input,,pheno_failed_non_missing_sample_info_file} !{input,,pheno_failed_missing_sample_info_file} > !{output,,pheno_sequenced_all_sample_info_file} class_level pheno


!+expand:;:qttype;extraprocess;extrarunif:\
reg;| sed '1! s/$case_pheno_helper$/1temp/' | sed '1! s/$control_pheno_helper$/0/' | sed '1! s/temp$//';skip_if pheno_qt:\
qt;;run_if pheno_qt+ \
local cmd make_pheno_qttype_sequenced_all_sample_box_info_file=$smart_cut_cmd --in-delim $tab !{input,--file,pheno_sequenced_all_sample_info_file} --select-col 1,1,'\\#ID !{prop,,pheno}' --exact --require-col-match | awk -F"\t" -v OFS="\t" '\$2 == "!{prop,,pheno,pheno_missing}" {\$2 = "NA"} {print}' | sed '1 s/!{prop,,pheno}/!{prop,,pheno,disp}/' extraprocess > !{output,,pheno_sequenced_all_sample_box_info_file} class_level pheno extrarunif

prop related_traits=list

related_trait_join_helper=--exec $smart_cut_cmd --in-delim $tab --select-col 1,1,'ID @pheno $display_col_name $order_col_name'

local cmd make_pheno_sequenced_all_trait_all_sample_box_info_file=$smart_join_cmd !{input,--file,pheno_sequenced_all_sample_box_info_file} !{input,--file,pheno_sequenced_all_sample_box_info_file,all_instances=1,if_prop=pheno:eq:@related_traits,if_prop=project:eq:@project,allow_empty=1,if_prop=pheno:ne:@pheno} --header 1 --in-delim $tab > !{output,,pheno_sequenced_all_trait_all_sample_box_info_file} class_level pheno run_if related_traits

plink_missing=-9

local cmd make_pheno_plink_phe_file=$sample_list_to_plink_sample_list(--exec "$smart_join_cmd !{input;--file;pheno_sample_basic_all_include_file} --exec \\"cut -f1\,2 !{input;;pheno_non_missing_sample_info_file}\\" --extra 2",0) | awk -v OFS="\t" -F "\t" '\$3 == !{prop,,pheno,pheno_missing} {\$3 = $plink_missing} {print}' > !{output,,pheno_plink_phe_file} class_level pheno with project

local cmd make_pheno_plink_alternate_phe_file=$sample_list_to_plink_sample_list(--exec "$smart_join_cmd --exec \\"sed '1 s/^/ID\n/' !{input;;pheno_sample_basic_all_include_file}\\" --exec \\"cat !{input;;pheno_sample_info_header_file} !{input;;pheno_non_missing_sample_info_file} | cut -f1\,2\,5-\\" --extra 2 --header 1",1) | awk -v OFS="\t" -F "\t" '\$3 == !{prop,,pheno,pheno_missing} {for (i=3;i<=NF;i++) {\$i = $plink_missing}} {print}' !{input,pheno_is_trait_file} > !{output,,pheno_plink_alternate_phe_file} class_level pheno

fix_plink_covar_names=sed '@1 s/-/_/g'

!|expand:,:filetype,extrafile:covar,:covar_cluster,!{input@--file@pheno_plinkseq_cluster_phe_file}| \
local cmd make_pheno_plink_filetype_file=$sample_list_to_plink_sample_list(--exec "$smart_join_cmd --in-delim $tab --header 1 --exec \\"sed '1 s/^/ID\n/' !{input;;pheno_sample_basic_all_include_file}\\" !{input\,--file\,pheno_plinkseq_strata_phe_file\,if_prop=strata_traits\,if_prop=covar_traits\,allow_empty=1\,or_if_prop=1} !{input\,--file\,pheno_plinkseq_covar_phe_file} extrafile --rest-extra 1 --comment \\\#\\\# | sed '1 s/\\\#//'",1) | $fix_plink_covar_names(1) !{input,pheno_is_trait_file} > !{output,,pheno_plink_filetype_file} class_level pheno

get_variable_covars=cat !{input,,@1} | cut -f@2- | awk 'NR == 1 {for (i=1;i<=NF;i++) {m[i]=\$i; v[i]=$plink_missing; c[i]=0; n=NF}} NR > 2 {for(i=1;i<=NF;i++) {if (v[i]==$plink_missing) {v[i]=\$i} else if (\$i != $plink_missing && v[i] != \$i) { c[i]=1 } }} END {for(i=1;i<=n;i++) { if (c[i]) {print m[i]} }}'

local cmd make_pheno_variable_covars_file=$get_variable_covars(pheno_plink_covar_file,3) > !{output,,pheno_variable_covars_file} class_level pheno

epacts_ped_helper=$smart_join_cmd --in-delim $tab --exec "sed '1 s/^/FID\tIID\n/' !{input,,pheno_sample_plink_@{1}_include_file}" --exec "awk -v OFS=\"\t\" 'NR == 1 {print \"FID\",\"IID\",\"PID\",\"MID\",\"SEX\",\"DUMMY\"} {print \\$1,\\$2,\\$3,\\$4,\\$5,\\$6}' !{input,,pheno_fam_file}" !{input,--file,pheno_plink_alternate_phe_file} @2 --rest-extra 1 --col 1 --col 2 --header 1 | sed '1 s/^/\#/'

!!expand:type:all:unrelated! \
"""
}
    