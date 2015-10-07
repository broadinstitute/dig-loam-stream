
package loamstream.pipeline.examples

/**
 * LoamStream
 * Created by oliverr on 10/6/2015.
 */
object PipelineTargetedPart18 {
  val string =
 """!!expand:,:projectt,projecte,skipif,shortt:project,,skip_if num_var_subsets,:project_variant_subset,_project_variant_subset,,short! \
!!expand:tname:qc_plus! \ 
projectt_tname_annotated_pre_vcounts_helper=$smart_join_cmd --header 1 --col 3,2 --multiple 3 --in-delim $tab --exec "$smart_cut_cmd !{input,--file,projectt_full_var_annot_file} !{input,--file,projectt_plinkseq_tname_combined_frq_file} --select-col 1,1,$vep_id_annot --select-col 2,1,SNP --exclude-row 1-2,1 | sort | uniq -d | sed '1 s/^/ID\n/'"  --exec "$smart_cut_cmd --in-delim $tab !{input,--file,projectt_full_var_annot_file} --select-col 1,1,$vep_id_annot --select-col 1,1,$vep_consequence_annot --select-row 1,1,$vep_consequence_annot,ne:$annot_missing_field --exact --require-col-match" !{input,--file,@1} --exec "$table_sum_stats_cmd --in-delim $tab --has-header --print-header --out-delim $tab !{input,,projectt_plinkseq_tname_combined_strata_frq_file} --group-col SNP --col MAF --threshold 0 | $smart_cut_cmd --in-delim $tab --select-col 0,1 --select-col 0,1,'${threshold_num_above}0 ${threshold_num_equal}0' | awk -F\"\t\" -v OFS=\"\t\" 'NR == 1 {\\$2=\"One\";\\$3=\"Every\";\\$4=\"Some\"} NR > 1 {if (\\$3 == 0) {\\$3 = 1; \\$2 = 0; \\$4 = 0} else if (\\$2 == 1) {\\$2 = 1; \\$3 = 0; \\$4 = 0} else {\\$4 = 1; \\$2 = 0; \\$3 = 0;}} {print}'" --rest-extra 1 

!!expand:,:projectt,projecte,skipif,shortt:project,,skip_if num_var_subsets,:project_variant_subset,_project_variant_subset,,short! \
!!expand:tname:qc_plus! \ 
shortt cmd make_projectt_tname_annotated_all_pre_vcounts_file=$projectt_tname_annotated_pre_vcounts_helper(projectt_plinkseq_tname_combined_frq_file,,) | perl /home/unix/flannick/lap/projects/common/add_function.pl --in-delim '	' --header 1 --col1 MAF --col2 NCHROBS --round --type multiply --val-header MAC --add-at NCHROBS > !{output,,projectt_tname_annotated_all_pre_vcounts_file} class_level projectt skipif bsub_batch 20

!!expand:,:projectt,projecte,skipif,shortt:project,,skip_if num_var_subsets,:project_variant_subset,_project_variant_subset,,short! \
!!expand:tname:qc_plus! \ 
shortt cmd make_projectt_tname_annotated_strata_pre_vcounts_file=$projectt_tname_annotated_pre_vcounts_helper(projectt_plinkseq_tname_combined_strata_frq_file,CLST,--group-col CLST) > !{output,,projectt_tname_annotated_strata_pre_vcounts_file} class_level projectt skipif bsub_batch 20


!!expand:,:projectt,projecte,skipif,shortt:project,,skip_if num_var_subsets,:project_variant_subset,_project_variant_subset,,short! \
!!expand:tname:qc_plus! \ 
projectt_tname_annotated_vcounts_helper=awk -F"\t" -v OFS="\t" 'NR == 1 {for (i=1;i<=NF;i++) {m[\$i]=i}} NR > 1 {if (\$m["MAF"] == 0) {\$m["One"] = 0; \$m["Every"] = 0; \$m["Some"] = 0}  {\$m["One"] *= \$m["MAF"]; \$m["Some"] *= \$m["MAF"]; \$m["Every"] *= \$m["MAF"]}} {print}' | $smart_cut_cmd --in-delim $tab --select-col 0,1,'$vep_consequence_annot MAF One Some Every @2' --select-row 0,1 --select-row 0,1,MAF,gt:0 | awk -F"\t" -v OFS="\t" '{split(\$1,a,",")} NR == 1 {print "$vep_consequence_annot",\$0} NR > 1 {for (i=1;i<=!{raw,,projectt,1,if_prop=one_annotation_per_variant,allow_empty=1}!{raw,,projectt,n,unless_prop=one_annotation_per_variant,allow_empty=1};i++) { print a[i],\$0 }}' | cut -f1,3- | $table_sum_stats_cmd --in-delim $tab --has-header --out-delim $tab --group-col $vep_consequence_annot @3 --totals --threshold $low_freq_maf --threshold $rare_maf --threshold 0 --print-header --col MAF --col One --col Some --col Every | $smart_cut_cmd --in-delim $tab --exact --require-col-match --select-col 0,1,'$vep_consequence_annot MAF_${threshold_num_above}0 MAF_${threshold_num_above}${low_freq_maf} MAF_${threshold_num_lte}${low_freq_maf} MAF_${threshold_num_lte}${rare_maf} MAF_${threshold_num_lte}0 One_${threshold_num_above}0 One_${threshold_num_above}${low_freq_maf} One_${threshold_num_lte}${low_freq_maf} One_${threshold_num_lte}${rare_maf} One_${threshold_num_lte}0 Some_${threshold_num_above}0 Some_${threshold_num_above}${low_freq_maf} Some_${threshold_num_lte}${low_freq_maf} Some_${threshold_num_lte}${rare_maf} Some_${threshold_num_lte}0 Every_${threshold_num_above}0 Every_${threshold_num_above}${low_freq_maf} Every_${threshold_num_lte}${low_freq_maf} Every_${threshold_num_lte}${rare_maf} Every_${threshold_num_lte}0 @2' | awk -F"\t" -v OFS="\t" 'NR > 1 {\$4=\$4-\$5; \$5=\$5-\$6; \$9=\$9-\$10; \$10=\$10-\$11; \$14=\$14-\$15; \$15=\$15-\$16; \$19=\$19-\$20; \$20=\$20-\$21} {print}' | $smart_cut_cmd --in-delim $tab --exclude-col 0,1,'${threshold_num_lte}0$' | sed '1 s/${threshold_num_above}${low_freq_maf}/>${low_freq_maf}/g' | sed '1 s/${threshold_num_lte}${low_freq_maf}/${rare_maf}-${low_freq_maf}/g' | sed '1 s/${threshold_num_lte}${rare_maf}/<$rare_maf/g' | sed '1 s/MAF_/All_/g' | sed '1 s/${threshold_num_above}0/num/g' | $smart_cut_cmd --in-delim $tab --select-col 0,1,'$vep_consequence_annot CLST .' 

!!expand:,:projectt,projecte,skipif,shortt:project,,skip_if num_var_subsets,:project_variant_subset,_project_variant_subset,,short! \
!!expand:tname:qc_plus! \ 
shortt cmd make_projectt_tname_annotated_all_vcounts_file=cat !{input,,projectt_tname_annotated_all_pre_vcounts_file} | $projectt_tname_annotated_vcounts_helper(projectt_plinkseq_tname_combined_frq_file,,) > !{output,,projectt_tname_annotated_all_vcounts_file} class_level projectt skipif bsub_batch 20

!!expand:,:projectt,projecte,skipif,shortt:project,,skip_if num_var_subsets,:project_variant_subset,_project_variant_subset,,short! \
!!expand:tname:qc_plus! \ 
shortt cmd make_projectt_tname_annotated_strata_vcounts_file=cat !{input,,projectt_tname_annotated_strata_pre_vcounts_file} | $projectt_tname_annotated_vcounts_helper(projectt_plinkseq_tname_combined_strata_frq_file,CLST,--group-col CLST) > !{output,,projectt_tname_annotated_strata_vcounts_file} class_level projectt skipif bsub_batch 20

!|expand:;:tstrat;groupcol:all;:strata;!{raw,,project,--group-col CLST,if_prop=maf_strata_trait,allow_empty=1}| \ 
!!expand:tname:qc_plus! \ 
local cmd make_cat_project_tname_annotated_tstrat_vcounts_file=(head -n1 !{input,,project_variant_subset_tname_annotated_tstrat_vcounts_file,limit=1} && tail -qn+2 !{input,,project_variant_subset_tname_annotated_tstrat_vcounts_file}) | $table_sum_stats_cmd --in-delim $tab --has-header --out-delim $tab --group-col $vep_consequence_annot groupcol --totals --print-header `for f in All One Some Every; do for g in num '>${low_freq_maf}' '${rare_maf}-${low_freq_maf}' '<${rare_maf}'; do echo --col \${f}_\${g}; done; done`  | $smart_cut_cmd --in-delim $tab --select-col 0,1,'$vep_consequence_annot !{raw,,project,CLST,if_prop=maf_strata_trait,allow_empty=1} _tot$' | sed '1 s/_tot//g' > !{output,,project_tname_annotated_tstrat_vcounts_file} class_level project run_if num_var_subsets 

!!expand:,:projectt,projecte,skipif,shortt:project,,skip_if num_var_subsets,:project_variant_subset,_project_variant_subset,,short! \
!!expand:tname:qc_plus! \ 
projectt_tname_annotated_sample_vcounts_helper=awk -F"\t" -v OFS="\t" 'NR == 1 {for (i=1;i<=NF;i++) {m[\$i]=i}} NR > 1 {if (\$m["MAF"] == 0) {\$m["One"] = 0; \$m["Some"] = 0; \$m["Every"] = 0}  {\$m["One"] *= \$m["MAC"]; \$m["Some"] *= \$m["MAC"]; \$m["Every"] *= \$m["MAC"]}} NR == 1 {print \$0,">${low_freq_maf}","${rare_maf}-${low_freq_maf}","<${rare_maf}"} NR > 1 {if (\$m["MAF"] > 0 && \$m["MAF"] <= $rare_maf) {r=\$m["MAC"]} else {r=0} if (\$m["MAF"] > $rare_maf && \$m["MAF"] <= $low_freq_maf) {l=\$m["MAC"]} else {l=0} if (\$m["MAF"] > $low_freq_maf) {c=\$m["MAC"]} else {c=0} {print \$0,c,l,r}}' | $smart_cut_cmd --in-delim $tab --select-col 0,1,'$vep_consequence_annot ^N$ MAC One Some Every >${low_freq_maf} ${rare_maf}-${low_freq_maf} <${rare_maf} @2' --select-row 0,1 --select-row 0,1,MAF,gt:0 | awk -F"\t" -v OFS="\t" '{split(\$1,a,","); } NR == 1 {print "$vep_consequence_annot",\$0} NR > 1 {for (i=1;i<=!{raw,,projectt,1,if_prop=one_annotation_per_variant,allow_empty=1}!{raw,,projectt,n,unless_prop=one_annotation_per_variant,allow_empty=1};i++) { print a[i],\$0 }}' | cut -f1,3- | $table_sum_stats_cmd --in-delim $tab --has-header --out-delim $tab --group-col $vep_consequence_annot @3 --totals --summaries --print-header --col MAC --col One --col Some --col Every --col '>${low_freq_maf}' --col '${rare_maf}-${low_freq_maf}' --col '<${rare_maf}' --col N | $smart_cut_cmd --in-delim $tab --exact --require-col-match --select-col 0,1,'$vep_consequence_annot ^N_mean$ MAC_tot One_tot Some_tot Every_tot >${low_freq_maf}_tot ${rare_maf}-${low_freq_maf}_tot <${rare_maf}_tot @2' | awk -F"\t" -v OFS="\t" 'NR > 1 {for (i=3;i<=9;i++) {\$i /= \$2}} {print}' | sed '1 s/\(MAC\|One\|Some\|Every\|>${low_freq_maf}\|${rare_maf}-${low_freq_maf}\|<${rare_maf}\)_tot/\1_mean/g' | sed '1 s/MAC/All/' | $smart_cut_cmd --in-delim $tab --select-col 0,1,'$vep_consequence_annot CLST .' 

!!expand:,:projectt,projecte,skipif,shortt:project,,skip_if num_var_subsets,:project_variant_subset,_project_variant_subset,,short! \
!!expand:tname:qc_plus! \ 
shortt cmd make_projectt_tname_annotated_all_sample_vcounts_file=n=`cat !{input,,project_sample_include_file} | wc -l` && cat !{input,,projectt_tname_annotated_all_pre_vcounts_file} | sed '1 s/$/\tN/' | sed "1! s/$/\t\$n/" | $projectt_tname_annotated_sample_vcounts_helper(projectt_plinkseq_tname_combined_frq_file,,) > !{output,,projectt_tname_annotated_all_sample_vcounts_file} class_level projectt skipif bsub_batch 20

!!expand:,:projectt,projecte,skipif,shortt:project,,skip_if num_var_subsets,:project_variant_subset,_project_variant_subset,,short! \
!!expand:tname:qc_plus! \ 
shortt cmd make_projectt_tname_annotated_strata_sample_vcounts_file=$smart_join_cmd --in-delim $tab --header 1 --exec "$smart_cut_cmd --in-delim $tab !{input,--file,projectt_tname_annotated_strata_pre_vcounts_file} --select-col 1,1,'CLST .'" --exec "sort -u !{input;;pheno_plink_phe_file;if_prop=pheno:eq:@maf_strata_trait} | awk '{print \\$3}' | sort | uniq -c | awk -v OFS=\"\\t\" '{print \\$2,\\$1}' | sed '1 s/^/CLST\tN\n/'" --multiple 1 | $projectt_tname_annotated_sample_vcounts_helper(projectt_plinkseq_tname_combined_strata_frq_file,CLST,--group-col CLST) > !{output,,projectt_tname_annotated_strata_sample_vcounts_file} class_level projectt skipif bsub_batch 20 run_if maf_strata_trait

!|expand:;:tstrat;groupcol:all;:strata;!{raw,,project,--group-col CLST,if_prop=maf_strata_trait,allow_empty=1}| \ 
!!expand:tname:qc_plus! \ 
local cmd make_cat_project_tname_annotated_tstrat_sample_vcounts_file=(head -n1 !{input,,project_variant_subset_tname_annotated_tstrat_sample_vcounts_file,limit=1} && tail -qn+2 !{input,,project_variant_subset_tname_annotated_tstrat_sample_vcounts_file}) | $table_sum_stats_cmd --in-delim $tab --has-header --out-delim $tab --group-col $vep_consequence_annot groupcol --totals --print-header `for f in All One Some Every '>${low_freq_maf}' '${rare_maf}-${low_freq_maf}' '<${rare_maf}'; do echo --col \${f}_mean; done` | $smart_cut_cmd --in-delim $tab --select-col 0,1,'$vep_consequence_annot !{raw,,project,CLST,if_prop=maf_strata_trait,allow_empty=1} _tot$' | sed '1 s/_tot//g' > !{output,,project_tname_annotated_tstrat_sample_vcounts_file} class_level project run_if num_var_subsets 


#!!expand:,:projectt,projecte,runif,shortt:project,,skip_if num_var_subsets,:project_variant_subset,_project_variant_subset,,short! \
#shortt cmd make_projectt_vfreq_file=$pseq_filter_samples_analysis_cmdprojecte(v-freq) $show_id > !{output,,projectt_vfreq_file} class_level projectt runif

!!expand:,:type,fileno,extracmd:snps,$pseq_snp_tag,:indels,$pseq_indel_tag,! \
!!expand:,:projectt,projecte,runif,shortt:project,,skip_if num_var_subsets,:project_variant_subset,_project_variant_subset,,short! \
shortt cmd make_projectt_type_vfreq_file=$pseq_filter_only_samples_analysis_cmdprojecte(v-freq) $show_id --mask file=fileno extracmd > !{output,,projectt_type_vfreq_file} class_level projectt runif

!!expand:,:projectt,projecte,runif,shortt:project,,skip_if num_var_subsets,:project_variant_subset,_project_variant_subset,,short! \
shortt cmd make_projectt_multiallelics_vfreq_file=$smart_join_cmd --ignore-err . --in-delim $tab --exec "$pseq_filter_only_samples_analysis_cmdprojecte(v-freq) $show_id --mask file=$pseq_multiallelic_tag | sed '1 s/^/CHR:POS:/' | sed 's/:/\t/' | sed 's/:/\t/'" --exec "$smart_cut_cmd --in-delim $tab !{input,--file,projectt_multiallelic_frq_file} --exact --require-col-match --select-col 1,1,'id call_rate'" --header 1 --col 1,3 | awk -F"\t" -v OFS="\t" '{t=\$1; \$1=\$2; \$2=\$3; \$3=t} {print}' | sed 's/\t/:/' | sed 's/\t/:/' | sed '1 s/\S\S*:\S\S*://' | awk -v OFS="\t" -F"\t" 'NR == 1 {for (i=1;i<=NF;i++) {if (\$i == "GENO") {g=i} if (\$i == "call_rate") {c=i}}} NR > 1 {\$g=\$c} {print}' | rev | cut -f2- | rev > !{output,,projectt_multiallelics_vfreq_file} class_level projectt runif

!!expand:,:project,runif:project,skip_if num_var_subsets:project_variant_subset,run_if num_var_subsets! \
local cmd make_project_vfreq_file=$smart_join_cmd --merge --header 1 --in-delim $tab !{input,--file,project_snps_vfreq_file} !{input,--file,project_indels_vfreq_file} !{input,--file,project_multiallelics_vfreq_file} > !{output,,project_vfreq_file} class_level project runif

vfreq_cols=QUAL GENO MAF HWE HET NSNP

show_id=--show-id
parse_out_id=cut -d: -f3-
split_parsed_id=sed 's/:/\t/' | sed 's/:/\t/'

prop extra_vstats_cols=list

vcf_variant_type_annot=TYPE

!!expand:projectt:project:project_variant_subset! \
mq0_helper_projectt=$zcat_helper(projectt) @1 | $vcf_utils_cmd --print-annots --annot-type $vcf_variant_type_annot | $smart_cut_cmd --in-delim $tab --select-col 0,1,ID --select-col 0,1,'$vcf_het_ab_annot $vcf_mq0_annot $vcf_variant_type_annot !{prop,,project,extra_vstats_cols,if_prop=extra_vstats_cols,allow_empty=1}' --exact

#!!expand:projecte::_project_variant_subset! \
 #mq0_helperprojecte=$pseq_filter_samples_analysis_cmdprojecte(meta-matrix) @1 | $smart_cut_cmd --in-delim $tab --select-col 0,3 --select-col 0,1,'$vcf_het_ab_annot $vcf_mq0_annot'

!!expand:projecte::_project_variant_subset! \
vstats_helperprojecte=$plinkseq_variant_meta_cmdprojecte(@1,@2) --out-delim $tab | $parse_out_id

!!expand:projecte::_project_variant_subset! \
vstats_dev_helperprojecte=$plinkseq_variant_meta_nohead_cmdprojecte(@1 @2,DEV_@{1}) --type dev --out-delim $tab | $parse_out_id

!!expand:,:projectt,projecte:project,:project_variant_subset,_project_variant_subset! \
projectt_vstats_alt_gq_helperprojecte=$plinkseq_variant_meta_nohead_cmdprojecte(GQ @1,MEAN_ALT_GQ) --out-delim $tab !{input,--ref-info,projectt_plinkseq_@{2}_vmatrix_file} --ref-gt 0 --ref-col-start 4 | $parse_out_id



#!!expand:,:projectt,projecte,runif,shortt:project,,skip_if num_var_subsets,:project_variant_subset,_project_variant_subset,,short! \
#shortt cmd make_projectt_vstats_file=$smart_join_cmd --exec "$smart_cut_cmd !{input,--file,projectt_vfreq_file} --select-col 1,1 --select-col 1,1,'$vfreq_cols MAF' --in-delim $tab | $parse_out_id | $add_function_cmd --col1 HWE --type minus_log --header 1 --val-header LOG_P_HWE --in-delim $tab | $smart_cut_cmd --in-delim $tab --exclude-col 0,1,HWE --exact" --exec "$pseq_filter_samples_analysis_cmdprojecte(meta-matrix) | $smart_cut_cmd --in-delim $tab --select-col 0,3 --select-col 0,1,'$vcf_het_ab_annot $vcf_mq0_annot'" --exec "$plinkseq_variant_meta_cmdprojecte(DP) --out-delim $tab | $parse_out_id" --header 1 --in-delim $tab --out-delim , --ignore-err $plinkseq_okay_err > !{output,,projectt_vstats_file} class_level projectt runif

!!expand:,:projectt,projecte,runif,shortt:project,,skip_if num_var_subsets,:project_variant_subset,_project_variant_subset,,short! \
shortt cmd make_projectt_mean_alt_gq_file=$smart_join_cmd --merge --header 1 --in-delim $tab --exec "$projectt_vstats_alt_gq_helperprojecte(--mask file=$pseq_snp_tag,snps)" --exec "$projectt_vstats_alt_gq_helperprojecte(--mask file=$pseq_indel_tag,indels)" --exec "$projectt_vstats_alt_gq_helperprojecte(--mask file=$pseq_multiallelic_tag,multiallelics)" --ignore-err $plinkseq_okay_err > !{output,,projectt_mean_alt_gq_file} class_level projectt runif

!!expand:,:metaname,metaName:gq,GQ:dp,DP! \
!!expand:,:projectt,projecte,runif,shortt:project,,skip_if num_var_subsets,:project_variant_subset,_project_variant_subset,,short! \
shortt cmd make_projectt_mean_metaname_file=$smart_join_cmd --merge --header 1 --in-delim $tab --exec "$vstats_helperprojecte(metaName,--mask file=$pseq_snp_tag)" --exec "$vstats_helperprojecte(metaName,--mask file=$pseq_indel_tag)" --exec "$vstats_helperprojecte(metaName,--mask file=$pseq_multiallelic_tag)" --ignore-err $plinkseq_okay_err > !{output,,projectt_mean_metaname_file} class_level projectt runif

!!expand:,:projectt,projecte,runif,shortt:project,,skip_if num_var_subsets,:project_variant_subset,_project_variant_subset,,short! \
shortt cmd make_projectt_dev_dp_file=$smart_join_cmd --merge --header 1 --in-delim $tab --exec "$vstats_dev_helperprojecte(DP,--mask file=$pseq_snp_tag)" --exec "$vstats_dev_helperprojecte(DP,--mask file=$pseq_indel_tag)" --exec "$vstats_dev_helperprojecte(DP,--mask file=$pseq_multiallelic_tag)" --ignore-err $plinkseq_okay_err > !{output,,projectt_dev_dp_file} class_level projectt runif

!!expand%;%projectt;projecte;phenol;pheno_variant_qc_stratat;pheno_variant_qc_pheno_stratat;runif;shortt;extraifprop%project;;pheno;pheno_variant_qc_strata;pheno_variant_qc_pheno_strata;skip_if num_var_subsets;;%project_variant_subset;_project_variant_subset;pheno_variant_subset;pheno_variant_qc_strata_variant_subset;pheno_variant_qc_pheno_strata_variant_subset;consistent_prop var_subset_num bsub_batch 5;short;,if_prop=var_subset_num:eq:@var_subset_num! \
shortt cmd make_projectt_vstats_file=$smart_join_cmd --exec "$smart_cut_cmd --in-delim $tab !{input,--file,projectt_vfreq_file} --select-col 1,1 --select-col 1,1,'$vfreq_cols MAF' --select-row 1,1 --select-row 1,1,FILTER,$vcf_pass --in-delim $tab | $parse_out_id | $add_function_cmd --col1 HWE --type minus_log --header 1 --val-header LOG_P_HWE --in-delim $tab | $smart_cut_cmd --in-delim $tab --exclude-col 0,1,HWE --exact" --exec "$smart_join_cmd --in-delim $tab --header 1 --merge --exec \"$mq0_helper_projectt(!{input::projectt_indel_vcf_file})\" --exec \"$mq0_helper_projectt(!{input::projectt_vcf_file})\" --exec \"$mq0_helper_projectt(!{input::projectt_multiallelic_vcf_file})\" --ignore-err $plinkseq_okay_err"  !{input,--file,projectt_mean_gq_file} !{input,--file,projectt_mean_alt_gq_file} !{input,--file,projectt_mean_dp_file} !{input,--file,projectt_dev_dp_file} !{input,--file,project_custom_vstats_annot_file,if_prop=project_custom_vstats_annot_file,allow_empty=1} !{raw,--exec,projectt,"$smart_join_cmd --in-delim $tab --exec \"sort -u *project_variant_custom_exclude_file | $smart_cut_cmd --in-delim $tab --exec 'cut -f1 *projectt_vfreq_file | tail -n+2 | $parse_out_id' | sort | uniq -d | sed 's/$/\t1/'\" --exec \"cut -f1 *projectt_vfreq_file | tail -n+2 | $parse_out_id | sed 's/$/\t0/'\" --merge | sed '1 s/^/ID\t$custom_exclude_header\n/'",if_prop=project_variant_custom_exclude_file,allow_empty=1} !{input,project_variant_custom_exclude_file,if_prop=project_variant_custom_exclude_file,allow_empty=1}  --header 1 --rest-extra 1 --in-delim $tab --out-delim $vstats_delim --ignore-err $plinkseq_okay_err > !{output,,projectt_vstats_file} class_level projectt runif

!!expand%;%projectt;projecte;phenol;pheno_variant_qc_stratat;pheno_variant_qc_pheno_stratat;runif;shortt;extraifprop%project;;pheno;pheno_variant_qc_strata;pheno_variant_qc_pheno_strata;skip_if num_var_subsets;;%project_variant_subset;_project_variant_subset;pheno_variant_subset;pheno_variant_qc_strata_variant_subset;pheno_variant_qc_pheno_strata_variant_subset;consistent_prop var_subset_num bsub_batch 5;short;,if_prop=var_subset_num:eq:@var_subset_num! \
shortt cmd make_projectt_extended_vstats_file=$smart_join_cmd !{input,--file,projectt_vstats_file} !{input,--file,phenol_variant_qc_strata_vstats_summary_file,if_prop=qc_strata_trait,allow_empty=1extraifprop} !{input,--file,phenol_variant_qc_pheno_strata_vstats_summary_file,if_prop=qc_strata_trait,if_prop=pheno_stratas,allow_empty=1extraifprop}  --header 1 --rest-extra 1 --in-delim $tab --in-delim 1:$vstats_delim --out-delim $vstats_delim --ignore-err $plinkseq_okay_err --arg-delim : !{raw,,projectt,--fill 2 --fill-value NA,if_prop=qc_strata_trait,allow_empty=1extraifprop} !{raw,,projectt,--fill 3,if_prop=qc_strata_trait,if_prop=pheno_stratas,allow_empty=1extraifprop} > !{output,,projectt_extended_vstats_file} class_level projectt runif

#cut out from above command before --exec of smart join
#!{input,--file,pheno_variant_qc_stratat_vstats_file,if_prop=qc_strata_trait,allow_empty=1extraifprop} !{input,--file,pheno_variant_qc_pheno_stratat_vstats_file,if_prop=qc_strata_trait,if_prop=pheno_stratas,allow_empty=1extraifprop}

!|expand:;:vtype;extraadd\
:multiallelics_vfreq_file;\
:indels_vfreq_file;\
:snps_vfreq_file;\
:vfreq_file;\
:mean_alt_gq_file;\
:mean_gq_file;\
:mean_dp_file;\
:dev_dp_file;\
:vstats_file;\
:extended_vstats_file;\
:gene_variant_file;\
:clean_gene_variant_file;\
:clean_all_variant_site_vcf_file;\
:snpeff_file;\
:snpsift_file;\
:chaos_file;\
:vep_file;\
:parsed_vep_file;\
:var_transcript_file;\
:custom_trans_annot_file;skip_if !project_custom_var_annot_file\
:pph2_pos_file;\
:pph2_file;\
:complete_full_annot_file;\
:full_annot_file;\
:full_var_annot_file;\
:transcript_gene_map_file;with burden\
:plinkseq_qc_pass_frq_file;\
:plinkseq_qc_plus_frq_file;skip_if and,!sample_qc_filter,!var_qc_filter,!project_variant_custom_exclude_file,!project_sample_custom_exclude_file\
:plinkseq_qc_pass_counts_file;\
:plinkseq_qc_plus_counts_file;skip_if and,!sample_qc_filter,!var_qc_filter,!project_variant_custom_exclude_file,!project_sample_custom_exclude_file\
:plinkseq_qc_pass_strata_frq_file;\
:plinkseq_qc_plus_strata_frq_file;\
:multiallelic_frq_file;\
:plinkseq_qc_pass_combined_frq_file;\
:plinkseq_qc_plus_combined_frq_file;\
:plinkseq_qc_pass_combined_strata_frq_file;\
:plinkseq_qc_plus_combined_strata_frq_file;\
:plinkseq_qc_pass_strata_frq_summary_file;\
:plinkseq_qc_plus_strata_frq_summary_file;\
:plinkseq_syn_reg_file;\
:plinkseq_ns_reg_file;\
:plinkseq_nonsense_reg_file;\
:plinkseq_coding_reg_file;\
:plinkseq_qc_pass_gstats_file;\
:plinkseq_qc_pass_estats_file;\
:plinkseq_qc_pass_ns_gstats_file;\
:plinkseq_qc_pass_ns_estats_file;\
:plinkseq_qc_pass_syn_gstats_file;\
:plinkseq_qc_pass_syn_estats_file;\
:plinkseq_filtered_gstats_file;\
|\
local cmd make_cat_project_vtype=(head -qn+1 !{input,,project_variant_subset_vtype,limit=1,sort_prop=project_variant_subset} | head -n1 && tail -qn+2 !{input,,project_variant_subset_vtype,sort_prop=project_variant_subset}) > !{output,,project_vtype} class_level project run_if num_var_subsets extraadd

!|expand:;:vtype;extraadd\
:clean_all_variant_site_vcf_file;\
|\
local cmd make_cat_project_vtype=(head -qn+1 !{input,,project_variant_subset_vtype,limit=1,sort_prop=project_variant_subset} | head -n2 && tail -qn+3 !{input,,project_variant_subset_vtype,sort_prop=var_subset_num}) > !{output,,project_vtype} class_level project run_if num_var_subsets extraadd



vstats_delim=,
#local cmd make_project_variant_outlier_file=$write_outlier_table_cmd !{input,,project_vstats_file} !{output,,project_variant_outlier_file} -VAR VAR sep=$vstats_delim out.sep=$vstats_delim class_level project

!!expand:,:variant_outlier,project_vstats_file:variant_outlier,project_vstats_file:variant_extended_outlier,project_extended_vstats_file! \
exclude_variant_outlier_raw_int=$smart_cut_cmd --exec @3"sed '1! s/^/@1$vstats_delim/' !{input,,project_vstats_file} | sed '1 s/^/METRIC$vstats_delim/'@3" --select-row 1,1,@1,@2 --and-row-all --in-delim $vstats_delim --exact --select-col 1,'2 1' --select-col 1,1,@1 --exclude-row 1,1

!!expand:,:variant_outlier,project_vstats_file:variant_outlier,project_vstats_file:variant_extended_outlier,project_extended_vstats_file! \
exclude_variant_outlier_raw=$exclude_variant_outlier_raw_int(@1,@2,)

#$exclude_outlier_raw(project_variant_outlier_file,@1,@2,$vstats_delim)

#exclude_variant_outlier_iqr=$exclude_outlier_iqr(project_variant_outlier_file,@1,@2,$vstats_delim)

!|expand:;:make_var_qc_filter;variant_outlier;skipif:make_var_qc_filter;variant_outlier;skip_if or,is_extended_filter,is_extended_strict_filter:make_extended_var_qc_filter;variant_extended_outlier;skip_if and,!is_extended_filter,!is_extended_strict_filter)| \
short cmd make_var_qc_filter_metric_modifier_exclude_file=$exclude_variant_outlier_raw(!{prop;;var_qc_filter;modifier_filter_metric},!{prop;;var_qc_filter;modifier_filter_metric_op}:!{prop;;var_qc_filter;modifier_filter_metric_value}) > !{output,,var_qc_filter_modifier_exclude_file} class_level var_qc_filter run_if modifier_filter_metric skipif

!|expand:;:var_qc_filter_exclude;variant_outlier:var_qc_filter_exclude;variant_outlier:extended_var_qc_filter_exclude;variant_extended_outlier| \
var_qc_filter_exclude_helper_int=$exclude_variant_outlier_raw_int(!{prop;;var_qc_filter;filter_metric},!{prop;;var_qc_filter;filter_metric_op}:!{prop;;var_qc_filter;filter_metric_value},@1)

!|expand:;:var_qc_filter_exclude;variant_outlier:var_qc_filter_exclude;variant_outlier:extended_var_qc_filter_exclude;variant_extended_outlier| \
var_qc_filter_exclude_helper=$var_qc_filter_exclude_helper_int()

!|expand:;:make_var_qc_filter;var_qc_filter_exclude_helper;skipif:make_var_qc_filter;var_qc_filter_exclude_helper;skip_if or,is_extended_filter,is_extended_strict_filter:make_extended_var_qc_filter;extended_var_qc_filter_exclude_helper;skip_if and,!is_extended_filter,!is_extended_strict_filter| \
short cmd make_var_qc_filter_metric_exclude_file=$var_qc_filter_exclude_helper > !{output,,var_qc_filter_exclude_file} class_level var_qc_filter run_if and,filter_metric,!modifier_filter_metric skipif

!|expand:;:make_var_qc_filter;var_qc_filter_exclude_helper;skipif:make_var_qc_filter;var_qc_filter_exclude_helper;skip_if or,is_extended_filter,is_extended_strict_filter:make_extended_var_qc_filter;extended_var_qc_filter_exclude_helper;skip_if and,!is_extended_filter,!is_extended_strict_filter| \
short cmd make_var_qc_filter_metric_with_modifier_exclude_file=$smart_join_cmd --arg-delim : --out-delim , --in-delim , --exec "$var_qc_filter_exclude_helper_int(\)" --exec "$var_qc_filter_exclude_helper_int(\) | cat - !{input,,var_qc_filter_modifier_exclude_file} | cut -d, -f1 | sort | uniq -d" --extra 1 > !{output,,var_qc_filter_exclude_file} class_level var_qc_filter run_if and,filter_metric,modifier_filter_metric skipif

short cmd make_project_variant_exclude_detail_file=\
 $exclude_variant_outlier_raw(MAF,eq:0) \ 
 !{raw;;project;| $exclude_variant_outlier_raw($custom_exclude_header,eq:1);if_prop=project_variant_custom_exclude_file;allow_empty=1} \
 !{raw,|,var_qc_filter,cat - *var_qc_filter_exclude_file,if_prop=var_qc_filter,unless_prop=is_extended_filter,unless_prop=is_extended_strict_filter,allow_empty=1} !{input,var_qc_filter_exclude_file,if_prop=var_qc_filter,unless_prop=is_extended_filter,unless_prop=is_extended_strict_filter,allow_empty=1} \
 | sort -u | $add_header_cmd "LABEL${vstats_delim}MEASURE${vstats_delim}VALUE" > !{output,,project_variant_exclude_detail_file} class_level project

prop custom_var_keep=list
prop custom_var_exclude=list

local cmd make_project_variant_exclude_file=cut -d$vstats_delim -f1 !{input,,project_variant_exclude_detail_file} | tail -n+2 | sort -u !{raw,,project,| $smart_cut_cmd --exec "echo @custom_var_keep @custom_var_keep | sed 's/\s\s*/\n/g'" | sort| uniq -u,if_prop=custom_var_keep,allow_empty=1} !{raw,,project,| $smart_cut_cmd --exec "echo @custom_var_exclude | sed 's/\s\s*/\n/g'" | sort -u,if_prop=custom_var_exclude,allow_empty=1} > !{output,,project_variant_exclude_file} class_level project

short cmd make_project_extended_variant_exclude_detail_file=\
 cat /dev/null !{raw,|,var_qc_filter,cat - *var_qc_filter_exclude_file,if_prop=var_qc_filter,if_prop=is_extended_filter,allow_empty=1} !{input,var_qc_filter_exclude_file,if_prop=var_qc_filter,if_prop=is_extended_filter,allow_empty=1} \
 | sort -u | $add_header_cmd "LABEL${vstats_delim}MEASURE${vstats_delim}VALUE" > !{output,,project_extended_variant_exclude_detail_file} class_level project

local cmd make_project_extended_variant_exclude_file=cut -d$vstats_delim -f1 !{input,,project_extended_variant_exclude_detail_file} | tail -n+2 !{raw,,project,| cat - *project_extended_variant_custom_exclude_file,if_prop=project_extended_variant_custom_exclude_file,allow_empty=1} !{input,project_extended_variant_custom_exclude_file,if_prop=project_extended_variant_custom_exclude_file,allow_empty=1} | sort -u > !{output,,project_extended_variant_exclude_file} class_level project

short cmd make_project_extended_strict_variant_exclude_detail_file=\
tail -n+2 !{input,,project_extended_variant_exclude_detail_file} !{raw,|,var_qc_filter,cat - *var_qc_filter_exclude_file,if_prop=var_qc_filter,if_prop=is_extended_strict_filter,allow_empty=1} !{input,var_qc_filter_exclude_file,if_prop=var_qc_filter,if_prop=is_extended_strict_filter,allow_empty=1} \
 | sort -u | $add_header_cmd "LABEL${vstats_delim}MEASURE${vstats_delim}VALUE" > !{output,,project_extended_strict_variant_exclude_detail_file} class_level project

local cmd make_project_extended_strict_variant_exclude_file=cut -d$vstats_delim -f1 !{input,,project_extended_strict_variant_exclude_detail_file} | tail -n+2 !{raw,,project,| cat - *project_extended_variant_custom_exclude_file,if_prop=project_extended_variant_custom_exclude_file,allow_empty=1} !{input,project_extended_variant_custom_exclude_file,if_prop=project_extended_variant_custom_exclude_file,allow_empty=1} !{raw,,project,| cat - *project_extended_strict_variant_custom_exclude_file,if_prop=project_extended_variant_custom_exclude_file,allow_empty=1} !{input,project_extended_strict_variant_custom_exclude_file,if_prop=project_extended_strict_variant_custom_exclude_file,allow_empty=1} | sort -u > !{output,,project_extended_strict_variant_exclude_file} class_level project

short cmd make_annot_var_qc_filter_exclude_file=$exclude_variant_extended_outlier_raw_int(!{prop;;annot_var_qc_filter;filter_metric},!{prop;;annot_var_qc_filter;filter_metric_op}:!{prop;;annot_var_qc_filter;filter_metric_value},) > !{output,,annot_var_qc_filter_exclude_file} class_level annot_var_qc_filter

short cmd make_annot_variant_exclude_detail_file=\
 cat /dev/null !{raw,|,annot_var_qc_filter,cat - *annot_var_qc_filter_exclude_file,if_prop=annot_var_qc_filter,allow_empty=1} !{input,annot_var_qc_filter_exclude_file,if_prop=annot_var_qc_filter,allow_empty=1} \
 | sort -u | $add_header_cmd "LABEL${vstats_delim}MEASURE${vstats_delim}VALUE" > !{output,,annot_variant_exclude_detail_file} class_level annot

local cmd make_annot_variant_exclude_file=cut -d$vstats_delim -f1 !{input,,annot_variant_exclude_detail_file} | tail -n+2 | sort -u > !{output,,annot_variant_exclude_file} class_level annot

!!expand:,:projectt,runif,shortt:project,skip_if num_merge_subsets,:project_merge_subset,bsub_batch 10,short! \
shortt cmd make_projectt_snp_indel_overlap_file=$zcat_helper(projectt) !{input,,projectt_merged_vcf_file} | fgrep -v '\\#' | cut -f1-2 | sort | uniq -d > !{output,,projectt_snp_indel_overlap_file} class_level projectt runif

local cmd cat_project_snp_indel_overlap_file=cat !{input,,project_merge_subset_snp_indel_overlap_file,sort_prop=project_merge_subset} > !{output,,project_snp_indel_overlap_file} class_level project run_if num_merge_subsets

max_vstats_points=1000
max_vstats_highlighted_points=200

!!expand:,:vstats_pdf_helper,project_vstats_file:vstats_pdf_helper,project_vstats_file:extended_vstats_pdf_helper,project_extended_vstats_file! \
vstats_pdf_helper=$draw_box_plot_cmd() !{input,,project_vstats_file} !{output,,@1} 'Variant QC properties' '' 'Variant Values' -VAR sep=$vstats_delim max.plot.points=$max_vstats_points max.highlighted.points=$max_vstats_highlighted_points @2
!!expand:,:variant_highlight_info,project_variant_exclude_detail_file:variant_highlight_info,project_variant_exclude_detail_file:extended_variant_highlight_info,project_extended_variant_exclude_detail_file! \
variant_highlight_info=id.col=1 highlight.list.file=!{input,,project_variant_exclude_detail_file} highlight.id.col=1 highlight.label.col=2 highlight.sep=$vstats_delim

!!expand:,:vstats_pdf_helper,project_vstats:vstats_pdf_helper,project_vstats:extended_vstats_pdf_helper,project_extended_vstats! \
restart_mem short cmd make_project_vstats_initial_pdf_file=$vstats_pdf_helper(project_vstats_initial_pdf_file,) class_level project rusage_mod $vstats_plot_mem
!!expand:,:vstats_pdf_helper,project_vstats,variant_highlight_info:vstats_pdf_helper,project_vstats,variant_highlight_info:extended_vstats_pdf_helper,project_extended_vstats,extended_variant_highlight_info! \
restart_mem short cmd make_project_vstats_highlighted_pdf_file=$vstats_pdf_helper(project_vstats_highlighted_pdf_file, $variant_highlight_info) class_level project rusage_mod $vstats_plot_mem
!!expand:,:vstats_pdf_helper,project_vstats,variant_highlight_info:vstats_pdf_helper,project_vstats,variant_highlight_info:extended_vstats_pdf_helper,project_extended_vstats,extended_variant_highlight_info! \
restart_mem short cmd make_project_vstats_final_pdf_file=$vstats_pdf_helper(project_vstats_final_pdf_file, $variant_highlight_info exclude.highlighted=TRUE) class_level project rusage_mod $vstats_plot_mem

fix_locstats_file=awk -F"\t" 'NF > 1'
!!expand:,:gtype,mtype:gene,pseq_genes_loc_group:exon,pseq_exons_loc_group! \
cmd make_plinkseq_gtype_locstats_file=$pseq_project_cmd loc-stats --group $mtype | $catch_sqlite_errors | $fix_locstats_file > !{output,,project_plinkseq_gtype_locstats_file} !{input,project_plinkseq_db_done_file} !{input,project_plinkseq_locdb_file} class_level project

prop do_detailed_gstats=scalar 
prop do_pheno_gstats=scalar 

prop bad_regions_for_genes=list
bad_for_genes_regex_mask=--mask reg.ex=!{prop,,project,bad_regions_for_genes,missing=,sep=\,} 


!!expand:,:stattype,groupname:gstats,pseq_genes_loc_group:estats,pseq_exons_loc_group! \
!|expand:;:projectt;projectl;skipif;shortt;pseqcmd;extramask:\
project;project;skip_if num_var_subsets;;pseq_filter_samples_analysis_cmd;:\
project_variant_subset;project_variant_subset;;;pseq_filter_samples_analysis_cmd_project_variant_subset;:\
pheno;project;skip_if or,num_var_subsets,!do_pheno_gstats;;pseq_all_analysis_cmd_pheno;$mono_ex_mask:\
pheno_variant_subset;project_variant_subset;skip_if !do_pheno_gstats;;pseq_all_analysis_cmd_pheno_variant_subset;$mono_ex_mask| \
!|expand:;:vartype;varmask;runif:;;run_if do_detailed_gstats:_syn;--mask reg.req=@!{input,,projectl_plinkseq_syn_reg_file};run_if do_detailed_gstats:_ns;--mask reg.req=@!{input,,projectl_plinkseq_ns_reg_file};run_if do_detailed_gstats| \
shortt cmd make_projectt_plinkseq_qc_passvartype_stattype_file=$pseqcmd(g-stats) --mask loc.group=$groupname varmask extramask $bad_for_genes_regex_mask > !{output,,projectt_plinkseq_qc_passvartype_stattype_file} class_level projectt runif skipif

#!!expand:,:projectt,projecte,runif,shortt:project,,skip_if num_var_subsets,:project_variant_subset,_project_variant_subset,,short! \
#shortt cmd make_projectt_plinkseq_filtered_gstats_file=$pseq_filtered_analysis_cmdprojecte(g-stats) --mask loc.group=$pseq_genes_loc_group $bad_for_genes_regex_mask > !{output,,projectt_plinkseq_filtered_gstats_file} class_level projectt runif

remove_dup_genes=sed '/^\s\*\S\*_dup/d'

fix_gstats=cut -f1-12
project_gstats_helper=$smart_join_cmd --exec "$smart_cut_cmd !{input,--file,project_plinkseq_qc_pass_gstats_file} --comment \\# --exclude-col 1,1,QC_FAIL --exclude-col 1,1,QC_PCT --in-delim $tab | $fix_gstats" --exec "$smart_cut_cmd !{input,--file,project_plinkseq_filtered_gstats_file} --comment \\# --select-col 1,1 --select-col 1,1,QC_FAIL --select-col 1,1,QC_PCT --in-delim $tab" @1 --exec "$smart_cut_cmd --out-delim $tab --exec \"$catch_sqlite_errors !{input,,project_plinkseq_gene_locstats_file}\" --comment \\# --select-col 1,1 --select-col 1,1,'GC GC_ALL N N_ALL' --exact --require-col-match" --extra 3 --comment \\# --in-delim $tab --arg-delim : --header 1 --out-delim ,

local cmd make_project_gstats_no_coverage_file=$project_gstats_helper() > !{output,,project_gstats_file} class_level project run_if no_coverage

local cmd make_project_gstats_file=$project_gstats_helper(--exec "$table_sum_stats_cmd --summaries --has-header --col $frac_above_threshold --group-col 1 --label '!{prop\,\,project}' --in-delim $coverage_dat_delim --out-delim $tab --print-header < !{input\,\,project_gene_dist_coverage_dat_file} | sed '1 s/$frac_above_threshold_mean/PCT_BASES_${threshold}x/' | cut -f1\,3") --extra 4 | $add_function_cmd --in-delim , --header 1 --col1 BP --col2 PCT_BASES_${threshold}x --type multiply --val-header BP_EFF | $add_function_cmd --in-delim , --header 1 --col1 NVAR --col2 BP_EFF --type divide --val-header VAR_PER_BP > !{output,,project_gstats_file} class_level project skip_if no_coverage

gstats_cols_no_cov=DENS\,NVAR\,SING\,RATE\,GC
gstats_cols=$gstats_cols_no_cov\,PCT_BASES_${threshold}x\,VAR_PER_BP
gstats_cols_helper=!{raw,,project,$gstats_cols,unless_prop=no_coverage,allow_empty=1} !{raw,,project,$gstats_cols_no_cov,if_prop=no_coverage,allow_empty=1}

local cmd make_project_gene_outlier_file_no_coverage=$write_outlier_table_cmd !{input,,project_gstats_file} !{output,,project_gene_outlier_file} $gstats_cols_helper  NAME sep=, out.sep=, class_level project run_if no_coverage

local cmd make_project_gene_outlier_file=$write_outlier_table_cmd !{input,,project_gstats_file} !{output,,project_gene_outlier_file} $gstats_cols_helper NAME sep=, out.sep=, class_level project skip_if no_coverage

exclude_gene_outlier_iqr=$exclude_outlier_iqr(project_gene_outlier_file,@1,@2,\,)
local cmd make_project_gene_exclude_detail_file=$exclude_gene_outlier_iqr(VRATE,ge:2) | $exclude_gene_outlier_iqr(NSING,ge:2) | $exclude_gene_outlier_iqr(QC_PCT,le:-2) | $exclude_gene_outlier_iqr(GC,le:-2) | $smart_cut_cmd --file !{input,,project_gene_outlier_file} --in-delim , --select-row 1,1 > !{output,,project_gene_exclude_detail_file} class_level project

max_gstats_points=1000
max_gstats_highlighted_points=200
gstats_pdf_helper=$draw_box_plot_cmd(!{input\,\,project_gstats_file} !{output\,\,@1} 'Gene QC properties' '' 'Gene Values' $gstats_cols_helper sep=\, max.plot.points=$max_gstats_points max.highlighted.points=$max_gstats_highlighted_points @2) class_level project
gene_highlight_info=id.col=1 highlight.list.file=!{input,,project_gene_exclude_detail_file} highlight.id.col=1 highlight.label.col=2 highlight.sep=,

local cmd make_project_gstats_pdf_file=$gstats_pdf_helper(project_gstats_pdf_file, ) class_level project

local cmd make_project_gstats_highlighted_pdf_file=$gstats_pdf_helper(project_gstats_highlighted_pdf_file, $gene_highlight_info) class_level project

"""
}
    