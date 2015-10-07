
package loamstream.pipeline.examples

/**
 * LoamStream
 * Created by oliverr on 10/6/2015.
 */
object PipelineTargetedPart26 {
  val string =
 """local cmd make_burden_burden_test_info_tex_file=$smart_cut_cmd --in-delim $tab !{input,--file,burden_test_info_file} --select-col 1,1 --select-col .,2 --paste | $table_to_beamer_cmd --header-rows 1 --header-cols 1 --in-delim $tab --title "Burden tests" --auto-dim --font-size 8pt > !{output,,burden_burden_test_info_tex_file} class_level burden run_if burden_test
local cmd make_burden_burden_test_info_pdf_file=$run_latex_cmd(burden_burden_test_info_tex_file,burden_burden_test_info_pdf_file) class_level burden run_if burden_test

!|expand:;:alltype;allskipif:all;!include_related:unrelated;include_related| \
!|expand:;:atype;cmdtype;extracmd;extrarunif:;;;run_if run_raw:;_cluster;$add_strata(burden_test);run_if and,!run_raw,!analytic,(or,strat_cluster,strata_traits):analytic_;_covar;$add_covar(burden_test);run_if and,!run_raw,analytic,(or,strat_covar,covar_traits)| \
!!expand:pathwayburdentype:custom! \
cmd make_atypecmdtypeburden_test_pathway_pathwayburdentypecmdtype_alltype_gassoc_file=$pseq_qc_plus_alltype_assoc_cmd_phenocmdtype($assoc) extracmd $qt_plinkseq_phenotype_selector $burden_test_indiv_ex_helper --perm $nperm_pathway --mask locset.group=$pseq_pathwayburdentype_loc_group,$pseq_pathwayburdentype_locset_name $burden_pathway_pathwayburdentype_mask_helper | $assoc_fix_bug > !{output,,burden_test_pathway_pathwayburdentype_gassoc_file} class_level burden_test extrarunif skip_if or,not_trait,only_for_interesting,!project_pathwayburdentype_locset_file,test_software:ne:pseq,allskipif rusage_mod $pseq_pathway_mem

!|expand:;:alltype;relskipif:all;!include_related:unrelated;include_related| \
!!expand:pathwayburdentype:custom! \
cmd make_burden_test_alltype_epacts_strata_pathway_pathwayburdentype_gassoc_file=c=$get_epacts_covars(burden_test) $meta_skat_covars && $burden_test_epacts_group_helper(burden_test_pathway_pathwayburdentype_gassoc,!{prop::burden_test:test_name},covar_alltype,\$c,!{prop::pheno},!{input::burden_pathway_pathwayburdentype_epacts_group_file}) class_level burden_test run_if or,strat_covar,covar_traits skip_if or,only_for_interesting,run_raw,test_software:ne:epacts,relskipif run_with burden_test_variant_subset

!|expand:;:alltype;relskipif:all;!include_related:unrelated;include_related| \
!!expand:pathwayburdentype:custom! \
cmd make_burden_test_alltype_epacts_raw_pathway_pathwayburdentype_gassoc_file=$burden_test_epacts_group_helper(burden_test_pathway_pathwayburdentype_gassoc,!{prop::burden_test:test_name},popgen_alltype,,!{prop::pheno},!{input::burden_pathway_pathwayburdentype_epacts_group_file}) class_level burden_test run_if run_raw skip_if or,only_for_interesting,test_software:ne:epacts,relskipif run_with burden_test_variant_subset

translate_gene_variant_reg_req=cut -f2,3 | sed 's/\t/:/' | tr '\n' , | sed 's/,$//'

add_burden_gassoc_key=sed 's/^\(\S\S*\)\(\s\s*\)\(\S\S*\)/\1:\3\2\1\2\3/'

default_missing_pheno=-9
default_case_pheno=2
default_control_pheno=1

default_missing_pheno_order=2
default_case_pheno_order=0
default_control_pheno_order=1

prop case_pheno=scalar default 2
prop control_pheno=scalar default 1

fix_two_hit_helper=!{raw,,burden_test,| sed 's/\(TWO-HIT\t\)\(\S\S*\)\(.*\)\(P\=\)\([^;][^;]*\)/\1\5\3\4\5/',if_prop=test_name:eq:two-hit,allow_empty=1}

!|expand:;:alltype;allskipif:all;!include_related:unrelated;include_related| \
!|expand:;:shortt;phenol;burdenl;burden_testl;assocl;exskipif:;pheno;burden;burden_test;assoc;,burden_test_variant_subset:short;pheno_variant_subset;burden_variant_subset;burden_test_variant_subset;assoc_variant_subset;| \
!|expand:;:atype;cmdtype;extracmd;extrarunif:;;;run_if run_raw:;_cluster;$add_strata(burden_testl);run_if and,!run_raw,!analytic,(or,strat_cluster,strata_traits):analytic_;_covar;$add_covar(burden_test);run_if and,!run_raw,analytic,(or,strat_covar,covar_traits)| \
shortt cmd make_atypeburden_testl_case_side_pseqcmdtype_alltype_gassoc_file=$pseq_qc_plus_alltype_assoc_cmd_phenolcmdtype($assocl) extracmd $qt_plinkseq_phenotype_selector $burdenl_gassoc_helper($nperm_gassoc,) $fix_two_hit_helper | $assoc_fix_bug > !{output,,burden_testl_case_side_gassoc_file} !{input,pheno_is_trait_file} class_level burden_testl extrarunif skip_if or,test_software:ne:pseqexskipif,allskipif rusage_mod $pseq_gene_mem run_with burden_test_variant_subset

epacts_group_unit=5

group_dev_null=>&

!!expand:,:burden_testl,burdenl,whichvcf:burden_test,burden,project_clean_all_vcf_file:burden_test_variant_subset,burden_variant_subset,pheno_variant_subset_clean_all_vcf_file! \
burden_testl_epacts_group_helper=!{input,burden_testl_epacts_ready_file} rm -f !{raw,,burden_testl,*@{1}_epacts_trunk.epacts} && rm -f !{output,,@{1}_file} && !{raw,,burden_testl,rm -f *@{1}_epacts_trunk.reml && ln -s *burden_test_in_aux1_file *@{1}_epacts_trunk.reml && rm -f *@{1}_epacts_trunk.eigR && ln -s *burden_test_in_aux2_file *@{1}_epacts_trunk.eigR &&,if_prop=test_name:eq:emmaxCMC,if_prop=test_name:eq:emmaxVT,if_prop=test_name:eq:mmskat,or_if_prop=1,allow_empty=1} !{input;burden_test_in_aux1_file;if_prop=test_name:eq:emmaxCMC;if_prop=test_name:eq:emmaxVT;if_prop=test_name:eq:mmskat;or_if_prop=1;allow_empty=1} !{input;burden_test_in_aux2_file;if_prop=test_name:eq:emmaxCMC;if_prop=test_name:eq:emmaxVT;if_prop=test_name:eq:mmskat;or_if_prop=1;allow_empty=1} $epacts_cmd group -restart --missing $plink_missing !{prop,-unit,burden_test,epacts_unit,missing_key=epacts_group_unit} !{input,--vcf,whichvcf} !{input,--ped,pheno_epacts_@{3}_ped_file,unless_prop=burden_test_sample_include_file,unless_prop=extra_covar_traits,unless_prop=test_name:eq:metaSKAT,allow_empty=1} !{input,--ped,burden_test_sample_include_aux_file,if_prop=burden_test_sample_include_file,unless_prop=extra_covar_traits,unless_prop=alternate_pheno,unless_prop=test_name:eq:metaSKAT,allow_empty=1} !{input,--ped,burden_test_covars_aux_file,if_prop=extra_covar_traits,unless_prop=test_name:eq:metaSKAT,allow_empty=1} !{input,--ped,burden_test_covars_aux_file,unless_prop=test_name:eq:metaSKAT,if_prop=alternate_pheno,allow_empty=1} !{input,--ped,burden_test_in_aux1_file,if_prop=test_name:eq:metaSKAT,allow_empty=1} !{input,--kinf,pheno_kinship_file,if_prop=test_name:eq:emmaxCMC,if_prop=test_name:eq:emmaxVT,if_prop=test_name:eq:mmskat,or_if_prop=1,allow_empty=1} !{raw,--remlf,burden_test,*@{1}_epacts_trunk.reml,if_prop=test_name:eq:emmaxCMC,if_prop=test_name:eq:emmaxVT,if_prop=test_name:eq:mmskat,or_if_prop=1,allow_empty=1} --min-maf 0 --min-callrate 0 --max-maf 1 --pheno @5 --groupf @6 --test @{2} !{prop;;burden_test;test_options;if_prop=test_options;allow_empty=1} !{raw;--strat;burden_test;$meta_skat_strat_col;if_prop=test_name:eq:metaSKAT;allow_empty=1} -run 1 !{prop,-field,burden_test,epacts_gt_field} @4 !{raw,--out,burden_testl,*@{1}_epacts_trunk} ${group_dev_null}/dev/null && awk -F"\t" '\$1 != ""' !{raw,,burden_testl,*@{1}_epacts_trunk.epacts} > !{output,,@{1}_file}

burden_test_epacts_eigen_helper=rm -f !{raw,,burden_test,*burden_test_case_side_gassoc_epacts_trunk.eigR} && rm -f !{raw,,burden_test,*burden_test_case_side_gassoc_epacts_trunk.reml} && !{input,burden_test_epacts_ready_file} $epacts_cmd single -restart --missing $plink_missing !{input,--vcf,pheno_sample_marker_pruned_vcf_file} !{input,--ped,pheno_epacts_@{1}_ped_file,unless_prop=burden_test_sample_include_file,unless_prop=extra_covar_traits,allow_empty=1} !{input,--ped,burden_test_sample_include_aux_file,if_prop=burden_test_sample_include_file,unless_prop=extra_covar_traits,unless_prop=alternate_pheno,allow_empty=1} !{input,--ped,burden_test_covars_aux_file,if_prop=extra_covar_traits,if_prop=alternate_pheno,or_if_prop=1,allow_empty=1} !{input,--kinf,pheno_kinship_file,if_prop=test_name:eq:emmaxCMC,if_prop=test_name:eq:emmaxVT,if_prop=test_name:eq:mmskat,or_if_prop=1,allow_empty=1} !{prop,-unit,burden_test,epacts_unit,missing_key=epacts_single_unit} --min-maf 0 --min-callrate 0 --min-mac 0 !{prop:--pheno:burden_test:pheno:unless_prop=alternate_pheno:allow_empty=1}!{prop:--pheno:burden_test:alternate_pheno:if_prop=alternate_pheno:allow_empty=1} --test q.emmax !{prop;;burden_test;test_options;if_prop=test_options;allow_empty=1} -run 1 !{prop,-field,burden_test,epacts_gt_field} @2 --chr -1 !{raw,--out,burden_test,*burden_test_case_side_gassoc_epacts_trunk} && mv !{raw,,burden_test,*burden_test_case_side_gassoc_epacts_trunk.reml} !{output,,burden_test_in_aux1_file} && mv !{raw,,burden_test,*burden_test_case_side_gassoc_epacts_trunk.eigR} !{output,,burden_test_in_aux2_file}

!|expand:;:alltype;runif:all;include_related:unrelated;!include_related| \
cmd make_burden_test_alltype_epacts_strata_in_aux_files=c=$get_epacts_covars(burden_test) && $burden_test_epacts_eigen_helper(covar_alltype,\$c) class_level burden_test run_if and,!run_raw,(or,strat_covar,covar_traits),test_software:eq:epacts,runif skip_if and,test_name:ne:emmaxCMC,test_name:ne:emmaxVT,test_name:ne:mmskat rusage_mod $epacts_eigen_mem run_with burden_test_variant_subset

meta_skat_covars_helper=!{raw;;burden_test;&& c\=`head -n1 *burden_test_in_aux1_file | cut -f8- | $smart_cut_cmd --exclude-col 0,1,$meta_skat_strat_col | sed 's/\(\S\S*\)/@1\1/g'`;if_prop=test_name:eq:metaSKAT;allow_empty=1} !{input,burden_test_in_aux1_file,if_prop=test_name:eq:metaSKAT,allow_empty=1}

meta_skat_covars=$meta_skat_covars_helper(--cov )

!|expand:;:alltype;relskipif:all;!include_related:unrelated;include_related| \
!|expand:;:shortt;phenol;burdenl;burden_testl;exskipif:;pheno;burden;burden_test;,burden_test_variant_subset:short;pheno_variant_subset;burden_variant_subset;burden_test_variant_subset;| \
shortt cmd make_burden_testl_case_side_alltype_epacts_strata_gassoc_file=c=$get_epacts_covars(burden_test) $meta_skat_covars && $burden_testl_epacts_group_helper(burden_testl_case_side_gassoc,!{prop::burden_test:test_name},covar_alltype,\$c,!{prop::burden_test:pheno:unless_prop=alternate_pheno:allow_empty=1}!{prop::burden_test:alternate_pheno:if_prop=alternate_pheno:allow_empty=1},!{input::burdenl_epacts_group_file}) class_level burden_testl run_if or,strat_covar,covar_traits skip_if or,run_raw,test_software:ne:epactsexskipif,relskipif run_with burden_test_variant_subset rusage_mod $epacts_gassoc_mem

!|expand:;:alltype;relskipif:all;!include_related:unrelated;include_related| \
local cmd make_burden_test_alltype_epacts_strata_info_file=c="$get_disp_covars_burden_test" $meta_skat_covars_helper(\,); c=`echo \$c | sed 's/^,//'`; $record_burden_test_info(\$c,no,!{raw::burden_test:@epacts_gt_field},no,no,!{prop::burden_test:max_assoc_null:missing=none},!{prop::burden_test:min_test_p_missing:missing=none},!{prop::burden_test:min_test_p_hwe:missing=none},!{input;;pheno_sample_covar_alltype_include_file;unless_prop=burden_test_sample_include_file;unless_prop=extra_covar_traits;unless_prop=alternate_pheno;allow_empty=1} !{raw;;burden_test;*burden_test_sample_include_aux_file | tail -n+2;if_prop=burden_test_sample_include_file;unless_prop=extra_covar_traits;unless_prop=alternate_pheno;allow_empty=1} !{input;burden_test_sample_include_aux_file;if_prop=burden_test_sample_include_file;unless_prop=extra_covar_traits;unless_prop=alternate_pheno;allow_empty=1} !{raw;;burden_test;*burden_test_covars_aux_file | tail -n+2;if_prop=extra_covar_traits;if_prop=alternate_pheno;or_if_prop=1;allow_empty=1} !{input;burden_test_covars_aux_file;if_prop=extra_covar_traits;if_prop=alternate_pheno;or_if_prop=1;allow_empty=1} ) class_level burden_test run_if or,strat_covar,covar_traits skip_if or,run_raw,test_software:ne:epacts,relskipif

!|expand:;:alltype;runif:all;include_related:unrelated;!include_related| \
cmd make_burden_test_alltype_epacts_raw_in_aux_files=$burden_test_epacts_eigen_helper(popgen_alltype,) class_level burden_test run_if and,run_raw,test_software:eq:epacts,runif skip_if and,test_name:ne:emmaxCMC,test_name:ne:emmaxVT,test_name:ne:mmskat rusage_mod $epacts_eigen_mem run_with burden_test_variant_subset

!|expand:;:alltype;relskipif:all;!include_related:unrelated;include_related| \
!|expand:;:shortt;phenol;burdenl;burden_testl;exskipif:;pheno;burden;burden_test;,burden_test_variant_subset:short;pheno_variant_subset;burden_variant_subset;burden_test_variant_subset;| \
shortt cmd make_burden_testl_case_side_alltype_epacts_raw_gassoc_file=$burden_testl_epacts_group_helper(burden_testl_case_side_gassoc,!{prop::burden_test:test_name},popgen_alltype,,!{prop::burden_test:pheno:unless_prop=alternate_pheno:allow_empty=1}!{prop::burden_test:alternate_pheno:if_prop=alternate_pheno:allow_empty=1},!{input::burdenl_epacts_group_file}) class_level burden_testl run_if run_raw skip_if or,test_software:ne:epactsexskipif,relskipif run_with burden_test_variant_subset rusage_mod $epacts_gassoc_mem

!|expand:;:alltype;relskipif:all;!include_related:unrelated;include_related| \
local cmd make_burden_test_alltype_epacts_raw_info_file=$record_burden_test_info(no,no,!{raw::burden_test:@epacts_gt_field},no,no,!{prop::burden_test:max_assoc_null:missing=none},!{prop::burden_test:min_test_p_missing:missing=none},!{prop::burden_test:min_test_p_hwe:missing=none},!{input;;pheno_sample_popgen_alltype_include_file;unless_prop=burden_test_sample_include_file;unless_prop=alternate_pheno;allow_empty=1} !{raw;;burden_test;*burden_test_sample_include_aux_file | tail -n+2;if_prop=burden_test_sample_include_file;if_prop=alternate_pheno;or_if_prop=1;allow_empty=1} !{input;burden_test_sample_include_aux_file;if_prop=burden_test_sample_include_file;if_prop=alternate_pheno;or_if_prop=1;allow_empty=1}) class_level burden_test run_if run_raw skip_if or,test_software:ne:epacts,relskipif

meta_skat_aux1_helper=$smart_cut_cmd --in-delim $tab !{input,--file,pheno_epacts_@{1}_ped_file,unless_prop=burden_test_sample_include_file,unless_prop=extra_covar_traits,allow_empty=1} !{input,--file,burden_test_sample_include_aux_file,if_prop=burden_test_sample_include_file,unless_prop=extra_covar_traits,unless_prop=alternate_pheno,allow_empty=1} !{input,--file,burden_test_covars_aux_file,if_prop=extra_covar_traits,if_prop=alternate_pheno,or_if_prop=1,allow_empty=1} --select-col 1,1-7 --require-col-match --exact

meta_skat_strat_col=STRAT

meta_skat_append_strat=sed '1 s/$/\t$meta_skat_strat_col/' | sed '1! s/$/\t!{prop,,pheno}/'

!|expand:;:alltype;runif:all;include_related:unrelated;!include_related| \
short cmd make_burden_test_alltype_epacts_meta_skat_strata_in_aux1_files=$meta_skat_aux1_helper(covar_alltype) --select-col 1,1,$get_spaced_covars(burden_test) | $meta_skat_append_strat > !{output,,burden_test_in_aux1_file} class_level burden_test run_if and,!run_raw,(or,strat_covar,covar_traits),test_software:eq:epacts,runif skip_if or,test_name:ne:metaSKAT,meta_trait_inv run_with burden_test_variant_subset

!|expand:;:alltype;runif:all;include_related:unrelated;!include_related| \
short cmd make_burden_test_alltype_epacts_meta_skat_raw_in_aux1_files=$meta_skat_aux1_helper(popgen_alltype) | $meta_skat_append_strat > !{output,,burden_test_in_aux1_file} class_level burden_test run_if and,run_raw,test_software:eq:epacts,runif skip_if or,test_name:ne:metaSKAT,meta_trait_inv run_with burden_test_variant_subset

get_meta_skat_meta_covars=$smart_cut_cmd --in-delim $tab !{input,--file,burden_test_in_aux1_file,all_instances=1,if_prop=project:eq:@project,if_prop=pheno:eq:@meta_trait_inv,if_prop=burden:eq:@burden,if_prop=burden_test:eq:@meta_test} --exclude-col .,1-7 --exclude-col .,1,$meta_skat_strat_col --select-row .,1 | perl -lane 'foreach (@F) {print unless \$m{\$_}; \$m{\$_}=1}' | tr '\n' ' '

short cmd make_burden_test_epacts_meta_skat_meta_in_aux1_files=c=`$get_meta_skat_meta_covars` && $smart_cut_cmd --in-delim $tab !{input,--file,burden_test_in_aux1_file,all_instances=1,if_prop=project:eq:@project,if_prop=pheno:eq:@meta_trait_inv,if_prop=burden:eq:@burden,if_prop=burden_test:eq:@meta_test} --select-col .,1-7 --select-col .,1,$meta_skat_strat_col --select-col .,1,"\$c" --fill-value NA --exclude-row 2-.,1 | awk -v OFS="\t" -F"\t" 'NR == 1 {\$7="!{prop,,pheno}"} {print}' > !{output,,burden_test_in_aux1_file} class_level burden_test run_if and,!run_raw,(or,strat_covar,covar_traits),test_software:eq:epacts skip_if or,test_name:ne:metaSKAT,!meta_trait_inv run_with burden_test_variant_subset


#!|expand:,:shortt,phenol,burdenl,exskipif:,pheno,burden,num_var_subsets:short,pheno_variant_subset,burden_variant_subset,!num_var_subsets| \
#!|expand:;:keytype;cmdtype;phefiletype;extrarunif:raw_;;phe;run_if run_raw:strata_;_covar;phe_covar;run_if or,strat_covar,covar_traits| \
#shortt cmd make_burdenl_keytypecase_side_score_gassoc_file=$score_seq_cmd !{input,-pfile,pheno_score_seq_phefiletype_file} !{input,-mfile,burdenl_score_seq_map_file} !{output,-ofile,burdenl_keytypecase_side_score_gassoc_file} !{input,-gfile,phenol_keytypevmatrix_file} -vtlog /dev/null -msglog /dev/null -MAF 1 -MAC 0 !{input,pheno_is_trait_file} class_level burdenl extrarunif skip_if or,no_burden_test,exskipif


!|expand:;:alltype;allskipif:all;!include_related:unrelated;include_related| \
!|expand:;:shortt;phenol;burdenl;burden_testl;assocl;exskipif:;pheno;burden;burden_test;assoc;burden_test_variant_subset:short;pheno_variant_subset;burden_variant_subset;burden_test_variant_subset;assoc_variant_subset;!burden_test_variant_subset| \
!|expand:;:atype;cmdtype;extracmd;extrarunif:;;;run_if run_raw:;_cluster;$add_strata(burden_testl);run_if and,!run_raw,!analytic,(or,strat_cluster,strata_traits):analytic_;_covar;$add_covar(burden_test);run_if and,!run_raw,analytic,(or,strat_covar,covar_traits)| \
shortt cmd make_atypeburden_testl_control_sidecmdtype_alltype_gassoc_file=$pseq_qc_plus_alltype_assoc_cmd_phenolcmdtype($assocl) extracmd $qt_plinkseq_phenotype_selector_raw(--make-phenotype)=$control_pheno_helper:$case_pheno_helper $burdenl_gassoc_helper($nperm_gassoc,) | $assoc_fix_bug | $smart_cut_cmd --in-delim $tab --select-col 0,1 --select-col 0,1,'TEST P I DESC' --require-col-match --exact | sed '1 s/\(\s*\)\(P\|I\|DESC\)\(\s*\)/\1\22\3/g' $fix_two_hit_helper > !{output,,burden_testl_control_side_gassoc_file} !{input,pheno_is_trait_file} class_level burden_testl extrarunif skip_if or,test_software:ne:pseq,!make_two_sided,exskipif,allskipif rusage_mod $pseq_gene_mem run_with burden_test_variant_subset

!|expand:;:alltype;allskipif:all;!include_related:unrelated;include_related| \
!|expand@;@atype;cmdtype;covars;clustering;whichinclude;extrarunif@\
;;no;no;popgen;run_if and,run_raw,!analytic@\
;_cluster;no;$get_disp_cluster_burden_test;cluster;run_if and,!run_raw,!analytic,(or,strat_cluster,strata_traits)@\
analytic_;;no;no;popgen;run_if and,run_raw,analytic@\
analytic_;_covar;$get_disp_covars_burden_test;no;covar;run_if and,!run_raw,analytic,(or,strat_covar,covar_traits)| \
local cmd make_atypeburden_test_pseqcmdtype_alltype_info_file=$record_burden_test_info(covars,clustering,$gq_crit_helper,!{raw::burden_test:no:if_prop=analytic:allow_empty=1}!{raw::burden_test:adaptive:unless_prop=analytic:allow_empty=1}!{raw::burden_test:\\, fix null:if_prop=fix_null:allow_empty=1},!{raw::burden_test:yes:if_prop=make_two_sided:allow_empty=1}!{raw::burden_test:no:unless_prop=make_two_sided:allow_empty=1},!{prop::burden_test:max_assoc_null:missing=none},!{prop::burden_test:min_test_p_missing:missing=none},!{prop::burden_test:min_test_p_hwe:missing=none},!{input;;pheno_sample_whichinclude_alltype_include_file} !{input;;burden_test_sample_include_aux_file;if_prop=burden_test_sample_include_file;if_prop=alternate_pheno;or_if_prop=1;allow_empty=1} !{input;;burden_test_sample_include_aux_file;if_prop=burden_test_sample_include_file;if_prop=alternate_pheno;or_if_prop=1;allow_empty=1} | sort | uniq -u) class_level burden_test extrarunif skip_if or,test_software:ne:pseq,allskipif

local cmd make_burden_test_metal_info_file=$record_burden_test_info_int(!{prop;;burden_test;test_tag;missing_prop=test_name;sep=\,;all_instances=1;all_instances=1;if_prop=project:eq:@project;if_prop=pheno:eq:@meta_trait_inv;if_prop=burden_test:eq:@meta_test;limit=1}: !{prop;;burden_test;meta_trait_inv;sep=\,},NA,NA,NA,NA,NA,NA,NA,NA,NA) class_level burden_test run_if test_software:eq:metal

local cmd make_burden_test_metal_in_aux1_file=(echo SCHEME !{prop,,burden_test,metal_scheme} && echo MARKER $id_col_disp && echo SEPARATOR TAB && echo PVALUE $p_col_disp && echo WEIGHT $n_col_disp && echo ALLELE ALT REF && echo STDERR $se_col_disp && echo EFFECT $beta_col_disp && echo CUSTOMVARIABLE TotalSampleSize && echo LABEL TotalSampleSize as $n_col_disp && !{raw,,burden_test,echo PROCESS *burden_test_small_gassoc_file,sep=&&,all_instances=1,if_prop=project:eq:@project,if_prop=pheno:eq:@meta_trait_inv,if_prop=burden:eq:@burden,if_prop=burden_test:eq:@meta_test} !{input,burden_test_small_gassoc_file,all_instances=1,if_prop=project:eq:@project,if_prop=pheno:eq:@meta_trait_inv,if_prop=burden:eq:@burden,if_prop=burden_test:eq:@meta_test} && echo OUTFILE !{raw,,burden_test,*burden_test_case_side_gassoc_file} .tbl && echo ANALYZE) > !{output,,burden_test_in_aux1_file} class_level burden_test run_if and,test_software:eq:metal,meta_trait_inv 

short cmd make_burden_test_metal_case_side_gassoc_file=cat !{input,,burden_test_in_aux1_file} !{input,burden_test_small_gassoc_file,all_instances=1,if_prop=project:eq:@project,if_prop=pheno:eq:@meta_trait_inv,if_prop=burden_test:eq:@meta_test} | $metal_cmd !{output,burden_test_case_side_gassoc_file} > !{output,,burden_test_out_aux1_file} && mv !{output,,burden_test_case_side_gassoc_file}1.tbl !{output,,burden_test_case_side_gassoc_file} class_level burden_test run_if and,test_software:eq:metal,meta_trait_inv

format_dist_test=calpha
format_count_test=burden

#!|expand:;:shortt;phenol;burdenl;burden_testl;exskipif:;pheno;burden;burden_test;burden_variant_subset:short;pheno_variant_subset;burden_variant_subset;burden_test_variant_subset;!burden_variant_subset| \
#shortt cmd make_burdenl_gassoc_counts_data_file=$pseq_qc_plus_unrelated_assoc_cmd_phenol($assoc_raw --tests $format_dist_test $format_count_test) $bad_for_genes_regex_mask $qt_plinkseq_phenotype_selector $burdenl_gassoc_helper(0,) | $assoc_fix_bug > !{output,,burdenl_gassoc_counts_data_file} !{input,pheno_is_trait_file} class_level burdenl skip_if or,exskipif run_if burden_test rusage_mod $pseq_gene_mem run_with burden_variant_subset

!|expand:;:shortt;phenol;burdenl;burden_testl;assocl;exskipif:;pheno;burden;burden_test;assoc;burden_variant_subset:short;pheno_variant_subset;burden_variant_subset;burden_test_variant_subset;assoc_variant_subset;!burden_variant_subset| \
shortt cmd make_burdenl_gassoc_counts_data_file=cat !{input,,phenol_counts_file} | $add_gene_annot_cmd --locus-col 1 --header 1 !{input,--gene-file,burdenl_locdb_reg_file} --gene-file-id-col 4 --gene-file-chr-col 1 --gene-file-start-col 2 --gene-file-end-col 3 --print-multiple --gene-file-header 1 --in-delim $tab | $smart_cut_cmd --in-delim $tab --select-col 0,1,'GENE CNTA CNTU' --exclude-row 0,1 | sort -k1,1 | awk -F"\t" -v OFS="\t" 'function print_dist(g,n,mina,minu,m) {d=""; for (v in m) {if (d) {d = d";"} d = d""v"("m[v]")"} print g,n,mina+minu,mina,minu,d} BEGIN {print "LOCUS","NVAR","MAC","MINA","MINU","DIST"; n=0; split("", m, ""); mina=0; minu=0; g=""} g && \$1 != g {print_dist(g,n,mina,minu,m); n=0; split("", m, ""); mina=0; minu=0; g=""} {g = \$1; n++; k = \$2"/"\$3; m[k]++; mina += \$2; minu += \$3} END {if (g != "") {print_dist(g,n,mina,minu,m)}}' > !{output,,burdenl_gassoc_counts_data_file} !{input,pheno_is_trait_file} class_level burdenl skip_if or,exskipif run_if burden_test rusage_mod $pseq_gene_mem run_with burden_variant_subset

gassoc_mina_minu=MINA/MINU
gassoc_count=MAC
gassoc_dist=DIST

small_gassoc_helper1=$smart_join_cmd --in-delim $tab --header 1 --exec "$smart_cut_cmd --tab-delim !{input,--file,project_transcript_gene_alias_file} --exec 'cut -f2 !{input,,project_variant_gene_annotation_file} | sort -u' --exclude-row 1,1 --select-col 1,1,'transcript gene' | awk -v OFS=\"\t\" -F\"\t\" 'NF == 2 {print} NF == 1 {print \\$1,\\$1}' | sed '1 s/^/TRANSCRIPT\tGENE\n/'" --extra 1

small_gassoc_helper2=awk -v OFS="\t" -F "\t" '{s=\$1;\$1=\$2;\$2=s} \$1 == \$2 {\$2="$consensus_transcript"} {print}'

#short cmd make_burden_gassoc_counts_file=$small_gassoc_helper1 --exec "$smart_cut_cmd --in-delim $tab !{input:--file:burden_gassoc_counts_data_file} --select-col 1,1,'LOCUS POS NVAR DESC' --require-col-match --select-row 1,1 --exact  --select-row 1,1,TEST,!{raw::burden:$format_count_test:uc=1} --exact | perl -lne 'chomp; @a = split(\"\t\"); \\$er = \"$gassoc_count\"; if (\\$. > 1) {@cc = split(\"/\", \\$a[\\$\#a]); \\$er = \\$cc[0]+\\$cc[1];} print join(\"\t\", @a[0..2]) . \"\t\\$er\t\\$a[3]\"' | sed '1 s;\S\S*$;$gassoc_mina_minu;'" --exec "$smart_cut_cmd --in-delim $tab !{input:--file:burden_gassoc_counts_data_file} --select-col 1,1,'LOCUS DESC' --require-col-match --select-row 1,1 --exact --select-row 1,1,TEST,!{raw::burden:$format_dist_test:uc=1} --exact | sed '1 s;\S\S*$;$gassoc_dist;'" | $small_gassoc_helper2 > !{output,,burden_gassoc_counts_file} class_level burden run_if burden_test

short cmd make_burden_gassoc_counts_file=$small_gassoc_helper1 --exec "$smart_cut_cmd --in-delim $tab !{input:--file:burden_gassoc_counts_data_file} --select-col 1,1,'LOCUS NVAR MAC MINA MINU DIST' --require-col-match --exact --exclude-row 1,1,LOCUS,$outside_gene_name | awk -F\"\t\" -v OFS=\"\t\" '{print \\$1,\\$2,\\$3,\\$4\"/\"\\$5,\\$6}' | tail -n+2 | sed '1 s;.*;LOCUS\tNVAR\t$gassoc_count\t$gassoc_mina_minu\t$gassoc_dist;'" | $small_gassoc_helper2 > !{output,,burden_gassoc_counts_file} class_level burden run_if burden_test

!|expand:;:shortt;phenol;burdenl;burden_testl;exskipif:short;pheno;burden;burden_test;burden_test_variant_subset:local;pheno_variant_subset;burden_variant_subset;burden_test_variant_subset;!burden_test_variant_subset| \
shortt cmd make_burden_testl_gassoc_file=rm -f !{output,,burden_testl_gassoc_file} && $smart_join_cmd --in-delim $tab --header 1 --ignore-err "$burden_include_warning" --exec "$smart_cut_cmd --tab-delim !{input,--file,burden_testl_case_side_gassoc_file} !{input,--file,burden_testl_control_side_gassoc_file} --exclude-row 1-2,1 --select-col 1-2,1,'LOCUS TEST' | sort | uniq -d | $add_header_cmd LOCUS${tab}TEST" --exec "$smart_cut_cmd --tab-delim !{input,--file,burden_testl_case_side_gassoc_file} --select-col 1,1,'^LOCUS$ ^TEST$ .' --require-col-match" --exec "$smart_cut_cmd --tab-delim !{input,--file,burden_testl_control_side_gassoc_file} --select-col 1,1,'^LOCUS$ ^TEST$ .'" --rest-extra 1 --col 1 --col 2 | awk -v OFS="\t" -F "\t" 'NR == 1 {for (i=1;i<=NF;i++) {map[\$i]=i}} NR != 1 {if (\$map["P2"] < \$map["P"]) {\$map["P"] = \$map["P2"]; \$map["I"] = \$map["I2"]; \$map["DESC"] = \$map["DESC2"]} \$map["P"] = 2 * \$map["P"] - \$map["P"] ** 2} {print}' | $smart_cut_cmd --in-delim $tab --exclude-col 0,1,'P2 I2 DESC2' --exact --require-col-match > !{output,,burden_testl_gassoc_file} !{input,pheno_is_trait_file} class_level burden_testl skip_if or,test_software:ne:pseq,!make_two_sided,exskipif run_with burden_test_variant_subset

!|expand:;:shortt;phenol;burdenl;burden_testl;exskipif:short;pheno;burden;burden_test;burden_test_variant_subset:local;pheno_variant_subset;burden_variant_subset;burden_test_variant_subset;!burden_test_variant_subset| \
local cmd ln_burden_testl_gassoc_file=ln -s !{input,,burden_testl_case_side_gassoc_file} !{output,,burden_testl_gassoc_file} !{input,pheno_is_trait_file} class_level burden_testl run_if or,test_software:ne:pseq,!make_two_sided skip_if exskipif run_with burden_test_variant_subset

consensus_transcript=NA

desc_header=DESC

short cmd make_burden_test_pseq_small_gassoc_file=$small_gassoc_helper1 --exec "$smart_cut_cmd --in-delim $tab !{input:--file:burden_test_gassoc_file} --select-col 1,1,'LOCUS TEST P DESC' --require-col-match --exact --exclude-row 1,1,LOCUS,$outside_gene_name" --multiple 2 | $small_gassoc_helper2 | $swap_name | awk -F"\t" -v OFS="\t" 'NR = 1 {print \$0,NS} NR > 1 {print \$0,"NA"}' | $small_gassoc_helper3 > !{output,,burden_test_small_gassoc_file} class_level burden_test run_if test_software:eq:pseq

swap_name=eval `(echo !{prop,,burden_test,test_name,uc=1} && echo !{prop,,burden_test,test_tag,missing_prop=test_name,uc=1}) | $transpose_cmd --out-delim $tab | sed "s;\(\S\S*\)\t\(\S\S*\).*;sed '1! s/\1/\2/';" | tr '\n' '|' | sed 's/|$//'`

small_gassoc_helper3=sed '1 s/\tNS/\t$n_col_disp/' | sed '1 s/\tPVALUE/\t$p_col_disp/' | sed '1 s/\tSEBETA/\t$se_col_disp/' | sed '1 s/\tBETA/\t$beta_col_disp/' | awk -F"\t" -v OFS="\t" 'NR == 1 {print \$0,"ID","$ref_col_disp","$alt_col_disp"} NR > 1 {print \$0,\$1" "\$2,"R","A"}'

short cmd make_burden_test_epacts_small_gassoc_file=$small_gassoc_helper1 --exec "$fill_file_helper_int(--exec 'cut -d_ -f2- !{input::burden_test_gassoc_file}',cut -d_ -f2- !{input::burden_test_gassoc_file},!{input::burden_epacts_group_file},NA,1,1,1,1,\t) | $smart_cut_cmd --in-delim $tab --select-col 0\,1\,'ID PVALUE NS ^(QSTAT|SCORE_TEST|STATRHO|BETA|SEBETA|OPT_FRAC_WITH_RARE|CONVERGED|muQ|RHO_EST)$' --require-col-match --exclude-row 0,1,ID,$outside_gene_name | awk -F\"\t\" -v OFS=\"\t\" 'NR == 1 {\\$1=\\$1\"\tTEST\"} NR > 1 && \\$1 != \"\" {\\$1=\\$1\"\t!{prop::burden_test:test_tag:missing_prop=test_name:uc=1}\"} \\$1 != \"\" {print}' | sed '1 s/PVALUE/$p_col_disp/' | awk -F\"\t\" -v OFS=\"\t\" 'NR == 1 {t=\\$NF; d=(t != \"BETA\" && t != \"SEBETA\")} NR == 1 && d {\\$NF=\"$desc_header\"} NR > 1 && d {\\$NF=t\"=\"\\$NF} {print}'" | $small_gassoc_helper2 | $small_gassoc_helper3 > !{output,,burden_test_small_gassoc_file} class_level burden_test run_if test_software:eq:epacts

short cmd make_burden_test_metal_small_gassoc_file=$smart_cut_cmd !{input,--file,burden_test_gassoc_file} --in-delim $tab --select-col 1,1,'MarkerName Allele1 Allele2 TotalSampleSize TotalEffectiveSampleSize Effect Zscore StdErr P-value Direction' --exact | awk -F"\t" -v OFS="\t" '{print \$1,\$0}' | sed '1 s/^MarkerName/GENE\tTRANSCRIPT/' | sed '1! s/ /\t/' | $replace_nan(NA) | awk -F"\t" -v OFS="\t" 'NR == 1 {\$1=\$1"\tTEST"} NR > 1 && \$1 != "" {\$1=\$1"\t!{prop::burden_test:test_tag:missing_prop=test_name:uc=1}"} \$1 != "" {print}' | sed '1 s/\tP-value/\t$p_col_disp/' | sed '1 s/\tTotalSampleSize/\t$n_col_disp/' | sed '1 s/\tAllele2/\t$ref_col_disp/' | sed '1 s/\tAllele1/\t$alt_col_disp/' | sed '1 s/\tStdErr/\t$se_col_disp/' | sed '1 s/\t!{raw,,burden_test,Effect,if_prop=metal_scheme:ne:SAMPLESIZE,allow_empty=1}!{raw,,burden_test,Zscore,if_prop=metal_scheme:eq:SAMPLESIZE,allow_empty=1}/\t$beta_col_disp/' | sed '1 s/MarkerName/$id_col_disp/' | sed '1 s/Direction/$dir_col_disp/' | $smart_cut_cmd --in-delim $tab --select-col 0,1,'GENE TRANSCRIPT TEST $n_col_disp $beta_col_disp !{raw,,burden_test,$se_col_disp,if_prop=metal_scheme:eq:STDERR,allow_empty=1} $p_col_disp $dir_col_disp $id_col_disp $ref_col_disp $alt_col_disp' --require-col-match --exact > !{output,,burden_test_small_gassoc_file} class_level burden_test run_if and,test_software:eq:metal,meta_trait_inv

short cmd make_burden_gassoc_file=cols=`perl -le 'foreach ("$id_col_disp","$alt_col_disp","$ref_col_disp") {\$m{"\$_"}=1} foreach (qw(!{input,,burden_test_small_gassoc_file})) {open IN, \$_ or die "Cannot open \$_"; \$header = <IN>; chomp(\$header); close IN; foreach \$v (split("\t", \$header)) {print \$v unless \$m{\$v}; \$m{\$v}=1}}'` && $smart_join_cmd --in-delim $tab --exec "head -n1 !{input,,burden_gassoc_counts_file} | cut -f1-2 && tail -qn+2 !{input,,burden_test_small_gassoc_file} | cut -f1-2 | sort -u | cat - !{input,,burden_gassoc_counts_file} | cut -f1-2 | sort | uniq -d | sed '1 s/LOCUS/TRANSCRIPT/'" !{input,--file,burden_gassoc_counts_file} --exec "$smart_cut_cmd !{raw;;burden_test;--exec \"$smart_cut_cmd --file *burden_test_small_gassoc_file --in-delim $tab --select-col 1-.,1,'\$cols' --fill-value NA\"} !{input,burden_test_small_gassoc_file} --in-delim $tab --exclude-row 2-.,1 --select-col 1-.,1,'\$cols' --require-col-match" --rest-extra 1 --header 1 --multiple 3 --col 1 --col 2 | $prepend_var_group > !{output,,burden_gassoc_file} class_level burden run_if burden_test

#!!expand:pathwayburdentype:custom! \
#local cmd make_burden_pathway_pathwayburdentype_gassoc_file=(head -qn+1 !{input,,burden_test_pathway_pathwayburdentype_gassoc_file,if_prop=test_software:eq:pseq,if_prop=test_software:eq:epacts,or_if_prop=1,limit=1} | head -n1 && tail -qn+2 !{input,,burden_test_pathway_pathwayburdentype_gassoc_file,if_prop=test_software:eq:pseq,if_prop=test_software:eq:epacts,or_if_prop=1}) | $prepend_var_group > !{output,,burden_pathway_pathwayburdentype_gassoc_file} class_level burden run_if burden_test skip_if !project_pathwayburdentype_locset_file

get_burden_flat_join_execs=`for f in !{prop,,burden_test,test_tag,missing_prop=test_name,uc=1}; do echo "--exec \"$smart_cut_cmd !{input,--file,@1} --tab-delim --select-row 1,1 --select-row 1,1,TEST,eq:\$f --select-col 1,'2 3' --select-col 1,1,'$beta_col_disp $desc_header P' --exact | sed '1 s/\S\S*\(\s\s*\)\S\S*\(\s\s*\)\S\S*$/\${f}_$beta_col_disp\t\${f}_DESC\1\$f/'\""; done | tr '\n' ' '`

summarize_gassoc_pvals=perl -pe 'chomp; if (++\$num == 1) {\$_ .= "\tP_MIN\tP_MEAN\tP_MEDIAN\n"; @pcols = (); @cols = split; for (my \$i = 0; \$i <= \$\#cols; \$i++) {foreach \$test (qw(!{prop,,burden_test,test_tag,if_prop=use_for_interesting,missing_prop=test_name,uc=1})) {push @pcols, \$i if \$cols[\$i] eq \$test}}} else {@a = split; \$min = 1; \$mean = 0; \$n = 0; @p_values = (); foreach \$col (@pcols) {my \$pval = \$a[\$col]; next if \$pval !~ /^[0-9\.e\-]+$/; \$min = \$pval if \$pval < \$min; \$n++; \$mean += \$pval; push @p_values, \$pval } if (\$n == 0) {\$mean = \$min = \$median = "NA"} else {\$mean /= \$n; \$median = "NA"; @p_values = sort {\$a <=> \$b} @p_values; \$ind = int((scalar @p_values) / 2); if ((scalar @p_values) % 2 == 0) {\$median = (\$p_values[\$ind] + \$p_values[\$ind - 1]) / 2} else {\$median = \$p_values[\$ind]}} \$_ .= "\t\$min\t\$mean\t\$median\n"}'

local cmd make_burden_flat_gassoc_file=f=$get_burden_flat_join_execs(burden_gassoc_file) && eval "$smart_join_cmd --in-delim $tab --header 1 --col 1 --col 2 !{input,--file,burden_gassoc_counts_file} \$f --fill-all" | $summarize_gassoc_pvals | $prepend_var_group > !{output,,burden_flat_gassoc_file} class_level burden run_if burden_test

gassoc_var_group_col=VAR_GROUP
prepend_var_group=sed '1 s/^/$gassoc_var_group_col\t/' | sed '1! s/^/!{prop,,burden,disp}\t/'

burden_interesting_variant_helper=$get_burden_all_vassoc_list(@6) | $smart_cut_cmd --in-delim $tab --require-col-match --exact --select-row 0,1 --select-row 0,1,MAF,le:!{prop,,burden,max_interesting_variant_maf} --select-row 0,1,MAC,ge:!{prop::burden:@5} !{raw,,burden,--select-row 0\,1\,P_MISSING\,ge:,unless_prop=pheno_qt,allow_empty=1}!{prop,,burden,@1,unless_prop=pheno_qt,allow_empty=1} --and-row-all | $smart_cut_cmd --in-delim $tab --require-col-match --exact !{raw;;pheno_test;--select-row 0,1,${p_col_disp}_\@test_tag,le:@2;if_prop=use_for_interesting} --select-col 0,1,'@3' --exclude-row 0,1 | sort | uniq -c | $smart_cut_cmd --out-delim $tab --select-row 0,1,ge:@4 --exclude-col 0,1 | sort -u 

short cmd make_burden_interesting_variant_genes_dat_file=for c in !{prop,,burden,max_interesting_variant_p_value},1 !{prop,,burden,interesting_variant_gene_combinations,if_prop=interesting_variant_gene_combinations,allow_empty=1}; do t=`echo \$c | cut -d, -f1` && n=`echo \$c | cut -d, -f2` && $burden_interesting_variant_helper(min_interesting_variant_p_missing_value,\$t,GENE,\$n,min_pheno_interesting_mac,); done | sort -u  > !{output,,burden_interesting_variant_genes_dat_file} class_level burden skip_if only_for_interesting


short cmd make_burden_interesting_genes_dat_file=$smart_join_cmd --in-delim $tab --exec "$smart_cut_cmd --in-delim $tab !{input,--file,burden_gassoc_file} --select-col 1,1,GENE  --exclude-row 1,1  | sort -u | $smart_cut_cmd --in-delim $tab !{input,--file,project_gene_target_file} --select-col 1,1 | sort | uniq -d | sed '1 s/^/GENE\n/'" --exec "$smart_cut_cmd --in-delim $tab !{input,--file,burden_gassoc_file} | cut -f2-" --header 1 --multiple 2 --extra 2 | $smart_cut_cmd --in-delim $tab --select-row 0,1 --select-row 0,1,TEST,'!{prop,,burden_test,test_tag,missing_prop=test_name,uc=1,if_prop=use_for_interesting}' | $smart_cut_cmd --in-delim $tab --select-row 0,1,P,lt:!{prop,,burden,max_interesting_gene_p_value,missing_key=default_max_interesting_gene_p_value} --select-col 0,1,P --select-col 0,1,GENE --out-delim $tab --require-col-match --exclude-row 0,1 --exact | sort -gk1 | cut -f2 | sort -u | $smart_cut_cmd --in-delim $tab --exec "cut -f1 !{input,,project_gene_target_file} | sort -u" | sort | uniq -d > !{output,,burden_interesting_genes_dat_file} class_level burden run_if burden_test skip_if only_for_interesting

local cmd make_empty_burden_interesting_genes_dat_file=echo > !{output,,burden_interesting_genes_dat_file} class_level burden skip_if burden_test run_if only_for_interesting

short cmd make_burden_interesting_gene_burdens_meta_file=if [[ `cat !{input,,pheno_interesting_genes_dat_file} | wc -l` -gt 0 ]]; then id="!{prop,,pheno}_gene_!{prop,,burden}" && echo "\$id class gene_burden" > !{output,,burden_interesting_gene_burdens_meta_file} && echo "\$id disp !{prop,,burden,disp}" >> !{output,,burden_interesting_gene_burdens_meta_file} && echo "\$id sort !{prop,,burden,sort}" >> !{output,,burden_interesting_gene_burdens_meta_file} >> !{output,,burden_interesting_gene_burdens_meta_file} && echo "\$id consistent !{prop,,burden}" >> !{output,,burden_interesting_gene_burdens_meta_file} && echo "\$id consistent !{prop,,annot}" >> !{output,,burden_interesting_gene_burdens_meta_file} !{raw,,burden,&& echo "\$id no_var_qq @no_var_qq" >> *burden_interesting_gene_burdens_meta_file,if_prop=no_var_qq,allow_empty=1}  !{raw,,burden,&& echo "\$id no_var_filter @no_var_filter" >> *burden_interesting_gene_burdens_meta_file,if_prop=no_var_filter,allow_empty=1} && $smart_join_cmd --exec "$smart_cut_cmd --in-delim $tab !{input,--file,burden_all_variant_list_file} !{input,--file,project_clean_gene_variant_file} --select-col 2,1,ID --exclude-row 2,1 | sort | uniq -d | sed '1 s/^/ID\n/'" --exec "$smart_cut_cmd !{input,--file,project_clean_gene_variant_file} --exact --require-col-match --in-delim $tab --select-col 1,1,'ID GENE'" --header 1 --rest-extra 1 --in-delim $tab | $smart_cut_cmd --in-delim $tab --exclude-row 0,1 --select-col 0,1,'GENE' | sort -u | cat - !{input,,pheno_interesting_genes_dat_file} | sort | uniq -d | perl -ne 'chomp; @a = split("\t"); $fix_gene_internal_id(\$a[0]); print "!select:!{prop,,project}:!{prop,,pheno} '"\$id"' parent $gene_internal_id(\$a[0])\n"' >> !{output,,burden_interesting_gene_burdens_meta_file}; else echo > !{output,,burden_interesting_gene_burdens_meta_file}; fi class_level burden skip_if or,not_trait,no_gene_burden

short cmd make_empty_burden_interesting_gene_burdens_meta_file=echo > !{output,,burden_interesting_gene_burdens_meta_file} class_level burden skip_if not_trait run_if no_gene_burden

short cmd make_burden_interesting_gene_variants_dat_file=v=!{prop,,burden,max_interesting_gene_variant_p_value} && $smart_join_cmd --exec "$burden_interesting_variant_helper(min_interesting_gene_variant_p_missing_value,\$v,GENE,1,min_pheno_gene_interesting_mac,\) | sort - !{input,,pheno_interesting_genes_dat_file} | uniq -d" --exec "$burden_interesting_variant_helper(min_interesting_gene_variant_p_missing_value,\$v,GENE ID,1,min_pheno_gene_interesting_mac,\)" --in-delim $tab --extra 2 --multiple 2 | cut -f2 > !{output,,burden_interesting_gene_variants_dat_file} class_level burden skip_if only_for_interesting

short cmd make_burden_all_interesting_gene_variants_dat_file=$smart_join_cmd --exec "$get_burden_all_vassoc_list(\) | $smart_cut_cmd --in-delim $tab --select-col 0,1,GENE --exclude-row 0,1 --exact --require-col-match | sort -u | $smart_cut_cmd --in-delim $tab !{input,--file,burden_interesting_genes_dat_file} | sort | uniq -d" --exec "$get_burden_all_vassoc_list(\) | $smart_cut_cmd --in-delim $tab --select-col 0,1,'GENE ID' --exclude-row 0,1 --exact --require-col-match" --in-delim $tab --multiple 2 --extra 2 | cut -f2 | sort -u > !{output,,burden_all_interesting_gene_variants_dat_file} class_level burden run_if and,all_variants_interesting,burden_test skip_if only_for_interesting

local cmd make_empty_burden_all_interesting_gene_variants_dat_file=rm -f !{output,,burden_all_interesting_gene_variants_dat_file} && touch !{output,,burden_all_interesting_gene_variants_dat_file} class_level burden skip_if or,(and,all_variants_interesting,burden_test),only_for_interesting

awk_normalize_sort=awk -v n=\$@1 -v OFS=\"\t\" '{print \\$1,\\$2\*n}'

subset_flat_gassoc_file=$smart_join_cmd --in-delim $tab --header 1 --exec @1"$smart_cut_cmd --in-delim $tab !{input,--file,burden_flat_gassoc_file} --select-col 1,2 --exclude-row 1,1 | sort -u | $smart_cut_cmd !{input,--file,project_gene_target_file}  --select-col 1,1 --exclude-row 1,1 | sort | uniq -d | sed '1 s/^/GENE\n/'@1" --exec @1"cut -f2- !{input,,burden_flat_gassoc_file}@1" --extra 2 --multiple 2

short cmd make_burden_gene_sort_values_file=a=`$subset_flat_gassoc_file() | wc -l` && b=`$get_burden_vassoc_file(vassoc,!{prop::burden:min_pheno_mac},gt:0,,) | tail -n+2 | wc -l` && $smart_cut_cmd --in-delim $tab --exec "$subset_flat_gassoc_file(\) | $smart_cut_cmd --in-delim $tab --exact --require-col-match --select-col 0,1,'GENE P_MIN' | tail -n+2 | $awk_normalize_sort(a)" --exec "$get_burden_vassoc_file(vassoc,!{prop::burden:min_pheno_mac},gt:0,,\) | $smart_cut_cmd --select-col 0,1,'GENE !{raw::pheno_test:${p_col_disp}_@test_tag:if_prop=use_for_interesting}' --exclude-row 0,1 !{raw;;pheno_test;--select-row 0,1,${p_col_disp}_@test_tag,ne:NA;if_prop=use_for_interesting} --in-delim $tab --exact --require-col-match | $awk_normalize_sort(b)" | awk '\$1 != "$outside_gene_name"' > !{output,,burden_gene_sort_values_file} class_level burden run_if burden_test skip_if only_for_interesting


#awk '{print "chr"\$1":"\$2}' !{input,,burdenl_chr_pos_var_list_file} | sed 's/^chrchr/chr/' > !{output,,burdenl_reg_list_file} class_level burdenl exrunif

#!|expand:;:burdenl;exrunif:burden;run_if or,annot_genes,annot_manual_gene_list_file,!num_var_subsets:burden_variant_subset;run_if and,!annot_genes,!annot_manual_gene_list_file,num_var_subsets| \
#local cmd make_burdenl_score_seq_map_file=cat !{input,,burdenl_chr_pos_var_list_file} | $add_gene_annot_cmd --in-delim $tab --print-multiple --chr-col 1 --pos-col 2 --gene-file !{input,,project_gene_target_file} --out-delim $tab | cut -f1,4 > !{output,,burdenl_score_seq_map_file} class_level burdenl exrunif

!!expand:;:btype;exskipif\
:gassoc_counts_data_file;\
:locdb_detail_reg_file;\
!\
local cmd make_cat_burden_btype=(head -qn+1 !{input,,burden_variant_subset_btype,limit=1,unless_prop=annot_manual_variant_list_file,allow_empty=1} !{input,,burden_variant_subset_btype,if_prop=annot_manual_variant_list_file,allow_empty=1} | awk 'NR == 1' && tail -qn+2 !{input,,burden_variant_subset_btype}) !{input,pheno_is_trait_file} > !{output,,burden_btype} class_level burden run_if burden_variant_subset exskipif run_with burden_variant_subset

fix_overlap_bug2=| awk -F"\t" '{split(\$4, a, "_")} !m[a[2]] {m[a[2]]=1; print}'

!|expand%;%btype;exskipif\
%case_side_gassoc_file;\
%control_side_gassoc_file;\
%gassoc_file;\
|\
local cmd make_cat_burden_test_btype=(head -qn+1 !{input,,burden_test_variant_subset_btype,limit=1} | head -n1 && tail -qn+2 !{input,,burden_test_variant_subset_btype}) !{input,pheno_is_trait_file} > !{output,,burden_test_btype} class_level burden_test run_if burden_test_variant_subset exskipif run_with burden_test_variant_subset

!!expand:;:atype\
:all_variant_list_file\
:annot_variant_list_file\
:non_annot_variant_list_file\
:epacts_group_file\
:setid_file\
!\
local cmd make_cat_no_head_annot_atype=cat !{input,,annot_variant_subset_atype} > !{output,,annot_atype} class_level annot run_if annot_variant_subset run_with annot_variant_subset

!!expand:;:atype\
:locdb_detail_reg_file\
!\
local cmd make_cat_annot_atype=(head -qn+1 !{input,,annot_variant_subset_atype,limit=1,unless_prop=annot_manual_variant_list_file,allow_empty=1} !{input,,annot_variant_subset_atype,if_prop=annot_manual_variant_list_file,allow_empty=1} | awk 'NR == 1' && tail -qn+2 !{input,,annot_variant_subset_atype}) > !{output,,annot_atype} class_level annot run_if annot_variant_subset run_with annot_variant_subset

!!expand:;:btype;exskipif;extracmd\
:non_annot_variant_list_file;skip_if only_for_interesting;\
:all_variant_list_file;skip_if only_for_interesting;\
:epacts_group_file;;\
:setid_file;;\
:regex_file;;| sort -u\
!\
local cmd make_cat_no_head_burden_btype=cat !{input,,burden_variant_subset_btype} !{input,pheno_is_trait_file} extracmd > !{output,,burden_btype} class_level burden run_if burden_variant_subset exskipif run_with burden_variant_subset

max_gassoc_points=5000

missing_key=default_major_burden_tests

prop burden_qq_min_mac=scalar default 10


burden_gassoc_qq_pdf_helper=$draw_qq_plot_cmd !{input,,@1} !{output,,@2}  @3 value.col=`perl -e 'print join(",", qw(!{prop,,burden_test,test_tag,if_prop=use_for_display,missing_prop=test_name,uc=1}))'` confidence.intervals=TRUE do.unif=TRUE nrow=1 ncol=1 sep=$tab plot.type=b max.plot.points=$max_gassoc_points shade.col=NVAR filter.col=MAC filter.value=">$((!{prop,,burden,burden_qq_min_mac}-1))"

local cmd make_burden_gassoc_qq_pdf_file=$burden_gassoc_qq_pdf_helper(burden_flat_gassoc_file,burden_gassoc_qq_pdf_file,'QC Plus Gene Associations for !{prop\,\,burden\,disp} variants: !{prop\,\,pheno\,disp}') class_level burden skip_if or,not_trait,no_gene_qq run_if burden_test

max_vassoc_points=10000
vassoc_qq_common_maf_threshold=.05
vassoc_qq_discrete_mac_threshold=15
exome_qq_perm=15
small_qq_perm=75

prop calibrate_qq=scalar

burden_vassoc_qq_pdf_helper=$draw_qq_plot_cmd @1 !{output,,@2} @3 value.col=`perl -e 'print join(",", qw(!{raw::pheno_test:${p_col_disp}_\@test_tag:if_prop=use_for_display}))'` do.unif=TRUE sep=$tab max.plot.points=$max_vassoc_points confidence.intervals=TRUE plot.type= shade.col=MAF !{raw,,@4,count.col\=MAC freq.col\=CASEF tot.col\=OBS_HAP p.type\=OR,if_prop=calibrate_qq,allow_empty=1} !{raw,n.perm=,pheno,$exome_qq_perm,if_prop=whole_exome,allow_empty=1} !{raw,n.perm=,pheno,$small_qq_perm,unless_prop=whole_exome,if_prop=calibrate_qq,allow_empty=1} main.cex=.85 @5

get_burden_vassoc_list_helper=$smart_join_cmd --header 1 --in-delim $tab --exec 'cut -f1 !{input,,@1} | tail -n+2 | sort - !{input,,@2} | uniq -d | $add_header_cmd ID' !{input,--file,@1} --rest-extra 1

get_burden_all_vassoc_list=$get_burden_vassoc_list_helper(pheno_vassoc_annot_file,burden_all_variant_list_file)
get_burden_vassoc_list=$get_burden_vassoc_list_helper(pheno_vassoc_clean_annot_file,burden_clean_variant_list_file)

get_burden_non_top_vassoc_list=$smart_join_cmd --header 1 --in-delim $tab !{input,--file,pheno_vassoc_clean_annot_file} --exec "sort !{input,,burden_clean_variant_list_file} !{input,,pheno_top_hits_ld_seq_var_list} !{input,,pheno_top_hits_ld_seq_var_list} | uniq -u" --extra 1

get_vassoc_file_helper=| $add_function_cmd --in-delim $tab --header 1 --col1 OBSA --col2 OBSU --val-header OBST --type add | $add_function_cmd --in-delim $tab --header 1 --col1 OBSA --col2 OBST --val-header CASEF --type divide | $add_function_cmd --in-delim $tab --header 1 --col1 OBST --val2 2 --val-header OBS_HAP --type multiply | $add_function_cmd --in-delim $tab --header 1 --col1 MAF --val2 .5 --val-header MAF_DIFF --type subtract | $add_function_cmd --in-delim $tab --header 1 --col1 MAF_DIFF --val-header MAF_DIFF_ABS --type abs | $add_function_cmd --in-delim $tab --header 1 --val1 1 --col2 MAF_DIFF_ABS --val-header MAF_NORM --type subtract | $smart_cut_cmd --in-delim $tab --exact --and-row-all --select-row 0,1 --select-row 0,1,MAC,'ge:@1' --select-row 0,1,MAF,'@2' @3

get_burden_vassoc_file=${get_burden_@{1}_list}(@5) $get_vassoc_file_helper(@2,@3,@4,burden)
get_gene_vassoc_file=$smart_cut_cmd --in-delim $tab !{input,--file,pheno_vassoc_clean_annot_file} --select-row 1,1,GENE,!{prop,,gene,external_id} --select-row 1,1 $get_vassoc_file_helper(@1,@2,@3,gene)

!|expand;%;keytype%freqtype%minmac%mafselect%extraselect%disptitle\
;common%common%!{prop::burden:min_pheno_mac}%ge:$vassoc_qq_common_maf_threshold%%MAF > $vassoc_qq_common_maf_threshold\
;rare%rare%!{prop::burden:min_pheno_mac}%le:$vassoc_qq_common_maf_threshold%%MAC > !{prop::burden:min_pheno_mac}\, MAF < $vassoc_qq_common_maf_threshold\
;uncommon%uncommon%$vassoc_qq_discrete_mac_threshold%le:$vassoc_qq_common_maf_threshold%%MAC > $vassoc_qq_discrete_mac_threshold\, MAF < $vassoc_qq_common_maf_threshold| \
restart_mem short cmd make_burden_keytype_vassoc_qq_pdf_file=$get_burden_vassoc_file(vassoc,minmac,mafselect,extraselect,) | $burden_vassoc_qq_pdf_helper(/dev/stdin,burden_freqtype_vassoc_qq_pdf_file,'!{prop\,\,burden\,disp} variant associations for !{prop\,\,pheno\,disp}: disptitle',burden,) class_level burden skip_if no_var_qq

num_slide_unique=25
num_slide_associated=25

mac_qualifier=, MAC >= !{prop::burden:min_pheno_mac}

!~expand%@%keytype@numprop@mafprop@extraselect@titletext@extrarunif%\
common_vassoc@$num_slide_associated@ge:.05@@--- MAF > .05@or,max_associated_maf,burden_maf,burden_mac_ub%\
vassoc@$num_slide_associated@le:!{prop\\\,\\\,burden\\\,max_associated_maf\\\,missing=.05}@@!{raw,,burden,--- MAF < ,unless_prop=burden_maf,allow_empty=1}!{prop,,burden,max_associated_maf,missing=.05,unless_prop=burden_maf,allow_empty=1}$mac_qualifier@%\
unique@$num_slide_unique@.@| $smart_cut_cmd --in-delim $tab --select-row 0,1 --select-row 0,1,MAFA,0 --select-row 0,1,MAFU,0 --exact | awk -F"\t" -v OFS="\t" 'NR == 1 {for(i=1;i<=NF;i++) {m[\$i]=i}} {if (\$m["MAFA"] > \$m["MAFU"]) {v=\$m["MAFA"]} else {v=\$m["MAFU"]}} NR == 1 {print 2,\$0} NR > 1 {print v,\$0}' | sort -grk1 | cut -f2-@unique to cases or controls$mac_qualifier@~ \
short cmd make_burden_slide_keytype_tex_file=$pheno_slide_associated_variants_helper(.,.,.,mafprop,pheno_vassoc_clean_annot_file) | cat !{input,,burden_all_variant_list_file} - | awk 'NF == 1 { is_selected[\$1] = 1; h = 1; } NF > 1 && (h || is_selected[\$2]) {print; h=0}' extraselect | $head_cmd(numprop) | $translate_variant_type | $replace_var_id | $process_associated_variants_helper("Most associated !{prop,,burden,disp} variants titletext: !{prop,,pheno,disp}",7,,5,$vassoc_max_chars_per_col,2) > !{output,,burden_slide_keytype_tex_file} class_level burden skip_if extrarunifno_var_filter

!!expand:keytype:common_vassoc:vassoc:unique:common_vassoc_meta_trait:vassoc_meta_trait:unique_meta_trait! \
local cmd make_burden_slide_keytype_pdf_file=$run_latex_cmd(burden_slide_keytype_tex_file,burden_slide_keytype_pdf_file) class_level burden


!~expand%@%keytype@numprop@mafprop@extraselect@titletext@extrarunif%\
common_vassoc@$num_slide_associated@ge:.05@@--- MAF > .05@or,max_associated_maf,burden_maf,burden_mac_ub,%\
vassoc@$num_slide_associated@le:!{prop\\,\\,burden\\,max_associated_maf\\,missing=.05}@@!{raw,,burden,--- MAF < ,unless_prop=burden_maf,allow_empty=1}!{prop,,burden,max_associated_maf,missing=.05,unless_prop=burden_maf,allow_empty=1}$mac_qualifier@%\
unique@$num_slide_unique@.@| $smart_cut_cmd --in-delim $tab --select-row 0,1 --select-row 0,1,MAFA,0 --select-row 0,1,MAFU,0 --exact | awk -F"\t" -v OFS="\t" 'NR == 1 {for(i=1;i<=NF;i++) {m[\$i]=i}} {if (\$m["MAFA"] > \$m["MAFU"]) {v=\$m["MAFA"]} else {v=\$m["MAFU"]}} NR == 1 {print 2,\$0} NR > 1 {print v,\$0}' | sort -grk1 | cut -f2-@unique to cases or controls$mac_qualifier@~ \
short cmd make_burden_slide_keytype_meta_trait_tex_file=$pheno_slide_variants_helper(.,.,.,.*,,!{raw::pheno_test:${p_col_disp}_@test_tag:if_prop=use_for_display:limit=1}_!{prop::pheno:disp},mafprop,pheno_meta_trait_vassoc_annot_file,,!{prop::burden:min_pheno_mac}) | cat !{input,,burden_all_variant_list_file} - | awk 'NF == 1 { is_selected[\$1] = 1; h = 1; } NF > 1 && (h || is_selected[\$1]) {print; h=0}' extraselect | $head_cmd(numprop) | $translate_variant_type | $replace_var_id | $transpose_cmd --in-delim $tab | $smart_cut_cmd --in-delim $tab --vec-delim @ --select-row 0,1,CLOSEST_GENE --select-row 0,1,ID --select-row 0,1,'^(!{prop,,pheno,vassoc_meta_disp,sep=|})$' --select-row 0,1,'eq:${or_col_disp_pheno_test}_!{prop::pheno:disp}' --select-row 0,1,'eq:!{raw::pheno_test:${p_col_disp}_@{test_tag}:limit=1}_!{prop::pheno:disp}' !{raw;;pheno;--select-row 0,1,"^(`head -n1 *pheno_small_vassoc_annot_file | rev | cut -f1-2 | rev | sed 's/^/MAF\t/' | sed 's/\(\S\S*\)/\1_@disp/g' | sed 's/)/\\\\)/g' | sed 's/(/\\\\(/g' | sed 's/\t/\|/g'`)$";all_instances=1;if_prop=pheno:eq:@meta_trait_inv;if_prop=project:eq:@project} | sed 's/^\([^\t][^\t]*\)/\1\t\1/' | sed 's/^[^\t][^\t]*\(\s\s*\)\([^\t][^\t]*\)_\(!{prop,,pheno,disp}\)\t/\3\1\2\t/' !{raw;;pheno;| sed 's/^[^\t][^\t]*\(\s\s*\)\([^\t][^\t]*\)_\(@disp\)\t/\3\1\2\t/';all_instances=1;if_prop=pheno:eq:@meta_trait_inv;if_prop=project:eq:@project} |  $transpose_cmd --in-delim $tab | $smart_cut_cmd --in-delim $tab | $swap_header(!{prop::pheno:vassoc_meta_disp} ID CLOSEST_GENE,!{prop::pheno:vassoc_meta_headers} Variant Gene,1\,2) | $translate_variant_type | $replace_var_id | $replace_nan(-) | $format_columns_cmd --in-delim $tab --number-format %.2g | $table_to_beamer_cmd --allow-breaks --font-size 8pt --auto-dim --bottom-margin 0in --left-margin 0in --right-margin -in --title "Most associated !{prop,,burden,disp} variants titletext: !{prop,,pheno,disp}" --in-delim $tab --header-rows 2 --multi-row .,2 --multi-col 1-2 > !{output,,burden_slide_keytype_meta_trait_tex_file} class_level burden run_if meta_trait_inv skip_if extrarunifno_var_filter

max_dist_per_gene=7
num_slide_associated_gene=25

associated_genes_tex_select_helper=--select-col @1,1,'P_MIN GENE TRANSCRIPT' --exact --select-col @1,1,'!{prop,,burden_test,test_tag,missing_prop=test_name,if_prop=use_for_display,uc=1} $gassoc_mina_minu $gassoc_dist'
"""
}
    