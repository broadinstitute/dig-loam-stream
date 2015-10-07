
package loamstream.compiler

/**
 * LoamStream
 * Created by oliverr on 10/6/2015.
 */
object PipelineTargetedPart23 {
  val string =
 """
#epacts commands

!!expand:pheno_testl:pheno_test:pheno_test_variant_subset:burden_test:burden_test_variant_subset! \
local cmd make_pheno_testl_epacts_ready_file=touch !{output,,pheno_testl_epacts_ready_file} class_level pheno_testl

!|expand:pheno_test:pheno_test:burden_test| \
!|expand:;:covartype:covar:popgen| \
!|expand:;:alltype:all:unrelated| \
pheno_test_epacts_or_plink_covartype_alltype_keep_helper=$smart_join_cmd --in-delim $tab --exec "sort -u !{input,,pheno_test_sample_include_file,if_prop=pheno_test_sample_include_file,allow_empty=1} !{raw,,pheno_test,/dev/null,unless_prop=pheno_test_sample_include_file,allow_empty=1} | sed 's/^\(\S\S*\)/\1\t\1/' | cat - !{input,,pheno_sample_covartype_alltype_include_file} | cut -f2 | sort !{raw,,pheno_test,| uniq -d,if_prop=pheno_test_sample_include_file,allow_empty=1} !{raw,,pheno_test,| cat - *pheno_test_alternate_pheno_file | cut -f1 | sort | uniq -d,if_prop=alternate_pheno,allow_empty=1} !{input,pheno_test_alternate_pheno_file,if_prop=alternate_pheno,allow_empty=1} @4" !{input,--file,pheno_@{1}_covartype_alltype_@{2}_file} --rest-extra 1 @3 --col 2,2 | awk -F"\t" -v OFS="\t" '{t=\$1;\$1=\$2;\$2=t} {print}' > !{output,,pheno_test_sample_include_aux_file}

!|expand:pheno_test:pheno_test:burden_test| \
!|expand:;:covartype;skipif:covar;or,run_raw,(and,!strat_covar,!covar_traits):popgen;!run_raw| \
!|expand:;:alltype;runif:all;include_related:unrelated;!include_related| \
local cmd make_pheno_test_epacts_covartype_alltype_sample_include_aux_file=$pheno_test_epacts_or_plink_covartype_alltype_keep_helper(epacts,ped,--header 1,| sed '1 s/^/IID\n/') class_level pheno_test skip_if skipif run_if and,(or,pheno_test_sample_include_file,alternate_pheno),test_software:eq:epacts,runif

!|expand:;:alltype;allrunif:all;include_related:unrelated;!include_related| \
!!expand:pheno_test:pheno_test:burden_test! \
local cmd make_pheno_test_epacts_covars_alltype_aux_file=$pheno_test_covars_aux_helper(pheno_epacts_covar_alltype_ped_file) class_level pheno_test run_if and,!pheno_test_sample_include_file,!alternate_pheno,extra_covar_traits,test_software:eq:epacts,allrunif

!|expand:;:alltype;allrunif:all;include_related:unrelated;!include_related| \
!!expand:pheno_test:pheno_test:burden_test! \
local cmd make_pheno_test_epacts_alternate_pheno_covars_alltype_aux_file=$pheno_test_alternate_pheno_covars_aux_helper(pheno_epacts_popgen_alltype_ped_file,pheno_epacts_covar_alltype_ped_file,6,8,) class_level pheno_test run_if and,!pheno_test_sample_include_file,alternate_pheno,test_software:eq:epacts,allrunif

!!expand:pheno_test:pheno_test:burden_test! \
local cmd make_pheno_test_epacts_covars_with_include_aux_file=$pheno_test_covars_aux_helper(pheno_test_sample_include_aux_file) class_level pheno_test run_if and,pheno_test_sample_include_file,!alternate_pheno,extra_covar_traits,test_software:eq:epacts

!|expand:;:alltype;allrunif:all;include_related:unrelated;!include_related| \
!!expand:pheno_test:pheno_test:burden_test! \
local cmd make_pheno_test_epacts_alternate_pheno_covars_with_include_alltype_aux_file=$pheno_test_alternate_pheno_covars_aux_helper(pheno_epacts_alltype_ped_file,pheno_test_sample_include_aux_file,6,8,) class_level pheno_test run_if and,pheno_test_sample_include_file,alternate_pheno,test_software:eq:epacts,allrunif


epacts_single_unit=10000000

epacts_get_chr_flag=`cut -f1 !{input,,project_variant_subset_var_keep_file} | sort -u | tr '\n' ' ' | awk 'NF == 1 {print "--chr",\$1}'`

prop epacts_unit=scalar

!!expand:,:pheno_testl,whichvcf,getflag:pheno_test,project_clean_all_vcf_file,'':pheno_test_variant_subset,pheno_variant_subset_clean_all_vcf_file,$epacts_get_chr_flag! \
pheno_testl_epacts_single_helper1=!{input,pheno_testl_epacts_ready_file} chr_flag=getflag && $epacts_cmd single -restart --missing $plink_missing !{input,--vcf,@6} !{input,--ped,pheno_epacts_@{3}_ped_file,unless_prop=pheno_test_sample_include_file,unless_prop=extra_covar_traits,unless_prop=alternate_pheno,allow_empty=1} !{input,--ped,pheno_test_sample_include_aux_file,if_prop=pheno_test_sample_include_file,unless_prop=extra_covar_traits,unless_prop=alternate_pheno,allow_empty=1} !{input,--ped,pheno_test_covars_aux_file,if_prop=extra_covar_traits,if_prop=alternate_pheno,or_if_prop=1,allow_empty=1} !{input,--kinf,pheno_kinship_file,if_prop=test_name:eq:q.emmax,allow_empty=1} !{prop,-unit,pheno_test,epacts_unit,missing_key=epacts_single_unit} \$chr_flag --min-maf 0 --min-callrate 0 --min-mac 1 --pheno @5 --test @{2} !{prop;;pheno_test;test_options;if_prop=test_options;allow_empty=1} -run 1 !{prop,-field,pheno_test,epacts_gt_field} @4 !{raw,--out,pheno_testl,*@{1}} 


!!expand:,:pheno_testl,whichvcf:pheno_test,project_clean_all_vcf_file:pheno_test_variant_subset,pheno_variant_subset_clean_all_vcf_file! \
pheno_testl_epacts_single_helper=rm -f !{raw,,pheno_testl,*@{1}.epacts.gz} && !{raw,,pheno_testl,rm -f *@{1}.reml && ln -s *pheno_test_in_aux1_file *@{1}.reml && rm -f *@{1}.eigR && ln -s *pheno_test_in_aux2_file *@{1}.eigR &&,if_prop=test_name:eq:q.emmax,allow_empty=1} !{input;pheno_test_in_aux1_file;if_prop=test_name:eq:q.emmax;allow_empty=1} !{input;pheno_test_in_aux2_file;if_prop=test_name:eq:q.emmax;allow_empty=1} $pheno_testl_epacts_single_helper1(@1,@2,@3,@4,@5,whichvcf) !{raw,--remlf,pheno_test,*@{1}.reml,if_prop=test_name:eq:q.emmax,allow_empty=1} >&/dev/null && zcat !{raw,,pheno_testl,*@{1}.epacts.gz} 

pheno_test_epacts_eigen_helper=rm -f !{raw,,pheno_test,*pheno_test_vassoc_epacts_trunk.eigR} && rm -f !{raw,,pheno_test,*pheno_test_vassoc_epacts_trunk.reml} && $pheno_test_epacts_single_helper1(pheno_test_vassoc_epacts_trunk,!{prop::pheno_test:test_name},@1,@2 --chr -1,!{prop::pheno_test:pheno:unless_prop=alternate_pheno:allow_empty=1}!{prop::pheno_test:alternate_pheno:if_prop=alternate_pheno:allow_empty=1},pheno_sample_marker_pruned_vcf_file) && mv !{raw,,pheno_test,*pheno_test_vassoc_epacts_trunk.reml} !{output,,pheno_test_in_aux1_file} && mv !{raw,,pheno_test,*pheno_test_vassoc_epacts_trunk.eigR} !{output,,pheno_test_in_aux2_file}

get_epacts_covars="--cov $get_covar_traits( --cov ,@1)!{raw,,@1, --cov ,if_prop=covar_traits,if_prop=strat_covar,allow_empty=1}!{raw,,@1, --cov ,if_prop=manual_covar_traits,unless_prop=covar_traits,if_prop=strat_covar,allow_empty=1}`!{raw::@1:$get_mds_cols_int(\@num_mds_covar, --cov ,\):if_prop=strat_covar:allow_empty=1`}`"

get_spaced_covars="$get_covar_traits( ,@1)!{raw::@1: :if_prop=covar_traits:if_prop=strat_covar:allow_empty=1}!{raw::@1: :if_prop=manual_covar_traits:unless_prop=covar_traits:if_prop=strat_covar:allow_empty=1}`!{raw::@1:$get_mds_cols_int(\@num_mds_covar, ,\):if_prop=strat_covar:allow_empty=1`}`"

!|expand:;:alltype;runif:all;include_related:unrelated;!include_related| \
cmd make_pheno_test_alltype_epacts_raw_in_aux_files=$pheno_test_epacts_eigen_helper(popgen_alltype,) class_level pheno_test run_if and,run_raw,test_software:eq:epacts,runif skip_if test_name:ne:q.emmax rusage_mod $epacts_eigen_mem run_with pheno_test_variant_subset

!|expand:;:alltype;runif:all;include_related:unrelated;!include_related| \
!|expand:,:shortt,pheno_testl,exskipif:,pheno_test,num_var_subsets:short,pheno_test_variant_subset,!num_var_subsets| \
shortt cmd make_pheno_testl_alltype_epacts_raw_vassoc_file=$pheno_testl_epacts_single_helper(pheno_testl_vassoc_epacts_trunk,!{prop::pheno_test:test_name},popgen_alltype,,!{prop::pheno_test:pheno:unless_prop=alternate_pheno:allow_empty=1}!{prop::pheno_test:alternate_pheno:if_prop=alternate_pheno:allow_empty=1}) > !{output,,pheno_testl_vassoc_file} class_level pheno_testl run_if and,run_raw,test_software:eq:epacts,runif skip_if exskipif rusage_mod $epacts_vassoc_mem

!|expand:;:alltype;runif:all;include_related:unrelated;!include_related| \
local cmd make_pheno_test_alltype_epacts_raw_info_file=$record_pheno_test_info(!{prop;;pheno_test;test_name;sep=\,},no,no,!{raw::pheno_test:@epacts_gt_field},!{input;;pheno_sample_popgen_alltype_include_file;unless_prop=pheno_test_sample_include_file;unless_prop=alternate_pheno;allow_empty=1} !{raw;;pheno_test;*pheno_test_sample_include_aux_file | tail -n+2;if_prop=pheno_test_sample_include_file;if_prop=alternate_pheno;or_if_prop=1;allow_empty=1} !{input;pheno_test_sample_include_aux_file;if_prop=pheno_test_sample_include_file;if_prop=alternate_pheno;or_if_prop=1;allow_empty=1}) class_level pheno_test run_if and,run_raw,test_software:eq:epacts,runif

!|expand:;:alltype;runif:all;include_related:unrelated;!include_related| \
cmd make_pheno_test_alltype_epacts_strata_in_aux_files=c=$get_epacts_covars(pheno_test) && $pheno_test_epacts_eigen_helper(covar_alltype,\$c) class_level pheno_test run_if and,!run_raw,(or,strat_covar,covar_traits),test_software:eq:epacts,runif skip_if test_name:ne:q.emmax rusage_mod $epacts_eigen_mem run_with pheno_test_variant_subset

!|expand:;:alltype;runif:all;include_related:unrelated;!include_related| \
!|expand:,:shortt,pheno_testl,exskipif:,pheno_test,num_var_subsets:short,pheno_test_variant_subset,!num_var_subsets| \
shortt cmd make_pheno_testl_alltype_epacts_strata_vassoc_file=c=$get_epacts_covars(pheno_test) && $pheno_testl_epacts_single_helper(pheno_testl_vassoc_epacts_trunk,!{prop::pheno_test:test_name},covar_alltype,\$c,!{prop::pheno_test:pheno:unless_prop=alternate_pheno:allow_empty=1}!{prop::pheno_test:alternate_pheno:if_prop=alternate_pheno:allow_empty=1}) > !{output,,pheno_testl_vassoc_file} class_level pheno_testl run_if and,!run_raw,(or,strat_covar,covar_traits),test_software:eq:epacts,runif skip_if exskipif rusage_mod $epacts_vassoc_mem

!|expand:;:alltype;runif:all;include_related:unrelated;!include_related| \
local cmd make_pheno_test_alltype_epacts_strata_info_file=$record_pheno_test_info(!{prop;;pheno_test;test_name;sep=\,},$get_disp_covars_pheno_test,no,!{raw::pheno_test:@epacts_gt_field},!{input;;pheno_sample_covar_alltype_include_file;unless_prop=pheno_test_sample_include_file;unless_prop=extra_covar_traits;unless_prop=alternate_pheno;allow_empty=1} !{raw;;pheno_test;*pheno_test_sample_include_aux_file | tail -n+2;if_prop=pheno_test_sample_include_file;unless_prop=extra_covar_traits;unless_prop=alternate_pheno;allow_empty=1} !{input;pheno_test_sample_include_aux_file;if_prop=pheno_test_sample_include_file;unless_prop=extra_covar_traits;unless_prop=alternate_pheno;allow_empty=1} !{raw;;pheno_test;*pheno_test_covars_aux_file | tail -n+2;if_prop=extra_covar_traits;if_prop=alternate_pheno;or_if_prop=1;allow_empty=1} !{input;pheno_test_covars_aux_file;if_prop=extra_covar_traits;if_prop=alternate_pheno;or_if_prop=1;allow_empty=1}) class_level pheno_test run_if and,!run_raw,(or,strat_covar,covar_traits),test_software:eq:epacts,runif rusage_mod $epacts_vassoc_mem

#add new columns based on the test

#!|expand:,:shortt,pheno_testl,whichvcf,phenol,projectl,exskipif:,pheno_test,project_clean_all_vcf_file,pheno,project,skip_if num_var_subsets:short,pheno_test_variant_subset,pheno_variant_subset_clean_all_vcf_file,pheno_variant_subset,project_variant_subset,skip_if !num_var_subsets| \
#shortt cmd make_pheno_testl_epacts_small_vassoc_file=$smart_join_cmd --in-delim $tab --header 1 --exec "$smart_join_cmd --in-delim $tab !{input,--file,projectl_vfreq_file} --exec \"$smart_cut_cmd --in-delim $tab !{input,--file,phenol_vassoc_counts_file} --select-col 1,1,'VAR NEFF'\" --header 1 --extra 1 | $smart_cut_cmd --in-delim $tab --select-col 0,1,'VAR CHR POS REF ALT NEFF' --exact --require-col-match | $parse_out_id | tail -n+2 | perl -ne 'BEGIN {open IN, qw(!{input,,project_chr_map_file}) or die; while (<IN>) {@a = split; \\$m{\\$a[0]}=\\$a[1]}} @a = split(\"\\t\"); if (\\$m{\\$a[1]}) {\\$a[1]=\\$m{\\$a[1]}} print join(\"\\t\", @a)' | awk -F\"\t\" -v OFS=\"\t\" '{print \\$2\":\"\\$3\"_\"\\$4\"/\"\\$5,\\$1,\\$4,\\$5,\\$6}' | sed '1 s/^/EPACTS_ID\tID\tREF\tALT\tNEFF\n/'" !{input,--file,pheno_testl_vassoc_file} --rest-extra 2 --header 1 --col 2,4 \
#			 | $add_function_cmd --in-delim $tab --header 1 --col1 $beta_col_disp --type exp --val-header $or_col_disp \
#			 | $smart_cut_cmd --tab-delim --exact --require-col-match --select-col 0,2 --select-col 0,1,'NS NEFF MAF REF ALT $or_col_disp BETA SEBETA PVALUE' | sed '1 s/\tPVALUE/\t$p_col_disp/' | sed '1 s/\tNS/\t$n_col_disp/' | sed '1 s/\tNEFF/\t$neff_col_disp/' | sed '1 s/\tMAF/\t$maf_col_disp/' | sed '1 s/\tREF/\t$ref_col_disp/' | sed '1 s/\tALT/\t$alt_col_disp/' | sed '1 s/\tSEBETA/\t$se_col_disp/' | sed '1 s/\tBETA/\t$beta_col_disp/' | sed '1 s/^\S\S*/$id_col_disp/' | $replace_nan(NA) > !{output,,pheno_testl_small_vassoc_file} class_level pheno_testl run_if test_software:eq:epacts exskipif

#old: --exec "$smart_cut_cmd --in-delim $tab !{input:--file:pheno_testl_vassoc_file} --select-col 1,4 --select-col 1,1,. | sed '1 s/^/:_:_/' | cut -d_ -f3- "

!|expand:,:shortt,pheno_testl,whichvcf,phenol,projectl,exskipif:,pheno_test,project_clean_all_vcf_file,pheno,project,skip_if num_var_subsets:short,pheno_test_variant_subset,pheno_variant_subset_clean_all_vcf_file,pheno_variant_subset,project_variant_subset,skip_if !num_var_subsets| \
shortt cmd make_pheno_testl_epacts_small_vassoc_file=$smart_join_cmd --in-delim $tab --header 1 --exec "$smart_join_cmd --in-delim $tab !{input,--file,projectl_vfreq_file} --exec \"$smart_cut_cmd --in-delim $tab !{input,--file,phenol_vassoc_counts_file} --select-col 1,1,'VAR NEFF'\" --header 1 --extra 1 | $smart_cut_cmd --in-delim $tab --select-col 0,1,'VAR REF ALT NEFF' --exact --require-col-match | $parse_out_id | tail -n+2 | sed '1 s/^/ID\tREF\tALT\tNEFF\n/'" --exec "$fill_file_helper_int(--exec \\"$smart_cut_cmd --in-delim $tab !{input:--file:pheno_testl_vassoc_file} --arg-delim : --select-col 1:4 --select-col 1:1:. | sed '1 s/^/:_:_/' | cut -d_ -f3-\\",cat !{input::pheno_testl_vassoc_file},!{input::phenol_vassoc_counts_file} | $parse_out_id,NA,1,1,1,1,\t)" --rest-extra 2 --header 1 \
			 | $add_function_cmd --in-delim $tab --header 1 --col1 $beta_col_disp --type exp --val-header $or_col_disp \
			 | $smart_cut_cmd --tab-delim --exact --require-col-match --select-col 0,1 --select-col 0,1,'NS NEFF MAF REF ALT $or_col_disp BETA SEBETA PVALUE' | sed '1 s/\tPVALUE/\t$p_col_disp/' | sed '1 s/\tNS/\t$n_col_disp/' | sed '1 s/\tNEFF/\t$neff_col_disp/' | sed '1 s/\tMAF/\t$maf_col_disp/' | sed '1 s/\tREF/\t$ref_col_disp/' | sed '1 s/\tALT/\t$alt_col_disp/' | sed '1 s/\tSEBETA/\t$se_col_disp/' | sed '1 s/\tBETA/\t$beta_col_disp/' | sed '1 s/^\S\S*/$id_col_disp/' | $replace_nan(NA) > !{output,,pheno_testl_small_vassoc_file} class_level pheno_testl run_if test_software:eq:epacts exskipif bsub_batch 50

#plink commands

!|expand:;:covartype;skipif:covar;or,run_raw,(and,!strat_covar,!covar_traits):popgen;!run_raw| \
!|expand:;:alltype;runif:all;include_related:unrelated;!include_related| \
local cmd make_pheno_test_plink_covartype_alltype_sample_include_aux_file=$pheno_test_epacts_or_plink_covartype_alltype_keep_helper(sample_plink,include,,) class_level pheno_test skip_if skipif run_if and,(or,pheno_test_sample_include_file,alternate_pheno),test_software:eq:plink,runif


!|expand:,:phenol,pheno_testl:pheno,pheno_test:pheno_variant_subset,pheno_test_variant_subset| \
plink_pheno_testl_helper_no_flag=$plink_cmd $plink_in_bed_helper(phenol) !{input,--pheno,pheno_test_covars_aux_file,if_prop=alternate_pheno,allow_empty=1} !{input,--keep,pheno_sample_plink_@{3}_include_file,unless_prop=pheno_test_sample_include_file,unless_prop=alternate_pheno,allow_empty=1} !{input,--keep,pheno_test_sample_include_aux_file,if_prop=pheno_test_sample_include_file,if_prop=alternate_pheno,or_if_prop=1,allow_empty=1} --allow-no-sex !{raw,--out,pheno_testl,*{@{1}}} !{output,@{2}} 

!|expand:,:pheno_testl:pheno_test:pheno_test_variant_subset| \
plink_pheno_testl_helper=$plink_pheno_testl_helper_no_flag(@1,@2,@3) --!{prop::pheno_test:test_name} !{prop;;pheno_test;test_options;if_prop=test_options;allow_empty=1}

!|expand:;:alltype;allrunif:all;include_related:unrelated;!include_related| \
!|expand:,:shortt,pheno_testl,exskipif:,pheno_test,num_var_subsets:short,pheno_test_variant_subset,!num_var_subsets| \
shortt cmd make_pheno_testl_plink_raw_alltype_vassoc_file=$plink_pheno_testl_helper(pheno_testl_vassoc_plink_trunk,pheno_testl_vassoc_file,popgen_alltype) && mv !{raw,,pheno_testl,*pheno_testl_vassoc_plink_trunk}.assoc.!{prop::pheno_test:test_name} !{output,,pheno_testl_vassoc_file} class_level pheno_testl run_if and,run_raw,test_software:eq:plink,allrunif skip_if exskipif

!|expand:;:alltype;allrunif:all;include_related:unrelated;!include_related| \
local cmd make_pheno_test_plink_raw_alltype_info_file=$record_pheno_test_info(!{prop;;pheno_test;test_name;sep=\,},no,no,$gq_crit_helper,!{input;;pheno_sample_plink_popgen_alltype_include_file;unless_prop=pheno_test_sample_include_file;unless_prop=alternate_pheno,allow_empty=1} !{input;;pheno_test_sample_include_aux_file;if_prop=pheno_test_sample_include_file;if_prop=alternate_pheno,or_if_prop=1,allow_empty=1}) class_level pheno_test run_if and,run_raw,test_software:eq:plink,allrunif

plink_vif=100

!|expand:;:alltype;allrunif:all;include_related:unrelated;!include_related| \
!|expand:,:shortt,pheno_testl,exskipif:,pheno_test,num_var_subsets:short,pheno_test_variant_subset,!num_var_subsets| \
shortt cmd make_pheno_testl_plink_strata_alltype_vassoc_file=$plink_pheno_testl_helper(pheno_testl_vassoc_plink_trunk,pheno_testl_vassoc_file,covar_alltype) !{input,--covar,pheno_plink_covar_file,unless_prop=extra_covar_traits,allow_empty=1} !{input,--covar,pheno_test_covars_aux_file,if_prop=extra_covar_traits,allow_empty=1} --covar-name $get_plink_covars(pheno_test) --vif $plink_vif && mv !{raw,,pheno_testl,*pheno_testl_vassoc_plink_trunk}.assoc.!{prop::pheno_test:test_name} !{output,,pheno_testl_vassoc_file} class_level pheno_testl run_if and,!run_raw,(or,strat_covar,covar_traits),test_software:eq:plink,allrunif skip_if exskipif

!|expand:;:alltype;allrunif:all;include_related:unrelated;!include_related| \
local cmd make_pheno_test_plink_strata_alltype_info_file=$record_pheno_test_info(!{prop;;pheno_test;test_name;sep=\,},$get_disp_covars_pheno_test,no,$gq_crit_helper,!{input;pheno_test_covars_aux_file;if_prop=extra_covar_traits;allow_empty=1} !{raw;;pheno_test;*pheno_test_covars_aux_file | sed 's/ /\t/g' | cut -f1-2 | cat -;if_prop=extra_covar_traits;allow_empty=1} !{input;;pheno_sample_plink_covar_alltype_include_file;unless_prop=pheno_test_sample_include_file;unless_prop=alternate_pheno;allow_empty=1} !{input;;pheno_test_sample_include_aux_file;if_prop=pheno_test_sample_include_file;if_prop=alternate_pheno;or_if_prop=1;allow_empty=1} | sed 's/\s\s*/\t/g' !{raw;;pheno_test;| sort | uniq -d | cat -;if_prop=extra_covar_traits;allow_empty=1}) class_level pheno_test run_if and,!run_raw,(or,strat_covar,covar_traits),test_software:eq:plink,allrunif

#while converting, need to
#1. Make sure OR is relative to non-reference allele (in PLINK it is minor)
#2. Replace the var ID with chr:pos:id (which is what PSEQ outputs for glm)
#This is used to convert any plink glm (assoc) file
#And also to convert the dominant model when merging

!|expand:,:phenol,projectl:pheno,project:pheno_variant_subset,project_variant_subset| \
parse_ref_min_phenol=$smart_cut_cmd --in-delim $tab !{input,--file,projectl_vfreq_file} --select-col 1,1,'VAR REFMIN REF ALT' --require-col-match --exact | sed 's/^\(\S*:\S*:\(\S*\)\)/\2\t\1/' | sed '1 s/.*/VAR\tFULL_ID\tREFMIN\tREF\tALT/'

!|expand:,:shortt,pheno_testl,phenol,projectl,exskipif:,pheno_test,pheno,project,skip_if num_var_subsets:short,pheno_test_variant_subset,pheno_variant_subset,project_variant_subset,skip_if !num_var_subsets| \
shortt cmd make_pheno_testl_plink_small_vassoc_file=$smart_join_cmd --in-delim $tab --header 1 --col 3,2 --rest-extra 3 --exec "$parse_ref_min_phenol" --exec "$smart_cut_cmd --out-delim $tab !{input,--file,phenol_vassoc_counts_file} --select-col 1,1 --select-col 1,1,'MAF NEFF' | $parse_out_id" --exec "$smart_cut_cmd !{input,--file,pheno_testl_vassoc_file} --out-delim $tab --select-row 1,1,TEST,ADD --select-row 1,1"  !{raw;;pheno_test;| $add_function_cmd --in-delim $tab --col1 OR --type ln --header 1 --val-header BETA;if_prop=test_name:eq:logistic;allow_empty=1} !{raw;;pheno_test;| $add_function_cmd --in-delim $tab --header 1 --col1 BETA --type exp --val-header OR;if_prop=test_name:eq:linear;allow_empty=1} | $add_function_cmd --in-delim $tab --col1 BETA --col2 STAT --type divide --header 1 --val-header SE | $smart_cut_cmd --in-delim $tab --select-col 0,1,'REFMIN FULL_ID MAF REF ALT NMISS BETA OR SE STAT P NEFF' --exact --require-col-match | awk -F"\t" -v OFS="\t" 'NR > 1 && \$1 == 1 {if (\$8 == "NA") { } else if (\$8 == "inf") { \$8 = 0 } else if (\$8 != 0) {\$8 = 1/\$8} else {\$8 = $glm_max_or} \$10=-\$10; \$7=-\$7} {print}' | sed '1 s/NMISS/N/' | sed '1 s/MAF/F/' | cut -f2- | sed '1 s/^\S\S*/CHR:POS:VAR/' | $parse_out_id | $smart_cut_cmd --tab-delim --exact --require-col-match --select-col 0,1 --exact --select-col 0,1,'N NEFF F REF ALT OR BETA SE P' | sed '1 s/\tN/\t$n_col_disp/' | sed '1 s/\tNEFF/\t$neff_col_disp/' | sed '1 s/\tF/\t$maf_col_disp/' | sed '1 s/\tREF/\t$ref_col_disp/' | sed '1 s/\tALT/\t$alt_col_disp/' | sed '1 s/\tOR/\t$or_col_disp/' | sed '1 s/\tBETA/\t$beta_col_disp/' | sed '1 s/\tSE/\t$se_col_disp/' | sed '1 s/\tP/\t$p_col_disp/' | sed '1 s/^\S\S*/$id_col_disp/' | $replace_nan(NA) > !{output,,pheno_testl_small_vassoc_file} class_level pheno_testl run_if test_software:eq:plink exskipif

#pseq commands

get_disp_cluster=!{prop::@1:strata_traits:if_prop=strata_traits:sep=,:allow_empty=1}!{raw::@1:, :if_prop=strata_traits:if_prop=strat_cluster:allow_empty=1}!{raw::@1:with clustering:if_prop=strat_cluster:allow_empty=1}
get_disp_cluster_burden_test=$get_disp_cluster(burden_test)
get_disp_cluster_pheno_test=$get_disp_cluster(pheno_test)

!|expand:;:alltype;allrunif:all;include_related:unrelated;!include_related| \
!|expand:,:shortt,pheno_testl,phenol,exskipif:,pheno_test,pheno,num_var_subsets:short,pheno_test_variant_subset,pheno_variant_subset,!num_var_subsets| \
shortt cmd make_pheno_testl_pseq_raw_alltype_vassoc_file=$pseq_qc_plus_alltype_analysis_cmd_phenol($vassoc) $pheno_test_indiv_ex_helper $show_id $pheno_vassoc_helper(-1) !{raw,,pheno_test,--fix-null,if_prop=fix_null,allow_empty=1} !{prop;;pheno_test;test_options;if_prop=test_options;allow_empty=1} > !{output,,pheno_testl_vassoc_file} class_level pheno_testl run_if and,run_raw,test_software:eq:pseq,test_name:eq:v-assoc,allrunif skip_if exskipif

!|expand:;:alltype;allrunif:all;include_related:unrelated;!include_related| \
local cmd make_pheno_test_pseq_raw_alltype_info_file=$record_pheno_test_info(!{prop;;pheno_test;test_name;sep=\,},no,no,$gq_crit_helper,!{input;;pheno_sample_popgen_alltype_include_file} !{input;;pheno_test_sample_include_aux_file;if_prop=pheno_test_sample_include_file;allow_empty=1} !{input;;pheno_test_sample_include_aux_file;if_prop=pheno_test_sample_include_file;allow_empty=1} | sort | uniq -u) class_level pheno_test run_if and,run_raw,test_software:eq:pseq,test_name:eq:v-assoc,allrunif

!|expand:;:alltype;allrunif:all;include_related:unrelated;!include_related| \
!|expand:,:shortt,pheno_testl,phenol,exskipif:,pheno_test,pheno,num_var_subsets:short,pheno_test_variant_subset,pheno_variant_subset,!num_var_subsets| \
shortt cmd make_pheno_testl_pseq_strata_alltype_vassoc_file=$pseq_qc_plus_alltype_analysis_cmd_phenol_cluster($vassoc) $pheno_test_indiv_ex_helper $show_id $pheno_vassoc_helper(-1) $add_strata(pheno_testl) !{raw,,pheno_test,--fix-null,if_prop=fix_null,allow_empty=1} !{prop;;pheno_test;test_options;if_prop=test_options;allow_empty=1} > !{output,,pheno_testl_vassoc_file} class_level pheno_testl run_if and,!run_raw,(or,strat_cluster,strata_traits),test_software:eq:pseq,test_name:eq:v-assoc,allrunif skip_if exskipif
!|expand:;:alltype;allrunif:all;include_related:unrelated;!include_related| \
local cmd make_pheno_test_pseq_strata_alltype_info_file=$record_pheno_test_info(!{prop;;pheno_test;test_name;sep=\,},no,$get_disp_cluster_pheno_test,$gq_crit_helper,!{input;;pheno_sample_cluster_alltype_include_file} !{input;;pheno_test_sample_include_aux_file;if_prop=pheno_test_sample_include_file;allow_empty=1} !{input;;pheno_test_sample_include_aux_file;if_prop=pheno_test_sample_include_file;allow_empty=1} | sort | uniq -u) class_level pheno_test run_if and,!run_raw,(or,strat_cluster,strata_traits),test_software:eq:pseq,test_name:eq:v-assoc

!|expand:,:shortt,pheno_testl,whichvcf,phenol,projectl,exskipif:,pheno_test,project_clean_all_vcf_file,pheno,project,skip_if num_var_subsets:short,pheno_test_variant_subset,pheno_variant_subset_clean_all_vcf_file,pheno_variant_subset,project_variant_subset,skip_if !num_var_subsets| \
shortt cmd make_pheno_testl_pseq_small_vassoc_file=$smart_join_cmd --in-delim $tab !{input,--file,pheno_testl_vassoc_file} --exec "$smart_cut_cmd --in-delim $tab !{input,--file,phenol_vassoc_counts_file} --select-col 1,1,'VAR NEFF'" --rest-extra 1 --header 1 | sed '1 s/^/CHR:POS:/' | $parse_out_id | $add_function_cmd --in-delim $tab --col1 OBSA --col2 OBSU --header 1 --type add --val-header OBS | $add_function_cmd --in-delim $tab --col1 OR --header 1 --type exp --val-header BETA  | sed '1 s/$/\tSEBETA/' | sed '1! s/$/\tNA/' | $smart_cut_cmd --tab-delim --exact --require-col-match --select-col 0,1 --select-col 0,1,'OBS NEFF MAF REF ALT OR BETA SEBETA P I ORDOM PDOM IDOM ORREC PREC IREC' --require-col-match --exact | sed '1 s/\tP/\t$p_col_disp/' | sed '1 s/\tOBS/\t$n_col_disp/' | sed '1 s/\tNEFF/\t$neff_col_disp/' | sed '1 s/\tMAF/\t$maf_col_disp/' | sed '1 s/\tREF/\t$ref_col_disp/' | sed '1 s/\tALT/\t$alt_col_disp/' | sed '1 s/\tSEBETA/\t$se_col_disp/' | sed '1 s/\tBETA/\t$beta_col_disp/' | sed '1 s/\tOR/\t$or_col_disp/' | sed '1 s/^\S\S*/$id_col_disp/' | $replace_nan(NA) > !{output,,pheno_testl_small_vassoc_file} class_level pheno_testl run_if and,test_software:eq:pseq,test_name:eq:v-assoc exskipif

#MANTRA

#local cmd make_pheno_test_mantra_info_file=$record_pheno_meta_test_info(!{prop;;pheno_test;test_name;sep=\,;all_instances=1;if_prop=pheno_test:eq:@meta_test;if_prop=pheno:eq:@pheno;if_prop=project:eq:@project},NA,NA,NA) class_level pheno_test run_if and,test_software:eq:mantra,meta_trait_inv

local cmd make_pheno_test_mantra_info_file=$record_pheno_meta_test_info(!{prop;;pheno_test;test_name;sep=\,;all_instances=1;if_prop=pheno_test:eq:@meta_test;if_prop=pheno:eq:@meta_trait_inv;if_prop=project:eq:@project},NA,NA,NA) class_level pheno_test run_if and,test_software:eq:mantra,meta_trait_inv

local cmd make_pheno_test_mantra_in_aux1_file=perl -le 'print "!{prop,,pheno,meta_trait_inv,sep=\n}"' | sort > !{output,,pheno_test_in_aux1_file} class_level pheno_test run_if and,test_software:eq:mantra,meta_trait_inv

!|expand%;%shortt;pheno_testi;pheno_testl;exifprop;excons;phenol;projectl;exskipif%\
;pheno_test;pheno_test;;;project;project;skip_if num_var_subsets%\
short;pheno_test_variant_subset;pheno_test_variant_subset;if_prop=var_subset_num:eq:@var_subset_num,if_prop=expand_subset_num:eq:@expand_subset_num,;consistent_prop var_subset_num;pheno_variant_subset;project_variant_subset;skip_if or,!num_var_subsets,meta_test_no_subsets bsub_batch 25%\
short;pheno_test;pheno_test_variant_subset;;;pheno_variant_subset;project_variant_subset;skip_if or,!num_var_subsets,!meta_test_no_subsets bsub_batch 25| \
shortt cmd make_pheno_testl_pheno_testi_mantra_in_aux2_file=$smart_join_cmd --in-delim $tab --header 1 --rest-extra 1 --exec "$smart_cut_cmd --in-delim $tab !{input,--file,phenol_clean_gene_variant_file} --select-col 1,1,ID" --exec "$smart_cut_cmd  --in-delim $tab !{input,--file,projectl_vfreq_file} --exact --require-col-match --select-col 1,1,'VAR CHR POS REF ALT' | awk -F\"\\t\" -v OFS=\"\\t\" 'NR > 1 && (length(\\$4) > 1 || length(\\$5) > 1) {\\$4=\"R\"; \\$5=\"X\"} {print}' | $parse_out_id | sed 's/^\(\S\S*\)/\1\t\1\t1\tNA\t\1/'" !{raw,--exec,pheno_testi,"$smart_cut_cmd --in-delim $tab --file *pheno_testi_clean_small_vassoc_file --select-col 1\,1 --select-col 1\,1\,'$neff_col_disp $maf_col_disp $beta_col_disp $se_col_disp $n_col_disp' --require-col-match --exact | awk -F\"\\t\" -v OFS\=\"\\t\" 'NR > 1 && \\$2 \=\= \"NA\" {\\$2\=\\$NF} {print}' | $add_function_cmd --in-delim $tab --header 1 --col1 $n_col_disp --col2 $maf_col_disp --val2 2 --type multiply --val-header MAC | awk -v OFS\=\"\\t\" -F\"\\t\" '{\\$NF\=int(\\$NF+.5)} {print}' | $smart_cut_cmd --in-delim $tab --exclude-col 0\,1\,$n_col_disp --exact | sed 's/^\(\S\S*\)/\1\t@pheno\t\1\t2\t@pheno/'",all_instances=1,exifpropif_prop=project:eq:@project,if_prop=pheno:eq:@meta_trait_inv,if_prop=pheno_test:eq:@meta_test} !{input,pheno_testi_clean_small_vassoc_file,all_instances=1,exifpropif_prop=project:eq:@project,if_prop=pheno:eq:@meta_trait_inv,if_prop=pheno_test:eq:@meta_test,instance_level=pheno_testi} | tail -n+2 !{raw,|,pheno_testi,sed 's/\t@pheno/\n/',all_instances=1,exifpropif_prop=project:eq:@project,if_prop=pheno:eq:@meta_trait_inv,if_prop=pheno_test:eq:@meta_test} | cut -f2- | sort -k1,1 -k2,2n -k3,3 | cut -f2,4- | awk -F"\t" -v OFS="\t" '\$1 == 2 {good=1; for (i=1;i<=NF;i++) {if (\$i !~ /[0-9\-\.e]+/) {good=0}} if (\$6 < !{prop,,pheno_test,min_mantra_mac}) {good=0} if (!good || \$3 == 0) {for (i=2;i<=NF;i++) {\$i=0} {print 0,\$0}} else {print 1,\$0}} \$1 == 1 {\$1=\$2; print}' | cut -f1,3- > !{output,,pheno_testl_in_aux2_file} class_level pheno_testl run_if and,test_software:eq:mantra,meta_trait_inv exskipif excons

local cmd make_pheno_test_mantra_in_aux4_file=awk '{print \$2}' !{input,,pheno_sample_pruned_marker_bim_file} | cat - !{input,,pheno_test_in_aux2_file} | awk -F"\t" -v OFS="\t" 'NF == 5 {v=\$1; print \$1,NR,\$0} NF == 6 {print v,NR,\$0} NF == 1 {print \$1,0}' | sort -k1,1 -k2,2n | awk -F"\t" -v OFS="\t" 'NF == 2 {v=\$1} NF > 2 && v && v == \$1 {print}' | sort -k2,2n | cut -f3- > !{output,,pheno_test_in_aux4_file} class_level pheno_test run_if and,test_software:eq:mantra,meta_trait_inv 

#!|expand%;%shortt;pheno_testi;pheno_testl;exifprop;excons;phenol;projectl;exskipif%\
#;pheno_test;pheno_test;;;project;project;skip_if num_var_subsets%\
#short;pheno_test_variant_subset;pheno_test_variant_subset;if_prop=var_subset_num:eq:@var_subset_num,if_prop=expand_subset_num:eq:@expand_subset_num,;consistent_prop var_subset_num;pheno_variant_subset;project_variant_subset;skip_if or,!num_var_subsets,meta_test_no_subsets%\
#short;pheno_test;pheno_test_variant_subset;;;pheno_variant_subset;project_variant_subset;skip_if or,!num_var_subsets,!meta_test_no_subsets| \
#shortt cmd make_pheno_testl_pheno_testi_mantra_in_aux2_file=$smart_join_cmd --in-delim $tab --header 1 --rest-extra 1 --exec "$smart_cut_cmd --in-delim $tab !{input,--file,phenol_clean_gene_variant_file} --select-col 1,1,ID" --exec "$smart_cut_cmd --in-delim $tab !{input,--file,projectl_vfreq_file} --exact --require-col-match --select-col 1,1,'VAR CHR POS REF ALT' | $parse_out_id | sed 's/^\(\S\S*\)/\1\t\1\t1\tNA\t\1/'" !{raw,--exec,pheno_testi,"$smart_cut_cmd --in-delim $tab --file *pheno_testi_small_vassoc_file --select-col 1\,1 --select-col 1\,1\,'$neff_col_disp $maf_col_disp $beta_col_disp $se_col_disp $n_col_disp' --require-col-match --exact | $add_function_cmd --in-delim $tab --header 1 --col1 $n_col_disp --col2 $maf_col_disp --type multiply --val-header MAC | sed 's/^\(\S\S*\)/\1\t@pheno\t\1\t2\t@pheno/'",all_instances=1,exifpropif_prop=project:eq:@project,if_prop=pheno:eq:@meta_trait_inv,if_prop=pheno_test:eq:@meta_test} !{input,pheno_testi_small_vassoc_file,all_instances=1,exifpropif_prop=project:eq:@project,if_prop=pheno:eq:@meta_trait_inv,if_prop=pheno_test:eq:@meta_test,instance_level=pheno_testi}  > !{output,,pheno_testl_in_aux2_file} class_level pheno_testl run_if and,test_software:eq:mantra,meta_trait_inv exskipif excons


short cmd make_pheno_test_mantra_in_aux3_file=(echo !{input,,pheno_test_in_aux1_file} && echo !{input,,pheno_test_in_aux4_file} && echo !{output,,pheno_test_in_aux3_file}) | $dmatcal_cmd class_level pheno_test run_if and,test_software:eq:mantra,meta_trait_inv

!|expand:,:shortt,pheno_testl,phenol,exskipif:,pheno_test,pheno,num_var_subsets:short,pheno_test_variant_subset,pheno_variant_subset,!num_var_subsets| \
shortt cmd make_pheno_testl_mantra_vassoc_file=(echo !{input,,pheno_test_in_aux1_file} && echo !{input,,pheno_testl_in_aux2_file} && echo !{input,,pheno_test_in_aux3_file} && echo !{output,,pheno_testl_vassoc_file} && echo !{output,,pheno_testl_out_aux1_file} && echo 1) | $mantra_cmd class_level pheno_testl run_if and,test_software:eq:mantra,meta_trait_inv skip_if exskipif

!|expand:,:shortt,pheno_testl,phenol,exskipif:,pheno_test,project,num_var_subsets:short,pheno_test_variant_subset,pheno_variant_subset,!num_var_subsets| \
shortt cmd make_pheno_testl_mantra_small_vassoc_file=ap=`head -n1 !{input,,pheno_testl_vassoc_file} | sed 's/^\S\S*\s\s*//' | sed 's/\S\S*/NA/g'` && $smart_join_cmd --out-delim $tab !{input,--file,pheno_testl_vassoc_file} --exec "$smart_cut_cmd --in-delim $tab !{input,--file,phenol_clean_gene_variant_file} --select-col 1,1,ID --exclude-row 1,1 | sed 's/$/ \$ap/'" --merge | sed 's/\s\s*/\t/g' | sed '1 s/^/$id_col_disp\tCHROM\tPOS\tEFFECT\tOTHER\tSTUDIES\tEAC\tMAC\tEAF\t${bf_col_disp}\t${bf_col_disp}_FE\t${bf_col_disp}_HET\t$n_col_disp\t$dir_col_disp\n/' | $replace_nan(NA) > !{output,,pheno_testl_small_vassoc_file} class_level pheno_testl run_if and,test_software:eq:mantra,meta_trait_inv skip_if exskipif


#METASOFT

local cmd make_pheno_test_metasoft_info_file=$record_pheno_meta_test_info(!{prop;;pheno_test;test_name;sep=\,;all_instances=1;if_prop=pheno_test:eq:@meta_test;if_prop=pheno:eq:@meta_trait_inv;if_prop=project:eq:@project},NA,NA,NA) class_level pheno_test run_if and,test_software:eq:metasoft,meta_trait_inv

short cmd make_pheno_test_metasoft_in_aux1_file=$smart_join_cmd --in-delim $tab !{raw,--exec,pheno_test,"$smart_cut_cmd --in-delim $tab --file *pheno_test_clean_small_vassoc_file --select-col 1\,1 --select-col 1\,1\,'$beta_col_disp $se_col_disp' --require-col-match --exact --exclude-row 1\,1",all_instances=1,if_prop=project:eq:@project,if_prop=pheno:eq:@meta_trait_inv,if_prop=pheno_test:eq:@meta_test} !{input,pheno_test_clean_small_vassoc_file,all_instances=1,if_prop=project:eq:@project,if_prop=pheno:eq:@meta_trait_inv,if_prop=pheno_test:eq:@meta_test} > !{output,,pheno_test_in_aux1_file} class_level pheno_test run_if and,test_software:eq:metasoft,meta_trait_inv

short cmd make_pheno_test_metasoft_vassoc_file=$metasoft_cmd !{input,-input,pheno_test_in_aux1_file} -pvalue_table $metasoft_pvalue_table !{output,-log,pheno_test_out_aux1_file} !{output,-output,pheno_test_vassoc_file} class_level pheno_test run_if and,test_software:eq:metasoft,meta_trait_inv

short cmd make_pheno_test_metasoft_small_vassoc_file=cut -f1-16 !{input,,pheno_test_vassoc_file} | $smart_cut_cmd --in-delim $tab --select-col 0,1 --select-col 0,1,'BETA_RE PVALUE_RE2' | sed '1 s/.*/$id_col_disp\t$beta_col_disp\t$p_col_disp/' | $add_function_cmd --in-delim $tab --col1 $beta_col_disp --header 1 --type exp --val-header $or_col_disp --add-at $beta_col_disp | $replace_nan(NA) > !{output,,pheno_test_small_vassoc_file} class_level pheno_test run_if and,test_software:eq:metasoft,meta_trait_inv skip_if exskipif

#METAL

local cmd make_pheno_test_metal_info_file=$record_pheno_meta_test_info(!{prop;;pheno_test;test_name;sep=\,;all_instances=1;all_instances=1;if_prop=project:eq:@project;if_prop=pheno:eq:@meta_trait_inv;if_prop=pheno_test:eq:@meta_test;limit=1}: !{prop;;pheno_test;meta_trait_inv;sep=\,},NA,NA,NA) class_level pheno_test run_if and,test_software:eq:metal,meta_trait_inv

local cmd make_pheno_test_metal_in_aux1_file=(echo SCHEME !{prop,,pheno_test,metal_scheme} !{raw,,pheno_test,&& echo GENOMICCONTROL ON,if_prop=gc_correct,allow_empty=1} && echo MARKER $id_col_disp && echo PVALUE $p_col_disp && echo WEIGHT $neff_col_disp && echo FREQ $maf_col_disp && echo ALLELE ALT REF && echo AVERAGEFREQ ON && echo MINMAXFREQ ON && echo STDERR $se_col_disp && echo EFFECT $beta_col_disp && echo CUSTOMVARIABLE TotalSampleSize && echo LABEL TotalSampleSize as $n_col_disp && echo CUSTOMVARIABLE TotalEffectiveSampleSize && echo LABEL TotalEffectiveSampleSize as $neff_col_disp && !{raw,,pheno_test,echo PROCESS *pheno_test_clean_small_vassoc_file,sep=&&,all_instances=1,if_prop=project:eq:@project,if_prop=pheno:eq:@meta_trait_inv,if_prop=pheno_test:eq:@meta_test} !{input,pheno_test_clean_small_vassoc_file,all_instances=1,if_prop=project:eq:@project,if_prop=pheno:eq:@meta_trait_inv,if_prop=pheno_test:eq:@meta_test} && echo OUTFILE !{raw,,pheno_test,*pheno_test_vassoc_file} .tbl && echo ANALYZE HETEROGENEITY) > !{output,,pheno_test_in_aux1_file} class_level pheno_test run_if and,test_software:eq:metal,meta_trait_inv 

#local cmd make_pheno_test_metal_in_aux1_file=(echo SCHEME !{prop,,pheno_test,metal_scheme} && echo MARKER $id_col_disp && echo PVALUE $p_col_disp && echo FREQ $maf_col_disp && echo ALLELE ALT REF && echo AVERAGEFREQ ON && echo MINMAXFREQ ON && echo STDERR $se_col_disp && echo EFFECT $beta_col_disp && echo CUSTOMVARIABLE TotalSampleSize && echo LABEL TotalSampleSize as $n_col_disp && !{raw,,pheno_test,echo PROCESS *pheno_test_small_vassoc_file,sep=&&,all_instances=1,if_prop=project:eq:@project,if_prop=pheno:eq:@meta_trait_inv,if_prop=pheno_test:eq:@meta_test} !{input,pheno_test_small_vassoc_file,all_instances=1,if_prop=project:eq:@project,if_prop=pheno:eq:@meta_trait_inv,if_prop=pheno_test:eq:@meta_test} && echo OUTFILE !{raw,,pheno_test,*pheno_test_vassoc_file} .tbl && echo ANALYZE) > !{output,,pheno_test_in_aux1_file} class_level pheno_test run_if and,test_software:eq:metal,meta_trait_inv 

short cmd make_pheno_test_metal_vassoc_file=cat !{input,,pheno_test_in_aux1_file} | $metal_cmd !{output,pheno_test_vassoc_file} !{input,pheno_test_clean_small_vassoc_file,all_instances=1,if_prop=project:eq:@project,if_prop=pheno:eq:@meta_trait_inv,if_prop=pheno_test:eq:@meta_test} > !{output,,pheno_test_out_aux1_file} && mv !{output,,pheno_test_vassoc_file}1.tbl !{output,,pheno_test_vassoc_file} class_level pheno_test run_if and,test_software:eq:metal,meta_trait_inv

short cmd make_pheno_test_metal_small_vassoc_file=$smart_join_cmd --in-delim $tab --header 1 --rest-extra 2  --exec "$parse_ref_min_pheno" --exec "$fill_file_helper(pheno_test_vassoc_file,project_clean_gene_variant_file,NA,1,$clean_gene_variant_id_col,1,1) | perl -lne 'chomp; @a = split(\"\t\"); \\$a[1] =~ tr/[a-z]/[A-Z]/; \\$a[2] =~ tr/[a-z]/[A-Z]/; print join(\"\t\", @a)'" | $smart_cut_cmd --in-delim $tab --select-col 0,1,'VAR Freq1 ALT REF ALLELE1 ALLELE2 TotalSampleSize TotalEffectiveSampleSize Effect Zscore StdErr P-value Direction' --exact | $replace_nan(NA) | awk -F"\t" -v OFS="\t" 'NR > 1 && toupper(\$3) != toupper(\$5) && \$7 != "NA" {\$9 = -\$9; gsub("-","*",\$13); gsub("+","-",\$13); gsub("*","+",\$13)} {print}' | sed '1 s/\tP-value/\t$p_col_disp/' | sed '1 s/\tFreq1/\t$maf_col_disp/' | sed '1 s/\tTotalSampleSize/\t$n_col_disp/' | sed '1 s/\tTotalEffectiveSampleSize/\t$neff_col_disp/' | sed '1 s/\tREF/\t$ref_col_disp/' | sed '1 s/\tALT/\t$alt_col_disp/' | sed '1 s/\tStdErr/\t$se_col_disp/' | sed '1 s/\t!{raw,,pheno_test,Effect,if_prop=metal_scheme:ne:SAMPLESIZE,allow_empty=1}!{raw,,pheno_test,Zscore,if_prop=metal_scheme:eq:SAMPLESIZE,allow_empty=1}/\t$beta_col_disp/' | sed '1 s/VAR/$id_col_disp/' | sed '1 s/Direction/$dir_col_disp/' !{raw;;pheno_test;| $add_function_cmd --in-delim $tab --header 1 --col1 $beta_col_disp --type exp --val-header $or_col_disp;if_prop=metal_scheme:eq:STDERR;allow_empty=1} | $smart_cut_cmd --in-delim $tab --select-col 0,1,'$id_col_disp $ref_col_disp $alt_col_disp $n_col_disp $neff_col_disp $maf_col_disp !{raw,,pheno_test,$or_col_disp $se_col_disp,if_prop=metal_scheme:eq:STDERR,allow_empty=1} $beta_col_disp $p_col_disp $dir_col_disp' --require-col-match --exact > !{output,,pheno_test_small_vassoc_file} class_level pheno_test run_if and,test_software:eq:metal,meta_trait_inv

#END TESTS

prop apply_gc_correction=scalar default 0

short cmd make_pheno_test_clean_include_file=cat !{input,,pheno_vassoc_pre_annot_file} | $apply_vassoc_clean_filters(pheno_test) $extended_qc_filter_helper(pheno_test) > !{output,,pheno_test_clean_include_file} class_level pheno_test skip_if or,not_trait,(and,(or,pheno_qt,min_clean_p_missing:le:0),min_clean_geno:le:0,min_clean_hwe:le:0,!custom_clean_annot_exclude_filters,!extended_clean_annot_exclude_filters,!apply_extended_strict_qc,!apply_extended_qc)

#short cmd make_pheno_test_clean_include_file=$smart_join_cmd --in-delim 1,$tab !{input,--file,pheno_vassoc_pre_annot_file} !{input,--file,pheno_lmiss_file} --header 1 --col 2,2 | $apply_vassoc_clean_filters(pheno_test) $extended_qc_filter_helper(pheno_test) > !{output,,pheno_test_clean_include_file} class_level pheno_test skip_if or,not_trait,(and,(or,pheno_qt,min_clean_p_missing:le:0),min_clean_geno:le:0,min_clean_hwe:le:0,!custom_clean_annot_exclude_filters,!extended_clean_annot_exclude_filters,!apply_extended_strict_qc,!apply_extended_qc)

short cmd make_dummy_pheno_test_clean_include_file=cut -f1 !{input,,pheno_test_small_vassoc_file} | tail -n+2 > !{output,,pheno_test_clean_include_file} class_level pheno_test run_if and,(or,pheno_qt,min_clean_p_missing:le:0),min_clean_geno:le:0,min_clean_hwe:le:0,!custom_clean_annot_exclude_filters,!extended_clean_annot_exclude_filters,!apply_extended_strict_qc,!apply_extended_qc

short cmd make_pheno_test_lambda_file=$smart_join_cmd --exec "awk '{print \\$2}' !{input,,pheno_sample_pruned_marker_bim_file} | cat - !{input,,pheno_test_clean_include_file} | sort | uniq -d | sed '1 s/^/ID\n/'" !{input,--file,pheno_test_small_vassoc_file} --rest-extra 1 --header 1 | $smart_cut_cmd --in-delim $tab --exclude-row 0,1 --select-col 0,1,$p_col_disp | $compute_lambda_cmd | awk '{print \$NF}' > !{output,,pheno_test_lambda_file} class_level pheno_test run_if apply_gc_correction

local cmd make_dummy_pheno_test_lambda_file=echo 1 > !{output,,pheno_test_lambda_file} class_level pheno_test skip_if apply_gc_correction

!|expand@;@shortt;pheno_testl;phenol;projectl;exskipif@;pheno_test;pheno;project;run_if or,!num_var_subsets,test_software:eq:metasoft,test_software:eq:metal@short;pheno_test_variant_subset;pheno_variant_subset;project_variant_subset;run_if and,num_var_subsets,test_software:ne:metasoft,test_software:ne:metal bsub_batch 20| \
short cmd make_pheno_testl_clean_small_vassoc_file=gc=`cat !{input,,pheno_test_lambda_file} | perl -ne 'print \$_ > 1 ? \$_ : 1'` && gc_sqrt=`perl -e "print sqrt(\$gc)"` && $smart_join_cmd --in-delim $tab --exec "tail -n+2 !{input,,pheno_testl_small_vassoc_file} | cut -f1 | cat - !{input,,pheno_test_clean_include_file} | sort | uniq -d | sed '1 s/^/ID\n/'" !{input,--file,pheno_testl_small_vassoc_file} --header 1 --rest-extra 1 | $add_function_cmd --in-delim $tab --header 1  --col1 $p_col_disp --val2 \$gc --type gc_correct --val-header P_ADJ --add-at $p_col_disp | $add_function_cmd --in-delim $tab --header 1  --col1 $se_col_disp --val2 \$gc_sqrt --type multiply --val-header SE_ADJ --add-at $se_col_disp | $smart_cut_cmd --in-delim $tab --exclude-col 0,1,'$p_col_disp $se_col_disp' --exact --empty-okay | sed '1 s/P_ADJ/$p_col_disp/' | sed '1 s/SE_ADJ/$se_col_disp/' > !{output,,pheno_testl_clean_small_vassoc_file} class_level pheno_testl skip_if or,not_trait,(and,(or,pheno_qt,min_clean_p_missing:le:0),min_clean_geno:le:0,min_clean_hwe:le:0,!custom_clean_annot_exclude_filters,!apply_gc_correction,!apply_extended_strict_qc,!apply_extended_qc) exskipif 

!|expand@;@shortt;pheno_testl;phenol;projectl;exskipif@;pheno_test;pheno;project;skip_if and,num_var_subsets,test_software:ne:metasoft,test_software:ne:metal@short;pheno_test_variant_subset;pheno_variant_subset;project_variant_subset;skip_if or,!num_var_subsets,test_software:eq:metasoft,test_software:eq:metal| \
local cmd ln_pheno_testl_clean_small_vassoc_file=ln -s !{input,,pheno_testl_small_vassoc_file} !{output,,pheno_testl_clean_small_vassoc_file} class_level pheno_testl run_if and,(or,pheno_qt,min_clean_p_missing:le:0),min_clean_geno:le:0,min_clean_hwe:le:0,!custom_clean_annot_exclude_filters,!extended_clean_annot_exclude_filters,!apply_gc_correction,!apply_extended_strict_qc,!apply_extended_qc exskipif

prop include_dp=scalar default 1

dp_helper=!{raw,,pheno,DP,if_prop=include_dp,allow_empty=1}

prop vassoc_extra_vstats_annot=list default "MQ0"
vassoc_vstats_annot=!{raw,,pheno,HET_AB,unless_prop=no_sample_ab,allow_empty=1} $dp_helper !{prop,,pheno,vassoc_extra_vstats_annot,if_prop=vassoc_extra_vstats_annot,allow_empty=1}

#convert_pph_synonymous_to_na=awk -v OFS="\t" -F "\t" 'NR == 1 {for (i=1;i<=NF;i++) {map[\$i]=i}} NR != 1 && \$map["$pph2_class_annot"] == $pph2_class_synonymous && \$map["$vcf_type_annot"] != "$vcf_type_synonymous_annot" {\$map["$pph2_class_annot"] = "NA"} {print}'

#RIGHT NOW MAY NEED TO ADD THIS TO THE EXECS; SHOULD BE ABLE TO REMOVE ONCE --remove-dup-snps is run
undupify=#| sed 's/^\(\S\S*\)\.[0-9]*\(\s\)/\1\2/' | awk '!a[\\$1] {print} {a[\\$1] = 1}'

glm_max_se=5000
glm_max_or=25000

!!expand:clean_:clean_::! \
local cmd make_pheno_variant_subset_clean_gene_variant_file=rm -f !{output,,pheno_variant_subset_clean_gene_variant_file} && $smart_join_cmd --exec "sed '1 s/^/ID\n/' !{input,,pheno_variant_subset_var_keep_file}" !{input,--file,project_variant_subset_clean_gene_variant_file} --header 1 --in-delim $tab --col 2,4 --extra 2 | awk -F"\t" -v OFS="\t" '{print \$2,\$3,\$4,\$1}' > !{output,,pheno_variant_subset_clean_gene_variant_file} class_level pheno_variant_subset

#helpers for vassoc annot
!|expand:,:phenol,projectl:pheno,project:pheno_variant_subset,pheno_variant_subset| \
get_phenol_clean_gene_variants=$smart_cut_cmd --in-delim $tab !{input,--file,projectl_clean_gene_variant_file} --select-col 1,1,ID --select-col 1,1,'GENE CHROM POS' $undupify --require-col-match --exact

!|expand:,:phenol,projectl:pheno,project:pheno_variant_subset,pheno_variant_subset| \
get_phenol_closest_gene=$smart_cut_cmd --in-delim $tab !{input,--file,projectl_clean_gene_variant_file} --select-col 1,1,ID --select-col 1,1,'ID CHROM POS' $undupify --require-col-match --exact | $add_gene_annot_cmd --keep-outside --header 1 --in-delim $tab --chr-col 2 --pos-col 3 --gene-file !{input,,project_closest_gene_file} --out-delim $tab | $smart_cut_cmd --in-delim $tab --select-col 0,'2 1' | sed '1 s/.*/ID\tCLOSEST_GENE/'

prop extra_vassoc_meta_annot=list

vassoc_meta_fields=$vcf_type_annot $vcf_protein_change_annot !{prop,,pheno,vassoc_meta_annot} !{prop,,pheno,extra_vassoc_meta_annot,if_prop=extra_vassoc_meta_annot,allow_empty=1}

ucfirst_helper=perl -F\\\\t -lane 'if (\\$. > 1) {for (\\$r=1; \\$r<=\\$\\#F; \\$r++) {next if \\$F[\\$r] eq \"$annot_missing_field\"; \\$F[\\$r] =~ tr/[A-Z]/[a-z]/; \\$F[\\$r] = ucfirst(\\$F[\\$r]); \\$F[\\$r] =~ tr/_/ /;}} print join(\"\\t\", @F)'



!|expand:,:phenol,projectl,projectt:pheno,project,project:pheno_variant_subset,project_variant_subset,pheno_variant_subset| \
get_phenol_variant_meta_info=$fill_file_helper(projectl_full_var_annot_file,project_clean_gene_variant_file,$vep_missing_field,1,$clean_gene_variant_id_col,1,1) | $smart_cut_cmd --in-delim $tab --select-col 0,1 --select-col 0,1,'$vassoc_meta_fields' --exact --require-col-match 

!|expand:,:phenol,projectl:pheno,project:pheno_variant_subset,project_variant_subset| \
get_phenol_variant_vstats_meta_info=$smart_cut_cmd --in-delim $vstats_delim --out-delim $tab !{input,--file,projectl_vstats_file} --select-col 1,1,'VAR $vassoc_vstats_annot GENO' --exact --require-col-match $undupify

#!|expand:,:phenol,projectl,phenog:pheno,project,project:pheno_variant_subset,project_variant_subset,pheno_variant_subset| \
#phenol_vstats_meta_cut=$smart_cut_cmd --in-delim $vstats_delim --out-delim $tab !{input;--file;projectl_vstats_file} --select-col 1,1,'VAR $vassoc_vstats_annot GENO' --exact --require-col-match $undupify
#!|expand:,:phenol,projectl,phenog:pheno,project,project:pheno_variant_subset,project_variant_subset,pheno_variant_subset| \
#get_phenol_variant_vstats_meta_info=$fill_file_helper_int(--exec \\"$phenol_vstats_meta_cut\\",$phenol_vstats_meta_cut,!{input::phenog_clean_gene_variant_file},NA,1,4,1,1,\,)

!|expand:,:phenol,projectl:pheno,project:pheno_variant_subset,project_variant_subset| \
get_phenol_var_distance_meta_info=$pseq_qc_plus_all_analysis_cmd_phenol(write-vcf) | cut -f1-3 | grep -v \\\# | awk -v OFS=\"\t\" 'END {print prevv,prevd} {curc = \\$1; curd = \"\"} length(prevp) && curc == prevc {curd = \\$2 - prevp} length(prevd) && length(curd) && curd < prevd {print prevv,curd } length(prevd) && length(curd) && curd >= prevd {print prevv,prevd } length(prevd) && length(curd) == 0 {print prevv,prevd} length(curd) && length(prevd) == 0 {print prevv,curd} prevv && length(curd) == 0 && length(prevd) == 0 {print prevv} { prevd = curd; } {prevp = \\$2; prevc = \\$1; prevv = \\$3;}' | awk 'NF == 1 {print \\$1\"\tNA\"} NF == 2 {print}' | sed '1 s/^/ID\tSNP_DIST\n/'

!|expand:,:phenol,projectl:pheno,project:pheno_variant_subset,project_variant_subset| \
get_phenol_missing_meta_info=$smart_join_cmd --in-delim $tab --exec \"$smart_cut_cmd --in-delim $tab !{input,--file,phenol_multiallelic_frq_file} --select-col 1,1,'id call_rate' --exact | awk -v OFS=\\\"\t\\\" 'NR > 1 {\\\\$2 = 1 - \\\\$2; print}' | sed '1 s/.*/SNP\tF_MISS/'\" --exec \"$smart_cut_cmd !{input,--file,phenol_lmiss_file} --select-col 1,1,'SNP F_MISS' --exact --out-delim $tab\" --header 1 --merge $undupify

!|expand:,:phenol,projectl:pheno,project:pheno_variant_subset,project_variant_subset| \
get_phenol_p_missing_meta_info=$smart_join_cmd --in-delim $tab --exec \"$smart_cut_cmd --in-delim $tab !{input,--file,phenol_multiallelic_group_frq_file} --select-col 1,1,'id pval' --exact | sed 's/\t/\tNA\tNA\t/'\" --exec \"$smart_cut_cmd !{input,--file,phenol_test_missing_file} --select-col 1,1,'SNP F_MISS_A F_MISS_U P' --exact --out-delim $tab\" --merge --header 1 | sed '1 s/.*/SNP\tF_MISSA\tF_MISSU\tP_MISSING/' $undupify


#get_phenol_p_missing_meta_info=$smart_cut_cmd !{input,--file,phenol_test_missing_file} --select-col 1,1,'SNP F_MISS_A F_MISS_U P' --exact --out-delim $tab | sed '1 s/.*/SNP\tF_MISSA\tF_MISSU\tP_MISSING/' $undupify

!|expand:,:phenol,projectl:pheno,project:pheno_variant_subset,project_variant_subset| \
get_phenol_hwe_meta_info=$smart_join_cmd --exec \"awk '\\\\$3 == \\\"AFF\\\" {print \\\\$2,\\\\$NF}' !{input,,phenol_hwe_file}\" --exec \"awk '\\\\$3 == \\\"UNAFF\\\" {print \\\\$2,\\\\$NF}' !{input,,phenol_hwe_file}\" --out-delim $tab --header 1 | sed '1 s/^/SNP\tHWEA\tHWEU\n/' $undupify

add_mafa_mafu=$add_function_cmd --in-delim $tab --header 1 --col1 OBSA --val2 2 --type multiply --val-header OBSA2 | $add_function_cmd --in-delim $tab --header 1 --col1 OBSU --val2 2 --type multiply --val-header OBSU2 | $add_function_cmd --in-delim $tab --header 1 --col1 MINA --col2 OBSA2 --type divide --val-header MAFA --add-after MAF | $add_function_cmd --in-delim $tab --header 1 --col1 MINU --col2 OBSU2 --type divide --val-header MAFU --add-after MAFA | $smart_cut_cmd --in-delim $tab --exclude-col 0,1,'OBSA2 OBSU2' | $format_columns_cmd --in-delim $tab --header 1 --number-format MAFA,%.4g --number-format MAFU,%.4g

!|expand:,:phenol,projectl:pheno,project:pheno_variant_subset,project_variant_subset| \
get_phenol_vassoc_count_info=$smart_join_cmd --in-delim $tab --exec \"$smart_join_cmd --in-delim $tab --header 1 --rest-extra 3 --fill 1 --fill 2 --exec \\\"zcat !{input,,projectl_multiallelic_vcf_file} | fgrep -v \\\\\\\# | cut -f3,4,5 | sed '1 s/.*/VAR\tREF\tALT\tSAMPLES\tFILTER/' | sed '1! s/$/\tNA\tNA/'\\\" --exec \\\"$smart_cut_cmd --in-delim $tab !{input,--file,phenol_multiallelic_frq_file} --select-col 1,1,'id aaf pval' --exact | sed '1 s/.*/VAR\tMAF\tHWE/'\\\" --exec \\\"$smart_cut_cmd --in-delim $tab !{input,--file,phenol_multiallelic_group_frq_file} --select-col 1,1,'id mina minu obsa obsu' --exact | sed '1 s/.*/VAR\tMINA\tMINU\tOBSA\tOBSU\tREFA\tHETA\tHOMA\tREFU\tHETU\tHOMU\tP\tOR/' | sed '1! s/$/\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA/' | $neff_helper\\\"\" --exec \"$smart_cut_cmd --in-delim $tab !{input,--file,phenol_vassoc_counts_file} --select-col 1,1,'VAR REF ALT SAMPLES FILTER MAF HWE MINA MINU NEFF OBSA OBSU REFA HETA HOMA REFU HETU HOMU P OR'  --exact | $parse_out_id\" --header 1 --merge | $add_mafa_mafu $undupify | $add_fisher(P) | $add_fisher(OR)

#KEEP THIS AS EXAMPLE FOR HOW TO MERGE ADD WITH DOM
#min_add_mac=5
#first take common ADD, then DOM, then any SNPs not present in DOM
!|expand:,:phenol,projectl:pheno,project:pheno_variant_subset,project_variant_subset| \
#get_phenol_glm_info=$smart_join_cmd --in-delim $tab --header 1 --merge --exec \"cat !{input,,@1} | $smart_cut_cmd --in-delim $tab --select-col 0,1,'VAR N F BETA SE STAT P' --exact | awk -F\\\"\t\\\" -v OFS=\\\"\t\\\" 'NR > 1 && \\\\$3 > .5 {\\\\$3 = 1 - \\\\$3} {print}' | $add_function_cmd --in-delim $tab --col1 N --col2 F --val2 2 --type multiply --header 1 --val-header MAC | $smart_cut_cmd --in-delim $tab --select-col 0,1,'VAR N BETA SE STAT P' --exact --select-row 0,1 --select-row 0,1,MAC,ge:$min_add_mac | $parse_out_id $undupify | sed '1! s/$/\tADD/' | sed '1 s/$/\tTEST/'\" --exec \"$smart_join_cmd --in-delim $tab --exec \\\"$parse_ref_min_phenol\\\" --exec \\\"$smart_cut_cmd --out-delim $tab !{input,--file,@2} --select-col 1,1,'SNP NMISS BETA STAT P TEST' --select-row 1,1 --select-row 1,1,TEST,DOM --exact | $add_function_cmd --in-delim $tab --header 1 --col1 BETA --col2 STAT --type divide --val-header SE --add-at STAT\\\" --extra 1 --header 1 | $smart_cut_cmd --in-delim $tab --select-col 0,1,'REFMIN VAR NMISS BETA SE STAT P TEST' | awk -F\\\"\t\\\" -v OFS=\\\"\t\\\" 'NR > 1 && \\\\$1 == 1 {\\\\$4 = -\\\\$4; \\\\$6 = -\\\\$6} {print}' | cut -f2-\"  --exec \"cat !{input,,@1} | $smart_cut_cmd --in-delim $tab --select-col 0,1,'VAR N BETA SE STAT P' --exact | $parse_out_id $undupify | sed '1! s/$/\tADD/' | sed '1 s/$/\tTEST/'\" 

#KEEP THESE AS AN EXAMPLE AS TO HOW TO MERGE FISHER WITH GLM
#convert_beta_to_or=| $add_function_cmd --in-delim $tab --header 1 --col1 beta --type exp --val-header OR | $smart_cut_cmd --in-delim $tab --exclude-col 0:1:beta --exact --arg-delim :
#!!expand:,:ftype,fname,varname,pname,sename,orname,naname,addfunctioncmd:\
#glm,phenol_raw_glm_file,VAR,P,SE,OR,NA,:\
#score,phenol_raw_score_file,ID,PrChi,SEbeta,beta,NaN,$convert_beta_to_or! \
#!|expand:,:phenol,projectl:pheno,project:pheno_variant_subset,project_variant_subset| \
#merge_phenol_raw_ftype_vassoc_p=$smart_join_cmd --exec \"$smart_cut_cmd !{input,--file,fname} --in-delim $tab --select-col 1,1,'varname pname sename orname' --select-row 1,1 --select-row 1,1,pname,ne:naname --select-row 1,1,sename,le:$glm_max_se --and-row-all --exact --require-col-match addfunctioncmd | $smart_cut_cmd --in-delim $tab --select-col 0,1,'varname pname OR' | sed 1's/.*/VAR P OR/' | $add_p_type_col($used_logistic_p)\" --exec \"$smart_cut_cmd !{input,--file,phenol_strata_vassoc_file} --in-delim $tab --select-col 1,1,'VAR P OR' --exact --require-col-match | $add_p_type_col($used_pseq_p)\" --merge --header 1 | $parse_out_id | $add_raw(P) | $add_raw(OR) | $add_raw($p_type_col)

"""
}
    