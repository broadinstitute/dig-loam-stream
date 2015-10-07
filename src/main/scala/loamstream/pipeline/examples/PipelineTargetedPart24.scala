
package loamstream.pipeline.examples

/**
 * LoamStream
 * Created by oliverr on 10/6/2015.
 */
object PipelineTargetedPart24 {
  val string =
 """#!!expand:,:ftype,fname,fexname,varname,pname,sename,orname,naname,addfunctioncmd:\
#glm,phenol_glm_file,phenol_ex_glm_file,VAR,P,SE,OR,NA,:\
#score,phenol_score_file,phenol_ex_score_file,ID,PrChi,SEbeta,beta,NaN,$convert_beta_to_or! \
#!|expand:,:phenol,projectl:pheno,project:pheno_variant_subset,project_variant_subset| \
#merge_phenol_strata_ftype_vassoc_p=$smart_join_cmd --exec \"$smart_cut_cmd --in-delim $tab --exec \\\"$smart_join_cmd --in-delim $tab --exec \\\\\\\"$pseq_qc_plus_analysis_cmd_phenol_covar($vassoc) $show_id --perm 1 $qt_plinkseq_phenotype_selector | $smart_cut_cmd --in-delim $tab --require-col-match --select-col 0,1,'VAR MINA MINU OBSA OBSU'\\\\\\\" !{input,--file,fname,unless_prop=pheno_qt,allow_empty=1} !{input,--file,fexname,if_prop=pheno_qt,allow_empty=1} --in-delim $tab --header 1 --rest-extra 1\\\" --select-col 1,1,'VAR MINA MINU OBSA OBSU pname sename orname' --select-row 1,1 --select-row 1,1,sename,le:$glm_max_se --select-row 1,1,pname,ne:NaN --and-row-all --exact --require-col-match addfunctioncmd | $smart_cut_cmd --in-delim $tab --exclude-col 0,1,sename --exact | sed 's/\(\s\s*\)pname/\1P/' | sed 's/\(\s\s*\)orname/\1OR/' | $add_p_type_col($used_logistic_p)\" --exec \"$smart_cut_cmd !{input,--file,phenol_strata_vassoc_file} --in-delim $tab --select-col 1,1,'VAR MINA MINU OBSA OBSU P OR' --exact --require-col-match | $add_p_type_col($used_pseq_p)\" --merge --header 1 | $parse_out_id | $adjust_strata_vassoc_p

add_tag=sed '1 s/\(\s\s*\|^\)\(@1\)\(\s\s*\|$\)/\1\2_@2\3/g'
add_glm=$add_tag(@1,GLM)
add_raw=$add_tag(@1,RAW)
add_fisher=$add_tag(@1,FISH)
add_adj=$add_tag(@1,ADJ)

or_col_disp_int=!{raw::@1:${or_col_disp}@2:unless_prop=pheno_qt:allow_empty=1@3}!{raw::@1:${beta_col_disp}@2:if_prop=pheno_qt:allow_empty=1@4}
or_col_disp_pheno=$or_col_disp_int(pheno,,,)
or_col_disp_pheno_test=$or_col_disp_int(pheno_test,_\@test_tag,if_prop=use_for_display:if_prop=use_for_or:limit=1,if_prop=use_for_display:limit=1)

!!expand:,:keytype,colname:non_qt,or_col_disp:qt,beta_col_disp! \
!!expand:pheno_testl:pheno_test:pheno_test_variant_subset! \
get_pheno_testl_keytype_small_vassoc_for_annot=!{raw,,pheno_testl,--exec "$smart_cut_cmd --in-delim $tab --file *pheno_testl_small_vassoc_file --select-col 1\,1 --exact --select-col 1\,1\,'$dir_col_disp $colname $bf_col_disp $p_col_disp' | sed '1 s/\(\s\s*\)\(\S\S*\)/\1\2_\@test_tag/g' ",sort_prop=pheno_test,@1} !{input,pheno_testl_small_vassoc_file,@1}

!|expand:,:keyname,file1,file2,runif\
:non_qt,--exec "$get_phenol_hwe_meta_info",--exec "$get_phenol_p_missing_meta_info",run_if !pheno_qt\
:qt,,,run_if pheno_qt| \
!|expand@;@shortt;phenol;pheno_testl;exskipif@;pheno;pheno_test;num_var_subsets@short;pheno_variant_subset;pheno_test_variant_subset;!num_var_subsets| \
shortt cmd make_phenol_keyname_vassoc_pre_annot_file=$smart_join_cmd \
 --exec "$get_phenol_clean_gene_variants" \
 --exec "$get_phenol_closest_gene" \
 --exec "$get_phenol_variant_meta_info" \
 --exec "$get_phenol_variant_vstats_meta_info" --fill 4 \
 --exec "$get_phenol_var_distance_meta_info" \
 --exec "$get_phenol_missing_meta_info" \
 file1 \
 file2 \
 --exec "$get_phenol_vassoc_count_info" \
 --rest-extra 1 --header 1 --in-delim $tab --ignore-err $plinkseq_okay_err \
 > !{output,,phenol_vassoc_pre_annot_file} !{input,pheno_is_trait_file} class_level phenol runif skip_if or,not_trait,exskipif runif with pheno_testl

!|expand:,:keyname,runif\
:non_qt,run_if !pheno_qt\
:qt,run_if pheno_qt| \
!|expand@%@shortt%phenol%pheno_testl%file1%exskipif@%pheno%pheno_test%$get_pheno_test_keyname_small_vassoc_for_annot(unless_prop=num_var_subsets\,if_prop=test_software:eq:metasoft\,if_prop=test_software:eq:metal\,or_if_prop=1\,allow_empty=1)%pheno_test;num_var_subsets@short%pheno_variant_subset%pheno_test_variant_subset%%!pheno_test;num_var_subsets bsub_batch 10| \
shortt cmd make_phenol_keyname_vassoc_annot_file=$smart_join_cmd \
 !{input,--file,phenol_vassoc_pre_annot_file} \
 $get_pheno_testl_keyname_small_vassoc_for_annot(if_prop=num_var_subsets\,unless_prop=test_software:eq:metal\,unless_prop=test_software:eq:metasoft\,allow_empty=1) \
 file1 \
 --rest-extra 1 --header 1 --in-delim $tab \
 | sed '1 s/^\S\S*/$id_col_disp/' \
 | $add_function_cmd --in-delim $tab --header 1 --col1 MINA --col2 MINU --type add --val-header MAC --add-at MINA \
 > !{output,,phenol_vassoc_annot_file} !{input,pheno_is_trait_file} class_level phenol runif skip_if or,not_trait,exskipif runif with pheno_testl


!|expand:,:keyname,skipif\
:non_qt,skip_if pheno_qt\
:qt,skip_if !pheno_qt| \
short cmd make_cat_pheno_keyname_vassoc_annot_file=$smart_join_cmd --in-delim $tab --header 1 --rest-extra 1 --exec "(head -qn+1 !{input,,pheno_variant_subset_vassoc_annot_file,limit=1,sort_prop=pheno_variant_subset} | head -n1 && tail -qn+2 !{input,,pheno_variant_subset_vassoc_annot_file,sort_prop=pheno_variant_subset}) $fix_overlap_bug(1)"  $get_pheno_test_keyname_small_vassoc_for_annot(unless_prop=num_var_subsets\,if_prop=test_software:eq:metal\,if_prop=test_software:eq:metasoft\,or_if_prop=1\,allow_empty=1) > !{output,,pheno_vassoc_annot_file} class_level pheno run_if and,!not_trait,num_var_subsets,pheno_test;num_var_subsets skipif with pheno_test 


###JASON
#LIST TO CAT
#pheno_test_missing_file
#pheno_hwe_file
#pheno_raw_vassoc_file
#pheno_strata_vassoc_file
#pheno_raw_glm_file
#pheno_plink_glm_file
#pneno_plink_ex_glm_file
#pheno_dom_glm_file
#pheno_glm_file
#pheno_ex_glm_file
#pheno_vassoc_annot_file

short cmd make_pheno_small_vassoc_annot_file=$smart_cut_cmd --in-delim $tab !{input,--file,pheno_vassoc_annot_file} --exact --require-col-match --select-col 1,1,'ID GENE CLOSEST_GENE CHROM POS $vassoc_meta_fields MAF MAC MAFA MAFU OBSA OBSU MINA MINU $or_col_disp_pheno_test !{raw::pheno_test:${p_col_disp}_@test_tag:limit=1}' > !{output,,pheno_small_vassoc_annot_file} class_level pheno skip_if not_trait with pheno_test

local cmd ln_vassoc_clean_annot_file=rm -f !{output,,pheno_vassoc_clean_annot_file} && ln -s !{input,,pheno_vassoc_annot_file} !{output,,pheno_vassoc_clean_annot_file} class_level pheno run_if and,(or,pheno_qt,min_clean_p_missing:le:0),min_clean_geno:le:0,min_clean_hwe:le:0,!custom_clean_annot_exclude_filters,!extended_clean_annot_exclude_filters with pheno_test

prop custom_clean_annot_exclude_filters=list
prop extended_clean_annot_exclude_filters=list

apply_custom_clean_annot_filters=!{raw::@1:| eval `perl -e 'print qq(\@{@2})' | sed 's/\s\s*/\n/g' | sed 's/\($select_and_delim\|^\)/ --exclude-row @3,1,/g' | sed 's/$select_filter_delim/,/g' | sed 's;^;$smart_cut_cmd @4 @5 --tab-delim --and-row-all ;g' | tr '\n' '|' | sed 's/|$//'` @6:if_prop=@2:allow_empty=1}

apply_vassoc_clean_filters=$smart_cut_cmd --select-row 0,1 !{raw;;@1;--select-row 0,1,P_MISSING,ge:\@min_clean_p_missing;unless_prop=pheno_qt;allow_empty=1} --select-row 0,1,F_MISS,lt:`perl -e 'print 1-!{prop,,@1,min_clean_geno}'` --select-row 0,1,HWE,ge:!{prop,,@1,min_clean_hwe} --and-row-all --in-delim $tab $apply_custom_clean_annot_filters(@1,custom_clean_annot_exclude_filters,0,,,) | cut -f1 | tail -n+2 $apply_custom_clean_annot_filters(@1,extended_clean_annot_exclude_filters,1,--file *project_extended_vstats_file,--in-delim \,, | cut -f1 | sort | uniq -d) !{input;project_extended_vstats_file;if_prop=extended_clean_annot_exclude_filters;allow_empty=1}

short cmd make_pheno_vassoc_clean_include_file=cat !{input,,pheno_vassoc_pre_annot_file} | $apply_vassoc_clean_filters(pheno) $extended_qc_filter_helper(pheno) > !{output,,pheno_vassoc_clean_include_file} class_level pheno skip_if or,not_trait,(and,(or,pheno_qt,min_clean_p_missing:le:0),min_clean_geno:le:0,min_clean_hwe:le:0,!custom_clean_annot_exclude_filters,!extended_clean_annot_exclude_filters) with pheno_test

#short cmd make_pheno_vassoc_clean_include_file=$smart_join_cmd --in-delim 1,$tab !{input,--file,pheno_vassoc_pre_annot_file} !{input,--file,pheno_lmiss_file} --header 1 --col 2,2 | $apply_vassoc_clean_filters(pheno) $extended_qc_filter_helper(pheno) > !{output,,pheno_vassoc_clean_include_file} class_level pheno skip_if or,not_trait,(and,(or,pheno_qt,min_clean_p_missing:le:0),min_clean_geno:le:0,min_clean_hwe:le:0,!custom_clean_annot_exclude_filters,!extended_clean_annot_exclude_filters)

short cmd make_pheno_vassoc_clean_annot_file=$smart_join_cmd --in-delim $tab --exec "sed '1 s/^/ID\n/' !{input,,pheno_vassoc_clean_include_file}" !{input,--file,pheno_vassoc_annot_file} --header 1 --rest-extra 1 > !{output,,pheno_vassoc_clean_annot_file} class_level pheno skip_if or,not_trait,(and,(or,pheno_qt,min_clean_p_missing:le:0),min_clean_geno:le:0,min_clean_hwe:le:0,!custom_clean_annot_exclude_filters) with pheno_test

pheno_all_trait_vassoc_helper=$smart_join_cmd --file *pheno_small_vassoc_annot_file --exec \"cat *pheno_vassoc_counts_file | $parse_out_id | $smart_cut_cmd --tab-delim --exclude-col 0,1,'`head -n1 *pheno_small_vassoc_annot_file | cut -f2-`'\" --in-delim $tab --header 1 --extra 2 | $smart_cut_cmd --in-delim $tab --select-col 0,1,'CHROM POS @1 `head -n1 *pheno_small_vassoc_annot_file | rev | cut -f1-2 | rev`' | sed '1 s/\(\S\S*\)/\1_\@disp/g' 

!|expand%;%ctype;extracols;selector;runif%all;;pheno:eq:@related_traits;run_if related_traits%meta;MAF MAC MINA MINU REFA HETA HOMA REFU HETU HOMU;pheno:eq:@meta_trait_inv;run_if meta_trait_inv| \
short cmd make_pheno_ctype_trait_vassoc_annot_file=$smart_join_cmd --in-delim $tab --header 1 --exec "cat !{input,,pheno_small_vassoc_annot_file} | $add_tag($or_col_disp_pheno_test,!{prop\,\,pheno\,disp}) | $add_tag(!{raw::pheno_test:${p_col_disp}_@test_tag:limit=1},!{prop\,\,pheno\,disp}) | $smart_cut_cmd --in-delim $tab --select-col 0,1,'^CHROM$ ^POS$ .'" !{raw;--exec;pheno;"$pheno_all_trait_vassoc_helper(extracols)";all_instances=1;if_prop=selector;if_prop=project:eq:@project} !{input,pheno_small_vassoc_annot_file,all_instances=1,if_prop=selector,if_prop=project:eq:@project} !{input,pheno_vassoc_counts_file,all_instances=1,if_prop=selector,if_prop=project:eq:@project} --col 1 --col 2 --rest-extra 1 | cut -f3- > !{output,,pheno_ctype_trait_vassoc_annot_file} class_level pheno runif rusage_mod $trait_vassoc_annot_mem

fix_overlap_bug=#| awk -F"\t" '!m[\$@1] {m[\$@1]=1; print}'

!|expand:;:vtype;exskipif;exrunif\
:plinkseq_qc_pass_gstats_file;skip_if !do_pheno_gstats;\
:plinkseq_qc_pass_estats_file;skip_if !do_pheno_gstats;\
:plinkseq_qc_pass_ns_gstats_file;skip_if !do_pheno_gstats;\
:plinkseq_qc_pass_ns_estats_file;skip_if !do_pheno_gstats;\
:plinkseq_qc_pass_syn_gstats_file;skip_if !do_pheno_gstats;\
:plinkseq_qc_pass_syn_estats_file;skip_if !do_pheno_gstats;\
:counts_file;;\
:test_missing_file;;\
:hwe_file;;\
:male_hwe_file;;\
:female_hwe_file;;\
:lmiss_file;;\
:frq_file;;\
:sex_frq_file;;\
:multiallelic_frq_file;;\
:multiallelic_group_frq_file;;\
:combined_frq_file;;\
:vassoc_counts_file;;\
:vassoc_pre_annot_file;;\
|\
local cmd make_cat_pheno_vtype=(head -qn+1 !{input,,pheno_variant_subset_vtype,limit=1} | head -n1 && tail -qn+2 !{input,,pheno_variant_subset_vtype}) $fix_overlap_bug(1) !{input,pheno_is_trait_file} > !{output,,pheno_vtype} class_level pheno run_if and,!not_trait,num_var_subsetsexrunif exskipif

!|expand:;:vtype;exskipif\
:variant_qc_strata_vstats_summary_file;\
:variant_qc_pheno_strata_vstats_summary_file;\
|\
local cmd make_cat_pheno_vtype=(head -qn+1 !{input,,pheno_variant_subset_vtype,limit=1} | head -n1 && tail -qn+2 !{input,,pheno_variant_subset_vtype}) $fix_overlap_bug(1) > !{output,,pheno_vtype} class_level pheno run_if num_var_subsets exskipif

!|expand:;:itype;exskipif;exrunif\
:mds_file;;,!pheno_custom_mds_file,recompute_pca,parallelize_pca\
:all_popgen_istats_file;;,parallelize_genome\
:unrelated_popgen_istats_file;;,parallelize_genome\
|\
local cmd make_cat_pheno_itype=(head -qn+1 !{input,,pheno_sample_subset_itype} | tail -n1 && tail -qn+2 !{input,,pheno_sample_subset_itype}) !{input,pheno_is_trait_file} > !{output,,pheno_itype} class_level pheno run_if and,!not_trait,num_samp_subsetsexrunif exskipif


!|expand@;@vtype;exskipif;exrunif\
@vassoc_file;skip_if or,test_software:eq:metal,test_software:eq:metasoft,test_software:eq:mantra;\
@small_vassoc_file;;\
@clean_small_vassoc_file;;\
|\
local cmd make_cat_pheno_test_vtype=(head -qn+1 !{input,,pheno_test_variant_subset_vtype,limit=1} | head -n1 && tail -qn+2 !{input,,pheno_test_variant_subset_vtype}) $fix_overlap_bug(1) !{input,pheno_is_trait_file} > !{output,,pheno_test_vtype} !{input,pheno_is_trait_file} class_level pheno_test run_if and,!not_trait,num_var_subsets,!test_software:eq:metasoft,!test_software:eq:metalexrunif exskipif

!|expand@;@vtype;exskipif;exrunif\
@in_aux2_file;skip_if test_software:ne:mantra;\
@vassoc_file;skip_if test_software:ne:mantra;\
|\
local cmd make_cat_no_head_pheno_test_vtype=cat !{input,,pheno_test_variant_subset_vtype} > !{output,,pheno_test_vtype} !{input,pheno_is_trait_file} class_level pheno_test run_if and,!not_trait,num_var_subsetsexrunif exskipif


sort_name_col_int=awk -F$tab -v OFS=$tab 'NR == 1 {print 1,0,\$0; for (i=1;i<=NF;i++) {m[\$i]=i}} NR > 1 {print @3,\$m["@1"],\$0}' | sort -t$tab -k1,1n -k2,2g@2 | cut -f3-
sort_name_col_with_header=$sort_name_col_int(@1,@2,2)
sort_name_col_no_header=$sort_name_col_int(@1,@2,1)

pheno_process_slide_variants_helper=$smart_cut_cmd --in-delim $tab --select-row 0,1 --select-row 0,1,MAFU,@4 --select-row 0,1,MAFA,@4 | $smart_cut_cmd --in-delim $tab --exact --select-col 0,1,'@1' --require-col-match | $smart_cut_cmd --in-delim $tab --exclude-row 0,1,'@3','^(NA|na|NAN|NaN|nan)$' | $sort_name_col_with_header(@3,@2)


vassoc_max_chars_per_col=20
truncate_cols=perl -lne 'chomp; \@F = split("\t"); foreach (\@F) {if (length > @1) {\$_ = substr(\$_, 0, @1) . "..."}} print join("\t", \@F)'


internal_input_headers=CLOSEST_GENE ID MINA MINU MAFA MAFU $or_col_disp_pheno_test $dp_helper GENO HWE P_MISSING
internal_output_headers1=Gene Variant Counts Counts MAF MAF $or_col_disp_pheno $dp_helper CR HWE P_Miss
internal_output_headers2=Gene Variant Case Ctrl Case Ctrl $or_col_disp_pheno $dp_helper CR HWE P_Miss

#first argument: space separated list of old headers
#second argument: space separated list of new headers
#third argument: row to apply
swap_header=eval `(echo @1 && echo @2) | $transpose_cmd | sed 's;\(\S\S*\)\s\s*\(..*\);sed '\''@3 s/\\\(^\\\|\\\t\\\)\1/\\\1\2/g'\'';' | tr '\n' '|' | sed 's/|$//'` | sed '@3 s/_/ /g'

swap_first_and_duplicate=eval `perl -e 'print "@1\n@2\n"' | $transpose_cmd | sed 's/_/ /g' | sed 's;\(\S\S*\)\s\s*\(..*\);sed '\''s/^\1/\2\t\2/'\'';' | tr '\n' '|' | sed 's/|$//'`

fix_vassoc_annot_disp_headers=sed '1 s/\(.*\)/\1\n\1/' | $swap_header($internal_input_headers !{prop::pheno:vassoc_meta_disp},$internal_output_headers1 !{prop::pheno:vassoc_meta_headers},1) | $swap_header($internal_input_headers !{prop::pheno:vassoc_meta_disp},$internal_output_headers2 !{prop::pheno:vassoc_meta_headers},2)

replace_var_id=sed 's/var_\(\S\S*\)_\(\S\S*\)/\1:\2/'

process_associated_variants_helper=$format_columns_cmd --in-delim $tab --header 1 --number-format MAFA,%.2g --number-format MAFU,%.2g --percentage MAFU --percentage MAFA --number-format ^${or_col_disp_pheno}_,%.1f --number-format ^P_,%.2g --regex-headers @3 | $fix_vassoc_annot_disp_headers | $truncate_cols(@5) | $table_to_beamer_cmd --allow-breaks --multi-row . --force-multi-row-break @6 --multi-col 1-2 --font-size 8pt --auto-dim --bottom-margin 0in --left-margin 0in --right-margin 0in --title "@1" --in-delim $tab --header-rows 2

head_cmd=awk 'NR <= @1'
head_frac_cmd_raw=perl -ne 'BEGIN {\$size = @3`cat !{input,,@1} | wc -l@3`}; print if \$i++ < @2 \* \$size' 
head_frac_cmd_inside=$head_frac_cmd_raw(@1,@2,\)
head_frac_cmd_outside=$head_frac_cmd_raw(@1,@2,)

pheno_select_slide_variants_helper=$smart_cut_cmd !{input,--file,@3} --in-delim $tab --select-row 1,1 --select-row 1,1,$vcf_type_annot,@1 --select-row 1,1,GENE,@2 --and-row-all --select-row 1,1,MAC,ge:@4

pheno_slide_variants_helper=$pheno_select_slide_variants_helper(@1,@3,@8,@10) | $pheno_process_slide_variants_helper(@4,@5,@6,@7,@9)

old_annots=[^\\\t]\*SPLICE_SITE[^\\\t]* [^\\\t]\*$vcf_type_missense_annot $vcf_type_synonymous_annot $vcf_type_nonsense_annot INTRONIC tolerated benign probably.damaging possibly.damaging neutral deleterious unknown FRAMESHIFT_CODING 3PRIME_UTR 5PRIME_UTR STOP_LOST COMPLEX_IN.DEL PARTIAL_CODON CODING_UNKNOWN WITHIN_MATURE_miRNA NMD_TRANSCRIPT WITHIN_NON_CODING_GENE UPSTREAM DOWNSTREAM REGULATORY_REGION TRANSCRIPTION_FACTOR_BINDING_MOTIF INTERGENIC intron.variant splice.acceptor.variant splice.region.variant splice.donor.variant downstream.gene.variant upstream.gene.variant frameshift.variant initiator.codon.variant
new_annots=Splice Mis Syn Stop Intron Tol Ben Prob Pos Neu Del NA Frame 3\\\'_UTR 5\\\'_UTR RdThru Indel ParCod Coding miRNA NMD NonCod Upstream Downstream Reg TFMotif Inter Intron Splice Splice Splice Downstream Upstream Frame Init

translate_variant_type=$swap_header($old_annots !{prop::pheno:old_disp_annots:if_prop=old_disp_annots:allow_empty=1},$new_annots !{prop::pheno:new_disp_annots:if_prop=new_disp_annots:allow_empty=1},1!)

pheno_slide_associated_variants_helper=$pheno_slide_variants_helper(@1,@2,@3,CLOSEST_GENE ID !{prop::burden:vassoc_meta_disp} MINA MINU MAFA MAFU $or_col_disp_pheno_test !{raw::pheno_test:${p_col_disp}_\@test_tag:if_prop=use_for_display},,!{raw::pheno_test:${p_col_disp}_\@test_tag:if_prop=use_for_display:limit=1},@4,@5,,!{prop::burden:min_pheno_mac})

local cmd make_interesting_genes_dat_file=\
  $smart_cut_cmd !{input,--file,burden_interesting_genes_dat_file,if_prop=burden_test,unless_prop=only_for_interesting,allow_empty=1} !{input,--file,burden_interesting_variant_genes_dat_file,unless_prop=only_for_interesting,allow_empty=1} | sort -u \
  | $smart_cut_cmd --exec "perl -e 'print \"!{prop,,pheno,extra_interesting_genes,sep=\n,if_prop=extra_interesting_genes,allow_empty=1}\"'" \
	| sed '/^\s*$/d' \
  | sort -u | awk '\$_ != "$outside_gene_name"' > !{output,,pheno_interesting_genes_dat_file} class_level pheno skip_if not_trait

prop external_id=scalar
prop gene_start=scalar
prop gene_end=scalar

variant_internal_id=!{prop,,pheno}_@1
gene_internal_id=!{prop,,pheno}_@1

annotate_genes_with_region=$add_gene_annot_cmd --no-outside --chr-col 2 --pos-col 3 --gene-file !{input,,project_region_list_file} --out-delim $tab

get_locus_for_gene=$smart_join_cmd !{input,--file,project_gene_target_file} !{input,--file,pheno_interesting_genes_dat_file} --extra 1 --out-delim $tab | cut -f1-3 | $annotate_genes_with_region | cut -f1,2
fix_gene_internal_id=@1 =~ s/\//-/g;

local cmd make_pheno_gene_sort_values_file=m=`cat !{input,,burden_gene_sort_values_file,if_prop=burden_test,unless_prop=only_for_interesting} | cut -f2 | awk 'BEGIN {m=""} {if (m=="" || \$1 > m) {m=\$1}} END {print m}'` && cat !{input,,burden_gene_sort_values_file,if_prop=burden_test,unless_prop=only_for_interesting} | $smart_cut_cmd --out-delim $tab --exec "sed 's/$/\t'\$m'/' !{input,,pheno_interesting_genes_dat_file}" | $table_sum_stats_cmd --in-delim $tab --out-delim $tab --group-col 1 --col 2 --summaries --print-header | $smart_cut_cmd --in-delim $tab --select-col 0,1 --select-col 0,1,$table_sum_stats_min | tail -n+2 > !{output,,pheno_gene_sort_values_file} class_level pheno skip_if not_trait

local cmd make_pheno_variant_subset_gene_to_variant_subset_map=$smart_cut_cmd --in-delim $tab !{input,--file,pheno_variant_subset_clean_gene_variant_file} --select-col 1,1,'GENE' --exclude-row 1,1 | sort -u | sed 's/$/\t!{prop,,pheno_variant_subset}\t!{prop,,project_variant_subset}/' > !{output,,pheno_variant_subset_gene_to_variant_subset_map} class_level pheno_variant_subset skip_if not_trait

local cmd make_pheno_gene_to_variant_subset_map=cat !{input,,pheno_variant_subset_gene_to_variant_subset_map} > !{output,,pheno_gene_to_variant_subset_map} class_level pheno run_if num_var_subsets

local cmd make_pheno_interesting_genes_meta_file=$smart_join_cmd --exec "$smart_join_cmd --in-delim $tab !{input,--file,pheno_interesting_loci_dat_file} --exec \"$get_locus_for_gene\"  --multiple 2 --extra 1 | awk -v OFS=\"\t\" '{print \\$3,\\$1,\\$2}'" !{input,--file,project_gene_target_file} !{input,--file,pheno_interesting_genes_dat_file} !{input,--file,pheno_gene_sort_values_file} !{input,--file,pheno_gene_to_variant_subset_map,if_prop=num_var_subsets,allow_empty=1} --rest-extra 3 --out-delim $tab | perl -ne 'chomp; @a = split("\t"); \$a[3] =~ s/^chr//; \$external_id = \$a[0]; $fix_gene_internal_id(\$a[0]); print "$gene_internal_id(\$a[0]) class gene\n!select:!{prop,,project} $gene_internal_id(\$a[0]) disp \$external_id\n$gene_internal_id(\$a[0]) external_id \$external_id\n!select:!{prop,,project} $gene_internal_id(\$a[0]) parent !{prop,,pheno}_locus_\$a[2]\n!select:!{prop,,project} $gene_internal_id(\$a[0]) chrom \$a[3]\n!select:!{prop,,project} $gene_internal_id(\$a[0]) gene_start \$a[4]\n!select:!{prop,,project} $gene_internal_id(\$a[0]) gene_end \$a[5]\n!select:!{prop,,project} $gene_internal_id(\$a[0]) sort \$a[6]\n"; if (scalar(@a) > 7) {print "!select:!{prop,,project} $gene_internal_id(\$a[0]) consistent \$a[7]\n!select:!{prop,,project} $gene_internal_id(\$a[0]) consistent \$a[8]\n"}' > !{output,,pheno_interesting_genes_meta_file} && $smart_join_cmd --exec "$smart_cut_cmd !{input,--file,project_gene_interesting_transcript_file,if_prop=project_gene_interesting_transcript_file,allow_empty=1} --select-col 1,1 | sort -u | cat - !{input,,pheno_interesting_genes_dat_file} | sort | uniq -d" !{input,--file,project_gene_interesting_transcript_file,if_prop=project_gene_interesting_transcript_file,allow_empty=1} --extra 2 --multiple 2 | sed 's/\(\S\S*\)\s\s*\(\S\S*\)/$gene_internal_id(\1) use_transcripts \2/' >> !{output,,pheno_interesting_genes_meta_file} class_level pheno skip_if not_trait with gene

interesting_region_or_loci_dat_helper=$smart_join_cmd --in-delim $tab --exec "@1" --exec "cat !{input,,project_gene_variant_file} | awk '!map[\\$1] && NR > 1 {map[\\$1] = 1; print}' | $annotate_genes_with_region | awk '!map[\\$2] {map[\\$2] = 1; print}' | awk -v OFS=\"\t\" '{print \\$1,\\$4,\\$2}' | sort -nk1,2 | cut -f1,3 | perl -lane 'chomp; if (\\$last && \\$F[0] ne \\$last) {print \\$string; \\$string = \"\"} \\$string = \"\\$F[0]\" unless \\$string; \\$last = \\$F[0]; \\$string .= \"\t\\$F[1]\"; END {print \\$string}' | awk 'NF > 2 {print \\$1\"\t\"\\$2\"-\"\\$NF} NF <= 2 {print}'" --extra 2

local cmd make_pheno_interesting_loci_dat_file=$interesting_region_or_loci_dat_helper("$get_locus_for_gene | cut -f1 | cat - !{input,,project_region_list_file,if_prop=all_loci_interesting,allow_empty=1} | cut -f1 | sort -u") > !{output,,pheno_interesting_loci_dat_file} class_level pheno skip_if not_trait

local cmd make_pheno_loci_sort_values_file=$smart_join_cmd --in-delim $tab --extra 1 --col 1,2 --exec "cat !{input,,project_gene_variant_file} | awk '!map[\\$1] && NR > 1 {map[\\$1] = 1; print}' | $annotate_genes_with_region | cut -f1-2 | awk '!map[\\$2] {map[\\$2] = 1; print}'" !{input,--file,pheno_gene_sort_values_file} | cut -f2- | $table_sum_stats_cmd --in-delim $tab --out-delim $tab --group-col 1 --col 2 --summaries --print-header | $smart_cut_cmd --in-delim $tab --select-col 0,1 --select-col 0,1,$table_sum_stats_min | tail -n+2 > !{output,,pheno_loci_sort_values_file} class_level pheno skip_if not_trait

prop locus_range=scalar
prop locus_start=scalar
prop locus_end=scalar

get_range=@1:@2-@3

interesting_regions_or_loci_helper=$smart_join_cmd !{input,--file,@3} !{input,--file,project_region_list_file} @6 --extra 2 --in-delim $tab | sort -k1 | perl -lane '\$name = "!{prop,,@2}_@{1}_\$F[1]"; \$chr = \$F[2]; \$ref_chr = \$chr; \$chr =~ s/^chr//; \$start = \$F[3]; \$end = \$F[4]; print "\$name class @1"; print "!select:!{prop,,project} \$name parent !{prop,,@2}"; print "\$name @{1}_range $get_range(\$chr,\$start,\$end)"; print "\$name disp \$F[1]"; print "\$name chrom \$chr"; print "\$name ref_chrom \$ref_chr"; print "\$name @{1}_start \$start"; print "\$name @{1}_end \$end"; @5' > !{output,,@4} 

#all loci should have same sort value; o.w. replace sort 1 with sort \$F[5]
local cmd make_pheno_interesting_loci_meta_file=$interesting_regions_or_loci_helper(locus,pheno,pheno_interesting_loci_dat_file,pheno_interesting_loci_meta_file,print "\$name consistent !{prop\,\,project}_region_\$F[1]\n\$name sort 1",!{input\,--file\,pheno_loci_sort_values_file} --extra 3) class_level pheno skip_if not_trait

prop consequence=scalar
prop proteinchange=scalar
prop pos=scalar
prop ncase=scalar
prop ncontrol=scalar
prop nind=scalar
prop var_annot=scalar


annot_file_for_interesting_variants=pheno_vassoc_annot_file

local cmd make_pheno_interesting_variants_dat_file=$smart_cut_cmd --in-delim $tab !{input,--file,burden_interesting_gene_variants_dat_file,unless_prop=only_for_interesting} !{input,--file,burden_all_interesting_gene_variants_dat_file,if_prop=burden_test,unless_prop=only_for_interesting} | sort -u \
  | $smart_cut_cmd --exec "perl -e 'print \"!{prop,,pheno,extra_interesting_variants,sep=\n,if_prop=extra_interesting_variants,allow_empty=1}\"'" \
  | sort -u \
  | $smart_cut_cmd --in-delim $tab !{input,--file,pheno_vassoc_annot_file} --select-col 1,1,ID --exact --require-col-match \
  | sort | uniq -d > !{output,,pheno_interesting_variants_dat_file} class_level pheno skip_if not_trait

local cmd make_pheno_interesting_variants_reg_file=$smart_join_cmd --in-delim $tab --exec "cat !{input,,pheno_interesting_variants_dat_file} | $add_header_cmd ID" --exec "$smart_cut_cmd --in-delim $tab !{input,--file,project_clean_gene_variant_file} --exclude-row 1,1 --select-col 1,1,'ID CHROM POS' --exact --require-col-match" --header 1 --extra 2 --rest-extra 1 | cut -f2- | sed 's/^chr//' | awk '{print "chr"\$1":"\$2}' > !{output,,pheno_interesting_variants_reg_file} class_level pheno skip_if not_trait

short cmd make_pheno_interesting_variants_pre_meta_file=$smart_join_cmd \ 
  --exec "cat !{input,,pheno_interesting_variants_dat_file} | $add_header_cmd ID"  \
  --exec "$add_header_cmd ID < !{input,,pheno_interesting_variants_dat_file}"  \
  --exec "$smart_cut_cmd !{input,--file,pheno_vassoc_annot_file} --in-delim $tab --select-col 1,1,'ID $vcf_type_annot $vcf_protein_change_annot $col_for_var_annot MINA MINU MAC' --exact --require-col-match" \
	--exec "$smart_cut_cmd !{input,--file,project_clean_gene_variant_file} --in-delim $tab --select-col 1,1,'ID GENE POS'" \
	--in-delim $tab --header 1 --rest-extra 1 --ignore-err $plinkseq_okay_err  \
  | $smart_cut_cmd --in-delim $tab --select-col 0,1,'ID GENE POS $vcf_type_annot $vcf_protein_change_annot $col_for_var_annot MINA MINU MAC' --exact --require-col-match --exclude-row 0,1,GENE,$outside_gene_name --exclude-row 0,1 \
  > !{output,,pheno_interesting_variants_pre_meta_file} !{input,pheno_interesting_genes_meta_file} class_level pheno skip_if not_trait

local cmd make_pheno_interesting_variants_meta_file=cat !{input,,pheno_interesting_variants_pre_meta_file} | perl -ne 'chomp; @a = split("\t"); \$external_id = \$a[0]; \$id = "$variant_internal_id(\$a[0])"; \$gene = \$a[1]; $fix_gene_internal_id(\$gene); \$pos = \$a[2]; \$type = \$a[3]; \$proteinchange = \$a[4]; \$var_annot = \$a[5]; \$ncase = \$a[6]; \$ncontrol = \$a[7]; \$nind = int(\$a[8]); print "\$id class variant\n!select:!{prop,,project}:!{prop,,pheno} \$id parent $gene_internal_id(\$gene)\n\$id external_id \$external_id\n\$id disp \$external_id\n\$id consequence \$type\n\$id proteinchange \$proteinchange\n\$id var_annot \$var_annot\n\$id pos \$pos\n\$id ncase \$ncase\n\$id ncontrol \$ncontrol\n\$id nind \$nind\n" if (\$ncase + \$ncontrol) > 0' > !{output,,pheno_interesting_variants_meta_file} class_level pheno skip_if not_trait

wc_helper_int=wc -l !{input,,@1} @2 | fgrep -v /dev/null | sed 's/^\s*//' | sed 's/\s\s*/\t/' @3
wc_helper_mult=$wc_helper_int(@1,/dev/null,| sed '\\$d')
wc_helper_sing=$wc_helper_int(@1,,)

local cmd make_pheno_interesting_counts_tex_file=let n=`perl -e 'print "!{prop,,burden,unless_prop=only_for_interesting,sep=\n}\n"' | wc -l`+3 && $smart_cut_cmd --in-delim $tab --exec "perl -e 'print \"!{prop,,burden,disp,unless_prop=only_for_interesting,sep=\n}\n\"'" --exec "$wc_helper_mult(burden_interesting_genes_dat_file\\,unless_prop=only_for_interesting\\,if_prop=burden_test)" --exec "$wc_helper_mult(burden_interesting_variant_genes_dat_file\\,unless_prop=only_for_interesting)" --exec "$wc_helper_mult(burden_interesting_gene_variants_dat_file\\,unless_prop=only_for_interesting)" --exec "$wc_helper_mult(burden_all_interesting_gene_variants_dat_file\\,unless_prop=only_for_interesting)" --select-col .,1 --paste | $smart_cut_cmd --stdin-first --in-delim $tab --exec "$smart_cut_cmd --in-delim $tab --exec \"echo Total\" --exec \"$wc_helper_sing(pheno_interesting_genes_dat_file)\" --exec \"$wc_helper_sing(pheno_interesting_genes_dat_file)\" --exec \"$wc_helper_sing(pheno_interesting_variants_dat_file)\" --exec \"$wc_helper_sing(pheno_interesting_variants_dat_file)\" --paste --select-col .,1" | sed '1 s/^/Class\tGenes\tGenes\tVariants\tVariants\nClass\tBurden\tVariants\tP-value\tAll\n/' | $table_to_beamer_cmd --in-delim $tab --header-rows 2 --header-cols 1 --font-size 8pt --footer-rows 1 --multi-row 1,1-2 --multi-col 1,2-3 --multi-col 1,4-5 --multi-col \$n,2-3 --multi-col \$n,4-5 --title "Interesting Gene/Variant Counts" > !{output,,pheno_interesting_counts_tex_file} class_level pheno skip_if not_trait

local cmd make_pheno_interesting_counts_pdf_file=$run_latex_cmd(pheno_interesting_counts_tex_file,pheno_interesting_counts_pdf_file) class_level pheno

#merge a file with excluded id samples having -9 with the sequenced pheno file followed by the marker 

local cmd make_pheno_marker_pheno_file=$smart_join_cmd --exec "$smart_cut_cmd !{input,--file,project_all_marker_assoc_sample_keep_file,unless_prop=pheno_all_marker_assoc_sample_keep_file,allow_empty=1} !{input,--file,project_all_marker_assoc_sample_keep_file,unless_prop=pheno_all_marker_assoc_sample_keep_file,allow_empty=1} !{input,--file,pheno_all_marker_assoc_sample_keep_file,if_prop=pheno_all_marker_assoc_sample_keep_file,allow_empty=1} !{input,--file,pheno_all_marker_assoc_sample_keep_file,if_prop=pheno_all_marker_assoc_sample_keep_file,allow_empty=1} --exec \"tail -n+2 !{input,,project_all_marker_pheno_file,unless_prop=pheno_marker_initial_pheno_file,allow_empty=1} !{input,--file,pheno_marker_initial_pheno_file,if_prop=pheno_marker_initial_pheno_file,allow_empty=1}\" --select-col 1,2 --select-col 2,2 --select-col 3,2 | sort | uniq -u | sed 's/$/ $plink_missing/'" --exec "$smart_join_cmd --in-delim $tab --exec \"$smart_cut_cmd !{input,--file,project_all_marker_pheno_file,unless_prop=pheno_marker_initial_pheno_file,allow_empty=1} !{input,--file,pheno_marker_initial_pheno_file,if_prop=pheno_marker_initial_pheno_file,allow_empty=1} --select-col 1,2 --exclude-row 1,1 --require-col-match --exact | $smart_cut_cmd --in-delim $tab !{input,--file,pheno_sequenced_all_sample_info_file} --select-col 1,1,ID | sort | uniq -d | sed '1 s/^/ID\n/'\" !{input,--file,pheno_sequenced_all_sample_info_file} --extra 2 --header 1 | $smart_cut_cmd --in-delim $tab --select-col 0,1,'ID !{prop,,pheno}'" --exec "$smart_cut_cmd !{input,--file,project_all_marker_pheno_file,unless_prop=pheno_marker_initial_pheno_file,allow_empty=1} !{input,--file,pheno_marker_initial_pheno_file,if_prop=pheno_marker_initial_pheno_file,allow_empty=1} --select-col 1,2 --exclude-row 1,1 --select-col 1,1,!{prop,,pheno} --require-col-match --exact" --merge > !{output,,pheno_marker_pheno_file} class_level pheno skip_if not_trait


!!expand:impute_type:hap:traditional! \
meta_table cmd make_pheno_imputation_impute_type_gene_list_file=!{input,,gene_impute_type_summary_file} !{output,pheno_imputation_impute_type_gene_list_file} class_level pheno skip_if not_trait

meta_table cmd make_pheno_snptest_locus_list_file=!{input,,locus_combined_snptest_out_file} !{output,pheno_snptest_locus_list_file} class_level pheno skip_if not_trait

#!!expand:impute_type:hap:traditional! \
#local cmd make_pheno_imputation_impute_type_summary_file=$smart_join_cmd --in-delim $tab --exec "$smart_cut_cmd !{input,--file,pheno_vassoc_annot_file} --in-delim $tab --select-col 1,1,'ID GENE $vcf_protein_change_annot MINA MINU OBSA OBSU P OR' --exact | $add_function_cmd --header 1 --in-delim $tab --col1 MINA --col2 OBSA --type divide --val-header MAFA --add-at 6 | $add_function_cmd --header 1 --in-delim $tab --col1 MINU --col2 OBSU --type divide --val-header MAFU --add-at 7 | $smart_cut_cmd --in-delim $tab --exclude-col 0,1,'OBSA OBSU'" --exec "(cat !{input,,pheno_imputation_impute_type_gene_list_file} | xargs head -q -n1 | awk 'NR == 1'  && cat !{input,,pheno_imputation_impute_type_gene_list_file} | xargs tail -q -n+2)" --extra 1 --header 1 | $add_function_cmd --in-delim $tab --header 1 --type subtract --col1 OR --val2 1 --val-header CONSISTENT | $add_function_cmd --in-delim $tab --header 1 --type subtract --col1 F_CA_MIN --col2 F_CT_MIN --val-header CONSISTENT2 | awk -v OFS="\t" -F"\t" 'NR > 1 { if (\$NF == "NA" || \$(NF - 1) == "NA") {\$(NF - 1) = "NA"} else if (\$NF * \$(NF - 1) > 0) {\$(NF - 1) ="TRUE"} else {\$(NF - 1)="FALSE"}} {print}' | rev | cut -f2- | rev | $smart_cut_cmd --in-delim $tab --exclude-col 0,1,CHROM --exact > !{output,,pheno_imputation_impute_type_summary_file} !{input,gene_impute_type_summary_file} class_level pheno skip_if or,not_trait,pheno_qt

impute2_info_col=8
impute2_varid_col=2

local cmd make_pheno_snptest_summary_file=(head -q -n1 !{input,,pheno_snptest_locus_list_file} | xargs head -q -n1  && cat !{input,,pheno_snptest_locus_list_file} | xargs tail -q -n+2 | sort -nrk$impute2_info_col | awk 'map[\$2] != 1 {print} {map[\$2] = 1}') !{input,locus_combined_snptest_out_file} > !{output,,pheno_snptest_summary_file} class_level pheno skip_if not_trait

prop do_hap_burden=scalar
#meta_table cmd make_pheno_haplotype_burden_input_list_file=!{prop,,locus,disp} !{input,,locus_index_geno_counts_file} !{input,,locus_strat_index_snp_file} !{input,,locus_top_common_marker_pos_file} !{input,,locus_all_seq_tped_file} !{input,,locus_all_seq_tfam_file} !{input,,locus_all_marker_tped_file} !{input,,locus_all_marker_tfam_file} !{output,pheno_haplotype_burden_input_list_file} class_level pheno run_if do_hap_burden

pheno_slide_failures_tex_helper_int=$smart_join_cmd !{input,--file,project_sample_failure_status_file,if_prop=not_trait,allow_empty=1} !{input,--file,pheno_sample_failure_status_file,unless_prop=not_trait,allow_empty=1} --exec \\\"cat !{input,,pheno_sample_info_header_file} !{input,,pheno_non_missing_sample_info_file} !{input,,pheno_failed_non_missing_sample_info_file}\\\" @2 --extra 1 --header 1 --in-delim $tab --out-delim $tab | $table_sum_stats_cmd --has-header --in-delim $tab --out-delim $tab --summaries --totals --col '@1' @3 --print-header | $smart_cut_cmd --no-vec-delim --in-delim $tab --select-col 0,1 @4 --select-col 0,1,'$summary_tot(@1)' --select-col 0,1,'$summary_mean(@1)' --exclude-row 0,1 --require-col-match | $format_columns_cmd --in-delim $tab --number-format @5,@6 --percentage @5 | sed 's/\t\(\S\S*\)$/\t\(\1\)/'

pheno_slide_failures_tex_helper=$pheno_slide_failures_tex_helper_int(@1,,--group-col $display_col_name,,3,%.1f)

pheno_slide_cross_failures_tex_helper=$pheno_slide_failures_tex_helper_int(@1,!{input\,--file\,pheno_cross_classification_phe_file} --extra 3,--group-col $display_col_name --group-col @2,--select-col 0\,1\,@2,4,%d) | sed 's/\t/:/'

!!expand:;:extrakey;extracolumncmd1;extracolumncmd2;addcols:\
trait;--exec \"$pheno_slide_failures_tex_helper(${popgen_fail_status})\" --exec \"$pheno_slide_failures_tex_helper(${related_fail_status})\";--exec \"$pheno_slide_failures_tex_helper(${covar_pass_status})\" --exec \"$pheno_slide_failures_tex_helper(${cluster_pass_status})\";--col2 8 --col2 10:\
"""
}
    