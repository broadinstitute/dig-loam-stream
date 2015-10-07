
package loamstream.pipeline.examples

/**
 * LoamStream
 * Created by oliverr on 10/6/2015.
 */
object PipelineTargetedPart25 {
  val string =
 """not_trait;;;! \
failures_helper_extrakey=$smart_join_cmd --in-delim $tab --out-delim $tab --multiple 2 --extra 1 \
 !{input,--file,pheno_disp_order_file} \
 --exec "$smart_join_cmd --in-delim $tab --header 1 \
  --exec \"$pheno_slide_failures_tex_helper(${failed_status})\" \
  --exec \"$pheno_slide_failures_tex_helper(${qc_fail_status})\" \
  extracolumncmd1 \
  --exec \"$pheno_slide_failures_tex_helper(${qc_pass_status})\" \
  extracolumncmd2 "\
 | sort -t$tab -nk2 | $smart_cut_cmd --in-delim $tab --exclude-col 0,2 | $add_function_cmd --in-delim $tab --type add --col1 2 --col2 4 --col2 6 addcols --add-at 2 | cut -f3 --complement

!!expand:,:extrakey,extrahelper,extracolumns1,extracolumns2,passstatustype,colemphcol,extrarunif:\
,trait,\t$popgen_fail_status\t$related_fail_status,\t$covar_pass_status\t$cluster_pass_status,popgen,6,skip_if not_trait:\
_not_trait,not_trait,,,qc,5,run_if not_trait! \
local cmd make_pheno_slide_failures_tex_fileextrakey=$failures_helper_extrahelper | sed 's/\t(/ (/g' | sed '1 s/^/Group\t$total_status\t$qc_fail_statusextracolumns1\t$passstatustype_pass_statusextracolumns2\n/' | $table_to_beamer_cmd --allow-breaks --in-delim $tab --header-rows 1 --font-size 8pt --auto-dim --col-emph colemphcol --title "!{prop,,pheno,disp} Sample Status" > !{output,,pheno_slide_failures_tex_file} class_level pheno extrarunif

local cmd make_pheno_slide_failures_pdf_file=$run_latex_cmd(pheno_slide_failures_tex_file,pheno_slide_failures_pdf_file) class_level pheno

!!expand:;:extrakey;extrahelper;extracut;extracolumns;passstatustype;extrarunif:\
;trait;,8,10;\t$popgen_fail_status_long\t$related_fail_status_long;popgen;skip_if not_trait:\
_not_trait;not_trait;;;qc;run_if not_trait! \
local cmd make_pheno_slide_failures_dat_fileextrakey=$failures_helper_extrahelper | cut -f1,4,6extracut | sed '1 s/^/Group\t$qc_fail_status_longextracolumns\t$passstatustype_pass_status_long\n/' | sed '1 s/ /_/g' > !{output,,pheno_slide_failures_dat_file} class_level pheno extrarunif

!!expand:;:extrakey;extrahelper;extracolumns;extrarunif:;trait;\,4\,5;skip_if not_trait:_not_trait;not_trait;;run_if not_trait! \
local cmd make_pheno_slide_failures_bar_pdf_fileextrakey=$draw_bar_plot_cmd(!{input\,\,pheno_slide_failures_dat_file} !{output\,\,pheno_slide_failures_bar_pdf_file} 'Sample Status' 'Num Samples' 2\,3extracolumns 1 sep=$tab subtitle='!{prop\,\,pheno\,disp}') class_level pheno extrarunif

pheno_slide_cross_failures_font_size=8pt

!!expand:;:extrakey;extracut1;extracut2;extracolumns;extraheaders1;extraheaders2;passstatustype;colemphcol;extrarunif\
:;--exec \"$pheno_slide_cross_failures_tex_helper(${popgen_fail_status},\$f)\" --exec \"$pheno_slide_cross_failures_tex_helper(${related_fail_status},\$f)\";--exec \"$pheno_slide_cross_failures_tex_helper(${covar_pass_status},\$f)\" --exec \"$pheno_slide_cross_failures_tex_helper(${cluster_pass_status},\$f)\" ;--col2 9 --col2 11;\t$popgen_fail_status\t$related_fail_status;\t$covar_pass_status\t$cluster_pass_status;popgen;7;skip_if not_trait\
:_not_trait;;;;;;qc;6;run_if not_trait! \
local cmd make_pheno_slide_cross_failures_tex_fileextrakey=$table_to_beamer_cmd --allow-breaks --font-size $pheno_slide_cross_failures_font_size --no-footer --custom-dim 9in,5in --no-body > !{output,,pheno_slide_cross_failures_tex_file} && \
for f in `head -n1 !{input,,pheno_cross_classification_phe_file} | cut -f1 --complement | $smart_cut_cmd --in-delim $tab --exclude-col 0,1,!{prop,,pheno}`; do \
$smart_join_cmd --in-delim $tab --out-delim $tab --extra 1 --multiple 2 \
 !{input,--file,pheno_disp_order_file} \
 --exec "$smart_join_cmd --in-delim $tab --header 1 \
  --exec \"$pheno_slide_cross_failures_tex_helper(${failed_status},\$f)\" \
  --exec \"$pheno_slide_cross_failures_tex_helper(${qc_fail_status},\$f)\" \
  extracut1 \
  --exec \"$pheno_slide_cross_failures_tex_helper(${qc_pass_status},\$f)\" \
  extracut2 \
  | sed 's/:/\t/'"\
 | sort -t$tab -nk2 | $smart_cut_cmd --in-delim $tab --exclude-col 0,2 | $add_function_cmd --in-delim $tab --type add --col1 3 --col2 5 --col2 7 extracolumns --add-at 3 | cut -f4 --complement | sed 's/\t(/ (/g' ; done | sed '1 s/^/Group\tGroup\tTot\t$qc_fail_statusextraheaders1\t$passstatustype_pass_statusextraheaders2\n/' | $table_to_beamer_cmd  --allow-breaks --font-size $pheno_slide_cross_failures_font_size --multi-col 1,1-2 --multi-row 1 --col-emph colemphcol --no-header --no-footer --in-delim $tab --header-rows 1 --header-cols 2 --title "!{prop,,pheno,disp} Sample Status" >> !{output,,pheno_slide_cross_failures_tex_file} && \
$table_to_beamer_cmd --allow-breaks --font-size $pheno_slide_cross_failures_font_size --no-header --no-body >> !{output,,pheno_slide_cross_failures_tex_file} class_level pheno extrarunif

local cmd make_pheno_slide_cross_failures_pdf_file=$run_latex_cmd(pheno_slide_cross_failures_tex_file,pheno_slide_cross_failures_pdf_file) class_level pheno

!!expand:filetype:gassoc:flat_gassoc! \
local cmd make_pheno_filetype_file=(head -q -n1 !{input,,burden_filetype_file,if_prop=burden_test,limit=1} && tail -q -n+2 !{input,,burden_filetype_file,if_prop=burden_test}) > !{output,,pheno_filetype_file} class_level pheno skip_if not_trait

#short cmd make_burden_sample_list_file=$burden_sample_list_helper(gt,burden_sample_list_file) class_level burden run_if or,annot_genes,annot_manual_gene_list_file

#short cmd make_burden_sample_without_list_file=$burden_sample_list_helper(le,burden_sample_without_list_file) class_level burden run_if or,annot_genes,annot_manual_gene_list_file

#local cmd make_burden_pheno_pdf_file=$pheno_pdf_helper(burden,!{prop\\,\\,burden\\,disp} variants) class_level burden run_if or,annot_genes,annot_manual_gene_list_file

!!expand:pathwayburdentype:custom! \
local cmd make_pheno_pathway_pathwayburdentype_gassoc_file=(head -q -n1 !{input,,burden_pathway_pathwayburdentype_gassoc_file,if_prop=burden_test,limit=1} && tail -q -n+2 !{input,,burden_pathway_pathwayburdentype_gassoc_file,if_prop=burden_test}) > !{output,,pheno_pathway_pathwayburdentype_gassoc_file} class_level pheno skip_if or,not_trait run_if burden_test

#burden cmds

burden_include_warning=could not evaluate filter expression

meta_table cmd make_annot_manual_gene_list_file=!{prop,,annot,annot_genes,sep=\n} !{output,annot_manual_gene_list_file} class_level annot run_if annot_genes skip_if annot_manual_gene_list_file

local cmd make_annot_manual_gene_variant_list_file=$smart_join_cmd --in-delim $tab !{input,--file,annot_manual_gene_list_file} --exec "$smart_cut_cmd !{input,--file,project_gene_variant_file,if_prop=use_raw_variants,allow_empty=1} !{input,--file,project_clean_gene_variant_file,unless_prop=use_raw_variants,allow_empty=1} --exclude-row 1,1 --select-col 1,1,'CHROM POS ID' --require-col-match --exact --in-delim $tab | $add_gene_annot_cmd --in-delim $tab --print-multiple --chr-col 1 --pos-col 2 --gene-file !{input,,project_gene_target_file} --out-delim $tab | cut -f1,4" --multiple 2 --extra 2 | cut -f2 > !{output,,annot_manual_gene_variant_list_file} class_level annot run_if or,annot_genes,annot_manual_gene_list_file

get_annot_genes_helper=!{raw,,annot,| cat - *annot_manual_gene_variant_list_file | sort | uniq -d,if_prop=annot_genes,if_prop=annot_manual_gene_list_file,or_if_prop=1,allow_empty=1} !{input,annot_manual_gene_variant_list_file,if_prop=annot_genes,if_prop=annot_manual_gene_list_file,or_if_prop=1,allow_empty=1}

manual_annot_helper=!{raw,,annot,| $smart_cut_cmd --in-delim $tab - --exec 'sort -u *annot_manual_variant_list_file' | sort | uniq -d,if_prop=annot_manual_variant_list_file,allow_empty=1,max=1} !{input,annot_manual_variant_list_file,if_prop=annot_manual_variant_list_file,allow_empty=1,max=1} $get_annot_genes_helper 

annot_variant_list_helper=eval `perl -e 'print qq(!{prop,,annot,annot_mask,sep=\n,if_prop=annot_mask,allow_empty=1})' | sed 's/\($select_and_delim\|^\)/ --select-row 1,1,/g' | sed 's/$select_filter_delim/,/g' | sed 's;^;$smart_cut_cmd !{input,--file,@1} --tab-delim --and-row-all --select-col 1,1 @2;g' | tr '\n' '|' | sed 's/|$//'` | sort -u $manual_annot_helper 

no_annot_variant_list_helper=$smart_cut_cmd !{input,--file,@1} --tab-delim --select-col 1,1 @2 | sort -u $manual_annot_helper

!|expand:;:annotype;exskipif:annot;!annot_mask:no_annot;annot_mask| \
!|expand:;:shortt;projectl;annotl;exrunif:;project;annot;run_if !annot_variant_subset:short;project_variant_subset;annot_variant_subset;run_if annot_variant_subset bsub_batch 10| \
shortt cmd make_annotl_annotype_variant_list_file=$annotype_variant_list_helper(projectl_full_var_annot_file,--exclude-row 1\,1,) > !{output,,annotl_annot_variant_list_file} class_level annotl exrunif run_with annot_variant_subset skip_if or,union_annots,exskipif

short cmd make_burden_only_interesting_variant_list_file=$smart_join_cmd --in-delim $tab --rest-extra 1 --multiple 2 --col 2,4 --exec "$smart_join_cmd --in-delim $tab !{input,--file,pheno_interesting_genes_dat_file} !{input,--file,project_transcript_gene_alias_file} --col 2,2 --extra 2 --multiple 2 | cut -f2 | cat - !{input,,pheno_interesting_genes_dat_file} | sort -u | $smart_cut_cmd --in-delim $tab --exec 'cut -f4 !{input,,annot_locdb_detail_reg_file,max=1} | sort -u' | sort | uniq -d" !{input,--file,annot_locdb_detail_reg_file,max=1} | cut -f5 > !{output,,burden_only_interesting_variant_list_file} class_level burden skip_if or,not_trait,!only_for_interesting

!!expand:,:qc_plus,clean_:qc_plus,clean_:qc_pass,! \
!!expand:;:burdenp;burdenl;projectl;varkeepfile;varkeepprocess;frqfile:burden;burden;project;project_gene_variant_file; | $smart_cut_cmd --tab-delim --select-col 0,1,ID --exclude-row 0,1;pheno_combined_frq_file:annot;annot;project;project_gene_variant_file; | $smart_cut_cmd --tab-delim --select-col 0,1,ID --exclude-row 0,1;project_plinkseq_qc_plus_frq_file:burden;burden_variant_subset;project_variant_subset;pheno_variant_subset_var_keep_file;;pheno_variant_subset_combined_frq_file:annot;annot_variant_subset;project_variant_subset;project_variant_subset_vstats_file; | $smart_cut_cmd --in-delim , --select-col 0,1,VAR --exclude-row 0,1;project_variant_subset_plinkseq_qc_plus_combined_frq_file! \
burdenl_qc_plus_non_annot_helper=pseq_cmd="$pseq_qc_plus_@1(v-matrix)"; mask="@2"; !{raw;;burdenl;eval \$pseq_cmd $show_id \$mask | cut -f1 | tail -n+2 | $parse_out_id;if_prop=burdenp_mac_ub;if_prop=burdenp_mac_lb;or_if_prop=1;allow_empty=1} \
 !{raw;;projectl;cat *varkeepfile varkeepprocess;unless_prop=burdenp_mac_ub;unless_prop=burdenp_mac_lb;allow_empty=1} !{input;varkeepfile;unless_prop=burdenp_mac_ub;unless_prop=burden_macp_lb;allow_empty=1} \
 !{raw;;burdenl;| $smart_cut_cmd --file *frqfile --select-row 1,1,MAF,le:\@burdenp_maf --select-col 1,1,SNP | sort | uniq -d;if_prop=burdenp_maf;unless_prop=burdenp_strat_maf_summary;allow_empty=1} !{input;frqfile;if_prop=burdenp_maf;unless_prop=burdenp_strat_maf_summary;allow_empty=1;instance_level=burdenl} \
 !{raw;;burdenl;| $smart_cut_cmd --in-delim , --file *projectl_plinkseq_qc_plus_strata_frq_summary_file --select-row 1,1,MAF_\@burdenp_strat_maf_summary,le:\@burdenp_maf --select-col 1,1 | sort | uniq -d;if_prop=burdenp_strat_maf_summary;allow_empty=1} !{input;projectl_plinkseq_qc_plus_strata_frq_summary_file;if_prop=burdenp_strat_maf_summary;allow_empty=1;instance_level=burdenl}  | $smart_cut_cmd --in-delim $tab !{input,--file,projectl_clean_gene_variant_file} --exclude-row 1,1 --select-col 1,1,ID | sort | uniq -d

extended_qc_filter_helper=!{raw,,@1,| cat - *project_extended_variant_exclude_file *project_extended_variant_exclude_file | sort | uniq -u,if_prop=apply_extended_qc,allow_empty=1} !{raw,,@1,| cat - *project_extended_strict_variant_exclude_file *project_extended_strict_variant_exclude_file | sort | uniq -u,if_prop=apply_extended_strict_qc,allow_empty=1} !{input,project_extended_strict_variant_exclude_file,if_prop=apply_extended_strict_qc,allow_empty=1} !{input,project_extended_variant_exclude_file,if_prop=apply_extended_qc,allow_empty=1}

!|expand:;:qc_plus;clean_;exrunif:qc_plus;clean_;!use_raw_variants:qc_pass;;use_raw_variants| \
!|expand:,:burdenl,assocpheno:annot,analysis_cmd| \
cmd make_burdenl_qc_plus_non_annot_variant_list_file=$burdenl_qc_plus_non_annot_helper(assocpheno,$raw_burdenl_mask_helper) $extended_qc_filter_helper(annot) | cat - !{input,,annot_variant_exclude_file} !{input,,annot_variant_exclude_file} | sort | uniq -u > !{output,,burdenl_non_annot_variant_list_file} class_level burdenl skip_if or,(and,!burdenl_maf,!burdenl_mac_lb,!burdenl_mac_ub,!apply_extended_qc,!apply_extended_strict_qc),only_for_interesting run_if and,!burdenl_variant_subset,!union_burdenls,exrunif run_with burdenl_variant_subset

!|expand:;:qc_plus;clean_;exrunif:qc_plus;clean_;!use_raw_variants:qc_pass;;use_raw_variants| \
!|expand:,:burdenl,assocpheno:burden,all_assoc_cmd_pheno| \
cmd make_burdenl_qc_plus_non_annot_variant_list_file=$burdenl_qc_plus_non_annot_helper(assocpheno,$raw_burdenl_mask_helper) !{raw,,burden,| cat - *burden_only_interesting_variant_list_file | sort | uniq -d,if_prop=only_for_interesting,allow_empty=1=1} !{input,burden_only_interesting_variant_list_file,if_prop=only_for_interesting,allow_empty=1} > !{output,,burdenl_non_annot_variant_list_file} class_level burdenl skip_if or,(and,!burdenl_maf,!burdenl_mac_lb,!burdenl_mac_ub),!only_for_interesting,union_burdens run_if exrunif run_with burdenl_variant_subset

!|expand:,:burdenl,assocpheno:burden,all_assoc_cmd_pheno| \
cmd make_burdenl_only_interesting_non_annot_variant_list_file=ln -s !{input,,burden_only_interesting_variant_list_file} !{output,,burdenl_non_annot_variant_list_file} class_level burdenl run_if and,!burdenl_maf,!burdenl_mac_lb,!burdenl_mac_ub,only_for_interesting,!union_burdens run_with burdenl_variant_subset

!|expand:;:qc_plus;clean_;exrunif:qc_plus;clean_;!use_raw_variants:qc_pass;;use_raw_variants| \
!|expand@;@ktype;mask;skipif@maf;$raw_burden_mask_helper;skip_if and,!burden_maf,!burden_mac_lb,!burden_mac_ub@expand;;skip_if or,burden_maf,burden_mac_lb,burden_mac_ub,expand_pheno_subsets:eq:1| \
short cmd make_burden_variant_subset_qc_plus_ktype_non_annot_variant_list_file=$burden_variant_subset_qc_plus_non_annot_helper(all_assoc_cmd_pheno_variant_subset,mask) > !{output,,burden_variant_subset_non_annot_variant_list_file} class_level burden_variant_subset skipif run_if and,!union_burdens,exrunif run_with burden_variant_subset bsub_batch 50

!|expand:;:qc_plus;clean_;exrunif:qc_plus;clean_;!use_raw_variants:qc_pass;;use_raw_variants| \
short cmd make_annot_variant_subset_qc_plus_non_annot_variant_list_file=$annot_variant_subset_qc_plus_non_annot_helper(analysis_cmd_project_variant_subset,$raw_annot_mask_helper) $extended_qc_filter_helper(annot) | cat - !{input,,annot_variant_exclude_file} !{input,,annot_variant_exclude_file} | sort | uniq -u > !{output,,annot_variant_subset_non_annot_variant_list_file} class_level annot_variant_subset skip_if and,!annot_maf,!annot_mac_lb,!annot_mac_ub,!apply_extended_qc,!apply_extended_strict_qc run_if and,!union_annots,exrunif run_with annot_variant_subset bsub_batch 50

!|expand@;@shortt;phenol;burdenl;annotl;exskipif;exrunif@;pheno;burden;annot;;!burden_variant_subset@short;pheno_variant_subset;burden_variant_subset;annot_variant_subset;,expand_pheno_subsets:eq:1;burden_variant_subset bsub_batch 50| \
short cmd make_burdenl_all_variant_list_file=cat !{input,,burdenl_non_annot_variant_list_file} !{raw,,annotl,| cat - *annotl_annot_variant_list_file | sort | uniq -d,if_prop=annot_mask,allow_empty=1,max=1} !{input,annotl_annot_variant_list_file,if_prop=annot_mask,allow_empty=1,max=1} > !{output,,burdenl_all_variant_list_file} class_level burdenl run_if and,!union_burdens,exrunif skip_if and,!only_for_interesting,!burden_mac_lb,!burden_mac_ub,!burden_mafexskipif run_with burden_variant_subset

!|expand@;@shortt;phenol;annotl;exskipif;exrunif@;pheno;annot;;!annot_variant_subset@short;pheno_variant_subset;annot_variant_subset;;annot_variant_subset bsub_batch 50| \
short cmd make_annotl_all_variant_list_file=cat !{input,,annotl_non_annot_variant_list_file} | cat - !{input,,annotl_annot_variant_list_file} | sort | uniq -d > !{output,,annotl_all_variant_list_file} class_level annotl run_if and,!union_annots,exrunif skip_if and,!annot_mac_lb,!annot_mac_ub,!annot_maf,!apply_extended_qc,!apply_extended_strict_qcexskipif run_with annot_variant_subset

!|expand@;@shortt;phenol;burdenl;annotl;exrunif;exskipif@;pheno;burden;annot;!burden_variant_subset;@short;pheno_variant_subset;burden_variant_subset;annot_variant_subset;burden_variant_subset;,expand_pheno_subsets:ne:1| \
local cmd ln_burdenl_all_variant_list_file=ln -s !{input,,annotl_all_variant_list_file,max=1} !{output,,burdenl_all_variant_list_file} class_level burdenl run_if and,exrunif skip_if or,only_for_interesting,burden_mac_lb,burden_mac_ub,burden_mafexskipif run_with burden_variant_subset

!|expand:;:annotl;exrunif:annot;!annot_variant_subset:annot_variant_subset;annot_variant_subset| \
local cmd ln_annotl_all_variant_list_file=ln -s !{input,,annotl_annot_variant_list_file} !{output,,annotl_all_variant_list_file} class_level annotl run_if and,exrunif skip_if or,annot_mac_lb,annot_mac_ub,annot_maf,apply_extended_qc,apply_extended_strict_qc run_with annot_variant_subset

bad_regions_regex_helper=$smart_cut_cmd --exec "echo !{prop,,burden_test,bad_regions_for_genes,missing=,sep=\,,if_prop=test_software:eq:pseq,allow_empty=1} | sed 's/,/\n/g'" | awk 'NF > 0' !{raw;;burden_test;| $smart_cut_cmd --exec "awk '{print \"chr\"\\$1\":\"\\$2}' *burden_chr_pos_exclude_file";if_prop=burden_chr_pos_exclude_file;allow_empty=1} !{input,burden_chr_pos_exclude_file,if_prop=burden_chr_pos_exclude_file,allow_empty=1} | sed 's/chrchr/chr/' | sort -u

!|expand:;:projectl;phenol;clean_gene_variant_file;burdenl;exskipif;excons:\
project;pheno;project_clean_gene_variant_file;burden;burden_variant_subset;:\
project_variant_subset;pheno_variant_subset;pheno_variant_subset_clean_gene_variant_file;burden_variant_subset;!burden_variant_subset;consistent_prop var_subset_num bsub_batch 20| \
!|expand%;%hwetype;hwefiles;hweskipif%\
my_hwe_pheno;!{input,--file,phenol_hwe_file};phenos_for_p_hwe%\
other_hwe_phenos;!{input,--file,phenol_hwe_file,all_instances=1,if_prop=project:eq:@project,if_prop=pheno:eq:@phenos_for_p_hwe,instance_level=phenol};!phenos_for_p_hwe| \
short cmd make_burdenl_hwetype_idex_file=$smart_cut_cmd --out-delim $tab !{input,--file,phenol_test_missing_file,unless_prop=pheno_qt,allow_empty=1} !{raw;;pheno;--select-col 1,1,'SNP P' --and-row-all --select-row 1,1 --select-row 1,1,P,le:;unless_prop=pheno_qt;allow_empty=1}!{prop,,burden,min_test_p_missing,missing=0,unless_prop=pheno_qt,allow_empty=1} !{raw,--file,burden,/dev/null,if_prop=pheno_qt,allow_empty=1} --exec "$smart_cut_cmd --in-delim $tab hwefiles --exclude-row .,1 | $smart_cut_cmd !{input,--file,phenol_hwe_file} --select-row 1,1" --select-row 2,1,TEST,eq:ALL --select-col 2,1,'SNP P' --select-row 2,1,P,le:!{prop,,burden,min_test_p_hwe,missing=0} --exclude-row 2,1 !{input,--file,phenol_lmiss_file} --select-col 3,1,'SNP F_MISS' --exclude-row 3,1 --select-row 3,1,F_MISS,ge:!{prop,,burden,max_assoc_null,missing=1} !{raw::burden:| eval `perl -e 'print qq(@custom_burden_test_exclude_filters)' | sed 's/\s\s*/\n/g' | sed 's/\($select_and_delim\|^\)/ --exclude-row 1,1 --select-row 1,1,/g' | sed 's/$select_filter_delim/,/g' | sed 's;^;$smart_cut_cmd --file *projectl_extended_vstats_file --in-delim $vstats_delim --and-row-all --select-col 1,1 --select-col 1,2 ;g' | tr '\n' '|' | sed 's/|$//'` | awk -F"\t" -v OFS\="\t" 'NF \=\= 1 {sub(/$vstats_delim/,"\t",\$0)} {print \$0}':if_prop=custom_burden_test_exclude_filters:allow_empty=1} !{input,projectl_extended_vstats_file,if_prop=custom_burden_test_exclude_filters,allow_empty=1,instance_level=burden} > !{output,,burdenl_idex_file} class_level burdenl skip_if or,exskipif,hweskipif run_if or,min_test_p_missing,min_test_p_hwe,max_assoc_null,custom_burden_test_exclude_filters,burden_chr_pos_exclude_file run_with burden_variant_subset excons

!|expand:;:projectl;phenol;clngnvfile;gnvfile;burdenl;exskipif;excons:\
project;pheno;project_clean_gene_variant_file;project_gene_variant_file;burden;burden_variant_subset;:\
project_variant_subset;pheno_variant_subset;pheno_variant_subset_clean_gene_variant_file;pheno_variant_subset_gene_variant_file;burden_variant_subset;!burden_variant_subset;consistent_prop var_subset_num bsub_batch 20| \
short cmd make_burdenl_regex_file=$smart_join_cmd --in-delim $tab --exec "$smart_cut_cmd --in-delim $tab !{input,--file,gnvfile,if_prop=use_raw_variants,allow_empty=1} !{input,--file,clngnvfile,unless_prop=use_raw_variants,allow_empty=1} --select-col 1,1,ID | tail -n+2 | cat - !{input,,burdenl_idex_file}  | cut -f1 | sort | uniq -d" --exec "$smart_cut_cmd --in-delim $tab !{input,--file,gnvfile,if_prop=use_raw_variants,allow_empty=1} !{input,--file,clngnvfile,unless_prop=use_raw_variants,allow_empty=1} --select-col 1,1,'ID CHROM POS' --exclude-row 1,1" !{input,--file,burdenl_idex_file} --rest-extra 1 --multiple 3 | tail -n+2 | cut -f2-3 | sort -u | sed 's/^/chr/' | awk '{print \$1":"\$2}' | $bad_regions_regex_helper > !{output,,burdenl_regex_file} class_level burdenl skip_if exskipif run_if or,min_test_p_missing,min_test_p_hwe,max_assoc_null,custom_burden_test_exclude_filters,burden_chr_pos_exclude_file run_with burden_variant_subset excons

local cmd make_burden_clean_variant_list_file=cut -f1 !{input,,pheno_vassoc_clean_annot_file} | cat - !{input,,burden_all_variant_list_file} | sort | uniq -d > !{output,,burden_clean_variant_list_file} class_level burden

#First get the annot transcripts
#Then get all variants, annotated by gene (NA transcripts)
#Then intersect with non annot


!|expand:;:annotype:annot:no_annot| \
!|expand:;:projectl:project:project_variant_subset| \
projectl_annotype_locdb_detail_reg_helper=$smart_join_cmd --in-delim $tab --extra 1 --multiple 2 --exec "$smart_cut_cmd --in-delim $tab !{input,--file,projectl_gene_variant_file,if_prop=use_raw_variants,allow_empty=1} !{input,--file,projectl_clean_gene_variant_file,unless_prop=use_raw_variants,allow_empty=1}  --select-col 1,1,'ID CHROM POS' --exclude-row 1,1" --exec "$smart_cut_cmd --in-delim $tab --exec \"$annotype_variant_list_helper(projectl_full_annot_file,--select-row 1\,1\,$vep_ccds_annot\,ne:$annot_missing_field --select-col 1\,2,\\\) | $smart_cut_cmd --in-delim $tab --exec \\\"cut -f1 !{input,,project_transcript_target_file} | sed 's/\(.*\)\..*$/\1/'\\\" | awk -F$tab -v OFS=$tab 'NF == 1 {m[\\\\$1]=1} NF > 1 && m[\\\\$2] {print}' | cat !{input,,@1} - | awk -F$tab -v OFS=$tab 'NF == 1 {m[\\\\$1]=1} NF > 1 && m[\\\\$1] {print}'\" --exec \"$smart_join_cmd --rest-extra 1 --exec \\\"$smart_cut_cmd --in-delim $tab !{input,--file,projectl_gene_variant_file,if_prop=use_raw_variants,allow_empty=1} !{input,--file,projectl_clean_gene_variant_file,unless_prop=use_raw_variants,allow_empty=1} !{input,--file,@1} --exclude-row 1,1 --select-col 1,1,ID | sort | uniq -d\\\"  --exec \\\"$smart_cut_cmd --in-delim $tab !{input,--file,projectl_gene_variant_file,if_prop=use_raw_variants,allow_empty=1} !{input,--file,projectl_clean_gene_variant_file,unless_prop=use_raw_variants,allow_empty=1} --exclude-row 1,1 --select-col 1,1,'ID CHROM POS GENE' --exact --require-col-match\\\"\" --select-col 1,'1 2' --select-col 2,'1 4' | $smart_cut_cmd --in-delim $tab --exec \"$smart_cut_cmd --in-delim $tab !{input,--file,projectl_gene_variant_file,if_prop=use_raw_variants,allow_empty=1} !{input,--file,projectl_clean_gene_variant_file,unless_prop=use_raw_variants,allow_empty=1} --select-col 1,1,ID --exclude-row 1,1 @2 | sort | uniq -d\" | awk -F$tab -v OFS=$tab 'NF == 1 {m[\\$1]=1} NF > 1 && m[\\$1] {print}'" | awk -F"\t" -v OFS="\t" 'NR == 1 {print "\\\#CHR","POS1","POS2","ID","VAR"} \$4 != "$outside_gene_name" {print \$2,\$3,\$3,\$4,\$1}' | sed '1! s/^/chr/' | sed '1! s/chrchr/chr/'

!|expand:;:annotl;projectl;exrunif:annot;project;run_if !annot_variant_subset:annot_variant_subset;project_variant_subset;run_if annot_variant_subset bsub_batch 20| \
short cmd make_annotl_no_maf_locdb_detail_reg_file=$projectl_annot_locdb_detail_reg_helper(annotl_all_variant_list_file,| $smart_cut_cmd --in-delim $tab !{input:--file:projectl_gene_variant_file:if_prop=use_raw_variants:allow_empty=1} !{input:--file:projectl_clean_gene_variant_file:unless_prop=use_raw_variants:allow_empty=1} --select-col 1\,1\,ID --exclude-row 1\,1) > !{output,,annotl_locdb_detail_reg_file} class_level annotl exrunif run_with annot_variant_subset skip_if or,annot_mac_lb,annot_mac_ub,annot_maf,union_annots

!|expand:;:annotl;projectl;exrunif:annot;project;!annot_variant_subset:annot_variant_subset;project_variant_subset;annot_variant_subset bsub_batch 20| \
short cmd make_annotl_locdb_detail_reg_file=$projectl_annot_locdb_detail_reg_helper(annotl_all_variant_list_file,!{input:--file:annotl_non_annot_variant_list_file}) > !{output,,annotl_locdb_detail_reg_file} class_level annotl run_if and,!union_annots,exrunif run_with annot_variant_subset skip_if and,!annot_mac_lb,!annot_mac_ub,!annot_maf

!|expand@;@burdenl;projectl;exrunif;ifskip@burden;project;run_if !burden_variant_subset;@burden_variant_subset;project_variant_subset;run_if and,burden_variant_subset bsub_batch 20;,expand_pheno_subsets:eq:1| \
short cmd make_burdenl_no_annot_locdb_detail_reg_file=$projectl_no_annot_locdb_detail_reg_helper(burdenl_all_variant_list_file,!{input:--file:burdenl_non_annot_variant_list_file}) > !{output,,burdenl_locdb_detail_reg_file} class_level burdenl exrunif skip_if or,(and,!only_for_interesting,!burden_mac_lb,!burden_mac_ub,!burden_mafifskip),annot_mask,union_burdens run_with burden_variant_subset

!|expand@;@burdenl;annotl;projectl;exrunif;ifskip@burden;annot;project;run_if !burden_variant_subset;@burden_variant_subset;annot_variant_subset;project_variant_subset;run_if and,burden_variant_subset bsub_batch 20;,expand_pheno_subsets:eq:1| \
short cmd make_burdenl_annot_locdb_detail_reg_file=$smart_join_cmd --in-delim $tab --exec "tail -n+2 !{input,,annotl_locdb_detail_reg_file,max=1} | cut -f5 | sort -u | sort - !{input,,burdenl_all_variant_list_file} | uniq -d | sed '1 s/^/VAR\n/'" !{input,--file,annotl_locdb_detail_reg_file,max=1} --header 1 --multiple 2 --rest-extra 1 --col 2,5 | awk -F"\t" -v OFS="\t" '{print \$2,\$3,\$4,\$5,\$1}' > !{output,,burdenl_locdb_detail_reg_file} class_level burdenl exrunif skip_if or,(and,!only_for_interesting,!burden_mac_lb,!burden_mac_ub,!burden_mafifskip),!annot_mask,union_burdens run_with burden_variant_subset

!|expand@;@burdenl;annotl;exrunif;exskipif@burden;annot;run_if !burden_variant_subset;@burden_variant_subset;annot_variant_subset;run_if burden_variant_subset;,expand_pheno_subsets:ne:1| \
local cmd ln_burdenl_locdb_detail_reg_file=ln -s !{input,,annotl_locdb_detail_reg_file,max=1} !{output,,burdenl_locdb_detail_reg_file} class_level burdenl exrunif skip_if or,only_for_interesting,burden_mac_lb,burden_mac_ub,burden_mafexskipif run_with burden_variant_subset

#if you don't have annot_mask, and you don't have any freq masks, nothing will run

!|expand:;:burdenl;shortt:burden;short:burden_variant_subset;local| \
shortt cmd make_burdenl_locdb_reg_file=cut -f1-4 !{input,,burdenl_locdb_detail_reg_file} > !{output,,burdenl_locdb_reg_file} class_level burdenl

!|expand:;:burdenl;shortt:burden;short:burden_variant_subset;local| \
shortt cmd make_burdenl_reg_file=cut -f1-2 !{input,,burdenl_locdb_detail_reg_file} | tail -n+2 | sed 's/\s\s*/:/' > !{output,,burdenl_reg_file} class_level burdenl

#NEED TO FIX
#1) BROKEN SYNTAC (so many quotes)
#2) Need To DOUBLE PRINT THE EXCLUSIONS

!!expand:,:burdenl,projectl:burden,project:burden_variant_subset,project_variant_subset! \
burdenl_epacts_group_filter=$smart_cut_cmd --in-delim $tab --exec \"$smart_join_cmd --in-delim $tab --exec 'cat !{input,,burden_chr_pos_exclude_file,if_prop=burden_chr_pos_exclude_file,allow_empty=1} !{input,,burdenl_regex_file,if_prop=min_test_p_missing,if_prop=min_test_p_hwe,if_prop=max_assoc_null,if_prop=custom_burden_test_exclude_filters,or_if_prop=1,allow_empty=1} /dev/null | sed \\\"s/:/\t/\\\" | awk \\\"NF == 2\\\" | sed \\\"s/\s\s*/\t/g\\\" | sed \\\"s/^chr//\\\" | sort -u | $smart_cut_cmd --tab-delim !{input,--file,projectl_gene_variant_file,if_prop=use_raw_variants,allow_empty=1} !{input,--file,projectl_clean_gene_variant_file,unless_prop=use_raw_variants,allow_empty=1} --exclude-row 1,1 --select-col 1,1,CHROM --select-col 1,1,POS | sed \\\"s/^chr//\\\" | sort | uniq -d' --exec \\\"$smart_cut_cmd --tab-delim !{input,--file,projectl_gene_variant_file,if_prop=use_raw_variants,allow_empty=1} !{input,--file,projectl_clean_gene_variant_file,unless_prop=use_raw_variants,allow_empty=1} --exclude-row 1,1 --select-col 1,1,CHROM --select-col 1,1,POS --select-col 1,1,ID | sed 's/^chr//'\\\" --col 1 --col 2 --extra 2 --multiple 2 | cut -f3 | sed 's/\(\S\S*\)/\1\n\1/'\" | sort | uniq -u

group_flatten=awk -F"\t" -v OFS="\t" 'NR > 1 && g != \$2 {print g,v;v=""} {g=\$2;v=v" "\$1} END {print g,v}' | sed 's/\t /\t/'

prop burden_max_variants_per_epacts_group=scalar

!!expand:burdent:burden:annot! \
burdent_epacts_group_max_variants_filterer=awk -F"\t" '{split(\$2, a, " "); if (length(a) <= !{prop,,burdent,burden_max_variants_per_epacts_group}) {print}}'

!@expand%;%classl;parentp;burdenl;projectl;extraselect;exrunif;exskipif\
%annot;annot;annot;project;;!annot_variant_subset;\
%annot_variant_subset;annot;annot_variant_subset;project_variant_subset;;annot_variant_subset;bsub_batch 20\
%burden;burden;burden;project;| $burden_epacts_group_filter;!burden_variant_subset;skip_if and,!burden_mac_lb,!burden_mac_ub,!burden_maf,!min_test_p_missing,!min_test_p_hwe,!max_assoc_null,!custom_burden_test_exclude_filters,!burden_chr_pos_exclude_file\
%burden_variant_subset;burden;burden_variant_subset;project_variant_subset;| cat - !{input,,pheno_variant_subset_var_keep_file} | sort | uniq -d | $burden_variant_subset_epacts_group_filter;burden_variant_subset;skip_if and,!burden_mac_lb,!burden_mac_ub,!burden_maf,!min_test_p_missing,!min_test_p_hwe,!max_assoc_null,!custom_burden_test_exclude_filters,!burden_chr_pos_exclude_file,expand_pheno_subsets:eq:1\
@ \
short cmd make_classl_epacts_group_file=$smart_join_cmd --in-delim $tab --exec "tail -n+2 !{input,,burdenl_locdb_detail_reg_file} | cut -f5 | sort -u extraselect" --exec "$smart_join_cmd --in-delim $tab --merge --col 3 --exec \"cat !{input,,projectl_clean_all_variant_site_vcf_file,unless_prop=use_raw_variants,allow_empty=1,instance_level=classl} !{input,,projectl_variant_site_vcf_file,if_prop=use_raw_variants,allow_empty=1,instance_level=classl} | fgrep -v '\\#' | cut -f1-5\" --exec \"cat !{input,,projectl_vfreq_file} | $parse_out_id | $smart_cut_cmd --in-delim $tab --exclude-row 0,1 --select-col 0,1,'CHR POS VAR REF ALT' --exact --require-col-match\" | awk -F$tab -v OFS=$tab '{t=\\$1; \\$1=\\$2; \\$2=\\$3; \\$3=t} {print}'" --exec "tail -n+2 !{input,,burdenl_locdb_detail_reg_file} | cut -f4,5" --rest-extra 1 --multiple 3 --col 2,3 --col 3,2 | cut -f2- | sort -k 5,5 -k 1,1n -k 2,2n | awk -F"\t" -v OFS="\t" '{print \$1":"\$2"_"\$3"/"\$4,\$5}' | $group_flatten | sed 's/\t /\t/' | $parentp_epacts_group_max_variants_filterer > !{output,,classl_epacts_group_file} class_level classl run_with burden_variant_subset,burden_test_variant_subset exskipif run_if and,!union_parentps,exrunif

!|expand%;%burdenl;annotl;exrunif;exskipif%burden;annot;run_if !burden_variant_subset;%burden_variant_subset;annot_variant_subset;run_if burden_variant_subset;,expand_pheno_subsets:ne:1| \
local cmd ln_burdenl_epacts_group_file=ln -s !{input,,annotl_epacts_group_file,max=1} !{output,,burdenl_epacts_group_file} class_level burdenl exrunif skip_if or,burden_max_variants_per_epacts_group,burden_mac_lb,burden_mac_ub,burden_maf,min_test_p_missing,min_test_p_hwe,max_assoc_null,custom_burden_test_exclude_filters,burden_chr_pos_exclude_fileexskipif

!|expand%;%burdenl;annotl;exrunif;exskipif%burden;annot;run_if !burden_variant_subset;%burden_variant_subset;annot_variant_subset;run_if burden_variant_subset;,expand_pheno_subsets:ne:1| \
local cmd cap_burdenl_epacts_group_file=cat !{input,,annotl_epacts_group_file,max=1} | $burden_epacts_group_max_variants_filterer > !{output,,burdenl_epacts_group_file} class_level burdenl exrunif skip_if or,!burden_max_variants_per_epacts_group,burden_mac_lb,burden_mac_ub,burden_maf,min_test_p_missing,min_test_p_hwe,max_assoc_null,custom_burden_test_exclude_filters,burden_chr_pos_exclude_fileexskipif

!@expand%;%classl;parentp;burdenl;projectl;extraselect;exrunif;exskipif\
%annot;annot;annot;project;;!annot_variant_subset;\
%annot_variant_subset;annot;annot_variant_subset;project_variant_subset;;annot_variant_subset;bsub_batch 20\
%burden;burden;burden;project;| $burden_epacts_group_filter;!burden_variant_subset;skip_if and,!burden_mac_lb,!burden_mac_ub,!burden_maf,!min_test_p_missing,!min_test_p_hwe,!max_assoc_null,!custom_burden_test_exclude_filters,!burden_chr_pos_exclude_file\
%burden_variant_subset;burden;burden_variant_subset;project_variant_subset;| cat - !{input,,pheno_variant_subset_var_keep_file} | sort | uniq -d | $burden_variant_subset_epacts_group_filter;burden_variant_subset;skip_if and,!burden_mac_lb,!burden_mac_ub,!burden_maf,!min_test_p_missing,!min_test_p_hwe,!max_assoc_null,!custom_burden_test_exclude_filters,!burden_chr_pos_exclude_file,expand_pheno_subsets:eq:1\
@ \
short cmd make_classl_setid_file=$smart_join_cmd --in-delim $tab --exec "tail -n+2 !{input,,burdenl_locdb_detail_reg_file} | cut -f5 | sort -u extraselect" --exec "tail -n+2 !{input,,burdenl_locdb_detail_reg_file} | cut -f4,5" --rest-extra 1 --multiple 2 --col 2,2 | awk -v OFS="\t" -F"\t" '{print \$2,\$1}' > !{output,,classl_setid_file} class_level classl run_with burden_variant_subset,burden_test_variant_subset exskipif run_if and,!union_parentps,exrunif

!|expand%;%burdenl;annotl;exrunif;exskipif%burden;annot;run_if !burden_variant_subset;%burden_variant_subset;annot_variant_subset;run_if burden_variant_subset;,expand_pheno_subsets:ne:1| \
local cmd ln_burdenl_setid_file=ln -s !{input,,annotl_setid_file,max=1} !{output,,burdenl_setid_file} class_level burdenl exrunif skip_if or,burden_mac_lb,burden_mac_ub,burden_maf,min_test_p_missing,min_test_p_hwe,max_assoc_null,custom_burden_test_exclude_filters,burden_chr_pos_exclude_file,expand_pheno_subsets:ne:1

!!expand%;%shortt;annotl;exskipif;exif;ifprop;consistentprop%short;annot;or,annot_variant_subset,;;;%local;annot_variant_subset;;;,if_prop=var_subset_num:eq:@var_subset_num;consistent_prop var_subset_num! \
!!expand%;%atype;sortkey;runif\
%all_variant_list_file;1;run_if or,annot_mac_lb,annot_mac_ub,annot_maf,apply_extended_qc,apply_extended_strict_qcexif\
%annot_variant_list_file;1;\
%non_annot_variant_list_file;1;\
%setid_file;2;\
!\
shortt cmd make_union_annotl_atype=sort -ksortkey !{input,,annotl_atype,all_instances=1,if_prop=project:eq:@project,if_prop=annot:eq:@union_annotsifprop} | uniq > !{output,,annotl_atype} class_level annotl skip_if exskipif!union_annots runif consistentprop run_with annot_variant_subset

!!expand%;%shortt;burdenl;exskipif;exif;ifprop;consistentprop%short;burden;or,burden_variant_subset,;;;%local;burden_variant_subset;;,expand_pheno_subsets:ne:1;,if_prop=var_subset_num:eq:@var_subset_num;consistent_prop var_subset_num! \
!!expand%;%atype;sortkey;runif\
%all_variant_list_file;1;run_if or,only_for_interesting,burden_mac_lb,burden_mac_ub,burden_mafexif\
%non_annot_variant_list_file;1;\
%setid_file;2;run_if or,burden_mac_lb,burden_mac_ub,burden_maf,min_test_p_missing,min_test_p_hwe,max_assoc_null,custom_burden_test_exclude_filters,burden_chr_pos_exclude_fileexif\
!\
shortt cmd make_union_burdenl_atype=sort -ksortkey !{input,,burdenl_atype,all_instances=1,if_prop=project:eq:@project,if_prop=burden:eq:@union_burdensifprop} | uniq > !{output,,burdenl_atype} class_level burdenl skip_if exskipif!union_burdens runif consistentprop


!!expand%;%shortt;burdenl;burdenp;exskipif;runif;ifprop;consistentprop\
%short;burden;burden;or,burden_variant_subset,;run_if or,burden_mac_lb,burden_mac_ub,burden_maf,min_test_p_missing,min_test_p_hwe,max_assoc_null,custom_burden_test_exclude_filters,burden_chr_pos_exclude_file;;\
%local;burden_variant_subset;burden;;run_if or,burden_mac_lb,burden_mac_ub,burden_maf,min_test_p_missing,min_test_p_hwe,max_assoc_null,custom_burden_test_exclude_filters,burden_chr_pos_exclude_file,expand_pheno_subsets:ne:1;,if_prop=var_subset_num:eq:@var_subset_num;consistent_prop var_subset_num\
%short;annot;annot;or,annot_variant_subset,;;;\
%local;annot_variant_subset;annot;;;,if_prop=var_subset_num:eq:@var_subset_num;consistent_prop var_subset_num! \
!!expand%;%atype;sortkey\
%locdb_detail_reg_file;5\
!\
shortt cmd make_union_header_burdenl_atype=(head -qn1 !{input,,burdenl_atype,all_instances=1,limit=1,if_prop=project:eq:@project,if_prop=burdenp:eq:@union_burdenpsifprop} && tail -qn+2 !{input,,burdenl_atype,all_instances=1,if_prop=project:eq:@project,if_prop=burdenp:eq:@union_burdenpsifprop} | sort -ksortkey) | uniq > !{output,,burdenl_atype} class_level burdenl skip_if exskipif!union_burdenps runif consistentprop

!!expand%;%shortt;burdenl;burdenp;exskipif;runif;ifprop;consistentprop\
%short;burden;burden;or,burden_variant_subset,;run_if or,only_for_interesting,burden_mac_lb,burden_mac_ub,burden_maf;;\
%local;burden_variant_subset;burden;;run_if or,only_for_interesting,burden_mac_lb,burden_mac_ub,burden_maf,expand_pheno_subsets:ne:1;,if_prop=var_subset_num:eq:@var_subset_num;consistent_prop var_subset_num\
%short;annot;annot;or,annot_variant_subset,;;;\
%local;annot_variant_subset;annot;;;,if_prop=var_subset_num:eq:@var_subset_num;consistent_prop var_subset_num\
! \
shortt cmd make_union_burdenl_epacts_group_file=cat !{input,,burdenl_epacts_group_file,all_instances=1,if_prop=project:eq:@project,if_prop=burdenp:eq:@union_burdenpsifprop} | awk -v OFS="\t" '{for (i=2;i<=NF;i++) {print \$i,\$1}}' | sort -k2,2 -k1,1 | uniq | $group_flatten | sed 's/\t /\t/' > !{output,,burdenl_epacts_group_file} class_level burdenl runif skip_if exskipif!union_burdenps consistentprop

#!|expand:;:burdenl;exrunif:burden;:burden_variant_subset;run_if burden_variant_subset| \
#short cmd make_burdenl_locdb_file=rm -f !{output,,burdenl_locdb_file} && $pseq_no_project_cmd loc-load !{output,--locdb,burdenl_locdb_file} --group $pseq_genes_loc_group !{input,--file,burdenl_locdb_reg_file} && $pseq_no_project_cmd loc-load-alias !{output,--locdb,burdenl_locdb_file} !{input,--file,project_transcript_gene_alias_file} class_level burdenl exrunif

!|expand:;:burdenl;exrunif:burden;:burden_variant_subset;run_if burden_variant_subset| \
short cmd make_burdenl_locdb_file=rm -f !{output,,burdenl_locdb_file} && $pseq_no_project_cmd loc-load !{output,--locdb,burdenl_locdb_file} --group $pseq_genes_loc_group !{input,--file,burdenl_locdb_reg_file} class_level burdenl exrunif

!!expand:pathwayburdentype:custom! \
local cmd make_burden_pathway_pathwayburdentype_locdb_file=rm -f !{output,,burden_pathway_pathwayburdentype_locdb_file} && cp !{input,,burden_locdb_file} !{output,,burden_pathway_pathwayburdentype_locdb_file} && $pseq_no_project_cmd locset-load !{output,--locdb,burden_pathway_pathwayburdentype_locdb_file} --group $pseq_pathwayburdentype_loc_group --name $pseq_pathwayburdentype_locset_name !{input,--file,project_pathwayburdentype_locset_file} class_level burden run_if project_pathwayburdentype_locset_file skip_if only_for_interesting

!!expand:,:burdenl:burden:annot! \
!!expand:pathwayburdentype:custom! \
short cmd make_burdenl_pathway_pathwayburdentype_epacts_group_file=$smart_join_cmd --in-delim $tab --rest-extra 1 --multiple 2 --exec "$smart_cut_cmd --exec 'cut -f1 !{input,,project_pathwayburdentype_locset_file} | sort -u' --exec 'cut -f1 !{input,,burdenl_epacts_group_file} | sort -u' | sort | uniq -d" !{input,--file,project_pathwayburdentype_locset_file} !{input,--file,burdenl_epacts_group_file} | cut -f2- | sort -k1,1 -k2,2 | uniq -u | awk -F"\t" -v OFS="\t" 'NR > 1 && g != \$1 {print g,v;v=""} {g=\$1;v=v" "\$2} END {print g,v}' | sed 's/\t /\t/' > !{output,,burdenl_pathway_pathwayburdentype_epacts_group_file} class_level burdenl run_if project_pathwayburdentype_locset_file skip_if only_for_interesting

!!expand:burdenl:burden:annot! \
raw_burdenl_mask_helper=--mask !{raw,,burdenl,mac\=,if_prop=burdenl_mac_lb,if_prop=burdenl_mac_ub,or_if_prop=1,allow_empty=1}!{prop,,burdenl,burdenl_mac_lb,if_prop=burdenl_mac_lb,allow_empty=1}!{raw,,burdenl,0,unless_prop=burdenl_mac_lb,if_prop=burdenl_mac_ub,allow_empty=1}!{raw,,burdenl,-,if_prop=burdenl_mac_lb,if_prop=burdenl_mac_ub,or_if_prop=1,allow_empty=1}!{prop,,burdenl,burdenl_mac_ub,if_prop=burdenl_mac_ub,allow_empty=1}

#raw_burdenl_mask_helper=--mask !{raw,maf=,burdenl,0-,if_prop=burdenl_maf,unless_prop=burdenl_strat_maf_summary,allow_empty=1}!{prop,,burdenl,burdenl_maf,if_prop=burdenl_maf,unless_prop=burdenl_strat_maf_summary,allow_empty=1} !{raw,,burdenl,mac\=,if_prop=burdenl_mac_lb,if_prop=burdenl_mac_ub,or_if_prop=1,allow_empty=1}!{prop,,burdenl,burdenl_mac_lb,if_prop=burdenl_mac_lb,allow_empty=1}!{raw,,burdenl,0,unless_prop=burdenl_mac_lb,if_prop=burdenl_mac_ub,allow_empty=1}!{raw,,burdenl,-,if_prop=burdenl_mac_lb,if_prop=burdenl_mac_ub,or_if_prop=1,allow_empty=1}!{prop,,burdenl,burdenl_mac_ub,if_prop=burdenl_mac_ub,allow_empty=1}

!!expand:burdenl:burden:burden_variant_subset! \
burdenl_mask_helper=!{input,--locdb,burdenl_locdb_file}
#burdenl_mask_helper=reg.req=@!{input,,burdenl_reg_list_file}

!!expand:pathwayburdentype:custom! \
burden_pathway_pathwayburdentype_mask_helper=!{input,--locdb,burden_pathway_pathwayburdentype_locdb_file}

!{raw,,burden,-,if_prop=burden_mac_lb,if_prop=burden_mac_ub,allow_empty=1}!{prop,,burden,burden_mac_ub,if_prop=burden_mac_ub,allow_empty=1}

!!expand:burdenl:burden:burden_variant_subset! \
burdenl_gassoc_helper=--perm @1 --mask loc.group=$pseq_genes_loc_group $burdenl_mask_helper $burden_test_indiv_ex_helper @2 | $remove_dup_genes | sed 's/^\s*//'

#cmd make_burden_level1_gassoc_file=$pseq_qc_plus_assoc_cmd_pheno($assoc) !{prop,--phenotype,pheno} $burden_gassoc_helper($nperm1_gene,) > !{output,,burden_level1_gassoc_file} class_level burden skip_if not_trait

burden_sample_list_var_delim=,

burden_sample_list_helper_int=$smart_join_cmd --in-delim $tab --header 1 --extra 2 --ignore-err $plinkseq_okay_err --exec "(echo VAR && cat !{input,,@1})" --exec "if [[ `cat !{input,,@1} | wc -l` -gt 0 ]]; then $pseq_qc_plus_all_analysis_cmd_pheno_variant_subset(v-matrix) $show_id --mask reg.req=@!{input,,@2} | $smart_cut_cmd --exclude-col 0,2-3 --in-delim $tab | $parse_out_id; else echo VAR; fi" | perl -lne 'chomp; \@F = split("\t"); \$id = shift \@F; if (!\$first) {\$first = 1; \$nf = scalar \@F; for (my \$i = 0; \$i < \$nf; \$i++) {\$t[\$i]=0;\$v[\$i]=""}; print join("\t", \@F)} else {die "Not square" unless \$nf == scalar \@F; \$s = 0; \$n = 0; map {if (\$_ =~ /^[0-9]+$/) {\$s += \$_; \$n+= 2}} \@F; \$f = \$n > 0 ? \$s / \$n : 0; if (\$f > .5) {map {\$F[\$_] = 2 - \$F[\$_] if \$F[\$_] =~ /^[0-9]+$/} 0..\$nf} map {if (\$F[\$_] =~ /^[0-9]+$/){\$t[\$_]+=\$F[\$_]; if (\$F[\$_] > 0) {\$v[\$_].="$burden_sample_list_var_delim" if \$v[\$_]; \$v[\$_].="\$id"} }} 0..\$nf} END {print join("\t", \@t);print join("\t", \@v)}' | $transpose_cmd --in-delim $tab | $smart_cut_cmd --select-col 0,@5 --select-row 0,2,@3:0 --in-delim $tab  > !{output,,@4}

gene_burden_or_variant_sstats_helper=$smart_join_cmd --out-delim $tab --header 1 --exec "cut -d@3 -f1 !{input,,@2} | $smart_cut_cmd --in-delim $tab !{input,--file,@{1}_sample_without_list_file} !{input,--file,@{1}_sample_list_file} --select-col 1,1 --select-col 2,1 | sort | uniq -d | sed '1 s/^/ID\n/'" --exec "$smart_cut_cmd --in-delim $tab --exec \"cut -f1 !{input,,@{1}_sample_list_file} | sed 's/$/\t1\tHas Variant/'\" --exec \"cut -f1 !{input,,@{1}_sample_without_list_file} | sed 's/$/\t0\tNo Variant/'\" | sed '1 s/^/ID\tVariant\tDisp\n/'" !{input,--file,@2} --rest-extra 1 --arg-delim : --in-delim 3:@3 --in-delim 2:$tab
burden_sstats_helper=$gene_burden_or_variant_sstats_helper(burden,@1,@2)

record_burden_test_info_int=(echo Test$tab!{prop,,burden_test,test_tag,missing_prop=test_name,sep=/,uc=1} && \
	 echo Software$tab!{raw,,burden_test,\@test_software} && \
	 echo Test name${tab}@1 && \
	 echo Samples${tab}@10 && \
	 echo Covariates${tab}@2 && \
	 echo Clustering${tab}@3 && \
	 echo GT cutoff${tab}@4 && \
	 echo Permutations${tab}@5 && \
	 echo Make two-sided${tab}@6 && \
	 echo Max missing${tab}@7 && \
	 echo P missing cutoff${tab}@8 && \
	 echo HWE cutoff${tab}@9 && \
	 echo PI_HAT cutoff$tab!{raw,,burden_test,\@max_related,unless_prop=include_related,allow_empty=1}!{raw,,burden_test,1,if_prop=include_related,allow_empty=1} \
	 )> !{output,,burden_test_info_file}

record_burden_test_info=$record_burden_test_info_int(!{prop;;burden_test;test_name} !{prop;;burden_test;test_options;if_prop=test_options;allow_empty=1},@1,@2,@3,@4,@5,@6,@7,@8,`cat @9 | wc -l`)

"""
}
    