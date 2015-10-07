
package loamstream.pipeline.examples

/**
 * LoamStream
 * Created by oliverr on 10/6/2015.
 */
object PipelineTargetedPart28 {
  val string =
 """				 --exec \"$vassoc_subset_helper($vcf_type_nonsense_annot,$var_counts_nonsense_type)\" \
				 --exec \"$vassoc_subset_helper($vcf_type_missense_annot,$var_counts_missense_type)\" \
				 --exec \"$vassoc_subset_helper($vcf_type_synonymous_annot,$var_counts_synonymous_type)\" \
				 --exec \"$vassoc_subset_helper(.,$var_counts_other_type)\" \
  | awk '!map[\\$1] {print; map[\\$1] = 1}' "\
  | @1 \
  | $smart_cut_cmd --in-delim $tab --select-col 0,1,'ID POS $vcf_protein_change_annot VAR_TYPE OBSA OBSU MINA MINU MAF MAFA MAFU !{raw::pheno_test:${p_col_disp}_\@test_tag:if_prop=use_for_display}' --exact --require-col-match | sed '1! s/^var_//' | sed '1! s/^\(\S\S\*\)\(\s\s\*\S\S\*\)\(\s\s\*\)NA/\1\2\3\1/' | $smart_cut_cmd --in-delim $tab --select-row 0,1 --select-row 0,1,MINA,gt:0 --select-row 0,1,MINU,gt:0

short cmd make_gene_var_counts_dat_file=$var_counts_dat_helper($gene_var_counts_selector) > !{output,,gene_var_counts_dat_file} class_level gene

local cmd make_gene_rare_var_counts_dat_file=$smart_cut_cmd --in-delim $tab !{input,--file,gene_var_counts_dat_file} --select-row 1,1 --select-row 1,1,MAF,lt:$max_gene_var_counts_maf > !{output,,gene_rare_var_counts_dat_file} class_level gene

local cmd make_gene_var_counts_pdf_file=$draw_bar_plot_cmd("!{input,,gene_rare_var_counts_dat_file},!{input,,gene_rare_var_counts_dat_file} !{output,,gene_var_counts_pdf_file} 'Variants in !{prop,,gene,disp}: MAF < $max_gene_var_counts_maf' 'Frequency' MAFA $vcf_protein_change_annot sep=$tab value.cols=MAFU label.colors.col=VAR_TYPE max.label.size=15") class_level gene

!!expand:curpheno:case:control! \
cmd make_gene_curpheno_position_coverage_file=awk 'NR == 1 || (\$1 == !{prop,,gene,chrom} && \$2 >= !{prop,,gene,gene_start} && \$2 <= !{prop,,gene,gene_end})' !{input,,locus_curpheno_position_coverage_file} > !{output,,gene_curpheno_position_coverage_file} class_level gene skip_if no_coverage

#(for f in `cat !{input,,pheno_curpheno_sample_coverage_file_list} !{input,sample_coverage_file,unless_prop=failed}`; do zcat \$f | tail -n+2; done) | sed 's/^chr//' | sed 's/^\(\S*\):\(\S\S*\)/\1\t\2/' | awk -v OFS="\t" '\$1 == !{prop,,gene,chrom} && \$2 >= !{prop,,gene,gene_start} && \$2 <= !{prop,,gene,gene_end} {print \$1,\$2,\$3}' | sort | $table_sum_stats_cmd --in-delim $tab --group-col 1 --group-col 2 --col 3 --summaries --threshold $threshold --out-delim $tab --print-header --groups-sorted  > !{output,,gene_curpheno_position_coverage_file} class_level gene

local cmd make_gene_exon_target_file=awk -v OFS="\t" -F"\t" 'BEGIN {print "EXON","START","END"} \$1 == "!{prop,,gene,disp}" {print \$2,\$4,\$5}' !{input,,project_expanded_exon_target_file} > !{output,,gene_exon_target_file} class_level gene

var_coverage_helper=$draw_var_coverage_plot_cmd !{input,,@{1}_exon_target_file} !{input,,@{1}_var_counts_dat_file} 'Variants @3 for !{prop,,@{1},disp}' !{output,,@{1}_@{2}_pdf_file} !{input,case.coverage.file=,@{1}_case_position_coverage_file,unless_prop=no_coverage,allow_empty=1} !{input,control.coverage.file=,@{1}_control_position_coverage_file,unless_prop=no_coverage,allow_empty=1} coverage.cols=2,$frac_above_threshold var.cols=POS,VAR_TYPE,!{raw::pheno_test:${p_col_disp}_\@test_tag:if_prop=use_for_display:limit=1},MAFA,MAFU,MAF coverage.label='Percent samples above ${threshold}x' sep=$tab @4 known.var.types=$var_counts_types

var_helper=$draw_var_coverage_plot_cmd !{input,,@{1}_exon_target_file} !{input,,@{1}_var_counts_dat_file} 'Variants@3 for !{prop,,@{1},disp}' !{output,,@{1}_@{2}_pdf_file} var.cols=POS,VAR_TYPE,!{raw::pheno_test:${p_col_disp}_\@test_tag:if_prop=use_for_display:limit=1},MAFA,MAFU,MAF sep=$tab @4 known.var.types=$var_counts_types

local cmd make_gene_var_pdf_file=$var_helper(gene,var,,) class_level gene

short cmd make_gene_qq_pdf_file=$get_gene_vassoc_file(1,le:1,) | $burden_vassoc_qq_pdf_helper(/dev/stdin,gene_qq_pdf_file,'Variant associations for !{prop\,\,gene\,disp}: !{prop\,\,pheno\,disp}',gene,nrow=2 ncol=3) class_level gene

local cmd make_gene_var_coverage_pdf_file=$var_coverage_helper(gene,var_coverage, and coverage,draw.cov.lines=TRUE) class_level gene

make_gene_or_variant_qc_metrics_tex_file=$smart_cut_cmd !{input,--file,project_@{1}_outlier_file} --select-row 1,1,LABEL,!{prop,,@{1},external_id} --exact --select-row 1,1 --in-delim , --out-delim $tab --exclude-col 1,1,LABEL | $format_columns_cmd --in-delim $tab --number-format %.4g  | $table_to_beamer_cmd --in-delim $tab --header-rows 1 --header-cols 1 --title "!{prop,,@{1},disp} QC Metrics" > !{output,,@{1}_qc_metrics_tex_file} 

local cmd make_gene_qc_metrics_tex_file=$make_gene_or_variant_qc_metrics_tex_file(gene) class_level gene

local cmd make_gene_qc_metrics_pdf_file=$run_latex_cmd(gene_qc_metrics_tex_file,gene_qc_metrics_pdf_file) class_level gene

pseq_gene_mask=reg.req=!{prop,,gene,chrom}:!{prop,,gene,gene_start}..!{prop,,gene,gene_end}

pseq_all_data_analysis_helper=${pseq_@{1}_cmd_pheno_variant_subset}(@2) --mask $pseq_gene_mask

mono_ex_mask=--mask monomorphic.ex

!|expand:;:fname;pseq_cmd_type;tex_title;extrarunif:all_data;all_analysis;All Variants, All Samples;:all_variants;filter_only_samples_all_analysis;All Variants, QC/Popgen/Strat+ Samples, All Genotypes;run_if !no_filters| \
cmd make_gene_fname_tex_file=$smart_join_cmd --header 1 --in-delim $tab --exec "$pseq_all_data_analysis_helper(pseq_cmd_type,$vassoc) $show_id $mono_ex_mask !{prop,--phenotype,pheno} --perm 0 | $parse_out_id | cut -f1 | cat - !{input,,project_variant_subset_full_var_annot_file} | cut -f1 | sort | uniq -d | sed '1 s/^/ID\n/'" --exec "cat !{input,,project_variant_subset_full_var_annot_file} | $smart_cut_cmd --in-delim $tab --select-col 0,1,'$vep_id_annot $vcf_protein_change_annot' --exact --require-col-match" --exec "$pseq_all_data_analysis_helper(pseq_cmd_type,$vassoc) $show_id $mono_ex_mask !{prop,--phenotype,pheno} --perm 0 | $parse_out_id"  --ignore-err $plinkseq_okay_err --rest-extra 1 | $add_function_cmd --in-delim $tab --header 1 --col1 MINA --col2 OBSA --val-header MAFA --type divide | $add_function_cmd --in-delim $tab --header 1 --col1 MINU --col2 OBSU --val-header MAFU --type divide | $pheno_process_slide_variants_helper(ID FILTER $vcf_protein_change_annot MINA MINU MAFA MAFU P,,P,.,) | $replace_var_id | awk -F"\t" -v OFS="\t" '\$2!="PASS" && NR > 1 {\$2="FAIL"} {print}' | $format_columns_cmd --in-delim $tab --header 1 --number-format MAFA,%.2g --number-format MAFU,%.2g --number-format P,%.2g --percentage MAFA --percentage MAFU | $truncate_cols($vassoc_max_chars_per_col) | tail -n+2 | sed '1 s/^/Variant\tFilter\tProt Change\tCounts\tCounts\tMAF\tMAF\tOR\nVariant\tFilter\tProt Change\tCase\tCtrl\tCase\tCtrl\tP\n/' | $table_to_beamer_cmd --allow-breaks --multi-row 1-3,1-2 --multi-row 8,1-2 --multi-col 1,4-5 --multi-col 1,6-7 --font-size 8pt --bottom-margin 0in --left-margin 0in --right-margin 0in --title "tex_title: !{prop,,gene,disp}" --in-delim $tab --header-rows 2 > !{output,,gene_fname_tex_file} class_level gene extrarunif skip_if pheno_qt

!!expand:fname:all_data:all_variants! \
local cmd make_gene_fname_pdf_file=$run_latex_cmd(gene_fname_tex_file,gene_fname_pdf_file) class_level gene



#short cmd make_gene_associated_tex_file=$pheno_slide_variants_helper(.,.,^!{prop\\,\\,gene\\,external_id}$,GENE ID !{prop::pheno:vassoc_meta_disp} $dp_helper GENO HWE P_MISSING MINA MINU MAFA MAFU OR P_RAW P,,P,.,pheno_vassoc_annot_file,,1) | $gene_associated_tex_helper("'!{prop,,gene,disp} variants for !{prop,,pheno,disp}'") > !{output,,gene_associated_tex_file} class_level gene skip_if pheno_qt

#short cmd make_gene_qt_associated_tex_file=$pheno_qt_slide_variants_helper(.,.,^!{prop\\,\\,gene\\,external_id}$,GENE ID !{prop::pheno:vassoc_meta_disp} $dp_helper GENO HWE MAF MINA_EX MINU_EX BETA SE P_RAW P,,P,.,pheno_vassoc_annot_file,,1) | $gene_qt_associated_tex_helper("'!{prop,,gene,disp} variants for !{prop,,pheno,disp}'") > !{output,,gene_associated_tex_file} class_level gene run_if pheno_qt

#local cmd make_gene_associated_pdf_file=$run_latex_cmd(gene_associated_tex_file,gene_associated_pdf_file) class_level gene

replace_nan=sed 's/\b\(NA\|na\|nan\|NaN\|NAN\)\b/@1/g'

!!expand%;%ctype;width;traitselector;extrased;runif%all;1;related_traits;;run_if related_traits%meta;3;meta_trait_inv;| sed 's/^/MAF\t/';run_if meta_trait_inv! \
local cmd make_gene_ctype_trait_associated_tex_file=let w=`perl -e "print int(.5 + 6 + width * \`echo !{prop,,pheno,traitselector,sep=:} | sed 's/:/\n/g' | wc -l\`)"` && $smart_cut_cmd --in-delim $tab !{input,--file,pheno_ctype_trait_vassoc_annot_file} --select-row 1,1 --exact --select-row 1,1,GENE,!{prop,,gene,external_id} | $sort_name_col_with_header(!{raw::pheno_test:${p_col_disp}_@{test_tag}:if_prop=use_for_display:limit=1}_!{prop::pheno:disp},) | $transpose_cmd --in-delim $tab | $smart_cut_cmd --in-delim $tab --vec-delim @  --select-row 0,1,ID --select-row 0,1,'^(!{prop,,pheno,vassoc_meta_disp,sep=|})$' --select-row 0,1,'eq:${or_col_disp_pheno_test}_!{prop::pheno:disp}' --select-row 0,1,eq:MAF --select-row 0,1,'eq:!{raw::pheno_test:${p_col_disp}_@{test_tag}:limit=1}_!{prop::pheno:disp}' !{raw;;pheno;--select-row 0,1,"^(`head -n1 *pheno_small_vassoc_annot_file | rev | cut -f1-2 | rev extrased | sed 's/\(\S\S*\)/\1_@disp/g' | sed 's/)/\\\\)/g' | sed 's/(/\\\\(/g' | sed 's/\t/\|/g'`)$";all_instances=1;if_prop=pheno:eq:@traitselector;if_prop=project:eq:@project} | sed 's/^\([^\t][^\t]*\)/\1\t\1/' | sed 's/^[^\t][^\t]*\(\s\s*\)\([^\t][^\t]*\)_\(!{prop,,pheno,disp}\)\t/\3\1\2\t/' !{raw;;pheno;| sed 's/^[^\t][^\t]*\(\s\s*\)\([^\t][^\t]*\)_\(@disp\)\t/\3\1\2\t/';all_instances=1;if_prop=pheno:eq:@traitselector;if_prop=project:eq:@project} |  $transpose_cmd --in-delim $tab | $smart_cut_cmd --in-delim $tab --exclude-row 0,1,MAF,eq:0 | $swap_header(!{prop::pheno:vassoc_meta_disp},!{prop::pheno:vassoc_meta_headers},1\,2) | $translate_variant_type | $replace_var_id | $replace_nan(-) | $format_columns_cmd --in-delim $tab --number-format %.2g | $table_to_beamer_cmd --allow-breaks --font-size 8pt --auto-dim --bottom-margin 0in --left-margin 0in --right-margin -in --title "!{prop,,gene,disp} variants: ctype trait values" --in-delim $tab --header-rows 2 --multi-row .,2 --multi-col 1-2 > !{output,,gene_ctype_trait_associated_tex_file} class_level gene runif

!!expand:ctype:all:meta! \
local cmd make_gene_ctype_trait_associated_pdf_file=$run_latex_cmd(gene_ctype_trait_associated_tex_file,gene_ctype_trait_associated_pdf_file) class_level gene

short cmd make_gene_gassoc_tex_file=(head -n2 !{input,,transcript_burden_gassoc_file,if_prop=burden_test,limit=1} && tail -qn+3 !{input,,transcript_burden_gassoc_file,if_prop=burden_test,sort_prop=sort}) | awk -F"\t" -v OFS="\t" '!m[\$1] {m[\$1]=NR} NR <= 2 {print NR,m[\$1],\$0} NR > 2 {print 3,m[\$1],\$0}' | sort -k1,1n -k2,2n -k4,4 | cut -f3- | $format_columns_cmd --in-delim $tab --number-format 3,%d --number-format 4,%d --number-format 5,%d --number-format 6,%d --number-format %.2g | $table_to_beamer_cmd --in-delim $tab --force-multi-row-break 1 --header-rows 2 --multi-col 1-2 --multi-row . --auto-dim  --font-size 8pt --title "Variant counts per group for !{prop,,gene,disp}: !{prop,,pheno,disp}" > !{output,,gene_gassoc_tex_file} class_level gene run_if gene_burden

local cmd make_gene_gassoc_pdf_file=$run_latex_cmd(gene_gassoc_tex_file,gene_gassoc_pdf_file) class_level gene

#gene_burden cmds

#fix run_if to pull from consistent???
local cmd make_transcript_burden_annot_variant_list_file=$annot_variant_list_helper(gene_all_annot_file,--select-row 1\,1\,$vep_trans_annot\,eq:!{prop::transcript_burden:external_id},) > !{output,,transcript_burden_annot_variant_list_file} class_level transcript_burden run_if annot_mask

local cmd make_transcript_burden_all_variant_list_file=$smart_cut_cmd --in-delim $tab !{input,--file,gene_all_annot_file} --select-col 1,1 --select-row 1,1,$vep_trans_annot,eq:!{prop::transcript_burden:external_id} !{raw,,burden,| cat - *burden_non_annot_variant_list_file | sort | uniq -d,if_prop=burden_maf,if_prop=burden_mac_lb,if_prop=burden_mac_ub,or_if_prop=1,allow_empty=1} !{raw,,annot,| cat - *annot_non_annot_variant_list_file | sort | uniq -d,if_prop=annot_maf,if_prop=annot_mac_lb,if_prop=annot_mac_ub,if_prop=apply_extended_qc,if_prop=apply_extended_strict_qc,or_if_prop=1,allow_empty=1} !{input,burden_non_annot_variant_list_file,if_prop=burden_maf,if_prop=burden_mac_lb,if_prop=burden_mac_ub} !{input,annot_non_annot_variant_list_file,if_prop=annot_maf,if_prop=annot_mac_lb,if_prop=annot_mac_ub,or_if_prop=1,allow_empty=1} !{raw,,transcript_burden,| cat - *transcript_burden_annot_variant_list_file | sort | uniq -d,if_prop=annot_mask,allow_empty=1,max=1} !{input,transcript_burden_annot_variant_list_file,if_prop=annot_mask,allow_empty=1,max=1} > !{output,,transcript_burden_all_variant_list_file} class_level transcript_burden class_level transcript_burden

short cmd make_transcript_burden_reg_list_file=$smart_join_cmd !{input,--file,transcript_burden_all_variant_list_file} --exec "$smart_cut_cmd --in-delim $tab !{input,--file,project_clean_gene_variant_file} --select-col 1,1,'ID CHROM POS' --exclude-row 1,1" --extra 2 | cut -f2-3 | sed 's/\s\s*/:/' | sed 's/^/chr/' | sed 's/^chrchr/chr/' > !{output,,transcript_burden_reg_list_file} class_level transcript_burden

short cmd make_transcript_burden_vassoc_annot_file=$smart_join_cmd --in-delim $tab --header 1 --exec "sed '1 s/^/ID\n/' !{input,,transcript_burden_all_variant_list_file}" --exec "$smart_cut_cmd --in-delim $tab !{input,--file,gene_all_annot_file} --select-row 1,1 --select-row 1,1,$vep_trans_annot,eq:!{prop::transcript_burden:external_id} --select-col 1,1 --select-col 1,1,'$vassoc_meta_fields' --exact --require-col-match" --exec "$smart_cut_cmd --in-delim $tab !{input,--file,pheno_vassoc_annot_file}  --exclude-col 1,1,'$vassoc_meta_fields' --exact --require-col-match" --rest-extra 1 > !{output,,transcript_burden_vassoc_annot_file} class_level transcript_burden

case_count_helper=$smart_join_cmd --in-delim $tab --exec "$smart_join_cmd --in-delim $tab --header 1 --exec \"cut -f1 !{input,,transcript_burden_sample_list_file} | sed '1 s/^/ID\n/'\" !{input,--file,pheno_sequenced_all_sample_info_file} --extra 2 | $smart_cut_cmd --in-delim $tab --select-col 0,1,'ID !{prop,,pheno}' | $table_sum_stats_cmd --has-header --in-delim $tab --out-delim $tab --print-header --col !{prop,,pheno} --totals --group-col !{prop,,pheno} | $smart_cut_cmd --in-delim $tab --select-col 0,1 --select-col 0,1,!{prop,,pheno}_num --require-col-match --select-row 0,1,'$case_pheno_helper $control_pheno_helper' --exact" --exec "perl -e 'print \"$case_pheno_helper\t0\n$control_pheno_helper\t0\n\"'" --merge | $transpose_cmd --in-delim $tab | $smart_cut_cmd --in-delim $tab --select-col 0,1,'$case_pheno_helper $control_pheno_helper' --exact | tail -n+2

local cmd make_transcript_burden_gassoc_file=a=`$case_count_helper` && $pseq_qc_plus_all_assoc_cmd_pheno_variant_subset(v-assoc) $show_id --mask reg.req=@!{input,,transcript_burden_reg_list_file} --perm 1 !{prop,--phenotype,pheno} | $smart_cut_cmd --in-delim $tab --select-col 0,1,'MINA MINU OBSA OBSU' | $table_sum_stats_cmd --in-delim $tab --out-delim $tab --totals --col MINA --col MINU --col OBSA --col OBSU --has-header --print-header | $add_function_cmd --in-delim $tab --header 1 --col1 OBSA_tot --col2 OBSA_num --type divide --val-header OBSA | $add_function_cmd --in-delim $tab --header 1 --col1 OBSU_tot --col2 OBSU_num --type divide --val-header OBSU | $add_function_cmd --in-delim $tab --header 1 --col1 OBSU --val2 2 --type multiply --val-header OBSU2 | $add_function_cmd --in-delim $tab --header 1 --col1 OBSA --val2 2 --type multiply --val-header OBSA2 | $smart_cut_cmd --in-delim $tab --exact --select-col 0,1,'OBSA OBSU OBSA2 OBSU2 MINA_tot MINU_tot' | sed '1! s/^/!{prop,,gene_burden,disp}\t!{prop,,transcript_burden,disp}\t/' | sed '1 s/^/Variant\tTranscript\t/' | sed "1! s/$/\t\$a/" | sed '1 s/$/\tCase_carrier_count\tControl_carrier_count/' | $add_function_cmd --in-delim $tab --header 1 --col1 Case_carrier_count --col2 OBSA2 --type divide --val-header Case_carrier | $add_function_cmd --in-delim $tab --header 1 --col1 Control_carrier_count --col2 OBSU2 --type divide --val-header Control_carrier | $add_function_cmd --in-delim $tab --header 1 --col1 MINA_tot --col2 OBSA2 --type divide --val-header Case_load | $add_function_cmd --in-delim $tab --header 1 --col1 MINU_tot --col2 OBSU2 --type divide --val-header Control_load | $smart_cut_cmd --in-delim $tab --select-col 0,1,'Variant Transcript OBSA OBSU MINA_tot MINU_tot Case_load Control_load Case_carrier Control_carrier' --exclude-row 0,1 --exact --require-col-match | sed '1 s/^/Variants\tTranscript\tN\tN\tTot Counts\tTot Counts\tMean Count\tMean Count\tCarrier Frequency\tCarrier Frequency\nVariants\tTranscript\tCase\tControl\tCase\tControl\tCase\tControl\tCase\tControl\n/' | $smart_cut_cmd --in-delim $tab --exec "sed '1 s/\(.*\)/\1\n\1/' !{input,,burden_flat_gassoc_file}" --exact --require-col-match --select-col 1,1,'!{prop,,burden_test,test_tag,missing_prop=test_name,uc=1,if_prop=burden:eq:@burden}' --select-row 1,1 --select-row 1,2 `if [[ !{prop,,transcript_burden,external_id} == $consensus_transcript ]]; then echo --select-row 1,1,GENE,!{prop,,gene,external_id}; fi` --select-row 1,1,TRANSCRIPT,!{prop,,transcript_burden,external_id}  --and-row-all --paste --stdin-first  > !{output,,transcript_burden_gassoc_file} class_level transcript_burden skip_if pheno_qt run_if burden_test

transcript_burden_sample_list_helper=$burden_sample_list_helper_int(transcript_burden_all_variant_list_file,transcript_burden_reg_list_file,@1,@2,@3)

transcript_burden_sstats_helper=$gene_burden_or_variant_sstats_helper(transcript_burden,@1,@2)

pheno_pdf_helper1=$@{1}_sstats_helper(pheno_sequenced_all_sample_box_info_file,$tab) | $smart_cut_cmd --in-delim $tab --vec-delim : --exact --require-col-match --select-col 0,1,"`echo 'ID:Variant:Disp:!{prop,,pheno,disp} | sed 's/(/\\(/g' | sed 's/)/\\)/g'`" 
pheno_pdf_helper2=$draw_box_plot_cmd("/dev/stdin !{output,,@{1}_pheno_pdf_file} 'Effect of @2 on !{prop,,pheno,disp}' '' '!{prop,,pheno,disp} @3' '!{prop,,pheno,disp}' sep=$tab label=Disp order=Variant title.newline=: max.ncol=7")

pheno_pdf_helper=$pheno_pdf_helper1(@1) | $pheno_pdf_helper2(@1,@2,)

short cmd make_transcript_burden_sample_list_file=$transcript_burden_sample_list_helper(gt,transcript_burden_sample_list_file,'1 3') class_level transcript_burden

short cmd make_transcript_burden_sample_without_list_file=$transcript_burden_sample_list_helper(le,transcript_burden_sample_without_list_file,1) class_level transcript_burden

local cmd make_transcript_burden_pheno_pdf_file=$pheno_pdf_helper(transcript_burden,!{prop\\,\\,transcript_burden\\,disp} variants:in !{prop\\,\\,gene\\,disp}) class_level transcript_burden run_if burden_test

all_pheno_pdf_helper1=$@{1}_sstats_helper(pheno_sequenced_all_trait_all_sample_box_info_file,$tab) | $smart_cut_cmd --in-delim $tab --vec-delim : --exact --require-col-match --select-col 0,1,"`echo ID:Variant:Disp:!{prop,,pheno,disp}:!{prop,,pheno,disp,if_prop=pheno:eq:\@related_traits,if_prop=project:eq:\@project,all_instances=1,sep=:} | sed 's/(/\\(/g' | sed 's/)/\\)/g`" 
all_pheno_pdf_helper2=$draw_box_plot_cmd("/dev/stdin !{output,,@{1}_all_pheno_pdf_file} 'Effect of @2 on !{prop,,pheno,disp} and related traits' '' '' -ID,Variant,Disp sep=$tab label=Disp order=Variant title.newline=: max.ncol=7")

all_pheno_pdf_helper=$all_pheno_pdf_helper1(@1) | $all_pheno_pdf_helper2(@1,@2, fraction)

local cmd make_transcript_burden_all_pheno_pdf_file=$all_pheno_pdf_helper(transcript_burden,!{prop\\,\\,transcript_burden\\,disp} variants:in !{prop\\,\\,gene\\,disp}) class_level transcript_burden run_if related_traits run_if burden_test

short cmd make_transcript_burden_qq_pdf_file=cat !{input,,transcript_burden_vassoc_annot_file} $get_vassoc_file_helper(1,le:1,,transcript_burden) | $burden_vassoc_qq_pdf_helper(/dev/stdin,transcript_burden_qq_pdf_file,'!{prop\,\,gene_burden\,disp} variant associations for !{prop\,\,transcript_burden\,disp}: !{prop\,\,pheno\,disp}',transcript_burden,nrow=2 ncol=3) class_level transcript_burden skip_if no_var_qq

local cmd make_gene_burden_vassoc_tex_file=(head -n1 !{input,,transcript_burden_vassoc_annot_file,limit=1} | sed 's/^/Transcript\t/' && $smart_cut_cmd --in-delim $tab --exclude-row .,1 !{raw,,transcript_burden,--exec "sed 's/^/@disp\t/' *transcript_burden_vassoc_annot_file"} !{input,transcript_burden_vassoc_annot_file}) | $smart_cut_cmd --in-delim $tab --select-col 0,1,'ID Transcript !{prop::pheno:vassoc_meta_disp} $dp_helper GENO HWE P_MISSING MINA MINU MAFA MAFU $or_col_disp_pheno_test !{raw::pheno_test:${p_col_disp}_@test_tag:if_prop=use_for_display}' --exact --require-col-match | $sort_name_col_with_header(!{raw::pheno_test:${p_col_disp}_@test_tag:if_prop=use_for_display:limit=1},) | $translate_variant_type | $replace_var_id | $head_cmd(500) | $process_associated_variants_helper("!{prop::gene_burden:disp} variants for !{prop::gene:disp}: !{prop::pheno:disp}",11,--number-format P_MISSING\,%.2g --number-format HWE\,%.2g --number-format GENO\,%.2g !{raw::pheno:--number-format DP\,%d:if_prop=include_dp:allow_empty=1},10,20,1) > !{output,,gene_burden_vassoc_tex_file} class_level gene_burden run_if transcript_burden run_with transcript_burden skip_if no_var_filter

local cmd make_gene_burden_vassoc_pdf_file=$run_latex_cmd(gene_burden_vassoc_tex_file,gene_burden_vassoc_pdf_file) class_level gene_burden 

#variant cmds

variant_sample_list_helper=$pseq_qc_plus_all_analysis_cmd_pheno_variant_subset(v-matrix) $show_id --mask reg=!{prop,,variant,chrom}:!{prop,,variant,pos} | $smart_cut_cmd --exclude-col 0,1-3 --select-row 0,1 --select-row 0,1,!{prop,,variant,external_id} --in-delim $tab | $transpose_cmd --in-delim $tab | $smart_cut_cmd --select-col 0,1 --select-row 0,2,@1:0 --in-delim $tab | sed 's/\t/\n/g' > !{output,,@2}

variant_maf_helper_int=\$with = \`cat !{input,,variant_sample_list_file} | wc -l\`; \$without = \`cat !{input,,variant_sample_without_list_file} | wc -l\`; \$freq = \$with / (\$with + \$without); \$nsamp = \$with + \$without; \$dev = sqrt(\$freq * (1 - \$freq) / \$nsamp)

variant_maf_helper=`perl -e '$variant_maf_helper_int; print \$freq'`
variant_maf_lb_helper=`perl -e '$variant_maf_helper_int; \$lb = \$freq - 2 * \$dev; \$lb = 0 if \$lb < 0; print \$lb'`
variant_maf_ub_helper=`perl -e '$variant_maf_helper_int; \$ub = \$freq + 2 * \$dev; \$ub = 1 if \$ub > 1; print \$ub'`


local cmd make_variant_sample_list_file=$variant_sample_list_helper(gt,variant_sample_list_file) class_level variant

local cmd make_variant_sample_without_list_file=$variant_sample_list_helper(eq,variant_sample_without_list_file) class_level variant

!!expand:;:cmdtype;caselabel;ctrllabel;extrarunif\
:;Case;Ctrl;skip_if pheno_qt\
:qt_;$qt_top_disp;$qt_bottom_disp;run_if pheno_qt! \
local cmd make_variant_cmdtypetitle_slide_tex_file=perl -e 'print "Gene\t!{prop,,gene,disp}\nConsequence\t!{prop,,variant,consequence}\nChange\t!{prop,,variant,proteinchange}\n$disp_for_var_annot\t!{prop,,variant,var_annot}\ncaselabel:ctrllabel\t!{prop,,variant,ncase}:!{prop,,variant,ncontrol}\n"' | $table_to_beamer_cmd --no-border --col-emph 1 --title "Variant !{prop,,variant,chrom}:!{prop,,variant,pos}" --in-delim $tab --header-cols 1  > !{output,,variant_title_slide_tex_file} class_level variant extrarunif

local cmd make_variant_title_slide_pdf_file=$run_latex_cmd(variant_title_slide_tex_file,variant_title_slide_pdf_file) class_level variant

has_var_col_name=HAS_VAR

local cmd make_variant_mds_dat_file=$smart_join_cmd --exec "$smart_cut_cmd --exec \"sed 's/$/\tHas Variant/' !{input,,variant_sample_list_file}\" --exec \"sed 's/$/\tNo Variant/' !{input,,variant_sample_without_list_file}\" --in-delim $tab --out-delim $tab | sed '1 s/^/ID\t$has_var_col_name\n/'" --exec "$smart_join_cmd --exec \"$smart_cut_cmd !{input,--file,variant_sample_list_file} !{input,--file,variant_sample_without_list_file} !{input,--file,pheno_annotated_mds_file} --exclude-row 3,1 --select-col 3,1,ID --in-delim $tab --require-col-match | sort | uniq -d | sed '1 s/^/IID\n/'\" --exec \"$smart_cut_cmd !{input,--file,pheno_annotated_mds_file} --select-col 1,1,'ID ^C[0-9]' --in-delim $tab --require-col-match\" --header 1 --extra 2 --in-delim $tab" --header 1 --extra 1 --in-delim $tab > !{output,,variant_mds_dat_file} class_level variant

get_mds_cols_int=perl -e 'print join(@3"@2@3", map {@3"C@3\$_@3"} (1..@1))'
get_mds_cols=$get_mds_cols_int(@1,@2,)

variant_disp_helper=!{prop,,variant,proteinchange,if_prop=proteinchange:ne:NA,allow_empty=1}!{prop,,variant,disp,if_prop=proteinchange:eq:NA,allow_empty=1}

!|expand:;:type;colsarg:top;C1,C2:all;`$get_mds_cols(!{prop\\,\\,pheno\\,num_mds_plot},\\,)`| \
local cmd make_variant_type_mds_pdf_file=$draw_matrix_plot_cmd !{input,,variant_mds_dat_file} !{output,,variant_type_mds_pdf_file} 'Sample MDS Values: $variant_disp_helper' colsarg color.col=$has_var_col_name  sep=$tab cex=$mds_cex $mds_color class_level variant

#variant_sstats_helper=$smart_join_cmd --out-delim $tab --header 1 --exec "cut -d@2 -f1 !{input,,@1} | cat - !{input,,variant_sample_without_list_file} !{input,,variant_sample_list_file} | sort | uniq -d | sed '1 s/^/ID\n/'" --exec "$smart_cut_cmd --in-delim $tab --exec \"sed 's/$/\t1\tHas Variant/' !{input,,variant_sample_list_file}\" --exec \"sed 's/$/\t0\tNo Variant/' !{input,,variant_sample_without_list_file}\" | sed '1 s/^/ID\tVariant\tDisp\n/'" !{input,--file,@1} --rest-extra 1 --arg-delim : --in-delim 3:@2 --in-delim 2:$tab
variant_sstats_helper=$gene_burden_or_variant_sstats_helper(variant,@1,@2)

#local cmd make_variant_pheno_pdf_file=$variant_sstats_helper(pheno_sequenced_all_sample_info_file,$tab) | $smart_cut_cmd --in-delim $tab --vec-delim : --exact --require-col-match --select-col 0,1,'ID:Variant:Disp:!{prop,,pheno}' | awk -F"\t" '\$4 != !{prop,,pheno,pheno_missing}' | $draw_box_plot_cmd("/dev/stdin !{output,,variant_pheno_pdf_file} 'Effect of !{prop,,variant,disp} on !{prop,,pheno,disp}' '' '!{prop,,pheno,disp}' !{prop,,pheno} sep=$tab label=Disp order=Variant")  class_level variant run_if pheno_qt

local cmd make_variant_pheno_pdf_file=$pheno_pdf_helper(variant,!{prop\\,\\,gene\\,disp} $variant_disp_helper) class_level variant 

local cmd make_variant_all_pheno_pdf_file=$all_pheno_pdf_helper(variant,$variant_disp_helper) class_level variant run_if related_traits

local cmd make_variant_sstats_highlight_file=sed 's/$/,Variant/' !{input,,variant_sample_list_file} | sed '1 s/^/LABEL,MEASURE\n/' > !{output,,variant_sstats_highlight_file} class_level variant

!!expand:;:slidetype;dispcols:;-1,$display_col_name,$order_col_name,NVAR:slide_;Variant,$all_major_sstats_cols! \
local cmd make_variant_slidetypesstats_pdf_file=$variant_sstats_helper(pheno_sstats_file,\\,) | $smart_cut_cmd --in-delim $tab --exclude-col 0,1,Disp --exact | $draw_box_plot_cmd("/dev/stdin !{output,,variant_slidetypesstats_pdf_file} 'Sample QC properties for $variant_disp_helper' '' 'Sample Values' 'dispcols' label=$display_col_name order=$order_col_name sep=$tab $pheno_highlight_helper highlight.list.file=!{input,,variant_sstats_highlight_file} highlight.label.col=2")  class_level variant

short cmd make_variant_qc_metrics_tex_file=$make_gene_or_variant_qc_metrics_tex_file(variant) class_level variant

local cmd make_variant_qc_metrics_pdf_file=$run_latex_cmd(variant_qc_metrics_tex_file,variant_qc_metrics_pdf_file) class_level variant

short cmd make_variant_genome_pdf_file=zcat !{input,,pheno_annotated_genome_file} | perl -lne 'BEGIN {\$first = 1; \$h="Has variant"; \$n = "No variant"; %o = (\$h=>0, \$n=>1); open WITH, "!{input,,variant_sample_list_file}" or die "Cannot read !{input,,variant_sample_list_file\n"; while (<WITH>) { chomp; \$a{\$_}="Has Variant" } open WITHOUT, "!{input,,variant_sample_without_list_file}" or die "Cannot read !{input,,variant_sample_list_file\n"; while (<WITHOUT>) { chomp; \$a{\$_}="No variant" }} if (\$first) {\$first = 0; print && next} chomp; @F=split("\t"); if (\$a{\$F[0]} && \$a{\$F[1]}) { @v = (\$a{\$F[0]}, \$a{\$F[1]}); @o = map {\$o{\$_}} @v; \$F[2] = join("/", sort(@v)); \$F[3] = join("/", sort(@o)); print join("\t", @F)}' | $draw_box_plot_cmd(/dev/stdin !{output\,\,variant_genome_pdf_file} 'IBD sharing by $variant_disp_helper carrier' '' 'PI_HAT' PI_HAT sep=$tab label=$display_col_name order=$order_col_name max.plot.points=1000) class_level variant rusage_mod $genome_pdf_mem


num_per_igv_snapshot=3
igv_wait=5
#PREFER CMD BELOW WITHOUT PORT
#local cmd make_variant_igv_png_file=$open_vnc_display && python $targeted_bin_dir/write_igv_png.py --port $igv_port --locus chr!{prop,,variant,chrom}:!{prop,,variant,pos} `$smart_join_cmd --in-delim $tab !{input,--file,variant_sample_list_file} !{input,--file,project_sample_bam_list_file} --extra 2 | cut -f2 | $head_cmd($num_per_igv_snapshot) | sed 's/^/--bam-file /' | tr '\n' ' '` --igv-jar $igv_jar --hg-build $hg_build !{output,--out-png,variant_igv_png_file}; $close_vnc_display class_level variant timeout 300
local cmd make_variant_igv_png_file=$open_vnc_display && python $targeted_bin_dir/write_igv_png.py --locus chr!{prop,,variant,chrom}:!{prop,,variant,pos} `$smart_join_cmd --in-delim $tab !{input,--file,variant_sample_list_file} !{input,--file,project_sample_bam_list_file} --extra 2 | cut -f2 | $head_cmd($num_per_igv_snapshot) | sed 's/^/--bam-file /' | tr '\n' ' '` --igv-jar $igv_jar --hg-build $hg_build !{output,--out-png,variant_igv_png_file}; $close_vnc_display class_level variant timeout 300

local cmd make_variant_igv_pdf_file=convert !{input,,variant_igv_png_file} !{output,,variant_igv_pdf_file} class_level variant

ucsc_variant_buffer=10

local cmd make_variant_ucsc_regions_file=(echo $variant_disp_helper, human, hg$hg_build, chr!{prop,,variant,chrom}, !{prop,,variant,pos}, !{prop,,variant,pos}, \"$variant_disp_helper\", $(($ucsc_variant_buffer-1)), $(($ucsc_variant_buffer+1))) > !{output,,variant_ucsc_regions_file} class_level variant

local cmd make_variant_ucsc_pdf_file=$fetch_ucsc_cmd !{input,-r,variant_ucsc_regions_file} !{input,-t,pheno_ucsc_tracks_file} !{input,-u,pheno_ucsc_browser_file} !{output,--output-file,variant_ucsc_pdf_file} class_level variant

local cmd make_variant_r2_ld_file=$plink_cmd !{raw,--tfile,gene,*locus_combined_plink_file} !{input,locus_combined_tped_file} !{input,locus_combined_tfam_file} --ld-snp `awk '\$1 == !{prop,,variant,chrom} && \$4 == !{prop,,variant,pos} {print \$2} END {print "$variant_disp_helper"}' !{input,,locus_combined_tped_file} | awk 'NR == 1'` --r2 $ld_window_params !{raw,--out,variant,*variant_r2_trunk} !{output,variant_r2_ld_file} !{output,variant_r2_ld_log_file} class_level variant

local cmd make_variant_r2_annotated_ld_file=$smart_join_cmd --in-delim $tab --header 1 --extra 2 --exec "$smart_cut_cmd --out-delim $tab !{input,--file,variant_r2_ld_file} --select-col 1,1,'BP_B R2'" --exec "$smart_cut_cmd --out-delim $tab --exec \"sed 's/$/ Marker/' !{input,,locus_all_marker_bim_file}\" --exec \"sed 's/$/ Sequence/' !{input,,locus_non_marker_seq_bim_file}\" --select-col 1,'4 7' --select-col 2,'4 7' | sed '1 s/^/POS\tLABEL\n/'" > !{output,,variant_r2_annotated_ld_file} class_level variant

local cmd make_variant_r2_pdf_file=$draw_matrix_plot_cmd !{input,,variant_r2_annotated_ld_file} !{output,,variant_r2_pdf_file} 'LD with $variant_disp_helper' BP_B,R2 color.col=LABEL color.at=!{prop,,variant,pos},black,$variant_disp_helper class_level variant

max_var_ld_show=15

local cmd make_variant_r2_table_tex_file=$smart_cut_cmd !{input,--file,variant_r2_ld_file} --select-col 1,1,'SNP_B R2' | sort -grk2 | awk "NR <= $(($max_var_ld_show+1))" | sed '1 s/.*/SNP\t\$r^2\$/' | $table_to_beamer_cmd --header-rows 1 --title "Variants in LD with $variant_disp_helper"  > !{output,,variant_r2_table_tex_file} class_level variant

local cmd make_variant_r2_table_pdf_file=$run_latex_cmd(variant_r2_table_tex_file,variant_r2_table_pdf_file) class_level variant


cmd make_variant_beagle_phased_file=perl $targeted_bin_dir/make_hap_analysis_beagle_file.pl --variant $variant_disp_helper !{prop,--chr,variant,chrom} !{prop,--start,variant,pos} !{input,--beagle,locus_beagle_phased_file} !{input,--with-sample-list,variant_sample_list_file} !{input,--without-sample-list,variant_sample_without_list_file} > !{output,,variant_beagle_phased_file} class_level variant

analyze_haplotypes_cmd=$targeted_bin_dir/analyze_haplotypes.py
create_haplotype_tree_cmd=$targeted_bin_dir/create_haplotype_tree.py

num_additional_haplotype_analysis_samples=500
num_haplotype_samples_plot=200
hap_share_pct=.51
haplotype_analysis_perm=250
haplotype_analysis_one_error_perm=100
cmd make_variant_haplotype_analysis_files=python $analyze_haplotypes_cmd !{input,--haps-file,variant_beagle_phased_file} --hap-share-pct $hap_share_pct --num-additional-samples `$num_without_cmd` !{output,--write-best-hap,variant_best_hap_file,if_prop=nind:gt:1,allow_empty=1} --max-perm $haplotype_analysis_perm --max-one-error-perm $haplotype_analysis_one_error_perm > !{output,,variant_haplotype_analysis_file} class_level variant

hap_analysis_p_value_line=random sample haplotype uniqueness empirical p-value:
hap_analysis_shared_p_value_line=random shared marker sample haplotype uniqueness empirical p-value:
median_sharing_variant_line=Median shared CNV samps:
median_sharing_random_line=Median sharing random:
median_sharing_shared_line=Median sharing random with shared marker:


local cmd make_variant_haplotype_analysis_info_tex_file=(echo "Haplotype uniqueness empirical p-values" && \
  grep '$hap_analysis_p_value_line' !{input,,variant_haplotype_analysis_file} | head -n1 | sed 's/$hap_analysis_p_value_line/\tRelative to random samples:/' && \
  grep '$hap_analysis_shared_p_value_line' !{input,,variant_haplotype_analysis_file} | head -n1 | sed 's/$hap_analysis_shared_p_value_line/\tRelative to samples that share a SNP nearby:/' && \
	echo "Median haplotype sharing between sample pairs " && \
  grep '$median_sharing_variant_line' !{input,,variant_haplotype_analysis_file} | head -n1 | sed 's/$median_sharing_variant_line/\tWith rare variant:/' && \
  grep '$median_sharing_random_line' !{input,,variant_haplotype_analysis_file} | head -n1 | sed 's/$median_sharing_random_line/\tRandom:/' && \
  grep '$median_sharing_shared_line' !{input,,variant_haplotype_analysis_file} | head -n1 | sed 's/$median_sharing_shared_line/\tRandom that share a SNP nearby:/') \
  | perl -pe '\$_ = "\"""+"""u\$_"' | sed 's/</\$<\$/' | $text_to_beamer_cmd --list-indent $tab --title "`echo \"Information about haplotype sharing: $variant_disp_helper\" | sed 's/\_/\\\_/g'`" > !{output,,variant_haplotype_analysis_info_tex_file,if_prop=nind:gt:1,allow_empty=1} class_level variant


local cmd make_variant_haplotype_analysis_info_pdf_file=$run_latex_cmd(variant_haplotype_analysis_info_tex_file,variant_haplotype_analysis_info_pdf_file) class_level variant

num_without_cmd=head -n1 !{input,,variant_beagle_phased_file} | sed 's/\s\s\*/\n/g' | tail -n+2 | sort -u | cat - !{input,,variant_sample_without_list_file} | sort | uniq -d | wc -l

create_haplotype_tree_helper=python $create_haplotype_tree_cmd !{input,--haps-file,variant_beagle_phased_file} --num-without `$num_without_cmd` !{input,--best-hap-file,variant_best_hap_file,if_prop=nind:gt:1,allow_empty=1} !{input,--phe-file,pheno_marker_pheno_file,if_prop=project_all_marker_pheno_file,if_prop=pheno_marker_initial_pheno_file,or_if_prop=1,allow_empty=1} --num-to-plot $num_haplotype_samples_plot --map-file $genetic_map_file("!{prop,,variant,chrom}")

short cmd make_variant_haplotype_analysis_pdf_file=$create_haplotype_tree_helper !{output,--out-pdf-file,variant_haplotype_analysis_pdf_file} --title "$variant_disp_helper haplotype sharing: sequenced samples" class_level variant

short cmd make_variant_haplotype_analysis_longest_pdf_file=$create_haplotype_tree_helper !{output,--out-pdf-file,variant_haplotype_analysis_longest_pdf_file} --plot-longest --label-all !{output,--out-freq-file,variant_haplotype_analysis_freq_file} !{output,--out-seg-file,variant_haplotype_analysis_initial_seg_file} --title "$variant_disp_helper haplotype sharing: longest shared" class_level variant

local cmd make_variant_haplotype_analysis_seg_file=perl $targeted_bin_dir/annotate_initial_seg_file.pl !{input,--fam-file,locus_common_marker_tfam_file} !{input,--map-file,locus_common_marker_tped_file} !{input,--pheno-file,pheno_marker_pheno_file} < !{input,,variant_haplotype_analysis_initial_seg_file} > !{output,,variant_haplotype_analysis_seg_file} class_level variant

local cmd make_variant_hap_posterior_file=$predict_log_reg_cmd("!{input,,variant_haplotype_analysis_initial_seg_file} !{output,,variant_hap_posterior_file} START,STOP,SHARES_MIN CARRIER out.sep=$tab") class_level variant

count_posterior_samp_include=!{input,--exclude-samps,variant_sample_list_file} !{input,--exclude-samps,variant_sample_without_list_file} !{input,--include-samps,project_all_marker_assoc_sample_keep_file,if_prop=project_all_marker_assoc_sample_keep_file,unless_prop=pheno_all_marker_assoc_sample_keep_file,allow_empty=1} !{input,--include-samps,pheno_all_marker_assoc_sample_keep_file,if_prop=pheno_all_marker_assoc_sample_keep_file,allow_empty=1}

local cmd make_variant_hap_count_posterior_file=perl $targeted_bin_dir/make_var_count_posterior.pl $count_posterior_samp_include < !{input,,variant_hap_posterior_file} > !{output,,variant_hap_count_posterior_file} class_level variant

#use the genotype file as backup
local cmd make_variant_traditional_count_posterior_file=cat !{input,,locus_combined_merged_impute2_file} | perl $targeted_bin_dir/make_var_count_posterior.pl $count_posterior_samp_include !{prop,--only-pos,variant,pos} !{input,--samp-file,locus_combined_sample_file}  --impute2-format > !{output,,variant_traditional_count_posterior_file} class_level variant

!!expand:impute_type:hap:traditional! \
cmd make_variant_impute_type_threshold_vassoc_file=$threshold_vassoc_cmd("!{input,,variant_impute_type_count_posterior_file} !{output,,variant_impute_type_threshold_vassoc_file} !{input,,pheno_marker_pheno_file} 1 2 pheno.has.header=FALSE !{input,covar.file=,project_all_marker_assoc_covars_file,if_prop=project_all_marker_assoc_covars_file,allow_empty=1} !{raw,covar.cols.delim=,project,:,if_prop=project_all_marker_assoc_covars_file,allow_empty=1} !{raw,covar.id.col=,project,IID,if_prop=project_all_marker_assoc_covars_file,allow_empty=1} !{raw,covar.covar.cols=,project,-FID:IID,if_prop=project_all_marker_assoc_covars_file,allow_empty=1}") class_level variant

add_na_row_helper=perl -pe 'END {print \"NA\tNA\tNA\n\"}' | sed '1! s/^NA.\*//'  | sed '/^\$/d'

!!expand:,:impute_type:hap:traditional! \
local cmd make_variant_impute_type_summary_file=$smart_cut_cmd --in-delim $tab \
			--exec "perl -e 'print \"VAR\n!{prop,,variant,disp}\n\"'" \
			--exec "perl -e 'print \"CHROM\n!{prop,,variant,chrom}\n\"'" \
			--exec "perl -e 'print \"POS\n!{prop,,variant,pos}\n\"'" \
  --exec "perl $targeted_bin_dir/summarize_threshold_vassoc.pl $variant_maf_lb_helper $variant_maf_ub_helper < !{input,,variant_impute_type_threshold_vassoc_file}" \
	--exec "(grep '$hap_analysis_p_value_line' !{input,,variant_haplotype_analysis_file} | head -n1 | sed 's/$hap_analysis_p_value_line\s*//' | sed 's/<//' || echo NA) | sed '1 s/^/P_UNIQUE\n/'" \
	--paste > !{output,,variant_impute_type_summary_file} class_level variant

threshold_vassoc_plot_width=10
threshold_vassoc_plot_height=6

!!expand:,:impute_type,impute_disp:hap,haplotype:traditional,Impute2! \
local cmd make_variant_impute_type_threshold_vassoc_pdf_file=$draw_line_plot_cmd !{input,,variant_impute_type_threshold_vassoc_file} !{output,,variant_impute_type_threshold_vassoc_pdf_file} '!{prop,,pheno,disp} association score for $variant_disp_helper --- impute_disp imputation' 'Posterior Threshold' 'Assoc P-value' THRESHOLD 'P' sep=$tab y2.col=F,F_CASE,F_CONTROL y2.label=Frequency hline=0.05 hline.name='P .05' hline2=$variant_maf_helper hline2.name='Seq MAF' x.log=T y2.log=T y.minus.log=T legend.pos="bottomleft" plot.width=$threshold_vassoc_plot_width plot.height=$threshold_vassoc_plot_height class_level variant


threshold_vassoc

#the master slide files

!|expand:;:curlevel;Curlevel;dispprop;extra:project;Project level;disp;:pheno;Phenotype level;pheno_description;:burden;Variant group;disp; variants:gene;Gene level;disp;\n!{prop,,pheno,pheno_description}\n:locus;Locus level;disp;\n!{prop,,pheno,pheno_description}\n| \
local cmd make_curlevel_title_slide_tex_file=perl -e 'print "!{prop,,curlevel,dispprop}extra"' | $table_to_beamer_cmd --no-border --row-emph 1 --title "Curlevel summary" --in-delim $tab --header-cols 1 > !{output,,curlevel_title_slide_tex_file} class_level curlevel

local cmd make_project_additional_slide_tex_file=perl -e 'print "Additional slides"' | $table_to_beamer_cmd --no-border --row-emph 1 --in-delim $tab > !{output,,project_additional_slide_tex_file} class_level project

!!expand:,:titletype,Titletype:qc,QC:assoc,Association:gene,Top genes! \
local cmd make_pheno_titletype_title_slide_tex_file=perl -e 'print "!{prop,,pheno,pheno_description}"' | $table_to_beamer_cmd --no-border --row-emph 1 --title "Titletype summary" --in-delim $tab --header-cols 1 > !{output,,pheno_titletype_title_slide_tex_file} class_level pheno


!!expand:,:curlevel,extra:project,:pheno,:burden,:pheno,_gene:pheno,_qc:pheno,_assoc:gene,:variant,:locus,! \
local cmd make_curlevelextra_title_slide_pdf_file=$run_latex_cmd(curlevelextra_title_slide_tex_file,curlevelextra_title_slide_pdf_file) class_level curlevel

local cmd make_project_additional_slide_pdf_file=$run_latex_cmd(project_additional_slide_tex_file,project_additional_slide_pdf_file) class_level project


optional_cat=if [ -e @1 ]; then cat @1; else true; fi
optional_pdftops=if [ -e @1 ]; then pdftops @1 -; else true; fi
optional_converttops=if [ -e @1 ]; then convert @1 ps:-; else true; fi

mpjh=!{raw,,@1,--exec "$optional_pdftops(*@2)"} !{input,@2,optional=1}
mpjhf=!{raw,,@1,--exec "$optional_pdftops(*@2)",@3} !{input,@2,optional=1}

mcjh=!{raw,,@1,--exec "$optional_converttops(*@2)"} !{input,@2,optional=1}
mcjhf=!{raw,,@1,--exec "$optional_converttops(*@2)",@3} !{input,@2,optional=1}

mph=!{raw,,@1,--exec "$optional_cat(*@2)"} !{input,@2,optional=1}
mphf=!{raw,,@1,--exec "$optional_cat(*@2)",@3} !{input,@2,optional=1}

local cmd make_project_slide_master_ps_file=$smart_cut_cmd \
 $mpjh(project,project_title_slide_pdf_file) \
 $mpjh(project,project_slide_failures_pdf_file) \
"""
}
    