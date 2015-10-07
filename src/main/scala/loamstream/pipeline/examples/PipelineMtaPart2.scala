package loamstream.pipeline.examples

/**
 * LoamStream
 * Created by oliverr on 10/6/2015.
 */
object PipelineMtaPart2 {
  val string: String =
    """#1. Include all transcripts that have a CCDS tag.
#2. Include transcripts labeled "protein_coding" and where either 'mRNA_start_NF' or 'mRNA_end_NF' are not included.

local cmd make_project_vep_file=(fgrep \\# !{input,,project_subset_vep_file,limit=1} && cat !{input,,project_subset_vep_file} | fgrep -v \\#) > !{output,,project_vep_file} class_level project

local cmd make_project_snpsift_file=(head -n1 !{input,,project_subset_snpsift_file,limit=1} && tail -qn+2 !{input,,project_subset_snpsift_file}) > !{output,,project_snpsift_file} class_level project

cmd make_project_maf_file=$pseq_cmd !{input,,project_variant_vcf_file} v-freq --show-id | cut -d: -f3- | cut -f1,10 | awk -F"\t" -v OFS="\t" '\$2 > .5 {\$2 = 1 - \$2} {print \$1,\$2}' > !{output,,project_maf_file} class_level project

cmd make_project_annot_file=$smart_join_cmd --in-delim $tab !{input,--file,project_maf_file} --exec "sed 's/^\\\#//g' !{input,,project_vep_file} | fgrep -v \\\#" --exec "awk '!m[\\$1] {print} {m[\\$1]=1}' !{input,,project_snpsift_file}" --header 1 --multiple 2 --extra 1 --ignore-status 141 > !{output,,project_annot_file} class_level project

short cmd make_project_gene_name_map_file=zcat !{input,,project_gencode_gtf_gz_file} | fgrep -v \\#\\# | cut -d\; -f1,5 | sed 's/.\*gene_id\s\s*//' | awk -v OFS="\t" '{print \$1,\$3}' | sed 's/"//g' | sed 's/\.[0-9][0-9]*;*//g' | sort -u > !{output,,project_gene_name_map_file} class_level project

!!expand:,:large,filter:large,:medium,| egrep -v '(mRNA_start_NF|mRNA_end_NF)':small,| fgrep 'tag "CCDS"'! \
short cmd make_project_large_transcript_gene_file=zcat !{input,,project_gencode_gtf_gz_file} | fgrep -v \\\#\\\# | egrep '(transcript_type "protein_coding"|tag "CCDS")' filter | cut -d\; -f1,2 | sed 's/.\*gene_id\s\s*//' | awk -v OFS="\t" '{print \$1,\$3}' | sed 's/"//g' | sed 's/\.[0-9][0-9]*;*//g' | sort -u | $smart_cut_cmd --in-delim $tab --select-col 0,'2 1' | awk -F"\t" '\$1 != \$2' > !{output,,project_large_transcript_gene_file} class_level project

canonical_header=Canonical

short cmd make_project_gene_canonical_file=$smart_cut_cmd --in-delim $tab !{input,--file,project_annot_file} --select-col 1,1,'$vep_gene_annot $vep_trans_annot' --exact --require-col-match --select-row 1,1,$vep_canonical_annot,YES --exclude-row 1,1 | sort -u | sed '1 s/^/Gene\t$canonical_header\n/' > !{output,,project_gene_canonical_file} class_level project

!!expand:large:large:medium:small! \
short cmd make_project_large_annot_file=$smart_join_cmd --in-delim $tab --exec "$smart_cut_cmd --in-delim $tab !{input,--file,project_annot_file} --select-col 1,1,'$vep_trans_annot $vep_gene_annot' --exact | tail -n+2 | sort -u | cat - !{input,,project_large_transcript_gene_file} | cut -f1,2 | sort | uniq -d | sed '1 s/^/$vep_trans_annot\t$vep_gene_annot\n/'" !{input,--file,project_annot_file} --col 1,1 --col 1,2 --col 2,$(($vep_trans_col+1)) --col 2,$(($vep_gene_col+1)) --extra 2 --multiple 2 --header 1 > !{output,,project_large_annot_file} class_level project

compute_num_transcripts=tail -n+@4 !{input,,@1} | $smart_cut_cmd --tab-delim --select-col 0,'@2' | sed 's/$/\t1/' | $table_sum_stats_cmd --in-delim $tab --out-delim $tab --col @6 --group-col 1 @8 --totals --print-header | cut -f1,@7 | sed '1 s/.*/@5\t@3/'

large_disp=All GENCODE
medium_disp=Confirmed GENCODE
small_disp=CCDS

short cmd make_project_num_gene_transcripts_text_file=$smart_join_cmd --in-delim $tab --fill 1 --extra 1 --fill 2 --extra 2 --fill-value 0 --header 1 --exec "$compute_num_transcripts(project_small_transcript_gene_file,2,$small_disp,1,GENE,2,3,)" --exec "$compute_num_transcripts(project_medium_transcript_gene_file,2,$medium_disp,1,GENE,2,3,)" --exec "$compute_num_transcripts(project_large_transcript_gene_file,2,$large_disp,1,GENE,2,3,)" > !{output,,project_num_gene_transcripts_text_file} class_level project

!!expand:large:large:medium:small! \
cmd make_project_num_variant_large_transcripts_text_file=$compute_num_transcripts(project_large_annot_file,2 3,$large_disp,2,GENE\tVARIANT,3,2-3,--group-col 2) > !{output,,project_num_variant_large_transcripts_text_file} class_level project rusage_mod $variant_group_mem

cmd make_project_num_variant_transcripts_text_file=$smart_join_cmd --in-delim $tab --header 1 --exec "$smart_join_cmd --in-delim $tab --col 1 --col 2 --fill 1 --extra 1 --fill 2 --extra 2 --fill-value 0 --header 1 !{input,--file,project_num_variant_small_transcripts_text_file} !{input,--file,project_num_variant_medium_transcripts_text_file} !{input,--file,project_num_variant_large_transcripts_text_file}" !{input,--file,project_maf_file} --multiple 1 --extra 2 --col 1,2 | awk -F"\t" -v OFS="\t" '{t=\$1; \$1=\$2; \$2=t} {print}'  > !{output,,project_num_variant_transcripts_text_file} class_level project

prop max_num_transcripts=scalar default 15
prop num_gene_transcript_breaks=scalar default 60
prop num_variant_transcript_breaks=scalar default 120

small_color=red
medium_color=blue
large_color=gray

local cmd make_project_num_gene_transcripts_pdf_file=$draw_hist_plot_cmd !{input,,project_num_gene_transcripts_text_file} !{output,,project_num_gene_transcripts_pdf_file} 2,3,4 '' 'Number of transcripts per gene' colors='$small_color,$medium_color,$large_color' sep=$tab alpha=.4 x.max=!{prop,,project,max_num_transcripts} breaks=!{prop,,project,num_gene_transcript_breaks} overlay.density=T cex=1.1 class_level project

rare_maf=0.001
common_maf=0.01

rare_disp=rare
lowfreq_disp=low frequency
common_disp=common
all_disp=all

Rare_disp=Rare
Lowfreq_disp=Low frequency
Common_disp=Common
All_disp=All

awk_rare_filter=\$@1 > 0 && \$@1 <= $rare_maf
awk_lowfreq_filter=\$@1 > $rare_maf && \$@1 <= $common_maf
awk_common_filter=\$@1 > $common_maf


!!expand:,:common,Common:common,$common_disp:lowfreq,$lowfreq_disp:rare,$rare_disp! \
cmd make_project_num_common_variant_transcripts_pdf_file=awk -F"\t" 'NR == 1 || ($awk_common_filter(6))' !{input,,project_num_variant_transcripts_text_file} | $draw_hist_plot_cmd /dev/stdin !{output,,project_num_common_variant_transcripts_pdf_file} 3,4,5 '' 'Number of transcripts per Common variant' colors='$small_color,$medium_color,$large_color' sep=$tab alpha=.4 x.max=!{prop,,project,max_num_transcripts} breaks=!{prop,,project,num_variant_transcript_breaks} overlay.density=T cex=1.1 class_level project

common_color=cyan
lowfreq_color=cornflowerblue
rare_color=darkblue

!!expand:,:large,column:large,5:medium,4:small,3! \
short cmd make_project_num_variant_maf_large_transcripts_pdf_file=awk -F"\t" -v OFS="\t" 'NR == 1 {\$7="$Common_disp"; \$8="$Lowfreq_disp"; \$9="$Rare_disp"} NR > 1 { \$7=\$8=\$9="NA"; if ($awk_common_filter(6)) {\$7=\$column} else if ($awk_lowfreq_filter(6)) {\$8=\$column} else if ($awk_rare_filter(6)) {\$9=\$column}} {print}' !{input,,project_num_variant_transcripts_text_file} | $draw_hist_plot_cmd /dev/stdin !{output,,project_num_variant_maf_large_transcripts_pdf_file} 7,8,9 '' 'Number of transcripts per variant' colors='$common_color,$lowfreq_color,$rare_color' sep=$tab alpha=.4 x.max=!{prop,,project,max_num_transcripts} breaks=!{prop,,project,num_variant_transcript_breaks} overlay.cumulative=T legend.pos=right height.scale=.75 cex=1.1 class_level project

prop mask_filter_condition=scalar

!!expand:large:large:medium:small! \
large_setid_helper=cat !{input,,project_large_annot_file} | awk -F"\t" -v OFS="\t" 'NR == 1 {for(i=1;i<=NF;i++) {m[\$i]=i}} NR == 1 || (!{prop,,mask,mask_filter_condition}) {print \$m["$vep_gene_annot"],\$m["$vep_trans_annot"],\$m["@1"]}' | tail -n+2 | sort -u | sed '1 s/^/$vep_gene_annot\t$vep_trans_annot\tVAR\n/'

!!expand:large:large:medium:small! \
short cmd make_mask_large_setid_file=$large_setid_helper(VAR) > !{output,,mask_large_setid_file} class_level mask

!!expand:large:large:medium:small! \
short cmd make_mask_large_set_chrpos_file=$large_setid_helper(Location) | sed '1 s/VAR/Chr\tPos/' | sed '1! s/:\([0-9][0-9]*\)/\t\1/' > !{output,,mask_large_set_chrpos_file} class_level mask

!!expand:,:large,title:large,${large_disp}:medium,${medium_disp}:small,${small_disp}! \
short cmd make_mask_num_variant_large_transcripts_text_file=$smart_join_cmd --in-delim $tab --exec "$smart_join_cmd --in-delim $tab --fill 1 --extra 1 --fill-value 0 --header 1 --exec \"$compute_num_transcripts(mask_large_setid_file,1 3,title_!{prop;;mask},2,GENE\tVARIANT,3,2-3,--group-col 2)\" --exec \"sed '1 s/\(\t[^\t][^\t]*\)/\1_with_annot/g' !{input,,project_num_variant_large_transcripts_text_file}\" --col 1 --col 2" --exec "$smart_cut_cmd --in-delim $tab !{input,--file,project_num_gene_transcripts_text_file} --select-col 1,1 --select-col 1,1,'$large_disp' --vec-delim : | sed '1 s/\(\t[^\t][^\t]*\)/\1_total_for_gene/g'" --header 1 --extra 2 --fill 2 --fill-value 0 --multiple 1 | awk -F"\t" -v OFS="\t" 'NR == 1 {print \$0,"!{prop,,mask}_any","!{prop,,mask}_all"} NR > 1 {any=(\$3>0); all=(\$3==\$5); print \$0,any,all }' > !{output,,mask_num_variant_large_transcripts_text_file} class_level mask rusage_mod $variant_group_mem

!!expand:large:large:medium:small! \
short cmd make_mask_num_any_all_variant_large_transcripts_text_file=$smart_join_cmd !{input,--file,project_maf_file} --in-delim $tab !{input,--file,mask_num_variant_large_transcripts_text_file} --header 1 --extra 1 --col 2,2 --multiple 2 | awk -F"\t" -v OFS="\t" 'NR == 1 {print} NR > 1 && \$2 > 0 { f=\$2; \$2="$all_disp"; print; \$2=f; if ($awk_rare_filter(2)) { \$2="$rare_disp" } else if ($awk_lowfreq_filter(2)) { \$2="$lowfreq_disp" } else if ($awk_common_filter(2)) { \$2="$common_disp" } {print}}' | $table_sum_stats_cmd --in-delim $tab --out-delim $tab --col !{prop,,mask}_any --col !{prop,,mask}_all --totals --summaries --print-header --has-header --group-col MAF | $smart_cut_cmd --in-delim $tab --select-col 0,1,MAF --select-col 0,1,!{prop,,mask}_all_tot --select-col 0,1,!{prop,,mask}_all_mean --select-col 0,1,!{prop,,mask}_any_tot --select-col 0,1,!{prop,,mask}_any_mean --require-col-match -x | $add_function_cmd --in-delim $tab --header 1 --val-header !{prop,,mask}_fraction_constant --col1 !{prop,,mask}_all_tot --col2 !{prop,,mask}_any_tot --type divide | $add_function_cmd --in-delim $tab --header 1 --val-header !{prop,,mask}_fraction_which_change --val1 1 --col2 !{prop,,mask}_fraction_constant --type subtract > !{output,,mask_num_any_all_variant_large_transcripts_text_file} class_level mask

num_variants_header=NUM_VARIANTS

compute_num_transcripts=tail -n+@4 !{input,,@1} | $smart_cut_cmd --tab-delim --select-col 0,'@2' | sed 's/$/\t1/' | $table_sum_stats_cmd --in-delim $tab --out-delim $tab --col @6 --group-col 1 @8 --totals --print-header | cut -f1,@7 | sed '1 s/.*/@5\t@3/'

most_del_disp=MOST_DEL
least_del_disp=LEAST_DEL

!!expand:large:large:medium:small! \
cmd make_mask_gene_size_large_most_del_text_file=$smart_cut_cmd !{input,--file,mask_num_variant_large_transcripts_text_file} --tab-delim --select-col 1,1,'GENE !{prop,,mask}_any !{prop,,mask}_all' | $table_sum_stats_cmd --in-delim $tab --out-delim $tab --has-header --col !{prop,,mask}_any --col !{prop,,mask}_all  --group-col GENE --totals --print-header | $smart_cut_cmd --in-delim $tab --select-col 0,1,'GENE !{prop,,mask}_any_tot !{prop,,mask}_all_tot' | sed '1 s/.*/GENE\t$most_del_disp\t$least_del_disp/' > !{output,,mask_gene_size_large_most_del_text_file} class_level mask

!!expand:large:large:medium:small! \
cmd make_mask_gene_size_large_transcripts_text_file=$smart_join_cmd --header 1 --in-delim $tab --exec "$compute_num_transcripts(mask_large_setid_file,1 2,$num_variants_header,2,GENE\tTRANSCRIPT,3,2-3,--group-col 2)" --exec "$smart_cut_cmd --in-delim $tab !{input,--file,project_large_transcript_gene_file} --select-col 1,'2 1' | sed '1 s/^/GENE\tTRANSCRIPT\n/' | sed 's/$/\t0/'" --merge --col 1 --col 2 > !{output,,mask_gene_size_large_transcripts_text_file} class_level mask

low_quantile=.25
med_quantile=.50
high_quantile=.75

appris_header=APPRIS

!!expand:large:large:medium:small! \
cmd make_mask_gene_size_stats_large_transcripts_text_file=$smart_join_cmd --exec "$smart_join_cmd --in-delim $tab !{input,--file,mask_gene_size_large_transcripts_text_file} !{input,--file,project_gene_canonical_file} !{input,--file,mask_gene_size_large_most_del_text_file} --header 1 --multiple 1 --extra 2 --fill 2 --extra 3 --fill 3 --fill-value 0" --exec "cut -f2,3 !{input,,project_gene_appris_file} | sed '1 s/^/Gene\tTranscript\t$appris_header\n/' | sed '1! s/$/\t0/'" --col 1 --col 2 --extra 2 --fill 2 --fill-value NA --header 1 | awk -F"\t" -v OFS="\t" 'NR > 1 {if (\$4 == \$2) {\$4 = \$3} else {\$4 = 0} if (\$7 == 0) {\$7 = \$3} else {\$7 = "NA"}} {print}'  | $smart_cut_cmd --in-delim $tab --exclude-row 0,1,$num_variants_header,eq:0 | $table_sum_stats_cmd --in-delim $tab --out-delim $tab --has-header --print-header --group-col GENE --col $num_variants_header --col $canonical_header --col $appris_header --col $most_del_disp --col $least_del_disp --quantile $med_quantile --quantile $low_quantile --quantile $high_quantile --quantile 0 --quantile 1 --totals --summaries | $smart_cut_cmd --in-delim $tab --select-col 0,1,'GENE ${num_variants_header}_mean ${num_variants_header}_quant_$med_quantile ${num_variants_header}_quant_$low_quantile ${num_variants_header}_quant_$high_quantile ${least_del_disp}_quant_0 ${most_del_disp}_quant_0 ${canonical_header}_tot ${appris_header}_mean' --exact --require-col-match | awk -F"\t" -v OFS="\t" 'NR == 1 {\$2="Mean"; \$3="50pct"; \$4="25pct"; \$5="75pct"; \$6="LEAST_DEL"; \$7="MOST_DEL"; \$8="CANONICAL"; \$9="APPRIS"; print \$0,"MOST_LEAST_CHANGE","MOST_CANONICAL_CHANGE","MOST_APPRIS_CHANGE","FOLD_CHANGE","MOST_MEAN_CHANGE","MEAN_LEAST_CHANGE","CANONICAL_MEAN_CHANGE","MOST_MEDIAN_CHANGE","MEDIAN_LEAST_CHANGE","CANONICAL_MEDIAN_CHANGE"; for (i=1;i<=NF;i++) {m[\$i]=i}} NR > 1 {if (\$m["CANONICAL"] != "NA" && \$m["CANONICAL"] > 0) {most_canonical=\$m["MOST_DEL"]/\$m["CANONICAL"]} else {most_canonical="NA"} if (\$m["APPRIS"] != "NA" && \$m["APPRIS"] > 0) {most_appris=\$m["MOST_DEL"]/\$m["APPRIS"]} else {most_appris="NA"} if (\$m["LEAST_DEL"] != "NA" && \$m["LEAST_DEL"] > 0) {most_least=\$m["MOST_DEL"]/\$m["LEAST_DEL"]; mean_least=\$m["Mean"]/\$m["LEAST_DEL"]; median_least=\$m["50pct"]/\$m["LEAST_DEL"]} else {most_least="NA"; mean_least="NA"; median_least="NA"} if (\$m["Mean"] != "NA" && \$m["Mean"] > 0) {most_mean=\$m["MOST_DEL"]/\$m["Mean"]; canonical_mean=\$m["CANONICAL"]/\$m["Mean"]} else {most_mean="NA"; canonical_mean="NA"} if (\$m["50pct"] != "NA" && \$m["50pct"] > 0) {most_median=\$m["MOST_DEL"]/\$m["50pct"]; canonical_median=\$m["CANONICAL"]/\$m["50pct"]} else {most_median="NA"; canonical_median="NA"} print \$0,most_least,most_canonical,most_appris,\$m["75pct"]/\$m["25pct"],most_mean,mean_least,canonical_mean,most_median,median_least,canonical_median}' > !{output,,mask_gene_size_stats_large_transcripts_text_file} class_level mask

short cmd make_mask_gene_size_stats_transcripts_text_file=$smart_join_cmd --in-delim $tab --exec "tail -qn+2 !{input,,mask_gene_size_stats_large_transcripts_text_file} !{input,,mask_gene_size_stats_medium_transcripts_text_file} !{input,,mask_gene_size_stats_small_transcripts_text_file} | cut -f1 | sort -u | sed '1 s/^/Gene\n/'" --exec "sed '1 s/\(\S\S*\)/${small_disp}_\1/g' !{input,,mask_gene_size_stats_small_transcripts_text_file}" --exec "sed '1 s/\(\S\S*\)/${medium_disp}_\1/g' !{input,,mask_gene_size_stats_medium_transcripts_text_file}" --exec "sed '1 s/\(\S\S*\)/${large_disp}_\1/g' !{input,,mask_gene_size_stats_large_transcripts_text_file}" --header 1 --rest-extra 1 --fill 2 --fill 3 --fill 4 > !{output,,mask_gene_size_stats_transcripts_text_file} class_level mask

!!expand:common:all:common:lowfreq:rare! \
!!expand:large:large:medium:small! \
local cmd make_project_num_any_all_common_variant_large_transcripts_text_file=$smart_cut_cmd --tab-delim --exec "sed '1 s/!{prop,,mask,limit=1}_//g' !{input,,mask_num_any_all_variant_large_transcripts_text_file,limit=1} | sed '1 s/MAF/Mask/'" --select-row 1,1 | $smart_cut_cmd --tab-delim !{raw;;mask;--exec "$smart_cut_cmd --tab-delim --file *mask_num_any_all_variant_large_transcripts_text_file --select-row 1,1,MAF,$common_disp | sed 's/^\S\S*/@disp/'"} !{input,mask_num_any_all_variant_large_transcripts_text_file} --stdin-first > !{output,,project_num_any_all_common_variant_large_transcripts_text_file} class_level project

Annotated=\# annotated
Annotation_changes=\% variable

local cmd make_project_num_any_all_all_variant_tex_file=$smart_cut_cmd --in-delim $tab --exclude-col 2-3,1 --paste --exec "$smart_cut_cmd !{input,--file,project_num_any_all_all_variant_small_transcripts_text_file} --in-delim $tab -x --require-col-match --select-col 1,1 --select-col 1,1,'any_tot any_mean fraction_which_change' | sed '1 s/\(.*\)/\1\n\1/' | sed '1 s/\t\S\S*/\t$small_disp/g'" --exec "$smart_cut_cmd !{input,--file,project_num_any_all_all_variant_medium_transcripts_text_file} --in-delim $tab -x --require-col-match --select-col 1,1 --select-col 1,1,'any_tot any_mean fraction_which_change' | sed '1 s/\(.*\)/\1\n\1/' | sed '1 s/\t\S\S*/\t$medium_disp/g'" --exec "$smart_cut_cmd !{input,--file,project_num_any_all_all_variant_large_transcripts_text_file} --in-delim $tab -x --require-col-match --select-col 1,1 --select-col 1,1,'any_tot any_mean fraction_which_change' | sed '1 s/\(.*\)/\1\n\1/' | sed '1 s/\t\S\S*/\t$large_disp/g'" | $format_columns_cmd --in-delim $tab --header 2 --commify 2 --commify 5 --commify 8 --number-format '3 4 6 7 9 10',%.1f --percentage '3 4 6 7 9 10' | sed '2 s/\tany_tot\tany_mean\tfraction_which_change/\t$Annotated\t$Annotated\t$Annotation_changes/g' | awk -F"\t" -v OFS="\t" 'NR > 2 {for (i=3;i<=NF;i+=3) {\$i="("\$i")"}} {print}' | sed 's/\(strict\|broad\)/\$_{\\text{\1}}$/' | $table_to_beamer_cmd --in-delim $tab --header-cols 1 --header-rows 2 --auto-dim --multi-col 1-2 --multi-row 1 --left-align 1 `for i in 2 5 8; do echo --right-align \$i --right-align \$((i+1)) --right-align \$((i+2)); done` > !{output,,project_num_any_all_all_variant_tex_file} class_level project

local cmd make_project_num_any_all_all_variant_pdf_file=$run_latex_cmd(project_num_any_all_all_variant_tex_file,project_num_any_all_all_variant_pdf_file) class_level project

!!expand:large:large:medium:small! \
local cmd make_project_gene_size_stats_large_transcripts_text_file=$smart_join_cmd --in-delim $tab --exec "tail -qn+2 !{input,,mask_gene_size_stats_large_transcripts_text_file} | cut -f1 | sort -u | sed '1 s/^/GENE\n/'" !{raw,,mask,--exec "sed '1 s/\(\S\S*\)/@{disp}_\1/g' *mask_gene_size_stats_large_transcripts_text_file"} !{input,mask_gene_size_stats_large_transcripts_text_file} --header 1 --rest-extra 1 `i=2; for f in !{prop,,mask}; do echo --fill \$i; i=$((i+1)); done` --fill-value NA > !{output,,project_gene_size_stats_large_transcripts_text_file} class_level project

prop max_fold_increase=scalar default 3
prop num_fold_breaks=scalar

#!!expand:,:canonical,Max,Min,cols:max,most deleterious,least deleterious,10:canonical,most deleterious,canonical transcript,11:appris,most deleterious,appris transcript,12:quantile,75th percentile,25th percentile,13:max_mean,most deleterious,mean,14:mean_min,mean,least deleterious,15:canonical_mean,canonical,mean,16:max_med,most deleterious,median,17:med_min,median,least deleterious,18:canonical_med,canonical,median,19! \

prop r_disp=scalar

#CHANGE BACK TO LOCAL

!!expand:,:canonical,Max,Min,cols:max_mean,"most deleterious",mean,14:max_med,most deleterious,median,17! \
short cmd make_mask_gene_size_canonical_transcripts_pdf_file=sed '1 s/\($small_disp\|$medium_disp\|$large_disp\)_\S\S*/\1/g' !{input,,mask_gene_size_stats_transcripts_text_file} | $draw_hist_plot_cmd /dev/stdin !{output,,mask_gene_size_canonical_transcripts_pdf_file} cols,$((cols+18)),$((cols+36)) '' 'Fold-increase in variants between Max and Min transcript' colors='$small_color,$medium_color,$large_color' sep=$tab alpha=.4 x.max=!{prop,,project,max_fold_increase} !{prop,breaks=,project,num_fold_breaks,if_prop=num_fold_breaks,allow_empty=1} overlay.density=T overlay.cumulative=T legend.pos=topright log=y inset=0,0.2 title.shift=-2 cex=1.1 class_level mask

#!!expand:;:canonical;Min;Max;col1;col2:max;Least deleterious;Most deleterious;LEAST_DEL;MOST_DEL:canonical;Canonical transcript;Most deleterious;CANONICAL;MOST_DEL:appris;APPRIS transcript;Most deleterious;APPRIS;MOST_DEL:quantile;25th percentile;75th percentile;25pct;75pct:max_mean;Mean number of variants;Most deleterious;Mean;MOST_DEL:mean_min;Least deleterious;Mean number of variants;LEAST_DEL;Mean:canonical_mean;Mean number of variants;Canonical transcript;Mean;CANONICAL:max_med;Median number of variants;Most deleterious;50pct;MOST_DEL:med_min;Least deleterious;Median number of variants;LEAST_DEL;50pct:canonical_med;Median number of variants;Canonical transcript;50pct;CANONICAL! \

!!expand:;:canonical;Min;Max;col1;col2:max_mean;Mean number of variants;Most deleterious;Mean;MOST_DEL:max_med;Median number of variants;Most deleterious;50pct;MOST_DEL! \
local cmd make_mask_gene_size_canonical_transcripts_scatter_pdf_file=$smart_cut_cmd --tab-delim --exec "sed 's/$/\t$small_disp\t$small_color\t3/g' !{input,,mask_gene_size_stats_transcripts_text_file}" --exec "sed 's/$/\t$medium_disp\t$medium_color\t2/g' !{input,,mask_gene_size_stats_transcripts_text_file}" --exec "sed 's/$/\t$large_disp\t$large_color\t1/g' !{input,,mask_gene_size_stats_transcripts_text_file}" --vec-delim : --select-col 1-3,1 --exclude-row 1-3,1 --select-col 1,1,'${small_disp}_col1:${small_disp}_col2:${small_disp}:$small_color:3' --select-col 2,1,'${medium_disp}_col1:${medium_disp}_col2:${medium_disp}:$medium_color:2' --select-col 3,1,'${large_disp}_col1:${large_disp}_col2:${large_disp}:$large_color:1' --exact --require-col-match | sed '1 s/^/Gene\tMin\tMax\tTranscripts\tColor\tOrder\n/' | $draw_matrix_plot_cmd /dev/stdin !{output,,mask_gene_size_canonical_transcripts_scatter_pdf_file} 'Number of variants in each gene' 'Min,Max' sep=$tab fill.col=Color order.col=Order fill.label=Transcripts alpha=.2 rev.legend=TRUE class_level mask


min_fold_bin=1
max_fold_bin=3
bin_width=.2

!!expand:large:large:medium:small! \
local cmd make_mask_gene_size_strat_large_transcripts_text_file=$smart_cut_cmd --in-delim $tab !{input,--file,mask_gene_size_stats_large_transcripts_text_file} --exact --require-col-match --select-col 1,1,'GENE FOLD_CHANGE 50pct' | awk -F"\t" -v OFS="\t" 'NR > 1 {\$2 = int(\$2 / $bin_width) * $bin_width} NR == 1 || (\$2 >= $min_fold_bin && \$2 <= $max_fold_bin) {print}' > !{output,,mask_gene_size_strat_large_transcripts_text_file} class_level mask

!!expand:large:large:medium:small! \
local cmd make_mask_gene_size_strat_large_transcripts_pdf_file=sed '1 s/50pct/Size of genes by fold increase/' !{input,,mask_gene_size_strat_large_transcripts_text_file} | $draw_box_plot_cmd /dev/stdin !{output,,mask_gene_size_strat_large_transcripts_pdf_file} '' 'Fold increase' 'Median number of variants' 'Size of genes by fold increase' header=TRUE label=FOLD_CHANGE order=FOLD_CHANGE sep=$tab show.p=FALSE class_level mask

!!expand:,:plotsmall_,plotheight,plotcex:,.8,1.1:small_,.7,1.3! \
!!expand:,:lambdav,freqv:.5,.001:4,.001:.5,.01:4,.01! \
local cmd make_mask_analytical_sample_size_lambdalambdav_freqfreqv_plotsmall_pdf_file=sed '1 s/\($small_disp\|$medium_disp\|$large_disp\)_\S\S*/\1/g' !{input,,mask_gene_size_stats_transcripts_text_file} | cut -f14,$((14+18)),$((14+36)) | awk -v OFS="\t" -F"\t" 'function rel_n(lambda, f) { return (1 / ((1 + lambda) * f * log(1 + lambda) + (1 - (1 + lambda) * f) * log((1 - (1 + lambda) * f)/(1 - f)))) } NR > 1 {for (i=1;i<=NF;i++) {if (\$i != "NA") {\$i=rel_n(lambdav, freqv)/rel_n(lambdav/\$i, freqv*\$i)}}} {print \$0}' | $draw_hist_plot_cmd /dev/stdin !{output,,mask_analytical_sample_size_lambdalambdav_freqfreqv_plotsmall_pdf_file} 1,2,3 '' 'Relative sample size of "most deleterious"' colors='$small_color,$medium_color,$large_color' sep=$tab alpha=.4 x.min=.5 x.max=1 rev.x=T !{prop,breaks=,project,num_fold_breaks,if_prop=num_fold_breaks,allow_empty=1} overlay.cumulative=T legend.pos=bottomright log=y title.shift=-2 ylab="Number of genes" height.scale=plotheight cex=plotcex class_level mask

prop gene_list=list
prop transcript_list=list

!!expand:large:large:medium:small! \
local cmd make_special_gene_list_large_stats_txt_file=$smart_join_cmd --header 1 --in-delim $tab --extra 1 --exec "sed '1 s/^/Gene\tGene\n/' !{input,,project_gene_name_map_file}" --exec "$smart_join_cmd --in-delim $tab --header 1 !{input,mask_gene_size_stats_transcripts_text_file} --exec \"tail -qn+2  !{input,,mask_gene_size_stats_transcripts_text_file} | cut -f1 | sort -u | sed '1 s/^/Gene\n/'\" !{raw;;mask;--exec \"$smart_cut_cmd --in-delim $tab --file *mask_gene_size_stats_transcripts_text_file --exact --require-col-match --vec-delim : --select-col 1,1,'Gene:${large_disp}_MOST_DEL' | sed '1 s/\t\(\S\S*\)/\t@{disp}_\1/g'\"} --rest-extra 1 --fill-all --fill-value 0 | $smart_cut_cmd --in-delim $tab --select-row 0,1 --select-row 0,1,Gene,'!{prop;;special_gene_list;gene_list;sep=|}' --exact --require-col-match" --exec "$smart_join_cmd --in-delim $tab --header 1 !{input,mask_gene_size_large_transcripts_text_file} --exec \"tail -qn+2  !{input,,mask_gene_size_large_transcripts_text_file} | cut -f1-2 | sort -u | sed '1 s/^/Gene\tTRANSCRIPT\n/'\" !{raw,,mask,--exec \"sed '1 s/\(\S\S*\)$/@{disp}_TRANSCRIPT/' *mask_gene_size_large_transcripts_text_file\"} --col 1 --col 2 --rest-extra 1 --fill-all --fill-value 0 | $smart_cut_cmd --in-delim $tab --vec-delim : --select-row 0,1 --select-row 0,1,TRANSCRIPT,'!{prop;;special_gene_list;transcript_list;sep=|}' --exclude-col 0,1,TRANSCRIPT --exact --require-col-match" !{raw;;mask;| $add_function_cmd --in-delim $tab --header 1 --val-header '@{disp}_MOST_DEL_TRANSCRIPT_FOLD_CHANGE' --col1 '@{disp}_${large_disp}_MOST_DEL' --col2 '@{disp}_TRANSCRIPT' --type divide} | cut -f2- | sed '1 s/${large_disp}_//g' > !{output,,special_gene_list_large_stats_txt_file} class_level special_gene_list

!!expand:large:large:medium:small! \
local cmd make_special_gene_list_large_stats_tex_file=cat !{input,,special_gene_list_large_stats_txt_file} | $smart_cut_cmd --in-delim $tab --select-col 0,1,Gene --vec-delim : !{raw;;mask;--select-col 0,1,'@{disp}_TRANSCRIPT:@{disp}_MOST_DEL:@{disp}_MOST_DEL_TRANSCRIPT_FOLD_CHANGE'} --no-regex --require-col-match | sed '1 s/\(.*\)/\1\n\1/' | sed '1 s/_\(MOST_DEL\|TRANSCRIPT\|FOLD_CHANGE\)//g' | sed '2 s/\(!{prop,,mask,disp,sep=\|}\)_//g' | sed '2 s/MOST_DEL_TRANSCRIPT_FOLD_CHANGE/Fold change/g' | sed '2 s/MOST_DEL/Most del./g' | sed '2 s/TRANSCRIPT/Transcript/g' | $format_columns_cmd --in-delim $tab --header 2 --number-format '4 7 10 13',%.2f | $table_to_beamer_cmd --in-delim $tab --header-cols 1 --header-rows 2 --auto-dim --multi-col 1-2 --multi-row 1 --left-align 1 `i=2; for m in !{prop,,mask}; do for g in 0 1 2; do echo --right-align $((i+g)); done; i=$((i+3)); done` > !{output,,special_gene_list_large_stats_tex_file} class_level special_gene_list

!!expand:large:large:medium:small! \
local cmd make_special_gene_list_large_stats_pdf_file=$run_latex_cmd(special_gene_list_large_stats_tex_file,special_gene_list_large_stats_pdf_file) class_level special_gene_list

prop color=scalar

#!!expand:,:canonical,Max,Min,cols:max,most deleterious,least deleterious,MOST_LEAST_CHANGE:canonical,most deleterious,canonical transcript,CANONICAL:appris,most deleterious,appris transcript,APPRIS:quantile,75th percentile,25th percentile,FOLD_CHANGE:max_mean,most deleterious,mean,MOST_MEAN_CHANGE:mean_min,mean,least deleterious,MEAN_LEAST_CHANGE:canonical_mean,canonical,mean,CANONICAL_MEAN_CHANGE:max_med,most deleterious,median,MOST_MEDIAN_CHANGE:med_min,median,least deleterious,MEDIAN_LEAST_CHANGE:canonical_med,canonical,median,CANONICAL_MEDIAN_CHANGE! \

!!expand:,:canonical,Max,Min,cols:max_mean,most deleterious,mean,MOST_MEAN_CHANGE:max_med,most deleterious,median,MOST_MEDIAN_CHANGE! \
!!expand:large:large:medium:small! \
local cmd make_project_gene_size_canonical_large_transcripts_pdf_file=$smart_cut_cmd --in-delim $tab !{input,--file,project_gene_size_stats_large_transcripts_text_file} --select-col 1,1 --select-col 1,1,'cols' --vec-delim : | sed 's/_cols//g' | $draw_hist_plot_cmd /dev/stdin !{output,,project_gene_size_canonical_large_transcripts_pdf_file} '!{prop::mask:disp:sep=,}' '' 'Fold-increase in variants between Max and Min transcript' colors='!{prop::mask:color:sep=,}' sep=$tab alpha=.4 x.max=!{prop,,project,max_fold_increase} !{prop,breaks=,project,num_fold_breaks,if_prop=num_fold_breaks,allow_empty=1} overlay.density=T overlay.cumulative=T legend.pos=topright log=y inset=0,0.2 cex=1.1 class_level project

!!expand:,:canonical,Max,Min,intcols,Cols:max_mean,most deleterious,mean,Mean MOST_DEL MOST_MEAN_CHANGE,Mean\tMost del.\tFold change:max_med,most deleterious,median,50pct MOST_DEL MOST_MEDIAN_CHANGE,Median transcript\tMost del.\tFold change! \
!!expand:large:large:medium:small! \
local cmd make_project_gene_size_canonical_large_transcripts_sum_text_file=$smart_cut_cmd --in-delim $tab --exclude-row .,1 !{input,mask_gene_size_stats_large_transcripts_text_file} !{raw::mask:--exec "$smart_cut_cmd --in-delim $tab --file *mask_gene_size_stats_large_transcripts_text_file --select-col 1,1 --select-col 1,1,'intcols' | $table_sum_stats_cmd --has-header --print-header \`for f in intcols; do echo --col \\$f; done\` --summaries --in-delim $tab --out-delim $tab | $smart_cut_cmd --in-delim $tab --select-col 0,1,mean | sed 's/_mean//g' | sed 's/^/@disp\t/'"} | sed '1 s/^/Mask\tCols\n/' | $format_columns_cmd --in-delim $tab --header 1 --number-format '2 3 4',%.2f > !{output,,project_gene_size_canonical_large_transcripts_sum_text_file} class_level project

!!expand:,:canonical,Max,Min,intcols,Cols:max_mean,most deleterious,mean,Mean MOST_DEL MOST_MEAN_CHANGE,Mean\tMost del.\tFold change:max_med,most deleterious,median,50pct MOST_DEL MOST_MEDIAN_CHANGE,Median transcript\tMost del.\tFold change! \
!!expand:large:large:medium:small! \
local cmd make_project_gene_size_canonical_large_transcripts_sum_tex_file=cat !{input,,project_gene_size_canonical_large_transcripts_sum_text_file} | $table_to_beamer_cmd --in-delim $tab --header-cols 1 --header-rows 1 --auto-dim --left-align 1 > !{output,,project_gene_size_canonical_large_transcripts_sum_tex_file} class_level project

!!expand:,:canonical:max_mean:max_med! \
!!expand:large:large:medium:small! \
local cmd make_project_gene_size_canonical_large_transcripts_sum_pdf_file=$run_latex_cmd(project_gene_size_canonical_large_transcripts_sum_tex_file,project_gene_size_canonical_large_transcripts_sum_pdf_file) class_level project

!!expand:,:canonical,Max,Min,intcols,Cols:max_mean,most deleterious,mean,Mean MOST_DEL MOST_MEAN_CHANGE,Mean\tMost del.\tFold change:max_med,most deleterious,median,50pct MOST_DEL MOST_MEDIAN_CHANGE,Median transcript\tMost del.\tFold change! \
local cmd make_project_gene_size_canonical_transcripts_sum_tex_file=$smart_cut_cmd --in-delim $tab --exec "cat !{input,,project_gene_size_canonical_small_transcripts_sum_text_file} | sed '1 s/\(.*\)/\1\n\1/' | sed '1 s/\t[^\t][^\t]*/\t$small_disp/g'" --exec "cat !{input,,project_gene_size_canonical_medium_transcripts_sum_text_file} | sed '1 s/\(.*\)/\1\n\1/' | sed '1 s/\t[^\t][^\t]*/\t$medium_disp/g'" --exec "cat !{input,,project_gene_size_canonical_large_transcripts_sum_text_file} | sed '1 s/\(.*\)/\1\n\1/' | sed '1 s/\t[^\t][^\t]*/\t$large_disp/g'" --paste --exclude-col 2-3,1 | $table_to_beamer_cmd --in-delim $tab --header-cols 1 --header-rows 2 --multi-row 1 --multi-col 1 --auto-dim --left-align 1 > !{output,,project_gene_size_canonical_transcripts_sum_tex_file} class_level project

!!expand:,:canonical:max_mean:max_med! \
local cmd make_project_gene_size_canonical_transcripts_sum_pdf_file=$run_latex_cmd(project_gene_size_canonical_transcripts_sum_tex_file,project_gene_size_canonical_transcripts_sum_pdf_file) class_level project

    """

}

