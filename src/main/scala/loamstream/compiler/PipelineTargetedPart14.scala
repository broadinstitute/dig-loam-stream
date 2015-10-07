
package loamstream.compiler

/**
 * LoamStream
 * Created by oliverr on 10/6/2015.
 */
object PipelineTargetedPart14 {
  val string =
 """  | $vcf_utils_cmd --print-annots | $smart_cut_cmd --in-delim $tab --select-col 0,1,ID --select-col 0,1,"\$cols" \ 
  > !{output,,projectt_snpsift_file} class_level projectt runif

!|expand:,:shortt,projectt,projecte,runif:,project,,run_if !num_var_subsets:short,project_variant_subset,_project_variant_subset,| \
shortt cmd make_projectt_snpeff_file=$combine_vcfs_for_annotation(projectt,) | cut -f1-8 | $snpeff_exe_helper(txt) | tail -n+3 | sed '1 s/\\# //' | awk -v OFS="\t" -F"\t" 'NR == 1 {print 0,\$0} NR > 1 { if (/SPLICE/) {print 1,\$0} else {print NR,\$0} }' | sort -t$tab -k1 | cut -f2- | awk -v OFS="\t" -F"\t" '{k=\$$snpeff_id_col":"\$$snpeff_trans_col} !m[k] {m[k]=1; print}' > !{output,,projectt_snpeff_file} class_level projectt runif

!|expand:,:shortt,projectt,projecte,runif:,project,,run_if !num_var_subsets:short,project_variant_subset,_project_variant_subset,| \
shortt cmd make_projectt_chaos_vcf_file=$combine_vcfs_for_annotation(projectt,) | cut -f1-8 | $chaos_cmd addtx -i /dev/stdin !{output,-o,projectt_chaos_vcf_file} class_level projectt runif

chaos_merge_cols=4, 5

!|expand:,:shortt,projectt,projecte,runif:,project,,run_if !num_var_subsets:short,project_variant_subset,_project_variant_subset,| \
shortt cmd make_projectt_chaos_file=$chaos_cmd extract !{input,--vcf,projectt_chaos_vcf_file} | sed '1 s/.*/ID\tGENE\tTRANS\tCHANGE\tTYPE/' | perl -ne '@a = split("\t", \$_, -1); if (\$a[$chaos_trans_col - 1] =~ /(.+)\.[0-9]+/) {\$a[$chaos_trans_col - 1] = \$1} print join("\t", @a)' | awk -F"\t" -v OFS="\t" '{print NR,\$0}' | sort -t$tab -k$((1+$chaos_id_col)),$((1+$chaos_id_col)) -k$((1+$chaos_trans_col)),$((1+$chaos_trans_col)) | uniq -f1 | perl -lne 'chomp; @a = split("\t"); \$key="\$a[$chaos_id_col]\t\$a[$chaos_trans_col]"; if (@last_line && \$last_key eq \$key) {foreach \$c ($chaos_merge_cols) {\$a[\$c]="\$last_line[\$c],\$a[\$c]"}} elsif (@last_line) {print join("\t", @last_line)} \$last_key = \$key; @last_line = @a; END {if (@last_line) {print join("\t", @last_line)}}' | sort -nk1,1 | cut -f2- > !{output,,projectt_chaos_file} class_level projectt runif

vep_custom_flags=`(echo $vep_custom_tabix && echo $vep_custom_names) | $transpose_cmd | awk -v OFS=, 'NF == 2 {print \$1,\$2,"bed","exact"}' | sed 's/^/-custom /' | tr '\n' ' '`
vep_custom_cols=`echo $vep_custom_names | sed 's/\s*\(\S\S*\)/,\1/g'`

prop do_hgvs=scalar

!|expand:,:shortt,projectt,projecte,runif:,project,,run_if !num_var_subsets:short,project_variant_subset,_project_variant_subset,| \
shortt cmd make_projectt_vep_file=$combine_vcfs_for_annotation(projectt,) | $vep_cmd --offline !{raw,--fasta,project,$ensembl_reference,if_prop=do_hgvs,allow_empty=1} -o STDOUT --dir $ensembl_cache_dir --polyphen b --sift b --ccds --canonical --regulatory --domains flags --plugin Condel,$vep_condel_config_dir,b,2 --plugin LoF,human_ancestor_fa:$loftee_human_ancestor_path $vep_custom_flags !{raw,,project,--hgvs,if_prop=do_hgvs,allow_empty=1} --fields Uploaded_variation,Location,Allele,Gene,Feature,Feature_type,Consequence,cDNA_position,CDS_position,Protein_position,Amino_acids,Codons,Existing_variation,CCDS,CANONICAL,HGNC,ENSP,DOMAINS,MOTIF_NAME,MOTIF_POS,HIGH_INF_POS,MOTIF_SCORE_CHANGE,SIFT,PolyPhen,Condel,LoF,LoF_filter,LoF_flags$vep_custom_cols!{raw::project:,HGVSc,HGVSp:if_prop=do_hgvs:allow_empty=1} | fgrep -v \\\#\\\# !{raw,,project,| tail -n+2,if_prop=do_hgvs,allow_empty=1} > !{output,,projectt_vep_file} class_level projectt runif rusage_mod $vep_mem

!|expand:,:shortt,projectt,projecte,runif:,project,,run_if !num_var_subsets:short,project_variant_subset,_project_variant_subset,| \
shortt cmd make_projectt_parsed_vep_file=eval "cat !{input,,projectt_vep_file} | $replace_missing_helper()" | awk -v OFS="\t" -F"\t" '{k=\$$vep_id_col":"\$$vep_trans_col} !m[k] {m[k]=1; print}' | sed '1 s/^\\\#//' | awk -v OFS="\t" -F"\t" 'NR == 1 {for (i=1;i<=NF;i++) {m[\$i]=i} p[1]="PolyPhen"; p[2]="SIFT"; p[3]="Condel"; for (j=1;j<=3;j++) {\$m[p[j]]=\$m[p[j]]"_PRED\t"\$m[p[j]]"_SCORE"} aa=m["Amino_acids"]; pp=m["Protein_position"]; \$aa="Amino_acids\tProtein_change"} NR > 1 {for (j=1;j<=3;j++) {if (\$m[p[j]] == "$vep_missing_field") {\$m[p[j]]="$vep_missing_field\t$vep_missing_field"} else if (match(\$m[p[j]],/([^\(\)]*)\(([0-9\.][0-9\.]*)\)/,ary)) {\$m[p[j]]=ary[1]"\t"ary[2]} else {\$m[p[j]]=\$m[p[j]]"\t$vep_missing_field"}} if (\$aa == "$vep_missing_field" || \$pp == "$vep_missing_field") {\$aa=\$aa"\t$vep_missing_field"} else {if (match(\$aa,/^(.)\/(.)$/,ary)) {\$aa=\$aa"\tp."ary[1]\$pp""ary[2]} else if (match(\$aa,/^(.)$/,ary)) {\$aa=\$aa"\tp."ary[1]\$pp""ary[1]} else {\$aa=\$aa"\tp."\$pp\$aa}}} {print}' > !{output,,projectt_parsed_vep_file} class_level projectt runif

!|expand:,:shortt,projectt,projecte,runif:,project,,run_if !num_var_subsets:short,project_variant_subset,_project_variant_subset,| \
shortt cmd make_projectt_transcript_gene_map_file=$smart_join_cmd --in-delim $tab --exec "$smart_cut_cmd --in-delim $tab !{input,--file,projectt_parsed_vep_file} --select-col 1,1,'$vep_id_annot $vep_trans_annot' --exclude-row 1,1,$vep_trans_annot,$vep_missing_field --exact --require-col-match --exclude-row 1,1" --exec "$smart_cut_cmd --in-delim $tab !{input,--file,projectt_gene_variant_file} --exclude-row 1,1 --select-col 1,1,'ID GENE'" --multiple 1 --extra 2 | cut -f2- | $smart_cut_cmd --in-delim $tab --select-col 0,'2 1' | sort -u | sed '1 s/^/gene\ttranscript\n/' > !{output,,projectt_transcript_gene_map_file} class_level projectt runif with burden

short cmd make_project_transcript_gene_alias_file=tail -n+2 !{input,,project_transcript_gene_map_file} | sort | uniq -c | awk '{if (\$2 ~ /$outside_gene_name/) {print 2,\$0} else {print 1,\$0}}' | sort -k4,4 -k1,1n -k2,2nr | sed 's/^\s*//' | sed 's/\s\s*/\t/g' | cut -f3- | uniq -f1 | $smart_cut_cmd --in-delim $tab --select-col 0,'2 1' | sed '1 s/^/\#transcript\tgene\n/' > !{output,,project_transcript_gene_alias_file} class_level project with burden


!|expand:,:shortt,projectt,projecte,runif:,project,,run_if !num_var_subsets:short,project_variant_subset,_project_variant_subset,| \
shortt cmd make_projectt_pph2_pos_file=(zcat $pph2_whess_file | head -n1; $combine_vcfs_for_annotation(projectt,) | grep -v \\\# | cut -f1,2 | while read f; do $tabix_cmd $pph2_whess_file `echo \\$f | awk '{print \$1":"\$2"-"\$2+1}'`; done) > !{output,,projectt_pph2_pos_file} class_level projectt runif

whess_chr_col=1
whess_pos_col=2
whess_nt1_col=6
whess_nt2_col=6

!|expand:,:projectt,projecte,runif:project,,run_if !num_var_subsets:project_variant_subset,_project_variant_subset,| \
cmd make_projectt_pph2_file=$smart_join_cmd --header 1 --in-delim $tab --rest-extra 1 --exec "$smart_cut_cmd --in-delim $tab --exec \"$combine_vcfs_for_annotation(projectt,\\\) | grep -v \\\\\\\\\# | cut -f1,2,4,5\" --exec \"$smart_cut_cmd --in-delim $tab !{input,--file,projectt_pph2_pos_file} --exclude-row 1,1 --select-col 1,1,'chr pos nt1 nt2' | sort -u\" | sort | uniq -d | sed '1 s/^/CHR\tPOS\tREF\tALT\n/'" --exec "$combine_vcfs_for_annotation(projectt,\) | grep -v \\\\\#\\\\\# | $smart_cut_cmd --in-delim $tab --select-col 0,'1 2 4 5 3'" --exec "$smart_cut_cmd --in-delim $tab !{input,--file,projectt_pph2_pos_file} --select-col 1,1,'chr pos nt1 nt2 ENSEMBL .'" --multiple 3 --col 1 --col 2 --col 3 --col 4 | sed 's/\t */\t/g' | sed 's/ *\t/\t/g' | awk -F"\t" -v OFS="\t" '{k=\$1":"\$2":"\$3":"\$4":"\$5":"\$6} !m[k] {print; m[k]=1}' > !{output,,projectt_pph2_file} class_level projectt runif

!!expand:custom_var:custom_var:custom_trans! \
short cmd make_subset_project_variant_subset_custom_var_annot_file=$smart_join_cmd --in-delim $tab --header 1 --exec "$combine_vcfs_for_annotation(project_variant_subset,\) | fgrep -v '\\#' | cut -f3 | sort -u | $smart_cut_cmd --in-delim $tab !{input,--file,project_custom_var_annot_file} --exclude-row 1,1 | cut -f$custom_id_col | sort | uniq -d | sed '1 s/^/ID\n/'" !{input,--file,project_custom_var_annot_file} --extra 2 --multiple 2 > !{output,,project_variant_subset_custom_var_annot_file} class_level project_variant_subset run_if project_custom_var_annot_file

!|expand:,:shortt,projectt,projecte,runif:,project,,run_if !num_var_subsets:short,project_variant_subset,_project_variant_subset,| \
shortt cmd make_projectt_custom_trans_annot_file=$smart_join_cmd --in-delim $tab --exec "cut -f$vep_id_col !{input,,projectt_parsed_vep_file} | tail -n+2 | sort -u | $smart_cut_cmd --exec 'cut -f$custom_id_col !{input,,projectt_custom_var_annot_file} | tail -n+2 | sort -u' | sort | uniq -d | sed '1 s/^/ID\n/'" --exec "cut -f$vep_id_col,$vep_trans_col !{input,,projectt_parsed_vep_file}" !{input,--file,projectt_custom_var_annot_file} --header 1 --rest-extra 1 --multiple 2 > !{output,,projectt_custom_trans_annot_file} class_level projectt runif skip_if or,project_custom_trans_annot_file,!project_custom_var_annot

#1: File to fill
#2: File to fill
#3: Ids to use
#4: What to fill
#5: ID col in file to fill
#6: ID col in other file to use
#7: Number of ID cols
#8: Number of header rows
#9: Delim to use to split other file
fill_file_helper_int=$smart_join_cmd --in-delim $tab --header @8 --merge @1 `echo @6 | sed 's/\(\s\s*\)/\n/g' | awk '{print NR}' | sed 's/^\(\S\S*\)/--col 2,\1/g' | tr '\n' ' '` `echo @5 | sed 's/\(\S\S*\)/--col 1,\1/g'` --exec \"cat @3 | $smart_cut_cmd --in-delim $tab --select-col 0,'@6' | perl -lpe 'BEGIN {\\\\$ncol = \\\`@2 | head -n1\\\`; die if \\\\$?; \\\\$ncol = scalar(split(\\\"@9\\\", \\\\$ncol))} chomp; \\\\$l = join(\\\"\\\\t\\\", split(/:/, \\\"@{4}:\\\"x(\\\\$ncol-@7))); s/$/\t\\\\$l/'\"


fill_file_helper=$fill_file_helper_int(!{input:--file:@1},cat !{input::@1},!{input::@2},@3,@4,@5,@6,@7,\t)

#the following annotations are used internally, and are pulled from the VEP file
vep_id_annot=Uploaded_variation
vep_trans_annot=Feature
vep_ccds_annot=CCDS
vep_type_annot=Consequence
vep_loc_annot=Location
vep_protein_change_annot=Protein_change
vep_codon_change_annot=Codons
vep_type_synonymous_annot=synonymous_variant
vep_type_missense_annot=missense_variant
vep_type_nonsense_annot=stop_gained
vep_type_readthrough_annot=stop_lost

synonymous_mask=$vep_type_annot,eq:$vep_type_synonymous_annot
missense_mask=$vep_type_annot,eq:$vep_type_missense_annot
nonsense_mask=$vep_type_annot,'eq:$vep_type_nonsense_annot eq:$vep_type_readthrough_annot'
noncoding_mask=$vep_protein_change_annot,eq:$vep_missing_field
coding_mask=$vep_protein_change_annot,ne:$vep_missing_field

vep_consequence_annot=Consequence
vep_consequence_rank=transcript_ablation splice_donor_variant splice_acceptor_variant stop_gained frameshift_variant stop_lost initiator_codon_variant inframe_insertion inframe_deletion missense_variant transcript_amplification splice_region_variant incomplete_terminal_codon_variant synonymous_variant stop_retained_variant coding_sequence_variant mature_miRNA_variant 5_prime_UTR_variant 3_prime_UTR_variant intron_variant NMD_transcript_variant non_coding_exon_variant nc_transcript_variant upstream_gene_variant downstream_gene_variant TFBS_ablation TFBS_amplification TF_binding_site_variant regulatory_region_variant regulatory_region_ablation regulatory_region_amplification feature_elongation feature_truncation intergenic_variant 

vep_id_col=1
vep_trans_col=5
vep_gene_col=4
vep_ccds_col=14

snpeff_id_col=24
snpeff_trans_col=13

chaos_id_col=1
chaos_trans_col=3

pph2_id_col=5
pph2_trans_col=6

custom_id_col=1
custom_trans_col=2

snpeff_missing_field=
chaos_missing_field=.
pph2_missing_field=
vep_missing_field=-
annot_missing_field=NA

replace_missing_helper=perl -F\\\\t -lne '\@F = split(\"\t\", \\$_, -1); \@F = map {\\$_ eq \"@1\" ? \"$annot_missing_field\" : \\$_} \@F; print join(\"\t\", \@F)' 

adjust_missing_int=sed 's/\(\s\s*\|^\)$vep_missing_field\(\s\s*\|$\)/\1$annot_missing_field\2/g'
adjust_missing=$adjust_missing_int | $adjust_missing_int

snpeff_tag=SNPEFF
chaos_tag=CHAOS
vep_tag=VEP

snpeff_effect=SNPEFF_Effect
chaos_effect=CHAOS_TYPE
vep_effect=Consequence

!%expand:;:cmdtype;extracmd;skipif:\
no_custom;;skip_if or,project_custom_var_annot_file,project_custom_trans_annot_file:\
with_custom;--exec "$fill_file_helper(projectt_custom_trans_annot_file,projectt_parsed_vep_file,-,$custom_id_col $custom_trans_col,$vep_id_col $vep_trans_col,2,1) | sed '1 s/\(\s\s*\)\(\S\S*\)/\1CUSTOM_\2/g'" --col 6,1 --col 6,2;skip_if and,!project_custom_var_annot_file,!project_custom_trans_annot_file%\ 
!|expand:,:shortt,projectt,runif:,project,run_if !num_var_subsets:short,project_variant_subset,| \
shortt cmd make_projectt_cmdtype_complete_full_annot_file=$smart_join_cmd --in-delim $tab --header 1 --rest-extra 1 !{input,--file,projectt_parsed_vep_file} --col 1,$vep_id_col --col 1,$vep_trans_col \
  --exec "$fill_file_helper(projectt_snpeff_file,projectt_parsed_vep_file,$vep_missing_field,$snpeff_id_col $snpeff_trans_col,$vep_id_col $vep_trans_col,2,1) | sed '1 s/\(\s\s*\)\(\S\S*\)/\1SNPEFF_\2/g' | $replace_missing_helper($snpeff_missing_field) | perl -lne 'chomp; @a = split(\"\t\"); if ($. == 1) {\\$effect_col=undef; for (\\$i=0;\\$i<=\\$\\\#a;\\$i++) {if (\\$a[\\$i] eq \"$snpeff_effect\") {\\$effect_col=\\$i; last}} die unless defined \\$effect_col} else {\\$a[\\$effect_col] =~ s/:[^\t]*//} print join(\"\t\", @a)'"  --multiple 2 --col 2,1 --col 2,2 \
  --exec "$fill_file_helper(projectt_chaos_file,projectt_parsed_vep_file,$vep_missing_field,$chaos_id_col $chaos_trans_col,$vep_id_col $vep_trans_col,2,1) | sed '1 s/\(\s\s*\)\(\S\S*\)/\1CHAOS_\2/g' | $replace_missing_helper($chaos_missing_field)"  --col 3,1 --col 3,2 \
  --exec "$smart_join_cmd --header 1 --in-delim $tab --exec 'cut -f$vep_id_col,$vep_trans_col !{input,,projectt_parsed_vep_file}' !{input,--file,projectt_snpsift_file} --multiple 1 | sed '1 s/\(\s\s*\)\(\S\S*\)/\1SNPEFF_\2/g' | $replace_missing_helper($snpeff_missing_field)" --col 4,1 --col 4,2 \
  --exec "$fill_file_helper(projectt_pph2_file,projectt_parsed_vep_file,$vep_missing_field,$pph2_id_col $pph2_trans_col,$vep_id_col $vep_trans_col,2,1) | sed '1 s/\(\s\s*\)\(\S\S*\)/\1PPH2_\2/g' | $replace_missing_helper($pph2_missing_field)" --col 5,1 --col 5,2 \
  extracmd | $adjust_missing !{raw::projectt:| $map_effects_cmd *project_effect_size_map_file $vep_tag,$vep_effect $chaos_tag,$chaos_effect $snpeff_tag,$snpeff_effect:if_prop=project_effect_size_map_file:allow_empty=1} !{input,project_effect_size_map_file,if_prop=project_effect_size_map_file,allow_empty=1} > !{output,,projectt_complete_full_annot_file} class_level projectt runif skipif

!|expand:,:shortt,projectt,runif:,project,run_if !num_var_subsets:short,project_variant_subset,| \
shortt cmd make_projectt_cmdtype_full_annot_file=cat !{input,,projectt_complete_full_annot_file} | $smart_cut_cmd --in-delim $tab --exec "!{raw::projectt:cut -f$vep_trans_col *projectt_vep_file | sort -u:unless_prop=project_reduced_trans_file:allow_empty=1} !{raw::projectt:sort -u *project_reduced_trans_file:if_prop=project_reduced_trans_file:allow_empty=1} !{input:project_reduced_trans_file:if_prop=project_reduced_trans_file:allow_empty=1} " | awk -F"\t" -v OFS="\t" 'NR == 1 {h=0} NF == 1 {m[\$1]=1} NF > 1 && (!h || m[\$2]) {print; h=1}' > !{output,,projectt_full_annot_file} class_level projectt runif

var_transcript_col_select_helper=--select-col 1,1 --select-col 1,1,^$vep_consequence_annot$ --select-col 1,1,.

!|expand:,:shortt,projectt,projecte,runif:,project,,run_if !num_var_subsets:short,project_variant_subset,_project_variant_subset,| \
shortt cmd make_projectt_var_transcript_file=$smart_join_cmd --in-delim $tab --header 1 --rest-extra 1 --multiple 3 --exec "echo $vep_consequence_rank | sed 's/\s\s*/\n/g' | sort -u | $smart_cut_cmd --in-delim $tab --exec \"$smart_cut_cmd --in-delim $tab !{input,--file,projectt_full_annot_file} --select-col 1,1,$vep_consequence_annot --exclude-row 1,1 --select-row 1,1,$vep_ccds_annot,ne:$annot_missing_field --exact --require-col-match | sed 's/,/\n/g' | sort -u\" | sort | uniq -d | sed '1 s/^/ID\n/'" --exec "echo $vep_consequence_rank | sed 's/\s\s*/\n/g' | sed '1 s/^/ID\n/' | awk -v OFS=\"\t\" '{print \\$0,NR}'" --exec "$smart_cut_cmd --in-delim $tab !{input,--file,projectt_full_annot_file} $var_transcript_col_select_helper --select-row 1,1 --select-row 1,1,$vep_ccds_annot,ne:$annot_missing_field --require-col-match | awk -F\"\t\" -v OFS=\"\t\" 'NR == 1 {print 0,\\$0} NR > 1 {split(\\$2,a,\",\"); for(i=1;i<=length(a);i++){print a[i],\\$0}}' | awk -F\"\t\" -v OFS=\"\t\" '{n=0; for(i=3;i<=NF;i++) {if (\\$i == \"$annot_missing_field\") {n++}}} NR == 1 {\\$1=\\$1\"\tFIELDS_MISSING\"} NR > 1 {\\$1=\\$1\"\t\"n} {print}'" | sort -k2,2n -k3,3n | cut -f4- | $smart_cut_cmd --in-delim $tab --select-col 0,1,'$vep_id_annot $vep_trans_annot' --exact --require-col-match | $smart_cut_cmd --in-delim $tab --stdin-first --exec "$smart_cut_cmd --in-delim $tab !{input,--file,projectt_full_annot_file} --select-row 1,1,$vep_ccds_annot,ne:$annot_missing_field --exclude-row 1,1 --select-col 1,1,'$vep_id_annot $vep_trans_annot' --exact --require-col-match" --exec "$smart_cut_cmd --in-delim $tab !{input,--file,projectt_full_annot_file} --select-row 1,1,$vep_ccds_annot,eq:$annot_missing_field --exclude-row 1,1 --select-col 1,1,'$vep_id_annot $vep_trans_annot' --exact --require-col-match"  | awk -F"\t" -v OFS="\t" 'NR == 1 {print 0,NR,\$0} NR > 1 {print 1,NR,\$0}' | sort -k1,1n -k3,3 -k2,2n | cut -f3- | rev | uniq -f1 | rev | $adjust_missing > !{output,,projectt_var_transcript_file} class_level projectt runif

#!!expand:projectt:project:project_variant_subset! \
#projectt_full_var_annot_exec_helper=--exec "cat !{input,,projectt_per_gene_vep_file} | awk -F\"\t\" -v OFS=\"\t\" 'NR == 1 {print 0,\\$0} NR > 1 { if (\\$$vep_gene_col != \"$vep_missing_field\" && \\$$vep_ccds_col != \"$vep_missing_field\") {print 1,\\$0} else if (\\$$vep_gene_col != \"$vep_missing_field\") {print 2,\\$0} else {print 3,\\$0}}' | sort -t$tab -k1,1n -k2,2 | cut -f2- | awk -v OFS=\"\t\" -F\"\t\" '{k=\\$$vep_id_col} !m[k] {m[k]=1; print}' | sed 's/\(\s\s*\)$vep_missing_field/\1$annot_missing_field/g' | cut -f$vep_id_col,$vep_trans_col"

!!expand:projectt:project:project_variant_subset! \
projectt_full_var_annot_helper=$smart_join_cmd --in-delim $tab --header 1 !{input,--file,projectt_var_transcript_file} !{input,--file,projectt_full_annot_file} --extra 2 --col 1 --col 2

#!|expand:,:shortt,projectt,projecte,runif:,project,,run_if !num_var_subsets:short,project_variant_subset,_project_variant_subset,| \
#shortt cmd make_projectt_parsed_per_gene_vep_file=$smart_join_cmd --in-delim $tab --header 1 !{input,--file,projectt_var_transcript_file} --exec "sed '1! s/$vep_missing_field/$annot_missing_field/g' !{input,,projectt_parsed_vep_file}" --extra 2 --col 1,1 --col 1,2 --col 2,$vep_id_col --col 2,$vep_trans_col > !{output,,projectt_parsed_per_gene_vep_file} class_level projectt runif 

!|expand:,:shortt,projectt,runif:,project,run_if !num_var_subsets:short,project_variant_subset,| \
shortt cmd make_projectt_full_var_annot_file=$projectt_full_var_annot_helper > !{output,,projectt_full_var_annot_file} class_level projectt runif

#!|expand:,:projectt:project:project_variant_subset| \
#short cmd make_projectt_annot_pseq_file=$smart_cut_cmd --in-delim $tab !{input,--file,projectt_parsed_vep_file} --select-col 1,1 --select-col 1,1,'$vep_type_annot $vep_protein_change_annot' --exact --require-col-match | awk -F"\t" -v OFS="\t" '{print \$1,"$pseq_type_annot",\$2} {print \$1,"$pseq_protein_change_annot",\$3}' | awk -F"\t" '\$3 != $annot_missing_field' | sed '1 s/^/\\#\\#$pseq_type_annot,1,String,"Type of variant"\n\\#\\#$pseq_protein_change_annot,1,String,"Protein change"\n/' > !{output,,projectt_annot_pseq_file} class_level projectt

!!expand;,;maskkey,maskdescrip;syn,$synonymous_mask;ns,$missense_mask;nonsense,$nonsense_mask;coding,$coding_mask! \
!|expand:,:projectt,runif:project,run_if !num_var_subsets:project_variant_subset,| \
short cmd make_projectt_plinkseq_maskkey_reg_file=$smart_join_cmd --in-delim $tab --extra 2 --exec "$smart_join_cmd --in-delim $tab --header 1 --exec \"sed '1! s/$annot_missing_field$/$vep_missing_field/' !{input,,projectt_var_transcript_file}\" !{input,--file,projectt_parsed_vep_file} --extra 2 --col 1,1 --col 1,2 --col 2,$vep_id_col --col 2,$vep_trans_col | $smart_cut_cmd --in-delim $tab --select-col 0,1 --select-row 0,1,maskdescrip --exclude-row 0,1" --exec "$smart_cut_cmd --in-delim $tab --exec \"$zcat_helper(projectt) !{input,,projectt_vcf_file}\" --exec \"$zcat_helper(projectt) !{input,,projectt_indel_vcf_file}\" --exec \"$zcat_helper(projectt) !{input,,projectt_multiallelic_vcf_file}\" --comment '\\\#\\\#' --strip-comments --select-col 1-3,1,'ID \\\#CHROM POS' --exact --require-col-match --exclude-row 2,1 --exclude-row 1,1 --exclude-row 3,1" | cut -f2- | sed 's/\s\s*/:/' | sed 's/^/chr/' | sed 's/^chrchr/chr/' > !{output,,projectt_plinkseq_maskkey_reg_file} class_level projectt runif

#coverage cmds

#helper constants
coverage_dat_delim=,
threshold=10
threshold_frac_above=frac_above_
threshold_frac_lte=frac_lte_
threshold_frac_equal=frac_equal_
threshold_num_above=num_above_
threshold_num_lte=num_lte_
threshold_num_equal=num_equal_
frac_above_threshold=${threshold_frac_above}${threshold}
table_sum_stats_stddev=stddev
table_sum_stats_min=min
table_sum_stats_num=num
table_sum_stats_tot=tot
table_sum_stats_quantile=quant_

summary_tot=@{1}_tot
summary_mean=@{1}_mean
summary_max=@{1}_max
summary_min=@{1}_min
summary_dev=@{1}_$table_sum_stats_stddev
summary_num=@{1}_num
num_cum_bins=1000

haplotype_burden_bin_dir=$targeted_bin_dir/vineeta
recessive_bin_dir=$targeted_bin_dir/recessive/v2

conditional_exec_cmd=perl $common_bin_dir/conditional_exec.pl
smart_join_cmd=perl $common_bin_dir/smart_join.pl
smart_cut_cmd=perl $common_bin_dir/smart_cut.pl
bin_values_cmd=perl $common_bin_dir/bin_values.pl
add_function_cmd=perl $common_bin_dir/add_function.pl
add_header_cmd=perl $common_bin_dir/add_header.pl
transpose_cmd=perl $common_bin_dir/transpose.pl
table_to_beamer_cmd=perl $common_bin_dir/table_to_beamer.pl
text_to_beamer_cmd=perl $common_bin_dir/text_to_beamer.pl
format_columns_cmd=perl $common_bin_dir/format_columns.pl
vcf_utils_cmd=perl $targeted_bin_dir/vcf_utils.pl
sync_ref_alt_cmd=perl $targeted_bin_dir/sync_ref_alt.pl
tped_to_bed_cmd=perl $targeted_bin_dir/tped_to_bed.pl

table_sum_stats_cmd=perl $common_bin_dir/table_sum_stats.pl
table_sum_stats_threshold_cmd=$table_sum_stats_cmd --out-delim , --threshold $threshold

add_gene_annot_cmd=python $targeted_bin_dir/add_gene_annot.py
make_cum_dat_cmd=python $common_bin_dir/make_cum_dat.py

r_script_cmd=$r_cmd -f @1 --slave --vanilla --args
r_212_script_cmd=$r_212_cmd -f @1 --slave --vanilla --args
r_215_script_cmd=$r_215_cmd -f @1 --slave --vanilla --args


draw_bar_plot_cmd=$r_script_cmd($common_bin_dir/draw_bar_plot.R) @1
#args: input, output, title
draw_coverage_bar_plot_cmd=$draw_bar_plot_cmd("@1 @2 @3 '% bases >= ${threshold}x' $frac_above_threshold 2 sep=$coverage_dat_delim")

draw_gene_plot_cmd=$r_script_cmd($targeted_bin_dir/draw_gene_plot.R) @1
draw_fancy_locus_plot_cmd=$r_script_cmd($targeted_bin_dir/draw_fancy_locus_plot.R)
draw_var_coverage_plot_cmd=$r_script_cmd($targeted_bin_dir/draw_var_coverage_plot.R)
draw_matrix_plot_cmd=$r_script_cmd($common_bin_dir/draw_matrix_plot.R)
draw_box_plot_cmd=$r_script_cmd($common_bin_dir/draw_box_plot.R) @1
draw_hist_plot_cmd=$r_script_cmd($common_bin_dir/draw_hist_plot.R) @1
draw_qq_plot_cmd=$r_script_cmd($common_bin_dir/draw_qq_plot.R)
compute_lambda_cmd=$r_script_cmd($common_bin_dir/compute_lambda.R)
predict_log_reg_cmd=$r_script_cmd($common_bin_dir/predict_log_reg.R) @1
threshold_vassoc_cmd=$r_script_cmd($targeted_bin_dir/threshold_vassoc.R) @1
score_test_cmd=$r_script_cmd($targeted_bin_dir/Score_test.R)
multiallelic_qc_cmd=$r_215_script_cmd($targeted_bin_dir/multiallelic_QC_metrics.R)


write_outlier_table_cmd=$r_script_cmd($common_bin_dir/write_outlier_table.R)

#args: input, output, title
draw_coverage_box_plot_cmd=$draw_box_plot_cmd("@1 @2 @3 '' '% bases >= ${threshold}x' $frac_above_threshold label=2 sep=$coverage_dat_delim")

draw_line_plot_cmd=$r_script_cmd($common_bin_dir/draw_line_plot.R)

prepend_coverage_dat_label=sed 's/^/@1$coverage_dat_delim/'
#prepend_call_set_coverage_dat_label=$prepend_coverage_dat_label(call_set)
#prepend_call_set_disp_coverage_dat_label=$prepend_coverage_dat_label(!{prop\,\,call_set\,disp})

# actual cmds

prop id_cols=list
picard_prepend_helper=$smart_cut_cmd !{input,--file,@1} --select-col 1,1,'!{prop,,seq_batch,id_cols,sep=:}' --exact --vec-delim : --in-delim $tab | sed 's/\t/:/' | sed '1 s/.*/ID/' | paste - !{input,,@1} | $smart_cut_cmd --exclude-col '0,1,!{prop,,seq_batch,id_cols,sep=:}' --exact --vec-delim : --in-delim $tab

local cmd make_seq_batch_picard_sort_file=rm -f !{output,,seq_batch_picard_sort_file} && tail -n+2 !{input,,seq_batch_picard_map_file} | cut -f2 | sort -u | awk -v OFS="\t" 'NR == 1 {print "ID","Order"} {print \$1,NR}' > !{output,,seq_batch_picard_sort_file} class_level seq_batch skip_if seq_batch_picard_sort_file

prop project_picard_file_sample_col=scalar
local cmd make_seq_batch_picard_file=$smart_join_cmd !{input,--file,seq_batch_picard_sort_file} --exec "$smart_join_cmd --exec \"$picard_prepend_helper(seq_batch_picard_map_file)\" --exec \"$picard_prepend_helper(project_picard_file)\" --in-delim $tab --extra 1 --header 1 --multiple 2" --header 1 --multiple 2 --extra 1 --in-delim $tab --col 2,!{prop,,project,project_picard_file_sample_col,missing=2} | perl $common_bin_dir/uncommify.pl --in-delim $tab > !{output,,seq_batch_picard_file} class_level seq_batch

prop picard_cols=list
local cmd make_seq_batch_picard_pdf_file=$draw_box_plot_cmd(!{input\,\,seq_batch_picard_file} !{output\,\,seq_batch_picard_pdf_file} 'Picard Metrics by !{prop\,\,seq_batch\,disp}' '' 'Sample Values' '!{prop\,\,seq_batch\,picard_cols\,sep=\\,}' label=1 order=2 sep=$tab comments=FALSE) class_level seq_batch


prop failed=scalar default 0
local cmd make_sample_bam_xml_file=perl $targeted_bin_dir/write_igv_xml.pl !{input,--bam,sample_bam_file} --hg-build $hg_build > !{output,,sample_bam_xml_file} class_level sample skip_if failed

short cmd make_gatk_sample_coverage_file=$gatk_cmd_no_interval(DepthOfCoverage) !{input,-L,project_expanded_interval_list_file} -l OFF -o /dev/stdout -omitSampleSummary -omitSampleSummary -omitIntervals -omitLocusTable !{input,-I,sample_bam_file} | gzip - > !{output,,sample_coverage_file} class_level sample skip_if or,failed,sample_coverage_file,no_coverage run_if use_gatk_doc

!!expand:projectt_sample_subset:project_sample_subset:pheno_sample_subset! \
short cmd make_projectt_sample_subset_bam_list_file=$smart_join_cmd --in-delim $tab --exec "cut -f1 !{input,,project_sample_bam_list_file} | sort -u | cat - !{input,,project_sample_subset_samp_keep_file} | sort | uniq -d" !{input,--file,project_sample_subset_samp_keep_file} !{input,--file,project_sample_bam_list_file} --rest-extra 1 | cut -f2 > !{output,,projectt_sample_subset_bam_list_file} class_level projectt_sample_subset

prop compute_coverage=scalar

!!expand:project_sample_subset:project_sample_subset:pheno_sample_subset! \
short cmd make_project_sample_subset_coverage_file=$gatk_cmd_no_interval(DepthOfCoverage) !{input,-L,project_expanded_interval_list_file} -l OFF -o /dev/stdout -omitSampleSummary -omitIntervals -omitLocusTable !{input,-I,project_sample_subset_bam_list_file} | gzip -c > !{output,,project_sample_subset_coverage_file} class_level project_sample_subset run_if compute_coverage

sort_scratch_dir=$tmp_dir/sort

prop use_gatk_doc=scalar default 1

short cmd make_sample_coverage_file=mkdir -p $sort_scratch_dir && $samtools_cmd pileup !{input,,sample_bam_file} 2>/dev/null | sed 's/\s\s*/:/' | $smart_cut_cmd --select-col 0,'1 3' | $add_gene_annot_cmd --locus-col 1 --gene-file !{input,,project_expanded_interval_list_file} --gene-file-num-ids 0 --gene-file-comment @ | $smart_cut_cmd --exec "grep -v @ !{input,,project_expanded_interval_list_file} | awk '{for (i=\\$2;i<=\\$3;i++) {print \\$1\":\"i,0}}'" --out-delim $tab | sort -S 2G -T $sort_scratch_dir -k1,1 -k2,2nr | awk '!last || last!=\$1 {print} {last=\$1}' | sed '1 s/^/Locus\tTotal_Depth\n/' | gzip - > !{output,,sample_coverage_file} class_level sample skip_if or,failed,sample_coverage_file,no_coverage,use_gatk_doc

coverage_stats_helper=zcat !{input,,sample_coverage_file} | tail -n+2 | $add_gene_annot_cmd --gene-file-num-ids 2 --print-multiple --gene-id-join-char ' ' --outside-name $outside_gene_name --locus-col 1 !{input,--gene-file,project_expanded_exon_target_file,if_prop=expand_coverage_targets,allow_empty=1} !{input,--gene-file,project_exon_target_file,unless_prop=expand_coverage_targets,allow_empty=1} --print-multiple

short cmd make_sample_coverage_stats_file=$coverage_stats_helper | $table_sum_stats_threshold_cmd !{prop,--label,sample} --col 4 > !{output,,sample_coverage_stats_file} class_level sample skip_if or,failed,sample_coverage_stats_file

prop skip_exon_coverage=scalar

!!expand:;:type;extraprocess;group_col;val_col;extrarunif:\
gene;;1;4;:\
exon;;1 --group-col 2;4;,skip_exon_coverage! \
short cmd make_sample_type_coverage_stats_file=$coverage_stats_helper(project_type_target_file) extraprocess | $table_sum_stats_threshold_cmd !{prop,--label,sample} --col val_col --group-col group_col > !{output,,sample_type_coverage_stats_file} class_level sample skip_if or,failed,sample_type_coverage_stats_fileextrarunif

short cmd make_sample_bait_coverage_stats_file=$coverage_stats_helper(project_bait_target_file) | cut -f3- | $add_gene_annot_cmd --gene-file-num-ids 1 --print-multiple --gene-id-join-char ' ' --outside-name $outside_gene_name --locus-col 1 !{input,--gene-file,project_bait_target_file} --print-multiple | $table_sum_stats_threshold_cmd !{prop,--label,sample} --col 3 --group-col 1 > !{output,,sample_bait_coverage_stats_file} class_level sample skip_if or,failed,sample_bait_coverage_stats_file,skip_exon_coverage,!project_bait_target_file


prop whole_exome=scalar default 1
local cmd make_sample_gene_coverage_pdf_file=$draw_box_plot_cmd(!{input\,\,sample_gene_coverage_stats_file} !{output\,\,sample_gene_coverage_pdf_file} 'Coverage across genes' '' 'Coverage' 4 header=FALSE label=1 sep=$coverage_dat_delim) class_level sample skip_if or,failed,whole_exome


cmd meta_table make_project_sample_coverage_stats_file_list=!{input,,sample_coverage_stats_file,unless_prop=failed} !{output,project_sample_coverage_stats_file_list} class_level project

#local cmd make_call_set_sample_coverage_dat_file=$table_sum_stats_threshold_cmd --label sample --col 2 --only-print-header | $prepend_call_set_coverage_dat_label > !{output,,call_set_sample_coverage_dat_file} && xargs cat < !{input,,call_set_sample_coverage_stats_file_list} !{input,sample_coverage_stats_file,unless_prop=failed} | $prepend_call_set_disp_coverage_dat_label >> !{output,,call_set_sample_coverage_dat_file} class_level call_set skip_if failed

#cmd meta_table make_call_set_sample_gene_coverage_stats_file_list=!{input,,sample_gene_coverage_stats_file,unless_prop=failed} !{output,call_set_sample_gene_coverage_stats_file_list} class_level call_set skip_if failed

#local cmd make_call_set_gene_dist_coverage_dat_file=$table_sum_stats_threshold_cmd --label sample --col 4 --only-print-header --group-col 1 | $prepend_call_set_coverage_dat_label > !{output,,call_set_gene_dist_coverage_dat_file} && xargs cat < !{input,,call_set_sample_gene_coverage_stats_file_list} !{input,sample_gene_coverage_stats_file,unless_prop=failed} | $prepend_call_set_disp_coverage_dat_label >> !{output,,call_set_gene_dist_coverage_dat_file} class_level call_set skip_if failed

"""
}
    