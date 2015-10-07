
package loamstream.pipeline.examples

/**
 * LoamStream
 * Created by oliverr on 10/6/2015.
 */
object PipelineTargetedPart12 {
  val string =
 """
short cmd make_project_pos_transcript_file=cat !{input,,project_variant_site_vcf_file} | $pos_transcript_helper > !{output,,project_pos_transcript_file} class_level project skip_if num_merge_subsets

short cmd make_project_merge_subset_pos_transcript_file=$combine_vcfs_for_annotation(project_merge_subset,) | $pos_transcript_helper > !{output,,project_merge_subset_pos_transcript_file} class_level project_merge_subset

local cmd cat_project_pos_transcript_file=cat !{input,,project_merge_subset_pos_transcript_file} > !{output,,project_pos_transcript_file} class_level project run_if num_merge_subsets

short cmd make_project_transcript_target_file=cat !{input,,project_pos_transcript_file} | sort -k3,3 -k1,1 -k2,2n | fgrep -v '$vep_missing_field' | awk -F"\t" -v OFS="\t" '{ct=\$3"."\$1} t && ct != t {print t,c,s,e} ct != t || !t {s=\$2} {t=ct; e=\$2; c=\$1} END {print t,c,s,e}' > !{output,,project_transcript_target_file} class_level project

local cmd make_project_disjoint_gene_target_file=cat !{input,,project_gene_target_file} !{input,,project_transcript_target_file} | sort -k2,2 -k3,3n | awk -v OFS="\t" -F"\t" 'NR == 1 {m=1} NR == 1 || \$2 != c || \$3 > e {g="GeneGroup"m++; } {\$1=g} {print} NR == 1 || \$2 != c || \$4 > e {c=\$2; s=\$3; e=\$4}' | uniq > !{output,,project_disjoint_gene_target_file} class_level project

local cmd make_project_closest_gene_file=cat !{input,,project_gene_target_file} | sort -k2,2 -k3,3n  | awk -F"\t" -v OFS="\t" 'BEGIN {f=3000000000} (NR == 1 || (c && \$2 != c)) {if (g && c && l + 1 < f) {print g,c,l+1,f} g=\$1;c=\$2;l=0} g {m=int((l + \$3)/2); if (m > l) {print g,c,l+1,m} if (m + 1 < \$3 - 1) {print \$1,\$2,m+1,\$3-1}} {print} !g || \$4 > l {g=\$1;c=\$2;l=\$4;} END {if (l + 1 < f) {print g,c,l+1,f}}' > !{output,,project_closest_gene_file} class_level project

#perl $targeted_bin_dir/exon_to_gene_target.pl < !{input,,project_expanded_exon_target_file} > !{output,,project_gene_target_file} class_level project

local cmd make_project_genes_gtf_file=awk -v OFS="\t" '{print \$3,"$hg_refflat","CDS",\$4,\$5,"0.000000","+","0","gene_id \""\$1"\"; transcript_id \""\$1"\";"}' < !{input,,project_expanded_exon_target_file} > !{output,,project_genes_gtf_file} class_level project

prop validate_gtf=scalar default 1

interval_split_helper=perl -lne 'BEGIN {open IN, "!{input,,project_variant_subset_var_keep_file}"; while (<IN>) {\@a = split; \$min{\$a[0]} = \$a[1] unless defined \$min{\$a[0]} && \$min{\$a[0]} < \$a[1]; \$max{\$a[0]} = \$a[1] unless defined \$max{\$a[0]} && \$max{\$a[0]} > \$a[1];  }} chomp; \@a = split("\t"); if (\$min{\$a[@1]}) {\$f = \$min{\$a[@1]} <= \$a[@2] && \$max{\$a[@1]} >= \$a[@2]; \$g = \$min{\$a[@1]} <= \$a[@3] && \$max{\$a[@1]} >= \$a[@3]; if (\$f || \$g) {print join("\t",\@a); unless (\$f && \$g) {!{raw,,project,die "Error: a line in the gtf (\$_) file partially overlapped this region",if_prop=validate_gtf,allow_empty=1}}}}'

local cmd make_project_variant_subset_genes_gtf_file=$interval_split_helper(0,3,4) !{input,,project_genes_gtf_file} > !{output,,project_variant_subset_genes_gtf_file} class_level project_variant_subset

short cmd make_project_variant_subset_region_file=$interval_split_helper(1,2,3) !{input,,project_disjoint_gene_target_file} > !{output,,project_variant_subset_region_file} class_level project_variant_subset

#calls cmds
prop sample_vcf_id=scalar
meta_table cmd make_project_sample_vcf_id_to_sample_id_file=!{prop,,sample,sample_vcf_id,if_prop=sample_vcf_id,allow_empty=1}\t!{prop,,sample,if_prop=sample_vcf_id,allow_empty=1} !{output,project_sample_vcf_id_to_sample_id_file} class_level project skip_if no_sample_tracking

local cmd make_empty_project_sample_vcf_id_to_sample_id_file=rm -f !{output,,project_sample_vcf_id_to_sample_id_file} && touch !{output,,project_sample_vcf_id_to_sample_id_file} class_level project run_if no_sample_tracking

prop sample_call_set=scalar
meta_table cmd make_project_sample_to_call_set_file=!{prop,,sample,if_prop=sample_call_set,allow_empty=1}\t!{prop,,sample,sample_call_set,if_prop=sample_call_set,allow_empty=1} !{output,project_sample_to_call_set_file} class_level project skip_if no_sample_tracking

local cmd make_empty_project_sample_to_call_set_file=rm -f !{output,,project_sample_to_call_set_file} && touch !{output,,project_sample_to_call_set_file} class_level project run_if no_sample_tracking


meta_table cmd make_project_sample_bam_list_file=!{prop,,sample}\t!{input,,sample_bam_file,unless_prop=failed} !{output,project_sample_bam_list_file} class_level project skip_if no_sample_tracking
local cmd make_project_bam_list_file=cut -f2 !{input,,project_sample_bam_list_file} > !{output,,project_bam_list_file} class_level project

#remove samples who are excluded from this call_set
local cmd make_call_set_sample_vcf_id_to_sample_id_file=$smart_join_cmd --in-delim $tab --exec "awk '\\$2 != \"!{prop,,call_set}\" {print \\$1\"\n\"\\$1}' !{input,,project_sample_to_call_set_file} | $smart_cut_cmd --in-delim $tab --exec 'cut -f2 !{input,,project_sample_vcf_id_to_sample_id_file} | sort -u' | sort | uniq -u" !{input,--file,project_sample_vcf_id_to_sample_id_file} --col 2,2 --extra 2 | awk -F"\t" -v OFS="\t" '{print \$2,\$1}' > !{output,,call_set_sample_vcf_id_to_sample_id_file} class_level call_set 

zcat_helper=!{raw,,@1,z,if_prop=zip_vcf:eq:.gz,allow_empty=1}cat
bgzip_helper=!{raw,,@1,| $bgzip_cmd,if_prop=zip_vcf:eq:.gz,allow_empty=1}
run_tabix_cmd=$tabix_cmd -f -p vcf *@1
tabix_helper_int=!{raw,,@1,&& $run_tabix_cmd(@2),if_prop=zip_vcf:eq:.gz,allow_empty=1@3}
tabix_helper=$tabix_helper_int(@1,@2,)
tabix_helper_if=$tabix_helper_int(@1,@2,\,@3)
ln_tabix_helper=!{raw,,@1,&& ln *@2.tbi *@3.tbi,if_prop=zip_vcf:eq:.gz,allow_empty=1}

filter_call_set_bim_file=awk -v OFS="\t" '\$1 !~ /\#/ && \$1 > 0 && \$5 ~ /^[ATCG]+$/ && \$6 ~ /^[ATCG]+$/ {k=\$1":"\$4; if (!m[k]) {print @1; m[k]=1}}'

local cmd make_call_set_ref_alt_file=(echo '\#\#fileformat=VCFv4.1' && echo '\#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO' && $filter_call_set_bim_file() !{input,,call_set_bim_file} | awk -v OFS="\t" '{print \$1,\$4,\$2,\$6,\$5,".",".","."}') > !{output,,call_set_ref_alt_file} class_level call_set run_if call_set_bim_file 

mv_cmd_helper=mv @1 @2

local cmd make_call_set_keep_file=$filter_call_set_bim_file(\$2) !{input,,call_set_bim_file} > !{output,,call_set_keep_file} class_level call_set run_if call_set_bim_file



local cmd make_call_set_subset_bim_keep_file=sort -k1,1n -k4,4n !{input,,call_set_bim_file} | $split_call_set_file_helper(!{input::call_set_bim_file}) | awk '{print \$2}' > !{output,,call_set_subset_bim_keep_file} class_level call_set_subset

short cmd make_call_set_subset_plink_files=$plink_cmd $plink_in_bed_helper(call_set) !{input,--extract,call_set_subset_bim_keep_file} --make-bed $plink_out_bed_helper(call_set_subset) class_level call_set_subset

process_call_set_plink_input=$sort_vcf_file | $vcf_utils_cmd --add-string-format GT,"Genotype" --valid-ref-alt !{input,--reference-vcf,call_set_ref_alt_file} !{input,--var-keep,call_set_keep_file} !{input,--chr-remap,project_chr_map_file} --remove-repeated-samples

sort_vcf_file=awk -F"\t" -v OFS="\t" '{if (/^\#/) {print 0,NR,\$0} else {print 1,1,\$0}}' | sort -k1,1n -k2,2n -k3,3n -k4,4n | cut -f3-

!|expand:;:shortt;call_setl;exskipif;rusage:;call_set;,num_call_set_subsets;rusage_mod $recode_call_set_vcf_mem:short;call_set_subset;;| \
shortt cmd make_recode_call_setl_compressed_vcf_file=$plink108_cmd !{input,--bed,call_setl_bed_file} !{input,--bim,call_setl_bim_file} !{input,--fam,call_setl_fam_file} !{input,--extract,call_set_extract_file,if_prop=call_set_extract_file,allow_empty=1} --recode-vcf !{raw,--out,call_setl,*call_setl_temp_plink_file} && $plink_mv_log_cmd(!{raw\,\,call_setl\,*call_setl_temp_plink_file},!{output\,\,call_setl_recode_vcf_log_file}) && cat !{raw::call_setl:*call_setl_temp_plink_file.vcf} | $fix_vcf_file(call_setl_fam_file) | $process_call_set_plink_input $bgzip_helper(call_setl) > !{output,,call_setl_compressed_vcf_file} && rm !{raw,,call_setl,*call_setl_temp_plink_file.vcf} class_level call_setl run_if and,call_set_bed_file,call_set_bim_file,call_set_fam_file skip_if or,call_set_vcf_file,call_set_compressed_vcf_fileexskipif rusage

auto_zcat=if [[ `file -L !{input,,@1} | grep gzip` ]]; then zcat !{input,,@1}; else cat !{input,,@1}; fi

!!expand:,:inputtype,othertype,catcmd:,_compressed,cat:_compressed,,zcat! \
short cmd make_frominputtype_plink_call_set_subset_vcf_file=catcmd !{input,,call_set_from_plinkinputtype_vcf_file} | $vcf_utils_cmd !{input,--var-keep,call_set_subset_bim_keep_file} > !{output,,call_set_subset_vcf_file} class_level call_set_subset run_if and,call_set_from_plinkinputtype_vcf_file skip_if or,call_set_compressed_vcf_file,call_set_from_plinkothertype_vcf_file

make_call_set_plink_vcf_helper=!{raw::call_set:| $vcf_utils_cmd --var-keep *call_set_extract_file:if_prop=call_set_extract_file:allow_empty=1} !{input,call_set_extract_file,if_prop=call_set_extract_file,allow_empty=1} | $process_call_set_plink_input


short cmd make_plink_filter_call_set_subset_compressed_vcf_file=cat !{input,,call_set_subset_vcf_file} $make_call_set_plink_vcf_helper $bgzip_helper(call_set_subset) > !{output,,call_set_subset_compressed_vcf_file} class_level call_set_subset run_if or,call_set_from_plink_vcf_file,call_set_from_plink_compressed_vcf_file skip_if call_set_compressed_vcf_file

!!expand:,:inputtype,othertype,catcmd:,_compressed,cat:_compressed,,zcat! \
cmd make_from_plinkinputtype_filter_call_set_compressed_vcf_file=catcmd !{input,,call_set_from_plinkinputtype_vcf_file} $make_call_set_plink_vcf_helper $bgzip_helper(call_set) > !{output,,call_set_compressed_vcf_file} class_level call_set run_if call_set_from_plinkinputtype_vcf_file skip_if or,call_set_compressed_vcf_file,call_set_from_plinkothertype_vcf_file,num_call_set_subsets

local cmd ln_call_set_compressed_vcf_file=rm -f !{output,,call_set_compressed_vcf_file} && ln -s !{input,,call_set_vcf_file} !{output,,call_set_compressed_vcf_file} class_level call_set skip_if or,zip_vcf,call_set_compressed_vcf_file,(or,call_set_from_plink_vcf_file,call_set_from_plink_compressed_vcf_file) run_if call_set_vcf_file

short cmd make_call_set_compressed_vcf_file=rm -f !{output,,call_set_compressed_vcf_file} && cat !{input,,call_set_vcf_file} $bgzip_helper(call_set) > !{output,,call_set_compressed_vcf_file} class_level call_set run_if and,zip_vcf,call_set_vcf_file skip_if or,(or,call_set_from_plink_vcf_file,call_set_from_plink_compressed_vcf_file),call_set_compressed_vcf_file

#short cmd make_merged_call_set_compressed_vcf_file=$gatk_cmd(CombineVariants) !{output,-o,call_set_compressed_vcf_file} !{raw,,call_set_subset,--variant:@call_set_subset *call_set_subset_compressed_vcf_file} !{input,call_set_subset_compressed_vcf_file} !{input,call_set_subset_compressed_vcf_index_file,if_prop=zip_vcf,allow_empty=1} -genotypeMergeOptions REQUIRE_UNIQUE -filteredRecordsMergeType KEEP_IF_ANY_UNFILTERED -priority "!{prop,,call_set,sep=\,}" $bgzip_helper(call_set) class_level call_set run_if or,(and,call_set_bim_file,call_set_fam_file,call_set_bed_file),call_set_from_plink_vcf_file,call_set_from_plink_compressed_vcf_file skip_if or,call_set_compressed_vcf_file,!num_call_set_subsets

#Will this work? If not, switch back to above

short cmd make_merged_call_set_compressed_vcf_file=($zcat_helper(call_set) !{input,,call_set_subset_compressed_vcf_file,sort_prop=call_set_subset_num,limit=1} | grep '^\\#' && $zcat_helper(call_set) !{input,,call_set_subset_compressed_vcf_file,sort_prop=call_set_subset_num} | awk '!/^\\#/') $bgzip_helper(call_set) > !{output,,call_set_compressed_vcf_file} class_level call_set run_if or,(and,call_set_bim_file,call_set_fam_file,call_set_bed_file),call_set_from_plink_vcf_file,call_set_from_plink_compressed_vcf_file skip_if or,call_set_compressed_vcf_file,!num_call_set_subsets

!!expand:,:call_set,skipif:call_set,skip_if call_set_compressed_vcf_index_file:call_set_subset,! \
short cmd make_call_set_compressed_vcf_index_file=$tabix_cmd -f -p vcf !{input,,call_set_compressed_vcf_file} !{output,call_set_compressed_vcf_index_file} class_level call_set run_if zip_vcf skipif

broken_pipe_status=141

#get samples in this vcf file
!!expand:,:ftype,sbtype,col:bam,sample_bam,2:sample,passed_sample,1! \
local cmd make_call_set_ftype_list_file=$smart_join_cmd --ignore-status $broken_pipe_status --exec "$zcat_helper(call_set) !{input,,call_set_compressed_vcf_file} !{input,call_set_compressed_vcf_index_file,if_prop=zip_vcf,allow_empty=1} | $vcf_utils_cmd !{input,--samp-map,call_set_sample_vcf_id_to_sample_id_file} --print-samps | cat - !{input,,project_sbtype_list_file} | cut -f1 | sort | uniq -d" !{input,--file,project_sbtype_list_file} --rest-extra 1 --out-delim $tab | cut -fcol > !{output,,call_set_ftype_list_file} class_level call_set run_if do_merge

variant_site_vcf_helper=awk -v OFS="\t" -F "\t" '/^\#\#fileformat/ {print} @1 !/^\#\#/  {print \$1,\$2,\$3,\$4,\$5,\$6,\$7,\$8}'
variant_site_remove_vcf_helper=$variant_site_vcf_helper(!/^\#/ {\$8 = "."})

prop zip_vcf=scalar default .gz

short cmd make_call_set_variant_site_vcf_file=$zcat_helper(call_set) !{input,,call_set_compressed_vcf_file} !{input,call_set_compressed_vcf_index_file,if_prop=zip_vcf,allow_empty=1} | cut -f1-8 | $variant_site_remove_vcf_helper > !{output,,call_set_variant_site_vcf_file} class_level call_set

project_variant_site_helper=rm -f !{output,,project_variant_site_vcf_file} && $project_merge_vcf_helper(-l OFF -o /dev/stdout,call_set_variant_site_vcf_file,) -sites_only -minimalVCF | $vcf_utils_cmd --add-id

prop call_filtered=scalar default 1

short cmd make_project_variant_site_vcf_file=$project_variant_site_helper > !{output,,project_variant_site_vcf_file} class_level project run_if !call_filtered

short cmd make_project_all_variant_site_vcf_file=$project_variant_site_helper | awk -F"\t" -v OFS="\t" '/^[^\#]/ {\$7="$vcf_pass"} {print}' > !{output,,project_variant_site_vcf_file} class_level project run_if call_filtered

#local cmd cp_project_variant_site_vcf_file=rm -f !{output,,project_variant_site_vcf_file} && $vcf_utils_cmd --add-id !{input,,call_set_variant_site_vcf_file} > !{output,,project_variant_site_vcf_file} class_level project skip_if do_merge

short cmd make_project_variant_site_interval_list_file=egrep -v '^\#' !{input,,project_variant_site_vcf_file} | cut -f1-2 | sed 's/\t/:/' | uniq > !{output,,project_variant_site_interval_list_file} class_level project

!|expand:,:shortt,projectt,outtype,intype,expost:\
short,project,snp,,skip_if num_merge_subsets run_with project_variant_subset:\
short,project_merge_subset,snp,,skip_if !num_merge_subsets bsub_batch 20:\
short,project,indel,indel_,skip_if num_merge_subsets run_with project_variant_subset:\
local,project_merge_subset,indel,indel_,skip_if !num_merge_subsets bsub_batch 20|\
shortt cmd make_projectt_outtype_id_site_vcf_file=$zcat_helper(projectt) !{input,,projectt_intypevcf_file} | cut -f1-8 > !{output,,projectt_outtype_id_site_vcf_file} class_level projectt run_with project_variant_subset expost

short cmd make_project_merge_subset_variant_site_interval_list_file=awk 'NR >= !{prop,,project_merge_subset,merge_start} && NR < !{prop,,project_merge_subset,merge_end}' !{input,,project_variant_site_interval_list_file} > !{output,,project_merge_subset_variant_site_interval_list_file} class_level project_merge_subset

!!expand:,:keyext,fileext:,:_clean,.clean:_indel,.indel:_clean_indel,.clean.indel:_multiallelic,.multiallelic:_clean_multiallelic,.clean.multiallelic! \
meta_table cmd make_project_project_merge_subsetkeyext_vcf_list_file=!{input,project_project_merge_subset_meta_file} !{input,,project_merge_subset_variant_site_interval_list_file} !{input,,project_merge_subsetkeyext_vcf_file} !{output,project_project_merge_subsetkeyext_vcf_list_file} class_level project run_if num_merge_subsets 

!!expand:,:keyext,fileext:,:_clean,.clean:_indel,.indel:_clean_indel,.clean.indel:_multiallelic,.multiallelic:_clean_multiallelic,.clean.multiallelic! \
meta_table cmd make_project_projectkeyext_vcf_list_file=!{input,,project_expanded_interval_list_file} !{input,,projectkeyext_vcf_file} !{output,project_project_merge_subsetkeyext_vcf_list_file} class_level project skip_if num_merge_subsets 

!!expand:,:keyext,fileext:,:_clean,.clean:_indel,.indel:_clean_indel,.clean.indel:_multiallelic,.multiallelic:_clean_multiallelic,.clean.multiallelic! \
short cmd make_project_variant_subset_project_merge_subsetkeyext_vcf_list_file=cat !{input,,project_project_merge_subsetkeyext_vcf_list_file} | perl -lne 'BEGIN {open (IN, "!{input,,project_variant_subset_var_keep_file}") or die; while (<IN>) {chomp; @a = split(); \$m{\$a[0]}{\$a[1]} = 1} close IN} chomp; @a = split; open (IN, \$a[0]) or die; while (<IN>) {chomp; @b = split(":"); if (exists \$m{\$b[0]} && exists \$m{\$b[0]}{\$b[1]}) {print "\$a[1]"; close IN; last}} close IN;' > !{output,,project_variant_subset_project_merge_subsetkeyext_vcf_list_file} class_level project_variant_subset

short cmd make_project_merge_subset_call_set_vcf_list_file=echo !{raw,,call_set,*call_set_all_sites_site_vcf_file *call_set_all_sites_vcf_file @call_set,unless_prop=failed,sep=\,} | sed 's/,/\n/g' !{input,call_set_all_sites_site_vcf_file,unless_prop=failed} !{input,call_set_all_sites_vcf_file,unless_prop=failed} | perl -lne 'BEGIN {open (IN, "!{input,,project_merge_subset_variant_site_interval_list_file}") or die; while (<IN>) {chomp; @a = split(/:/); \$m{\$a[0]}{\$a[1]} = 1} close IN} chomp; @a = split; open (IN, \$a[0]) or die; while (<IN>) {next if /\#/; @b = split("\t"); if (exists \$m{\$b[0]} && exists \$m{\$b[0]}{\$b[1]}) {print "\$a[1]\t\$a[2]"; close IN; last}} close IN' > !{output,,project_merge_subset_call_set_vcf_list_file} class_level project_merge_subset

!!expand:type:snp:indel! \
short cmd make_project_type_site_vcf_file=$vcf_type_selector < !{input,,project_variant_site_vcf_file} > !{output,,project_type_site_vcf_file} class_level project

short cmd make_project_variant_subset_variant_site_vcf_file=cat !{input,,project_variant_site_vcf_file} | $vcf_utils_cmd !{input,--chr-pos-keep,project_variant_subset_var_keep_file} > !{output,,project_variant_subset_variant_site_vcf_file} class_level project_variant_subset

!!expand:,:project,runif:project,skip_if num_var_subsets:project_variant_subset,run_if num_var_subsets! \
short cmd make_project_clean_all_variant_site_vcf_file=zcat !{input,,project_clean_all_vcf_file} | $variant_site_vcf_helper() > !{output,,project_clean_all_variant_site_vcf_file} class_level project runif

#vcf_indel_awk_helper=if (/^\\#/) {print; next;} @alt_alleles=split(",", \$F[4]); @multi_alt=(); @single_alt=(); foreach \$alt (@alt_alleles) {if (length(\$alt) > 1 || length(\$F[3]) > 1) {push @multi_alt, \$alt} else {push @single_alt, \$alt}} 
#vcf_indel_selector=perl -lane '$vcf_indel_awk_helper; if (length(\$F[3]) > 1 || @multi_alt) {\$F[4]=join(",", @multi_alt); print join("\t", @F)}'
#vcf_snp_selector=perl -lane '$vcf_indel_awk_helper; if (length(\$F[3]) == 1 && @single_alt) {\$F[4]=join(",", @single_alt); print join("\t", @F)}'

vcf_indel_selector=$vcf_utils_cmd --remove-snps
vcf_snp_selector=$vcf_utils_cmd --remove-non-snps

prop genotype_snps=scalar default 1
prop genotype_indels=scalar default 1

project_site_vcf_selector=!{input,,project_variant_site_vcf_file,if_prop=genotype_snps,if_prop=genotype_indels,allow_empty=1} !{input,,project_snp_site_vcf_file,if_prop=genotype_snps,unless_prop=genotype_indels,allow_empty=1} !{input,,project_indel_site_vcf_file,unless_prop=genotype_snps,if_prop=genotype_indels,allow_empty=1}

#get variant,alleles combinations unique to the project level file
#then print those followed by the full project level vcf file
#then print sites the second time you see them (i.e. the full vcf lines that have ids in the unique set)
short cmd make_call_set_missed_variants_site_vcf_file=cat !{input,,call_set_variant_site_vcf_file} !{input,,call_set_variant_site_vcf_file} $project_site_vcf_selector | grep -v \\\# | cut -f1-2,4-5 | sort | uniq -u | cat - $project_site_vcf_selector | awk -v OFS="\t" 'NF == 4 {print \$1,\$2,".",\$3,\$4} NF != 4 {print}' | awk '/^\\#/ || m[\$1":"\$2":"\$4":"\$5]++ == 1' > !{output,,call_set_missed_variants_site_vcf_file} class_level call_set skip_if failed

#CURRENT ISSUE: MULIPLE SITES WITH MULTIPLE ALLELES AT PRJECT LEVEL

#get chr,pos allele combinations unique to the original site file
#assuming no bug upstream, these must have different alleles
#print those followed by the call_set VCF file
#print the full VCF lines that are not in the common combinations
short cmd make_call_set_dis_removed_vcf_file=$zcat_helper(call_set) !{input,,call_set_compressed_vcf_file} !{input,call_set_compressed_vcf_index_file,if_prop=zip_vcf,allow_empty=1} | grep -v \\\# | cut -f1-2,4-5 | sort -u | awk -F"\t" -v OFS="\t" '{print \$1,\$2,".",\$3,\$4}' | cat - $project_site_vcf_selector $project_site_vcf_selector | grep -v \\\# | cut -f1-2,4-5 | sort | uniq -u | $smart_cut_cmd --no-delim --stdin-first --exec "$zcat_helper(call_set) !{input,,call_set_compressed_vcf_file}" | awk -v OFS="\t" '{if (/^\\#/) {print} else if (NF != 4 && !m[\$1":"\$2":"\$4":"\$5]) {print} else {m[\$1":"\$2":"\$3":"\$4]++}}' !{raw,,call_set,| $vcf_utils_cmd --remove-non-biallelic,if_prop=ignore_multiallelics,allow_empty=1} $bgzip_helper(call_set) > !{output,,call_set_dis_removed_vcf_file} $tabix_helper(call_set,call_set_dis_removed_vcf_file) class_level call_set skip_if failed run_if do_merge

variant_annot_vcf_helper=awk -v OFS="\t" -F "\t" '/^\#\#/ {print} !/^\#\#/  {print \$1,\$2,\$3,\$4,\$5,\$6,\$7,\$8}'

local cmd make_call_set_variant_annot_site_vcf_file=$zcat_helper(call_set) !{input,,call_set_dis_removed_vcf_file} | $variant_annot_vcf_helper > !{output,,call_set_variant_annot_site_vcf_file} class_level call_set run_if do_merge



call_set_subsets_meta_helper=cs="{1..!{prop,,call_set,num_call_set_subsets}}" && (echo "cs\$cs  class call_set_subset" && echo "cs\$cs parent !{prop,,call_set}" && echo "cs\$cs call_set_subset_num \$cs")
call_set_sample_subsets_meta_helper=css="{1..!{prop,,call_set,num_call_set_sample_subsets}}" && (echo "css\$css  class call_set_sample_subset" && echo "css\$css parent !{prop,,call_set}" && echo "css\$css call_set_sample_subset_num \$css")

local cmd make_call_set_call_set_subsets_meta_file=$call_set_subsets_meta_helper > !{output,,call_set_call_set_subsets_meta_file} class_level call_set run_if and,num_call_set_subsets,!num_call_set_sample_subsets

local cmd make_call_set_call_set_sample_subsets_meta_file=($call_set_subsets_meta_helper && $call_set_sample_subsets_meta_helper) > !{output,,call_set_call_set_subsets_meta_file} class_level call_set run_if and,num_call_set_subsets,num_call_set_sample_subsets

local cmd make_project_chr_map_file=(echo $plink_chrX $chrX && echo $plink_chrY $chrY && echo $plink_chrXY $chrXY &&  echo $plink_chrM $chrM) > !{output,,project_chr_map_file} class_level project

split_helper=perl -ne 'BEGIN {use POSIX "ceil"; \$size = `grep -v \\\# @1 | wc -l`; \$ntot = !{prop,,@{2}_subset,num_@{2}_subsets}; \$size = ceil(\$size / \$ntot); \$i = 0; \$cur_num = !{prop,,@{2}_subset,@{2}_subset_num}; die "Bad number: \$cur_num" unless \$cur_num > 0 && \$cur_num <= \$ntot} print if (/^\\#/ || (\$i >= (\$cur_num - 1) * \$size && \$i < (\$cur_num * \$size))); \$i++'

prop num_call_set_subsets=scalar
prop call_set_subset_num=scalar
prop num_call_set_sample_subsets=scalar
prop call_set_sample_subset_num=scalar
split_call_set_file_helper=$split_helper(@1,call_set)

prop do_merge=scalar default 1
prop recall_all=scalar default 1
a
#Use only missed sites as template for call_set_subset_missed_sites_interval_list_file

local cmd make_call_set_subset_missed_variants_site_vcf_file=$split_call_set_file_helper(!{input\\,\\,call_set_missed_variants_site_vcf_file}) < !{input,,call_set_missed_variants_site_vcf_file} > !{output,,call_set_subset_missed_variants_site_vcf_file} class_level call_set_subset run_if and,do_merge,!failed,!recall_all

#Use all variant sites as template for call_set_subset_missed_sites_interval_list_file
local cmd make_call_set_subset_all_variants_site_vcf_file=$split_helper("$project_site_vcf_selector",call_set) < $project_site_vcf_selector > !{output,,call_set_subset_missed_variants_site_vcf_file} class_level call_set_subset run_if and,do_merge,!failed,recall_all

#cmd make_call_set_subset_missed_sites_temp_vcf_file=$gatk_cmd_no_interval(UnifiedGenotyper) -l INFO !{output,--out,call_set_subset_missed_sites_temp_vcf_file} !{input,-I,call_set_bam_list_file} !{input,sample_bam_file,unless_prop=failed} !{input,-L,call_set_subset_missed_sites_interval_list_file} -all_bases class_level call_set_subset run_if and,do_merge,!failed

#clean_vcf_helper=awk -F"\t" '/^\\#/ || NF != 8' !{input,,@1} | perl $targeted_bin_dir/add_freq_meta_field.pl > !{output,,@2}

#local cmd make_call_set_subset_missed_sites_vcf_file=$clean_vcf_helper(call_set_subset_missed_sites_temp_vcf_file,call_set_subset_missed_sites_vcf_file) class_level call_set_subset run_if and,do_merge,!failed

local cmd make_call_set_sample_subset_bam_list_file=$split_helper(!{input::call_set_bam_list_file},call_set_sample) < !{input::call_set_bam_list_file} > !{output,,call_set_sample_subset_bam_list_file} class_level call_set_sample_subset

!|expand:,:call_set_subsetl,bamlist,runif:call_set_subset,call_set_bam_list_file,!num_call_set_sample_subsets:call_set_sample_subset,call_set_sample_subset_bam_list_file,num_call_set_sample_subsets| \
short cmd make_call_set_subsetl_missed_variants_vcf_file=$gatk_cmd_no_interval(UnifiedGenotyper) !{output,-o,call_set_subsetl_missed_variants_vcf_file} !{input,-I,bamlist,is_list=1} --output_mode EMIT_ALL_SITES --alleles !{raw,,call_set_subset,*call_set_subset_missed_variants_site_vcf_file} !{raw,-L,call_set_subset,*call_set_subset_missed_variants_site_vcf_file} !{input,call_set_subset_missed_variants_site_vcf_file} -gt_mode GENOTYPE_GIVEN_ALLELES -glm BOTH -stand_call_conf 0.0  !{raw,-A,call_set,FisherStrand,if_prop=genotype_indels,allow_empty=1} $tabix_helper(call_set_subsetl,call_set_subsetl_missed_variants_vcf_file) class_level call_set_subsetl run_if and,do_merge,!failed,runif

short cmd make_merge_call_set_subset_missed_variants_vcf_file=$gatk_cmd(CombineVariants) !{output,-o,call_set_subset_missed_variants_vcf_file} !{raw,,call_set_sample_subset,--variant:@call_set_sample_subset *call_set_sample_subset_missed_variants_vcf_file} !{input,call_set_sample_subset_missed_variants_vcf_file} $require_unique_helper -filteredRecordsMergeType KEEP_IF_ANY_UNFILTERED -priority "!{prop,,call_set_sample_subset,sep=\,}" $bgzip_helper(call_set_subset) $tabix_helper(call_set_subset,call_set_subset_missed_variants_vcf_file) class_level call_set_subset run_if and,do_merge,!failed,num_call_set_sample_subsets

called_tag=called
uncalled_tag=uncalled

remove_info_from_missed_sites=$vcf_utils_cmd --remove-info AB --remove-info AF --remove-info AN --remove-info DP

short cmd make_call_set_missed_sites_vcf_file=rm -f !{output,,call_set_missed_sites_vcf_file} && $gatk_cmd(CombineVariants) !{output,-o,call_set_missed_sites_vcf_file} !{raw,,call_set_subset,--variant:@call_set_subset *call_set_subset_missed_variants_vcf_file} !{input,call_set_subset_missed_variants_vcf_file} $require_unique_helper -filteredRecordsMergeType KEEP_IF_ANY_UNFILTERED -priority "!{prop,,call_set_subset,sep=\,}" | $remove_info_from_missed_sites $bgzip_helper(call_set) $tabix_helper(call_set,call_set_missed_sites_vcf_file) class_level call_set run_if and,do_merge,!failed rusage_mod $missed_sites_mem

call_set_all_sites_vcf_file_helper=rm -f !{output,,call_set_all_sites_vcf_file} && $gatk_cmd(CombineVariants) @1 !{raw,,call_set,--variant:$called_tag *@2} !{raw,,call_set,--variant:$uncalled_tag *call_set_missed_sites_vcf_file} !{input,@2} !{input,call_set_missed_sites_vcf_file} -genotypeMergeOptions PRIORITIZE -filteredRecordsMergeType KEEP_IF_ANY_UNFILTERED -priority "$uncalled_tag,$called_tag"

#Set to map sample ids within call set
#Use when different call sets have samples with the same id
prop map_samples_within_call_set=scalar default 0

short cmd ln_call_set_all_sites_vcf_file=rm -f !{output,,call_set_all_sites_vcf_file} && ln -s !{input,,call_set_compressed_vcf_file} !{output,,call_set_all_sites_vcf_file} $tabix_helper(call_set,call_set_all_sites_vcf_file) class_level call_set run_if and,!do_merge,!map_samples_within_call_set

short cmd map_call_set_all_sites_vcf_file=$zcat_helper(call_set) !{input,,call_set_compressed_vcf_file} | if [[ `cat !{input,,call_set_sample_vcf_id_to_sample_id_file} | wc -l` -gt 0 ]]; then $vcf_utils_cmd !{input,--samp-map,call_set_sample_vcf_id_to_sample_id_file}; else cat -; fi $bgzip_helper(call_set) > !{output,,call_set_all_sites_vcf_file} $tabix_helper(call_set,call_set_all_sites_vcf_file) class_level call_set run_if and,!do_merge,map_samples_within_call_set

short cmd replace_call_set_all_sites_vcf_file=$zcat_helper(call_set) !{input,,call_set_missed_sites_vcf_file} | $vcf_utils_cmd !{input,--samp-map,call_set_sample_vcf_id_to_sample_id_file} $bgzip_helper(call_set) > !{output,,call_set_all_sites_vcf_file} $tabix_helper(call_set,call_set_all_sites_vcf_file) class_level call_set run_if and,do_merge,recall_all

short cmd make_call_set_all_sites_vcf_file=$call_set_all_sites_vcf_file_helper(-l OFF -o /dev/stdout,call_set_dis_removed_vcf_file) | $vcf_utils_cmd !{input,--samp-map,call_set_sample_vcf_id_to_sample_id_file} $bgzip_helper(call_set) > !{output,,call_set_all_sites_vcf_file} $tabix_helper(call_set,call_set_all_sites_vcf_file) class_level call_set run_if and,do_merge,!failed,!recall_all

#Second half here removes sites not on missed sites file --- with current version of GATK (Jan 2011), sites that are no-called in everyone are not output as part of all_bases

#cmd make_and_filter_call_set_all_sites_vcf_file=$call_set_all_sites_vcf_file_helper(-l OFF -o /dev/stdout) | $vcf_utils_cmd !{input,--reference-vcf,call_set_missed_sites_vcf_file} --valid-chr-pos > !{output,,call_set_all_sites_vcf_file} class_level call_set run_if and,do_merge,!failed,recall_all


#due to change in annotation, dbSNP membership flag is screwing up between different VCF versions
#That is, some call set subsets have dbsnp as a flag, others as an integer
#This causes the merge at the project level to screw up
#So, the second set of commands will strip the dbsnp header from the VCF files
#Once all filtered.maf_annotated.vcfs are rerun with latest version of GATK, can use first set of commands instead
#BEGIN FIRST
#cmd make_call_set_ab_annotated_vcf_file=rm -f !{output,,call_set_ab_annotated_vcf_file} && $gatk_cmd(CombineVariants) -l INFO !{output,-o,call_set_ab_annotated_vcf_file} !{raw,,call_set_subset,--variant:@call_set_subset *call_set_subset_ab_annotated_vcf_file} !{input,call_set_subset_ab_annotated_vcf_file} -genotypeMergeOptions REQUIRE_UNIQUE -variantMergeOptions INTERSECT -priority "!{prop,,call_set_subset,sep=\,}" class_level call_set skip_if or,one_call_set_subset,failed,ab_from_ad

#local cmd ln_call_set_ab_annotated_vcf_file=rm -f !{output,,call_set_ab_annotated_vcf_file} && ln -s !{input,,call_set_subset_ab_annotated_vcf_file} !{output,,call_set_ab_annotated_vcf_file} class_level call_set run_if and,one_call_set_subset,!failed,!ab_from_ad

#local cmd make_call_set_ab_annotated_vcf_file_from_ad=$vcf_utils_cmd < !{input,,call_set_all_sites_vcf_file} --ab-from-ad > !{output,,call_set_ab_annotated_vcf_file} class_level call_set run_if ab_from_ad
#END FIRST
#BEGIN SECOND

handle_dbsnp_hack=sed '/^\#\#INFO=<ID=DB/d'

#this is what is added to the VCF
prop vcf_utils_add_dosage_field=scalar default DOS

prop add_gq=scalar default 0
prop ensure_pl=scalar default 1
prop require_samples=scalar default 1

thresholded_nalt_field=NALTT

prop sex_for_dosage=scalar

vcf_utils_processing_flags=--add-id --remove-dup-ids --remove-dup-vars --add-num-nonref --add-dosage !{prop,--dosage-annot,project,vcf_utils_add_dosage_field} !{input,--sample-gender-file,pheno_non_missing_sample_pheno_file,if_prop=sex_for_dosage,if_prop=pheno:eq:@sex_for_dosage,max=1,allow_empty=1} !{input,--sample-gender-file,project_sample_genetic_sex_file,unless_prop=sex_for_dosage,unless_prop=no_x_chrom,allow_empty=1} --x-chrom $chrX `echo $chrX_par | sed 's/\(^\|\s\s*\)\(\S\S*\)/\1--par \2/g'` !{raw,,project,--add-gq,if_prop=add_gq,allow_empty=1} !{raw,,project,--add-thresholded-nalt @gq_crit} --fix-pass --round-format GQ !{input,--samp-map,project_sample_vcf_id_to_sample_id_file,unless_prop=map_samples_within_call_set,unless_prop=do_merge,allow_empty=1} !{input,--samp-require,project_passed_sample_list_file,if_prop=require_samples,allow_empty=1} --require-unique !{input,--samp-keep,project_passed_sample_list_file} 

prop add_protein_change=scalar

#do not use any type of sample level AB annotation
prop no_sample_ab=scalar
#any genotype with malformatted format, set to missing
prop append_missing_malformatted_format=scalar
#inform the pipeline that the VCF already has AB annotations -- useful to skip running time of annotating 
#in the future, pipeline should be able to recognize this on the fly
prop ab_present=scalar

#local cmd ln_call_set_ab_annotated_vcf_file=rm -f !{output,,call_set_ab_annotated_vcf_file} && ln -s !{input,,call_set_all_sites_vcf_file} !{output,,call_set_ab_annotated_vcf_file} $tabix_helper(call_set,call_set_ab_annotated_vcf_file) class_level call_set 

short cmd make_call_set_all_sites_site_vcf_file=$zcat_helper(call_set) !{input,,call_set_all_sites_vcf_file,if_prop=do_merge,if_prop=map_samples_within_call_set,or_if_prop=1,allow_empty=1} !{input,,call_set_compressed_vcf_file,unless_prop=do_merge,unless_prop=map_samples_within_call_set,allow_empty=1} | cut -f1-8 | $variant_site_vcf_helper() > !{output,,call_set_all_sites_site_vcf_file} class_level call_set

#END SECOND

#project_subset commands

prop num_project_subsets=scalar
prop project_subset_num=scalar
#local cmd make_project_subset_interval_list_file=grep -v \@ !{input,,project_expanded_interval_list_file} | $split_interval_file_helper(project_expanded_interval_list_file,project) | awk '{print \$1":"\$2"-"\$3}' > !{output,,project_subset_interval_list_file} class_level project_subset

#samtools_mpileup_cmd=$samtools_cmd mpileup -r @1 -C50 -DSug -f $reference_file !{input,-b,project_bam_list_file} | $bcftools_cmd view -vcg - | sed 's/\(^#.*PL,Number\)=[^,]*,/\1=3,/'
"""
}
    