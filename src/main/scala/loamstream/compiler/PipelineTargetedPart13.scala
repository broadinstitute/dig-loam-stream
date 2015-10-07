
package loamstream.compiler

/**
 * LoamStream
 * Created by oliverr on 10/6/2015.
 */
object PipelineTargetedPart13 {
  val string =
 """
#cmd make_project_subset_samtools_vcf_file=rm -f !{output,,project_subset_samtools_vcf_file} && $samtools_mpileup_cmd("`awk 'NR == 1' !{input,,project_subset_interval_list_file}`") >> !{output,,project_subset_samtools_vcf_file} && for f in `tail -n+2 !{input,,project_subset_interval_list_file}`; do ($samtools_mpileup_cmd(\$f) | (grep -v \\# || true) >> !{output,,project_subset_samtools_vcf_file}) || exit 1; done class_level project_subset

#project

#cmd make_project_samtools_vcf_file=$gatk_cmd(CombineVariants) !{output,-o,project_samtools_vcf_file} !{raw,,project_subset,--variant:@project_subset *project_subset_samtools_vcf_file} !{input,project_subset_samtools_vcf_file} -genotypeMergeOptions PRIORITIZE -variantMergeOptions INTERSECT -priority "!{prop,,project_subset,sep=\,}" class_level project

prop validate_unique=scalar default 0
require_unique_helper=!{raw,-genotypeMergeOptions,project,REQUIRE_UNIQUE,if_prop=validate_unique,allow_empty=1}
merge_vcf_helper=$require_unique_helper -filteredRecordsMergeType KEEP_IF_ANY_UNFILTERED
project_merge_vcf_helper=$gatk_cmd(CombineVariants) @1 !{raw,,call_set,--variant:\@call_set *@2,unless_prop=failed@3} !{input,@2,unless_prop=failed@3} -priority "!{prop,,call_set,sep=\,}" $merge_vcf_helper

prop union_filter=scalar default 1

#used to have this
#  | $vcf_utils_cmd !{input,--reference-vcf,project_variant_site_vcf_file} --valid-ref-alt removet !{input,--samp-map,project_sample_vcf_id_to_sample_id_file} \

#prop merge_chunk_size=scalar

queue_scratch_dir=$tmp_dir/queue
queue_lsf_queue=$lsf_short_queue

#local cmd make_scatter_project_merged_vcf_file=mkdir -p $queue_scratch_dir && java -Djava.io.tmpdir=$queue_scratch_dir -jar $queue_jar -S $sg_combine_scala_script !{input,-V,call_set_ab_annotated_vcf_file,unless_prop=failed} !{input,-L,project_expanded_interval_list_file,if_prop=expand_targets,allow_empty=1} !{input,-L,project_interval_list_file,unless_prop=expand_targets,allow_empty=1} !{output,-o,project_merged_vcf_file} --reference $reference_file -bsub -run -jobQueue $queue_lsf_queue !{prop,-c,project,merge_chunk_size} class_level project run_if and,do_merge,merge_chunk_size,!num_merge_subsets


cmd make_project_merged_vcf_file=$project_merge_vcf_helper(!{output:-o:project_merged_vcf_file},call_set_all_sites_vcf_file,) $tabix_helper(project,project_merged_vcf_file) class_level project skip_if num_merge_subsets rusage_mod $merge_vcf_mem

short cmd make_project_merge_subset_merged_vcf_file=$gatk_cmd_no_interval(CombineVariants) !{output,-o,project_merge_subset_merged_vcf_file} !{input,-L,project_merge_subset_variant_site_interval_list_file} `sed 's/\(\S\S*\)\s\s*\(\S\S*\)/--variant:\2 \1/'  !{input,,project_merge_subset_call_set_vcf_list_file}` !{input,call_set_all_sites_vcf_file} `cut -f2 !{input,,project_merge_subset_call_set_vcf_list_file} !{input,call_set_all_sites_site_vcf_file} | tr '\n' , | sed 's/,$//' | sed 's/^/-priority /'` $merge_vcf_helper $tabix_helper(project_merge_subset,project_merge_subset_merged_vcf_file) class_level project_merge_subset run_if num_merge_subsets 

prop no_x_chrom=scalar

!|expand:,:shortt,projectt,expost:,project,skip_if num_merge_subsets:short,project_merge_subset,skip_if !num_merge_subsets| \
shortt cmd make_projectt_for_genetic_sex_plink_files=(!{raw,,projectt,cat *projectt_merged_vcf_file,unless_prop=zip_vcf,allow_empty=1}!{raw,,projectt,zcat *projectt_merged_vcf_file | $vcf_utils_cmd --print-header; $tabix_cmd *projectt_merged_vcf_file $chrX,if_prop=zip_vcf,allow_empty=1}) !{input,projectt_merged_vcf_file} | $vcf_utils_cmd !{input,--samp-map,project_sample_vcf_id_to_sample_id_file,unless_prop=map_samples_within_call_set,unless_prop=do_merge,allow_empty=1} | $pseq_cmd - write-ped $gq_mask --mask filter=$vcf_pass --mask biallelic --mask indel.ex --name !{raw,,projectt,*projectt_for_genetic_sex_plink_file} !{output,projectt_for_genetic_sex_tfam_file} !{output,projectt_for_genetic_sex_tped_file} class_level projectt expost run_if !no_x_chrom

short cmd make_cat_project_for_genetic_sex_plink_files=cp !{input,,project_merge_subset_for_genetic_sex_tfam_file,limit=1} !{output,,project_for_genetic_sex_tfam_file} && cat !{input,,project_merge_subset_for_genetic_sex_tped_file} > !{output,,project_for_genetic_sex_tped_file} class_level project run_if num_merge_subsets skip_if no_x_chrom

short cmd make_project_genetic_sex_sexcheck_file=echo $chrX_par | sed 's/\s\s*/\n/g' | sed 's/-/\t/' | sed 's/^/$plink_chrX\t/' | awk -v OFS="\t" -F"\t" '{print \$0,"R"NR}' | $plink_cmd !{input,--tped,project_for_genetic_sex_tped_file} !{input,--tfam,project_for_genetic_sex_tfam_file} --exclude /dev/stdin --range $plink_analysis_helper(project_genetic_sex_sexcheck_file,check-sex --maf 0.05 --geno 0.1,sexcheck,project_genetic_sex_sexcheck_log_file) class_level project skip_if no_x_chrom

het_for_sex=.6

local cmd make_project_sample_genetic_sex_file=awk -v OFS="\t" 'NR > 1 && \$6 > $het_for_sex {print \$1,1} NR > 1 && \$6 < (1 - $het_for_sex) {print \$1,2}' !{input,,project_genetic_sex_sexcheck_file} > !{output,,project_sample_genetic_sex_file} class_level project skip_if no_x_chrom

#local cmd make_cat_project_merged_vcf_file=rm -f !{output,,project_merged_vcf_file} && ($zcat_helper(project) !{input,,call_set_ab_annotated_vcf_file,limit=1} | grep  '^\\#' && $zcat_helper(project) !{input,,call_set_ab_annotated_vcf_file} | grep -v '^\\#') $bgzip_helper(project) > !{output,,project_merged_vcf_file} $tabix_helper(project,project_merged_vcf_file) class_level project skip_if num_merge_subsets run_if !multiple_call_sets

!|expand:,:shortt,projectt,expost:,project,skip_if num_merge_subsets rusage_mod $initial_vcf_mem:short,project_merge_subset,skip_if !num_merge_subsets rusage_mod $initial_merge_vcf_mem| \
!|expand:,:keyname,vartype,removet,removeb:,snp,--remove-non-snps,--remove-non-biallelic:indel_,indel,--remove-snps,--remove-non-biallelic:multiallelic_,multiallelic,,--remove-biallelic| \
shortt cmd make_projectt_keynamevcf_file=rm -f !{output,,projectt_keynamevcf_file} && $zcat_helper(projectt) !{input,,projectt_merged_vcf_file} \
	!{raw,,project,| $vcf_utils_cmd --append-missing-malformatted-format,if_prop=append_missing_malformatted_format,allow_empty=1} \
	| $vcf_utils_cmd $vcf_utils_processing_flags removet removeb !{raw,,project,--ensure-pl,if_prop=ensure_pl,allow_empty=1} !{raw,,project,--ab-from-ad,unless_prop=no_sample_ab,unless_prop=ab_present,allow_empty=1} !{raw,,project,--het-ad-from-ad,unless_prop=no_sample_ab,allow_empty=1} \
  | $vcf_utils_cmd !{input,--var-excl,projectt_snp_indel_overlap_file} \
  !{raw,,project,| $vcf_utils_cmd --valid-filter,if_prop=do_merge,allow_empty=1} !{input,--chr-pos-keep,projectt_variant_site_interval_list_file,if_prop=do_merge,allow_empty=1} !{input,--reference-vcf,call_set_variant_site_vcf_file,unless_prop=failed,if_prop=genotype_vartypes,if_prop=do_merge,allow_empty=1} !{raw,,projectt,--union-filter,if_prop=union_filter,if_prop=do_merge,allow_empty=1} \
  !{raw,,project,| $vcf_utils_cmd --sum-info  $vcf_mq0_annot --sum-info $vcf_het_ad_annot,if_prop=do_merge,allow_empty=1} !{input,--chr-pos-keep,projectt_variant_site_interval_list_file,if_prop=do_merge,allow_empty=1} !{input,--reference-vcf,call_set_all_sites_site_vcf_file,unless_prop=failed,if_prop=genotype_vartypes,if_prop=do_merge,allow_empty=1} \
  !{raw,,project,| $vcf_utils_cmd --het-ab-from-het-ad,unless_prop=no_sample_ab,allow_empty=1} $bgzip_helper(projectt) \
  > !{output,,projectt_keynamevcf_file} $tabix_helper(projectt,projectt_keynamevcf_file) class_level projectt expost

select_variants_cmd=$gatk_cmd(SelectVariants) -o @2 !{input,--variant,@1} !{input,--sample_file,@3} --ALLOW_NONOVERLAPPING_COMMAND_LINE_SAMPLES --excludeNonVariants -l @4
project_merge_subset_select_variants_cmd=$gatk_cmd_no_interval(SelectVariants) !{input,-L,project_merge_subset_variant_site_interval_list_file} -o @2 !{input,--variant,@1} !{input,--sample_file,@3} --ALLOW_NONOVERLAPPING_COMMAND_LINE_SAMPLES --excludeNonVariants -l @4

#!|expand:,:projectt,expost:project,skip_if num_merge_subsets:project_merge_subset,skip_if !num_merge_subsets| \
#!!expand:type:snp:indel! \
#short cmd make_projectt_subsetted_type_vcf_file=$conditional_exec_cmd \
#  --cond-cmd "perl -e '\\$wc = `$zcat_helper(projectt) !{input,,projectt_initial_type_vcf_file} | $vcf_utils_cmd --print-samps  | sort - !{input,,project_passed_sample_list_file} | uniq -u | wc -l`; if (\\$wc == 0) {exit 1} else {exit 2}'" \
#	--pred-cmd 1,"rm -f !{output,,projectt_subsetted_type_vcf_file} && ln -s !{input,,projectt_initial_type_vcf_file} !{output,,projectt_subsetted_type_vcf_file} $tabix_helper(projectt,projectt_subsetted_type_vcf_file)" \ 
#	--pred-cmd 2,"rm -f !{output,,projectt_subsetted_type_vcf_file} && $select_variants_cmd(projectt_initial_type_vcf_file,/dev/stdout,project_passed_sample_list_file,OFF) $bgzip_helper(project) > !{output,,projectt_subsetted_type_vcf_file} $tabix_helper(projectt,projectt_subsetted_type_vcf_file)" class_level projectt run_if do_merge expost

#force SelectVariants to run if one_call_set_subset to make sure variants outside of interval list are removed


#!!expand:type:snp:indel! \
#short cmd make_project_force_subsetted_type_vcf_file=rm -f !{output,,project_subsetted_type_vcf_file} && $select_variants_cmd(project_initial_type_vcf_file,/dev/stdout,project_passed_sample_list_file,OFF) $bgzip_helper(project) > !{output,,project_subsetted_type_vcf_file} $tabix_helper(project,project_subsetted_type_vcf_file) class_level project skip_if do_merge



#!|expand:,:shortt,projectt,expost:local,project,skip_if num_merge_subsets:short,project_merge_subset,skip_if !num_merge_subsets| \
#shortt cmd make_projectt_vcf_file=$zcat_helper(projectt) !{input,,projectt_subsetted_snp_vcf_file} \
#			    $bgzip_helper(projectt) > !{output,,projectt_vcf_file} $tabix_helper(projectt,projectt_vcf_file) class_level projectt expost

!|expand:;:shortt;catfile;exskipif:\
;vcf;:\
;clean_vcf;skip_if and,!sample_qc_filter,!var_qc_filter,!project_variant_custom_exclude_file,!project_sample_custom_exclude_file:\
short;indel_vcf;:\
short;clean_indel_vcf;skip_if and,!sample_qc_filter,!var_qc_filter,!project_variant_custom_exclude_file,!project_sample_custom_exclude_file:\
;multiallelic_vcf;:\
;clean_multiallelic_vcf;skip_if and,!sample_qc_filter,!var_qc_filter,!project_variant_custom_exclude_file,!project_sample_custom_exclude_file|\
shortt cmd cat_project_catfile_file=rm -f !{output,,project_catfile_file} && ($zcat_helper(project) !{input,,project_merge_subset_catfile_file,limit=1} | grep '^\\#' && $zcat_helper(project) !{input,,project_merge_subset_catfile_file} | awk '!/^\\#/') $bgzip_helper(project) > !{output,,project_catfile_file} $tabix_helper(project,project_catfile_file) class_level project run_if num_merge_subsets exskipif run_with project_sample_subset,project_variant_subset

!|expand:;:shortt;catfile;exskipif:\
short;snp_id_site_vcf;:\
short;indel_id_site_vcf;|\
shortt cmd cat_no_bgzip_project_catfile_file=rm -f !{output,,project_catfile_file} && (cat !{input,,project_merge_subset_catfile_file,limit=1} | grep '^\\#' && cat !{input,,project_merge_subset_catfile_file} | awk '!/^\\#/') > !{output,,project_catfile_file} class_level project run_if num_merge_subsets exskipif run_with project_sample_subset,project_variant_subset


cmd cat_project_clean_all_vcf_file=rm -f !{output,,project_clean_all_vcf_file} && (zcat !{input,,project_merge_subset_clean_all_vcf_file,limit=1} | grep '^\\#' && zcat !{input,,project_merge_subset_clean_all_vcf_file} | awk '!/^\\#/') | $bgzip_cmd > !{output,,project_clean_all_vcf_file} && !{raw,,project,$run_tabix_cmd(project_clean_all_vcf_file)} class_level project run_if num_merge_subsets

#defined scope of project subsets
prop samp_start=scalar
prop samp_end=scalar

prop var_start=scalar
prop var_end=scalar

prop merge_start=scalar
prop merge_end=scalar

prop var_subset_num=scalar

prop expand_subset_num=scalar
#prop expand_subset=scalar

#Generate the subsets
prop num_samp_subsets=scalar
prop num_var_subsets=scalar
prop mean_expand_pheno_subsets=scalar
prop expand_pheno_subsets=scalar
prop num_merge_subsets=scalar

#in case some shouldn't be analyzed

#at project level; indicate is empty for clean file
prop empty_clean_subsets=list
#at pheno level: each element is two columns: variant subset, expand subset number
prop exclude_subset=list


#depend on variant subset meta file just to ensure that they are both built at the same time
#e.g., don't want to defer all variant subset commands
local cmd make_project_project_sample_subset_meta_file=perl -ne 'BEGIN {use POSIX "ceil"; \$n=0} \$n++; END {\$num_per = ceil(\$n / !{prop,,project,num_samp_subsets}); for (\$i = 0; \$i < !{prop,,project,num_samp_subsets}; \$i++) {\$b = \$i * \$num_per + 1; \$e = \$b + \$num_per; print "\$b\t\$e\n" if \$b <= \$n}}' < !{input,,project_plinkseq_ind_info_file} | awk '{n="!{prop,,project}_sample_subset_"NR; print n" class project_sample_subset\n"n" disp Sample subset "NR"\n"n" parent !{prop,,project}\n"n" samp_start "\$1"\n"n" samp_end "\$2}' > !{output,,project_project_sample_subset_meta_file} class_level project run_if num_samp_subsets with project_sample_subset run_with project_merge_subset

short cmd make_project_subset_vars_file=$smart_join_cmd --exec "cat !{input,,project_gene_target_file} !{input,,project_transcript_target_file} | sort -nk2,2 -k3,3 | $add_gene_annot_cmd --in-delim $tab --outside-name $outside_gene_name --keep-outside --chr-col 2 --pos-col 3 --gene-file !{input,,project_region_list_file} --out-delim $tab | cut -f1-2 | sed '1 s/^/0:0-1\t$outside_gene_name\n/' | sort -k2 | uniq -f1 | $smart_cut_cmd --in-delim $tab --select-col 0,'2 1'" --exec "cat !{input,,project_snp_site_vcf_file} !{input,,project_indel_site_vcf_file} | fgrep -v \\# | cut -f1-2 | sort -u | sed 's/$/\tvariant/' | $smart_cut_cmd --in-delim $tab --exec \"$smart_cut_cmd --in-delim $tab !{input,--file,project_expanded_exon_target_file} --select-col 1,3-4 !{input,--file,project_expanded_exon_target_file} --select-col 2,'3 5' | sed 's/$/\ttarget/'\" | sort -nk 1,1 -k 2,2 | $add_gene_annot_cmd --in-delim $tab --outside-name $outside_gene_name --print-multiple --keep-outside --chr-col 1 --pos-col 2 !{input,--gene-file,project_gene_target_file} !{input,--gene-file,project_transcript_target_file} --out-delim $tab" --in-delim $tab --multiple 2 --extra 1 | cut -f2- | sort -k2,2 -k3,3n | uniq > !{output,,project_subset_vars_file} class_level project run_if num_var_subsets run_with project_merge_subset

get_subset_vars=cat !{input,,project_subset_vars_file}

local cmd make_project_project_variant_subset_meta_file=n=`$get_subset_vars | wc -l` && $get_subset_vars | perl -ne 'BEGIN {use POSIX "ceil"; \$n = 1; \$v = 0; @starts = (1); \$num_per = ceil('"\$n"' / !{prop,,project,num_var_subsets}); \$next = \$num_per} @a = split; if (\$n >= \$next && defined \$reg && (\$reg eq "$outside_gene_name" || \$reg ne \$a[0])) {push @starts, \$n if \$v > 0; \$v = 0; \$next = \$n + \$num_per} \$n++; \$v++ if \$a[3] eq "variant"; \$reg = \$a[0]; END {if (\$starts[\$\#starts] != \$n) {push @starts, \$n if \$v > 0; \$v = 0;} for (\$i = 0; \$i < \$\#starts; \$i++) {print "\$starts[\$i]\t\$starts[\$i+1]\n";}}' | awk '{n="!{prop,,project}_variant_subset_"NR; print n" class project_variant_subset\n"n" disp Variant subset "NR"\n"n" parent !{prop,,project}\n"n" var_start "\$1"\n"n" var_end "\$2"\n"n" var_subset_num "NR}' > !{output,,project_project_variant_subset_meta_file} class_level project run_if num_var_subsets with project_variant_subset

annot_variant_subset_name=!{prop,,project}_!{prop,,annot,max=1}_variant_subset

local cmd make_annot_annot_variant_subset_meta_file=egrep "class project_variant_subset$" !{input,,project_project_variant_subset_meta_file} | sed 's/^\(!{prop,,project}_variant_subset_\(\S\S*\)\).*/\1 ${annot_variant_subset_name}_\2 \2/' | sed 's/^\(\S\S*\)\s\s*\(\S\S*\)\s\s*\(\S\S*\)/\2 class annot_variant_subset\n!select:!{prop,,project} \2 parent !{prop,,annot}\n\2 disp !{prop,,annot,disp} variant subset \3\n\2 consistent \1\n\2 var_subset_num \3/' > !{output,,annot_annot_variant_subset_meta_file} class_level annot run_if and,num_var_subsets,!annot_genes,!annot_manual_gene_list_file,!annot_manual_gene_variant_list_file with annot_variant_subset

local cmd make_annot_empty_annot_variant_subset_meta_file=rm -f !{output,,annot_annot_variant_subset_meta_file} && touch !{output,,annot_annot_variant_subset_meta_file} class_level annot skip_if and,num_var_subsets,!annot_genes,!annot_manual_gene_list_file,!annot_manual_gene_variant_list_file with annot_variant_subset

meta_table cmd make_dummy_project_project_variant_subset_pheno_expand_subset_file=!{prop,,project_variant_subset}\t!{prop,,project_variant_subset,expand_pheno_subsets,missing=1} !{output,project_project_variant_subset_pheno_expand_subset_file} class_level project with pheno skip_if mean_expand_pheno_subsets

local cmd make_project_project_variant_subset_pheno_expand_subset_file=cat !{input,,project_variant_subset_gene_burden_size_file} | perl -ne 'BEGIN {\$t=0; \$n = 0; @lines = ();} chomp; @F = split("\t"); \$t += \$F[1]; \$n++; push @lines, \$_; END {\$m = \$t / \$n; foreach (@lines) {@F = split("\t"); \$num = \$F[1] / \$m; \$num = 1 if (\$num < 1);  print "\$F[0]\t" . int(!{prop,,project,mean_expand_pheno_subsets} * \$num + .5) . "\n"}}' | $smart_cut_cmd --in-delim $tab --exec "perl -e 'print \"!{raw,,project_variant_subset,@project_variant_subset\t@expand_pheno_subsets,if_prop=expand_pheno_subsets,allow_empty=1}\"' | sed 's/ /\n/g' | awk 'NF > 1'" | awk -F"\t" -v OFS="\t" '!m[\$1] {print; m[\$1]=1}' > !{output,,project_project_variant_subset_pheno_expand_subset_file} class_level project run_if mean_expand_pheno_subsets with pheno

marker_subset_meta_file_helper=egrep "class project_@{1}_subset$" !{input,,project_project_@{1}_subset_meta_file} | sed 's/^\(!{prop,,project}_@{1}_subset_\(\S\S*\)\).*/\1 !{prop,,@{2}}_@{1}_subset_\2 \2/' | sed 's/^\(\S\S*\)\s\s*\(\S\S*\)\s\s*\(\S\S*\)/\2 class @{2}_@{1}_subset\n\2 parent \1\n\2 disp !{prop,,@{2},disp} @{1} subset \3\n\2 sort \3\n\2 consistent !{prop,,@{2}}\n!{prop,,@{2}} consistent \2@3/' > !{output,,@{2}_@{2}_@{1}_subset_meta_file}

!!expand:,:stype,sabbrv,extra:sample,samp,:variant,var,\n\2 var_subset_num \3! \
local cmd make_marker_marker_stype_subset_meta_file=$marker_subset_meta_file_helper(stype,marker,extra) class_level marker run_if num_sabbrv_subsets

local cmd make_project_project_merge_subset_meta_file=perl -ne 'BEGIN {use POSIX "ceil"; \$n=0} \$n++; END {\$num_per = ceil(\$n / !{prop,,project,num_merge_subsets}); for (\$i = 0; \$i < !{prop,,project,num_merge_subsets}; \$i++) {\$b = \$i * \$num_per + 1; \$e = \$b + \$num_per; print "\$b\t\$e\n"}}' < !{input,,project_variant_site_interval_list_file} | awk '{n="!{prop,,project}_merge_subset_"NR; print n" class project_merge_subset\n"n" disp Merge subset "NR"\n"n" parent !{prop,,project}\n"n" merge_start "\$1"\n"n" merge_end "\$2"\n"n" sort "NR}' > !{output,,project_project_merge_subset_meta_file} class_level project run_if num_merge_subsets with project_merge_subset

#Command to subset the VCF and clean VCF
local cmd make_project_sample_subset_samp_keep_file=cut -f1 !{input,,project_sample_subset_plinkseq_ind_info_file} > !{output,,project_sample_subset_samp_keep_file} class_level project_sample_subset

local cmd make_project_sample_subset_samp_exclude_file=cut -f1 !{input,,project_plinkseq_ind_info_file} !{input,,project_sample_subset_samp_keep_file} !{input,,project_sample_subset_samp_keep_file} | sort | uniq -u > !{output,,project_sample_subset_samp_exclude_file} class_level project_sample_subset

local cmd make_project_sample_subset_clean_samp_keep_file=$smart_cut_cmd --ignore-status $broken_pipe_status --exec "zcat !{input,,project_sample_subset_clean_vcf_file} | $vcf_utils_cmd --print-samps" > !{output,,project_sample_subset_clean_samp_keep_file} class_level project_sample_subset

short cmd make_project_variant_subset_var_keep_file=$get_subset_vars | awk -v OFS="\t" -F "\t" 'NR >= !{prop,,project_variant_subset,var_start} && NR < !{prop,,project_variant_subset,var_end} {print \$2,\$3}' > !{output,,project_variant_subset_var_keep_file} class_level project_variant_subset

short cmd make_project_variant_subset_interval_list_file=cat !{input,,project_variant_subset_var_keep_file} | awk 'c && \$1 != c {print c":"s"-"e; s=\$2} !c {s=\$2} {c=\$1;e=\$2} END {print c":"s"-"e}' > !{output,,project_variant_subset_interval_list_file} class_level project_variant_subset

#!!expand:;:vtype;runif:;:_indel;:_clean;run_if or,sample_qc_filter,var_qc_filter:_clean_indel;run_if or,sample_qc_filter,var_qc_filter! \
#short cmd make_project_variant_subsetvtype_vcf_file=rm -f !{output,,project_variant_subsetvtype_vcf_file} && $zcat_helper(project) !{input,,projectvtype_vcf_file} | $vcf_utils_cmd !{input,--chr-pos-keep,project_variant_subset_var_keep_file} $bgzip_helper(project) > !{output,,project_variant_subsetvtype_vcf_file} $tabix_helper(project,project_variant_subsetvtype_vcf_file) class_level project_variant_subset runif skip_if and,fast_split,zip_vcf:eq:.gz

one_vcf_header=awk 'BEGIN {infile=0} !/^\\#/ || !infile {print} !/^\\#\\#/ {infile=1}'

query_merge_subsets_helper=cat !{input,,@1,is_list=1} | awk '{print \$NF}' | xargs $zcat_helper(project) | $one_vcf_header

!!expand:;:vtype;runif:;:_indel;bsub_batch 5:_clean;run_if or,sample_qc_filter,var_qc_filter,project_variant_custom_exclude_file,project_sample_custom_exclude_file:_clean_indel;run_if or,sample_qc_filter,var_qc_filter,project_variant_custom_exclude_file,project_sample_custom_exclude_file bsub_batch 5:_multiallelic;bsub_batch 5:_clean_multiallelic;run_if or,sample_qc_filter,var_qc_filter,project_variant_custom_exclude_file,project_sample_custom_exclude_file bsub_batch 5! \
short cmd make_project_variant_subsetvtype_vcf_file=rm -f !{output,,project_variant_subsetvtype_vcf_file} && $query_merge_subsets_helper(project_variant_subset_project_merge_subsetvtype_vcf_list_file) | $vcf_utils_cmd !{input,--chr-pos-keep,project_variant_subset_var_keep_file} $bgzip_helper(project) > !{output,,project_variant_subsetvtype_vcf_file} $tabix_helper(project,project_variant_subsetvtype_vcf_file) class_level project_variant_subset runif 

#short cmd make_project_variant_subset_clean_all_vcf_file=rm -f !{output,,project_variant_subset_clean_all_vcf_file} && zcat !{input,,project_clean_all_vcf_file} | $vcf_utils_cmd !{input,--chr-pos-keep,project_variant_subset_var_keep_file} | $bgzip_cmd > !{output,,project_variant_subset_clean_all_vcf_file} && !{raw,,project_variant_subset,$run_tabix_cmd(project_variant_subset_clean_all_vcf_file)} class_level project_variant_subset skip_if fast_split

#!!expand:;:vtype;runif:;:_indel;:_clean;run_if or,sample_qc_filter,var_qc_filter:_clean_indel;run_if or,sample_qc_filter,var_qc_filter! \
#short cmd make_fast_project_variant_subsetvtype_vcf_file=($zcat_helper(project) !{input,,projectvtype_vcf_file} | $vcf_utils_cmd --print-header; cat !{input,,project_variant_subset_interval_list_file} | while read f; do $tabix_cmd !{input,,projectvtype_vcf_file} \$f; done) $bgzip_helper(project) > !{output,,project_variant_subsetvtype_vcf_file} $tabix_helper(project,project_variant_subsetvtype_vcf_file) class_level project_variant_subset runif skip_if or,!fast_split,zip_vcf:ne:.gz

#short cmd make_fast_project_variant_subset_clean_all_vcf_file=(zcat !{input,,project_clean_all_vcf_file} | $vcf_utils_cmd --print-header; cat !{input,,project_variant_subset_interval_list_file} | while read f; do $tabix_cmd !{input,,project_clean_all_vcf_file} \$f; done) | $bgzip_cmd > !{output,,project_variant_subset_clean_all_vcf_file} && !{raw,,project_variant_subset,$run_tabix_cmd(project_variant_subset_clean_all_vcf_file)} class_level project_variant_subset skip_if !fast_split

prop fast_split=scalar default 1

!!expand:;:vtype;runif:;:_indel;bsub_batch 5:_clean;run_if or,sample_qc_filter,var_qc_filter,project_variant_custom_exclude_file,project_sample_custom_exclude_file:_clean_indel;run_if sample_qc_filter bsub_batch 5:_multiallelic;bsub_batch 5:_clean_multiallelic;run_if sample_qc_filter bsub_batch 5! \
short cmd make_project_sample_subsetvtype_vcf_file=rm -f !{output,,project_sample_subsetvtype_vcf_file} && $query_merge_subsets_helper(project_project_merge_subsetvtype_vcf_list_file) | $vcf_utils_cmd !{input,--samp-keep,project_sample_subset_samp_keep_file} $bgzip_helper(project) > !{output,,project_sample_subsetvtype_vcf_file} $tabix_helper(project,project_sample_subsetvtype_vcf_file) class_level project_sample_subset runif skip_if fast_split

#!!expand:;:vtype;runif:;:_indel;:_clean;run_if or,sample_qc_filter,var_qc_filter:_clean_indel;run_if sample_qc_filter! \
#short cmd make_project_sample_subsetvtype_vcf_file=rm -f !{output,,project_sample_subsetvtype_vcf_file} && $zcat_helper(project) !{input,,projectvtype_vcf_file} | $vcf_utils_cmd !{input,--samp-keep,project_sample_subset_samp_keep_file} $bgzip_helper(project) > !{output,,project_sample_subsetvtype_vcf_file} $tabix_helper(project,project_sample_subsetvtype_vcf_file) class_level project_sample_subset runif skip_if fast_split

!!expand:;:vtype;runif:;:_indel;:_clean;run_if or,sample_qc_filter,var_qc_filter,project_variant_custom_exclude_file,project_sample_custom_exclude_file:_clean_indel;run_if sample_qc_filter:_multiallelic;:_clean_multiallelic;run_if sample_qc_filter! \
short cmd make_fast_project_sample_subsetvtype_vcf_file=$query_merge_subsets_helper(project_project_merge_subsetvtype_vcf_list_file) | cut -f1-9,$((9+!{prop,,project_sample_subset,samp_start}))-$((8+!{prop,,project_sample_subset,samp_end})) $bgzip_helper(project) > !{output,,project_sample_subsetvtype_vcf_file} $tabix_helper(project,project_sample_subsetvtype_vcf_file) class_level project_sample_subset runif skip_if !fast_split

#!!expand:;:vtype;runif:;:_indel;:_clean;run_if or,sample_qc_filter,var_qc_filter:_clean_indel;run_if sample_qc_filter! \
#short cmd make_fast_project_sample_subsetvtype_vcf_file=$zcat_helper(project) !{input,,projectvtype_vcf_file} | cut -f1-9,$((9+!{prop,,project_sample_subset,samp_start}))-$((8+!{prop,,project_sample_subset,samp_end})) $bgzip_helper(project) > !{output,,project_sample_subsetvtype_vcf_file} $tabix_helper(project,project_sample_subsetvtype_vcf_file) class_level project_sample_subset runif skip_if !fast_split


!!expand:;:ctype;itype;skipif:clean;;skip_if or,sample_qc_filter,var_qc_filter,project_variant_custom_exclude_file,project_sample_custom_exclude_file:clean_indel;indel_;skip_if or,sample_qc_filter,var_qc_filter,project_variant_custom_exclude_file,project_sample_custom_exclude_file:clean_multiallelic;multiallelic_;skip_if or,sample_qc_filter,var_qc_filter,project_variant_custom_exclude_file,project_sample_custom_exclude_file! \
!!expand:stype:variant:sample! \
short cmd ln_project_stype_subset_ctype_vcf_file=rm -f !{output,,project_stype_subset_ctype_vcf_file} && ln -s !{input,,project_stype_subset_itypevcf_file} !{output,,project_stype_subset_ctype_vcf_file} $tabix_helper(project,project_stype_subset_ctype_vcf_file) class_level project_stype_subset skipif

#!|expand:,:projectt,expost:project,skip_if num_merge_subsets:project_merge_subset,skip_if !num_merge_subsets| \
#local cmd make_projectt_indel_vcf_file=$zcat_helper(projectt) !{input,,projectt_subsetted_indel_vcf_file} | $vcf_utils_cmd $vcf_utils_processing_flags \
#	 !{input,--reference-vcf,project_indel_site_vcf_file} --valid-id \
#	 $bgzip_helper(projectt) > !{output,,projectt_indel_vcf_file} $tabix_helper(projectt,projectt_indel_vcf_file) class_level projectt expost

#USE THIS COMMAND ONCE YOU UPGRADE THE GATK
#cmd make_project_samtools_clean_vcf_file=$select_variants_cmd(project_samtools_vcf_file,/dev/stdout,project_sample_include_file,OFF) | $vcf_utils_cmd --remove-filtered  > !{output,,project_samtools_clean_vcf_file} class_level project
#REMOVE THIS COMMAND ONCE YOU UPGRADE THE GATK
#cmd make_project_samtools_clean_vcf_file=$vcf_utils_cmd !{input,--samp-keep,project_sample_include_file} --remove-mono  --remove-filtered < !{input,,project_samtools_vcf_file} > !{output,,project_samtools_clean_vcf_file} class_level project

prop remove_filtered_indels=scalar default 1
prop remove_filtered_multiallelic_snps=scalar default 1
prop remove_filtered_multiallelic_indels=scalar default 1

!|expand:;:ctype;itype;extracmd;runif:clean;;--remove-filtered;run_if or,sample_qc_filter,var_qc_filter,project_variant_custom_exclude_file,project_sample_custom_exclude_file rusage_mod $clean_vcf_mem:clean_indel;indel_;!{raw,,project,--remove-filtered,if_prop=remove_filtered_indels,allow_empty=1};run_if or,sample_qc_filter,var_qc_filter,project_variant_custom_exclude_file,project_sample_custom_exclude_file bsub_batch 5:clean_multiallelic;multiallelic_;!{raw,,project,--remove-filtered-snps,if_prop=remove_filtered_multiallelic_snps,allow_empty=1} !{raw,,project,--remove-filtered-non-snps,if_prop=remove_filtered_multiallelic_indels,allow_empty=1};run_if or,sample_qc_filter,project_variant_custom_exclude_file,project_sample_custom_exclude_file bsub_batch 5| \
!|expand:,:shortt,projectt,expost,svtype:,project,skip_if num_merge_subsets,select_variants_cmd:short,project_merge_subset,skip_if !num_merge_subsets,project_merge_subset_select_variants_cmd| \
shortt cmd make_projectt_ctype_vcf_file=rm -f !{output,,projectt_ctype_vcf_file} && $svtype(projectt_itypevcf_file,/dev/stdout,project_sample_include_file,OFF) | $vcf_utils_cmd extracmd !{input,--var-excl,project_variant_exclude_file} --append-missing-malformatted-format $bgzip_helper(projectt) > !{output,,projectt_ctype_vcf_file} $tabix_helper(projectt,projectt_ctype_vcf_file) class_level projectt expost runif

!!expand:project:project:project_merge_subset! \
!!expand:;:ctype;itype;skipif:clean;;skip_if or,sample_qc_filter,var_qc_filter,project_variant_custom_exclude_file,project_sample_custom_exclude_file:clean_indel;indel_;skip_if or,sample_qc_filter,var_qc_filter,project_variant_custom_exclude_file,project_sample_custom_exclude_file:clean_multiallelic;multiallelic_;skip_if or,sample_qc_filter,var_qc_filter,project_variant_custom_exclude_file,project_sample_custom_exclude_file! \
short cmd ln_project_ctype_vcf_file=rm -f !{output,,project_ctype_vcf_file} && ln -s !{input,,project_itypevcf_file} !{output,,project_ctype_vcf_file} $tabix_helper(project,project_ctype_vcf_file) class_level project skipif

project_rem_chr_helper=rem_chr=`tail -n1 !{input,,project_variant_site_vcf_file} | cut -f1 | awk '{if (\$1 !~ "chr") {print "sed \"s/^chr//\""} else {print "cat"}}'`

#!|expand:;:shortt;projectl;projecte;runif:;project;;run_if and,!num_var_subsets,!num_merge_subsets:short;project_variant_subset;_project_variant_subset;run_if num_var_subsets| \
#short cmd make_projectl_clean_all_vcf_file=$projectl_rem_chr_helper && $pseq_qc_plus_no_mask_analysis_cmdprojecte(write-vcf) | eval \$rem_chr | $bgzip_cmd > !{output,,projectl_clean_all_vcf_file} && !{raw,,projectl,$run_tabix_cmd(projectl_clean_all_vcf_file)} class_level projectl runif

!!expand:,:projectl,exskipif:project,skip_if num_merge_subsets:project_merge_subset,:project_variant_subset,! \
short cmd make_projectl_clean_all_vcf_file=$project_rem_chr_helper && $gatk_cmd(CombineVariants) -l OFF -o /dev/stdout !{raw,,projectl,--variant:snps *projectl_clean_vcf_file} !{raw,,projectl,--variant:indels *projectl_clean_indel_vcf_file} !{raw,,projectl,--variant:multiallelics *projectl_clean_multiallelic_vcf_file} !{input,projectl_clean_vcf_file} !{input,projectl_clean_indel_vcf_file} !{input,projectl_clean_multiallelic_vcf_file} $require_unique_helper -filteredRecordsMergeType KEEP_IF_ANY_UNFILTERED -priority snps,indels,multiallelics  | eval \$rem_chr | $bgzip_cmd > !{output,,projectl_clean_all_vcf_file} && !{raw,,projectl,$run_tabix_cmd(projectl_clean_all_vcf_file)} class_level projectl exskipif

outside_gene_name=Outside

clean_gene_variant_id_col=4


!!expand:clean_:clean_:! \
clean_gene_variant_helper=$smart_join_cmd --exec "$smart_cut_cmd --in-delim $tab --exec \"$zcat_helper(@1) !{input,,@{1}_clean_vcf_file}\" --exec \"$zcat_helper(@1) !{input,,@{1}_clean_indel_vcf_file}\" --exec \"$zcat_helper(@1) !{input,,@{1}_clean_multiallelic_vcf_file}\" --comment '\\#\\#' --strip-comments --select-col 1-3,1,'\\#CHROM' --select-col 1-3,1,'POS ID' --exact --require-col-match --exclude-row 2-3,1 | sed '1 s/^\\#//' | $smart_cut_cmd --in-delim $tab --select-col 0,1,'ID CHROM POS' --exclude-row 0,1" !{input,--file,project_variant_gene_annotation_file} --fill 2 --fill-value $outside_gene_name --extra 2 | awk -v OFS="\t" -F"\t" 'BEGIN {print "GENE","CHROM","POS","ID"} {print \$4,\$2,\$3,\$1}' > !{output,,@{1}_clean_gene_variant_file} 

!!expand:clean_:clean_:! \
cmd make_project_clean_gene_variant_file=$clean_gene_variant_helper(project) class_level project skip_if num_var_subsets

!!expand:clean_:clean_:! \
short cmd make_project_variant_subset_clean_gene_variant_file=$clean_gene_variant_helper(project_variant_subset) class_level project_variant_subset 

#Current size: maximum number of variants in any gene
local cmd make_project_variant_subset_gene_burden_size_file=tail -n+2 !{input,,project_variant_subset_gene_variant_file} | cut -f1 | sort | uniq -c | sort -nrk1 | awk 'NR == 1 {print \$1}' | sed 's/^/!{prop,,project_variant_subset}\t/' > !{output,,project_variant_subset_gene_burden_size_file} class_level project_variant_subset 

#plinkseq_vcf_file_helper=perl $targeted_bin_dir/vcf_utils.pl --prep-for-plinkseq < !{input,,@1} > !{output,,@2}

#local cmd make_project_plinkseq_vcf_file=$plinkseq_vcf_file_helper(project_vcf_file,project_plinkseq_vcf_file) class_level project

#local cmd make_project_clean_plinkseq_vcf_file=$plinkseq_vcf_file_helper(project_clean_vcf_file,project_clean_plinkseq_vcf_file) class_level project

two_fold_degenerate_name=2-fold-degenerate
four_fold_degenerate_name=4-fold-degenerate
non_degenerate_name=Non-degenerate


project_vcf_select=#-select "degeneracy==1" -selectName "$non_degenerate_name" -select "degeneracy==3 || degeneracy==2" -selectName "$two_fold_degenerate_name" -select "degeneracy==4" -selectName "$four_fold_degenerate_name"
vcf_eval_heap=$gatk_heap
#cmd make_project_vcf_eval_file=$gatk_cmd_dbsnp_no_heap(VariantEval,$vcf_eval_heap) -l OFF !{output,-o,project_vcf_eval_file} !{input,--eval,project_vcf_file} -EV ThetaVariantEvaluator $project_vcf_select  class_level project run_with project_sample_subset,project_variant_subset

#local cmd make_project_theta_eval_file=grep ThetaVariantEvaluator !{input,,project_vcf_eval_file} | grep -v "\\#" > !{output,,project_theta_eval_file} class_level project

num_per_theta_ac_bin=100
num_per_titv_ac_bin=100

#!!expand:qctype:qc_pass! \
#cmd make_project_qctype_theta_ac_dat_file=$smart_cut_cmd --in-delim $tab !{input,--file,project_plinkseq_qc_pass_counts_file} --select-col 1,1,COUNT --exclude-row 1,1 --exact --require-col-match | sort | uniq -c | awk -v OFS="\t" '{print \$2,\$1}' | sort -nk1 | perl -ane 'BEGIN {\$target_length = $perl_get_target_length; \$num_ac = 0; \$sum_recip = 0; \$sum_ac = 0; \$sum_counts = 0; \$last_ac = 0} \$cur_ac = \$F[0]; \$cur_counts = \$F[1]; for (\$i = \$last_ac + 1; \$i <= \$cur_ac; \$i++) { \$num_ac++; \$sum_recip += 1 / \$i; \$sum_ac += \$i } \$sum_counts += \$cur_counts; \$last_ac = \$cur_ac; if (\$sum_counts > $num_per_theta_ac_bin) {print \$sum_ac / \$num_ac; print "\t"; print \$sum_counts; print "\t"; print \$sum_counts / \$sum_recip / \$target_length; print "\n"; \$num_ac = 0; \$sum_recip = 0; \$sum_ac = 0; \$sum_counts = 0;}' > !{output,,project_qctype_theta_ac_dat_file} class_level project

#!!expand:qctype:qc_pass! \
#local cmd make_project_qctype_theta_ac_pdf_file=$draw_line_plot_cmd !{input,,project_qctype_theta_ac_dat_file} !{output,,project_qctype_theta_ac_pdf_file} 'Theta by Allele Count' 'Allele Count' 'Estimate of Theta' 1 3 header=FALSE x.log=TRUE class_level project

#$draw_line_plot_cmd !{input,,variant_impute_type_threshold_vassoc_file} !{output,,variant_impute_type_threshold_vassoc_pdf_file} '!{prop,,pheno,disp} association score for !{prop,,variant,disp} --- impute_disp imputation' 'Posterior Threshold' 'Assoc P-value' THRESHOLD 'P' sep=$tab y2.col=F,F_CASE,F_CONTROL y2.label=Frequency hline=0.05 hline.name='P .05' hline2=$variant_maf_helper hline2.name='Seq MAF' x.log=T y2.log=T plot.width=$threshold_vassoc_plot_width plot.height=$threshold_vassoc_plot_height class_level variant


local cmd make_project_titv_eval_file=grep TiTvVariantEvaluator !{input,,project_vcf_eval_file} | grep -v "\\#" > !{output,,project_titv_eval_file} class_level project

#local cmd make_project_titv_ac_file=$eval_parser_cmd --analysis-name "SimpleMetricsByAC" --table-name metrics < !{input,,project_vcf_eval_file} | $smart_cut_cmd --in-delim , --select-col 0,1,"AC ^n$ ^nTi$ ^nTv$" --select-row 0,1,JexlExpression,none --select-row 0,1,Novelty,all --select-row 0,1 --and-row 0  | cut -d, -f2- | tail -n+2 | sort -t, -nk 1 | sed '1 s;^;AC,n,nTi,nTv\n;' > !{output,,project_titv_ac_file} class_level project

local cmd make_project_titv_ac_binned_file=$bin_values_cmd --in-delim , --sum-col nTi --sum-col nTv --num-col n --header 1 --min-per-bin $num_per_titv_ac_bin < !{input,,project_titv_ac_file} | $add_function_cmd --in-delim , --header 1 --col1 nTi --col2 nTv --type divide --val-header Ti/Tv | $smart_cut_cmd --in-delim , --select-col 0,1,AC --select-col 0,1,Ti/Tv  > !{output,,project_titv_ac_binned_file} class_level project
local cmd make_project_titv_ac_pdf_file=$draw_line_plot_cmd !{input,,project_titv_ac_binned_file} !{output,,project_titv_ac_pdf_file} 'Ti/Tv by Allele Count' 'Allele Count' 'Ti/Tv' 1 2 sep=, x.log=TRUE class_level project

prop snpeff_regs=list

snpsift_exe_helper=java -Xmx$snpeff_heap -jar $snpsift_jar 

snpeff_exe_helper=java -Xmx$snpeff_heap -jar $snpeff_jar eff -v -noStats -c $snpeff_config	-o @1 !{prop,-reg,project,snpeff_regs,if_prop=snpeff_regs,allow_empty=1} $snpeff_genome

combine_vcfs_for_annotation=$smart_cut_cmd --in-delim $tab --exec @2"$zcat_helper(@1) !{input,,@{1}_vcf_file}@2" --exec @2"$zcat_helper(@1) !{input,,@{1}_indel_vcf_file} | awk '@2"\!@2"/\#/'@2" --exec @2"$zcat_helper(@1) !{input,,@{1}_multiallelic_vcf_file} | awk '@2"\!@2"/\#/'@2" --comment '\#\#' 

#snpeff_dbnsfp_cols=Ensembl_transcriptid Uniprot_acc Interpro_domain SIFT_score Polyphen2_HVAR_pred GERP++_NR GERP++_RS 29way_logOdds 1000Gp1_AF 1000Gp1_AFR_AF 1000Gp1_EUR_AF 1000Gp1_AMR_AF 1000Gp1_ASN_AF ESP5400_AA_AF ESP5400_EA_AF


#take only the first occurrence
#right now, only duplicates are SPLICE annotations
#so put those first

!|expand:,:shortt,projectt,projecte,runif:,project,,run_if !num_var_subsets:short,project_variant_subset,_project_variant_subset,| \
shortt cmd make_projectt_snpsift_file=cols=`zcat $snpeff_dbnsfp | head -n1 | cut -f${dbnsfp_cols_start}- | sed 's/\t/\n/g' | egrep -v '$dbnsfp_cols_ignore' | tr '\n' ' '`; \
  $combine_vcfs_for_annotation(projectt,) | cut -f1-8 \
  | $snpsift_exe_helper dbnsfp -v $snpeff_dbnsfp -f `echo \$cols | sed 's/\s\s*/,/g'` - \
"""
}
    