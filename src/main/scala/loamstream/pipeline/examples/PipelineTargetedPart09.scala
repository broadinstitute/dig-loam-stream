
package loamstream.pipeline.examples

/**
 * LoamStream
 * Created by oliverr on 10/6/2015.
 */
object PipelineTargetedPart09 {
  val string =
 """minor file path pheno_ucsc_tracks_file=@pheno.ucsc.tracks dir pheno_dir disp ".ucsc.tracks" parent cat_pheno_trait_info class_level pheno comment "Track information for UCSC screenshot fetching"

meta_table file path pheno_pheno_variant_qc_strata_meta_file=@pheno.pheno_variant_qc_strata.meta dir pheno_dir disp ".pheno_variant_qc_strata.meta" parent cat_pheno_subset_info class_level pheno comment "Meta file to load in pheno_variant_qc_strata instances" meta_level pheno_variant_qc_strata

meta_table file path pheno_pheno_variant_qc_pheno_strata_meta_file=@pheno.pheno_variant_qc_pheno_strata.meta dir pheno_dir disp ".pheno_variant_qc_pheno_strata.meta" parent cat_pheno_subset_info class_level pheno comment "Meta file to load in pheno_variant_qc_pheno instances" meta_level pheno_variant_qc_pheno_strata

meta_table file path pheno_pheno_variant_subset_meta_file=@pheno.pheno_variant_subset.meta dir pheno_dir disp ".pheno_variant_subset.meta" parent cat_pheno_subset_info class_level pheno comment "Meta file to load in pheno_variant_subsets" meta_level pheno_variant_subset

meta_table file path pheno_pheno_sample_subset_meta_file=@pheno.pheno_sample_subset.meta dir pheno_dir disp ".pheno_sample_subset.meta" parent cat_pheno_subset_info class_level pheno comment "Meta file to load in pheno_sample_subsets" meta_level pheno_sample_subset

meta_table file path pheno_pheno_test_variant_subset_meta_file=@pheno.pheno_test_variant_subset.meta dir pheno_dir disp ".pheno_test_variant_subset.meta" parent cat_pheno_subset_info class_level pheno comment "Meta file to load in pheno_test_variant_subsets" meta_level pheno_test_variant_subset

file path pheno_variant_subset_region_file=@pheno_variant_subset.regions dir pheno_variant_subset_dir disp ".regions" parent cat_pheno_variant_subset_meta_data class_level pheno_variant_subset comment "Regions in gtf file to use for this subset" 

!!expand:keeptype:var_chr_pos:var:chr_pos! \
file path pheno_variant_subset_keeptype_keep_file=@pheno_variant_subset.keeptype.keep dir pheno_variant_subset_dir disp ".keeptype.keep" parent cat_pheno_variant_subset_meta_data class_level pheno_variant_subset comment "keeptype to keep in pheno_variant_subset" 

!!expand:,:pheno,category:pheno,cat_pheno_subset_info:pheno_variant_subset,cat_pheno_variant_subset_meta_data! \
table nohead file path pheno_gene_to_variant_subset_map=@pheno.gene.to.variant_subset.map dir pheno_dir disp ".gene.to.variant_subset.map" parent category class_level pheno comment "Map of pheno variant subset for each gene"

!!expand:pheno_test:pheno_test:pheno_test_variant_subset! \
mkdir path pheno_test_epacts_dir=$pheno_test_dir/epacts class_level pheno_test chmod 777

!!expand:pheno_test:pheno_test:pheno_test_variant_subset! \
table path file pheno_test_epacts_ready_file=@pheno_test.epacts.ready dir pheno_test_epacts_dir disp ".epacts.ready"  class_level pheno_test comment "Dummy file to indicate ready to run epacts"

!!expand:pheno:pheno:pheno_variant_subset! \
table path file pheno_vassoc_counts_file=@pheno.vassoc.counts dir pheno_dir disp ".vassoc.counts" parent cat_pheno_variant_assoc_data class_level pheno comment "Count information for vassoc file"

table path file pheno_test_info_file=@pheno_test.info dir pheno_test_dir disp ".info" parent cat_pheno_test_data class_level pheno_test comment "Information on test run"

onecol table path file pheno_test_sample_include_file=@pheno_test.sample.include dir pheno_test_dir disp "sample.include" parent cat_pheno_test_data class_level pheno_test comment "Specify special list of samples to include (intersected with popgen+)"

table path file pheno_test_sample_include_aux_file=@pheno_test.sample.include.aux dir pheno_test_dir disp "sample.include.aux" parent cat_pheno_test_data class_level pheno_test comment "Auxiliary file used to implement pheno_test_sample_include_file; could be either samples to include or exclude, depending on the software used."

table path file pheno_test_alternate_pheno_file=@pheno_test.alternate.pheno dir pheno_test_dir disp "alternate.pheno" parent cat_pheno_test_data class_level pheno_test comment "Specify alternate list of phenotypes"

table path file pheno_test_extra_covars_file=@pheno_test.extra.covars dir pheno_test_dir disp "extra.covars" parent cat_pheno_test_data class_level pheno_test comment "Specify special list of extra covariates (intersected with popgen+)"

table path file pheno_test_variable_covars_file=@pheno_test.variable.covars dir pheno_test_dir disp "variable.covars" parent cat_pheno_test_data class_level pheno_test comment "Extra covars that are variable"

table path file pheno_test_covars_aux_file=@pheno_test.covars.aux dir pheno_test_dir disp ".covars.aux" parent cat_pheno_test_data class_level pheno_test comment "Auxiliary file used to implement pheno_test_extra_covars_file; could be whatever format accepted by the testing software."

!!expand:pheno_test:pheno_test:pheno_test_variant_subset! \
!!expand:num:1:2:3:4! \
path file pheno_test_in_auxnum_file=@pheno_test.in.auxnum dir pheno_test_dir disp ".in.auxnum" parent cat_pheno_test_data class_level pheno_test comment "Auxilary input file num for this test"

!!expand:pheno_test:pheno_test:pheno_test_variant_subset! \
!!expand:num:1! \
path file pheno_test_out_auxnum_file=@pheno_test.out.auxnum dir pheno_test_dir disp ".out.auxnum" parent cat_pheno_test_data class_level pheno_test comment "Auxilary output file num for this test"

!!expand:pheno_test:pheno_test:pheno_test_variant_subset! \
doubcom table path file pheno_test_vassoc_file=@pheno_test.vassoc dir pheno_test_dir disp ".vassoc" parent cat_pheno_test_data class_level pheno_test comment "Single variant associations"

!!expand:pheno_test:pheno_test:pheno_test_variant_subset! \
table path file pheno_test_small_vassoc_file=@pheno_test.small.vassoc dir pheno_test_dir disp ".small.vassoc" parent cat_pheno_test_data class_level pheno_test comment "Single variant association; cleaned and with reduced set of columns for joining"

table path file pheno_test_clean_include_file=@pheno_test.clean.include dir pheno_test_dir disp ".clean.include" parent cat_pheno_test_data class_level pheno_test comment "Variants to include to make pheno_test_clean_small_vassoc_file"

path file pheno_test_lambda_file=@pheno_test.lambda dir pheno_test_dir disp ".lambda" parent cat_pheno_test_data class_level pheno_test comment "Lambda value to apply for GC correction"

!!expand:pheno_test:pheno_test:pheno_test_variant_subset! \
table path file pheno_test_clean_small_vassoc_file=@pheno_test.clean.small.vassoc dir pheno_test_dir disp ".clean.small.vassoc" parent cat_pheno_test_data class_level pheno_test comment "Vassoc files with variants excluded and optional GC correction applied"

#!!expand:pheno_test:pheno_test:pheno_test_variant_subset! \
#table path file pheno_test_small_full_vassoc_file=@pheno_test.small.full.vassoc dir pheno_test_dir disp ".small.full.vassoc" parent cat_pheno_test_data class_level pheno_test comment "Small vassoc file filled with NA for variants not output by test for some reason"

!!expand:pheno_test:pheno_test:pheno_test_variant_subset! \
pheno_test_vassoc_epacts_trunk=@pheno_test.epacts dir pheno_test_epacts_dir

!!expand:pheno_test:pheno_test:pheno_test_variant_subset! \
pheno_test_vassoc_plink_trunk=@pheno_test.plink dir pheno_test_dir

file path doubcom table pheno_variant_subset_gene_variant_file=@pheno_variant_subset.gene.variant.tsv dir pheno_variant_subset_dir disp ".gene.variant.tsv" parent cat_pheno_variant_subset_variant_assoc_data class_level pheno_variant_subset comment "List of gene, variant for each variant in vcf file --- link from project_variant_subset"

file path doubcom table pheno_variant_subset_clean_gene_variant_file=@pheno_variant_subset.clean.gene.variant.tsv dir pheno_variant_subset_dir disp ".clean.gene.variant.tsv" parent cat_pheno_variant_subset_variant_assoc_data class_level pheno_variant_subset comment "List of gene, variant for each variant in clean vcf file --- link from project_variant_subset"

!!expand:pheno:pheno:pheno_variant_subset! \
major table path file pheno_vassoc_pre_annot_file=@pheno.vassoc.pre.annot dir pheno_dir disp ".vassoc.pre.annot" parent cat_pheno_variant_assoc_data class_level pheno comment "Annotations for variant level associations"

!!expand:pheno:pheno:pheno_variant_subset! \
major table path file pheno_vassoc_annot_file=@pheno.vassoc.annot dir pheno_dir disp ".vassoc.annot" parent cat_pheno_variant_assoc_data class_level pheno comment "Variant level associations dumped by pseq --- with additional annotations"

table path file pheno_vassoc_clean_include_file=@pheno.vassoc.clean.include dir pheno_dir disp ".vassoc.clean.include" parent cat_pheno_variant_assoc_data class_level pheno comment "Variants that pass clean vassoc filters"

major table path file pheno_vassoc_clean_annot_file=@pheno.vassoc.clean.annot dir pheno_dir disp ".vassoc.clean.annot" parent cat_pheno_variant_assoc_data class_level pheno comment "Associations after filters applied"

major table path file pheno_small_vassoc_annot_file=@pheno.small.vassoc.annot dir pheno_dir disp ".small.vassoc.annot" parent cat_pheno_variant_assoc_data class_level pheno comment "Vassoc file with smaller set of columns"

!!expand:ctype:all:meta! \
table path file pheno_ctype_trait_vassoc_annot_file=@pheno.ctype_trait.vassoc.annot dir pheno_dir disp ".vassoc.ctype_trait.annot" parent cat_pheno_variant_assoc_data class_level pheno comment "Basic association data for this trait and ctype related traits"

#!!expand:type:all:ns:nondb:pph! \
#major path file pheno_type_vassoc_qq_pdf_file=@pheno.type.vassoc.pdf dir pheno_dir disp ".type.vassoc.pdf" parent cat_pheno_variant_assoc_data class_level pheno comment "Variant level associations dumped by pseq --- qq-plot for type variants"

!!expand:impute_type:hap:traditional! \
path file pheno_imputation_impute_type_gene_list_file=@pheno.impute_type.gene.list dir pheno_dir disp ".impute_type.gene.list" parent cat_pheno_variant_imputation_data class_level pheno comment "List of files that will be merged to make pheno_imputation_impute_type_summary_file"

path file pheno_snptest_locus_list_file=@pheno.snptest.locus.list dir pheno_dir disp ".snptest.locus.list" parent cat_pheno_variant_imputation_data class_level pheno comment "List of files that will be merged to make pheno_snptest_summary_file"


!!expand:impute_type:hap:traditional! \
major table path file pheno_imputation_impute_type_summary_file=@pheno.impute_type.summary dir pheno_dir disp ".impute_type.summary" parent cat_pheno_variant_imputation_data class_level pheno comment "Summary of imputation of all interesting variants for this phenotype --- impute_type imputation"

major table path file pheno_snptest_summary_file=@pheno.snptest.summary dir pheno_dir disp ".snptest.summary" parent cat_pheno_variant_imputation_data class_level pheno comment "Summary of snptests for all marker variants  for this phenotype --- traditional phenotype"

#file path pheno_haplotype_burden_input_list_file=@pheno.hap.burden.input.list dir pheno_dir disp ".hap.burden.input.list" parent cat_pheno_haplotype_burden_data class_level pheno comment "List of files to go into haplotype burden analysis" skip_if not_trait

#major table path file pheno_dirty_vassoc_file=@pheno.dirty.vassoc dir pheno_dir disp ".vassoc" parent cat_pheno_dirty_all_variant_assoc_data class_level pheno comment "Variant level associations dumped by pseq --- no samples or variants excluded"

#major path file pheno_dirty_vassoc_qq_pdf_file=@pheno.dirty.vassoc.qq.pdf dir pheno_dir disp ".vassoc.pdf" parent cat_pheno_dirty_all_variant_assoc_data class_level pheno comment "Variant level associations dumped by pseq --- no samples or variants excluded --- qq-plot"

#major table path file pheno_dirty_qc_pass_vassoc_file=@pheno.dirty.qc_pass.vassoc dir pheno_dir disp ".vassoc" parent cat_pheno_dirty_qc_pass_variant_assoc_data class_level pheno comment "Variant level associations dumped by pseq --- no samples or variants excluded but sites excluded"

#major path file pheno_dirty_qc_pass_vassoc_qq_pdf_file=@pheno.dirty.qc_pass.vassoc.qq.pdf dir pheno_dir disp ".vassoc.pdf" parent cat_pheno_dirty_qc_pass_variant_assoc_data class_level pheno comment "Variant level associations dumped by pseq --- no samples or variants excluded but sites excluded --- qq-plot"


#!!expand:,:lname,ldescrip:1,initial:2,most associated initial:3,most associated level1! \
#!!expand:,:type,tname:all,all:ns,nonsynonymous:nondb,nondb:pph,pph deleterious! major table path file pheno_type_levellname_gassoc_file=@pheno.type.levellname.gassoc dir pheno_dir disp ".type.levellname.gassoc" parent cat_pheno_gene_assoc_data class_level pheno comment "Gene level associations dumped by pseq for ldescrip: for tname variants"

table path file pheno_gassoc_file=@pheno.gassoc dir pheno_dir disp ".gassoc" parent cat_pheno_gene_assoc_data class_level pheno comment "Combination of all gene level associations from burden level extradescription"

major table path file pheno_flat_gassoc_file=@pheno.flat.gassoc dir pheno_dir disp ".flat.gassoc" parent cat_pheno_gene_assoc_data class_level pheno comment "Combination of all gene level associations from burden level --- with one column for each different test"

!!expand:pathwayburdentype:custom! \
doubcom major table path file pheno_pathway_pathwayburdentype_gassoc_file=@pheno.pathway.pathwayburdentype.gassoc dir pheno_dir disp ".pathway.pathwayburdentype.gassoc" parent cat_pheno_pathway_assoc_data class_level pheno comment "Combination of all pathwayburdentype pathway level associations from burden level"

#burden files

minor file path burden_slide_master_ps_file=@burden.slide.master.ps dir burden_dir disp ".ps" parent cat_burden_slide_master_data class_level burden comment "Master join file of all relevant slides --- ps file"
major file path burden_slide_master_pdf_file=@burden.slide.master.pdf dir burden_dir disp ".pdf" parent cat_burden_slide_master_data class_level burden comment "Master join file of all relevant slides --- pdf file"

!!expand:,:ext,majmin:tex,minor:pdf,minor! \
majmin file path burden_title_slide_ext_file=@burden.title.ext dir burden_dir disp "title.ext" trunk @burden.title parent cat_burden_slide_master_data class_level burden comment "Master title slide --- ext file"

!!expand:ext:tex:pdf! \
file path burden_burden_test_info_ext_file=@burden.burden_test.info.ext dir burden_dir disp "burden_test.info.ext" trunk @burden.burden_test.info parent cat_burden_info_data class_level burden comment "Information on which tests were run"

#!!expand:,:lname,ldescrip:1,initial:2,most associated initial:3,most associated level1! \
#major table path file burden_levellname_gassoc_file=@burden.levellname.gassoc dir burden_dir disp ".levellname.gassoc" parent cat_burden_association_data class_level burden comment "Gene level associations dumped by pseq for ldescrip"

#!!expand:lname:2:3! \
#table nohead onecol path file burden_levellname_gene_list_file=@burden.levellname.gene.list dir burden_dir disp ".levellname.gene.list" parent cat_burden_association_data class_level burden comment "Genes to be run at level levellname"

!!expand:burden_test:burden_test:burden_test_variant_subset! \
mkdir path burden_test_epacts_dir=$burden_test_dir/epacts class_level burden_test chmod 777

!!expand:burden_test:burden_test:burden_test_variant_subset! \
table path file burden_test_epacts_ready_file=@burden_test.epacts.ready dir burden_test_epacts_dir disp ".epacts.ready"  class_level burden_test comment "Dummy file to indicate ready to run epacts"

!!expand:direction:case_side:control_side! \
!!expand:burden_test:burden_test:burden_test_variant_subset! \
burden_test_direction_gassoc_epacts_trunk=@burden_test.direction.epacts dir burden_test_epacts_dir

table path file burden_test_info_file=@burden_test.info dir burden_test_dir disp ".info" parent cat_burden_test_info_data class_level burden_test comment "Information on test run"

onecol table path file burden_test_sample_include_file=@burden_test.sample.include dir burden_test_dir disp "sample.include" parent cat_burden_test_samp_data class_level burden_test comment "Specify special list of samples to include (intersected with popgen+)"

table path file burden_test_sample_include_aux_file=@burden_test.sample.include.aux dir burden_test_dir disp "sample.include.aux" parent cat_burden_test_samp_data class_level burden_test comment "Auxiliary file used to implement burden_test_sample_include_file; could be either samples to include or exclude, depending on the software used."

table path file burden_test_alternate_pheno_file=@burden_test.alternate.pheno dir burden_test_dir disp "alternate.pheno" parent cat_burden_test_samp_data class_level burden_test comment "Specify special alternate phenotypes"

table path file burden_test_extra_covars_file=@burden_test.extra.covars dir burden_test_dir disp "extra.covars" parent cat_burden_test_samp_data class_level burden_test comment "Specify special list of extra covariates (intersected with popgen+)"

table path file burden_test_variable_covars_file=@burden_test.variable.covars dir burden_test_dir disp "variable.covars" parent cat_burden_test_samp_data class_level burden_test comment "Extra covars that are variable"

table path file burden_test_covars_aux_file=@burden_test.covars.aux dir burden_test_dir disp ".covars.aux" parent cat_burden_test_samp_data class_level burden_test comment "Auxiliary file used to implement burden_test_extra_covars_file; could be whatever format accepted by the testing software."

!!expand:burden_test:burden_test:burden_test_variant_subset! \
!!expand:num:1:2! \
path file burden_test_in_auxnum_file=@burden_test.in.auxnum dir burden_test_dir disp ".in.auxnum" parent cat_burden_test_input_association_data class_level burden_test comment "Auxilary input file num for this test"

!!expand:burden_test:burden_test:burden_test_variant_subset! \
!!expand:num:1! \
path file burden_test_out_auxnum_file=@burden_test.out.auxnum dir burden_test_dir disp ".out.auxnum" parent cat_burden_test_association_data class_level burden_test comment "Auxilary output file num for this test"

table path file burden_chr_pos_exclude_file=@burden.chr.pos.exclude dir burden_dir disp "chr.pos.exclude" parent cat_burden_var_data class_level burden comment "Specify two columns (chr, pos) to exclude from burden tests"

!!expand:burden_test:burden_test:burden_test_variant_subset! \
!!expand:direction:case_side:control_side! \
doubcom major table path file burden_test_direction_gassoc_file=@burden_test.direction.gassoc dir burden_test_dir disp ".direction.gassoc" parent cat_burden_test_association_data class_level burden_test comment "Gene level associations; for direction" 

!!expand:burden:burden:burden_variant_subset! \
major table path file burden_gassoc_counts_data_file=@burden.gassoc.counts.data dir burden_dir disp ".gassoc.counts.data" parent cat_burden_association_display_data class_level burden comment "Raw associations; input to display variant counts in tables" 

major table path file burden_gassoc_counts_file=@burden.gassoc.counts dir burden_dir disp ".gassoc.counts" parent cat_burden_association_display_data class_level burden comment "Gene level associations; for displaying variant counts in tables" 

major table path file burden_gassoc_file=@burden.gassoc dir burden_dir disp ".gassoc" parent cat_burden_association_test_data class_level burden comment "Gene level associations; aggregate of all tests" 

!!expand:burden_test:burden_test:burden_test_variant_subset! \
doubcom major table path file burden_test_gassoc_file=@burden_test.gassoc dir burden_test_dir disp ".gassoc" parent cat_burden_test_association_data class_level burden_test comment "Gene level associations" 

major table path file burden_test_small_gassoc_file=@burden_test.small.gassoc dir burden_test_dir disp ".small.gassoc" parent cat_burden_test_association_data class_level burden_test comment "Gene level associations with essential columns and uniform format across software" 

major table path file burden_gassoc_file=@burden.gassoc dir burden_dir disp ".gassoc" parent cat_burden_association_test_data class_level burden comment "Gene level associations" 

#!!expand:burden:burden:burden_variant_subset! \
#table path file burden_qt_gassoc_file=@burden.qt.gassoc dir burden_dir disp ".qt.gassoc" parent cat_burden_association_data class_level burden comment "Gene level associations for full distribution of QT (if applicable)" 

#!!expand:burden:burden:burden_variant_subset! \
#path file burden_qt_gassoc_qq_file=@burden.qt.gassoc.qq.pdf dir burden_dir disp ".qt.gassoc.qq.pdf" parent cat_burden_association_data class_level burden comment "QQ plot Gene level associations for full distribution of QT (if applicable)" 

major table path file burden_flat_gassoc_file=@burden.flat.gassoc dir burden_dir disp ".flat.gassoc" parent cat_burden_association_test_data class_level burden comment "Gene level associations --- gassoc file flattened to have one line per gene and different columns for different tests"

nohead onecol table path file burden_interesting_variant_genes_dat_file=@burden.interesting_variant_genes.dat dir burden_dir disp ".interesting_variant_genes.dat" parent cat_burden_interesting_data class_level burden comment "Interesting genes according to variants filtered by this burden test"

nohead onecol table path file burden_interesting_genes_dat_file=@burden.interesting_genes.dat dir burden_dir disp ".interesting_genes.dat" parent cat_burden_interesting_data class_level burden comment "Interesting genes according to this burden test"

meta_table file path burden_interesting_gene_burdens_meta_file=@burden.interesting_gene_burdens.meta dir burden_dir disp ".interesting_gene_burdens.meta" parent cat_burden_interesting_data class_level burden comment "Meta file to specify interesting gene burdens for this burden" meta_level gene_burden

nohead onecol table path file burden_interesting_gene_variants_dat_file=@burden.interesting_gene_variants.dat dir burden_dir disp ".interesting_gene_variants.dat" parent cat_burden_interesting_data class_level burden comment "Interesting variants within interesting genes"

nohead onecol table path file burden_all_interesting_gene_variants_dat_file=@burden.all.interesting_gene_variants.dat dir burden_dir disp ".all.interesting_gene_variants.dat" parent cat_burden_interesting_data class_level burden comment "All variants within interesting genes"

nohead table path file burden_gene_sort_values_file=@burden.gene.sort.values dir burden_dir disp ".gene.sort.values" parent cat_burden_interesting_data class_level burden comment "Sort values for each gene based on this burden level"
minor path file annot_manual_variant_list_file=@annot.manual.variant.list dir annot_dir disp ".manual.variant.list" parent cat_annot_var_data class_level annot comment "List of variants within this annot grouping --- can specify this by hand for custom set"

minor path file annot_manual_gene_list_file=@annot.manual.gene.list dir annot_dir disp ".manual.gene.list" parent cat_annot_var_data class_level annot comment "List of genes within this annot grouping --- can specify this by hand for custom set"

minor path file annot_manual_gene_variant_list_file=@annot.manual.gene.variant.list dir annot_dir disp ".manual.gene.variant.list" parent cat_annot_var_data class_level annot comment "List of genes within this annot grouping --- can specify this by hand for custom set"

!!expand:annotl:annot:annot_variant_subset! \
path file annotl_annot_variant_list_file=@annotl.annot.variant.list dir annotl_dir disp ".annot.variant.list" parent cat_annotl_var_data class_level annotl comment "List of variants within this annot grouping, according to annotations specified at the gene level"

path file burden_only_interesting_variant_list_file=@burden.only_interesting.variant.list dir burden_dir disp ".only_interesting.variant.list" parent cat_burden_var_data class_level burden comment "List of variants in interesting genes; includes ALL variants"

path file annot_variant_exclude_detail_file=@annot.variant.exclude.detail dir annot_dir disp ".variant.exclude.detail" parent cat_annot_var_data class_level annot comment "Extra variants to exclude with filters; with metrics"

path file annot_variant_exclude_file=@annot.variant.exclude dir annot_dir disp ".variant.exclude" parent cat_annot_var_data class_level annot comment "Extra variants to exclude with filters"

!!expand:burden:burden:burden_variant_subset:annot:annot_variant_subset! \
path file burden_non_annot_variant_list_file=@burden.non_annot.variant.list dir burden_dir disp ".non_annot.variant.list" parent cat_burden_var_data class_level burden comment "List of variants within this burden grouping, according to additional freq/etc. thresholds that are not annotation based"

!!expand:burden:burden:burden_variant_subset:annot:annot_variant_subset! \
path file burden_all_variant_list_file=@burden.all.variant.list dir burden_dir disp ".all.variant.list" parent cat_burden_var_data class_level burden comment "List of variants within this burden grouping. Annotations are at the gene level"

!!expand:burden:burden:burden_variant_subset! \
path file burden_idex_file=@burden.id.ex dir burden_dir disp ".id.ex" parent cat_burden_var_data class_level burden comment "List of variants to exclude from this burden"

!!expand:burden:burden:burden_variant_subset! \
path file burden_regex_file=@burden.reg.ex dir burden_dir disp ".reg.ex" parent cat_burden_var_data class_level burden comment "List of variants to exclude from this burden"

!!expand:annot:annot:annot_variant_subset! \
path file annot_locdb_detail_reg_file=@annot.locdb.detail.reg dir annot_dir disp ".locdb.detail.reg" parent cat_annot_var_data class_level annot comment "Details about grouping of variants by gene/transcript"

!!expand:burden:burden:burden_variant_subset! \
path file burden_locdb_detail_reg_file=@burden.locdb.detail.reg dir burden_dir disp ".locdb.detail.reg" parent cat_burden_var_data class_level burden comment "Details about grouping of variants by gene/transcript"

!!expand:burden:burden:burden_variant_subset! \
path file burden_locdb_reg_file=@burden.locdb.reg dir burden_dir disp ".locdb.reg" parent cat_burden_var_data class_level burden comment "Input file to load into locdb --- annotations at both the gene and transcript level"

!!expand:burden:burden:burden_variant_subset! \
path file burden_reg_file=@burden.reg dir burden_dir disp ".reg" parent cat_burden_var_data class_level burden comment "Input file to load into locdb --- annotations at both the gene and transcript level"

!!expand:burden:burden:burden_variant_subset! \
path file burden_locdb_file=@burden.locdb dir burden_dir disp ".locdb" parent cat_burden_var_data class_level burden comment "Custom locdb for this mask"

!!expand:burden:annot:annot_variant_subset:burden:burden_variant_subset! \
path file burden_setid_file=@burden.SetID dir burden_dir disp ".SetID" parent cat_burden_var_data class_level burden comment "SetID file for meta-skat"

!!expand:burden:annot:annot_variant_subset:burden:burden_variant_subset! \
path file burden_epacts_group_file=@burden.epacts.group dir burden_dir disp ".epacts.group" parent cat_burden_var_data class_level burden comment "Group file for EPACTs"

!!expand:pathwayburdentype:custom! \
file path burden_pathway_pathwayburdentype_locdb_file=pathway.pathwayburdentype.locdb dir burden_dir disp "pathway.pathwayburdentype.locdb" parent cat_burden_pathway_association_data class_level burden comment "Plink/Seq locdb file for use in pathway burden testing --- for pathwayburdentype locsets"

!!expand:pathwayburdentype:custom! \
burden_test_pathway_pathwayburdentype_gassoc_epacts_trunk=@burden_test.pathway.pathwayburdentype.epacts dir burden_test_epacts_dir

!!expand:burden:burden:annot! \
!!expand:pathwayburdentype:custom! \
file path burden_pathway_pathwayburdentype_epacts_group_file=pathway.pathwayburdentype.epacts.group dir burden_dir disp "pathway.pathwayburdentype.epacts.group" parent cat_burden_pathway_association_data class_level burden comment "EPACTS group file for use in pathway burden testing --- for pathwayburdentype locsets"

path file burden_clean_variant_list_file=@burden.clean.variant.list dir burden_dir disp ".clean.variant.list" parent cat_burden_var_data class_level burden comment "List of variants within this burden grouping (annotations at the gene level) --- includes only those that pass clean vassoc"

#!!expand:burden:burden:burden_variant_subset! \
#table file path burden_score_seq_map_file=@burden.score_seq.map dir burden_dir disp ".score_seq.map" parent cat_burden_var_data class_level burden comment "Map file for SCORE-Seq"

!!expand:freqtype:common:uncommon:rare! \
major path file burden_freqtype_vassoc_qq_pdf_file=@burden.freqtype.vassoc.qq.pdf dir burden_dir disp ".freqtype.vassoc.qq.pdf" parent cat_burden_summary_data class_level burden comment "Variant level associations dumped by pseq for this burden grouping --- qq-plot of freqtype variants"

major path file burden_gassoc_qq_pdf_file=@burden.gassoc.qq.pdf dir burden_dir disp ".gassoc.qq.pdf" parent cat_burden_summary_data class_level burden comment "Gene level associations dumped by pseq for this burden grouping --- qq-plot"

burden_slide_common_vassoc_trunk=@burden.slide.common.vassoc

!!expand:,:filetype,weblevel:tex,minor:pdf,major! \
weblevel file path burden_slide_common_vassoc_filetype_file=$burden_slide_common_vassoc_trunk.filetype trunk burden_slide_common_vassoc_trunk dir burden_dir disp ".slide.common.vassoc.filetype" parent cat_burden_summary_data class_level burden comment "List of top associated variants under this burden grouping --- filetype file"

burden_slide_vassoc_trunk=@burden.slide.vassoc

!!expand:,:filetype,weblevel:tex,minor:pdf,major! \
weblevel file path burden_slide_vassoc_filetype_file=$burden_slide_vassoc_trunk.filetype trunk burden_slide_vassoc_trunk dir burden_dir disp ".slide.vassoc.filetype" parent cat_burden_summary_data class_level burden comment "List of top associated variants under this burden grouping --- filetype file"

burden_slide_unique_trunk=@burden.slide.unique

!!expand:,:filetype,weblevel:tex,minor:pdf,major! \
weblevel file path burden_slide_unique_filetype_file=$burden_slide_unique_trunk.filetype trunk burden_slide_unique_trunk dir burden_dir disp ".slide.unique.filetype" parent cat_burden_summary_data class_level burden comment "List of top associated variants under this burden grouping --- filetype file"


burden_slide_common_vassoc_meta_trait_trunk=@burden.slide.common.vassoc.meta_trait
"""
}
    