
package loamstream.compiler

/**
 * LoamStream
 * Created by oliverr on 10/6/2015.
 */
object PipelineTargetedPart01 {
  val string =
 """bgl_to_ped_cmd=$germline_dir/parser/bgl_to_ped
germline_cmd=$germline_dir/germline-1-4-2/germline

igv_jar=$lib_dir/igv/IGV_2.0.9/igv.jar
haploview_jar=$lib_dir/haploview/Haploview4.1/Haploview.jar

beagle_jar=$lib_dir/beagle/beagle.jar
nsamples=1
omitprefix=true
beagle_mem=2g
beagle_missing=0

snpeff_dir=$lib_dir/snpEff/snpEff
snpeff_jar=$snpeff_dir/snpEff.jar
snpsift_dir=$lib_dir/snpEff/snpEff
snpsift_jar=$snpsift_dir/SnpSift.jar
snpeff_config=$snpeff_dir/snpEff.config

chaos_cmd=perl $lib_dir/chaos/chaos_hg$hg_build/xchaos
map_effects_cmd=perl $lib_dir/annot/annot.v2/bin/mapCat2.pl

vep_dir=$lib_dir/ensembl/variant_effect_predictor
vep_plugin_dir=$vep_dir/VEP_plugins
vep_condel_config_dir=$vep_plugin_dir/config/Condel/config
loftee_human_ancestor_path=$vep_plugin_dir/loftee-master/human_ancestor.fa.rz
ensembl_cache_dir=$lib_dir/ensembl/cache

vep_cmd=perl $vep_dir/variant_effect_predictor.pl

vep_custom_tabix=$conservation_dir/29way.omega.v2.allchr.bed.gz $conservation_dir/gerp.allchr.bed.gz $conservation_dir/phyloP.allchr.bed.gz $conservation_dir/1kg.20101123.snps_indels_sv.sites.bed.gz
vep_custom_names=29_mammals_omega GERP_UCSC_RS PhyloP 1000G

vep_custom_tabix=$conservation_dir/29way.omega.v2.allchr.bed.gz $conservation_dir/gerp.allchr.bed.gz
vep_custom_names=29_mammals_omega GERP_UCSC_RS

#local_plinkseq_home=$lib_dir/pseq
#nalt=NMIN
nalt=NALT

open_vnc_display=export DISPLAY=`python $common_bin_dir/open_vncdisplay.py`
close_vnc_display=out=\$?; vncserver -kill \$DISPLAY; exit \$out

plink_cmd=plink --noweb
plink2_cmd=$lib_dir/plink/plink2
plink108_cmd=$lib_dir/plink/plink108 --noweb
plink_mds_cmd=$lib_dir/plink/plink-1.07-src/plink --noweb
plink_mv_log_cmd=sleep 1 && mv @1.log @2
plinkseq_exome_browser_url=http://plinkseq.broadinstitute.org/cgi-bin/pbrowse.cgi

#ASSOCIATION TESTS
##=====================

#There are 4 steps to define association tests for a phenotype
#1. At pheno level, define whether to cluster and compute MDS and which traits should be used as covariates
#2. At the burden level, define the class of variants (using annotations)
#3. At the pheno test level, define the tests to run and give them properties
#4. At the burden test level, define the tests to run and give them properties


##TEST INPUTS
##============

##For stratification control
prop strata_traits=list #traits to use to stratify samples (for permutations and for clustering). Must override or set run_raw to turn off at test level 
prop covar_traits=list #potential traits to use as covariates. Must override or set run_raw to turn off at test level 

prop num_mds_calc=scalar default 10 #number of MDS values to compute
prop num_mds_plot=scalar default 4 #number of MDS values to plot
prop extra_traits=list #include additional traits in pheno file

prop run_cluster=scalar default 0 #must set to 1 if want any tests to use clustering


#for inclusion in clean annot --- display of top single variant hits
#Variants with differential missingness lower than this p-value will be removed
#i.e. .01
prop min_clean_p_missing=scalar
#Variants with call rates below this fraction will be removed
#i.e. .9
prop min_clean_geno=scalar
#Variants with HWE p-values below this number will be removed
#i.e. 1e-6
prop min_clean_hwe=scalar


##ANNOTATIONS
##====================

#the following annotation must appear as a column in the master annotation file
vcf_type_annot=Consequence
vcf_protein_change_annot=Protein_change
#Type must also have values representing these three classes
vcf_type_synonymous_annot=synonymous_variant
vcf_type_missense_annot=missense_variant
vcf_type_nonsense_annot=stop_gained

#These are added to vassoc annot file
#prop vassoc_meta_annot=list default "Codons SNPEFF_Effect SIFT_PRED SIFT_SCORE PolyPhen_PRED PolyPhen_SCORE Condel_PRED Condel_SCORE SNPEFF_dbNSFP_aapos_SIFT	SNPEFF_dbNSFP_SIFT_score	SNPEFF_dbNSFP_SIFT_converted_rankscore	SNPEFF_dbNSFP_SIFT_pred	SNPEFF_dbNSFP_Polyphen2_HDIV_score	SNPEFF_dbNSFP_Polyphen2_HDIV_rankscore	SNPEFF_dbNSFP_Polyphen2_HDIV_pred	SNPEFF_dbNSFP_Polyphen2_HVAR_score	SNPEFF_dbNSFP_Polyphen2_HVAR_rankscore	SNPEFF_dbNSFP_Polyphen2_HVAR_pred	SNPEFF_dbNSFP_LRT_score	SNPEFF_dbNSFP_LRT_converted_rankscore	SNPEFF_dbNSFP_LRT_pred	SNPEFF_dbNSFP_MutationTaster_score	SNPEFF_dbNSFP_MutationTaster_converted_rankscore	SNPEFF_dbNSFP_MutationTaster_pred	SNPEFF_dbNSFP_MutationAssessor_score	SNPEFF_dbNSFP_MutationAssessor_rankscore	SNPEFF_dbNSFP_MutationAssessor_pred	SNPEFF_dbNSFP_FATHMM_score	SNPEFF_dbNSFP_FATHMM_rankscore	SNPEFF_dbNSFP_FATHMM_pred	SNPEFF_dbNSFP_RadialSVM_score	SNPEFF_dbNSFP_RadialSVM_rankscore	SNPEFF_dbNSFP_RadialSVM_pred	SNPEFF_dbNSFP_LR_score	SNPEFF_dbNSFP_LR_rankscore	SNPEFF_dbNSFP_LR_pred	SNPEFF_dbNSFP_Reliability_index	SNPEFF_dbNSFP_CADD_raw_rankscore	SNPEFF_dbNSFP_CADD_raw	SNPEFF_dbNSFP_CADD_phred	SNPEFF_dbNSFP_phyloP46way_primate_rankscore	SNPEFF_dbNSFP_phyloP46way_primate	SNPEFF_dbNSFP_phyloP46way_placental	SNPEFF_dbNSFP_phyloP46way_placental_rankscore	SNPEFF_dbNSFP_phyloP100way_vertebrate	SNPEFF_dbNSFP_phyloP100way_vertebrate_rankscore	SNPEFF_dbNSFP_phastCons46way_primate	SNPEFF_dbNSFP_phastCons46way_primate_rankscore	SNPEFF_dbNSFP_phastCons46way_placental_rankscore	SNPEFF_dbNSFP_phastCons46way_placental	SNPEFF_dbNSFP_phastCons100way_vertebrate	SNPEFF_dbNSFP_phastCons100way_vertebrate_rankscore	SNPEFF_dbNSFP_SiPhy_29way_pi	SNPEFF_dbNSFP_SiPhy_29way_logOdds_rankscore	SNPEFF_dbNSFP_SiPhy_29way_logOdds	SNPEFF_dbNSFP_LRT_Omega	SNPEFF_dbNSFP_UniSNP_ids	SNPEFF_dbNSFP_1000Gp1_AC	SNPEFF_dbNSFP_1000Gp1_AFR_AF	SNPEFF_dbNSFP_1000Gp1_AFR_AC	SNPEFF_dbNSFP_1000Gp1_AF	SNPEFF_dbNSFP_1000Gp1_EUR_AC	SNPEFF_dbNSFP_1000Gp1_EUR_AF	SNPEFF_dbNSFP_1000Gp1_AMR_AC	SNPEFF_dbNSFP_1000Gp1_AMR_AF	SNPEFF_dbNSFP_1000Gp1_ASN_AC	SNPEFF_dbNSFP_1000Gp1_ASN_AF	SNPEFF_dbNSFP_ESP6500_AA_AF	SNPEFF_dbNSFP_ESP6500_EA_AF"
prop vassoc_meta_annot=list default "Codons SNPEFF_Effect SIFT_PRED SIFT_SCORE PolyPhen_PRED PolyPhen_SCORE Condel_PRED Condel_SCORE "
#These are the fields in the display tables
prop vassoc_meta_disp=list default "$vcf_type_annot $vcf_protein_change_annot SIFT_PRED PolyPhen_PRED Condel_PRED"
#Must be same length as vassoc_meta_disp; the values to insert for headers
prop vassoc_meta_headers=list default "Type Change SIFT PPH Condel"

#original entries in cells to change
prop old_disp_annots=list
#new entries in cells to change; must be same length as old disp annots
prop new_disp annots=list

#to specify the annotation mask for a burden test, you can specify an OR of AND clauses
#each element in the list is an AND clause; the clauses are ORed together
#elements in the AND clause should be separated by $burden_and_delim
#each element should first have the value to filter on, and then the filter (separated by $burden_filter_delim)
#lists (multiple OR clauses) can be specified on one line by separating clauses by a , within {}
#otherwise, multiple lines can be used (must be done when adding an AND clause unless $burden_and_delim is changed
select_and_delim=&
select_filter_delim=;
prop annot_mask=list

prop annot_genes=list #only consider variants from these genes
prop only_for_interesting=list #run this burden only for interesting genes

prop use_raw_variants=scalar #use raw variants rather than clean variants

#-----
#Example masks
#-----
#nonsynonymous or nonsense or readthrough
#!key vcf_readthrough_annot STOP_LOST
#burden_ns burden_annot_mask {$vcf_type_annot;eq:$vcf_missense_annot,$vcf_type_annot;eq:$vcf_nonsense_annot,$vcf_type_annot;eq:$vcf_readthrough_annot}
#loss of function
#burden_lof burden_annot_mask {SNPEFF_Effect;eq:SPLICE_SITE_DONOR,SNPEFF_Effect;eq:SPLICE_SITE_ACCEPTOR,$vcf_type_annot;eq:$vcf_nonsense_annot,$vcf_type_annot;eq:$vcf_readthrough_annot}
#severe
#burden_severe burden_annot_mask {SNPEFF_Effect;eq:SPLICE_SITE_DONOR,SNPEFF_Effect;eq:SPLICE_SITE_ACCEPTOR,$vcf_type_annot;eq:$vcf_nonsense_annot,$vcf_type_annot;eq:$vcf_readthrough_annot}
#burden_severe burden_annot_mask PolyPhen_PRED;probably damaging,SIFT_PRED;damaging
#-----


#-----
#example burden test entries in meta file
#-----
#{type_annot,missense_annot,synonymous_annot,nonsense_annot,readthrough_annot,snpeff_effect_annot,splice_acceptor_annot,splice_donor_annot,protein_change_annot,pph_annot,sift_annot,pph2_annot,condel_annot,complex_indel_annot,frameshift_annot} prop scalar
#project {type_annot,missense_annot,synonymous_annot,nonsense_annot,readthrough_annot,snpeff_effect_annot,splice_acceptor_annot,splice_donor_annot,protein_change_annot,pph_annot,sift_annot,pph2_annot,condel_annot,complex_indel_annot,frameshift_annot} {Consequence,NON_SYNONYMOUS_CODING,SYNONYMOUS_CODING,STOP_GAINED,STOP_LOST,SNPEFF_Effect,SPLICE_SITE_ACCEPTOR,SPLICE_SITE_DONOR,Protein_change,PolyPhen_PRED,SIFT_PRED,PPH2_hdiv_class,Condel_SCORE,COMPLEX_IN/DEL,FRAMESHIFT_CODING}
#project key_as_prop annot_missing_field

#macros to use for masks
#!macro :coding_mask: @protein_change_annot;ne:@annot_missing_field
#!macro :syn_mask: @type_annot;eq:@synonymous_annot

#setting up an internal macro
#!macro :lof_maski: @snpeff_effect_annot;eq:@splice_donor_annot,@snpeff_effect_annot;eq:@splice_acceptor_annot,@type_annot;eq:@nonsense_annot,@type_annot;eq:@readthrough_annot,@type_annot;eq:@complex_indel_annot,@type_annot;eq:@frameshift_annot

#expands all ',' in :lof_maski:, so treated as multiple entries (list) with OR semantics
#!macro :lof_mask: {:lof_maski:}

#lof OR missense
#!macro :ns_mask: {:lof_maski:,@type_annot;eq:@missense_annot}

#all polyphen damaging
#!macro :pph_mask: @pph_annot;damaging

#not in {}, so does not expand and recognized as a sigle entry, with ',' indicating AND
#!macro :pph_and_sift_mask: @pph_annot;damaging,@sift_annot;deleterious

#in {}, so expands and recognized as two entries, indicating OR
#!macro :pph_or_sift_mask: {@pph_annot;damaging,@sift_annot;deleterious}

#example masks

#coding
#burden_coding burden_annot_mask :coding_mask:

#loss of function
#burden_lof burden_annot_mask :lof_mask:

#example pph and sift: first entry has AND semantics, then OR with second (lof). Then cap MAF < 1%
#burden_pph_and_sift_1pct burden_annot_mask :pph_and_sift_mask:
#burden_pph_and_sift_1pct burden_annot_mask :lof_mask:
#burden_pph_and_sift_1pct burden_maf .01

#When create interesting variants, add this annotation
col_for_var_annot=PolyPhen_SCORE
disp_for_var_annot=PPH

#for annot: apply extended qc of variants (defined by is_extended_filter)
prop apply_extended_qc=scalar
#for annot: apply extended strict qc of variants (defined by is_extended_strict_filter)
prop apply_extended_strict_qc=scalar

#for annot: union multiple annots to get this one
prop union_annots=list

#MAF/MAC filters (applied on top of any annotation filters)

#at level of annot (project wide)
prop annot_maf=scalar #calculated averaged over all samples
prop annot_mac_lb=scalar
prop annot_mac_ub=scalar
prop annot_strat_maf_summary=scalar #could be mean,max,min,geometric,harmonic,contraharmonic


#for burden: union multiple burdens to get this one
prop union_burdens=list

#at level of burden (for non-missing phenotype data)
prop burden_maf=scalar #calculated averaged over all samples
prop burden_mac_lb=scalar
prop burden_mac_ub=scalar
prop burden_strat_maf_summary=scalar #could be mean,max,min,geometric,harmonic,contraharmonic


#to aid display
prop no_var_filter=scalar #dont display a table with top variants
prop no_var_qq=scalar #dont display a QQ plot
prop no_gene_qq=scalar #dont display a gene QQ plot
prop no_gene_burden=scalar #dont create a gene burden

prop max_associated_maf=scalar #maximum MAF to display variants in table of top hits
prop min_pheno_mac=scalar default 3 #minimum minor allele count to appear on slide of top associated (or QQ plot)



##ASSOCIATION TESTS
##====================

prop strat_cluster=scalar #use clusters to correct for stratification
prop strata_traits=list #stratify by traits to correct for stratification
prop strat_covar=scalar default 1 #use PCs to correct for stratification
prop num_mds_covar=scalar default 10 #num MDS to use for strat control
prop covar_traits=list #include traits as covariates to correct for stratification (if dont want these and defined at pheno level, make sure to override)
prop alternate_pheno=list #swap in alternate phenotype to run
prop extra_covar_traits=list #include extra traits as available for covariates specific to this burden test. Does not work for PLINK/SEQ. Must also make sure to specify in covar_traits the specific covariates to include.
prop manual_covar_traits=list #hard code these in as covariates.
prop run_raw=scalar #dont correct for stratification (overrides everything)
prop include_related=scalar #include related samples

prop test_tag=scalar #what to display as a tag in master join files
prop test_software=scalar #FOR SV: either pseq or epacts or plink or R #FOR BURDEN: either pseq or epacts
prop test_name=list #passed to software with the test flag
prop test_options=list #passed to software as additional test options

prop use_for_interesting=scalar default 1 #use this test statistic to define interesting genes or variants
prop use_for_display=scalar default 1 #display this test in summary tables


#EPACTS specific
prop epacts_gt_field=scalar default $thresholded_nalt_field #which field to use fo epacts (PL, GT, DOS also acceptable)

#PSEQ specific
#how to handle missing data in burden tests
#If true, permute phenotypes only of samples with non-missing genotypes
#If have a lot of missing data, setting true will kill power
#If have differential missingness, setting false will produce huge artifacts
#i.e. 0
prop fix_null=scalar 

#MANTRA/METAL SPECIFIC
#the pheno_test id to meta-analyze
prop meta_test=list
#flag if the meta test has no subsets (e.g. if it is METAL or has been changed to have no subsets)
prop meta_test_no_subsets=scalar

prop metal_scheme=scalar default STDERR
#gc correct metal input files
prop gc_correct=scalar 
#minimum mac for meta-analysis
prop min_mantra_mac=scalar default 0


##SV TEST SPECIFIC
##====================

#what the P value column is in the small annot file produced
#prop p_col=scalar default P
#what the OR column is in the small annot file produced
#prop or_col=scalar
#what should be displayed for the OR column
#prop or_disp=scalar default OR

prop use_for_or=scalar default 1 #treat output as valid odds ratio

#these will be available in the small vassoc file (if they can be produced from the test)
id_col_disp=ID
n_col_disp=N
neff_col_disp=N_EFF
ncase_col_disp=N_CASE
ncontrol_col_disp=N_CTRL
maf_col_disp=MAF
p_col_disp=P
or_col_disp=OR
dir_col_disp=DIR
bf_col_disp=LOG_BF
beta_col_disp=BETA
zscore_col_disp=ZSCORE
se_col_disp=SE
ref_col_disp=REF
alt_col_disp=ALT


##BURDEN TEST SPECIFIC
"""
}
    