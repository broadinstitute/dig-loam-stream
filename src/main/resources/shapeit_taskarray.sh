#!/bin/bash
#$ -cwd
#$ -j y

source /broad/software/scripts/useuse
reuse -q UGER

base_dir=/humgen/diabetes/users/kyuksel/imputation/shapeit_example

shapeit_exe=/humgen/diabetes/users/ryank/software/shapeit/bin/shapeit

i=$SGE_TASK_ID

$shapeit_exe \
-V $base_dir/gwas.vcf_copy_$i.gz \
-M $base_dir/genetic_map.txt_copy_$i.gz \
-O $base_dir/gwas.phased.haps_$i.gz $base_dir/gwas.phased.sample_$i.gz \
-L $base_dir/gwas.phased_$i.log \
--thread 16

