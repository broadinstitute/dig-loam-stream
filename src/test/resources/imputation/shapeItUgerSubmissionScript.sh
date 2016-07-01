#!/bin/bash
#$ -cwd
#$ -j y

source /broad/software/scripts/useuse
reuse -q UGER

i=$SGE_TASK_ID
      
if [ $i -eq 1 ]
then
	/home/clint/bin/shapeit \
	-V \
	/home/clint/workspace/imputation/shapeit_example/gwas.vcf.gz \
	-M \
	/home/clint/workspace/imputation/shapeit_example/genetic_map.txt.gz \
	-O \
	/home/clint/workspace/imputation/shapeit_example/gwas.phased.haps.gz \
	/home/clint/workspace/imputation/shapeit_example/gwas.phased.sample.gz \
	-L \
	/home/clint/workspace/imputation/shapeit_example/gwas.phased.log \
	--thread \
	8 \
elif [ $i -eq 2 ]
then
	/home/clint/bin/shapeit \
	-V \
	/home/clint/workspace/imputation/shapeit_example/gwas.vcf.gz \
	-M \
	/home/clint/workspace/imputation/shapeit_example/genetic_map.txt.gz \
	-O \
	/home/clint/workspace/imputation/shapeit_example/gwas.phased.haps.gz \
	/home/clint/workspace/imputation/shapeit_example/gwas.phased.sample.gz \
	-L \
	/home/clint/workspace/imputation/shapeit_example/gwas.phased.log \
	--thread \
	8 \
elif [ $i -eq 3 ]
then
	/home/clint/bin/shapeit \
	-V \
	/home/clint/workspace/imputation/shapeit_example/gwas.vcf.gz \
	-M \
	/home/clint/workspace/imputation/shapeit_example/genetic_map.txt.gz \
	-O \
	/home/clint/workspace/imputation/shapeit_example/gwas.phased.haps.gz \
	/home/clint/workspace/imputation/shapeit_example/gwas.phased.sample.gz \
	-L \
	/home/clint/workspace/imputation/shapeit_example/gwas.phased.log \
	--thread \
	8 \
fi
