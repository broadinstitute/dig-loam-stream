#!/bin/bash
#$ -cwd
#$ -j y

source /broad/software/scripts/useuse
reuse -q UGER

i=$SGE_TASK_ID
      
if [ $i -eq 1 ]
then
	/some/shapeit/executable \
	-V \
	/some/vcf/file \
	-M \
	/some/map/file \
	-O \
	/some/haplotype/file \
	/some/sample/file \
	-L \
	/some/log/file \
	--thread \
	2 \
elif [ $i -eq 2 ]
then
	/some/shapeit/executable \
	-V \
	/some/vcf/file \
	-M \
	/some/map/file \
	-O \
	/some/haplotype/file \
	/some/sample/file \
	-L \
	/some/log/file \
	--thread \
	2 \
elif [ $i -eq 3 ]
then
	/some/shapeit/executable \
	-V \
	/some/vcf/file \
	-M \
	/some/map/file \
	-O \
	/some/haplotype/file \
	/some/sample/file \
	-L \
	/some/log/file \
	--thread \
	2 \
fi
