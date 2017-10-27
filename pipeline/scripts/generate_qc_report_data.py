import argparse
import pysam
import numpy as np
import pandas as pd

def main(args=None):

	## open latex file for writing
	print ""
	with open(args.out,'w') as f:

		intro = ["""Prior to performing ancestry inference or sample QC, it is necessary to make sure that the input data contains variants that are easily interpretable and don’t add needless complexity to calculations. Before performing quality control, genotypes were harmonized with a modern reference panel. Using Genotype Harmonizer, variant strand was aligned with 1000 Genomes Phase 3 data. In the process of alignment, variant IDs were replaced with 1000 Genomes variant IDs. Additonally, many ambiguous variants (A/T and G/C variants) were properly aligned based on nearby LD scores. Any ambiguous variants that could not be aligned using LD were removed. Variants that did not match with any 1000 Genomes variant were maintained in this step. After harmonizing with Genotype Harmonizer, variants that did not match any 1000 Genomes variants were aligned manually with the human reference build GRCh37, flagging variants with non-reference allele for removal.""".format(nSamples, nArrays)]
		print "writing data section"
		f.write("\n"); f.write(r"\clearpage"); f.write("\n")
		f.write("\n"); f.write(r"\section{Introduction}"); f.write("\n")
		for p in intro:
			f.write("\n"); f.write(p.encode('utf-8')); f.write("\n")
			f.write("\n"); f.write(r"\bigskip"); f.write("\n")

	print "finished\n"

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--out', help='an output file name with extension .tex', required=True)
	requiredArgs.add_argument('--array-data', help='a comma separated list of array data (plink binary file name or vcf file) each a three underscore separated datatype (bfile or vcf) and data file pair', required=True)
	args = parser.parse_args()
	main(args)
