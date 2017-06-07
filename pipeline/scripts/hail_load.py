from hail import *
hc = HailContext()
import pandas as pd
import numpy as np
from math import log, isnan
import argparse

def main(args=None):

	print "reading vcf file"
	vds = hc.import_vcf(args.vcf, force_bgz=True)
	vds.summarize().report()

	print "splitting multiallelic variants and removing duplicates"
	vds = vds.split_multi().deduplicate()

	print "assigning family ID to match sample ID"
	vds = vds.annotate_samples_expr("sa.famID = s")

	print "reading sample annotations file and adding annotation to vds"
	sample_table = hc.import_table(args.sample, delimiter=" ", missing="NA", key=["IID"])
	vds = vds.annotate_samples_table(sample_table, root='sa.pheno')

	print "writing vds to disk"
	vds.write(args.vds, overwrite=True)
	vds.summarize().report()

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--vcf', help='a compressed vcf file', required=True)
	requiredArgs.add_argument('--sample', help='a sample annotation file (IID POP SUPERPOP SEX)', required=True)
	requiredArgs.add_argument('--vds', help='a hail vds directory name', required=True)
	args = parser.parse_args()
	main(args)
