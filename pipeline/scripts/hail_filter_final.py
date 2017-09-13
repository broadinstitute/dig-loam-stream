from hail import *
hc = HailContext()
import pandas as pd
import numpy as np
from math import log, isnan
import argparse
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
import seaborn

def main(args=None):

	print "reading vds dataset"
	vds = hc.read(args.vds_in)
	vds.summarize().report()

	print "adding ancestry annotation"
	annotate_anc = hc.import_table(args.ancestry_in,delimiter="\t",no_header=True).annotate('IID = f0').key_by('IID')
	vds = vds.annotate_samples_table(annotate_anc, expr='sa.pheno.GROUP = table.f1')

	print "adding imputed sex annotation"
	annotate_sex = hc.import_table(args.sexcheck_in, no_header=False, types={'isFemale': TBoolean()}).key_by('IID')
	vds = vds.annotate_samples_table(annotate_sex, expr='sa.isFemale = table.isFemale')

	print "adding case/control status annotation"
	annotate_cc = hc.import_table(args.pheno_in, no_header=False, types={args.case_ctrl_col: TInt()}).key_by('IID')
	vds = vds.annotate_samples_table(annotate_cc, expr='sa.isCase = table.' + args.case_ctrl_col + ' == 1')

	print "removing samples that failed QC"
	samples_remove = hc.import_table(args.samples_remove, no_header=True).key_by('f0')
	vds = vds.filter_samples_table(samples_remove, keep=False)
	vds.summarize().report()

	print "reducing to autosomal variants only"
	vds = vds.filter_variants_expr('v.isAutosomal', keep=True)

	samples_df = vds.samples_table().to_pandas()
	group_counts = samples_df['sa.pheno.GROUP'][~samples_df['sa.isCase']].value_counts().to_dict()
	failed_variants = []
	for group in group_counts:
		if group_counts[group] > 100 and group_counts[group] != 'AMR':
			print "filtering variants with pHWE <= 1e-6 in " + group + " samples"
			failed = vds.filter_samples_expr('sa.pheno.GROUP == "' + group + '" && ! sa.isCase', keep=True).variant_qc(root='va.qc.' + group).filter_variants_expr('va.qc.' + group + '.pHWE <= 1e-6').variants_table()
			vds = vds.filter_variants_table(failed,keep=False)

	print "writing Plink files to disk"
	vds.export_plink(args.plink_out, fam_expr='famID = s, id = s, isFemale = sa.isFemale')

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--vds-in', help='a hail vds dataset name', required=True)
	requiredArgs.add_argument('--ancestry-in', help='an inferred ancestry file', required=True)
	requiredArgs.add_argument('--sexcheck-in', help='an imputed sexcheck output file from Hail', required=True)
	requiredArgs.add_argument('--pheno-in', help='a phenotype file', required=True)
	requiredArgs.add_argument('--case-ctrl-col', help='column name for case/control status in phenotype file', required=True)
	requiredArgs.add_argument('--samples-remove', help='a file containing sample IDs that failed QC', required=True)
	requiredArgs.add_argument('--plink-out', help='a plink fileset name for output', required=True)
	args = parser.parse_args()
	main(args)
