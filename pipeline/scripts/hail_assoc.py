from hail import *
hc = HailContext()
import argparse


def main(args=None):

	print "reading vds dataset"
	vds = hc.read(args.vds_in)

	print "annotating samples with phenotype file"
	annot = hc.import_table(args.pheno_in, no_header=False, missing="NA", impute=True, types={args.iid_col: TString()}).key_by(args.iid_col)
	vds = vds.annotate_samples_table(annot, root="sa.pheno")

	print "reducing to samples with non-missing phenotype"
	vds = vds.filter_samples_expr('! sa.pheno.' + args.pheno_col + '.isMissing()')

	pheno_df = vds.samples_table().to_pandas()

	print "counting males and females"
	n = pheno_df.shape[0]
	nMales = pheno_df[~pheno_df['sa.isFemale']].shape[0]
	nFemales = pheno_df[pheno_df['sa.isFemale']].shape[0]
	vds = vds.annotate_global_expr('global.n = ' + str(n) + ', global.nMales = ' + str(nMales) + ', global.nFemales = ' + str(nFemales))

	print "counting male/female hets, homvars and called"
	vds = vds.annotate_variants_expr('va.nCalled = gs.filter(g => g.isCalled).count(), va.nMaleHet = gs.filter(g => ! sa.isFemale && g.isHet).count(), va.nMaleHomVar = gs.filter(g => ! sa.isFemale && g.isHomVar).count(), va.nMaleCalled = gs.filter(g => ! sa.isFemale && g.isCalled).count(), va.nFemaleHet = gs.filter(g => sa.isFemale && g.isHet).count(), va.nFemaleHomVar = gs.filter(g => sa.isFemale && g.isHomVar).count(), va.nFemaleCalled = gs.filter(g => sa.isFemale && g.isCalled).count()')

	print "calculating callRate, AC, and AF (accounting appropriately for sex chromosomes)"
	vds = vds.annotate_variants_expr('va.callRate = if (v.inYNonPar) ( va.nMaleCalled / global.nMales ) else if (v.inXNonPar) ( (va.nMaleCalled + 2*va.nFemaleCalled) / (global.nMales + 2*global.nFemales) ) else ( (va.nMaleCalled + va.nFemaleCalled) / (global.nMales + global.nFemales) ), va.AC = if (v.inYNonPar) ( va.nMaleHomVar ) else if (v.inXNonPar) ( va.nMaleHomVar + va.nFemaleHet + 2*va.nFemaleHomVar ) else ( va.nMaleHet + 2*va.nMaleHomVar + va.nFemaleHet + 2*va.nFemaleHomVar ), va.AF = if (v.inYNonPar) ( va.nMaleHomVar / va.nMaleCalled) else if (v.inXNonPar) ( (va.nMaleHomVar + va.nFemaleHet + 2*va.nFemaleHomVar) / (va.nMaleCalled + 2*va.nFemaleCalled) ) else ( (va.nMaleHet + 2*va.nMaleHomVar + va.nFemaleHet + 2*va.nFemaleHomVar) / (2*va.nMaleCalled + 2*va.nFemaleCalled) )')

	if args.test == 'lmm':
		print "reading in list of PCs to include in test"
		with hadoop_read(args.pcs_include) as f:
			pcs = f.read().splitlines()
	else:
		pcs = []

	covars = [x for x in args.covars.split("+")] if args.covars != "" else []
	covars = covars + pcs

	if args.trans == "invn":
		pheno_analyzed = args.pheno_col + '_invn_' + args.covars.replace('+','_')
	else:
		pheno_analyzed = args.pheno_col

	print "calculating test " + args.test + " on phenotype " + pheno_analyzed + " with covariates " + "+".join(covars)
	covars_analyzed = ['sa.pheno.' + x for x in covars]
	if args.test == 'lm':
		gwas = vds.linreg('sa.pheno.' + pheno_analyzed, covariates=covars_analyzed, root='va.' + args.test + '.' + pheno_analyzed, use_dosages=False)
		gwas.export_variants(args.out, expr="#chr = v.contig, pos = v.start, uid = v, id = va.rsid, ref = v.ref, alt = v.alt, n = va.nCalled, male = va.nMaleCalled, female = va.nFemaleCalled, callrate = va.callRate, ac = va.AC, af = va.AF, mac = if (va.AF <= 0.5) (va.AC) else (2 * va.nCalled - va.AC), maf = if (va.AF <= 0.5) (va.AF) else (1 - va.AF), beta = va." + args.test + "." + pheno_analyzed + ".beta, se = va." + args.test + "." + pheno_analyzed + ".se, tstat = va." + args.test + "." + pheno_analyzed + ".tstat, pval = va." + args.test + "." + pheno_analyzed + ".pval", types=False)
	elif args.test == 'wald':
		gwas = vds.logreg('wald','sa.pheno.' + pheno_analyzed, covariates=covars_analyzed, root='va.' + args.test + '.' + pheno_analyzed, use_dosages=False)
		gwas.export_variants(args.out, expr="#chr = v.contig, pos = v.start, uid = v, id = va.rsid, ref = v.ref, alt = v.alt, n = va.nCalled, male = va.nMaleCalled, female = va.nFemaleCalled, callrate = va.callRate, ac = va.AC, af = va.AF, mac = if (va.AF <= 0.5) (va.AC) else (2 * va.nCalled - va.AC), maf = if (va.AF <= 0.5) (va.AF) else (1 - va.AF), beta = va." + args.test + "." + pheno_analyzed + ".beta, se = va." + args.test + "." + pheno_analyzed + ".se, zstat = va." + args.test + "." + pheno_analyzed + ".zstat, pval = va." + args.test + "." + pheno_analyzed + ".pval, niter = va." + args.test + "." + pheno_analyzed + ".fit.nIter, converged = va." + args.test + "." + pheno_analyzed + ".fit.converged, exploded = va." + args.test + "." + pheno_analyzed + ".fit.exploded", types=False)
	elif args.test == 'firth':
		gwas = vds.logreg('firth','sa.pheno.' + pheno_analyzed, covariates=covars_analyzed, root='va.' + args.test + '.' + pheno_analyzed, use_dosages=False)
		gwas.export_variants(args.out, expr="#chr = v.contig, pos = v.start, uid = v, id = va.rsid, ref = v.ref, alt = v.alt, n = va.nCalled, male = va.nMaleCalled, female = va.nFemaleCalled, callrate = va.callRate, ac = va.AC, af = va.AF, mac = if (va.AF <= 0.5) (va.AC) else (2 * va.nCalled - va.AC), maf = if (va.AF <= 0.5) (va.AF) else (1 - va.AF), beta = va." + args.test + "." + pheno_analyzed + ".beta, chi2 = va." + args.test + "." + pheno_analyzed + ".chi2, pval = va." + args.test + "." + pheno_analyzed + ".pval, niter = va." + args.test + "." + pheno_analyzed + ".fit.nIter, converged = va." + args.test + "." + pheno_analyzed + ".fit.converged, exploded = va." + args.test + "." + pheno_analyzed + ".fit.exploded", types=False)
	elif args.test == 'lrt':
		gwas = vds.logreg('lrt','sa.pheno.' + pheno_analyzed, covariates=covars_analyzed, root='va.' + args.test + '.' + pheno_analyzed, use_dosages=False)
		gwas.export_variants(args.out, expr="#chr = v.contig, pos = v.start, uid = v, id = va.rsid, ref = v.ref, alt = v.alt, n = va.nCalled, male = va.nMaleCalled, female = va.nFemaleCalled, callrate = va.callRate, ac = va.AC, af = va.AF, mac = if (va.AF <= 0.5) (va.AC) else (2 * va.nCalled - va.AC), maf = if (va.AF <= 0.5) (va.AF) else (1 - va.AF), beta = va." + args.test + "." + pheno_analyzed + ".beta, chi2 = va." + args.test + "." + pheno_analyzed + ".chi2, pval = va." + args.test + "." + pheno_analyzed + ".pval, niter = va." + args.test + "." + pheno_analyzed + ".fit.nIter, converged = va." + args.test + "." + pheno_analyzed + ".fit.converged, exploded = va." + args.test + "." + pheno_analyzed + ".fit.exploded", types=False)
	elif args.test == 'score':
		gwas = vds.logreg('score','sa.pheno.' + pheno_analyzed, covariates=covars_analyzed, root='va.' + args.test + '.' + pheno_analyzed, use_dosages=False)
		gwas.export_variants(args.out, expr="#chr = v.contig, pos = v.start, uid = v, id = va.rsid, ref = v.ref, alt = v.alt, n = va.nCalled, male = va.nMaleCalled, female = va.nFemaleCalled, callrate = va.callRate, ac = va.AC, af = va.AF, mac = if (va.AF <= 0.5) (va.AC) else (2 * va.nCalled - va.AC), maf = if (va.AF <= 0.5) (va.AF) else (1 - va.AF), chi2 = va." + args.test + "." + pheno_analyzed + ".chi2, pval = va." + args.test + "." + pheno_analyzed + ".pval", types=False)
	elif args.test == 'lmm':
		print "extracting variants from previously filtered and pruned bim file"
		bim = hc.import_table(args.bim_in, no_header=True, types={'f1': TVariant()}).key_by('f1')
		kinship = vds.filter_variants_table(bim, keep=True).rrm()
		gwas = vds.lmmreg(kinship, 'sa.pheno.' + pheno_analyzed, covariates=covars_analyzed, global_root='global.' + args.test + '.' + pheno_analyzed, root='va.' + args.test + '.' + pheno_analyzed, use_dosages=False, dropped_variance_fraction=0.01, run_assoc=True, use_ml=False)
		gwas.export_variants(args.out, expr="#chr = v.contig, pos = v.start, uid = v, id = va.rsid, ref = v.ref, alt = v.alt, n = va.nCalled, male = va.nMaleCalled, female = va.nFemaleCalled, callrate = va.callRate, ac = va.AC, af = va.AF, mac = if (va.AF <= 0.5) (va.AC) else (2 * va.nCalled - va.AC), maf = if (va.AF <= 0.5) (va.AF) else (1 - va.AF), beta = va." + args.test + "." + pheno_analyzed + ".beta, sigmaG2 = va." + args.test + "." + pheno_analyzed + ".sigmaG2, chi2 = va." + args.test + "." + pheno_analyzed + ".chi2, pval = va." + args.test + "." + pheno_analyzed + ".pval", types=False)
	else:
		return 1

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--trans', help='a transformation code')
	parser.add_argument('--covars', help="a '+' separated list of covariates")
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--vds-in', help='a Hail vds directory path', required=True)
	requiredArgs.add_argument('--bim-in', help='a filtered and pruned bim file', required=True)
	requiredArgs.add_argument('--pheno-in', help='a phenotype file', required=True)
	requiredArgs.add_argument('--pcs-include', help='a file containing a list of PCs to include in test', required=True)
	requiredArgs.add_argument('--iid-col', help='a column name for sample ID', required=True)
	requiredArgs.add_argument('--pheno-col', help='a column name for the phenotype', required=True)
	requiredArgs.add_argument('--test', choices=['wald','lrt','firth','score','lm','lmm'], help='a regression test code', required=True)
	requiredArgs.add_argument('--out', help='an output file basename', required=True)
	args = parser.parse_args()
	main(args)
