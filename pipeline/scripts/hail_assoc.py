from hail import *
hc = HailContext()
import argparse
import statsmodels.formula.api as sm


def main(args=None):

	print "reading vds dataset"
	vds = hc.read(args.vds_in)

	annot = hc.import_table(args.pheno_in, no_header=False, missing="NA", types={'T2D_UNKNOWN': TInt(), 'Age': TInt(), 'INS_FAST': TDouble()}).key_by("ID")
	vds = vds.annotate_samples_table(annot, root="sa.pheno")
	pheno_df = annot.to_pandas()
    
	print "counting males and females"
	n = samples_df.shape[0]
	nMales = samples_df[~samples_df['sa.isFemale']].shape[0]
	nFemales = samples_df[samples_df['sa.isFemale']].shape[0]
	vds = vds.annotate_global_expr('global.n = ' + str(n) + ', global.nMales = ' + str(nMales) + ', global.nFemales = ' + str(nFemales))

	print "counting male/female hets, homvars and called"
	vds = vds.annotate_variants_expr('va.nCalled = gs.filter(g => g.isCalled).count(), va.nMaleHet = gs.filter(g => ! sa.isFemale && g.isHet).count(), va.nMaleHomVar = gs.filter(g => ! sa.isFemale && g.isHomVar).count(), va.nMaleCalled = gs.filter(g => ! sa.isFemale && g.isCalled).count(), va.nFemaleHet = gs.filter(g => sa.isFemale && g.isHet).count(), va.nFemaleHomVar = gs.filter(g => sa.isFemale && g.isHomVar).count(), va.nFemaleCalled = gs.filter(g => sa.isFemale && g.isCalled).count()')

	print "calculating callRate, MAC, and MAF (accounting appropriately for sex chromosomes)"
	vds = vds.annotate_variants_expr('va.callRate = if (v.inYNonPar) ( va.nMaleCalled / global.nMales ) else if (v.inXNonPar) ( (va.nMaleCalled + 2*va.nFemaleCalled) / (global.nMales + 2*global.nFemales) ) else ( (va.nMaleCalled + va.nFemaleCalled) / (global.nMales + global.nFemales) ), va.MAC = if (v.inYNonPar) ( va.nMaleHomVar ) else if (v.inXNonPar) ( va.nMaleHomVar + va.nFemaleHet + 2*va.nFemaleHomVar ) else ( va.nMaleHet + 2*va.nMaleHomVar + va.nFemaleHet + 2*va.nFemaleHomVar ), va.MAF = if (v.inYNonPar) ( va.nMaleHomVar / va.nMaleCalled) else if (v.inXNonPar) ( (va.nMaleHomVar + va.nFemaleHet + 2*va.nFemaleHomVar) / (va.nMaleCalled + 2*va.nFemaleCalled) ) else ( (va.nMaleHet + 2*va.nMaleHomVar + va.nFemaleHet + 2*va.nFemaleHomVar) / (2*va.nMaleCalled + 2*va.nFemaleCalled) )')

	covars = [x for x in args.covars.split("+")] if args.covars != "" else []
	covars = covars + [x for x in pheno_df.columns.values if x.startswith("PC")]

	if args.trans != "":
		if "invn:" in args.trans:
			args.pheno_col = args.pheno_col + '_' + args.trans.replace(':','_')

	print "calculating test " + args.test + " on phenotype " + args.pheno_col + " with covariates " + "+".join(covars)
	if args.test == 'lm':
		gwas = vds.linreg('sa.pheno.' + args.pheno_col, covariates=covars, root='va.' + args.test + '.' + args.pheno_col, use_dosages=True)
		gwas.export_variants(args.out, expr="chr = v.contig, pos = v.start, uid = v, id = va.rsid, ref = v.ref, alt = v.alt, n = va.nCalled, male = va.nMaleCalled, female = va.nFemaleCalled, callrate = va.Callrate, mac = va.MAC, maf = va.MAF, raf = if (va.MAF <= 0.5) (va.MAF) else (1 - va.MAF), beta = va." + args.test + ".T2D_UNKNOWN.beta, se = va." + args.test + ".T2D_UNKNOWN.se, tstat = va." + args.test + ".T2D_UNKNOWN.tstat, pval = va." + args.test + ".T2D_UNKNOWN.pval, niter = va." + args.test + ".T2D_UNKNOWN.fit.nIter, converged = va." + args.test + ".T2D_UNKNOWN.fit.converged, exploded = va." + args.test + ".T2D_UNKNOWN.fit.exploded", types=False)
	elif args.test == 'wald':
		gwas = vds.logreg('wald','sa.pheno.' + args.pheno_col, covariates=covars, root='va.' + args.test + '.' + args.pheno_col, use_dosages=True)
		gwas.export_variants(args.out, expr="chr = v.contig, pos = v.start, uid = v, id = va.rsid, ref = v.ref, alt = v.alt, n = va.nCalled, male = va.nMaleCalled, female = va.nFemaleCalled, callrate = va.Callrate, mac = va.MAC, maf = va.MAF, raf = if (va.MAF <= 0.5) (va.MAF) else (1 - va.MAF), beta = va." + args.test + ".T2D_UNKNOWN.beta, se = va." + args.test + ".T2D_UNKNOWN.se, zstat = va." + args.test + ".T2D_UNKNOWN.zstat, pval = va." + args.test + ".T2D_UNKNOWN.pval, niter = va." + args.test + ".T2D_UNKNOWN.fit.nIter, converged = va." + args.test + ".T2D_UNKNOWN.fit.converged, exploded = va." + args.test + ".T2D_UNKNOWN.fit.exploded", types=False)
	elif args.test == 'firth':
		gwas = vds.logreg('firth','sa.pheno.' + args.pheno_col, covariates=covars, root='va.' + args.test + '.' + args.pheno_col, use_dosages=True)
		gwas.export_variants(args.out, expr="chr = v.contig, pos = v.start, uid = v, id = va.rsid, ref = v.ref, alt = v.alt, n = va.nCalled, male = va.nMaleCalled, female = va.nFemaleCalled, callrate = va.Callrate, mac = va.MAC, maf = va.MAF, raf = if (va.MAF <= 0.5) (va.MAF) else (1 - va.MAF), beta = va." + args.test + ".T2D_UNKNOWN.beta, chi2 = va." + args.test + ".T2D_UNKNOWN.chi2, pval = va." + args.test + ".T2D_UNKNOWN.pval, niter = va." + args.test + ".T2D_UNKNOWN.fit.nIter, converged = va." + args.test + ".T2D_UNKNOWN.fit.converged, exploded = va." + args.test + ".T2D_UNKNOWN.fit.exploded", types=False)
	elif args.test == 'lrt':
		gwas = vds.logreg('lrt','sa.pheno.' + args.pheno_col, covariates=covars, root='va.' + args.test + '.' + args.pheno_col, use_dosages=True)
		gwas.export_variants(args.out, expr="chr = v.contig, pos = v.start, uid = v, id = va.rsid, ref = v.ref, alt = v.alt, n = va.nCalled, male = va.nMaleCalled, female = va.nFemaleCalled, callrate = va.Callrate, mac = va.MAC, maf = va.MAF, raf = if (va.MAF <= 0.5) (va.MAF) else (1 - va.MAF), beta = va." + args.test + ".T2D_UNKNOWN.beta, chi2 = va." + args.test + ".T2D_UNKNOWN.chi2, pval = va." + args.test + ".T2D_UNKNOWN.pval, niter = va." + args.test + ".T2D_UNKNOWN.fit.nIter, converged = va." + args.test + ".T2D_UNKNOWN.fit.converged, exploded = va." + args.test + ".T2D_UNKNOWN.fit.exploded", types=False)
	elif args.test == 'score':
		gwas = vds.logreg('score','sa.pheno.' + args.pheno_col, covariates=covars, root='va.' + args.test + '.' + args.pheno_col, use_dosages=True)
		gwas.export_variants(args.out, expr="chr = v.contig, pos = v.start, uid = v, id = va.rsid, ref = v.ref, alt = v.alt, n = va.nCalled, male = va.nMaleCalled, female = va.nFemaleCalled, callrate = va.Callrate, mac = va.MAC, maf = va.MAF, raf = if (va.MAF <= 0.5) (va.MAF) else (1 - va.MAF), chi2 = va." + args.test + ".T2D_UNKNOWN.chi2, pval = va." + args.test + ".T2D_UNKNOWN.pval", types=False)
	else:
		return 1

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--trans', help='a comma separated list of transformations to apply to the phenotype')
	parser.add_argument('--covars', help="a '+' separated list of covariates")
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--vds-in', help='a Hail vds directory path', required=True)
	requiredArgs.add_argument('--pheno-in', help='a phenotype file', required=True)
	requiredArgs.add_argument('--iid-col', help='a column name for sample ID', required=True)
	requiredArgs.add_argument('--pheno-col', help='a column name for the phenotype', required=True)
	requiredArgs.add_argument('--test', choices=['wald','lrt','firth','score','lm','lmm'], help='a regression test code', required=True)
	requiredArgs.add_argument('--out', help='an output file basename', required=True)
	args = parser.parse_args()
	main(args)
