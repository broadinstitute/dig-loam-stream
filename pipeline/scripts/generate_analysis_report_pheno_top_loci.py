import argparse
import numpy as np
import pandas as pd

def main(args=None):

	top = {}
	for s in args.top_results.split(","):
		top[s.split("___")[0]] = s.split("___")[1]

	reg = {}
	for s in args.regionals.split(","):
		reg[s.split("___")[0]] = s.split("___")[1].split("___")

	known = []
	for s in args.known_ld.split(","):
		df_known = pd.read_table(s, sep="\t")
		known.extend(df_known['SNP_A'].unique().tolist())
		known.extend(df_known['SNP_B'].unique().tolist())
	known = list(set(known))

	## open latex file for writing
	with open(args.out_tex,'w') as f:

		print "writing top associations section"
		f.write("\n"); f.write(r"\subsection{Top associations}"); f.write("\n")

		for model in top:

			# read in top results
			df = pd.read_table(top[model],sep="\t")

			tex = []
			tex.extend([r"\begin{table}[h!]",
				r"   \begin{center}",
				r"   \caption{Top variants in " + assoc_key.replace(r'_',r'\_') + r" (\textbf{bold} variants indicate previously identified associations)}",
				r"   \resizebox{\ifdim\width>\columnwidth\columnwidth\else\width\fi}{!}{%",
				r"      \pgfplotstabletypeset[",
				r"         font=\footnotesize,",
				r"         col sep=tab,"])
			if not 'META' in merge_val['map']:
				if cfg['phenotypes'][pheno]['dichotomous']:
					tex.extend([
						r"         columns={chr,pos,id,a1,a2,gene,test,n,case,ctrl,mac,freq,effect,stderr,or,chisq,p},",
						r"         column type={>{\fontseries{bx}\selectfont}c},",
						r"         columns/chr/.style={column name=CHR, string type},",
						r"         columns/pos/.style={column name=POS, string type},",
						r"         columns/id/.style={column name=ID, string type},",
						r"         columns/a1/.style={column name=EA, verb string type},",
						r"         columns/a2/.style={column name=NEA, verb string type},",
						r"         columns/gene/.style={column name=GENE\textsubscript{CLOSEST}, string type},",
						r"         columns/test/.style={column name=ARRAY, verb string type},",
						r"         columns/n/.style={column name=N},",
						r"         columns/case/.style={column name=CASE},",
						r"         columns/ctrl/.style={column name=CTRL},",
						r"         columns/mac/.style={column name=MAC},",
						r"         columns/freq/.style={column name=FREQ},",
						r"         columns/effect/.style={column name=EFFECT},",
						r"         columns/stderr/.style={column name=STDERR},",
						r"         columns/or/.style={column name=OR},",
						r"         columns/chisq/.style={column name=CHISQ},",
						r"         columns/p/.style={column name=P},"])
				else:
					tex.extend([
						r"         columns={chr,pos,id,a1,a2,gene,test,n,mac,freq,effect,stderr,t,p},",
						r"         column type={>{\fontseries{bx}\selectfont}c},",
						r"         columns/chr/.style={column name=CHR, string type},",
						r"         columns/pos/.style={column name=POS, string type},",
						r"         columns/id/.style={column name=ID, string type},",
						r"         columns/a1/.style={column name=EA, verb string type},",
						r"         columns/a2/.style={column name=NEA, verb string type},",
						r"         columns/gene/.style={column name=GENE\textsubscript{CLOSEST}, string type},",
						r"         columns/test/.style={column name=ARRAY, verb string type},",
						r"         columns/n/.style={column name=N},",
						r"         columns/mac/.style={column name=MAC},",
						r"         columns/freq/.style={column name=FREQ},",
						r"         columns/effect/.style={column name=EFFECT},",
						r"         columns/stderr/.style={column name=STDERR},",
						r"         columns/t/.style={column name=T},",
						r"         columns/p/.style={column name=P},"])
			else:
				if cfg['phenotypes'][pheno]['dichotomous']:
					tex.extend([
						r"         columns={chr,pos,id,a1,a2,gene,test,dir,n,case,ctrl,mac,freq,effect,stderr,or,z_chisq,p},",
						r"         column type={>{\fontseries{bx}\selectfont}c},",
						r"         columns/chr/.style={column name=CHR, string type},",
						r"         columns/pos/.style={column name=POS, string type},",
						r"         columns/id/.style={column name=ID, string type},",
						r"         columns/a1/.style={column name=EA, verb string type},",
						r"         columns/a2/.style={column name=NEA, verb string type},",
						r"         columns/gene/.style={column name=GENE\textsubscript{CLOSEST}, string type},",
						r"         columns/test/.style={column name=ARRAY, verb string type},",
						r"         columns/dir/.style={column name=DIR, verb string type},",
						r"         columns/n/.style={column name=N},",
						r"         columns/case/.style={column name=CASE},",
						r"         columns/ctrl/.style={column name=CTRL},",
						r"         columns/mac/.style={column name=MAC},",
						r"         columns/freq/.style={column name=FREQ},",
						r"         columns/effect/.style={column name=EFFECT},",
						r"         columns/stderr/.style={column name=STDERR},",
						r"         columns/or/.style={column name=OR},",
						r"         columns/z_chisq/.style={column name=Z|CHISQ},",
						r"         columns/p/.style={column name=P},"])
				else:
					tex.extend([
						r"         columns={chr,pos,id,a1,a2,gene,test,dir,n,mac,freq,effect,stderr,z_t,p},",
						r"         column type={>{\fontseries{bx}\selectfont}c},",
						r"         columns/chr/.style={column name=CHR, string type},",
						r"         columns/pos/.style={column name=POS, string type},",
						r"         columns/id/.style={column name=ID, string type},",
						r"         columns/a1/.style={column name=EA, verb string type},",
						r"         columns/a2/.style={column name=NEA, verb string type},",
						r"         columns/gene/.style={column name=GENE\textsubscript{CLOSEST}, string type},",
						r"         columns/test/.style={column name=ARRAY, verb string type},",
						r"         columns/dir/.style={column name=DIR, verb string type},",
						r"         columns/n/.style={column name=N},",
						r"         columns/mac/.style={column name=MAC},",
						r"         columns/freq/.style={column name=FREQ},",
						r"         columns/effect/.style={column name=EFFECT},",
						r"         columns/stderr/.style={column name=STDERR},",
						r"         columns/z_t/.style={column name=Z|T},",
						r"         columns/p/.style={column name=P},"])
			tex.extend([
				r"         postproc cell content/.append style={/pgfplots/table/@cell content/.add={\fontseries{\seriesdefault}\selectfont}{}},",
				r"         every head row/.style={before row={\toprule}, after row={\midrule}},",
				r"         every last row/.style={after row=\bottomrule}",
				r"         ]{" + top_loci + r"}}",
				r"   \label{table:" + merge_key + r"TOP_LOCI_" + assoc_key + r"}",
				r"   \end{center}",
				r"\end{table}"])



						regionals = glob.glob(cfg['path'] + '/assoc/phase' + str(cfg['phase']) + '/merge/' + merge_key + '.' + cfg['assocs'][assoc_key]['phenotype'] + '/' + merge_key + '.' + cfg['assocs'][assoc_key]['phenotype'] + '.reconciled.regional.*.png')
						nplots = 0
						if len(regionals) > 0:
							if len(regionals) > 1:
								tex.extend([r"\begin{figure}[h!]",
										r"   \centering"])
								for reg in np.sort(regionals):
									reg_base=os.path.basename(reg)
									variant = reg_base.split(".")[5]
									chromosome = reg_base.split(".")[6].replace('chr','')
									reg_size = reg_base.split(".")[7].replace('kb','')
									nplots = nplots + 1
									delim = r"\\" if nplots % 2 == 0 else r"%"
									tex.extend([r"   \begin{subfigure}{.5\textwidth}",
										r"      \centering",
										r"      \includegraphics[width=\linewidth]{" + reg + "}",
										r"      \caption{" + variant + r" $\pm$" + str(int(reg_size)/2) + r"kb}",
										r"      \label{fig:REGIONAL_" + assoc_key + "_" + variant + "}",
										r"   \end{subfigure}" + delim])
								tex.extend(["   \caption{Regional plot for model " + assoc_key.replace(r'_',r'\_') + r"}",r"   \label{fig:REGIONAL_" + assoc_key + "_" + variant + "}",r"\end{figure}"])
							else:
								reg_base=os.path.basename(regionals[0])
								variant = reg_base.split(".")[5]
								chromosome = reg_base.split(".")[6].replace('chr','')
								reg_size = reg_base.split(".")[7].replace('kb','')
								tex.extend([r"\begin{figure}[h!]",
										r"   \centering",
										r"   \includegraphics[width=.5\linewidth]{" + regionals[0] + "}",
										r"   \caption{Regional plot for model " + assoc_key.replace(r'_',r'\_') + r": " + variant + r" $\pm$" + str(int(reg_size)/2) + r"kb}",
										r"   \label{fig:REGIONAL_" + assoc_key + "_" + variant + "}",
										r"\end{figure}"])
					if assoc_val['meta'] is not None:
						for meta_key, meta_val in assoc_val['meta'].iteritems():
							if meta_val['report']:
								top_loci = cfg['path'] + '/assoc/phase' + str(cfg['phase']) + '/top_loci/' + cfg['assocs'][assoc_key]['phenotype'] + '/' + cfg['assocs'][assoc_key]['phenotype'] + '.top_loci_report.highlighted'
								tex.extend([r"\begin{table}[h!]",
									r"   \begin{center}",
									r"   \caption{Top variants in " + assoc_key.replace(r'_',r'\_') + r" (\textbf{bold} variants indicate previously identified associations)}",
									r"   \resizebox{\ifdim\width>\columnwidth\columnwidth\else\width\fi}{!}{%",
									r"      \pgfplotstabletypeset[",
									r"         font=\footnotesize,",
									r"         col sep=tab,"])
								if cfg['phenotypes'][pheno]['dichotomous']:
									tex.extend([
										r"         columns={chr,pos,id,a1,a2,gene,test,n,case,ctrl,mac,freq,effect,stderr,or,chisq,p},",
										r"         column type={>{\fontseries{bx}\selectfont}c},",
										r"         columns/chr/.style={column name=CHR, string type},",
										r"         columns/pos/.style={column name=POS, string type},",
										r"         columns/id/.style={column name=ID, string type},",
										r"         columns/a1/.style={column name=EA, verb string type},",
										r"         columns/a2/.style={column name=NEA, verb string type},",
										r"         columns/gene/.style={column name=GENE\textsubscript{CLOSEST}, string type},",
										r"         columns/test/.style={column name=ARRAY, verb string type},",
										r"         columns/n/.style={column name=N},",
										r"         columns/case/.style={column name=CASE},",
										r"         columns/ctrl/.style={column name=CTRL},",
										r"         columns/mac/.style={column name=MAC},",
										r"         columns/freq/.style={column name=FREQ},",
										r"         columns/effect/.style={column name=EFFECT},",
										r"         columns/stderr/.style={column name=STDERR},",
										r"         columns/or/.style={column name=OR},",
										r"         columns/chisq/.style={column name=CHISQ},",
										r"         columns/p/.style={column name=P},"])
								else:
									tex.extend([
										r"         columns={chr,pos,id,a1,a2,gene,test,n,mac,freq,effect,stderr,t,p},",
										r"         column type={>{\fontseries{bx}\selectfont}c},",
										r"         columns/chr/.style={column name=CHR, string type},",
										r"         columns/pos/.style={column name=POS, string type},",
										r"         columns/id/.style={column name=ID, string type},",
										r"         columns/a1/.style={column name=EA, verb string type},",
										r"         columns/a2/.style={column name=NEA, verb string type},",
										r"         columns/gene/.style={column name=GENE\textsubscript{CLOSEST}, string type},",
										r"         columns/test/.style={column name=ARRAY, verb string type},",
										r"         columns/n/.style={column name=N},",
										r"         columns/mac/.style={column name=MAC},",
										r"         columns/freq/.style={column name=FREQ},",
										r"         columns/effect/.style={column name=EFFECT},",
										r"         columns/stderr/.style={column name=STDERR},",
										r"         columns/t/.style={column name=T},",
										r"         columns/p/.style={column name=P},"])
								tex.extend([
									r"         postproc cell content/.append style={/pgfplots/table/@cell content/.add={\fontseries{\seriesdefault}\selectfont}{}},",
									r"         every head row/.style={before row={\toprule}, after row={\midrule}},",
									r"         every last row/.style={after row=\bottomrule}",
									r"         ]{" + top_loci + r"}}",
									r"   \label{table:" + meta_key + r"TOP_LOCI_" + assoc_key + r"}",
									r"   \end{center}",
									r"\end{table}"])

		text = r"\ExecuteMetaData[\currfilebase.input]{"  + args.pheno_long_name + r" top associations}"
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

	with open(args.out_input,'w') as f:

		text = ["",r"%<*"  + args.pheno_long_name + r" top associations>","%</"  + args.pheno_long_name + r" top associations>"]
		f.write("\n".join(text).encode('utf-8')); f.write("\n")

	print "finished\n"

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--top-results', help='a comma separated list of models and top results files (aligned to risk allele and gene annotated), each separated by 3 underscores', required=True)
	requiredArgs.add_argument('--regionals', help='a comma separated list of models and regional mht plots (list of mht plots separated by semicolon), each separated by 3 underscores', required=True)
	requiredArgs.add_argument('--known-loci', help='a comma separated list of known loci lists', required=True)
	requiredArgs.add_argument('--pheno-name', help='a column name for phenotype', required=True)
	requiredArgs.add_argument('--out-tex', help='an output file name with extension .tex', required=True)
	requiredArgs.add_argument('--out-input', help='an output file name with extension .input', required=True)
	args = parser.parse_args()
	main(args)
