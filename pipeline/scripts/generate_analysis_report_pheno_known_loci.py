import argparse
import numpy as np
import pandas as pd
import collections

def main(args=None):

	top = collections.OrderedDict()
	for s in args.top_known_loci.split(","):
		top[s.split("___")[0]] = s.split("___")[1]
	result_cols = ['chr','pos','id','alt','ref','n','case','ctrl','af','beta','se','or','pval','cohort','gene','n_known','case_known','ctrl_known','beta_known','se_known','or_known','pval_known']
	report_cols = ['CHR','POS','ID','EA','OA','N','CASE','CTRL','FREQ','EFFECT','STDERR','OR','P','COHORT',r"GENE\textsubscript{CLOSEST}",r"N\textsubscript{KNOWN}",r"CASE\textsubscript{KNOWN}",r"CTRL\textsubscript{KNOWN}",r"EFFECT\textsubscript{KNOWN}",r"STDERR\textsubscript{KNOWN}",r"OR\textsubscript{KNOWN}",r"P\textsubscript{KNOWN}"]
	cols = dict(zip(result_cols,report_cols))
	types = {'chr': 'string type', 'pos': 'string type', 'id': 'string type', 'alt': 'verb string type', 'ref': 'verb string type', 'gene': 'string type', 'cohort': 'verb string type', 'dir': 'verb string type'}

	## open latex file for writing
	with open(args.out_tex,'w') as f:

		print "writing top associations section"
		f.write("\n"); f.write(r"\subsection{Top known associations}"); f.write("\n")

		for model in top:

			# read in top results
			df = pd.read_table(top[model],sep="\t")
			df = df[[c for c in result_cols if c in df.columns]]

			text = []
			text.extend([r"\begin{table}[H]",
				r"   \begin{center}",
				r"   \caption{Top known loci in " + model.replace(r'_',r'\_') + r" (\textbf{bold} variants indicate matching direction of effect)}",
				r"   \resizebox{\ifdim\width>\columnwidth\columnwidth\else\width\fi}{!}{%",
				r"      \pgfplotstabletypeset[",
				r"         font=\footnotesize,",
				r"         col sep=tab,",
				r"         columns={" + ",".join(df.columns.tolist()) + r"},",
				r"         column type={>{\fontseries{bx}\selectfont}c},"])
			for c in df.columns.tolist():
				if c in types:
					text.extend([r"         columns/" + c + r"/.style={column name=" + cols[c] + r", " + types[c] + r"},"])
				else:
					text.extend([r"         columns/" + c + r"/.style={column name=" + cols[c] + r"},"])
			text.extend([r"         postproc cell content/.append style={/pgfplots/table/@cell content/.add={\fontseries{\seriesdefault}\selectfont}{}},",
				r"         every head row/.style={before row={\toprule}, after row={\midrule}},",
				r"         every last row/.style={after row=\bottomrule}",
				r"         ]{" + top[model] + r"}}",
				r"   \label{table:topLoci" + args.pheno_name + r"}",
				r"   \end{center}",
				r"\end{table}"])
			f.write("\n"); f.write("\n".join(text).encode('utf-8')); f.write("\n")

		text = r"\ExecuteMetaData[\currfilebase.input]{"  + args.pheno_long_name + r" known associations}"
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

	with open(args.out_input,'w') as f:

		text = ["",r"%<*"  + args.pheno_long_name + r" known associations>","%</"  + args.pheno_long_name + r" known associations>"]
		f.write("\n".join(text).encode('utf-8')); f.write("\n")

	print "finished\n"

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--top-known-loci', help='a comma separated list of models and top known loci results files (aligned to risk allele and gene annotated), each separated by 3 underscores', required=True)
	requiredArgs.add_argument('--pheno-name', help='a column name for phenotype', required=True)
	requiredArgs.add_argument('--pheno-long-name', help='a full phenotype name', required=True)
	requiredArgs.add_argument('--out-tex', help='an output file name with extension .tex', required=True)
	requiredArgs.add_argument('--out-input', help='an output file name with extension .input', required=True)
	args = parser.parse_args()
	main(args)
