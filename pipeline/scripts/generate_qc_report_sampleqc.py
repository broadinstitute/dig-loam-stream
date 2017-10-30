import argparse
import numpy as np
import pandas as pd

def main(args=None):

	## open latex file for writing
	with open(args.out,'w') as f:

		print "writing sampleqc section"
		f.write("\n"); f.write(r"\subsection{Sample Outlier Detection}"); f.write("\n")

		test=r"""Software used for this step:
		\begin{itemize}
			\item Hail \cite{hail}
			\item Klustakwik \cite{klustakwik}
		\end{itemize}"""
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		text=r"""Each sample was evaluated for inclusion in association tests based on 10 sample-by-variant metrics (Table \ref{table:sampleMetricDefinitions}), calculated using Hail."""
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		text=r"""\begin{table}[H]
			\caption{Sample Metrics}
			\centering
			\begin{tabular}{>{\bfseries}r l}
				nNonRef & nHet + nHomVar \\
				nHet & Number of heterozygous variants \\
				nCalled & nHomRef + nHet + nHomVar \\
				callRate & Fraction of variants with called genotypes \\
				rTiTv &  Transition/transversion ratio \\
				het & Inbreeding coefficient \\
				hetHigh & Inbreeding coefficient for variants with \(MAF >= 0.03\) \\
				hetLow & Inbreeding coefficient for variants with \(MAF < 0.03\) \\
				nHomVar & Number of homozygous alternate variants \\
				rHetHomVar & Het/HomVar ratio across all variants
			\end{tabular}
			\label{table:sampleMetricDefinitions}
		\end{table}"""
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		f.write("\n"); f.write(r"\subsubsection{Principal Component Adjustment and Normalization of Sample Metrics}"); f.write("\n")

		text=r"""Due to possible population substructure, the sample metrics exhibit some multi-modality in their distributions. To evaluate more normally distributed data, we calculated principal component adjusted residuals of the metrics using the top 10 principal components (PCARM's). Figure \ref{fig:nhetCompare} shows the nHet metric for {0} samples before and after adjustment.""".format(args.compare_dist_nhet.split("___")[0])
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		text=r"""\begin{figure}[H]
			\caption{Comparison of nHet distributions before and after adjustment / normalization}
			\centering
			\begin{subfigure}{\textwidth}
				\centering
				\includegraphics[width=\linewidth]{""" + args.compare_dist_nhet_unadj.split("___")[1] + r"""}
				\caption{nHet Original}
				\label{fig:nhet}
			\end{subfigure}\newline
			\begin{subfigure}{\textwidth}
				\centering
				\includegraphics[width=\linewidth]{""" + args.compare_dist_nhet_adj.split("___")[2] + r"""}
				\caption{nHet Adjusted}
				\label{fig:nhetAdj}
			\end{subfigure}
			\label{fig:nhetCompare}
		\end{figure}"""
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		f.write("\n"); f.write(r"\subsubsection{Individual Sample Metric Clustering}"); f.write("\n")

		text=r"""For outlier detection, we clustered the samples into Gaussian distributed subsets with respect to each PCARM using the software Klustakwik. During this process, samples that did not fit into any Gaussian distributed set of samples were identified and flagged for removal."""
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		f.write("\n"); f.write(r"\subsubsection{Principal Components of Variation in PCARM's}"); f.write("\n")

		text=r"""In addition to outliers along individual sample metrics, there may be samples that exhibit deviation from the norm across multiple metrics. In order to identify these samples, we calculated principal components explaining 95\% of the variation in all 10 PCARMs combined. """
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		f.write("\n"); f.write(r"\subsubsection{Combined PCARM Clustering}"); f.write("\n")

		text=r"""All samples were clustered into Gaussian distributed subsets along the principal components of the PCARM's, again using Klustakwik. This effectively removed any samples that were far enough outside the distribution on more than one PCARM, but not necessarily flagged as an outlier on any of the individual metrics alone."""
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		f.write("\n"); f.write(r"\subsubsection{Plots of Sample Outliers}"); f.write("\n")

		text_insert=""
		i = 0
		for a in args.sampleqc_outliers.split(","):
			i = i + 1
			l = a.split("___")[0]
			if i == 1:
				if len(args.sampleqc_outliers.split(",")) > 1:
					text_insert = r"Figures \ref{fig:adjSampleMetricDist" + l + r"}"
				else:
					text_insert = r"Figure \ref{fig:adjSampleMetricDist" + l + r"}"
			elif i < len(args.sampleqc_outliers.split(",")) - 1:
				text_insert = text_insert + r", \ref{fig:adjSampleMetricDist" + l + r"}"
			else:
				if len(args.sampleqc_outliers.split(",")) == 2:
					text_insert = text_insert + r" and \ref{fig:adjSampleMetricDist" + l + r"}"
				else:
					text_insert = text_insert + r", and \ref{fig:adjSampleMetricDist" + l + r"}"
		text=r"""The distributions for each PCARM and any outliers (cluster = 1) found are shown in {0}. Samples are labeled according to Table \ref{table:sampleOutlierLegend}.""".format(text_insert)
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		text=r"""\begin{table}[H]
			\caption{Sample Legend for Outlier Plots}
			\centering
			\begin{tabular}{>{\bfseries}r l}
				Grey & Clustered into Gaussian distributed subsets (not Flagged) \\
				Orange & Flagged as outlier based on individual PCARM's \\
				Blue & Flagged as outlier based on PC's of PCARM's \\
				Green & Flagged as outlier for both methods
			\end{tabular}
			\label{table:sampleOutlierLegend}
		\end{table}"""
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		for a in args.sampleqc_outliers.split(","):
			l = a.split("___")[0]
			m = a.split("___")[1]
			text=r"""\begin{figure}[H]
				\centering
				\includegraphics[width=\paperwidth,height=0.9\textheight,keepaspectratio]{""" + m + r"""}
				\caption{Adjusted sample metric distributions for """ + l + r"""}
				\label{fig:adjSampleMetricDist""" + l + r"""}
			\end{figure}"""
			f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		f.write("\n"); f.write(r"\subsection{Summary of Sample Outlier Detection}"); f.write("\n")

		text=r"""Table \ref{table:outlierSummaryTable} contains a summary of outliers detected by each method and across both genotyping technologies."""

\begin{table}[h!]
	\caption{Flagged Samples}
	\begin{center}
		\resizebox{\ifdim\width>\columnwidth\columnwidth\else\width\fi}{!}{%
			\pgfplotstabletypeset[
				font=\footnotesize,
				col sep=tab,
				columns={Method,Metric,METSIM_EX,METSIM_OMNI,Total},
				column type={>{\fontseries{bx}\selectfont}c},
				columns/Method/.style={column name=Method, string type, postproc cell content/.append style={/pgfplots/table/@cell content/.add={$\bf}{$},}},
				columns/Metric/.style={column name=Metric, string type, postproc cell content/.append style={/pgfplots/table/@cell content/.add={$\bf}{$},}},
				columns/METSIM_EX/.style={column name=METSIM\_EX, string type},
				columns/METSIM_OMNI/.style={column name=METSIM\_OMNI, string type},
				columns/Total/.style={column name=Total, string type},
				postproc cell content/.append style={/pgfplots/table/@cell content/.add={\fontseries{\seriesdefault}\selectfont}{}},
				every head row/.style={before row={\toprule}, after row={\midrule}},
				every last row/.style={after row=\bottomrule}
			]{outliers.table}}
	\label{table:outlierSummaryTable}
	\end{center}
\end{table}



		text=r"""Software used for this step:
		\begin{itemize}
			\item Hail \cite{hail}
			\item King \cite{king}
		\end{itemize}"""
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		text=r"""Sample pair kinship was determined using KING \cite{king} relationship inference software, which offers a robust algorithm for relationship inference under population stratification. Prior to inferring relationships, we filtered variants with low callrate, variants with low minor allele frequency, variants with positions in known high LD regions \cite{umichHiLd}, and known T2D associated loci using the software Hail \cite{hail}. Then an LD pruned dataset was created. The specific filters that were used are listed below.."""
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		text=r"""\begin{itemize}
			\item v.altAllele.isSNP 
			\item ! v.altAllele.isComplex
			\item {["A","C","G","T"]}.toSet.contains(v.altAllele.ref)
			\item {["A","C","G","T"]}.toSet.contains(v.altAllele.alt)
			\item va.qc.AF >= 0.01
			\item va.qc.callRate >= 0.98
		\end{itemize}"""
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		text_insert=""
		i = 0
		for a in args.filtered_bim.split(","):
			i = i + 1
			l = a.split("___")[0]
			m = a.split("___")[1]
			df = pd.read_table(m, header=None)
			if i == 1:
				text_insert = str(df.shape[0]) + " variants on the " + l + " array"
			elif i < len(args.filtered_bim.split(",")) -1:
				text_insert = text_insert + ", " + str(df.shape[0]) + " variants on the " + l + " array"
			else:
				if len(args.filtered_bim.split(",")) == 2:
					text_insert = text_insert + " and " + str(df.shape[0]) + " variants on the " + l + " array"
				else:
					text_insert = text_insert + ", and " + str(df.shape[0]) + " variants on the " + l + " array"
		text=r"""After filtering there were {0} """.format(text_insert)
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		text_insert=""
		i = 0
		for a in args.kin0_related.split(","):
			i = i + 1
			l = a.split("___")[0]
			m = a.split("___")[1]
			df = pd.read_table(m)
			df = df[df['Kinship'] > 0.4]
			if i == 1:
				text_insert = str(df.shape[0]) + " duplicate sample pairs on the " + l + " array"
			elif i < len(args.filtered_bim.split(",")) -1:
				text_insert = text_insert + ", " + str(df.shape[0]) + " duplicate sample pairs on the " + l + " array"
			else:
				if len(args.filtered_bim.split(",")) == 2:
					text_insert = text_insert + " and " + str(df.shape[0]) + " duplicate sample pairs on the " + l + " array"
				else:
					text_insert = text_insert + ", and " + str(df.shape[0]) + " duplicate sample pairs on the " + l + " array"
		if text_insert == "":
			text_insert = "no duplicate sample pairs"
		text=r"""Kinship coefficients were generated using the KING command $--kinship$. A threshold for identifying duplicate pairs was set to $Kinship > 0.4$. The output from KING indicated {0}.""".format(text_insert)
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		text_insert=""
		i = 0
		for a in args.famsizes.split(","):
			i = i + 1
			l = a.split("___")[0]
			m = a.split("___")[1]
			df = pd.read_table(m, header=None)
			df = df[df[1] >= 10]
			if i == 1:
				text_insert = str(df.shape[0]) + " samples on the " + l + " array"
			elif i < len(args.filtered_bim.split(",")) -1:
				text_insert = text_insert + ", " + str(df.shape[0]) + " samples on the " + l + " array"
			else:
				if len(args.filtered_bim.split(",")) == 2:
					text_insert = text_insert + " and " + str(df.shape[0]) + " samples on the " + l + " array"
				else:
					text_insert = text_insert + ", and " + str(df.shape[0]) + " samples on the " + l + " array"
			if text_insert == "":
				text_insert = "no samples with excessive sharing by IBD"
		text=r"""In addition to identifying duplicate samples, any single individual that exhibited Kinship values indicating a 2nd degree relative or higher relationship with 10 or more others was flagged for removal. The relationship count indicated {0}.""".format(text_insert)

		print "writing sex check section"
		f.write("\n"); f.write(r"\subsection{Sex Chromosome Check}"); f.write("\n")

		text=r"""Software used for this step:
		\begin{itemize}
			\item Hail \cite{hail}
		\end{itemize}"""
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		text_nomatch=""
		text_noimpute={}
		i = 0
		for a in args.sexcheck_problems.split(","):
			i = i + 1
			l = a.split("___")[0]
			m = a.split("___")[1]
			df = pd.read_table(m)
			if df.shape[0] > 0:
				nnomatch = df[~np.isnan(df['isFemale'])].shape[0]
				nnoimpute = df[np.isnan(df['isFemale'])].shape[0]
			else:
				nnomatch = 0
				nnoimpute = 0
			if i == 1:
				text_nomatch = str(nnomatch) + " samples on the " + l + " array"
				text_noimpute = str(nnoimpute) + " samples on the " + l + " array"
			elif i < len(args.sexcheck_problems.split(",")) -1:
				text_nomatch = text_nomatch + ", " + str(nnomatch) + " samples on the " + l + " array"
				text_noimpute = text_noimpute + ", " + str(nnoimpute) + " samples on the " + l + " array"
			else:
				if len(args.sexcheck_problems.split(",")) == 2:
					text_nomatch = text_nomatch + " and " + str(nnomatch) + " samples on the " + l + " array"
					text_noimpute = text_noimpute + " and " + str(nnoimpute) + " samples on the " + l + " array"
				else:
					text_nomatch = text_nomatch + ", and " + str(nnomatch) + " samples on the " + l + " array"
					text_noimpute = text_noimpute + ", and " + str(nnoimpute) + " samples on the " + l + " array"
			if text_nomatch == "":
				text_nomatch = "no samples"
				text_noimpute = "no samples"
		text=r"""Each array was then checked for genotype / clinical data agreement for sex. There were {0} that were flagged as a 'PROBLEM' by Hail because it was unable to impute sex and there were {1} that were flagged for removal because the genotype based sex did not match their clinical sex.""".format(text_noimpute, text_nomatch)
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

	print "finished\n"

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--out', help='an output file name with extension .tex', required=True)
	requiredArgs.add_argument('--compare-dist-nhet-unadj', help='an nHet plot', required=True)
	requiredArgs.add_argument('--compare-dist-nhet-adj', help='an nHet adjusted plot', required=True)
	requiredArgs.add_argument('--compare-dist-nhet-label', help='an array label', required=True)
	requiredArgs.add_argument('--pcarm-outlier', help='a pcarm outlier example plot', required=True)
	requiredArgs.add_argument('--sampleqc-outliers', help='a comma separated list of array labels and sampleqc outlier plots, each separated by 3 underscores', required=True)
	args = parser.parse_args()
	main(args)
