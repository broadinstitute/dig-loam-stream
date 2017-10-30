import argparse
import numpy as np
import pandas as pd

def main(args=None):

	## open latex file for writing
	with open(args.out,'w') as f:

		print "writing data section"
		f.write("\n"); f.write(r"\clearpage"); f.write("\n")
		f.write("\n"); f.write(r"\section{Sample QC}"); f.write("\n")
		f.write("\n"); f.write(r"\subsection{Ancestry Inference}"); f.write("\n")

		text=r"""Software used for this step:
		\begin{itemize}
			\item Hail \cite{hail}
			\item Klustakwik \cite{klustakwik}
		\end{itemize}"""
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		text=r"""Prior to association testing, it is useful to infer ancestry of a dataset in relation to a modern reference panel representing the major human populations. While our particular sample QC process does not depend on this information, it is useful to downstream analysis when stratifying the calculation of certain variant statistics that are sensitive to population substructure (eg. Hardy Weinberg equilibrium). Additionally, ancestry inference may identify samples that do not seem to fit into a well-defined major population group, which would allow them to be flagged for removal from association testing."""
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		text_insert=""
		i = 0
		for a in args.kg_merged_bim.split(","):
			i = i + 1
			l = a.split("___")[0]
			m = a.split("___")[1]
			df = pd.read_table(m, header=None)
			if i == 1:
				text_insert = str(df.shape[0]) + " variants on the " + l + " array"
			elif i < len(args.kg_merged_bim.split(",")) -1:
				text_insert = text_insert + ", " + str(df.shape[0]) + " variants on the " + l + " array"
			else:
				if len(args.kg_merged_bim.split(",")) == 2:
					text_insert = text_insert + " and " + str(df.shape[0]) + " variants on the " + l + " array"
				else:
					text_insert = text_insert + ", and " + str(df.shape[0]) + " variants on the " + l + " array"

		text=r"""Initially, each dataset is merged with reference data. In this case we used 2,504 1000 Genomes Phase 3 Version 5 samples. Our method restricts this merging to a set of 5,166 known ancestry informative SNPs. The merged data consisted of {0}. After merging, principal components were computed using the PC-AiR \cite{{pcair}} method in the GENESIS R package. The algorithm allows for the calculation of principal components that reflect ancestry in the presence of known or cryptic relatedness. The 1000 Genomes samples were forced into the "unrelated" set and the PC-AiR algorithm was used to find the "unrelated" samples from the array data. Then PCs were calculated on them and projected onto the remaining samples.""".format(text_insert)
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		text_insert=""
		i = 0
		for a in args.pca_plots.split(","):
			i = i + 1
			l = a.split("___")[0]
			if i == 1:
				if len(args.pca_plots.split(",")) > 1:
					text_insert = r"Figures \ref{fig:ancestryPcaPlots" + l + r"}"
				else:
					text_insert = r"Figure \ref{fig:ancestryPcaPlots" + l + r"} displays"
			elif i < len(args.kg_merged_bim.split(",")) - 1:
				text_insert = text_insert + r", \ref{fig:ancestryPcaPlots" + l + r"}"
			else:
				if len(args.pca_plots.split(",")) == 2:
					text_insert = text_insert + r" and \ref{fig:ancestryPcaPlots" + l + r"} display"
				else:
					text_insert = text_insert + r", and \ref{fig:ancestryPcaPlots" + l + r"} display"
		text=r"""{0} plots of the top three principal components along with the 1000 Genomes major population groups.""".format(text_insert)
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		i = 0
		for a in args.pca_plots.split(","):
			i = i + 1
			l = a.split("___")[0]
			p1 = a.split("___")[1]
			p2 = a.split("___")[2]
			text = [r"\begin{figure}[H]",
				r"   \centering",
				r"   \begin{subfigure}{.5\textwidth}",
				r"      \centering",
				r"      \includegraphics[width=\linewidth]{" + p1 + r"}",
				r"      \caption{PC1 vs. PC2}",
				r"      \label{fig:ancestryPca1vs2Plot" + l + r"}",
				r"   \end{subfigure}%",
				r"   \begin{subfigure}{.5\textwidth}",
				r"      \centering",
				r"      \includegraphics[width=\linewidth]{" + p2 + r"}",
				r"      \caption{PC2 vs. PC3}",
				r"      \label{fig:ancestryPca2vs3Plot" + l + r"}",
				r"   \end{subfigure}",
				r"   \caption{Principal components of ancestry for " + l + r"}",
				r"   \label{fig:ancestryPcaPlots" + l + r"}",
				r"\end{figure}"]
			f.write("\n"); f.write("\n".join(text).encode('utf-8')); f.write("\n")

		text_insert=""
		i = 0
		for a in args.cluster_plots.split(","):
			i = i + 1
			l = a.split("___")[0]
			if i == 1:
				if len(args.cluster_plots.split(",")) > 1:
					text_insert = r"Figures \ref{fig:ancestryClusterPlots" + l + r"}"
				else:
					text_insert = r"Figure \ref{fig:ancestryClusterPlots" + l + r"} clearly indicates"
			elif i < len(args.cluster_plots.split(",")) -1:
				text_insert = text_insert + r", \ref{fig:ancestryClusterPlots" + l + r"}"
			else:
				if len(args.cluster_plots.split(",")) == 2:
					text_insert = text_insert + r" and \ref{fig:ancestryClusterPlots" + l + r"} clearly indicate"
				else:
					text_insert = text_insert + r", and \ref{fig:ancestryClusterPlots" + l + r"} clearly indicate"
		text=r"""Using the principal components of ancestry as features, we employed the signal processing software Klustakwik \cite{{klustakwik}} to model METSIM datasets as a mixture of Gaussians, identifying clusters, or population groups/subgroups. In order to generate clusters of sufficient size for statistical association tests, we used the first three principal components as features in the clustering algorithm. This number of PC's distinctly separates the five major 1000 Genomes population groups: AFR, AMR, EUR, EAS, and SAS. {0} the population structure in the datasets. In Klustakwik output, cluster 1 is always reserved for outliers, or samples that did not fit into any of the clusters found by the program.""".format(text_insert)
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		i = 0
		for a in args.cluster_plots.split(","):
			i = i + 1
			l = a.split("___")[0]
			p1 = a.split("___")[1]
			p2 = a.split("___")[2]
			text = [r"\begin{figure}[H]",
				r"   \centering",
				r"   \begin{subfigure}{.5\textwidth}",
				r"      \centering",
				r"      \includegraphics[width=\linewidth]{" + p1 + r"}",
				r"      \caption{PC1 vs. PC2}",
				r"      \label{fig:ancestryClusterPc1vs2Plot" + l + r"}",
				r"   \end{subfigure}%",
				r"   \begin{subfigure}{.5\textwidth}",
				r"      \centering",
				r"      \includegraphics[width=\linewidth]{" + p2 + r"}",
				r"      \caption{PC2 vs. PC3}",
				r"      \label{fig:ancestryClusterPc2vs3Plot" + l + r"}",
				r"   \end{subfigure}",
				r"   \caption{Population clusters for " + l + r"}",
				r"   \label{fig:ancestryClusterPlots" + l + r"}",
				r"\end{figure}"]
			f.write("\n"); f.write("\n".join(text).encode('utf-8')); f.write("\n")

		text_insert=""
		i = 0
		for a in args.kg_merged_bim.split(","):
			i = i + 1
			l = a.split("___")[0]
			if i == 1:
				text_insert = l
			else:
				text_insert = text_insert + " > " + l
		text=r"""The resulting clusters are then combined with the nearest 1000 Genomes cohort. Table \ref{{table:ancestryClusterTable}} describes the classification of the datasets using this method. A final population assignment is determined by setting a hierarchy on the genotyping technologies ({0}) and assigning each sample to the population determined using the highest technology. Table \ref{{table:ancestryFinalTable}} describes the final population assignments.""".format(text_insert)
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

		text=r"""\begin{table}[H]
			\caption{Inferred ancestry by dataset and cluster}
			\begin{center}
				\resizebox{\ifdim\width>\columnwidth\columnwidth\else\width\fi}{!}{%
					\pgfplotstabletypeset[
						font=\footnotesize,
						col sep=tab,
						columns={Data,Population,Clusters,Samples},
						column type={>{\fontseries{bx}\selectfont}c},
						columns/Data/.style={column name=, string type, postproc cell content/.append style={/pgfplots/table/@cell content/.add={$\bf}{$},}},
						columns/Population/.style={column name=Population, string type},
						columns/Clusters/.style={column name=Clusters, string type},
						columns/Samples/.style={column name=Samples, string type},
						postproc cell content/.append style={/pgfplots/table/@cell content/.add={\fontseries{\seriesdefault}\selectfont}{}},
						every head row/.style={before row={\toprule}, after row={\midrule}},
						every last row/.style={after row=\bottomrule}
					]{""" + args.cluster_table + r"""}}
			\label{table:ancestryClusterTable}
			\end{center}
		\end{table}
		\begin{table}[H]
		\caption{Final inferred ancestry}
			\begin{center}
				\resizebox{\ifdim\width>\columnwidth\columnwidth\else\width\fi}{!}{%
					\pgfplotstabletypeset[
						font=\footnotesize,
						col sep=tab,
						columns={Population,Samples},
						column type={>{\fontseries{bx}\selectfont}c},
						columns/Population/.style={column name=Population, string type, postproc cell content/.append style={/pgfplots/table/@cell content/.add={$\bf}{$},}},
						columns/Samples/.style={column name=Samples, string type},
						postproc cell content/.append style={/pgfplots/table/@cell content/.add={\fontseries{\seriesdefault}\selectfont}{}},
						every head row/.style={before row={\toprule}, after row={\midrule}},
						every last row/.style={after row=\bottomrule}
					]{""" + args.final_table + r"""}}
			\label{table:ancestryFinalTable}
			\end{center}
		\end{table}"""
		f.write("\n"); f.write(text.encode('utf-8')); f.write("\n")

	print "finished\n"

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--out', help='an output file name with extension .tex', required=True)
	requiredArgs.add_argument('--kg-merged-bim', help='a comma separated list of array labels and bim files from merging array and 1kg data, each separated by 3 underscores', required=True)
	requiredArgs.add_argument('--pca-plots', help='a comma separated list of array labels and two PCA plots (PC1 vs PC2 and PC2 vs PC3), each separated by 3 underscores', required=True)
	requiredArgs.add_argument('--cluster-plots', help='a comma separated list of array labels and two PCA cluster plots (PC1 vs PC2 and PC2 vs PC3), each separated by 3 underscores', required=True)
	requiredArgs.add_argument('--cluster-table', help='an ancestry cluster table', required=True)
	requiredArgs.add_argument('--final-table', help='a final ancestry table', required=True)
	args = parser.parse_args()
	main(args)
