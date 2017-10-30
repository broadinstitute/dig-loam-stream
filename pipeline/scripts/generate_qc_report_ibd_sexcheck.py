import argparse
import numpy as np
import pandas as pd

def main(args=None):

	## open latex file for writing
	with open(args.out,'w') as f:

		print "writing duplicates and excessive sharing section"
		f.write("\n"); f.write(r"\subsection{Duplicates and Excessive Sharing of Identity-by-Descent (IBD)}"); f.write("\n")

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
	requiredArgs.add_argument('--filtered-bim', help='a comma separated list of array labels and filtered bim files, each separated by 3 underscores', required=True)
	requiredArgs.add_argument('--kin0-related', help='a comma separated list of array labels and kin0 related files, each separated by 3 underscores', required=True)
	requiredArgs.add_argument('--famsizes', help='a comma separated list of array labels and famsizes files, each separated by 3 underscores', required=True)
	requiredArgs.add_argument('--sexcheck-problems', help='a comma separated list of array labels and sexcheck problems files, each separated by 3 underscores', required=True)
	args = parser.parse_args()
	main(args)
