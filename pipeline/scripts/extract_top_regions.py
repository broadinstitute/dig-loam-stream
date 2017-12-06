import pandas as pd
import numpy as np
import argparse

def main(args=None):

	print "reading results from file"
	df=pd.read_table(args.results, low_memory=False, compression="gzip")
	df.dropna(subset=[args.p], inplace=True)
	df.reset_index(drop=True, inplace=True)

	if df.shape[0] >= 1000000:
		sig = 5.4e-8
	else:
		sig = 0.05 / df.shape[0]
	print "significance level set to p-value = {0:.3g} (-1*log10(p-value) = {1:.3g})".format(sig, -1 * np.log10(sig))

	df_sig = df[df[args.p] <= sig]
	if df_sig.shape[0] > 0:
		df_sig.reset_index(drop=True, inplace=True)
		print "{0:d} genome wide significant variants".format(df_sig.shape[0])
	
		df_sig = df_sig.assign(start=df_sig[args.pos].values - 100000)
		df_sig = df_sig.assign(end=df_sig[args.pos].values + 100000)
	
		df_sig = df_sig[[args.chr,'start','end']]
		df_sig.sort_values([args.chr,'start'], inplace=True)
		df_sig.reset_index(drop=True, inplace=True)
	
		print "extracting variants in significant regions"
		for index, row in df_sig.iterrows():
			if index == 0:
				out = df_sig.loc[[index]]
			else:
				if row[args.chr] != df_sig.loc[index-1,args.chr]:
					out = out.append(row, ignore_index=True)
				elif df_sig.loc[index-1,'start'] <= row['start'] and row['start'] <= df_sig.loc[index-1,'end'] and row['end'] > df_sig.loc[index-1,'end']:
					out.loc[out.shape[0]-1,'end'] = row['end']
				elif row['start'] > df_sig.loc[index-1,'end']:
					out = out.append(row, ignore_index=True)
	
		out['top_variant'] = ""
		out['top_pos'] = 0
		out['top_pval'] = 0
		for index, row in out.iterrows():
			df_region = df.loc[(df[args.pos] >= row['start']) & (df[args.pos] <= row['end'])].reset_index(drop=True)
			out.loc[index,'top_variant'] = df_region.loc[df_region[args.p].idxmin(), 'id']
			out.loc[index,'top_pos'] = df_region.loc[df_region[args.p].idxmin(), args.pos]
			out.loc[index,'top_pval'] = df_region.loc[df_region[args.p].idxmin(), args.p]
		out.sort_values(['top_pval'], inplace=True)
	else:
		out = pd.DataFrame({args.chr: [], 'start': [], 'end': [], 'top_variant': [], 'top_pos': [], 'top_pval': []})

	out[[args.chr,'start','end','top_variant','top_pos','top_pval']].to_csv(args.out, header=False, index=False, sep="\t")

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--results', help='a results file name', required=True)
	requiredArgs.add_argument('--chr', help='a chromosome column name in --results', required=True)
	requiredArgs.add_argument('--pos', help='a position column name in --results', required=True)
	requiredArgs.add_argument('--p', help='a p-value column name in --results', required=True)
	requiredArgs.add_argument('--out', help='an output filename', required=True)
	args = parser.parse_args()
	main(args)
