import pandas as pd
import numpy as np
import argparse
from biomart import BiomartServer

def main(args=None):

	server = BiomartServer("http://grch37.ensembl.org/biomart")
	server.verbose = True
	hsap = server.datasets['hsapiens_snp']
	response = hsap.search({ 'filters': { 'snp_filter': ['rs7903146','rs4458523'] }, 'attributes': ['chr_name','chrom_start','chrom_end','refsnp_id'] }, header = 1)

	x = [a for a in response.iter_lines()]
	df = pd.DataFrame(x[1:],columns=x[0])

	print "reading results from file"
	df=pd.read_table(args.results, low_memory=False)
	df.dropna(subset=[args.p], inplace=True)
	df.reset_index(drop=True, inplace=True)

	print "sorting by p value"
	df.sort_values(by=['pval'],inplace=True)

	print "extracting top 1000 variants"
	df = df.head(n=1000)
	df.to_csv(args.out, header=True, index=False, sep="\t")

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--results', help='a results file name', required=True)
	requiredArgs.add_argument('--p', help='a p-value column name in --results', required=True)
	requiredArgs.add_argument('--out', help='an output filename ending in .png or .pdf', required=True)
	args = parser.parse_args()
	main(args)
