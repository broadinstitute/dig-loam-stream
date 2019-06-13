import time
import argparse
import hail as hl
import pandas as pd
hl.init()

def main(args=None):

	with hl.hadoop_open(args.input, 'r') as f:
		tbl = pd.read_table(f)

	tbl['TOTAL'] = tbl.sum(axis = 1)

	with hl.hadoop_open(args.output, 'w') as f:
		tbl.to_csv(f, header=True, index=False, sep="\t")

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	requiredArgs = parser.add_argument_group('required arguments')
	requiredArgs.add_argument('--input', help='an input filename', required=True)
	requiredArgs.add_argument('--output', help='an output filename', required=True)
	args = parser.parse_args()
	main(args)
