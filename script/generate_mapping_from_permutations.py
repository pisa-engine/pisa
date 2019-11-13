 #!/usr/bin/env python

from argparse import ArgumentParser
import pandas as pd

def generate_mapping_from_permutations(original_ordering_file, new_ordering_file, output_mapping):
    original = pd.read_csv(original_ordering_file, header = None, names=['doc'])
    original['original_id'] = original.index.astype(int)
    new = pd.read_csv(new_ordering_file, header = None, names=['doc'])
    new['new_id'] = new.index.astype(int)
    merge = original.set_index('doc').join(new.set_index('doc'))
    merge.to_csv(output_mapping, index=False, header=None, sep=' ')


def main():
    parser = ArgumentParser(description="Takes two files containing a list of documents and generates a file with mappings between position in the first file to the position in the second according to the standard format '<ID> <new ID>' \
                            to use with the 'suffle_docids' script.")
    parser.add_argument("original_documents", help="File containing one document per line and \
                        where each line number (starting from zero) represents its docid")
    parser.add_argument("new_documents", help="File containing one document per line and \
                        where each line number (starting from zero) represents its docid")
    parser.add_argument("output", help="Output file mapping '<ID> <new ID>'")
    args = parser.parse_args()

    generate_mapping_from_permutations(args.original_documents, args.new_documents, args.output)
if __name__ == "__main__":
    main()
