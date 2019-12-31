 #!/usr/bin/env python

import string
from argparse import ArgumentParser

def dump(output, sorted_docids):
    output_file = file(output, "w")
    for newid in range(0, len(sorted_docids)):
        # Note that sorted_docids[newid] is the currid.
        output_file.write(str(sorted_docids[newid]) + " " + str(newid) + "\n")


def main():
    parser = ArgumentParser(description="Take a text file of numerical values (one line per document) \
                             sort it in decreasing order, generating a file mapping '<current ID> <new ID>' \
                            to use with the 'suffle_docids' script.")
    parser.add_argument("values", help="File containing one value per line and \
                        where each line number (starting from zero) represents its docid")
    parser.add_argument("output", help="Output file mapping '<current ID> <new ID>'")
    args = parser.parse_args()

    # Documents dict, where each element maps an id with a document (or url).
    with open(args.values, "r") as input_file: 
        documents = {docid: int(line[:-1]) for docid, line in enumerate(input_file)}

    # Sorts and then dumps the docs.
    print("Sorting documents...")
    sorted_docids = sorted(documents, key=documents.get, reverse=True)
    print("Writing mapping file...")
    dump(args.output, sorted_docids)

if __name__ == "__main__":
    main()
