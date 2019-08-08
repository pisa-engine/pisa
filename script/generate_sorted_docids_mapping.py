 #!/usr/bin/env python

import string
from argparse import ArgumentParser

def dump(output, sorted_docids):
    output_file = file(output, "w")
    for newid in range(0, len(sorted_docids)):
        # Note that sorted_docids[newid] is the currid.
        output_file.write(str(sorted_docids[newid]) + " " + str(newid) + "\n")


def main():
    parser = ArgumentParser(description="Take a text file lexicon (e.g. '.documents' or '.urls' \
                            file) and sort it, generating a file mapping '<current ID> <new ID>' \
                            to use with the 'suffle_docids' script.")
    parser.add_argument("documents", help="File containing one document (or URL) per line and \
                        where each line number (starting from zero) represents its docid")
    parser.add_argument("output", help="Output file mapping '<current ID> <new ID>'")
    args = parser.parse_args()

    # Documents dict, where each element maps an id with a document (or url).
    with open(args.documents, "r") as input_file: 
        documents = {docid: string.lower(line[:-1]) for docid, line in enumerate(input_file)}

    # Sorts and then dumps the docs.
    print("Sorting documents...")
    sorted_docids = sorted(documents, key=documents.get)
    print("Writing mapping file...")
    dump(args.output, sorted_docids)

if __name__ == "__main__":
    main()