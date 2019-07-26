 #!/usr/bin/env python

import string
from argparse import ArgumentParser

def dump(output, sorted_docids):
    fout = file(output, "w")
    for newid in range(0, len(sorted_docids)):
        # Note that sorted_docids[newid] is the currid.
        fout.write(str(sorted_docids[newid]) + " " + str(newid) + "\n")


def main():
    parser = ArgumentParser(description='Take a document lexicon (e.g. \'.documents\' or\
                            \'.urls\' file) and sort it, generating a file mapping \'<current ID>\
                            <new ID>\' to use with the \'suffle_docids\' script.')
    parser.add_argument('documents', help='File containing one document (or url) per line and \
                        where each line number (starting from zero) represents its docid')
    parser.add_argument('output', help='Output file mapping \'<current ID> <new ID>\'')
    args = parser.parse_args()

    # Documents dict, where each element maps an id with a document (or url).
    documents = dict()
    docid = 0
    for line in open(args.documents, "r"):
        documents[docid] = string.lower(line[:-1])
        docid += 1

    # Sorts and then dumps the docs.
    print("Sorting documents...")
    sorted_docids = sorted(documents, key=documents.get)
    print("Writting mapping file...")
    dump(args.output, sorted_docids)

if __name__ == "__main__":
    main()