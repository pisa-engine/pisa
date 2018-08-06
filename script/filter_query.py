#!/usr/bin/env python3
import csv
import argparse


def main():
    parser = argparse.ArgumentParser(description='Extract queries of provided length')
    parser.add_argument('-q','--query-filename', help='Query filename', required=True)
    parser.add_argument('-n','--term-count', help='Number of terms', required=True, type=int)
    parser.add_argument('-l','--include-longer', help='If set it will extract also queries longer than length provided', default=False, action='store_true', dest='include_longer')
    args = parser.parse_args()

    with open(args.query_filename) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter='\t')
        for row in csv_reader:
            condition = (len(row) >= args.term_count) if args.include_longer else (len(row) == args.term_count)
            if(condition):
                print('\t'.join(row))

if __name__ == '__main__':
    main()
