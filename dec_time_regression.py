#!/usr/bin/env python

import os
import sys
import logging
import json
import collections
import random

import numpy as np
import l1l1

from ext import baker

@baker.command
def parse_data(filename, output_filename):
    import pandas

    logging.info('Reading data from %s', filename)
    with open(filename) as fin:
        data = [json.loads(line) for line in fin]

    pd = pandas.DataFrame(data)
    logging.info('Saving dataframe to %s', output_filename)
    pd.to_pickle(output_filename)


@baker.command
def train(filename):
    import pandas
    logging.info('Reading data from %s', filename)
    df = pandas.read_pickle(filename)

    for t, gdf in df.groupby('type'):
        logging.info('Block type %d ------', t)

        idxs = list(gdf.index)
        random.shuffle(idxs)

        split_point = int(0.8 * len(idxs))
        training = gdf.ix[idxs[:split_point]]
        test = gdf.ix[idxs[split_point:]]

        median = training['time'].median()
        logging.info('Median time %.3f', median)

        median_pred_err = np.mean(np.abs(test['time'] - median))
        logging.info('Error for constant predictor %.3f',
                     median_pred_err)

        to_drop = ['type', 'time', 'n', 'entropy']
        training_X = training.drop(to_drop, axis=1)
        test_X = test.drop(to_drop, axis=1)

        opt = l1l1.solve_l1l1_approx(training_X.values, training['time'], 0.01)
        weights = opt[:-1]
        bias = opt[-1]

        predict = lambda X: np.dot(X.values, weights) + bias

        lr_pred_err = np.mean(np.abs(predict(test_X) - test['time']))

        logging.info('Error for linear predictor %.3f',
                     lr_pred_err)

        weights_str = ' + '.join('%.3f * %s' % (coef, col)
                                 for coef, col in zip(weights, training_X.columns.values)
                                 if abs(coef) > 1e-08) \
                                    + ' + %.3f' % bias
        logging.info('Linear params: %s', weights_str)

        # output
        fields = ['type', t, 'bias', bias]

        for coef, col in zip(weights, training_X.columns.values):
            fields += [col, coef]

        print '\t'.join(map(str, fields))


if __name__ == '__main__':
    random.seed(1729)
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s:%(levelname)s: %(message)s')
    baker.run()
