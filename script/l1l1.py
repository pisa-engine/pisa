import os
import sys
import logging
import time
import re
from collections import defaultdict, Counter
from heapq import nlargest
import cPickle as pickle
from multiprocessing import Pool
from itertools import imap, izip

import numpy as np
from scipy.optimize import lbfgsb

import theano
import theano.tensor as T

def huber(x, eps):
    return T.where(abs(x) < eps, x**2 / (2 * eps), abs(x) - eps/2.)

l1l1_approx = None

def make_l1l1_approx():
    global l1l1_approx
    if l1l1_approx is not None:
        return

    w = T.vector('w')
    X = T.matrix('X')
    y = T.vector('y')
    lbda = T.scalar('lbda')
    eps = 0.1

    cost = T.sum(huber(T.dot(X, w[:-1]) + w[-1] - y, eps)) \
           + lbda * X.shape[0] * T.sum(huber(w[:-1], eps))

    cost_grad = T.grad(cost, wrt=w)

    l1l1_approx = theano.function([w, X, y, lbda],
                                  (cost, cost_grad),
                                  mode='FAST_RUN')

def solve_l1l1_approx(X, y, lbda):
    make_l1l1_approx()
    f = lambda w, params=(X, y, lbda): l1l1_approx(w, *params)

    x0 = np.zeros(X.shape[1] + 1)
    opt = lbfgsb.fmin_l_bfgs_b(f, x0, bounds=[(0, None)] * x0.shape[0])

    logging.debug(opt[2])
    return opt[0].astype(np.float32)
