import numpy as np


def dist_to_zero(lst):
    lst = np.array(list(map(int, lst.split())))
    zeros = np.where(lst == 0)[0]
    return ', '.join([str(min(np.abs(i - zeros))) for i in np.nonzero(lst)[0]])
