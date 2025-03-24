import pickle

import numpy as np
import pickle as pkl
from collections import defaultdict
import random
# from sklearn.metrics.pairwise import euclidean_distances
import argparse
import datetime
import json

def construct_list(data_dir, max_time_len):
    user, profile, itm_spar, itm_dens, label, pos, list_len = pickle.load(open(data_dir, 'rb'))
    print(len(user), len(itm_spar))
    cut_itm_dens, cut_itm_spar, cut_label, cut_pos, cut_usr_spar, cut_usr_dens, de_label, cut_hist_pos = [], [], [], [], [], [], [], []
    for i, itm_spar_i, itm_dens_i, label_i, pos_i, list_len_i in zip(list(range(len(label))),
                                                    itm_spar, itm_dens, label, pos, list_len):

        if len(itm_spar_i) >= max_time_len:
            cut_itm_spar.append(itm_spar_i[: max_time_len])
            cut_itm_dens.append(itm_dens_i[: max_time_len])
            cut_label.append(label_i[: max_time_len])
            # de_label.append(de_lb[: max_time_len])
            cut_pos.append(pos_i[: max_time_len])
            list_len[i] = max_time_len
        else:
            cut_itm_spar.append(itm_spar_i + [np.zeros_like(np.array(itm_spar_i[0])).tolist()] * (max_time_len - len(itm_spar_i)))
            cut_itm_dens.append(itm_dens_i + [np.zeros_like(np.array(itm_dens_i[0])).tolist()] * (max_time_len - len(itm_dens_i)))
            cut_label.append(label_i + [0 for _ in range(max_time_len - list_len_i)])
            # de_label.append(de_lb + [0 for _ in range(max_time_len - list_len_i)])
            cut_pos.append(pos_i + [j for j in range(list_len_i, max_time_len)])

    return user, profile, cut_itm_spar, cut_itm_dens, cut_label, cut_pos, list_len