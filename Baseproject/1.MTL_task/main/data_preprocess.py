import pandas as pd
import numpy as np
import os
import json
import time
import gc
import datetime
from sklearn.preprocessing import MinMaxScaler, LabelEncoder
from tensorflow.python.keras.preprocessing.sequence import pad_sequences
from utils import reduce_mem_usage
from tqdm import tqdm, trange

def load_data(data_dir, n):
    """默认加载数据方式
    :param data_dir   :
    :param n          : 如果数据量太大,被按块存储,可以读取前n块,主要用于验证
    """
    # version 2.0 增加按块读取(仅支持读取csv文件)
    print('=' * 10, '> loading data')
    data = []
    data_set = os.listdir(data_dir)
    if n == 0:
        for i in data_set:
            if 'csv' not in i.split('.'):
                continue
            data.append(reduce_mem_usage(pd.read_csv(data_dir + i)))
            print('=' * 10, '> already load: ', i)
    else:
        for i in data_set[:n]:
            if 'csv' not in i.split('.'):
                continue
            data.append(reduce_mem_usage(pd.read_csv(data_dir + i)))
            print('=' * 10, '> already load ' + i)
    data = pd.concat(data, ignore_index=True)
    data = data.sample(frac=1, random_state=2019)
    if 'test.csv' in data_dir.split('/'):
        today = str(datetime.date.today()).replace('-', '')
        data[['pid', 'buid']].to_csv('D:/z_personal_file/DiDi/ycc/ycc/MTL_deepfm/MTL_deepfm/result/' + today + '/pid_buid.csv', index=False, header=None, sep=' ')
    print('=' * 10, '> loading done')
    return data

def load_data_by_chunks(data_dir, chunkSize=None, use_for_train=True):
    """按chunks读取数据
    :param data_dir   : 数据路径,默认./data/
    :param chunkSize  : 块的大小,默认100000
    """
    # version 3.0 增加chunks模块
    if chunkSize is None:
        chunkSize = 100000
    print('=' * 10, '> loading data')
    data = pd.read_csv(data_dir, iterator=True)
    loop = True
    chunks = []
    count = 0
    start = time.time()
    while loop:
        try:
            chunk = reduce_mem_usage(data.get_chunk(chunkSize), prefix=str(count + 1), verbose=False)
            chunks.append(chunk)
            count += 1
            time_elapsed = time.time() - start
            #if count == 2:
            #    break
            if count % 4 == 0:
                print('=' * 10, '> alread load {} data, complete in {:.0f}m {:.0f}s.'.format(chunkSize * count, time_elapsed // 60, time_elapsed % 60))
        except StopIteration:
            loop = False
            print('=' * 10, '> Iteration is stopped.')
    if not use_for_train:
        print('=' * 10, '> loading done')
        return chunks[:14]
    data = pd.concat(chunks, axis=0, ignore_index=True).sample(frac=1, random_state=2019)
    print('=' * 10, '> pid unique num: ', len(data.pid.unique()))
    if 'raw_predict_data.csv' in data_dir.split('/'):
        today = str(datetime.date.today()).replace('-', '')
        #data[['pid', 'buid']].to_csv('/nfs/project/chuchu/MTL_deepfm/result/' + today + '/pid_buid.csv', index=False, header=None, sep=' ')
    print('=' * 10, '> loading done')
    del chunks
    gc.collect()
    return data

def get_dense_feature(data, sparse_features, drop_features):
    print('=' * 10, '> get_dense_feature')
    """获取dense_feature
    :param data           : 数据
    :param sparse_features: 类别特征
    :param drop_features  : 需要丢弃的特征
    """
    dense_features = [x for x in data.columns.tolist() if x not in sparse_features + drop_features]
    return dense_features


def percentceil(data, need_precentceil_feature, limit=90, use_log1p=True):
    """分位数截断
    :param data                    :
    :param need_precentceil_feature:
    :param limit                   :
    :param use_log1p               :
    """
    print('=' * 10, '> percentceil')
    for fea in tqdm(need_precentceil_feature, desc='percentceil'):
        ulimit = np.percentile(data[fea], limit)
        data.loc[data[fea] > ulimit, fea] = ulimit
    if use_log1p:
        print('=' * 10, '> percentceil/log1p')
        for fea in tqdm(need_precentceil_feature, desc='percentceil/log1p'):
            data[fea] = data[fea].apply(lambda x: np.log1p(x))
    return data

def bin_feature(data, need_bin_feature):
    """分桶
    :param data            :
    :param need_bin_feature:
    """
    print('=' * 10, '> bin')

    def map_default_pattern_1(row):
        if row == 0:
            return 0
        elif 0 < row <= 0.5:
            return 1
        elif 0.5 < row < 1:
            return 2
        else:
            return 3

    def map_default_pattern_2(row):
        if row == 0:
            return 0
        elif 0 < row <= 0.2:
            return 1
        elif 0.2 < row <= 0.5:
            return 2
        elif 0.5 < row < 1:
            return 3
        else:
            return 4

    def map_default_pattern_3(row):
        if row < 18:
            return 0
        elif 18 <= row < 30:
            return 1
        elif 30 <= row < 45:
            return 2
        elif 45 <= row < 55:
            return 3
        else:
            return 4

    data['age_bin'] = data['age'].apply(map_default_pattern_3)###
    data['gas_coupon_rate_total_bin'] = data['gas_coupon_rate_total'].apply(map_default_pattern_2)##
    data['gas_coupon_rate_30d_bin'] = data['gas_coupon_rate_30d'].apply(map_default_pattern_2)###
    data['gas_coupon_rate_60d_bin'] = data['gas_coupon_rate_60d'].apply(map_default_pattern_2)###
    data['gas_coupon_rate_90d_bin'] = data['gas_coupon_rate_90d'].apply(map_default_pattern_2)###
    data['gas_coupon_rate_180d_bin'] = data['gas_coupon_rate_180d'].apply(map_default_pattern_2) ###
    return data, [fea + '_bin' for fea in need_bin_feature]


def categlory_encoder(data, sparse_features):
    """
    :param data: 
    :param sparse_features_not_fixed: 
    :return: 
    """
    print('=' * 10, '> categlory_encoder')
    today = str(datetime.date.today()).replace('-', '')
    with open('D:/z_personal_file/DiDi/ycc/ycc/MTL_deepfm/MTL_deepfm/data/dict_data_info.txt', 'w', encoding='utf-8') as f:
        f.write('use for model deepfm_' + today + '.h5')
    for col in tqdm(sparse_features, desc='categlory_Encoder'):
        trange(1, desc='categlory_encoder: ' + col, position=1, bar_format='{desc}')
        nf_dict = dict()
        nf_dict['UNK'] = -1
        idx = 0
        for value in data[col].values:
            if str(value) not in nf_dict:
                nf_dict[str(value)] = idx
                idx += 1
            else:
                continue
        with open('D:/z_personal_file/DiDi/ycc/ycc/MTL_deepfm/MTL_deepfm/data/dict_data/' + col + '.json', 'w', encoding='utf-8') as f:
            json.dump(nf_dict, f)
        data[col] = data[col].astype(str).map(nf_dict)
    return data


def multihot_encoder_for_train(data, sparse_features_multi_value):
    """
    :param data: 
    :param sparse_features_multi_value: 
    :return: 
    """
    print('=' * 10, '> multihot_encoder_for_train')
   
    def toset(row): # 去重,这样只能反应用户对uid的点击序列
        click_buid = ""
        for buid in set(row.split('|')):
            click_buid = click_buid + buid + "|"
        return click_buid[:-1] 
   
    def split(x):
        key_ans = x.split('|')
        for key in key_ans:
            if key not in key2index:
                # Notice : input value 0 is a special "padding",so we do not use 0 to encode valid feature for multi-hot input
                key2index[key] = len(key2index) + 1
        return list(map(lambda x: key2index[x], key_ans))

    multihot_feature_lists = []
    multihot_maxlen_lists = []
    multihot_key2index_lists = []
    for col in tqdm(sparse_features_multi_value, desc='multihot_Encoder_for_Train'):
        trange(1, desc='multihot_encoder: ' + col, position=1, bar_format='{desc}')
        key2index = dict()
        if col == 'click_buid_list':
            data[col] = data[col].apply(toset)
        col_list = list(map(split, data[col].values))
        col_length = np.array(list(map(len, col_list)))
        #max_length = max(col_length)
        max_length = 49 # 将max_length写死(buid数量)
        col_list = pad_sequences(col_list, maxlen=max_length, padding='post', )
        multihot_feature_lists.append(col_list)
        multihot_maxlen_lists.append(max_length)
        multihot_key2index_lists.append(key2index)
    return multihot_feature_lists, multihot_maxlen_lists, multihot_key2index_lists


def multihot_encoder_for_test(data, sparse_features_multi_value, multihot_key2index_lists):
    """
    :param data: 
    :param sparse_features_multi_value: 
    :param multihot_key2index_lists: 
    :return: 
    """
    print('=' * 10, '> multihot_encoder_for_test')
    count = 0

    def toset(row): # 去重,这样只能反应用户对uid的点击序列
        click_buid = ""
        for buid in set(row.split('|')):
            click_buid = click_buid + buid + "|"
        return click_buid[:-1]

    def match(x):
        key_ans = x.split('|')
        for key in key_ans:
            if key not in multihot_key2index_lists[count]:
                multihot_key2index_lists[count][key] = -1
        return list(map(lambda x: multihot_key2index_lists[count][x], key_ans))

    multihot_feature_lists = []
    for col in tqdm(sparse_features_multi_value, desc='multihot_Encoder_for_Test'):
        trange(1, desc='multihot_encoder: ' + col, position=1, bar_format='{desc}')
        if col == 'click_buid_list':
            data[col] = data[col].apply(toset)
        col_list = list(map(match, data[col].values))
        col_length = np.array(list(map(len, col_list)))
        #max_length = max(col_length)
        max_length = 49 # 将max_length写死(buid数量)
        col_list = pad_sequences(col_list, maxlen=max_length, padding='post', )
        multihot_feature_lists.append(col_list)
        count += 1
    return multihot_feature_lists


def log_feature(data, need_log10_feature, need_ln_feature):
    """
    :param data: 
    :param need_log10_feature: 
    :param need_ln_feature: 
    :return: 
    """
    print('=' * 10, '> log10p_feature')
    for fea in tqdm(need_log10_feature, desc='log10p'):
        trange(1, desc='log10p: ' + fea, position=1, bar_format='{desc}')
        try:
            data[fea] = data[fea].apply(lambda x: np.log10(1 + x) if x>=0 else x)###
        except:
            print('[', fea, '] has some problem, replcae \\N to 0')
            data[fea] = data[fea].replace('\\N', 0).astype(float)
            data[fea] = data[fea].apply(lambda x: np.log10(1 + x)if x>=0 else x)##
    tmp = data.isnull().any().sum()####
    print('the nan of log10 :',tmp)####

    print('=' * 10, '> log1p_feature')
    for fea in tqdm(need_ln_feature, desc='log1p'):
        trange(1, desc='log1p: ' + fea, position=1, bar_format='{desc}')
        try:
            data[fea] = data[fea].apply(lambda x: np.log1p(x) if x>=0 else x)###
        except:
            print('[', fea, '] has some problem, replcae \\N to 0')
            data[fea] = data[fea].replace('\\N', 0).astype(float)
            data[fea] = data[fea].apply(lambda x: np.log1p(x) if x>=0 else x)###
    
    tmp = data.isnull().any().sum()####
    print('the nan of log1p :',tmp)####
    return data


def minmax(data, dense_features, dlimit=0, ulimit=1):
    """
    :param data: 
    :param dense_features: 
    :param dlimit: 
    :param ulimit: 
    :return: 
    """
    print('=' * 10, '> MinMaxScaler')
    mms = MinMaxScaler(feature_range=(dlimit, ulimit))
    for col in tqdm(dense_features, desc='MinMaxScaler'):
        try:
            data[col] = mms.fit_transform(data[col].values.reshape(-1, 1))
        except:
            print(col, ' has some problem, replace \\N to 0')
            data[col] = data[col].replace('\\N', 0).astype(float)
            data[col] = mms.fit_transform(data[col].values.reshape(-1, 1))
    return data

