import pandas as pd
from utils import SparseFeat, DenseFeat, VarLenSparseFeat, reduce_mem_usage, getYesterday, gett_2_t_8day, gett_2_t_16day
from input_embedding import get_fixlen_feature_names, get_varlen_feature_names
from data_preprocess import get_dense_feature, log_feature, bin_feature, load_data, load_data_by_chunks
from data_preprocess import categlory_encoder, multihot_encoder_for_train, multihot_encoder_for_test, minmax, percentceil
import config
import datetime
import json
import gc
from tqdm import tqdm, trange

def get_standard_data(
        train_dir='./data/',
        test_dir='./data/',
        use_chunks=False,
        chunk_Size=None,
        data_nums=0,
        use_for_train=True,):
    # 载入数据
    if use_for_train:
        data_dir = train_dir
    else:
        data_dir = test_dir
    if use_chunks:
        chunks = load_data_by_chunks(data_dir, chunkSize=chunk_Size, use_for_train=False)
        pid_buid = pd.read_csv(data_dir, usecols=['pid', 'buid', 'is_click'])
        today = str(datetime.date.today()).replace('-', '')
        pid_buid[['pid', 'buid']].to_csv('/nfs/project/sundike/DeepFM_sdk/result/' + today + '/pid_buid.csv', index=False, header=None, sep=' ')
        data_size = (pid_buid.shape[0], chunks[0].shape[1])
        pid_num = len(pid_buid.pid.unique())
        label_num = dict(zip(pid_buid.is_click.value_counts().index.values, pid_buid.is_click.value_counts().values))
        del pid_buid
        gc.collect()
        print('=' * 10, '> data shape: ', data_size)
        print('=' * 10, '> pid amount: ', pid_num)
        print('=' * 10, '> label nums: ', label_num)
    else:
        data = load_data(data_dir, data_nums)
        data_size = data.shape
        pid_num = len(data.pid.unique())
        label_num = dict(zip(data.is_click.value_counts().index.values, data.is_click.value_counts().values))
        print('=' * 10, '> data shape: ', data_size)
        print('=' * 10, '> pid amount: ', pid_num)
        print('=' * 10, '> label nums: ', label_num)
 
    if use_for_train:
        print("=" * 10, "> train data check correctness")
        print("=" * 10, "> actual time : ", sorted(data.dt.unique()))
        print("=" * 10, "> correct time: ", gett_2_t_16day())
        if sorted(data.dt.unique()) != gett_2_t_16day():
            print("=" * 10, "> WARNING! Wrong train data time")
            print("=" * 10, "> WARNING! Wrong train data time")
            print("=" * 10, "> WARNING! Wrong train data time")
    else:
        print("=" * 10, "> test data check correctness")
        print("=" * 10, "> actual time : ", sorted(chunks[0].dt.unique()))
        print("=" * 10, "> correct time: ", gett_2_t_8day() + getYesterday())
        if sorted(chunks[0].dt.unique()) != gett_2_t_8day() + getYesterday():
            print("=" * 10, "> WARNING! Wrong test data time")
            print("=" * 10, "> WARNING! Wrong test data time")
            print("=" * 10, "> WARNING! Wrong test data time")
    return chunks, data_size, pid_num, label_num

def get_standard_input(
        data,
        use_percentceil=False,
        use_bin=True,
        use_for_train=True,
        dense_processmethod='log'):
    # 对disp_time(展示时间)做细粒度时间特征
    data['disp_hour'] = pd.to_datetime(data['disp_time']).dt.hour
    # version2.0 去掉disp_minute特征
    data.drop(['disp_time'], axis=1, inplace=True)
    
    # 对click_buid_list做特征
    # 1.将list拉伸成固定长度(buid数量),如果对应广告点击,则标记1,反之标记0
    def match_buid_isclick(row):
        click_buid = ""
        for i in config.buid:
            if i in set(row.split('|')):
                click_buid = click_buid + "1|"
            else:
                click_buid = click_buid + "0|"
        return click_buid[:-1]
    data['click_buid_list_isclick'] = data['click_buid_list'].apply(match_buid_isclick)
    # 2.将list拉伸成固定长度(buid数量),记录对应广告点击数量
    def get_n_from_list(list_, key):
        count = 0
        for i in list_:
            if key == i:
                count += 1
        return count   
    def match_buid_clicknum(row):
        click_buid = ""
        list_ = row.split('|')
        for i in buid:
            if i in set(list_):
                click_buid = click_buid + str(get_n_from_list(list_, i)) + "|"
            else:
                click_buid = click_buid + "0|"
        return click_buid[:-1]
    # data['click_buid_list_clicknum'] = data['click_buid_list'].apply(match_buid_clicknum)

    # 获取类别与统计特征
    dense_features = get_dense_feature(
        data,
        config.sparse_features + config.sparse_features_multi_value,
        config.drop_features)

    # 是否采用分位数截断
    if use_percentceil:
        '''
        limit=90:    按分布90%进行截断
        use_log1p=1: 使用ln(1+x)进行平滑处理
        '''
        data = percentceil(data, config.need_precentceil_feature, limit=90, use_log1p=True)

    # 是否对指定特征离散化(分桶)
    if use_bin:
        '''
        method='custom': 拍脑袋分桶
        method=‘pd.cut’: 等分分桶，可以设置分桶数bin_nums
        '''
        data, bin_features = bin_feature(data, config.need_bin_feature)

    # 对类别特征进行encoder
    if use_for_train:
        data = categlory_encoder(data, config.sparse_features + bin_features)
    else:
        for col in tqdm(config.sparse_features + bin_features, desc='categlory_encoder: predict'):
            with open('../data/dict_data/' + col + '.json', 'r', encoding='utf-8') as f:
                match_dict = json.load(f)
            data[col] = data[col].astype(str).apply(lambda x: x if x in match_dict else 'UNK')
            data[col] = data[col].map(match_dict)

    # 对统计特征采用minmax或者log或者minmax+log
    if dense_processmethod == 'minmax':
        '''
        可调整minmax上下限
        dlimit=0: min取值,默认0
        ulimit=1: max取值,默认1
        '''
        data = minmax(data, dense_features, dlimit=0, ulimit=1)
    elif dense_processmethod == 'log':
        data = log_feature(data, config.need_log10_feature, config.need_ln_feature)
    elif dense_processmethod == 'both':
        data = log_feature(data, config.need_log10_feature, config.need_ln_feature)
        data = minmax(data, dense_features, dlimit=0, ulimit=1)
    else:
        pass   
   
    # 优化内存
    if use_for_train:
        print('=' * 10, '> reduce memory')
        data_multihot = data[config.sparse_features_multi_value]
        data.drop(config.sparse_features_multi_value, axis=1, inplace=True)
        data = reduce_mem_usage(data, verbose=True) 

    # 对multi-value类别的特征进行encoder
    if use_for_train:
        multihot_feature_lists, multihot_maxlen_lists, multihot_key2index_lists = multihot_encoder_for_train(data_multihot, config.sparse_features_multi_value)
        del multihot_feature_lists, multihot_maxlen_lists
        fixlen_feature_columns = [SparseFeat(col, data[col].nunique()) for col in config.sparse_features + bin_features] + [DenseFeat(col, 1, ) for col in dense_features]
        varlen_feature_columns = [
            VarLenSparseFeat(config.sparse_features_multi_value[i], len(multihot_key2index_lists[i]) + 1,
                             multihot_maxlen_lists[i], 'mean') for i in range(len(config.sparse_features_multi_value))]
        linear_feature_columns = fixlen_feature_columns + varlen_feature_columns
        dnn_feature_columns = fixlen_feature_columns + varlen_feature_columns
        fixlen_feature_names = get_fixlen_feature_names(linear_feature_columns + dnn_feature_columns)
        print('=' * 10, '> process exception data')
        for col in tqdm(data.columns, desc='process exception data'):
            trange(1, desc='log10p: ' + col, position=1, bar_format='{desc}')
            data[col] = data[col].replace('\\N', 0)
        print('=' * 10, '> process exception done')
        data_model_input = [data[name].values for name in fixlen_feature_names] + [col for col in multihot_feature_lists]
        with open('../data/dict_data/multihot_dict.json', 'w', encoding='utf-8') as f:
            json.dump(multihot_key2index_lists, f)
        print('=' * 10, '> check data: ', len(data_model_input))
        return data_model_input, data['is_click'].values, linear_feature_columns, dnn_feature_columns, data_size, pid_num, label_num
    else: 
        with open('../data/dict_data/multihot_dict.json', 'r', encoding='utf-8') as f:
            multihot_key2index_lists = json.load(f)
        multihot_feature_lists = multihot_encoder_for_test(data_multihot, config.sparse_features_multi_value, multihot_key2index_lists)
        fixlen_feature_columns = [SparseFeat(col, data[col].nunique()) for col in config.sparse_features + bin_features] + \
                                 [DenseFeat(col, 1, ) for col in dense_features]
        fixlen_feature_names = get_fixlen_feature_names(fixlen_feature_columns) 
        print('=' * 10, '> process exception data')
        for col in tqdm(data.columns, desc='process exception data'):
            trange(1, desc='log10p: ' + col, position=1, bar_format='{desc}')
            data[col] = data[col].replace('\\N', 0)
        print('=' * 10, '> process exception done')
        data_model_input = [data[name].values for name in fixlen_feature_names] + \
                           [col for col in multihot_feature_lists]
        del multihot_feature_lists
        lastday = str(datetime.date.today() - datetime.timedelta(days=1)).replace('-', '')
        data_multihot_1day = data_multihot[data_multihot.dt == int(lastday)]
        multihot_feature_lists = multihot_encoder_for_test(data_multihot_1day, config.sparse_features_multi_value, multihot_key2index_lists)
        del data_multihot
        print('=' * 10, '> predict data: ', lastday)
        data_1day = data[data.dt == int(lastday)]
        data_1day_model_input = [data_1day[name].values for name in fixlen_feature_names] + \
                                [col for col in multihot_feature_lists]
        del multihot_feature_lists
        return data_model_input, data['is_click'].values, data_1day_model_input, data_1day['is_click']
