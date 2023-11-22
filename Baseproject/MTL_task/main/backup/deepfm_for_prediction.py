'''
version3.0
'''
from __future__ import print_function
import pandas as pd
import numpy as np
from sklearn.metrics import roc_auc_score
from base_model import DNN, FM, PredictionLayer
from pooling import SequencePoolingLayer
from utils import auc, LossHistory
from tensorflow.python.keras.models import load_model
from tqdm import tqdm
from get_standard_input import get_standard_input
import datetime
import matplotlib.pyplot as plt
import config
import json
import time
import gc
import os
import warnings
import argparse
warnings.filterwarnings('ignore')

def main():
    # 获取日期
    today = str(datetime.date.today()).replace('-', '')
    yesterday = str(datetime.date.today() - datetime.timedelta(days=1)).replace('-', '')
    model_load_path = '/nfs/project/sundike/DeepFM_sdk/model'
    save_log_path = '/nfs/project/sundike/DeepFM_sdk/log'
    save_res_path = '/nfs/project/sundike/DeepFM_sdk/result'
    parser = argparse.ArgumentParser(description='DeepFM')
    # 默认参数
    parser.add_argument('--train_dir', type=str, default='/nfs/project/sundike/DeepFM_sdk/data/', help='训练集路径')
    parser.add_argument('--test_dir', type=str, default='/nfs/project/sundike/DeepFM_sdk/data/' + yesterday +  '/raw_predict_data.csv', help='测试集路径') # yesterday
    parser.add_argument('--use_chunks', type=int, default=1, help='')
    parser.add_argument('--chunk_Size', type=int, default=500000, help='')
    parser.add_argument('--data_nums', type=int, default=0, help='如果按块读取,指定前几块读进, 0表示全读')  # 只做为测试用，一般不要使用
    parser.add_argument('--use_for_train', type=int, default=0, help='模型用于训练还是预测')
    # 特征工程参数
    parser.add_argument('--use_percentceil', type=int, default=0, help='是否使用分位数截断')
    parser.add_argument('--use_bin', type=int, default=1, help='是否对指定特征离散化')
    parser.add_argument('--dense_processmethod', type=str, default='log', help='对统计特征的处理方式')
    args = parser.parse_args()
    args.use_for_train = bool(args.use_for_train)
    args.use_percentceil = bool(args.use_percentceil)
    args.use_bin = bool(args.use_bin)
    # 获取标准输入输出
    print('=' * 10, '> get_standard_input')
    test_model_input, test_target, test_size, pid_num, label_num, day1_input, day1_target, day1_size, day1_pid, day1_label = get_standard_input(
        train_dir=args.train_dir,
        test_dir=args.test_dir,
        use_chunks=args.use_chunks,
        chunk_Size=args.chunk_Size,
        data_nums=args.data_nums,
        use_percentceil=args.use_percentceil,
        use_bin=args.use_bin,
        use_for_train=args.use_for_train,
        dense_processmethod=args.dense_processmethod)
    # 获取预测模型
    print('=' * 10, '> loading model: ', 'deepfm_' + yesterday + '.h5')
    model = load_model(model_load_path + '/deepfm_' + yesterday + '.h5', custom_objects={
        "DNN": DNN,
        "FM": FM,
        "PredictionLayer": PredictionLayer,
        "SequencePoolingLayer": SequencePoolingLayer,
        "auc": auc
        })
    print('=' * 10, '> starting predict')
    predictions_8day = model.predict(test_model_input, batch_size=2 ** 12)
    predictions_today = model.predict(day1_input, batch_size=2 ** 12)
    res_auc_today = roc_auc_score(day1_target, predictions_today)
    res_auc_8day = roc_auc_score(test_target, predictions_8day)
    print('=' * 10, '> AUC_1: {}'.format(round(res_auc_today, 5)))
    print('=' * 10, '> AUC_8: {}'.format(round(res_auc_8day, 5)))
    
    predict_log = dict()
    predict_log['预测时间'] = str(datetime.datetime.now())[:-7]
    predict_log['验证集AUC'] = str(round(res_auc_today, 4))
    predict_log['所用模型'] = 'deepfm_' + yesterday + '.h5'
    predict_log['数据量'] = test_size
    predict_log['pid数量'] = pid_num
    predict_log['正负样本量'] = label_num
    
    with open(save_log_path + '/predict_log.txt', 'a+') as f:
        f.write(str(list(predict_log.items())) + '\n')
    
    print('=' * 10, '> saving results')
    with open(save_res_path + "/" + today + "/results_.csv", 'wb') as f:
        predictions = pd.DataFrame(predictions_8day).values
        np.savetxt(f, predictions, fmt="%.5f")
    print('=' * 10, '> saving done')
    
if __name__ == '__main__':
    main()
