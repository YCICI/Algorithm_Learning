'''
version3.0
'''
from __future__ import print_function
import pandas as pd
import numpy as np
from sklearn.metrics import roc_auc_score,mean_squared_error, mean_absolute_error
from base_model import DNN, FM, PredictionLayer
from pooling import SequencePoolingLayer
from utils import auc, LossHistory
from tensorflow.python.keras.models import load_model
from tqdm import tqdm
from get_standard_input_predict import get_standard_data, get_standard_input
import datetime
import matplotlib.pyplot as plt
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
    model_load_path = '/nfs/project/chuchu/MTL_deepfm/model'
    save_log_path = '/nfs/project/chuchu/MTL_deepfm/log'
    save_res_path = '/nfs/project/chuchu/MTL_deepfm/result'
    parser = argparse.ArgumentParser(description='DeepFM_MTL')
    # 默认参数
    parser.add_argument('--test_dir', type=str, default='/nfs/project/chuchu/MTL_deepfm/data/'+yesterday+'/cvr_ltv_test.csv', help='测试集路径') 
    parser.add_argument('--use_chunks', type=int, default=1, help='')
    parser.add_argument('--chunk_Size', type=int, default=500000, help='')
    parser.add_argument('--data_nums', type=int, default=0, help='如果按块读取,指定前几块读进, 0表示全读')  # 只做为测试用，一般不要使用
    parser.add_argument('--use_for_train', type=int, default=0, help='模型用于训练还是预测')
    parser.add_argument('--task1_name', type=str, default='pLTV', help='')
    parser.add_argument('--task2_name', type=str, default='pCVR', help='')
    # 特征工程参数
    parser.add_argument('--use_percentceil', type=int, default=0, help='是否使用分位数截断')
    parser.add_argument('--use_bin', type=int, default=1, help='是否对指定特征离散化')
    parser.add_argument('--dense_processmethod', type=str, default='log', help='对统计特征的处理方式')
args = parser.parse_args()
    args.use_for_train = bool(args.use_for_train)
    args.use_percentceil = bool(args.use_percentceil)
    args.use_bin = bool(args.use_bin)
    # 获取预测模型
    print('=' * 10, '> loading model: ', 'deepfm_mtl_' + yesterday + '.h5')####
    model = load_model(model_load_path + '/deepfm_mtl_' + yesterday + '.h5', custom_objects={ 
        "DNN": DNN, 
        "FM": FM, 
        "PredictionLayer": PredictionLayer, 
        "SequencePoolingLayer": SequencePoolingLayer,
        "auc": auc
        })
    # 获取标准数据
    print('=' * 10, '> get_standard_data for task1-{} & {}'.format(args.task1_name, args.task2_name))
    chunks, test_size, pid_num, label_num = get_standard_data(
        test_dir=args.test_dir,
        use_chunks=args.use_chunks,
        chunk_Size=args.chunk_Size,
        data_nums=args.data_nums,
        use_for_train=args.use_for_train)
    # 获取标准输入
    predictions_8day_sum_task1 = []
    predictions_today_sum_task1 = []
    target_sum_task1 = []
    predictions_8day_sum_task2 = []
    predictions_today_sum_task2 = []
    target_sum_task2 = []
    for i in range(len(chunks)):
        print('=' * 10, '> get chunk {}'.format(i + 1))
        test_model_input, test_target, day1_input, day1_target = get_standard_input(
            chunks[i],
            use_percentceil=args.use_percentceil,
            use_bin=args.use_bin,
            use_for_train=args.use_for_train,
            dense_processmethod=args.dense_processmethod)
        print('========>day1_input : ', day1_input)#####
        print('========>day1_target : ', day1_target)#####
        predictions_8day = model.predict(test_model_input, batch_size=2 ** 12)
        predictions_today = model.predict(day1_input, batch_size=2 ** 12)
        predictions_8day_task1 = [float(x) for x in predictions_8day[0]]
        predictions_today_task1 = [float(x) for x in predictions_today[0]]
        predictions_8day_task2 = [float(x) for x in predictions_8day[1]]
        predictions_today_task2 = [float(x) for x in predictions_today[1]]
        test_target_task1 = [float(x) for x in day1_target[0]]
        test_target_task2 = [float(x) for x in day1_target[1]]
        predictions_8day_sum_task1 = predictions_8day_sum_task1 + predictions_8day_task1
        predictions_today_sum_task1 = predictions_today_sum_task1 + predictions_today_task1
        predictions_8day_sum_task2 = predictions_8day_sum_task2 + predictions_8day_task2
        predictions_today_sum_task2 = predictions_today_sum_task2 + predictions_today_task2
        target_sum_task1 = target_sum_task1 + test_target_task1
        target_sum_task2 = target_sum_task2 + test_target_task2
        del predictions_8day, predictions_today, predictions_8day_task1, predictions_8day_task2, predictions_today_task1, predictions_today_task2, test_target_task1, test_target_task2
    gc.collect()
    print('=' * 10, '> starting predict')
    res_auc_today_task1 = roc_auc_score(target_sum_task1, predictions_today_sum_task1)
    #res_mse_today_task1 = mean_squared_error(target_sum_task1,predictions_today_sum_task1)#####
    print('=' * 10, '> task1- {} AUC: {}'.format(args.task1_name, round(res_auc_today_task1, 5)))#####
    res_auc_today_task2 = roc_auc_score(target_sum_task2, predictions_today_sum_task2)
    print('=' * 10, '> task2- {} AUC: {}'.format(args.task2_name, round(res_auc_today_task2, 5)))
    '''
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
        predictions = pd.DataFrame(predictions_8day_sum).values
        np.savetxt(f, predictions, fmt="%.5f")
    print('=' * 10, '> saving done')
    '''
if __name__ == '__main__':
    main()
