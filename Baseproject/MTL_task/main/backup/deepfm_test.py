'''
version3.0
'''
from __future__ import print_function
import pandas as pd
import numpy as np
from sklearn.metrics import roc_auc_score
from base_model import DeepFM
from utils import auc, LossHistory
from tensorflow.python.keras.optimizers import Adam, SGD, RMSprop, Adagrad
from tqdm import tqdm
from get_standard_input import get_standard_input
import matplotlib.pyplot as plt
import config
import json
import time
import gc
import os
import warnings
import argparse
import datetime
warnings.filterwarnings('ignore')

def main():
    # 获取日期
    today = str(datetime.date.today()).replace('-', '')
    train_date = str(datetime.date.today() - datetime.timedelta(days=2)).replace('-', '')
    predict_date = str(datetime.date.today() - datetime.timedelta(days=1)).replace('-', '')
    save_model_path = '/nfs/project/sundike/DeepFM_sdk/model'
    svae_log_path = '/nfs/project/sundike/DeepFM_sdk/log'
    parser = argparse.ArgumentParser(description='DeepFM')
    # 默认参数
    parser.add_argument('--train_dir', type=str, default='/nfs/project/sundike/DeepFM_sdk/data/' + train_date + '/raw_train_data.csv', help='训练集路径') 
    parser.add_argument('--test_dir', type=str, default='/nfs/project/sundike/DeepFM_sdk/data/' + predict_date + '/raw_train_data.csv', help='测试集路径')
    parser.add_argument('--use_chunks', type=int, default=1, help='')
    parser.add_argument('--chunk_Size', type=int, default=500000, help='')
    parser.add_argument('--data_nums', type=int, default=0, help='如果按块读取,指定前几块读进, 0表示全读')  # 只做为测试用，一般不要使用
    parser.add_argument('--use_for_train', type=int, default=1, help='模型用于训练还是预测')
    # 特征工程参数
    parser.add_argument('--use_percentceil', type=int, default=0, help='是否使用分位数截断')
    parser.add_argument('--use_bin', type=int, default=1, help='是否对指定特征离散化')
    parser.add_argument('--dense_processmethod', type=str, default='log', help='对统计特征的处理方式')
    # 模型参数
    parser.add_argument('--learning_rate', type=float, default=0.001, help='优化器学习率')
    parser.add_argument('--embedding_size', type=int, default=8, help='embdding_size')
    parser.add_argument('--l2_reg_linear', type=float, default=0.01, help='线性层l2正则项')
    parser.add_argument('--l2_reg_embedding', type=float, default=0.01, help='embedding层l2正则项')
    parser.add_argument('--l2_reg_dnn', type=float, default=0.0001, help='DNN层l2正则项')
    parser.add_argument('--seed', type=int, default=2019, help='初始参数随机种子')
    parser.add_argument('--dnn_dropout', type=float, default=0, help='DNN-DropOut层舍弃率')
    parser.add_argument('--dnn_hidden_units', type=str, default='(256, 256, 256)', help='DNN层units')
    parser.add_argument('--dnn_use_bn', type=int, default=0, help='DNN层是否使用BatchNormalize')
    parser.add_argument('--Optimizer', type=str, default='Adam', help='优化器, Adam, ')
    parser.add_argument('--batch_size', type=int, default=4096, help='batch_size')
    parser.add_argument('--epochs', type=int, default=100, help='epochs')
    parser.add_argument('--use_global_epochs', type=int, default=1,
                        help='是否使用global_epoch')  # version2.0 增加global_epoch模块
    parser.add_argument('--init_std', type=float, default=0.0001, help='初始参数标准差')
    parser.add_argument('--verbose', type=int, default=1, help='模型verbose')
    args = parser.parse_args()
    args.dnn_hidden_units = eval(args.dnn_hidden_units)
    args.use_chunks = bool(args.use_chunks)
    args.use_for_train = bool(args.use_for_train)
    args.use_percentceil = bool(args.use_percentceil)
    args.use_bin = bool(args.use_bin)
    args.dnn_use_bn = bool(args.dnn_use_bn)
    args.use_global_epochs = bool(args.use_global_epochs)
    # 获取标准输入输出
    print('=' * 10, '> get_standard_input')
    train_model_input, train_target, linear_feature_columns, dnn_feature_columns, train_size, pid_num, label_num = get_standard_input(
        train_dir='/nfs/project/sundike/DeepFM_sdk/data/20190710/raw_train_data.csv',
        test_dir=args.test_dir,
        use_chunks=args.use_chunks,
        chunk_Size=args.chunk_Size,
        data_nums=args.data_nums,
        use_percentceil=args.use_percentceil,
        use_bin=args.use_bin,
        use_for_train=args.use_for_train,
        dense_processmethod=args.dense_processmethod)
    ################################
    test_model_input, test_target, test_size, pid_num, label_num, day1_input, day1_target, day1_size, day1_pid, day1_label = get_standard_input(
        train_dir=args.train_dir,
        test_dir='/nfs/project/sundike/DeepFM_sdk/data/20190711/raw_predict_data.csv',
        use_chunks=args.use_chunks,
        chunk_Size=args.chunk_Size,
        data_nums=args.data_nums,
        use_percentceil=args.use_percentceil,
        use_bin=args.use_bin,
        use_for_train=False,
        dense_processmethod=args.dense_processmethod)
    ################################
    print('-----model params-----')
    print('learning_rate:    ', args.learning_rate)
    print('embedding_size:   ', args.embedding_size)
    print('l2_reg_linear:    ', args.l2_reg_linear)
    print('l2_reg_embedding: ', args.l2_reg_embedding)
    print('l2_reg_dnn:       ', args.l2_reg_dnn)
    print('init_std:         ', args.init_std)
    print('dnn_dropout:      ', args.dnn_dropout)
    print('dnn_hidden_units: ', args.dnn_hidden_units)
    print('dnn_use_bn:       ', args.dnn_use_bn)
    print('batch_size:       ', args.batch_size)
    print('epochs:           ', args.epochs)
    print('optimizer:        ', args.Optimizer)
    print('=' * 10, '> init DeepFM')
    model = DeepFM(
        linear_feature_columns,
        dnn_feature_columns,
        embedding_size=args.embedding_size,
        dnn_hidden_units=args.dnn_hidden_units,
        l2_reg_linear=args.l2_reg_linear,
        l2_reg_embedding=args.l2_reg_embedding,
        l2_reg_dnn=args.l2_reg_dnn,
        init_std=args.init_std,
        seed=args.seed,
        dnn_dropout=args.dnn_dropout,
        dnn_use_bn=args.dnn_use_bn,
        task='binary')
    if args.Optimizer == 'Adam':
        optimizer = Adam(lr=args.learning_rate)
    elif args.Optimizer == 'Adagrad':
        optimizer = Adagrad(lr=args.learning_rate)
    elif args.Optimizer == 'RMSprop':
        optimizer = RMSprop(lr=args.learning_rate)
    elif args.Optimizer == 'SGD':
        optimizer = SGD(lr=args.learning_rate)
    print('=' * 10, '> compile DeepFM')
    model.compile(optimizer=optimizer, loss="binary_crossentropy", metrics=['binary_crossentropy', auc])
    if args.use_global_epochs:
        auc_dict = dict()
        for i in range(args.epochs):
            print('=' * 10, '> global_epoch_{}'.format(i + 1))
            model.fit(
                train_model_input,
                train_target,
                batch_size=args.batch_size,
                epochs=1,
                verbose=args.verbose
            )
            predictions_8day = model.predict(test_model_input, batch_size=2 ** 12)
            res_auc_8day = roc_auc_score(test_target, predictions_8day)
            predictions_today = model.predict(day1_input, batch_size=2 ** 12)
            res_auc_today = roc_auc_score(day1_target, predictions_today)
            if res_auc_today not in auc_dict:
                auc_dict[res_auc_today] = i + 1
            best_auc = sorted(auc_dict, reverse=True)[0]
            print('=' * 10, '> AUC_8: {}'.format(round(res_auc_8day, 5)))
            print('=' * 10, '> AUC_1: {}'.format(round(res_auc_today, 5)))
        print('=' * 10, '> BEST_AUC: {}'.format(round(best_auc, 5)))
        print('=' * 10, '> saving model')
        model.save(save_model_path + '/deepfm_' + today + '.h5') 
    else:
        model.fit(
            train_model_input,
            train_target,
            batch_size=args.batch_size,
            epochs=args.epochs,
            verbose=args.verbose
        )
        print('=' * 10, '> saving model')
        model.save(save_model_path + '/deepfm_' + today + '.h5') 
    train_log = dict()
    train_log['训练时间'] = str(datetime.datetime.now())[:-7]
    train_log['所存模型'] = 'deepfm_' + today + '.h5'
    train_log['数据size'] = train_size
    train_log['pid数量'] = pid_num
    train_log['正负样本量'] = label_num
    with open(save_log_path + '/train_log.txt', 'a+') as f:
        f.write(str(list(train_log.items())) + '\n')


if __name__ == '__main__':
    main()
