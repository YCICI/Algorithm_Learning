'''
version3.0
'''
from __future__ import print_function
import tensorflow as tf
import keras.backend.tensorflow_backend as KTF

config = tf.ConfigProto()
#config.gpu_options.allow_growth=True   #不全部占满显存, 按需分配
config.gpu_options.per_process_gpu_memory_fraction = 0.3
sess = tf.Session(config=config)
# 设置session
KTF.set_session(sess)

import pandas as pd
import numpy as np
from sklearn.metrics import roc_auc_score, mean_squared_error, mean_absolute_error
from keras.losses import binary_crossentropy ##############
from base_model import DeepFM, PredictionLayer, DNN
from utils import auc, LossHistory,LossAndErrorPrintingCallback, TotalLossCallback
from tensorflow.python.keras.optimizers import Adam, SGD, RMSprop, Adagrad
from input_embedding import build_input_features
from tqdm import tqdm
from get_standard_input_train import get_standard_input
import matplotlib.pyplot as plt
import tensorflow as tf
import json
import time
import gc
import os
import warnings
import argparse
import datetime
import config
warnings.filterwarnings('ignore')

def main():
    # 获取日期
    today = str(datetime.date.today()).replace('-', '')#20191008
    train_date = str(datetime.date.today() - datetime.timedelta(days=1)).replace('-', '')#20191006
    predict_date = str(datetime.date.today() - datetime.timedelta(days=1)).replace('-', '')#20191007
    save_model_path = 'D:/z_personal_file/DiDi/ycc/ycc/MTL_deepfm/MTL_deepfm/model/'
    save_log_path = 'D:/z_personal_file/DiDi/ycc/ycc/MTL_deepfm/MTL_deepfm/log/'
    parser = argparse.ArgumentParser(description='DeepFM_MTL')
    # 默认参数
    parser.add_argument('--train_dir', type=str, default='D:/z_personal_file/DiDi/ycc/ycc/MTL_deepfm/MTL_deepfm/data/20200304/s_cvr_ltv_train.csv', help='训练集路径(特征库)') ###
    parser.add_argument('--use_chunks', type=int, default=1, help='')
    parser.add_argument('--chunk_Size', type=int, default=500000, help='')
    parser.add_argument('--data_nums', type=int, default=0, help='如果按块读取,指定前几块读进, 0表示全读')  # 只做为测试用，一般不要使用
    parser.add_argument('--use_for_train', type=int, default=1, help='模型用于训练还是预测')
    # 特征工程参数
    parser.add_argument('--use_percentceil', type=int, default=0, help='是否使用分位数截断')
    parser.add_argument('--use_bin', type=int, default=1, help='是否对指定特征离散化')
    parser.add_argument('--dense_processmethod', type=str, default='log', help='对统计特征的处理方式')
    # deepfm1模型参数
    parser.add_argument('--task_type1', type=str, default='binary', help='任务类型:binary or regression')
    parser.add_argument('--learning_rate1', type=float, default=0.001, help='优化器学习率')
    parser.add_argument('--embedding_size1', type=int, default=8, help='embdding_size')
    parser.add_argument('--l2_reg_linear1', type=float, default=0.0001, help='线性层l2正则项')
    parser.add_argument('--l2_reg_embedding1', type=float, default=0.0001, help='embedding层l2正则项')
    parser.add_argument('--l2_reg_dnn1', type=float, default=0.0001, help='DNN层l2正则项')
    parser.add_argument('--seed1', type=int, default=2019, help='初始参数随机种子')
    parser.add_argument('--dnn_dropout1', type=float, default=0, help='DNN-DropOut层舍弃率')
    parser.add_argument('--dnn_hidden_units1', type=str, default='(128, 128)', help='DNN层units')
    parser.add_argument('--Optimizer1', type=str, default='Adam', help='优化器, Adam, ')
    parser.add_argument('--dnn_use_bn1', type=int, default=0, help='DNN层是否使用BatchNormalize')
    parser.add_argument('--init_std1', type=float, default=0.0001, help='初始参数标准差')
    # deepfm2模型参数
    parser.add_argument('--task_type2', type=str, default='binary', help='任务类型:binary or regression')
    parser.add_argument('--learning_rate2', type=float, default=0.001, help='优化器学习率')
    parser.add_argument('--embedding_size2', type=int, default=8, help='embdding_size')
    parser.add_argument('--l2_reg_linear2', type=float, default=0.0001, help='线性层l2正则项')
    parser.add_argument('--l2_reg_embedding2', type=float, default=0.0001, help='embedding层l2正则项')
    parser.add_argument('--l2_reg_dnn2', type=float, default=0.0001, help='DNN层l2正则项')
    parser.add_argument('--seed2', type=int, default=2019, help='初始参数随机种子')
    parser.add_argument('--dnn_dropout2', type=float, default=0, help='DNN-DropOut层舍弃率')
    parser.add_argument('--dnn_hidden_units2', type=str, default='(128, 128)', help='DNN层units')
    parser.add_argument('--Optimizer2', type=str, default='Adam', help='优化器, Adam, ')
    parser.add_argument('--dnn_use_bn2', type=int, default=0, help='DNN层是否使用BatchNormalize')
    parser.add_argument('--init_std2', type=float, default=0.0001, help='初始参数标准差')
    # 多任务模型参数
    parser.add_argument('--task1_net_units', type=str, default='(128,)', help='task1层units')
    parser.add_argument('--task2_net_units', type=str, default='(128,)', help='task2层units')
    parser.add_argument('--task1_name', type=str, default='pLTV', help='')
    parser.add_argument('--task2_name', type=str, default='pCVR', help='')
    parser.add_argument('--Optimizer_mtl', type=str, default='Adam', help='优化器, Adam, ')
    parser.add_argument('--learning_rate_mtl', type=float, default=0.001, help='优化器学习率')
    # 模型训练参数
    parser.add_argument('--loss_weights', type=str, default='[0.7, 0.3]', help='loss权重, ')
    parser.add_argument('--batch_size', type=int, default=32, help='batch_size')
    parser.add_argument('--epochs', type=int, default=1, help='epochs')
    parser.add_argument('--use_global_epochs', type=int, default=0, help='是否使用global_epoch')  # version2.0 增加global_epoch模块
    parser.add_argument('--verbose', type=int, default=1, help='模型verbose')
    args = parser.parse_args()
    args.dnn_hidden_units1 = eval(args.dnn_hidden_units1)
    args.dnn_hidden_units2 = eval(args.dnn_hidden_units2)
    args.task1_net_units = eval(args.task1_net_units)
    args.task2_net_units = eval(args.task2_net_units)
    args.loss_weights = eval(args.loss_weights)
    args.use_chunks = bool(args.use_chunks)
    args.use_for_train = bool(args.use_for_train)
    args.use_percentceil = bool(args.use_percentceil)
    args.use_bin = bool(args.use_bin)
    args.dnn_use_bn1 = bool(args.dnn_use_bn1)
    args.dnn_use_bn2 = bool(args.dnn_use_bn2)
    args.use_global_epochs = bool(args.use_global_epochs)
    # 获取标准输入输出
    print('=' * 10, '> get_standard_input for task1-{} & {}'.format(args.task1_name, args.task2_name))
    train_model_input, train_target, linear_feature_columns, dnn_feature_columns, train_size, pid_num, label_num = get_standard_input(
        train_dir=args.train_dir,
        use_chunks=args.use_chunks,
        chunk_Size=args.chunk_Size,
        data_nums=args.data_nums,
        use_percentceil=args.use_percentceil,
        use_bin=args.use_bin,
        use_for_train=args.use_for_train,
        dense_processmethod=args.dense_processmethod)
    features = build_input_features(linear_feature_columns + dnn_feature_columns)
    inputs_list = list(features.values())
    
    print('-' * 10, '> concat share features and task1 features')
    task1_features = features.copy()
    for col in tqdm(config.task1_features, desc='concat share features and task1 features'):
        task1_features.pop(col)
    input_list1 = list(task1_features.values())
    linear_feature_columns_1 = linear_feature_columns.copy()
    dnn_feature_columns_1 = dnn_feature_columns.copy()
    for col in linear_feature_columns:
        if col.name in config.task1_features:#移除非共享特征
            linear_feature_columns_1.remove(col)
            dnn_feature_columns_1.remove(col) 
    
    print('-' * 10, '> concat share features and task2 features')
    task2_features = features.copy()
    for col in tqdm(config.task2_features, desc='concat share features and task2 features'):
        task2_features.pop(col)
    input_list2 = list(task2_features.values())
    linear_feature_columns_2 = linear_feature_columns.copy()
    dnn_feature_columns_2 = dnn_feature_columns.copy()
    for col in linear_feature_columns:#移除非共享特征
        if col.name in config.task2_features:
            linear_feature_columns_2.remove(col)
            dnn_feature_columns_2.remove(col)     

    print('-----model1 params-----')
    print('learning_rate:    ', args.learning_rate1)
    print('optimizer:        ', args.Optimizer1)
    print('embedding_size:   ', args.embedding_size1)
    print('l2_reg_linear:    ', args.l2_reg_linear1)
    print('l2_reg_embedding: ', args.l2_reg_embedding1)
    print('l2_reg_dnn:       ', args.l2_reg_dnn1)
    print('init_std:         ', args.init_std1)
    print('dnn_dropout:      ', args.dnn_dropout1)
    print('dnn_hidden_units: ', args.dnn_hidden_units1)
    print('dnn_use_bn:       ', args.dnn_use_bn1)
    print('=' * 10, '> init DeepFM_1')
    if args.Optimizer1 == 'Adam':
        optimizer1 = Adam(lr=args.learning_rate1)
    elif args.Optimizer1 == 'Adagrad':
        optimizer1 = Adagrad(lr=args.learning_rate1)
    elif args.Optimizer1 == 'RMSprop':
        optimizer1 = RMSprop(lr=args.learning_rate1)
    elif args.Optimizer1 == 'SGD':
        optimizer1 = SGD(lr=args.learning_rate1)
    deepfm_model_1 = DeepFM(
        linear_feature_columns_1,
        dnn_feature_columns_1,
        embedding_size=args.embedding_size1,
        dnn_hidden_units=args.dnn_hidden_units1,
        l2_reg_linear=args.l2_reg_linear1,
        l2_reg_embedding=args.l2_reg_embedding1,
        l2_reg_dnn=args.l2_reg_dnn1,
        init_std=args.init_std1,
        seed=args.seed1,
        dnn_dropout=args.dnn_dropout1,
        dnn_use_bn=args.dnn_use_bn1,
        task=args.task_type1)

    if args.task_type1 == 'binary':
        print('=' * 10, '> Compile DeepFM_1 for Binary Task') 
       # deepfm_model_1.compile(optimizer=optimizer1, loss="binary_crossentropy", metrics=['binary_crossentropy',auc])###
        deepfm_model_1.compile(optimizer=optimizer1, loss="binary_crossentropy", metrics=[auc])
    elif args.task_type1 == 'regression':
        print('=' * 10, '> Compile DeepFM_1 for Regression Task')
       # deepfm_model_1.compile(optimizer=optimizer1, loss="mean_squared_error", metrics=['mse','mae'])
        deepfm_model_1.compile(optimizer=optimizer1, loss="mean_squared_error", metrics=['mse'])#####
    feature_task1 = deepfm_model_1(input_list1)

    print('-----model2 params-----')
    print('learning_rate:    ', args.learning_rate2)
    print('optimizer:        ', args.Optimizer2)
    print('embedding_size:   ', args.embedding_size2)
    print('l2_reg_linear:    ', args.l2_reg_linear2)
    print('l2_reg_embedding: ', args.l2_reg_embedding2)
    print('l2_reg_dnn:       ', args.l2_reg_dnn2)
    print('init_std:         ', args.init_std2)
    print('dnn_dropout:      ', args.dnn_dropout2)
    print('dnn_hidden_units: ', args.dnn_hidden_units2)
    print('dnn_use_bn:       ', args.dnn_use_bn2)
    print('=' * 10, '> init DeepFM_2')
    if args.Optimizer2 == 'Adam':
        optimizer2 = Adam(lr=args.learning_rate2)
    elif args.Optimizer2 == 'Adagrad':
        optimizer2 = Adagrad(lr=args.learning_rate2)
    elif args.Optimizer2 == 'RMSprop':
        optimizer2 = RMSprop(lr=args.learning_rate2)
    elif args.Optimizer2 == 'SGD':
        optimizer2 = SGD(lr=args.learning_rate2)
    deepfm_model_2 = DeepFM(
        linear_feature_columns_2,
        dnn_feature_columns_2,
        embedding_size=args.embedding_size2,
        dnn_hidden_units=args.dnn_hidden_units2,
        l2_reg_linear=args.l2_reg_linear2,
        l2_reg_embedding=args.l2_reg_embedding2,
        l2_reg_dnn=args.l2_reg_dnn2,
        init_std=args.init_std2,
        seed=args.seed2,
        dnn_dropout=args.dnn_dropout2,
        dnn_use_bn=args.dnn_use_bn2,
        task=args.task_type2)

    if args.task_type2 == 'binary':
        print('=' * 10, '> Compile DeepFM_2 for Binary Task')
        #deepfm_model_2.compile(optimizer=optimizer2, loss="binary_crossentropy", metrics=[auc])
        deepfm_model_1.compile(optimizer=optimizer1, loss="binary_crossentropy", metrics=['binary_crossentropy',auc])###
    elif args.task_type2 == 'regression':
        print('=' * 10, '> Compile DeepFM_2 for Regression Task')
        deepfm_model_2.compile(optimizer=optimizer2, loss="mean_squared_error", metrics=['mse','mae'])
    feature_task2 = deepfm_model_2(input_list2)
    # 构建多任务模型
    print('-----multi-task model params-----')
    print('learning_rate:    ', args.learning_rate_mtl)
    print('optimizer:        ', args.Optimizer_mtl)
    print('task1_net_units:  ', args.task1_net_units)
    print('task2_net_units:  ', args.task2_net_units)
    print('task1_name:       ', args.task1_name)
    print('task2_name:       ', args.task2_name)
    print('=' * 10, '> construct multi-task model')
    target1_out = DNN(args.task1_net_units, name='task_1_net')(feature_task1)
    target1_logit = tf.keras.layers.Dense(1, use_bias=False, activation=None)(target1_out)
    target2_out = DNN(args.task2_net_units, name='task_2_net')(feature_task2)
    target2_logit = tf.keras.layers.Dense(1, use_bias=False, activation=None)(target2_out)
    output_target1 = PredictionLayer(task=args.task_type1, name=args.task1_name)(target1_logit)
    output_target2 = PredictionLayer(task=args.task_type2, name=args.task2_name)(target2_logit)
    
    print('the shape of output_target1:',tf.shape(output_target1)) ####
    model_mtl = tf.keras.models.Model(inputs=inputs_list, outputs=[output_target1, output_target2])
    print('=' * 10, '> compile multi-task model')
    print('=' * 10, '> loss_weights: ', args.loss_weights)
    if args.Optimizer_mtl == 'Adam':
        optimizer_mtl = Adam(lr=args.learning_rate_mtl)
    elif args.Optimizer_mtl == 'Adagrad':
        optimizer_mtl = Adagrad(lr=args.learning_rate_mtl)
    elif args.Optimizer_mtl == 'RMSprop':
        optimizer_mtl = RMSprop(lr=args.learning_rate_mtl)
    elif args.Optimizer_mtl == 'SGD':
        optimizer_mtl = SGD(lr=args.learning_rate_mtl)
    #combine loss
    if args.task_type1 == 'binary':
         loss_type_1 = 'binary_crossentropy'
    elif args.task_type1 == 'regression':
         loss_type_1 = 'mse'

    if args.task_type2 == 'binary':
         loss_type_2 = 'binary_crossentropy'
    elif args.task_type2 == 'regression':
         loss_type_2 = 'mse'
    
    print('=' * 10, '> loss_types: ', [loss_type_1,loss_type_2]) 
    model_mtl.compile(optimizer=optimizer_mtl, loss=[loss_type_1,loss_type_2], \
#		      loss_weights=args.loss_weights, metrics={args.task1_name:auc,args.task2_name:['mse','mae']})
                      loss_weights=args.loss_weights, metrics={args.task1_name:auc,args.task2_name:auc}
) #########

    
    print('=' * 10, '> train params')
    print('batch_size:       ', args.batch_size)
    print('epochs:           ', args.epochs)

    #TotalLoss = TotalLossCallback(model_mtl, train_model_input, train_target)####
    '''
    callbacks = [#####
    EarlyStoppingByLossVal(monitor='val_pLTV_auc',min_delta=0.0001, patience=10,verbose=1,mode='max',###
    restore_best_weights=True),####
    ModelCheckpoint(weights_path, monitor='val_pLTV_auc', save_best_only=True, verbose=0),###
    ]###
    '''
    
    if args.use_global_epochs:
        for i in range(args.epochs):
            print('=' * 10, '> global_epoch_{}'.format(i + 1))
            model_mtl.fit(
                train_model_input,
                train_target,
                batch_size=args.batch_size,
                epochs=1,
                verbose=args.verbose,
                validation_data=[train_model_input, train_target]
                #callbacks=callbacks
            )
            pred_ans = model_mtl.predict(train_model_input, batch_size=args.batch_size)
          
            task1_mse=mean_squared_error(train_target[0], pred_ans[0])######
            #print('=' * 10, "> train " + args.task1_name + " MSE: ",round(task1_mse, 4))
            print('=' * 10, "> train " + args.task1_name + " AUC: ", round(roc_auc_score(train_target[0], pred_ans[0]),4))
            print('=' * 10, "> train " + args.task2_name + " AUC: ", round(roc_auc_score(train_target[1], pred_ans[1]), 4))
            #print('=' * 10, "> train " + args.task2_name + " binary_loss: ", round(task2_loss), 4))####
        print('=' * 10, '> saving model')
        model_mtl.save(save_model_path + '/deepfm_mtl_' + today + '.h5') 
    else:
        model_mtl.fit(
            train_model_input,
            train_target,
            batch_size=args.batch_size,
            epochs=args.epochs,
            verbose=args.verbose,
            validation_data=[train_model_input, train_target]
            #callbacks=[TotalLoss]
        )
        print('=' * 10, '> saving model')
        model_mtl.save(save_model_path + '/deepfm_mtl_' + today + '.h5',True,False) ###
        ##the_model.save(file_path,True/False,False)
    pred_ans = model_mtl.predict(train_model_input, batch_size=args.batch_size)
    #print('train_target[0]:',train_target[0])###
    print('the shape of train_target[0]:',train_target[0].shape)
   # print('pred_ans[0]:',pred_ans[0])###
    print('the shape of pre_ans[0]:',pred_ans[0].shape)
    #print('=' * 10, "> train " + args.task1_name + " MSE: ",round(mean_squared_error(train_target[0], pred_ans[0]),4))
    print('=' * 10, "> train " + args.task1_name + " AUC: ", round(roc_auc_score(train_target[0], pred_ans[0]), 4))
    print('=' * 10, "> train " + args.task2_name + " AUC: ", round(roc_auc_score(train_target[1], pred_ans[1]), 4))
#    print('=' * 10, "> train " + args.task2_name + " MSE: ", round(mean_squared_error(train_target[1], pred_ans[1]), 4), ', MAE:', round(mean_absolute_error(train_target[1], pred_ans[1]), 4))
    '''
    train_log = dict()
    train_log['训练时间'] = str(datetime.datetime.now())[:-7]
    train_log['训练集AUC'] = round(roc_auc_score(train_target, pred_ans), 4)
    train_log['所存模型'] = 'deepfm_mtl_' + today + '.h5'
    train_log['数据size'] = train_size
    train_log['pid数量'] = pid_num
    train_log['正负样本量'] = label_num
    with open(save_log_path + '/train_log.txt', 'a+') as f:
        f.write(str(list(train_log.items())) + '\n')
    '''

if __name__ == '__main__':
    main()
