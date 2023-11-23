from collections import namedtuple
import tensorflow as tf
import pandas as pd
import numpy as np
import keras
import datetime
import matplotlib.pyplot as plt
from keras import backend as K
from tqdm import tqdm, trange
from sklearn.metrics import accuracy_score, roc_auc_score, mean_squared_error, log_loss, mean_absolute_error
from keras.losses import binary_crossentropy
from math import log

class SparseFeat(namedtuple('SparseFeat', ['name', 'dimension', 'use_hash', 'dtype','embedding_name', 'embedding'])):
    __slots__ = ()

    def __new__(cls, name, dimension, use_hash=False, dtype="int32", embedding_name=None, embedding=True):
        if embedding and embedding_name is None:
            embedding_name = name
        return super(SparseFeat, cls).__new__(cls, name, dimension, use_hash, dtype, embedding_name, embedding)


class DenseFeat(namedtuple('DenseFeat', ['name', 'dimension', 'dtype'])):
    __slots__ = ()

    def __new__(cls, name, dimension=1, dtype="float32"):

        return super(DenseFeat, cls).__new__(cls, name, dimension, dtype)


class VarLenSparseFeat(namedtuple('VarLenFeat', ['name', 'dimension', 'maxlen', 'combiner', 'use_hash', 'dtype','embedding_name', 'embedding'])):
    __slots__ = ()

    def __new__(cls, name, dimension, maxlen, combiner="mean", use_hash=False, dtype="float32", embedding_name=None, embedding=True):
        if embedding_name is None:
            embedding_name = name
        return super(VarLenSparseFeat, cls).__new__(cls, name, dimension, maxlen, combiner, use_hash, dtype, embedding_name, embedding)


class Hash(tf.keras.layers.Layer):
    """
    hash the input to [0,num_buckets)
    if mask_zero = True,0 or 0.0 will be set to 0,other value will be set in range[1,num_buckets)
    """

    def __init__(self, num_buckets, mask_zero=False, **kwargs):
        self.num_buckets = num_buckets
        self.mask_zero = mask_zero
        super(Hash, self).__init__(**kwargs)

    def build(self, input_shape):
        # Be sure to call this somewhere!
        super(Hash, self).build(input_shape)

    def call(self, x, mask=None, **kwargs):
        if x.dtype != tf.string:
            x = tf.as_string(x, )
        hash_x = tf.string_to_hash_bucket_fast(x, self.num_buckets if not self.mask_zero else self.num_buckets - 1,
                                               name=None)  # weak hash
        if self.mask_zero:
            mask_1 = tf.cast(tf.not_equal(x, "0"), 'int64')
            mask_2 = tf.cast(tf.not_equal(x, "0.0"), 'int64')
            mask = mask_1 * mask_2
            hash_x = (hash_x + 1) * mask
        return hash_x

    def compute_mask(self, inputs, mask):
        return None

    def get_config(self, ):
        config = {'num_buckets': self.num_buckets, 'mask_zero': self.mask_zero}
        base_config = super(Hash, self).get_config()
        return dict(list(base_config.items()) + list(config.items()))


def concat_fun(inputs, axis=-1):
    if len(inputs) == 1:
        return inputs[0]
    else:
        return tf.keras.layers.Concatenate(axis=axis)(inputs)
    


def reduce_mem_usage(df, prefix='', verbose=True):
    """ iterate through all the columns of a dataframe and modify the data type
        to reduce memory usage.        
    """
    start_mem = df.memory_usage().sum() / 1024**2
    if verbose:
        print('[Memory usage of dataframe is {:.2f} MB]'.format(start_mem))
    for col in tqdm(df.columns, desc='reduce_mem_usage_' + prefix):
        if prefix == '':
            trange(1, desc='processing: ' + col, position=1, bar_format='{desc}')
        col_type = df[col].dtype
        if col_type != object:
            c_min = df[col].min()
            c_max = df[col].max()
            if str(col_type)[:3] == 'int':
                if c_min > np.iinfo(np.int8).min and c_max < np.iinfo(np.int8).max:
                    df[col] = df[col].astype(np.int8)
                elif c_min > np.iinfo(np.int16).min and c_max < np.iinfo(np.int16).max:
                    df[col] = df[col].astype(np.int16)
                elif c_min > np.iinfo(np.int32).min and c_max < np.iinfo(np.int32).max:
                    df[col] = df[col].astype(np.int32)
                elif c_min > np.iinfo(np.int64).min and c_max < np.iinfo(np.int64).max:
                    df[col] = df[col].astype(np.int64)  
            else:
                if c_min > np.finfo(np.float16).min and c_max < np.finfo(np.float16).max:
                    df[col] = df[col].astype(np.float16)
                elif c_min > np.finfo(np.float32).min and c_max < np.finfo(np.float32).max:
                    df[col] = df[col].astype(np.float32)
                else:
                    df[col] = df[col].astype(np.float64)
        else:
            df[col] = df[col].astype('category')

    end_mem = df.memory_usage().sum() / 1024**2
    if verbose:
        print('[Memory usage after optimization is: {:.2f} MB]'.format(end_mem))
        print('[Decreased by {:.1f}%]'.format(100 * (start_mem - end_mem) / start_mem))

    return df

def auc(y_true, y_pred):
    print('y_pred[0]',y_pred[0])
    print('y_pred[1]',y_pred[1])
    return tf.py_func(auc_local, (y_true, y_pred), tf.double)

def auc_local(y_true, y_pred):
    if len(np.unique(y_true)) == 1: # bug in roc_auc_score
        return accuracy_score(y_true, np.rint(y_pred))
    return roc_auc_score(y_true, y_pred)

class LossHistory(keras.callbacks.Callback):
    def on_train_begin(self, logs={}):
        self.loss = {'batch':[], 'epoch':[]}
        self.binary_crossentropy = {'batch':[], 'epoch':[]}
        self.auc = {'batch':[], 'epoch':[]}
        self.val_loss = {'batch':[], 'epoch':[]}
        self.val_binary_crossentropy = {'batch':[], 'epoch':[]}
        self.val_auc = {'batch':[], 'epoch':[]}
    def on_batch_end(self, batch, logs={}):
        self.loss['batch'].append(logs.get('loss'))
        self.binary_crossentropy['batch'].append(logs.get('binary_crossentropy'))
        self.auc['batch'].append(logs.get('auc'))
        self.val_loss['batch'].append(logs.get('loss'))
        self.val_binary_crossentropy['batch'].append(logs.get('binary_crossentropy'))
        self.val_auc['batch'].append(logs.get('auc'))
    def on_epoch_end(self, batch, logs={}):
        self.loss['epoch'].append(logs.get('loss'))
        self.binary_crossentropy['epoch'].append(logs.get('binary_crossentropy'))
        self.auc['epoch'].append(logs.get('auc'))
        self.val_loss['epoch'].append(logs.get('loss'))
        self.val_binary_crossentropy['epoch'].append(logs.get('binary_crossentropy'))
        self.val_auc['epoch'].append(logs.get('auc'))

    def loss_plot(self, loss_type):
        iters = range(len(self.loss[loss_type]))
        plt.figure()
        # binary_crossentropy
        plt.plot(iters, self.binary_crossentropy[loss_type], label='train binary_crossentropy')
        # loss
        plt.plot(iters, self.loss[loss_type], label='train loss')
        # auc
        plt.plot(iters, self.auc[loss_type], label='train auc')
        if loss_type == 'epoch':
            # val_binary_crossentropy
            plt.plot(iters, self.binary_crossentropy[loss_type], label='val binary_crossentropy')
            # val_loss
            plt.plot(iters, self.val_loss[loss_type], label='val loss')
            # val_auc
            plt.plot(iters, self.val_auc[loss_type], label='val auc')
        plt.grid(True)
        plt.xlabel(loss_type)
        plt.ylabel('binary_crossentropy-loss-auc')
        plt.legend(loc="upper right")
        plt.show()

def getYesterday(): 
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)   
    return [int(str(yesterday).replace('-', ''))]

def gett_2_t_8day():
    today = datetime.date.today()
    res_list = []
    for day in range(2, 9):
        oneday = datetime.timedelta(days=day)
        res_list.append(int(str(today-oneday).replace('-', '')))
    return sorted(res_list)

def gett_2_t_16day(): 
    today = datetime.date.today() 
    res_list = []
    for day in range(1, 15):
        oneday = datetime.timedelta(days=day)
        res_list.append(int(str(today-oneday).replace('-', ''))) 
    return sorted(res_list)


class LossAndErrorPrintingCallback(tf.keras.callbacks.Callback):

    def on_epoch_end(self, epoch, logs=None):
        print('\nThe average loss for epoch {}\n'.format(epoch))
        for k,v in logs.items():
            print (k,v)
        print('\n\n')

class TotalLossCallback(tf.keras.callbacks.Callback):
    def __init__(self, model, train, label):
        self.model = model
        self.train = train
        self.label = label

    def on_epoch_end(self, epoch, logs=None):
        predict = self.model.predict(self.train,batch_size=2**14)
        ltv_loss = mean_squared_error(self.label[0], predict[0])
        cvr_loss = log_loss(self.label[1], predict[1])
        ltv_mae = mean_absolute_error(self.label[0], predict[0])
        cvr_auc = roc_auc_score(self.label[1], predict[1])
        print("\n\nTotalLossCallback for epoch %s ltv_loss: %5.4f, cvr_loss: %5.4f, ltv_mae: %5.4f, cvr_auc: %5.4f\n\n" % (epoch,ltv_loss,cvr_loss,ltv_mae,cvr_auc))










