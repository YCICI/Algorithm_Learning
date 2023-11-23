# -*- coding:utf-8 -*-

import tensorflow as tf
from tensorflow.python.keras import backend as K
from tensorflow.python.keras.layers import Layer
from tensorflow.python.keras.initializers import Zeros, glorot_normal
from tensorflow.python.keras.regularizers import l2
from input_embedding import input_from_feature_columns, get_linear_logit, build_input_features, combined_dnn_input
from tensorflow.python.keras.optimizers import Adam, SGD, RMSprop, Adagrad
from utils import concat_fun

class FM(Layer):
    """Factorization Machine models pairwise (order-2) feature interactions
     without linear term and bias.

      Input shape
        - 3D tensor with shape: ``(batch_size,field_size,embedding_size)``.

      Output shape
        - 2D tensor with shape: ``(batch_size, 1)``.

      References
        - [Factorization Machines](https://www.csie.ntu.edu.tw/~b97053/paper/Rendle2010FM.pdf)
    """

    def __init__(self, **kwargs):

        super(FM, self).__init__(**kwargs)

    def build(self, input_shape):
        if len(input_shape) != 3:
            raise ValueError("Unexpected inputs dimensions % d, expect to be 3 dimensions" % (len(input_shape)))

        super(FM, self).build(input_shape)  # Be sure to call this somewhere!

    def call(self, inputs, **kwargs):
        if K.ndim(inputs) != 3:
            raise ValueError("Unexpected inputs dimensions %d, expect to be 3 dimensions" % (K.ndim(inputs)))
        concated_embeds_value = inputs
        square_of_sum = tf.square(tf.reduce_sum(concated_embeds_value, axis=1, keep_dims=True))
        sum_of_square = tf.reduce_sum(concated_embeds_value * concated_embeds_value, axis=1, keep_dims=True)
        cross_term = square_of_sum - sum_of_square
        cross_term = 0.5 * tf.reduce_sum(cross_term, axis=2, keep_dims=False)
        return cross_term

    def compute_output_shape(self, input_shape):
        return None, 1

def activation_layer(activation):
    return tf.keras.layers.Activation(activation)

class DNN(Layer):
    """The Multi Layer Percetron

      Input shape
        - nD tensor with shape: ``(batch_size, ..., input_dim)``. The most common situation would be a 2D input with shape ``(batch_size, input_dim)``.

      Output shape
        - nD tensor with shape: ``(batch_size, ..., hidden_size[-1])``. For instance, for a 2D input with shape ``(batch_size, input_dim)``, the output would have shape ``(batch_size, hidden_size[-1])``.

      Arguments
        - **hidden_units**:list of positive integer, the layer number and units in each layer.

        - **activation**: Activation function to use.

        - **l2_reg**: float between 0 and 1. L2 regularizer strength applied to the kernel weights matrix.

        - **dropout_rate**: float in [0,1). Fraction of the units to dropout.

        - **use_bn**: bool. Whether use BatchNormalization before activation or not.

        - **seed**: A Python integer to use as random seed.
    """

    def __init__(self, hidden_units, activation='relu', l2_reg=0, dropout_rate=0, use_bn=False, seed=1024, **kwargs):
        self.hidden_units = hidden_units
        self.activation = activation
        self.dropout_rate = dropout_rate
        self.seed = seed
        self.l2_reg = l2_reg
        self.use_bn = use_bn
        super(DNN, self).__init__(**kwargs)

    def build(self, input_shape):
        input_size = input_shape[-1]
        hidden_units = [int(input_size)] + list(self.hidden_units)
        self.kernels = [self.add_weight(name='kernel' + str(i),
                                        shape=(hidden_units[i], hidden_units[i + 1]),
                                        initializer=glorot_normal(seed=self.seed),
                                        regularizer=l2(self.l2_reg),
                                        trainable=True) for i in range(len(self.hidden_units))]
        self.bias = [self.add_weight(name='bias' + str(i),
                                     shape=(self.hidden_units[i],),
                                     initializer=Zeros(),
                                     trainable=True) for i in range(len(self.hidden_units))]
        if self.use_bn:
            self.bn_layers = [tf.keras.layers.BatchNormalization() for _ in range(len(self.hidden_units))]

        self.dropout_layers = [tf.keras.layers.Dropout(self.dropout_rate,seed=self.seed+i) for i in range(len(self.hidden_units))]

        self.activation_layers = [activation_layer(self.activation) for _ in range(len(self.hidden_units))]

        super(DNN, self).build(input_shape)  # Be sure to call this somewhere!

    def call(self, inputs, training=None, **kwargs):

        deep_input = inputs

        for i in range(len(self.hidden_units)):
            fc = tf.nn.bias_add(tf.tensordot(deep_input, self.kernels[i], axes=(-1, 0)), self.bias[i])
            if self.use_bn:
                fc = self.bn_layers[i](fc, training=training)

            fc = self.activation_layers[i](fc)

            fc = self.dropout_layers[i](fc,training = training)
            deep_input = fc

        return deep_input

    def compute_output_shape(self, input_shape):
        if len(self.hidden_units) > 0:
            shape = input_shape[:-1] + (self.hidden_units[-1],)
        else:
            shape = input_shape

        return tuple(shape)

    def get_config(self, ):
        config = {'activation': self.activation, 'hidden_units': self.hidden_units, 'l2_reg': self.l2_reg, 'use_bn': self.use_bn, 'dropout_rate': self.dropout_rate, 'seed': self.seed}
        base_config = super(DNN, self).get_config()
        # noinspection PyTypeChecker
        return dict(list(base_config.items()) + list(config.items()))


class PredictionLayer(Layer):
    """
      Arguments
         - **task**: str, ``"binary"`` for  binary logloss or  ``"regression"`` for regression loss

         - **use_bias**: bool.Whether add bias term or not.
    """

    def __init__(self, task='binary', use_bias=True, **kwargs):
        if task not in ["binary", "regression"]:
            raise ValueError("task must be binary or regression")
        self.task = task
        self.use_bias = use_bias
        super(PredictionLayer, self).__init__(**kwargs)

    def build(self, input_shape):

        if self.use_bias:
            self.global_bias = self.add_weight(shape=(1,), initializer=Zeros(), name="global_bias")

        # Be sure to call this somewhere!
        super(PredictionLayer, self).build(input_shape)

    def call(self, inputs, **kwargs):
        x = inputs
        if self.use_bias:
            x = tf.nn.bias_add(x, self.global_bias, data_format='NHWC')
        if self.task == "binary":
            x = tf.sigmoid(x)

        output = tf.reshape(x, (-1, 1))

        return output

    def compute_output_shape(self, input_shape):
        return None, 1

    def get_config(self, ):
        config = {'task': self.task, 'use_bias': self.use_bias}
        base_config = super(PredictionLayer, self).get_config()
        # noinspection PyTypeChecker
        return dict(list(base_config.items()) + list(config.items()))

def DeepFM(linear_feature_columns, dnn_feature_columns, embedding_size=8, use_fm=True, dnn_hidden_units=(128, 128),
           l2_reg_linear=0.00001, l2_reg_embedding=0.00001, l2_reg_dnn=0, init_std=0.0001, seed=1024, dnn_dropout=0,
           dnn_activation='relu', dnn_use_bn=False, task='binary'):
    """DeepFM Network architecture.

    :param linear_feature_columns: An iterable containing all the features used by linear part of the model.
    :param dnn_feature_columns: An iterable containing all the features used by deep part of the model.
    :param embedding_size: positive integer,sparse feature embedding_size
    :param use_fm: bool,use FM part or not
    :param dnn_hidden_units: list,list of positive integer or empty list, the layer number and units in each layer of DNN
    :param l2_reg_linear: float. L2 regularizer strength applied to linear part
    :param l2_reg_embedding: float. L2 regularizer strength applied to embedding vector
    :param l2_reg_dnn: float. L2 regularizer strength applied to DNN
    :param init_std: float,to use as the initialize std of embedding vector
    :param seed: integer ,to use as random seed.
    :param dnn_dropout: float in [0,1), the probability we will drop out a given DNN coordinate.
    :param dnn_activation: Activation function to use in DNN
    :param dnn_use_bn: bool. Whether use BatchNormalization before activation or not in DNN
    :param task: str, ``"binary"`` for  binary logloss or  ``"regression"`` for regression loss
    :return: A Keras model instance.
    """

    features = build_input_features(linear_feature_columns + dnn_feature_columns)

    inputs_list = list(features.values())

    sparse_embedding_list, dense_value_list = input_from_feature_columns(features, dnn_feature_columns, embedding_size, l2_reg_embedding,init_std, seed)

    linear_logit = get_linear_logit(features, linear_feature_columns, l2_reg=l2_reg_linear, init_std=init_std, seed=seed, prefix='linear')

    fm_input = concat_fun(sparse_embedding_list, axis=1)
    fm_logit = FM()(fm_input)

    dnn_input = combined_dnn_input(sparse_embedding_list, dense_value_list)
    dnn_out = DNN(dnn_hidden_units, dnn_activation, l2_reg_dnn, dnn_dropout, dnn_use_bn, seed)(dnn_input)
    dnn_logit = tf.keras.layers.Dense(1, use_bias=False, activation=None)(dnn_out)


    if len(dnn_hidden_units) == 0 and use_fm == False:  # only linear
        final_logit = linear_logit
    elif len(dnn_hidden_units) == 0 and use_fm == True:  # linear + FM
        final_logit = tf.keras.layers.add([linear_logit, fm_logit])
    elif len(dnn_hidden_units) > 0 and use_fm == False:  # linear + Deep
        final_logit = tf.keras.layers.add([linear_logit, dnn_logit])
    elif len(dnn_hidden_units) > 0 and use_fm == True:  # linear + FM + Deep
        final_logit = tf.keras.layers.add([linear_logit, fm_logit, dnn_logit])
    else:
        raise NotImplementedError

    output = PredictionLayer(task)(final_logit)
    model = tf.keras.models.Model(inputs=inputs_list, outputs=output)
    return model

'''dont use
def DeepFMTL(linear_feature_columns1, dnn_feature_columns1, embedding_size1=8, dnn_hidden_units1=(128, 128),
           l2_reg_linear1=0.00001, l2_reg_embedding1=0.00001, l2_reg_dnn1=0, init_std1=0.0001, seed1=2019, dnn_dropout1=0,
           dnn_activation1='relu', dnn_use_bn1=False, task1='binary', learning_rate1=0.001, Optimizer1='Adam',
           linear_feature_columns2, dnn_feature_columns2, embedding_size2=8, dnn_hidden_units2=(128, 128),
           l2_reg_linear2=0.00001, l2_reg_embedding2=0.00001, l2_reg_dnn2=0, init_std2=0.0001, seed2=2020, dnn_dropout2=0,
           dnn_activation2='relu', dnn_use_bn2=False, task2='binary', learning_rate2=0.001, Optimizer2='Adam'):
    """DeepFM_MTL network architecure.
    """
    print('-----model1 params-----')
    print('learning_rate:    ', learning_rate1)
    print('embedding_size:   ', embedding_size1)
    print('l2_reg_linear:    ', l2_reg_linear1)
    print('l2_reg_embedding: ', l2_reg_embedding1)
    print('l2_reg_dnn:       ', l2_reg_dnn1)
    print('init_std:         ', init_std1)
    print('dnn_dropout:      ', dnn_dropout1)
    print('dnn_hidden_units: ', dnn_hidden_units1)
    print('dnn_use_bn:       ', dnn_use_bn1)
    print('optimizer:        ', Optimizer1)
    print('-----model2 params-----')
    print('learning_rate:    ', learning_rate2)
    print('embedding_size:   ', embedding_size2)
    print('l2_reg_linear:    ', l2_reg_linear2)
    print('l2_reg_embedding: ', l2_reg_embedding2)
    print('l2_reg_dnn:       ', l2_reg_dnn2)
    print('init_std:         ', init_std2)
    print('dnn_dropout:      ', dnn_dropout2)
    print('dnn_hidden_units: ', dnn_hidden_units2)
    print('dnn_use_bn:       ', dnn_use_bn2)
    print('optimizer:        ', Optimizer2)
    # Optimizer1
    if Optimizer1 == 'Adam':
        optimizer1 = Adam(lr=learning_rate1)
    elif Optimizer1 == 'Adagrad':
        optimizer1 = Adagrad(lr=learning_rate1)
    elif Optimizer1 == 'RMSprop':
        optimizer1 = RMSprop(lr=learning_rate1)
    elif Optimizer1 == 'SGD':
        optimizer1 = SGD(lr=learning_rate1)
    print('=' * 10, '> init DeepFM_1')
    deepfm_model_1 = DeepFM(
        linear_feature_columns1,
        dnn_feature_columns1,
        embedding_size=embedding_size1,
        dnn_hidden_units=dnn_hidden_units1,
        l2_reg_linear=l2_reg_linear1,
        l2_reg_embedding=l2_reg_embedding1,
        l2_reg_dnn=l2_reg_dnn1,
        init_std=init_std1,
        seed=seed1,
        dnn_dropout=dnn_dropout1,
        dnn_use_bn=dnn_use_bn1,
        task=task1)
    print('=' * 10, '> compile DeepFM_1')
    deepfm_model_1.compile(optimizer=optimizer1, loss="binary_crossentropy", metrics=[auc])
    feature_task1 = deepfm_model_1(inputs_list_1)
    # Optimizer2
    if Optimizer2 == 'Adam':
        optimizer2 = Adam(lr=learning_rate2)
    elif Optimizer2 == 'Adagrad':
        optimizer2 = Adagrad(lr=learning_rate2)
    elif Optimizer2 == 'RMSprop':
        optimizer2 = RMSprop(lr=learning_rate2)
    elif Optimizer2 == 'SGD':
        optimizer2 = SGD(lr=learning_rate2)
    print('=' * 10, '> init DeepFM_2')
    deepfm_model_2 = DeepFM(
        linear_feature_columns2,
        dnn_feature_columns2,
        embedding_size=embedding_size2,
        dnn_hidden_units=dnn_hidden_units2,
        l2_reg_linear=l2_reg_linear2,
        l2_reg_embedding=l2_reg_embedding2,
        l2_reg_dnn=l2_reg_dnn2,
        init_std=init_std2,
        seed=seed2,
        dnn_dropout=dnn_dropout2,
        dnn_use_bn=dnn_use_bn2,
        task=task2)
    print('=' * 10, '> compile DeepFM_2')
    deepfm_model_2.compile(optimizer=optimizer2, loss="binary_crossentropy", metrics=[auc])
    feature_task2 = deepfm_model_2(inputs_list_2)
    # 构建多任务模型
    print('=' * 10, '> construct multi-task model')
    target1_out = DNN(args.task1_net_units, name='task_1_net')(feature_task1)
    target1_logit = tf.keras.layers.Dense(1, use_bias=False, activation=None)(target1_out)
    target2_out = DNN(args.task2_net_units, name='task_2_net')(feature_task2)
    target2_logit = tf.keras.layers.Dense(1, use_bias=False, activation=None)(target2_out)
    output_target1 = PredictionLayer(task='binary', name='pCTR')(target1_logit)
    output_target2 = PredictionLayer(task='binary', name='pLTV')(target2_logit)
    inputs_list = inputs_list_1 + inputs_list_2
    model_mtl = tf.keras.models.Model(inputs=inputs_list, outputs=[output_target1, output_target2])
    return model_mtl
'''
