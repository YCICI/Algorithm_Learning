'''
Author: yuchuchu yuchuchu77@163.com
Date: 2025-03-14 11:15:26
LastEditors: yuchuchu yuchuchu77@163.com
LastEditTime: 2025-03-14 15:01:15
FilePath: /Algorithm_Learning/Baseproject/rerank/transformblock.py
Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
'''

import math
import tensorflow.compat.v1 as tf


class MultiHeadAttention(tf.keras.layers.Layer):
    def __init__(self, d_in, d_out, context_length, dropout, num_heads, qkv_bias=False, **kwargs):
        super().__init__(**kwargs)
        assert (d_out % num_heads == 0), "d_out must be divisible by num_heads"

        self.d_in = d_in  # d_in # the input embedding size
        self.d_out = d_out # the output embedding size
        self.num_heads = num_heads
        self.head_dim = d_out // num_heads

        # 定义查询、键、值的线性变换
        self.W_query = tf.keras.layers.Dense(d_out, use_bias=qkv_bias)
        self.W_key = tf.keras.layers.Dense(d_out, use_bias=qkv_bias)
        self.W_value = tf.keras.layers.Dense(d_out, use_bias=qkv_bias)
        # 定义输出的线性变换
        self.out_proj = tf.keras.layers.Dense(d_out)
        # 定义dropout层
        self.dropout = tf.keras.layers.Dropout(dropout)
        ## 创建掩码，用于屏蔽未来的信息
        # self.mask = tf.linalg.band_part(tf.ones((context_length, context_length)), -1, 0)
        # 真实场景是 屏蔽掉为0 填充的非广告
        self.mask = tf.ones((context_length, context_length), dtype=tf.bool)

    def call(self, x):
        b, num_tokens, d_in = tf.shape(x)

        # 计算查询、键、值
        keys = self.W_key(x)
        queries = self.W_query(x)
        values = self.W_value(x)

        # 重塑以形成多头结构
        keys = tf.reshape(keys, (b, num_tokens, self.num_heads, self.head_dim))
        values = tf.reshape(values, (b, num_tokens, self.num_heads, self.head_dim))
        queries = tf.reshape(queries, (b, num_tokens, self.num_heads, self.head_dim))

        # 转置以将头维度放在前面
        keys = tf.transpose(keys, perm=[0, 2, 1, 3])
        queries = tf.transpose(queries, perm=[0, 2, 1, 3])
        values = tf.transpose(values, perm=[0, 2, 1, 3])

        # 计算注意力得分
        attn_scores = tf.matmul(queries, keys, transpose_b=True)

        # 应用掩码
        mask_bool = self.mask[:num_tokens, :num_tokens]
        attn_scores = tf.where(mask_bool, -1e9, attn_scores)

        # 计算注意力权重
        attn_weights = tf.nn.softmax(attn_scores / (self.head_dim ** 0.5), axis=-1)
        attn_weights = self.dropout(attn_weights)

        # 计算上下文向量
        context_vec = tf.matmul(attn_weights, values)
        context_vec = tf.transpose(context_vec, perm=[0, 2, 1, 3])

        # 合并多头
        context_vec = tf.reshape(context_vec, (b, num_tokens, self.d_out))
        # 可能的输出投影
        context_vec = self.out_proj(context_vec)

        return context_vec

class TransformerBlock(tf.keras.layers.Layer):
    def __init__(self, cfg, **kwargs):
        super().__init__(**kwargs)
        # 多头注意力层
        self.att = MultiHeadAttention(
            d_in=cfg["emb_dim"],
            d_out=cfg["emb_dim"],
            context_length=cfg["context_length"],
            num_heads=cfg["n_heads"],
            dropout=cfg["drop_rate"],
            qkv_bias=cfg["qkv_bias"]
        )
        # 前馈网络层，需要定义
        self.ff = FeedForward(cfg)
        # 层归一化层
        self.norm1 = tf.keras.layers.LayerNormalization(epsilon=1e-6)
        self.norm2 = tf.keras.layers.LayerNormalization(epsilon=1e-6)
        # Dropout层
        self.drop_shortcut = tf.keras.layers.Dropout(cfg["drop_rate"])

    def call(self, x):
        # 添加shortcut连接
        shortcut = x
        x = self.norm1(x)
        x = self.att(x)
        x = self.drop_shortcut(x)
        x = x + shortcut

        # 添加第二个shortcut连接
        shortcut = x
        x = self.norm2(x)
        x = self.ff(x)  # 前馈网络
        x = self.drop_shortcut(x)
        x = x + shortcut

        return x
