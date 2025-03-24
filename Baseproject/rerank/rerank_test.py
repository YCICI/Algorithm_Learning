'''
Author: yuchuchu yuchuchu77@163.com
Date: 2025-03-13 21:04:56
LastEditors: yuchuchu yuchuchu77@163.com
LastEditTime: 2025-03-14 17:56:49
FilePath: /Algorithm_Learning/Baseproject/rerank/rerank_test.py
Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
'''
import tensorflow.compat.v1 as tf
# import transformblock as transformblock, MultiHeadAttention
tf.disable_v2_behavior()


class MultiHeadAttention(tf.keras.layers.Layer):
    def __init__(self, d_in, d_out, context_length, dropout, num_heads, ad_idx_map, qkv_bias=False, **kwargs):
        super().__init__(**kwargs)
        print(d_out,num_heads)
        assert (d_out % num_heads == 0), "d_out must be divisible by num_heads"

        self.d_in = d_in  # d_in # the input embedding size
        self.d_out = d_out # the output embedding size
        self.num_heads = num_heads
        self.head_dim = d_out // num_heads
        print("self.head_dim", self.head_dim)
        self.ad_idx_map = ad_idx_map

        # 定义查询、键、值的线性变换
        self.W_query = tf.keras.layers.Dense(d_out, use_bias=qkv_bias)
        self.W_key = tf.keras.layers.Dense(d_out, use_bias=qkv_bias)
        self.W_value = tf.keras.layers.Dense(d_out, use_bias=qkv_bias)
        # 定义输出的线性变换
        self.out_proj = tf.keras.layers.Dense(d_out)
        # 定义dropout层
        self.dropout = tf.keras.layers.Dropout(dropout)
        # 创建掩码，用于屏蔽未来的信息
        # self.mask = tf.linalg.band_part(tf.ones((context_length, context_length)), -1, 0) 
        # 真实场景是 屏蔽掉为0 填充的非广告
        # self.mask = tf.ones((context_length, context_length), dtype=tf.bool)
    def ads_mask(self):
        mask = tf.cast(tf.not_equal(self.ad_idx_map, 0), tf.float32)
        mask = tf.Print(mask, [tf.shape(mask), mask], message='mask: ', summarize=100)

        mask = tf.expand_dims(mask, -1)  # [batch_size, ads, 1]
        mask = tf.matmul(mask, tf.transpose(mask, [0, 2, 1]))
        
        mask = tf.Print(mask, [tf.shape(mask), mask], message='mask: ', summarize=100)
        mask = tf.expand_dims(mask, 1) #[batch_size, 1, ads, ads]
        mask = tf.tile(mask, [1, self.num_heads, 1, 1]) 
        # mask = tf.tile(mask, [1, self.num_heads, 1])  # [batch_size, num_heads, num_tokens]
        # mask = tf.expand_dims(mask, 2)  # [batch_size, num_heads, 1, num_tokens]
        # mask_t = tf.transpose(mask, [0, 1, 3, 2])  # [batch_size, num_heads, num_tokens, 1]
        # mask_t = tf.Print(mask_t, [tf.shape(mask_t), mask_t], message='mask_t: ', summarize=100)
        return mask

    def call(self, x):
        b, num_tokens, d_in = tf.unstack(tf.shape(x))

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
        attn_scores = tf.Print(attn_scores, [tf.shape(attn_scores), attn_scores], message='attn_scores: ', summarize=100)

        # 应用掩码 
        ads_mask =  self.ads_mask()
        attn_scores = tf.where(ads_mask > 0, attn_scores, tf.fill(tf.shape(attn_scores), -1e9))
        

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

        return context_vec, attn_scores

class YourModelClass:
    def __init__(self, trival_emb_size, ad_idx_map):
        self.trival_emb_size = trival_emb_size
        self.ad_idx_map = ad_idx_map
    

    def create_context_samples(self, feature_embeddings):
        feature_embeddings = tf.reshape(feature_embeddings, [-1, self.trival_emb_size])
        feature_embeddings = tf.concat([feature_embeddings, tf.zeros([1, self.trival_emb_size], dtype=feature_embeddings.dtype)], axis=0)
        # ad_idx_map = tf.expand_dims(ad_idx_map, -1)
        context_samples = tf.nn.embedding_lookup(feature_embeddings, self.ad_idx_map)
        context_samples = tf.Print(context_samples, [tf.shape(context_samples), context_samples], message='context_samples: ', summarize=100)

        return context_samples
    
    def context_network(self, context_samples):
        # context_samples shape: (batch_size, num_ads, ns_num * emb_size)
        context_samples_shape = context_samples.get_shape()
        print(context_samples_shape)
        d_in = context_samples_shape[-1].value
        d_out = 8
        print("d_out", d_out)
        context_length = context_samples_shape[1].value
        print("context_length", context_length)
        # context_embedding = MultiHeadAttention(d_in, d_out, context_length, dropout = 0.01, num_heads = 1, qkv_bias=False)(context_samples)
        
        context_embedding = MultiHeadAttention(d_in, d_out, context_length, dropout = 0.01, num_heads = 2, ad_idx_map=self.ad_idx_map ,qkv_bias=False)(context_samples)

        return context_embedding
    
    def select_context_embedding(self, context_embedding):
        # context_embedding shape: (batch_size, num_ads, ns_num * emb_size)
        mask = tf.cast(self.ad_idx_map, tf.bool)
        non_zero_positions = tf.where(mask)
        selected_context_embedding = tf.gather_nd(context_embedding, non_zero_positions)
        selected_context_embedding = tf.Print(selected_context_embedding, [tf.shape(selected_context_embedding), selected_context_embedding], message='selected_context_embedding: ', summarize=100)

        return selected_context_embedding

# 假设的 trival_emb_size，根据实际情况调整
trival_emb_size = 64

# 创建模拟的 feature_embeddings 数据
# 假设有 10 个广告，每个广告的嵌入维度是 trival_emb_size
num_ads = 25
feature_embeddings = tf.random.uniform([num_ads, trival_emb_size])

# 创建模拟的 ad_idx_map 数据
# 假设有 512 个样本，每个样本有 10 个广告索引
# ad_idx_map = tf.random.uniform([5, 10], maxval=num_ads, dtype=tf.int32)
ad_idx_map =  tf.constant([
    [1, 2, 0, 0, 0, 0, 0, 0, 0, 0],
    [3, 4, 5, 0, 0, 0, 0, 0, 0, 0],
    [6, 7, 8, 9, 10, 11, 0, 0, 0, 0],
    [12, 13, 14, 15, 16, 17, 18, 19, 20, 21],
    [22, 0, 0, 0, 0, 0, 0, 0, 0, 0]
], dtype=tf.int32)
# mask_upper_triangle = tf.linalg.band_part(tf.ones_like(ad_idx_map), -1, 0)
# ad_idx_map = ad_idx_map * mask_upper_triangle


# 创建模型实例
model = YourModelClass(trival_emb_size=trival_emb_size, ad_idx_map = ad_idx_map)

# 调用函数
context_samples = model.create_context_samples(feature_embeddings)
context_vec, attn_scores = model.context_network(context_samples)
selected_context_embedding=model.select_context_embedding(context_vec)

# 启动 TensorFlow 会话来运行测试
with tf.Session() as sess:
    # 初始化变量
    sess.run(tf.global_variables_initializer())
    ad_idx_map_result = sess.run(ad_idx_map)
    print("ad_idx_map:")
    print(ad_idx_map_result.shape)
    print(ad_idx_map_result)
    # 计算结果
    context_samples_result = sess.run(context_samples)
    context_vec_result, attn_scores = sess.run([context_vec, attn_scores])
    select_context_result = sess.run(selected_context_embedding)
    # print(context_samples_result.shape)
    # print(context_samples_result)
    # # 输出应该是 (512, 65, trival_emb_size)
    
    # print(context_vec_result.shape)
    # print(context_vec_result)
    
    # print(select_context_result.shape)
    # print(select_context_result)
    
    # print("attn_scores")
    # print(attn_scores.shape)
    # print(attn_scores)