import keras
import os
#from tensorflow import keras
import tensorflow as tf
from tensorflow.python.util import compat
from keras import backend as K
from keras.models import load_model
from base_model import DNN

def export_savedmodel(model):
    '''
    传入keras model会自动保存为pb格式
    '''
    model_path = "D:/z_personal_file/DiDi/ycc/ycc/MTL_deepfm/MTL_deepfm/model"  # 模型保存的路径
    model_version = 0  # 模型保存的版本
    # 从网络的输入输出创建预测的签名
    model_signature = tf.saved_model.signature_def_utils.predict_signature_def(
        inputs={'input': model.input}, outputs={'output': model.output})
    # 使用utf-8编码将 字节或Unicode 转换为字节
    export_path = os.path.join(compat.as_bytes(model_path), compat.as_bytes(str(model_version)))  # 将保存路径和版本号join
    builder = tf.saved_model.builder.SavedModelBuilder(export_path)  # 生成"savedmodel"协议缓冲区并保存变量和模型
    builder.add_meta_graph_and_variables(  # 将当前元图添加到savedmodel并保存变量
        sess=K.get_session(),  # 返回一个 session 默认返回tf的sess,否则返回keras的sess,两者都没有将创建一个全新的sess返回
        tags=[tf.saved_model.tag_constants.SERVING],  # 导出模型tag为SERVING(其他可选TRAINING,EVAL,GPU,TPU)
        clear_devices=True,  # 清除设备信息
        signature_def_map={  # 签名定义映射
            tf.saved_model.signature_constants.DEFAULT_SERVING_SIGNATURE_DEF_KEY:  # 默认服务签名定义密钥
                model_signature  # 网络的输入输出策创建预测的签名
        })
    builder.save()  # 将"savedmodel"协议缓冲区写入磁盘.
    print("save model pb success ...")

#model = keras.models.load_model('D:/z_personal_file/DiDi/ycc/ycc/MTL_deepfm/MTL_deepfm/model/deepfm_mtl_20200305.h5')  # 加载已训练好的.h5格式的keras模

#model = tf.keras.models.load_model('D:/z_personal_file/DiDi/ycc/ycc/MTL_deepfm/MTL_deepfm/model/deepfm_mtl_20200305.h5',custom_objects={'DNN':DNN})
model= tf.keras.models.load_model('D:/z_personal_file/DiDi/ycc/ycc/MTL_deepfm/MTL_deepfm/model/deepfm_mtl_20200305.h5',custom_objects={'DNN': base_model.DNN})
export_savedmodel(model)  # 将模型传入保存模型的方法内,模型保存成功.