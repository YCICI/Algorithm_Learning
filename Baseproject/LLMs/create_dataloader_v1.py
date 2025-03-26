
from importlib.metadata import version  # 导入版本模块
import tiktoken  # 导入tiktoken库
print("tiktoken version:", version("tiktoken"))  # 打印tiktoken版本

import torch  # 导入torch
from torch.utils.data import Dataset, DataLoader  # 从torch.utils.data导入Dataset和DataLoader

class GPTDatasetV1(Dataset):  # 定义GPTDatasetV1类，继承自Dataset
    def __init__(self, txt, tokenizer, max_length, stride):  # 初始化方法
        self.input_ids = []  # 初始化输入ID列表
        self.target_ids = []  # 初始化目标ID列表

        token_ids = tokenizer.encode(txt)  # 对文本进行分词  

        for i in range(0, len(token_ids) - max_length, stride):  # 使用滑动窗口将文本分块  
            input_chunk = token_ids[i:i + max_length]  # 获取输入分块
            target_chunk = token_ids[i + 1: i + max_length + 1]  # 获取目标分块
            self.input_ids.append(torch.tensor(input_chunk))  # 将输入分块转换为张量并添加到列表中
            self.target_ids.append(torch.tensor(target_chunk))  # 将目标分块转换为张量并添加到列表中

    def __len__(self):  # 返回数据集的长度
        return len(self.input_ids)  # 返回输入ID的长度  

    def __getitem__(self, idx):  # 获取指定索引的数据
        return self.input_ids[idx], self.target_ids[idx]  # 返回输入和目标的张量  


def create_dataloader_v1(txt, batch_size=4, max_length=256,  # 创建数据加载器
                         stride=128, shuffle=True, drop_last=True, num_workers=0):
    tokenizer = tiktoken.get_encoding("gpt2")  # 初始化分词器  
    dataset = GPTDatasetV1(txt, tokenizer, max_length, stride)  # 创建数据集  
    dataloader = DataLoader(  # 创建数据加载器
        dataset,
        batch_size=batch_size,
        shuffle=shuffle,
        drop_last=drop_last,  # 如果最后一个批次小于指定批次大小，则丢弃  
        num_workers=num_workers  # 使用的CPU进程数量  
    )
    return dataloader  # 返回数据加载器
