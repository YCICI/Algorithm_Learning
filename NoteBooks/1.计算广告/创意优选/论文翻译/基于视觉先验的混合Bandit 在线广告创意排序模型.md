
## 2021: 基于视觉先验的混合Bandit 在线广告创意排序模型

论文地址：https://arxiv.org/pdf/2102.04033.pdf

### 摘要

本文提出：

1）提出视觉感知的排序模型（visual-aware ranking model，VAM），同时也提出了list-wise ranking loss，即通过图片的视觉外观对创意进行排序；  
2）VAM作为先验模型， 混合Bandit模型（ the hybrid bandit model， HBM）作为后验估计；  
3）构建了第一个大规模的[创意数据集](https://tianchi.aliyun.com/dataset/93585)，包括50w个商品，170w个创意，以及他们的曝光点击数据;


### 1、介绍

  为了发掘最有吸引力的创意，理论上所有的创意应该展示给用户。但是为了最大化广告效果，我们倾向展示预测CTR最高的创意。这个预测展示的过程称为多臂老虎机问题(multi-armed bandit, MAB). MAB不仅关注累计奖励最大化，还在有限的探索资源内平衡 exploration-exploitation(E&E)，使得预测的CTR更可信。MAB常见算法有Epsilon-greedy ,Thompson sampling  and Upper Confidence Bounds。但是这些方法，最终都没有很好的解决冷启动问题。解决冷启动问题常见的一个方法是结合视觉先验知识，以促进更好的探索，[3, 8, 9, 21]就是这样做的，但是深度存在计算量大，无法在线灵活更新的问题，因此如何结合视觉先验知识和bandit模型是一个具有挑战性的问题。

  本文提出的