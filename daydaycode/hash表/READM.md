### 总结

#### 一、python中的dict(hashmap)和set

**1.1 dict(hashmap)**


* 形式[key:value]   
key-> int,str   
value->int,str,list,tuple   

* 增
    ```python
    example_dict = {}
    example_dict['jack'] = 11 
    # example_dict {'jack': 11}
    ```

* 删 pop

    ```python
    example_dict = {}
    example_dict['jack'] = 11 
    # example_dict {'jack': 11}
    example_dict['bob'] = 12
    print(example_dict)
    # 
    example_dict.pop('bob')
    print(example_dict)
    #{'jack': 11, 'bob': 12}
    #{'jack': 11}

    ```
* 改
```python
    example_dict = {}
    example_dict['jack'] = 11 
    # example_dict {'jack': 11}
    example_dict['jack'] = 12
    print(example_dict)
    # example_dict {'jack': 12}
```

* 查

```python
    example_dict = {}
    example_dict['jack'] = 11 
    example_dict.get('jack')

    #
    print('jack' in example_dict)

```

**1.2 set**     
有序，不重合

* 增 add
    ```python
    se=set()
    se.add('a')
    se.add('b')
    se.add('a')
    se.add('c')
    print(se)
    # {'c', 'a', 'b'}
    ```
* 删 remove
    ```python
    se=set()
    se.add('a')
    se.add('b')
    se.add('a')
    se.add('c')
    print(se)
    # {'c', 'a', 'b'}
    se.remove('c')
    print(se)
    # {'a', 'b'}
    ```
* 改
* 查
    ```python
    se=set()
    se.add('a')
    se.add('b')
    se.add('a')
    se.add('c')
    print(se)
    # {'c', 'a', 'b'}
    se.remove('c')
    print(se)
    # {'a', 'b'}
    print('a' in se)
    # True
    ```


**1.3 我的问题**  


