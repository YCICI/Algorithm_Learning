import tensorflow as tf
import os


def get_dataset(file_path): 
    """
    获取数据
    """
    dataset = tf.data.experimental.make_csv_dataset(
        file_path,
        batch_size=12,
        label_name='label',
        na_value="?",
        num_epochs=1,
        ignore_errors=True
    ) 
    return dataset

def feature_process():
    '''
    特征处理
    '''
    
    ## 类别特征
    categorical_columns = []
    movie_col = tf.feature_column.categorical_column_with_identity(key='movieId', num_buckets=1001)
    movie_emb_col = tf.feature_column.embedding_column(movie_col, 10)
    categorical_columns.append(movie_emb_col)
    
    user_col = tf.feature_column.categorical_column_with_identity(key='userId', num_buckets=30001)
    user_emb_col = tf.feature_column.embedding_column(user_col, 10)
    categorical_columns.append(user_emb_col)
    
    # 类别特征one-hot编码
    genre_vocab = ['Film-Noir', 'Action', 'Adventure', 'Horror', 'Romance', 'War', 'Comedy', 'Western', 'Documentary', 'Sci-Fi', 'Drama', 'Thriller', 'Crime', 'Fantasy', 'Animation', 'IMAX', 'Mystery', 'Children', 'Musical']
    GENRE_FEATURES = { 'userGenre1': genre_vocab, 
                      'userGenre2': genre_vocab, 
                      'userGenre3': genre_vocab, 
                      'userGenre4': genre_vocab, 
                      'userGenre5': genre_vocab, 
                      'movieGenre1': genre_vocab, 
                      'movieGenre2': genre_vocab, 
                      'movieGenre3': genre_vocab}
    for feature, vocab in GENRE_FEATURES.items(): 
        cat_col = tf.feature_column.categorical_column_with_vocabulary_list(
            key=feature, 
            vocabulary_list=vocab
        ) 
        emb_col = tf.feature_column.embedding_column(cat_col, 10) 
        categorical_columns.append(emb_col)
    
    # 数值类的特征
    
    numerical_columns = [tf.feature_column.numeric_column('releaseYear'),
                         tf.feature_column.numeric_column('movieRatingCount'), 
                         tf.feature_column.numeric_column('movieAvgRating'), 
                         tf.feature_column.numeric_column('movieRatingStddev'), 
                         tf.feature_column.numeric_column('userRatingCount'), 
                         tf.feature_column.numeric_column('userAvgRating'), 
                         tf.feature_column.numeric_column('userRatingStddev')]
    
    return categorical_columns, numerical_columns
    

def init_model(numerical_columns, categorical_columns):
    preprocessing_layer = tf.keras.layers.DenseFeatures(numerical_columns + categorical_columns)
    model = tf.keras.Sequential([ preprocessing_layer, 
                                 tf.keras.layers.Dense(128, activation='relu'),
                                 tf.keras.layers.Dense(128, activation='relu'), 
                                 tf.keras.layers.Dense(1, activation='sigmoid'),]
                                )
    return model

def main():
    # 获取当前文件所在的目录
    current_file = os.path.abspath(__file__)
    current_dir = os.path.dirname(current_file)

    TRAIN_DATA_URL = "file://" + current_dir + '/data/trainingSamples.csv'
    samples_file_path = tf.keras.utils.get_file("modelSamples.csv", TRAIN_DATA_URL)
    # sample dataset size 110830/12(batch_size) = 9235
    raw_samples_data = get_dataset(samples_file_path)
    
    test_dataset = raw_samples_data.take(1000)
    train_dataset = raw_samples_data.skip(1000)
    
    categorical_columns, numerical_columns = feature_process()
    model = init_model(numerical_columns, categorical_columns)
    
    model.compile(
        loss='binary_crossentropy',
        optimizer='adam',
        metrics=['accuracy']
        )
    
    model.fit(train_dataset, epochs=10)
    test_loss, test_accuracy = model.evaluate(test_dataset)
    print('\n\nTest Loss {}, Test Accuracy {}'.format(test_loss, test_accuracy))

    return


if __name__ == "__main__":
    main()
    