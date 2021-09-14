import tensorflow as tf

USER_NUM = 579466
ANIME_NUM = 17746

def get_dataset(file_path):
    dataset = tf.data.experimental.make_csv_dataset(
        file_path,
        batch_size=12,
        label_name='label',
        na_value='0',
        num_epochs=1,
        ignore_errors=True
    )
    return dataset

if __name__ == '__main__':
    gpus = tf.config.experimental.list_physical_devices(device_type='GPU')
    for gpu in gpus:
        print(gpu)
        tf.config.experimental.set_memory_growth(gpu, True)
    # 训练数据集路径
    train_samples_file_path = r"D:\IDEA\bangumi-recommend-system\src\main\resources\sampledata\trainSample\*.csv"
    # 测试数据集路径
    test_samples_file_path = r"D:\IDEA\bangumi-recommend-system\src\main\resources\sampledata\testSample\*.csv"
    train_dataset = get_dataset(train_samples_file_path)
    test_dataset = get_dataset(test_samples_file_path)

    # 定义输入
    inputs = {
        'userAvgRating': tf.keras.layers.Input(name='userAvgRating', shape=(), dtype='float32'),
        'userRatingStddev': tf.keras.layers.Input(name='userRatingStddev', shape=(), dtype='float32'),
        'userRatingCount': tf.keras.layers.Input(name='userRatingCount', shape=(), dtype='int32'),
        'votes': tf.keras.layers.Input(name='votes', shape=(), dtype='float32'),
        'score': tf.keras.layers.Input(name='score', shape=(), dtype='float32'),

        'animeId': tf.keras.layers.Input(name='animeId', shape=(), dtype='int32'),
        'userId': tf.keras.layers.Input(name='userId', shape=(), dtype='int32'),
        'userRatedAnime1': tf.keras.layers.Input(name='userRatedAnime1', shape=(), dtype='int32'),

        'userTag1': tf.keras.layers.Input(name='userTag1', shape=(), dtype='string'),
        'userTag2': tf.keras.layers.Input(name='userTag2', shape=(), dtype='string'),
        'userTag3': tf.keras.layers.Input(name='userTag3', shape=(), dtype='string'),
        'userTag4': tf.keras.layers.Input(name='userTag4', shape=(), dtype='string'),
        'userTag5': tf.keras.layers.Input(name='userTag5', shape=(), dtype='string'),
        'animeTag1': tf.keras.layers.Input(name='animeTag1', shape=(), dtype='string'),
        'animeTag2': tf.keras.layers.Input(name='animeTag2', shape=(), dtype='string'),
        'animeTag3': tf.keras.layers.Input(name='animeTag3', shape=(), dtype='string'),
    }
    # animeId
    anime_col = tf.feature_column.categorical_column_with_identity(key='animeId', num_buckets=ANIME_NUM)
    anime_emb_col = tf.feature_column.embedding_column(anime_col, 10)
    anime_ind_col = tf.feature_column.indicator_column(anime_col)
    # userId
    user_col = tf.feature_column.categorical_column_with_identity(key='userId', num_buckets=USER_NUM)
    user_emb_col = tf.feature_column.embedding_column(user_col, 10)
    user_ind_col = tf.feature_column.indicator_column(user_col)
    # 标签词表
    tag_vocabulary = ['Film-Noir', 'Action', 'Adventure', 'Horror', 'Romance', 'War', 'Comedy', 'Western', 'Documentary',
                      'Sci-Fi', 'Drama', 'Thriller',
                      'Crime', 'Fantasy', 'Animation', 'IMAX', 'Mystery', 'Children', 'Musical']
    # 用户标签
    user_tag_col = tf.feature_column.categorical_column_with_vocabulary_list(key='userTag1', vocabulary_list=tag_vocabulary)
    user_tag_emb_col = tf.feature_column.embedding_column(user_tag_col, 10)
    user_tag_ind_col = tf.feature_column.indicator_column(user_tag_col)
    # 动画标签
    anime_tag_col = tf.feature_column.categorical_column_with_vocabulary_list(key='animeTag1', vocabulary_list=tag_vocabulary)
    anime_tag_emb_col = tf.feature_column.embedding_column(anime_tag_col, 10)
    anime_tag_ind_col = tf.feature_column.indicator_column(anime_tag_col)

    # fm first-order term columns: without embedding and concatenate to the output layer directly
    fm_first_order_columns = [anime_ind_col, user_ind_col, user_tag_ind_col, anime_tag_ind_col]

    deep_feature_columns = [
        tf.feature_column.numeric_column('votes'),
        tf.feature_column.numeric_column('score'),
        tf.feature_column.numeric_column('userRatingCount'),
        tf.feature_column.numeric_column('userAvgRating'),
        tf.feature_column.numeric_column('userRatingStddev'),
        anime_emb_col,
        user_emb_col
    ]

    # embedding
    item_emb_layer = tf.keras.layers.DenseFeatures([anime_emb_col], inputs)
    user_emb_layer = tf.keras.layers.DenseFeatures([user_emb_col], inputs)
    item_tag_emb_layer = tf.keras.layers.DenseFeatures([anime_tag_emb_col], inputs)
    user_tag_emb_layer = tf.keras.layers.DenseFeatures([user_tag_emb_col], inputs)

    fm_first_order_layer = tf.keras.layers.DenseFeatures(fm_first_order_columns)(inputs)

    # FM部分，交叉特征的embedding
    #   userId animeId
    product_layer_item_user = tf.keras.Dot(axes=1)([item_emb_layer, user_emb_layer])
    #   userTag animeTag
    product_layer_item_tag_user_tag = tf.keras.Dot(axes=1)([item_tag_emb_layer, user_tag_emb_layer])
    #   userId animeTag
    product_layer_item_tag_user = tf.keras.Dot(axes=1)([item_tag_emb_layer, user_emb_layer])
    #   itemId userTag
    product_layer_user_tag_item = tf.keras.Dot(axes=1)([user_tag_emb_layer, item_emb_layer])
    # deep部分， 将所有特征输入MLP
    deep = tf.keras.layers.DenseFeatures(deep_feature_columns)(inputs)
    deep = tf.keras.layers.Dense(64, activation='relu')(deep)
    deep = tf.keras.layers.Dense(64, activation='relu')(deep)
    # 拼接FM部分和deep部分
    concat_layer = tf.keras.layers.concatenate([
        fm_first_order_layer,
        product_layer_item_user,
        product_layer_item_tag_user_tag,
        product_layer_item_tag_user,
        product_layer_user_tag_item,
        deep
    ], axis=1)
    # 输出层
    output_layer = tf.keras.layers.Dense(1, activation='sigmoid')(concat_layer)

    model = tf.keras.Model(inputs, output_layer)
    # 编译模型
    model.compile(
        loss='binary_crossentropy',
        optimizer='adam',
        metrics=['accuracy', tf.keras.metrics.AUC(curve='ROC'), tf.keras.metrics.AUC(curve='PR')]
    )
    # 训练模型
    model.fit(train_dataset, epochs=5)
    # 评估模型
    # 评估模型
    test_loss, test_accuracy, test_roc_auc, test_pr_auc = model.evaluate(test_dataset)
    print('\n\nTest Loss {}, Test Accuracy {}, Test ROC AUC {}, Test PR AUC {}'.format(test_loss, test_accuracy,
                                                                                       test_roc_auc, test_pr_auc))
    # 预测
    # predictions = model.predict(test_dataset)
    # for prediction, goodRating in zip(predictions[:12], list(test_dataset)[0][1][:12]):
    #     print("Predicted good rating: {:.2%}".format(prediction[0]),
    #           " | Actual rating label: ",
    #           ("Good Rating" if bool(goodRating) else "Bad Rating"))
    # 保存模型
    tf.keras.models.save_model(
        model,
        "D:/IDEA/bangumi-recommend-system/src/main/resources/model/embmlp/001", # 001是版本号
        overwrite=True,
        include_optimizer=True,
        save_format=None,
        signatures=None,
        options=None
    )


