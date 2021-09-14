import tensorflow as tf
import pymongo

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

def get_tags():
    client = pymongo.MongoClient('mongodb://localhost:27017/')
    db = client['bangumi_test']
    collection = db['tag']
    tags = []
    for tag in collection.find():
        if tag['count'] > 1: # 去掉无意义的标签
            tags.append(tag['name'])
    # 去掉重复的标签
    return set(tags)

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

    # genre features vocabulary
    tag_vocabulary = get_tags()

    TAG_FEATURES = {
        'userTag1': tag_vocabulary,
        'userTag2': tag_vocabulary,
        'userTag3': tag_vocabulary,
        'userTag4': tag_vocabulary,
        'userTag5': tag_vocabulary,
        'animeTag1': tag_vocabulary,
        'animeTag2': tag_vocabulary,
        'animeTag3': tag_vocabulary
    }

    # 类别型特征
    categorical_columns = []
    # 用户和动画标签
    for feature, vocabulary in TAG_FEATURES.items():
        # 通过词表将字符串类型的类别特征转换成one-hot向量
        onehot_col = tf.feature_column.categorical_column_with_vocabulary_list(key=feature, vocabulary_list=vocabulary)
        # 将one-hot向量转换为10维的embedding向量
        emb_col = tf.feature_column.embedding_column(onehot_col, 10)
        categorical_columns.append(emb_col)
    # animeId
    anime_col = tf.feature_column.categorical_column_with_identity(key='animeId', num_buckets=ANIME_NUM)
    anime_emb_col = tf.feature_column.embedding_column(anime_col, 10)
    categorical_columns.append(anime_emb_col)
    # userId
    user_col = tf.feature_column.categorical_column_with_identity(key='userId', num_buckets=USER_NUM)
    user_emb_col = tf.feature_column.embedding_column(user_col, 10)
    categorical_columns.append(user_emb_col)

    # 数值型特征
    numerical_columns =[
        # tf.feature_column.numeric_column('releaseYear'),
        tf.feature_column.numeric_column('votes'),
        tf.feature_column.numeric_column('score'),
        tf.feature_column.numeric_column('userRatingCount'),
        tf.feature_column.numeric_column('userAvgRating'),
        tf.feature_column.numeric_column('userRatingStddev')
    ]

    # 构建模型
    model = tf.keras.Sequential([
        tf.keras.layers.DenseFeatures(numerical_columns + categorical_columns),
        tf.keras.layers.Dense(128, activation='relu'),
        tf.keras.layers.Dense(128, activation='relu'),
        tf.keras.layers.Dense(1, activation='sigmoid')
    ])
    model.compile(
        loss='binary_crossentropy',
        optimizer='adam',
        metrics=['accuracy', tf.keras.metrics.AUC(curve='ROC'), tf.keras.metrics.AUC(curve='PR')]
    )
    # 训练模型
    model.fit(train_dataset, epochs=5)
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