import tensorflow as tf

USER_NUM = 583442
ANIME_NUM = 17746


def get_dataset(file_path):
    dataset = tf.data.experimental.make_csv_dataset(
        file_path,
        batch_size=12,
        label_name="label",
        na_value="0",
        num_epochs=1,
        ignore_errors=True
    )
    return dataset.take()



def neural_cf_model1(feature_inputs, item_feature_cols, user_feature_cols, hidden_layer_units_list):
    """
    NeuralCF模型
    :param feature_inputs: 所有输入特征
    :param item_feature_cols: 物品侧特征（只有animeId)
    :param user_feature_cols: 用户侧特征（只有userId)
    :param hidden_layer_units_list: 隐层神经元数量
    :return:
    """
    # 物品侧特征层
    item_tower = tf.keras.layers.DenseFeatures(item_feature_cols)(feature_inputs)
    # 用户侧特征层
    user_tower = tf.keras.layers.DenseFeatures(user_feature_cols)(feature_inputs)
    # 将物品特征和用户特征拼接
    interact_layer = tf.keras.layers.concatenate([item_tower, user_tower])
    # 连接隐层
    for units in hidden_layer_units_list:
        interact_layer = tf.keras.layers.Dense(units, activation='relu')(interact_layer)
    # 输出层
    output_layer = tf.keras.layers.Dense(1, activation='sigmoid')(interact_layer)
    # 定义keras模型
    neural_cf_model = tf.keras.Model(feature_inputs, output_layer)
    return neural_cf_model

def neural_cf_model2(feature_inputs, item_feature_cols, user_feature_cols, hidden_layer_units_list):
    """
    NeuralCF模型的双塔版本
    :param feature_inputs: 所有输入特征
    :param item_feature_cols: 物品侧特征（只有animeId)
    :param user_feature_cols: 用户侧特征（只有userId)
    :param hidden_layer_units_list: 隐层神经元数量
    :return:
    """
    # 物品侧特征
    item_tower = tf.keras.layers.DenseFeatures(item_feature_cols)(feature_inputs)
    # 物品塔
    for units in hidden_layer_units_list:
        item_tower = tf.keras.layers.Dense(units, activation='relu')(item_tower)
    # 用户侧特征
    user_tower = tf.keras.layers.DenseFeatures(user_feature_cols)(feature_inputs)
    # 用户塔
    for units in hidden_layer_units_list:
        user_tower = tf.keras.layers.Dense(units, activation='relu')(user_tower)
    # 使用内积操作交互物品塔和用户塔，产生最后输出
    output = tf.keras.layers.Dot(axes=1)([item_tower, user_tower])
    # 定义keras模型
    neural_cf_model = tf.keras.Model(feature_inputs, output)
    return neural_cf_model

if __name__ == '__main__':
    gpus = tf.config.experimental.list_physical_devices(device_type='GPU')
    for gpu in gpus:
        print(gpu)
        tf.config.experimental.set_memory_growth(gpu, True)
    # training_samples_file_path = tf.keras.utils.get_file("trainingSamples.csv",
    #                                                     "file:///D:/IDEA/bangumi-recommend-system/src/main/resources/sampledata/trainSample/part-00000-032a54e4-68d4-47ec-a93b-9cb448526b90-c000.csv")
    # test_samples_file_path = tf.keras.utils.get_file("testSamples.csv",
    #                                                  "D:\IDEA\bangumi-recommend-system\src\main\resources\sampledata\trainSample\part-00000-032a54e4-68d4-47ec-a93b-9cb448526b90-c000.csv")
    train_samples_file_path = r"D:\IDEA\bangumi-recommend-system\src\main\resources\sampledata\trainSample\part-00000-f209ebb4-5020-4633-b5d6-2e915e21b519-c000.csv"
    test_samples_file_path = r"D:\IDEA\bangumi-recommend-system\src\main\resources\sampledata\testSample\part-00000-300780f9-e2a5-4ea0-8aca-24870bd59f46-c000.csv"
    # 加载数据
    train_dataset = get_dataset(train_samples_file_path)
    test_dataset = get_dataset(test_samples_file_path)
    # animeId的embedding
    anime_col = tf.feature_column.categorical_column_with_identity('animeId', num_buckets=ANIME_NUM) # 转换成one-hot
    anime_emb_col = tf.feature_column.embedding_column(anime_col, 10)  # embedding
    # userId的embedding
    user_col = tf.feature_column.categorical_column_with_identity('userId', num_buckets=USER_NUM)
    user_emb_col = tf.feature_column.embedding_column(user_col, 10)
    # 定义模型的输入层
    inputs = {
        'animeId': tf.keras.layers.Input(name='animeId', shape=(), dtype='int32'),
        'userId': tf.keras.layers.Input(name='userId', shape=(), dtype='int32')
    }
    # 构建模型
    model = neural_cf_model1(inputs, [anime_emb_col], [user_emb_col], [10, 10])
    model.compile(
        loss='binary_crossentropy',
        optimizer='adam',
        metrics=['accuracy', tf.keras.metrics.AUC(curve='ROC'), tf.keras.metrics.AUC(curve='PR')]
    )
    # 训练模型
    model.fit(train_dataset)
    # 评估模型
    test_loss, test_accuracy, test_roc_auc, test_pr_auc = model.evaluate(test_dataset)
    print('Test Loss: {},  Test Accuracy: {},  Test ROC AUC: {},  Test PR AUC: {}'.format(test_loss, test_accuracy, test_roc_auc, test_pr_auc))
    # 预测
    predictions = model.predict(test_dataset)
    for prediction in predictions:
        print(prediction)
    # 保存模型
    tf.keras.models.save_model(
        model,
        "D:/IDEA/bangumi-recommend-system/src/main/resources/model/neuralcf/001", # 001是版本号
        overwrite=True,
        include_optimizer=True,
        save_format=None,
        signatures=None,
        options=None
    )










