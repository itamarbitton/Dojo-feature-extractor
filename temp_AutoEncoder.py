from __future__ import division, print_function, absolute_import
import tensorflow as tf
import Data_Generator
from sklearn.metrics import mean_squared_error
from math import sqrt
import numpy as np
import time
from Ipfix_Constants import *
import statistics

# trn_data_percentage = how much from all the records allocated for training should we use in percentage
# part_of_data => 0 for all of the data, 1 for first 33%, 2 for middle 33%, 3 for most recent 33%
def train_and_test(train_data_size, offset_into_train_dataset):

    with open(september_meta_report, 'r') as csv_meta_report:
        reader = csv.reader(csv_meta_report)
        next(reader)  # ignore the path to dataset
        next(reader)  # ignore the unique number of ip samples

        trn_size = int(next(reader)[1])
        opt_size = int(next(reader)[1])
        tst_size = int(next(reader)[1])

    gen_class = Data_Generator.Data_Gen()
    trn_gen = gen_class.trn_data_gen(train_data_size, offset_into_file=offset_into_train_dataset)
    tst_gen = gen_class.tst_data_gen(trn_size + opt_size, tst_size)

    # Training Parameters
    learning_rate = 0.01
    num_steps = 1  # epochs
    display_step = 500000

    # Network Parameters
    num_hidden_1 = 8  # 1st layer num features
    num_hidden_2 = 4  # 2nd layer num features (the latent dim)
    num_input = 10  #

    X = tf.placeholder("float", [None, num_input])

    weights = {
        'encoder_h1': tf.Variable(tf.random_normal([num_input, num_hidden_1])),
        'encoder_h2': tf.Variable(tf.random_normal([num_hidden_1, num_hidden_2])),
        'decoder_h1': tf.Variable(tf.random_normal([num_hidden_2, num_hidden_1])),
        'decoder_h2': tf.Variable(tf.random_normal([num_hidden_1, num_input])),
    }
    biases = {
        'encoder_b1': tf.Variable(tf.random_normal([num_hidden_1])),
        'encoder_b2': tf.Variable(tf.random_normal([num_hidden_2])),
        'decoder_b1': tf.Variable(tf.random_normal([num_hidden_1])),
        'decoder_b2': tf.Variable(tf.random_normal([num_input])),
    }

    # Building the encoder
    def encoder(x):
        # Encoder Hidden layer with sigmoid activation #1
        layer_1 = tf.nn.sigmoid(tf.add(tf.matmul(x, weights['encoder_h1']),
                                       biases['encoder_b1']))
        # Encoder Hidden layer with sigmoid activation #2
        layer_2 = tf.nn.sigmoid(tf.add(tf.matmul(layer_1, weights['encoder_h2']),
                                       biases['encoder_b2']))
        return layer_2

    # Building the decoder
    def decoder(x):
        # Decoder Hidden layer with sigmoid activation #1
        layer_1 = tf.nn.sigmoid(tf.add(tf.matmul(x, weights['decoder_h1']),
                                       biases['decoder_b1']))
        # Decoder Hidden layer with sigmoid activation #2
        layer_2 = tf.nn.sigmoid(tf.add(tf.matmul(layer_1, weights['decoder_h2']),
                                       biases['decoder_b2']))
        return layer_2

    # Construct model
    encoder_op = encoder(X)
    decoder_op = decoder(encoder_op)

    # Prediction
    y_pred = decoder_op
    # Targets (Labels) are the input data.
    y_true = X

    # Define loss and optimizer, minimize the root mean squared error
    loss = tf.sqrt(tf.reduce_mean(tf.square(tf.subtract(y_true, y_pred))))
    # optimizer = tf.train.RMSPropOptimizer(learning_rate).minimize(loss)
    optimizer = tf.train.AdamOptimizer(learning_rate).minimize(loss)
    # Initialize the variables (i.e. assign their default value)
    init = tf.global_variables_initializer()

    start_time = time.time()

    # Start Training
    # Start a new TF session
    with tf.Session() as sess:

        # Run the initializer
        sess.run(init)

        for epoch in range(0, num_steps):
            print('Epoch: ', epoch)
            # Training
            for i in range(1, train_data_size + 1):
                # Prepare Data
                # (only features are needed, not labels)
                batch_x, _ = next(trn_gen)

                # Run optimization op (backprop) and cost op (to get loss value)
                _, l = sess.run([optimizer, loss], feed_dict={X: batch_x})
                # Display logs per step
                if i % display_step == 0 or i == 1:
                    print('Step %i: Minibatch Loss: %f' % (i, l))

        avg_rmse = 0
        rmse_list = []
        # Testing
        for i in range(1, tst_size + 1):
            batch_x, _ = next(tst_gen)
            g = sess.run(decoder_op, feed_dict={X: batch_x})
            rmse = sqrt(mean_squared_error(np.asarray(batch_x), g))
            avg_rmse += (rmse / tst_size)
            rmse_list.append(rmse)
        # calculating standard deviation
        standart_dev = statistics.stdev(rmse_list)
    print("--- %s seconds ---" % (time.time() - start_time))
    return [avg_rmse, standart_dev]




