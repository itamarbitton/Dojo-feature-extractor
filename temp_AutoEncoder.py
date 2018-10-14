from __future__ import division, print_function, absolute_import
import tensorflow as tf
import Data_Generator
from sklearn.metrics import mean_squared_error
from math import sqrt
import numpy as np
import time
from Ipfix_Constants import *

unique_ip_lines_range = [1, 347719]
trn_data_lines_range = [347719, 2781752]
opt_data_lines_range = [2781752, 2781752]
tst_data_lines_range = [2781752, 3477190]

# trn_batch_size = trn_data_lines_range[1] - trn_data_lines_range[0]
# opt_batch_size = opt_data_lines_range[1] - opt_data_lines_range[0]
# tst_batch_size = tst_data_lines_range[1] - tst_data_lines_range[0] - 1
trn_batch_size = 60000
opt_batch_size = 0
tst_batch_size = 10000

# Training Parameters
learning_rate = 0.01
num_steps = 2  # epochs
display_step = 10000

# Network Parameters
num_hidden_1 = 8  # 1st layer num features
num_hidden_2 = 4  # 2nd layer num features (the latent dim)
num_input = 8  #

X = tf.placeholder("float", [None, num_input])

gen_class = Data_Generator.Data_Gen()
trn_gen = gen_class.trn_data_gen(trn_batch_size)
tst_gen = gen_class.tst_data_gen(trn_batch_size + opt_batch_size, tst_batch_size)
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

# Define loss and optimizer, minimize the squared error
#loss = tf.reduce_mean(tf.pow(y_true - y_pred, 2))
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
        for i in range(1, trn_batch_size+1):
            # Prepare Data
            # (only features are needed, not labels)
            batch_x, _ = next(trn_gen)

            # Run optimization op (backprop) and cost op (to get loss value)
            _, l = sess.run([optimizer, loss], feed_dict={X: batch_x})
            # Display logs per step
            if i % display_step == 0 or i == 1:
                print('Step %i: Minibatch Loss: %f' % (i, l))

    k = 5
    # Testing
    for i in range(1, tst_batch_size+1):
        batch_x, _ = next(tst_gen)
        g = sess.run(decoder_op, feed_dict={X: batch_x})
        rms = sqrt(mean_squared_error(np.asarray(batch_x), g))
        if k != 0:
            print(rms)
            k -= 1

print("--- %s seconds ---" % (time.time() - start_time))

