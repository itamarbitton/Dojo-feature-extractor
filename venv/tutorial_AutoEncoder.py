import tensorflow as tf
from tensorflow.examples.tutorials.mnist import input_data

mnist = input_data.read_data_sets("/tmp/data", one_hot=True)

n_nodes_1 = 500
n_nodes_2 = 500
n_nodes_3 = 500

n_classes = 10
batch_size = 100

x = tf.placeholder('float', [None, 724])
y = tf.placeholder('float')


