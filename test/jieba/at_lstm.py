#!/usr/bin/env python
# encoding: utf-8
# @author: yangjun
# email: yangjunny@126.com


import sys
import tensorflow as tf
from utils import batch_index, load_inputs_data_at, load_data_init,load_inputs_data_at_test
from helper import wordseg, splitfile
# from evaluation import run_eval
# from sklearn.cross_validation import train_test_split
# from sklearn.cross_validation import StratifiedShuffleSplit
import argparse
import numpy as np
import os,os.path
import time

batch_size_init = 25
n_hidden_init = 300
learning_rate_init = 0.001
n_class_init = 3
l2_reg_init = 0.001
n_iter_init = 10
keep_prob1_init = 0.5
keep_prob2_init = 0.5
method = 'MAT'

# if len(sys.argv)>1:
#     batch_size_init=int(sys.argv[1])
#     n_hidden_init = int(sys.argv[2])
#     learning_rate_init = float(sys.argv[3])
#     n_class_init = int(sys.argv[4])
#     l2_reg_init = float(sys.argv[5])
#     n_iter_init = int(sys.argv[6])
#     keep_prob1_init = float(sys.argv[7])
#     keep_prob2_init = float(sys.argv[8])
#     method = sys.argv[9]




class LSTM(object):

    def __init__(self,vocab_size, embedding_dim=100, pos_embedding_dim=100, batch_size=64, n_hidden=100, learning_rate=0.01,
                 n_class=3, max_sentence_len=140, l2_reg=0., display_step=4, n_iter=100, type_='',category='', rootdir='',date=''):
        self.embedding_dim = embedding_dim
        self.pos_embedding_dim = pos_embedding_dim
        self.batch_size = batch_size
        self.n_hidden = n_hidden
        self.learning_rate = learning_rate
        self.n_class = n_class
        self.max_sentence_len = max_sentence_len
        self.l2_reg = l2_reg
        self.display_step = display_step
        self.n_iter = n_iter
        self.type_ = type_
        self.category = category
        self.rootdir = rootdir
        self.date = date
        self.Embed = tf.Variable(tf.constant(0.0, shape=[vocab_size, embedding_dim]),
                                 trainable=True, name="embedd")
        self.pos_Embed = tf.Variable(tf.constant(0.0, shape=[pos_embedding_dim, pos_embedding_dim]),
                                 trainable=True, name="pos_embedd")
        # self.embedding_placeholder变量用于接受初始化时PreEmbedding参数，其余时刻用不到。
        self.embedding_placeholder = tf.placeholder(tf.float32, [vocab_size, embedding_dim])
        self.pos_embedding_placeholder = tf.placeholder(tf.float32, [pos_embedding_dim, pos_embedding_dim])
        # self.embedding_init用于初始化词向量操作，之后用不到。
        self.embedding_init = self.Embed.assign(self.embedding_placeholder)
        self.pos_embedding_init = self.pos_Embed.assign(self.pos_embedding_placeholder)
        self.sentence_len = tf.placeholder(tf.int32, [None])

        self.keep_prob1 = tf.placeholder(tf.float32)
        self.keep_prob2 = tf.placeholder(tf.float32)

        self.restore=True
        self.needdev=False
        self.pY = []
        if self.restore:
            self.needdev = False
        self.showtrainerr=True

        with tf.name_scope('inputs'):
            self.x = tf.placeholder(tf.int32, [None, self.max_sentence_len], name='x')
            self.pos_x = tf.placeholder(tf.int32, [None, self.max_sentence_len], name='pos_x')
            self.y = tf.placeholder(tf.int32, [None, self.n_class], name='y')
            self.sen_len = tf.placeholder(tf.int32, None, name='sen_len')
            self.entity_id = tf.placeholder(tf.int32, None, name='entity_id')
            self.aspect_id = tf.placeholder(tf.int32, None, name='aspect_id')

        with tf.name_scope('weights'):
            self.weights = {
                'softmax': tf.get_variable(
                    name='softmax_w',
                    shape=[self.n_hidden, self.n_class],
                    initializer=tf.random_uniform_initializer(-0.01, 0.01),
                    regularizer=tf.contrib.layers.l2_regularizer(self.l2_reg)
                )
            }

        with tf.name_scope('biases'):
            self.biases = {
                'softmax': tf.get_variable(
                    name='softmax_b',
                    shape=[self.n_class],
                    initializer=tf.random_uniform_initializer(-0.01, 0.01),
                    regularizer=tf.contrib.layers.l2_regularizer(self.l2_reg)
                )
            }

        self.W = tf.get_variable(
            name='W',
            shape=[self.n_hidden + 2*self.embedding_dim, self.n_hidden +2* self.embedding_dim],
            initializer=tf.random_uniform_initializer(-0.01, 0.01),
            regularizer=tf.contrib.layers.l2_regularizer(self.l2_reg)
        )
        self.w = tf.get_variable(
            name='w',
            shape=[self.n_hidden + 2*self.embedding_dim, 1],
            initializer=tf.random_uniform_initializer(-0.01, 0.01),
            regularizer=tf.contrib.layers.l2_regularizer(self.l2_reg)
        )
        self.b = tf.get_variable(
            name='b',
            shape=[1, self.max_sentence_len],
            initializer=tf.random_uniform_initializer(-0.01, 0.01),
            regularizer=tf.contrib.layers.l2_regularizer(self.l2_reg)
        )
        self.We = tf.get_variable(
            name='We',
            shape=[self.max_sentence_len, self.max_sentence_len],
            initializer=tf.random_uniform_initializer(-0.01, 0.01),
            regularizer=tf.contrib.layers.l2_regularizer(self.l2_reg)
        )
        self.Wa = tf.get_variable(
            name='Wa',
            shape=[self.max_sentence_len, self.max_sentence_len],
            initializer=tf.random_uniform_initializer(-0.01, 0.01),
            regularizer=tf.contrib.layers.l2_regularizer(self.l2_reg)
        )
        self.Wea = tf.get_variable(
            name='Wea',
            shape=[self.embedding_dim, self.n_hidden],
            initializer=tf.random_uniform_initializer(-0.01, 0.01),
            regularizer=tf.contrib.layers.l2_regularizer(self.l2_reg)
        )
        self.Wetat = tf.get_variable(
            name='Wetat',
            shape=[self.n_hidden, self.embedding_dim],
            initializer=tf.random_uniform_initializer(-0.01, 0.01),
            regularizer=tf.contrib.layers.l2_regularizer(self.l2_reg)
        )
        self.Watat = tf.get_variable(
            name='Watat',
            shape=[self.n_hidden, self.embedding_dim],
            initializer=tf.random_uniform_initializer(-0.01, 0.01),
            regularizer=tf.contrib.layers.l2_regularizer(self.l2_reg)
        )
        self.Wp = tf.get_variable(
            name='Wp',
            shape=[self.n_hidden, self.n_hidden],
            initializer=tf.random_uniform_initializer(-0.01, 0.01),
            regularizer=tf.contrib.layers.l2_regularizer(self.l2_reg)
        )
        self.Wx = tf.get_variable(
            name='Wx',
            shape=[self.n_hidden, self.n_hidden],
            initializer=tf.random_uniform_initializer(-0.01, 0.01),
            regularizer=tf.contrib.layers.l2_regularizer(self.l2_reg)
        )
        self.Wdmne = tf.get_variable(
            name='Wdmne',
            shape=[self.embedding_dim, self.embedding_dim],
            initializer=tf.random_uniform_initializer(-0.01, 0.01),
            regularizer=tf.contrib.layers.l2_regularizer(self.l2_reg)
        )
        self.Wdmna = tf.get_variable(
            name='Wdmna',
            shape=[self.embedding_dim, self.embedding_dim],
            initializer=tf.random_uniform_initializer(-0.01, 0.01),
            regularizer=tf.contrib.layers.l2_regularizer(self.l2_reg)
        )
        self.Wdmnr = tf.get_variable(
            name='Wdmnr',
            shape=[self.embedding_dim, self.embedding_dim],
            initializer=tf.random_uniform_initializer(-0.01, 0.01),
            regularizer=tf.contrib.layers.l2_regularizer(self.l2_reg)
        )
        self.Wdmn = tf.get_variable(
            name='Wdmn',
            shape=[self.n_hidden*2, self.n_hidden],
            initializer=tf.random_uniform_initializer(-0.01, 0.01),
            regularizer=tf.contrib.layers.l2_regularizer(self.l2_reg)
        )
        self.Bdmn = tf.get_variable(
            name='Bdmn',
            shape=[1, self.n_hidden],
            initializer=tf.random_uniform_initializer(-0.01, 0.01),
            regularizer=tf.contrib.layers.l2_regularizer(self.l2_reg)
        )
        self.Wdmn_e = tf.get_variable(
            name='Wdmn_e',
            shape=[self.embedding_dim, self.embedding_dim],
            initializer=tf.random_uniform_initializer(-0.01, 0.01),
            regularizer=tf.contrib.layers.l2_regularizer(self.l2_reg)
        )
        self.Wdmn_a = tf.get_variable(
            name='Wdmn_a',
            shape=[self.embedding_dim, self.embedding_dim],
            initializer=tf.random_uniform_initializer(-0.01, 0.01),
            regularizer=tf.contrib.layers.l2_regularizer(self.l2_reg)
        )

    def dynamic_rnn_2(self, cell, inputs, length, max_len, scope_name, out_type='all'):
        outputs, state = tf.nn.dynamic_rnn(
            cell(self.n_hidden),
            inputs=inputs,
            sequence_length=length,
            dtype=tf.float32,
            scope=scope_name
        )  # outputs -> batch_size * max_len * n_hidden
        cell2 = tf.nn.rnn_cell.LSTMCell
        outputs, state = tf.nn.dynamic_rnn(
            cell2(self.n_hidden),
            inputs=outputs,
            sequence_length=length,
            dtype=tf.float32,
            scope=scope_name
        )
        batch_size = tf.shape(outputs)[0]
        if out_type == 'last':
            index = tf.range(0, batch_size) * max_len + (length - 1)
            outputs = tf.gather(tf.reshape(outputs, [-1, self.n_hidden]), index)  # batch_size * n_hidden #???
        elif out_type == 'all_avg':
            outputs = LSTM.reduce_mean(outputs, length)
        return outputs

    def dynamic_rnn(self, cell, inputs, length, max_len, scope_name, out_type='all'):
        outputs, state = tf.nn.dynamic_rnn(
            cell(self.n_hidden),
            inputs=inputs,
            sequence_length=length,
            dtype=tf.float32,
            scope=scope_name
        )  # outputs -> batch_size * max_len * n_hidden
        batch_size = tf.shape(outputs)[0]
        if out_type == 'last':
            index = tf.range(0, batch_size) * max_len + (length - 1)
            outputs = tf.gather(tf.reshape(outputs, [-1, self.n_hidden]), index)  # batch_size * n_hidden #???
        elif out_type == 'all_avg':
            outputs = LSTM.reduce_mean(outputs, length)
        return outputs

    def bi_dynamic_rnn(self, cell, inputs, length, max_len, scope_name, out_type='all'):
        outputs, state = tf.nn.bidirectional_dynamic_rnn(
            cell_fw=cell(self.n_hidden / 2),
            cell_bw=cell(self.n_hidden / 2),
            inputs=inputs,
            sequence_length=length,
            dtype=tf.float32,
            scope=scope_name
        )
        outputs_fw, outputs_bw = outputs
        outputs_bw = tf.reverse_sequence(outputs_bw, tf.cast(length, tf.int64), seq_dim=1)
        outputs = tf.concat(2, [outputs_fw, outputs_bw])

        batch_size = tf.shape(outputs)[0]
        if out_type == 'last':
            index = tf.range(0, batch_size) * max_len + (length - 1)
            outputs = tf.gather(tf.reshape(outputs, [-1, 2 * self.n_hidden]), index)  # batch_size * 2n_hidden
        elif out_type == 'all_avg':
            outputs = LSTM.reduce_mean(outputs, length)  # batch_size * 2n_hidden
        return outputs

    def bi_dynamic_rnn_2(self, cell, inputs, length, max_len, scope_name, out_type='all'):
        outputs, state = tf.nn.bidirectional_dynamic_rnn(
            cell_fw=cell(self.n_hidden / 2),
            cell_bw=cell(self.n_hidden / 2),
            inputs=inputs,
            sequence_length=length,
            dtype=tf.float32,
            scope=scope_name
        )
        cell2 = tf.nn.rnn_cell.LSTMCell
        outputs, state = tf.nn.bidirectional_dynamic_rnn(
            cell_fw=cell2(self.n_hidden / 2),
            cell_bw=cell2(self.n_hidden / 2),
            inputs=outputs,
            sequence_length=length,
            dtype=tf.float32,
            scope=scope_name
        )


        outputs_fw, outputs_bw = outputs
        outputs_bw = tf.reverse_sequence(outputs_bw, tf.cast(length, tf.int64), seq_dim=1)
        outputs = tf.concat(2, [outputs_fw, outputs_bw])

        batch_size = tf.shape(outputs)[0]
        if out_type == 'last':
            index = tf.range(0, batch_size) * max_len + (length - 1)
            outputs = tf.gather(tf.reshape(outputs, [-1, 2 * self.n_hidden]), index)  # batch_size * 2n_hidden
        elif out_type == 'all_avg':
            outputs = LSTM.reduce_mean(outputs, length)  # batch_size * 2n_hidden
        return outputs

    def AE(self, inputs, entity,aspect, type_='last'):
        """
        :params: self.x, self.seq_len, self.weights['softmax_lstm'], self.biases['sof
        :return: non-norm prediction values
        """
        print('I am AE.')
        batch_size = tf.shape(inputs)[0]
        entity = tf.reshape(entity, [-1, 1, self.embedding_dim])
        aspect = tf.reshape(aspect, [-1, 1, self.embedding_dim])
        entity = tf.ones([batch_size, self.max_sentence_len, self.embedding_dim], dtype=tf.float32) * entity
        aspect = tf.ones([batch_size, self.max_sentence_len, self.embedding_dim], dtype=tf.float32) * aspect
        inputs = tf.concat(2, [inputs, entity, aspect])
        inputs = tf.nn.dropout(inputs, keep_prob=self.keep_prob1)

        cell = tf.nn.rnn_cell.LSTMCell
        outputs = self.dynamic_rnn(cell, inputs, self.sen_len, self.max_sentence_len, 'AE', type_)

        return LSTM.softmax_layer(outputs, self.weights['softmax'], self.biases['softmax'], self.keep_prob2)

    def AT(self, inputs, entity, aspect, type_='last'):
        print('I am AT.')
        batch_size = tf.shape(inputs)[0]
        entity0 = tf.reshape(entity, [-1, 1, self.embedding_dim])
        entity = tf.ones([batch_size, self.max_sentence_len, self.embedding_dim], dtype=tf.float32) * entity0
        aspect0 = tf.reshape(aspect, [-1, 1, self.embedding_dim])
        aspect = tf.ones([batch_size, self.max_sentence_len, self.embedding_dim], dtype=tf.float32) * aspect0
        in_t = tf.concat(2, [inputs, entity, aspect])

        in_t = tf.nn.dropout(in_t, keep_prob=self.keep_prob1)
        cell = tf.nn.rnn_cell.LSTMCell
        hiddens = self.dynamic_rnn(cell, in_t, self.sen_len, self.max_sentence_len, 'AT', "all")

        h_t = tf.reshape(tf.concat(2, [hiddens, entity, aspect]), [-1, self.n_hidden + 2*self.embedding_dim])

        M = tf.matmul(tf.tanh(tf.matmul(h_t, self.W)), self.w)
        alpha = LSTM.softmax(tf.reshape(M, [-1, 1, self.max_sentence_len]), self.sen_len, self.max_sentence_len)
        # alpha.shape = (batch_size, 1, max_sentence_len)

        self.alpha = tf.reshape(alpha, [-1, self.max_sentence_len])

        r = tf.reshape(tf.batch_matmul(alpha, hiddens), [-1, self.n_hidden])
        index = tf.range(0, batch_size) * self.max_sentence_len + (self.sen_len - 1)
        hn = tf.gather(tf.reshape(hiddens, [-1, self.n_hidden]), index)  # batch_size * n_hidden

        h = tf.tanh(tf.matmul(r, self.Wp) + tf.matmul(hn, self.Wx))

        return LSTM.softmax_layer(h, self.weights['softmax'], self.biases['softmax'], self.keep_prob2)


    # MAT for tensorflow 1.3r
    def MAT(self, inputs, entity, aspect, type_='last'):
       print('I am MAT.')
       batch_size = tf.shape(inputs)[0]
       entity0 = tf.reshape(entity, [-1, 1, self.embedding_dim])
       aspect0 = tf.reshape(aspect, [-1, 1, self.embedding_dim])
       entity = inputs * entity0
       aspect = inputs * aspect0
       in_t = tf.concat([inputs, entity, aspect],2)

       in_t = tf.nn.dropout(in_t, keep_prob=self.keep_prob1)
       cell = tf.nn.rnn_cell.LSTMCell
       hiddens = self.dynamic_rnn(cell, in_t, self.sen_len, self.max_sentence_len, 'AT', "all")
       entity = hiddens * entity0
       aspect = hiddens * aspect0
       h_t = tf.reshape(tf.concat([hiddens, entity, aspect],2), [-1, self.n_hidden + 2*self.embedding_dim])

       M = tf.matmul(tf.tanh(tf.matmul(h_t, self.W)), self.w)
       alpha = LSTM.softmax(tf.reshape(M, [-1, 1, self.max_sentence_len]), self.sen_len, self.max_sentence_len)
       # alpha.shape = (batch_size, 1, max_sentence_len)

       self.alpha = tf.reshape(alpha, [-1, self.max_sentence_len])

       r = tf.reshape(tf.matmul(alpha, hiddens), [-1, self.n_hidden])
       index = tf.range(0, batch_size) * self.max_sentence_len + (self.sen_len - 1)
       hn = tf.gather(tf.reshape(hiddens, [-1, self.n_hidden]), index)  # batch_size * n_hidden

       h = tf.tanh(tf.matmul(r, self.Wp) + tf.matmul(hn, self.Wx))

       return LSTM.softmax_layer(h, self.weights['softmax'], self.biases['softmax'], self.keep_prob2)

    # # previous version for tensorflow r0.12
    # def MAT(self, inputs, entity, aspect, type_='last'):
    #     print('I am MAT.')
    #     batch_size = tf.shape(inputs)[0]
    #     entity0 = tf.reshape(entity, [-1, 1, self.embedding_dim])
    #     aspect0 = tf.reshape(aspect, [-1, 1, self.embedding_dim])
    #     entity = inputs * entity0
    #     aspect = inputs * aspect0
    #     in_t = tf.concat(2, [inputs, entity, aspect])
    #
    #     in_t = tf.nn.dropout(in_t, keep_prob=self.keep_prob1)
    #     cell = tf.nn.rnn_cell.LSTMCell
    #     hiddens = self.dynamic_rnn(cell, in_t, self.sen_len, self.max_sentence_len, 'AT', "all")
    #     entity = hiddens * entity0
    #     aspect = hiddens * aspect0
    #     h_t = tf.reshape(tf.concat(2, [hiddens, entity, aspect]), [-1, self.n_hidden + 2*self.embedding_dim])
    #
    #     M = tf.matmul(tf.tanh(tf.matmul(h_t, self.W)), self.w)
    #     alpha = LSTM.softmax(tf.reshape(M, [-1, 1, self.max_sentence_len]), self.sen_len, self.max_sentence_len)
    #     # alpha.shape = (batch_size, 1, max_sentence_len)
    #
    #     self.alpha = tf.reshape(alpha, [-1, self.max_sentence_len])
    #
    #     r = tf.reshape(tf.batch_matmul(alpha, hiddens), [-1, self.n_hidden])
    #     index = tf.range(0, batch_size) * self.max_sentence_len + (self.sen_len - 1)
    #     hn = tf.gather(tf.reshape(hiddens, [-1, self.n_hidden]), index)  # batch_size * n_hidden
    #
    #     h = tf.tanh(tf.matmul(r, self.Wp) + tf.matmul(hn, self.Wx))
    #
    #     return LSTM.softmax_layer(h, self.weights['softmax'], self.biases['softmax'], self.keep_prob2)

    def TAT(self, inputs, entity, aspect, type_='last'):
        print('I am TAT.')
        batch_size = tf.shape(inputs)[0]

        entity0 = tf.reshape(entity, [-1, self.embedding_dim, 1])
        aspect0 = tf.reshape(aspect, [-1, self.embedding_dim, 1])

        entity = tf.reshape(entity, [-1, 1, self.embedding_dim])
        entity = tf.ones([batch_size, self.max_sentence_len, self.embedding_dim], dtype=tf.float32) * entity
        aspect = tf.reshape(aspect, [-1, 1, self.embedding_dim])
        aspect = tf.ones([batch_size, self.max_sentence_len, self.embedding_dim], dtype=tf.float32) * aspect
        in_t = tf.concat(2, [inputs, entity, aspect])

        in_t = tf.nn.dropout(in_t, keep_prob=self.keep_prob1)
        cell = tf.nn.rnn_cell.LSTMCell
        hiddens = self.dynamic_rnn(cell, in_t, self.sen_len, self.max_sentence_len, 'AT', "all")
        # hiddens.shape: batch_size *max_sentence_len* n_hidden

        hiddens = tf.reshape(hiddens, [batch_size, self.max_sentence_len, self.n_hidden])

        attentionorder = 2.1
        if attentionorder == 2.1:
            # w*(h*W*e) + w*(h*W*a)
            # self.Wetat [self.n_hidden, self.embedding_dim]
            # [batch_size, self.max_sentence_len, self.n_hidden] * [self.n_hidden, self.embedding_dim] * [batch_size, self.embedding_dim, 1]
            scoreentity = tf.matmul(tf.reshape(hiddens,[-1,self.n_hidden]),self.Wetat)
            scoreentity = tf.matmul(tf.reshape(scoreentity,[-1,self.max_sentence_len,self.embedding_dim]),entity0)
            scoreaspect = tf.matmul(tf.reshape(hiddens, [-1, self.n_hidden]), self.Watat)
            scoreaspect = tf.matmul(tf.reshape(scoreaspect, [-1, self.max_sentence_len, self.embedding_dim]), aspect0)
            # scoreentity = tf.reshape(tf.matmul(tf.einsum('ijk,km->ijm', hiddens, tf.reshape(self.Wetat,[self.n_hidden,self.embedding_dim])), entity0),
            #                          [-1, self.max_sentence_len])
            # scoreaspect = tf.reshape(tf.matmul(tf.einsum('ijk,km->ijm', hiddens, self.Watat), aspect0),
            #                          [-1, self.max_sentence_len])
            score = tf.tanh(tf.matmul(tf.reshape(scoreentity,[-1,self.max_sentence_len]), self.We) + tf.matmul(tf.reshape(scoreaspect,[-1,self.max_sentence_len]), self.Wa))
        elif attentionorder==2:
            # W*(h*e) + W*(h*a) 要求n_hidden和embedding_dim一致
            scoreentity = tf.reshape(tf.matmul(hiddens, entity0), [-1, self.max_sentence_len])
            scoreaspect = tf.reshape(tf.matmul(hiddens, aspect0), [-1, self.max_sentence_len])
            score = tf.tanh(tf.matmul(scoreentity, self.We) + tf.matmul(scoreaspect, self.Wa))
        elif attentionorder==3:
            ea = tf.reshape(entity0,[-1,self.embedding_dim]) * tf.reshape(aspect0,[-1,self.embedding_dim])
            eaw = tf.reshape(tf.matmul(ea, self.Wea),[-1,self.n_hidden,1])
            score = tf.matmul(hiddens,eaw)

        alpha = LSTM.softmax(tf.reshape(score, [-1, 1, self.max_sentence_len]), self.sen_len, self.max_sentence_len)
        # alpha.shape = (batch_size, 1, max_sentence_len)

        self.alpha = tf.reshape(alpha, [-1, self.max_sentence_len])
        # score = tf.matmul(hiddens, entity0)+tf.matmul(hiddens, aspect0))
        # score.shape = batch_size * max_sentence_len * 1
        #self.alpha = tf.reshape(score, [-1, self.max_sentence_len])
        score = tf.reshape(score, [-1, 1, self.max_sentence_len])

        r = tf.reshape(tf.batch_matmul(score, hiddens), [-1, self.n_hidden])
        index = tf.range(0, batch_size) * self.max_sentence_len + (self.sen_len - 1)
        hn = tf.gather(tf.reshape(hiddens, [-1, self.n_hidden]), index)  # batch_size * n_hidden

        h = tf.tanh(tf.matmul(r, self.Wp) + tf.matmul(hn, self.Wx))

        return LSTM.softmax_layer(h, self.weights['softmax'], self.biases['softmax'], self.keep_prob2)

    def DMN(self, inputs, entity, aspect, hopnum=5):
        print('I am DMN.')
        batch_size = tf.shape(inputs)[0]

        inputs = tf.nn.dropout(inputs, keep_prob=self.keep_prob1)
        for i in range(hopnum):
            entity0 = tf.reshape(entity, [-1, 1, self.embedding_dim])
            aspect0 = tf.reshape(aspect, [-1, 1, self.embedding_dim])
            entity1 = inputs * entity0
            aspect1 = inputs * aspect0
            h_t = tf.reshape(tf.concat(2, [inputs, entity1, aspect1]), [-1, self.n_hidden + 2 * self.embedding_dim])

            M = tf.matmul(tf.tanh(tf.matmul(h_t, self.W)), self.w)
            alpha = LSTM.softmax(tf.reshape(M, [-1, 1, self.max_sentence_len]), self.sen_len, self.max_sentence_len)
            # alpha.shape = (batch_size, 1, max_sentence_len)

            self.alpha = tf.reshape(alpha, [-1, self.max_sentence_len])

            r = tf.reshape(tf.batch_matmul(alpha, inputs), [-1, self.n_hidden])
            entity = tf.matmul(r,self.Wdmnr) + tf.matmul(entity,self.Wdmne)
            aspect = tf.matmul(r,self.Wdmnr) + tf.matmul(aspect,self.Wdmna)
        # h = tf.matmul(entity, self.Wdmn_e) + tf.matmul(aspect, self.Wdmn_a)
        h = tf.matmul(tf.reshape(tf.concat(1, [entity, aspect]), [-1, self.n_hidden * 2]), self.Wdmn)
        b = tf.ones([batch_size, self.n_hidden]) * self.Bdmn
        h = h + b
        return LSTM.softmax_layer(h, self.weights['softmax'], self.biases['softmax'], self.keep_prob2)

    def MATDMN(self, inputs, entity, aspect, hopnum=10):
        print('I am MATDMN.')
        batch_size = tf.shape(inputs)[0]
        entity0 = tf.reshape(entity, [-1, 1, self.embedding_dim])
        aspect0 = tf.reshape(aspect, [-1, 1, self.embedding_dim])
        entity1 = inputs * entity0
        aspect1 = inputs * aspect0
        in_t = tf.concat(2, [inputs, entity1, aspect1])

        in_t = tf.nn.dropout(in_t, keep_prob=self.keep_prob1)
        cell = tf.nn.rnn_cell.LSTMCell
        hiddens = self.dynamic_rnn(cell, in_t, self.sen_len, self.max_sentence_len, 'AT', "all")

        for i in range(hopnum):
            entity0 = tf.reshape(entity, [-1, 1, self.embedding_dim])
            aspect0 = tf.reshape(aspect, [-1, 1, self.embedding_dim])
            entity1 = hiddens * entity0
            aspect1 = hiddens * aspect0
            h_t = tf.reshape(tf.concat(2, [hiddens, entity1, aspect1]), [-1, self.n_hidden + 2 * self.embedding_dim])

            M = tf.matmul(tf.tanh(tf.matmul(h_t, self.W)), self.w)
            b = tf.ones([batch_size,self.max_sentence_len]) * self.b
            M = tf.reshape(M, [-1, self.max_sentence_len])
            M = M + b
            M = tf.reshape(M, [-1, 1, self.max_sentence_len])
            alpha = LSTM.softmax(M, self.sen_len, self.max_sentence_len)
            # alpha.shape = (batch_size, 1, max_sentence_len)

            self.alpha = tf.reshape(alpha, [-1, self.max_sentence_len])

            r = tf.reshape(tf.batch_matmul(alpha, hiddens), [-1, self.n_hidden])
            entity = tf.matmul(r,self.Wdmnr) + tf.matmul(entity,self.Wdmne)
            aspect = tf.matmul(r,self.Wdmnr) + tf.matmul(aspect,self.Wdmna)
        # h = tf.matmul(entity, self.Wdmn_e) + tf.matmul(aspect, self.Wdmn_a)

        h = tf.matmul(tf.reshape(tf.concat(1, [entity,aspect]),[-1,self.n_hidden*2]),self.Wdmn)
        b = tf.ones([batch_size,self.n_hidden])*self.Bdmn
        h = h + b
        return LSTM.softmax_layer(h, self.weights['softmax'], self.biases['softmax'], self.keep_prob2)

    @staticmethod
    def softmax_layer(inputs, weights, biases, keep_prob):
        with tf.name_scope('softmax'):
            outputs = tf.nn.dropout(inputs, keep_prob=keep_prob)
            predict = tf.matmul(outputs, weights) + biases
            predict = tf.nn.softmax(predict)
        return predict

    @staticmethod
    def reduce_mean(inputs, length):
        """
        :param inputs: 3-D tensor
        :param length: the length of dim [1]
        :return: 2-D tensor
        """
        length = tf.cast(tf.reshape(length, [-1, 1]), tf.float32) + 1e-9
        inputs = tf.reduce_sum(inputs, 1, keep_dims=False) / length
        return inputs

    @staticmethod
    def softmax(inputs, length, max_length):
        inputs = tf.cast(inputs, tf.float32)
        max_axis = tf.reduce_max(inputs, 2, keep_dims=True)
        inputs = tf.exp(inputs - max_axis)
        length = tf.reshape(length, [-1])
        mask = tf.reshape(tf.cast(tf.sequence_mask(length, max_length), tf.float32), tf.shape(inputs))
        inputs *= mask
        _sum = tf.reduce_sum(inputs, reduction_indices=2, keep_dims=True) + 1e-9
        return inputs / _sum

    def run(self, PreEmbedding, word2id, p2id, p2v, ):
        import time
        atime=time.time()
        inputs = tf.nn.embedding_lookup(self.Embed, self.x)
        # pos_inputs = tf.nn.embedding_lookup(self.pos_Embed, self.pos_x)
        # print(tf.shape(inputs),tf.shape(self.pos_x))
        # inputs = tf.concat(2, [inputs, pos_inputs])
        entity = tf.nn.embedding_lookup(self.Embed, self.entity_id)
        aspect = tf.nn.embedding_lookup(self.Embed, self.aspect_id)
        if FLAGS.method == 'AE':
            prob = self.AE(inputs, aspect, FLAGS.t) # 需增加entity
        elif FLAGS.method == 'AT':
            prob = self.AT(inputs, entity, aspect, FLAGS.t)
        elif FLAGS.method == "TAT":
            prob = self.TAT(inputs, entity, aspect, FLAGS.t)
        elif FLAGS.method == "MAT":
            prob = self.MAT(inputs, entity, aspect, FLAGS.t)
        elif FLAGS.method == "DMN":
            prob = self.DMN(inputs, entity, aspect, hopnum = 10)
        elif FLAGS.method == "MATDMN":
            prob = self.MATDMN(inputs, entity, aspect, hopnum = 10)

        with tf.name_scope('loss'):
            reg_loss = tf.get_collection(tf.GraphKeys.REGULARIZATION_LOSSES)
            cost = - tf.reduce_mean(tf.cast(self.y, tf.float32) * tf.log(prob)) + sum(reg_loss)

        with tf.name_scope('train'):
            global_step = tf.Variable(0, name="tr_global_step", trainable=False)
            optimizer = tf.train.AdamOptimizer(learning_rate=self.learning_rate).minimize(cost, global_step=global_step)

        with tf.name_scope('predict'):
            correct_pred = tf.equal(tf.argmax(prob, 1), tf.argmax(self.y, 1))
            true_y = tf.argmax(self.y, 1)
            pred_y = tf.argmax(prob, 1)
            accuracy = tf.reduce_sum(tf.cast(correct_pred, tf.int32))
            _acc = tf.reduce_mean(tf.cast(correct_pred, tf.float32))

        with tf.Session() as sess:

            saver = tf.train.Saver(write_version=tf.train.SaverDef.V2)

            init = tf.initialize_all_variables()
            sess.run(init)
            import os


            dt_x, dt_sen_len, dt_entity, dt_aspect, dt_y, dt_yvalue, idlist, dt_pos = load_inputs_data_at(
                FLAGS.dataset,
                word2id,
                self.max_sentence_len,
                p2id,
            )

            if self.needdev:
                """ shuffle the train set and split the train set into train and dev sets"""
                sss = StratifiedShuffleSplit(dt_yvalue, 1, test_size=0.2, random_state=0)
                print('len of sss',len(sss))
                for train_index, test_index in sss:
                    print("TRAIN:", len(train_index), "TEST:", len(test_index))
                    tr_x = dt_x[train_index]
                    te_x = dt_x[test_index]
                    tr_y = dt_y[train_index]
                    te_y = dt_y[test_index]
                    tr_sen_len = dt_sen_len[train_index]
                    te_sen_len = dt_sen_len[test_index]
                    tr_entity = dt_entity[train_index]
                    te_entity = dt_entity[test_index]
                    tr_aspect = dt_aspect[train_index]
                    te_aspect = dt_aspect[test_index]
                    tr_id = [idlist[x] for x in train_index]
                    te_id = [idlist[x] for x in test_index]
                    tr_pos = dt_pos[train_index]
                    te_pos = dt_pos[test_index]

                    ftrain = open('data/babycare/train.txt','w',encoding='utf8')
                    ftrain.write('\n'.join(tr_id))
                    ftrain.close()

                    ftrain = open('data/babycare/test.txt', 'w', encoding='utf8')
                    ftrain.write('\n'.join(te_id))
                    ftrain.close()

            else:
                tr_x, tr_y, tr_sen_len, tr_entity, tr_aspect, tr_pos = dt_x, dt_y, dt_sen_len, dt_entity, dt_aspect, dt_pos
                te_x, te_sen_len, te_entity, te_aspect, te_y, te_yvalue, te_idlist, te_pos = load_inputs_data_at(
                    FLAGS.testset,
                    word2id,
                    self.max_sentence_len,
                    p2id,
                )

            del dt_x
            del dt_y
            del dt_sen_len
            del dt_entity
            del dt_aspect
            del idlist
            del dt_pos

            prtstr = "Configs:{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n".format(self.batch_size, self.n_hidden,
                                                                           self.learning_rate,
                                                                           self.n_class, self.l2_reg, self.n_iter,
                                                                           keep_prob1_init, keep_prob2_init, method)
            prtstr2 = "Configs:{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n".format(self.batch_size, self.n_hidden,
                                                                            self.learning_rate,
                                                                            self.n_class, self.l2_reg, self.n_iter,
                                                                            keep_prob1_init, keep_prob2_init, method)

            # 初始化词向量
            sess.run([self.embedding_init,self.pos_embedding_init], feed_dict={self.embedding_placeholder: PreEmbedding,self.pos_embedding_placeholder:p2v})

            # save.filename = 'ME-ABSA0704'+FLAGS.method
            savefilename = 'ME-ABSA0904-MILKMAT' + self.category + FLAGS.method

            if self.restore:
                saver.restore(sess, './models/'+self.category+'/'+savefilename)
                acc, loss, cnt = 0., 0., 0
                ftestpr = open('devpred0619.txt','w')
                for test, num in self.get_batch_data(te_x, te_pos, te_sen_len, te_y, te_entity, te_aspect,
                                                     self.batch_size, 1.0, 1.0, False):
                    _loss, _acc, _step, alpha, ty, py = sess.run(
                        [cost, accuracy, global_step, self.alpha, true_y, pred_y],
                        feed_dict=test)

                    acc += _acc
                    loss += _loss * num
                    cnt += num
                    ftestpr.write('\n'.join([str(pyyy) for pyyy in py]) + '\n')
                ftestpr.close()
                print('Category: ', FLAGS.category)
                print(acc/cnt)
            else:
                max_acc = 0.

                batch_count = 0
                for i in range(self.n_iter):
                    for train, _ in self.get_batch_data(tr_x, tr_pos, tr_sen_len, tr_y, tr_entity, tr_aspect, self.batch_size, FLAGS.keep_prob1, FLAGS.keep_prob2):
                        _, step = sess.run([optimizer, global_step], feed_dict=train)
                        batch_count += 1
                        if batch_count%100==0:
                            print(batch_count * 100.0 * self.batch_size / len(tr_y) / self.n_iter,'% at iter',i)

                    acc, loss, cnt = 0., 0., 0
                    flag = True

                    self.test= True
                    if self.test:
                        # ftestpr = open('devpred0619.txt','w')
                        for test, num in self.get_batch_data(te_x,te_pos, te_sen_len, te_y, te_entity, te_aspect, self.batch_size, 1.0, 1.0, False):
                            _loss, _acc, _step, alpha, ty, py = sess.run([cost, accuracy, global_step, self.alpha, true_y, pred_y],
                                                                    feed_dict=test)

                            acc += _acc
                            loss += _loss * num
                            cnt += num
                            if flag:
                                flag = False
                        self.pY = py
                            # ftestpr.write('\n'.join([str(pyyy) for pyyy in py]) + '\n')

                        # ftestpr.close()

                        print('all samples={}, correct prediction={}'.format(cnt, acc))
                        print('Iter {}: mini-batch loss={:.6f}, test acc={:.6f}'.format(i, loss / cnt, acc / cnt))
                        prtstr += "{}\t{}\t{}\n".format(i, loss / cnt, acc / cnt)

                        if acc / cnt > max_acc:
                            max_acc = acc / cnt
                            saver.save(sess,  './models/'+self.category+'/'+savefilename)
                            ftestpr = open('devpred0619.txt', 'w')
                            ftestpr.write('\n'.join([str(pyyy) for pyyy in self.pY]) + '\n')
                            ftestpr.close()
                        if i==self.n_iter-1:
                            btime = time.time()
                            prtstr2 += '{}\t{}\n'.format(acc / cnt, btime - atime)

                    if self.showtrainerr:
                        acc, loss, cnt = 0., 0., 0
                        for test, num in self.get_batch_data(tr_x, tr_pos, tr_sen_len, tr_y, tr_entity, tr_aspect, self.batch_size, 1, 1,is_shuffle=False):
                            _loss, _acc= sess.run([cost, accuracy],feed_dict=test)
                            acc += _acc
                            loss += _loss * num
                            cnt += num
                            if flag:
                                flag = False
                        print('all samples={}, correct prediction in train={}'.format(cnt, acc))
                        print('Iter {}: mini-batch loss={:.6f}, train acc={:.6f}'.format(i, loss / cnt, acc / cnt))

                        prtstr += "{}\t{}\t{}\n".format(i, loss / cnt, acc / cnt)

                print('Optimization Finished! Max acc={}'.format(max_acc))
                prtstr += '\n'
                # saver.save(sess, './models/'+savefilename)

                if FLAGS.debugmode=="1":
                    f = open('result.txt', 'a')
                    f.write(prtstr)
                    f.close()
                    f = open('result-short.txt', 'a')
                    f.write(prtstr2)
                    f.close()

            def testfiles(rootdir):
                frpr = open('{}test-pred.txt'.format('./predict/'+FLAGS.date + '/output/'+ FLAGS.category), 'w',encoding='utf8')

                # datafilepath = 'data/babycare/compare_new.txt'

                # ftestfile = open(datafilepath, 'r', encoding='utf8')
                # testlines = ftestfile.readlines()
                # ftestfile.close()

                for parent, dirnames, filenames in os.walk(rootdir):
                    for filename in filenames:

                        testfilepath = os.path.join(parent, filename)
                        pys = []
                        print("filename with full path:" + testfilepath)
                        dt_x, dt_sen_len, dt_entity, dt_aspect,dt_y, _, idlist ,pos = load_inputs_data_at_test(
                            testfilepath,
                            word2id,
                            self.max_sentence_len,
                            case='test'
                        )
                        print('test data len {}'.format(len(dt_y)))
                        for _test, num in self.get_batch_data(dt_x,pos, dt_sen_len, dt_y, dt_entity, dt_aspect, 1000, 1, 1,is_shuffle=False):
                            _loss, _acc, _step, alpha, ty, py = sess.run(
                                [cost, accuracy, global_step, self.alpha, true_y, pred_y],
                                feed_dict=_test)
                            [pys.append(str(pyyy)) for pyyy in py]

                        print('len of prediction',len(pys))
                        indexdata = 0
                        for i in range(len(pys)):
                            frpr.write(idlist[indexdata] + '\t' + str(pys[i]) + '\n')
                            indexdata+=1
                frpr.close()
            testfiles(FLAGS.rootdir)#split_milk_0904


    def get_batch_data(self, x, pos, sen_len, y, entity, aspect, batch_size, keep_prob1, keep_prob2, is_shuffle=True):
        for index in batch_index(len(y), batch_size, 1, is_shuffle):
            feed_dict = {
                self.x: x[index],
                self.pos_x: pos[index],
                self.y: y[index],
                self.sen_len: sen_len[index],
                self.entity_id: entity[index],
                self.aspect_id: aspect[index],
                self.keep_prob1: keep_prob1,
                self.keep_prob2: keep_prob2,
            }
            yield feed_dict, len(index)

def main(_):
    word_dict, w2v, p2id, p2v, maxlen = load_data_init(FLAGS.dataset, FLAGS.testset, FLAGS.embedding_file_path, FLAGS.embedding_dim)
    lstm = LSTM(
        len(word_dict),
        embedding_dim=FLAGS.embedding_dim,
        pos_embedding_dim = len(p2v),
        batch_size=FLAGS.batch_size,
        n_hidden=FLAGS.n_hidden,
        learning_rate=FLAGS.learning_rate,
        n_class=FLAGS.n_class,
        max_sentence_len=maxlen,#max_len,
        l2_reg=FLAGS.l2_reg,
        display_step=FLAGS.display_step,
        n_iter=FLAGS.n_iter,
        type_=FLAGS.method,
        category=FLAGS.category,
        rootdir=FLAGS.rootdir,
        date=FLAGS.date

    )
    lstm.run(PreEmbedding=w2v,word2id=word_dict,p2id=p2id,p2v=p2v)


if __name__ == '__main__':
    FLAGS = tf.app.flags.FLAGS

    tf.app.flags.DEFINE_string('debugmode', '1', 'is debug mode: ')

    tf.app.flags.DEFINE_integer('embedding_dim', 300, 'dimension of word embedding')
    tf.app.flags.DEFINE_integer('batch_size', batch_size_init, 'number of example per batch')
    tf.app.flags.DEFINE_integer('n_hidden', n_hidden_init, 'number of hidden unit')
    tf.app.flags.DEFINE_float('learning_rate', learning_rate_init, 'learning rate')
    tf.app.flags.DEFINE_integer('n_class', n_class_init, 'number of distinct class')
    tf.app.flags.DEFINE_float('l2_reg', l2_reg_init, 'l2 regularization')
    tf.app.flags.DEFINE_integer('display_step', 4, 'number of test display step')
    tf.app.flags.DEFINE_integer('n_iter', n_iter_init, 'number of train iter')
    tf.app.flags.DEFINE_float('keep_prob1', keep_prob1_init, 'dropout keep prob')
    tf.app.flags.DEFINE_float('keep_prob2', keep_prob2_init, 'dropout keep prob')

    tf.app.flags.DEFINE_string('dataset', 'data/baojianpin/baojianpin_to_train.txt', 'training file')
    tf.app.flags.DEFINE_string('testset', 'data/baojianpin/baojianpin_to_test.txt', 'testing file')
    tf.app.flags.DEFINE_string('embedding_file_path', 'data/baojianpin/general2-300/baojianpin_reduced_vectors.txt',
                               'embedding file')
    tf.app.flags.DEFINE_string('entity_id_file_path', 'data/babycare/entity_id.txt', 'entity-id mapping file')
    tf.app.flags.DEFINE_string('aspect_id_file_path', 'data/babycare/aspect_id.txt', 'aspect-id mapping file')
    tf.app.flags.DEFINE_string('method', method.split('-')[0], 'model type: AE, AT or AEAT')
    tf.app.flags.DEFINE_string('t', 'last', 'model type: ')
    tf.app.flags.DEFINE_string('category', 'baojianpin', 'applied feild')
    tf.app.flags.DEFINE_string('rootdir', 'predict/baojianpin/1212/topred/', 'predict files fold')
    tf.app.flags.DEFINE_string('date', '0000', 'date folder')

    tf.app.run()



# ME-ABSA0614-2 可用模型，3类分类
