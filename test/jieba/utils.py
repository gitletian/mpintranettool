#!/usr/bin/env python
# encoding: utf-8
# @author: yangjun
# email: yangjunny@126.com


import numpy as np
import os


printmode=False

def batch_index(length, batch_size, n_iter=100, is_shuffle=True):
    index = np.arange(length)
    for j in range(n_iter):
        if is_shuffle:
            np.random.shuffle(index)
        for i in range(int(length / batch_size) + (1 if length % batch_size else 0)):
            yield index[i * batch_size:(i + 1) * batch_size]

def load_w2v(w2v_file, embedding_dim, is_skip=False):
    '''
        Loads and returns a word2id dictionary and a word embedding
    
        Args:
            w2v_file: w2v filename
            embedding_dim: dimention of word vectors
            is_skip: is there a headline to skip
    
        Returns:
            word_dict: a dictionary whose key is a word and value is the id of the word
            w2v: a list of vectors for each word
    '''
    # fp = open(w2v_file,encoding='utf8')
    # if is_skip:
    #     fp.readline()
    # w2v = []
    # word_dict = dict()
    # cnt = -1
    # for line in fp:
    #     cnt += 1
    #     line = line.split()
    #     if len(line) != embedding_dim + 1:
    #         print('a bad word embedding: {} at {}'.format(line[0],cnt))
    #         continue
    #     w2v.append([float(v) for v in line[1:]])
    #     word_dict[line[0]] = cnt
    # print('oringin length of word_dict:', len(word_dict), ',length of w2v', len(w2v))
    # if (len(word_dict)!=len(w2v)):
    #     raise Exception('w2v error')
    # return word_dict, w2v
    ###临时改###
    fp = open(w2v_file, encoding='utf8')
    wordset = set()
    w2v = []
    word_dict = dict()
    cnt = -1
    for line in fp:
        line = line.split()
        if len(line) != embedding_dim + 1:
            print('a bad word embedding: {} at {}'.format(line[0], cnt))
            continue
        if line[0] not in wordset:
            cnt += 1
            wordset.add(line[0])
            w2v.append([float(v) for v in line[1:]])
            word_dict[line[0]] = cnt
    print('oringin length of word_dict:', len(word_dict), ',length of w2v', len(w2v))
    if (len(word_dict) != len(w2v)):
        raise Exception('w2v error')
    return word_dict, w2v


def load_tag_embedding(tag_file, word_dict, w2v, embedding_dim):
    '''
        entity aspect embeddings are initialized here.

        Args:
            tag_file: entity file or aspect file
            word_dict: a dictionary whose key is a word and value is the id of the word
            w2v: a list of vectors for each word
            embedding_dim: dimention of word vectors

        Returns:
            word_dict: a dictionary whose key is a word and value is the id of the word
            w2v: a list of vectors for each word
    '''
    for line in open(tag_file, encoding='utf8'):
        line = line.lower().rstrip('\n')
        if line in word_dict or line.replace(' ','') in word_dict:
            continue
        else:
            info = line.split()
            tmp = []
            for word in info:
                if word in word_dict:
                    tmp.append(w2v[word_dict[word]])
                else:
                    if printmode:
                        print('$WARNING LEVEL-1$: {} not in dic'.format(word))
            if tmp:
                word_dict[line.replace(' ','')]=len(word_dict)+1#【后患】！！！！！！！！！！
                w2v.append(np.sum(tmp, axis=0) / len(tmp))
            else:  # no pre-trained entity/aspect embedding
                word_dict[line] = len(word_dict) + 1
                w2v.append(np.random.uniform(-0.01, 0.01, (embedding_dim,)))
                if printmode:
                    print('$WARNING LEVEL-2$: {} not in dic'.format(line))
    return word_dict, w2v

# 此函数加载词-编号映射表和词向量，并且把aspect词考虑进入。
def load_word_embedding(w2v_file, entity_id_file, aspect_id_file, embedding_dim, is_skip=False):
    '''
        Loads and returns a word2id dictionary and a word embedding;
        entity aspect embeddings are initialized here.

        Args:
            w2v_file: w2v filename
            entity_id_file: entity list
            aspect_id_file: aspect list
            embedding_dim: dimention of word vectors
            is_skip: is there a headline to skip

        Returns:
            word_dict: a dictionary whose key is a word and value is the id of the word
            w2v: a list of vectors for each word
    '''

    word_dict, w2v = load_w2v(w2v_file, embedding_dim, is_skip)
    print("w2v",len(word_dict))
    #word_dict, w2v = load_tag_embedding(entity_id_file, word_dict, w2v, embedding_dim)
    #print("w2v+entity_id_file", len(word_dict))
    #word_dict, w2v = load_tag_embedding(aspect_id_file, word_dict, w2v, embedding_dim)
    #print("w2v+aspect_id_file", len(word_dict))

    w2v = np.asarray(w2v, dtype=np.float32)
    print('Shape of PreEmbedding is',w2v.shape)
    print('modified length of word_dict:', len(word_dict), ',length of w2v', len(w2v))
    return word_dict, w2v


def change_y_to_onehot(y):
    from collections import Counter
    print(Counter(y))
    y_onehot_mapping = {}
    y_onehot_mapping['-1'] = 0
    y_onehot_mapping['0'] = 1
    y_onehot_mapping['1'] = 2
    n_class = 3

    onehot = []
    for label in y:
        tmp = [0] * n_class
        tmp[y_onehot_mapping[label]] = 1
        onehot.append(tmp)
    # for label in y_onehot_mapping:
    #     print('origin{} new{}'.format(label,y_onehot_mapping[label]))
    return np.asarray(onehot, dtype=np.int32)

def get_max_sentence_len(input_file):
    '''
    get max sentence length of input_file.
    :param input_file: file path
    :return: maxlen: max sentence length
              posset: POS tag set
    '''
    maxlen=0
    posset = set()
    lines = open(input_file, encoding='utf8').readlines()
    for i in range(0, len(lines)):
        id, beizhu, text, entity, aspect, senti = lines[i].rstrip().split('\t') #
        if senti!='0' and senti!='1' and senti!='-1':
            continue
        text = text.strip("\"")
        length=len(text.split())
        if length>maxlen:
            maxlen=length
        for x in text.split():
            xsp = x.split('|')
            if len(xsp)!=2:
                print(lines[i],xsp)
                raise Exception('None POS error')
            posset.add(xsp[1])
    return maxlen, posset

# 返回数据的向量表示，切分条数后可直接用于训练，返回值中的aspect_words是词的序号。
def load_inputs_data_at(dataset, word_to_id, max_sentence_len, p2id, encoding='utf8', case = 'train'):

    x, y, sen_len = [], [], []
    entity_words = []
    aspect_words = []
    poslist = []
    idlist = []
    position_entity = []
    position_aspect = []
    
    lines = open(dataset,encoding=encoding).readlines()

    for i in range(0, len(lines)):
        id, beizhu, text, entity, aspect, senti = lines[i].rstrip().split('\t')#, pst_entity, pst_aspect
        text = text.strip("\"")
        words = text.lower().split()
        if len(words)>max_sentence_len:
            if case == 'train':
                continue
            else:
                words = words[0:max_sentence_len]

        if senti!='0' and senti!='1' and senti!='-1':# and case=='train':
            continue
        noe_oov_words =0  #加入不是oov的词数量——mw
        ids = []
        poss = []
        for wordpos in words:
            word, pos = wordpos.split('|')
            if word in word_to_id:
                noe_oov_words += 1 #添加——mw
                ids.append(word_to_id[word])
                poss.append(p2id[pos])

            # 【没有else】OOV的词就跳过了
        # ids = list(map(lambda word: word_to_id.get(word, 0), words))
        # if(len(ids)<5):
        #     continue
        sen_len.append(len(ids))
        # if entity not in word_to_id:
        #     print('$WARNING LEVEL-2$:[', entity,'] not exist in word list!!!!!!!')
        # 如果没有就默认补充第0个词
        entity_words.append(word_to_id.get(entity, 0))

        # if aspect not in word_to_id:
        #     print('$WARNING LEVEL-2$:[', aspect,'] not exist in word list!!!!!!!')
        # 如果没有就默认补充第0个词
        aspect_words.append(word_to_id.get(aspect, 0))

        # print('pst_entity',pst_entity)
        # print('pst_aspect', pst_aspect)

        y.append(senti)
        # if (pst_entity == -1 or pst_entity == -2):
        #     pst_entity = np.random.randint(noe_oov_words)
        # loc_e_list = np.array([i for i in range(max_sentence_len)]) - float(pst_entity)
        # position_entity.append(loc_e_list)
        #
        # if pst_aspect == -1 or pst_aspect == -2:
        #     pst_aspect = np.random.randint(noe_oov_words)
        # loc_a_list = np.array([i for i in range(max_sentence_len)]) - float(pst_aspect)
        # position_aspect.append(loc_a_list)


        idlist.append(id+'\t'+beizhu+'\t'+text+'\t'+entity+'\t'+aspect+'\t'+senti)#+'\t'+pst_entity+'\t'+pst_aspect)

        x.append(ids + [0] * (max_sentence_len - len(ids)))
        poslist.append(poss + [0] * (max_sentence_len - len(ids)))

    # 以下这段原本想统计有多少个aspect找到了对应的词向量，我这里不允许有找不到的，前面229行设置了报错提示
    # cnt = 0
    # for item in aspect_words:
    #     if item > 0:
    #         cnt += 1
    # print('cnt=', cnt)
    y_onehot = change_y_to_onehot(y)
    for item in x:
        if len(item) != max_sentence_len:
            print('$WARNING LEVEL-3$ 句子长度不对！', len(item))
    x = np.asarray(x, dtype=np.int32)
    # print(poslist[:3])
    poslist = np.asarray(poslist, dtype=np.int32)


    return x, np.asarray(sen_len), np.asarray(entity_words), np.asarray(aspect_words), np.asarray(y_onehot), \
           np.asarray(y), idlist, poslist# , np.asarray(position_entity), np.asarray(position_aspect)


def load_inputs_data_at_test(dataset, word_to_id, max_sentence_len, encoding='utf8', case='train'):
    x, y, sen_len = [], [], []
    entity_words = []
    aspect_words = []
    poslist = []
    idlist = []
    position_entity = []
    position_aspect = []

    lines = open(dataset, encoding=encoding).readlines()
    print(len(lines))
    read_lines = 0
    for i in range(0, len(lines)):
        if len(lines[i].rstrip().split('\t'))!=5:
            continue
        read_lines+=1
        id, beizhu, text, entity, aspect = lines[i].rstrip().split('\t')
        text = text.strip("\"")
        words = text.lower().split()
        if len(words) > max_sentence_len:
            if case == 'train':
                continue
            else:
                words = words[0:max_sentence_len]

        senti = '1'
        noe_oov_words = 0  # 加入不是oov的词数量——mw
        ids = []
        poss = []
        for wordpos in words:
            word, pos = wordpos.split('|')
            if word in word_to_id:
                noe_oov_words += 1  # 添加——mw
                ids.append(word_to_id[word])
                poss.append(1)

        sen_len.append(len(ids))
        entity_words.append(word_to_id.get(entity, 0))


        aspect_words.append(word_to_id.get(aspect, 0))


        y.append(senti)


        idlist.append(
            id + '\t' + beizhu + '\t' + text + '\t' + entity + '\t' + aspect + '\t' + senti)  # +'\t'+pst_entity+'\t'+pst_aspect)

        x.append(ids + [0] * (max_sentence_len - len(ids)))
        poslist.append(poss + [0] * (max_sentence_len - len(ids)))

    # 以下这段原本想统计有多少个aspect找到了对应的词向量，我这里不允许有找不到的，前面229行设置了报错提示
    # cnt = 0
    # for item in aspect_words:
    #     if item > 0:
    #         cnt += 1
    # print('cnt=', cnt)
    print("read lines",read_lines)
    y_onehot = change_y_to_onehot(y)
    for item in x:
        if len(item) != max_sentence_len:
            print('$WARNING LEVEL-3$ 句子长度不对！', len(item))
    x = np.asarray(x, dtype=np.int32)
    # print(poslist[:3])
    poslist = np.asarray(poslist, dtype=np.int32)

    return x, np.asarray(sen_len), np.asarray(entity_words), np.asarray(aspect_words), np.asarray(y_onehot), \
           np.asarray(y), idlist,poslist

def check_file_exist(files):
    for file in files:
        if not os.path.exists(file):
            print(file, 'not exists')
            return False
    return True

def pos2vec(posset):
    '''
    encode POSs to one-hot vectors
    :param posset: a set of POSs
    :return: dictionary, keys are POSs, values are one-hot vectors for POSs
    '''
    count=0
    p2id = dict()
    p2v = []
    for k in posset:
        vec = [0]*len(posset)
        vec[count] = 1
        count += 1
        p2id[k] = count
        p2v.append(vec)
    return p2id, p2v

def load_data_init(datset_file, test_file, w2v_file, embedding_dim, entity_id_file='', aspect_id_file=''):
    if not check_file_exist([datset_file, test_file, w2v_file]):
        raise Exception('file not exist error')
    maxlen,posset = get_max_sentence_len(datset_file)
    maxlen2,posset2 = get_max_sentence_len(test_file)
    print('maxlen:',maxlen,maxlen2)
    if maxlen2 > maxlen:
        maxlen = maxlen2
    if maxlen>280:
        maxlen = 280
    posset = posset|posset2
    p2id, p2v = pos2vec(posset)
    w2id, w2v = load_word_embedding(w2v_file, entity_id_file, aspect_id_file, embedding_dim)
    print('type',type(w2id),type(p2id))
    print('final maxlen',maxlen)
    return w2id, w2v, p2id, p2v, maxlen

