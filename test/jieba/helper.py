#!/home/marcpoint/anaconda3/envs/tensorflow1.3
# coding: utf-8
# __author__: u"Jiang Yuqin"
import os
import jieba
import jieba.posseg as pseg
import argparse
import subprocess

jieba.enable_parallel(8)


def wordseg(file):
    print('wordseg')
    f = open(file, 'r', encoding='utf8')
    lines = f.readlines()
    f.close()
    count = 0
    s = ''
    alist = []
    fw = open(file+'_wordseg.txt', 'w', encoding='utf8')

    for line in lines:
        if count % 10000 == 0:
            print(count)
            print('Processing: ', count / len(lines))
            for i in alist:
                fw.write(i)
            alist = []

        count += 1
        tmp = line.strip().split('\t')
        text = ''
        words = pseg.cut(tmp[2].replace('|', '-'))
        for w in words:
            text += w.word + '|' + w.flag + ' '
        tmp[2] = text
        s += '\t'.join(tmp[:3])

        tmp_enti = []
        tmp_aspe = []
        for i in tmp[3].split('|'):
            if i.startswith('aspect'):
                tmp_aspe.append(i.split('.')[-1])
            else:
                tmp_enti.append(i.split('.')[-1])

        if len(tmp_aspe) == 0:
            for e in tmp_enti:
                alist.append(s + '\t' + e + '\t' + 'NA' + '\n')
        else:
            if len(tmp_enti) != 0:
                for e in tmp_enti:
                    for a in tmp_aspe:
                        alist.append(s + '\t' + e + '\t' + a + '\n')
            else:
                pass
        s = ''

    for i in alist:
        fw.write(i)
    fw.close()

def splitfile(filename,model,data):

    if os.path.exists('./predict/'+data+'/input/'+model+'_topred') == False:
        os.mkdir('./predict/'+data+'/input/'+model+'_topred')
    f= open(filename,'r',encoding='utf8')
    line = f.readline()
    count = 0
    sb=''
    alist = []
    while line:
        count += 1
        if count % 1000 == 0:
            alist.append(sb)
            sb = ''
        if count%10000==0:
            print(count)
            fw = open('./predict/'+data+'/input/'+model+'_topred'+'/split{}.txt'.format(count/10000),'w',encoding = 'utf8')
            for i in alist:
                fw.write(i)
            fw.close()
            alist = []
        sb += line
        line = f.readline()

    fw = open('./predict/'+data+'/input/'+model+'_topred'+'/split{}.txt'.format(count / 10000 + 1 ), 'w', encoding='utf8')
    for i in alist:
        fw.write(i)
    fw.write(sb)
    fw.close()
    f.close()
if __name__=='__main__':

    parser = argparse.ArgumentParser(description='Input file processing.')
    parser.add_argument('-date', help='date marker', default='1222')
    args = vars(parser.parse_args())
    date = args['date']
    processlist = []
    category = ''
    for parent, dirnames, filenames in os.walk('./predict/' + date + '/input/'):
        for filename in filenames:
            if filename.startswith('baby_diapers') and not filename.endswith('_wordseg.txt'):
                category = 'diaper'
                processlist.append((category, os.path.join(parent, filename)))
            elif filename.startswith('mother_health') and not filename.endswith('_wordseg.txt'):
                category = 'baojianpin'
                processlist.append((category, os.path.join(parent, filename)))
            elif filename.startswith('baby_milk') and not filename.endswith('_wordseg.txt'):
                category = 'milk'
                processlist.append((category, os.path.join(parent, filename)))
            elif filename.startswith('baby_food') and not filename.endswith('_wordseg.txt'):
                category = 'fushi'
                processlist.append((category, os.path.join(parent, filename)))
            elif filename.startswith('baby_feedtool') and not filename.endswith('_wordseg.txt'):
                category = 'feedtool'
                processlist.append((category, os.path.join(parent, filename)))
    for i in processlist:
        print(i)
    for file in processlist:
        wordseg(file[1])
        splitfile(file[1]+ '_wordseg.txt', file[0], date)
    python = 'python'
    for i in processlist:
        if i[0] == 'milk':
            subprocess.call([python, 
                             'at_lstm.py',
                             '--dataset','data/'+i[0]+'/data_to_train.txt',
                             '--testset','data/'+i[0]+'/data_to_test.txt',
                             '--embedding_file_path','data/'+i[0]+'/general2-300/reduced_vectors.txt',
                             '--category',i[0],
                             '--date',date,
                             '--rootdir','./predict/'+date+'/input/'+i[0]+'_topred'])

        if i[0] == 'diaper':
            subprocess.call([python, 
                             'at_lstm.py',
                             '--dataset','data/'+i[0]+'/data_to_train.txt',
                             '--testset','data/'+i[0]+'/data_to_test.txt',
                             '--embedding_file_path','data/'+i[0]+'/general2-300/reduced_vectors.txt',
                             '--category',i[0],
                             '--date',date,
                             '--rootdir','./predict/'+date+'/input/'+i[0]+'_topred'])
        if i[0] == 'feedtool':
            subprocess.call([python, 
                             'at_lstm.py',
                             '--dataset','data/'+i[0]+'/data_to_train.txt',
                             '--testset','data/'+i[0]+'/data_to_test.txt',
                             '--embedding_file_path','data/'+i[0]+'/general2-300/reduced_vectors.txt',
                             '--category',i[0],
                             '--date',date,
                             '--rootdir','./predict/'+date+'/input/'+i[0]+'_topred'])
        if i[0] == 'baojianpin':
           subprocess.call([python, 
                             'at_lstm.py',
                             '--dataset','data/'+i[0]+'/data_to_train.txt',
                             '--testset','data/'+i[0]+'/data_to_test.txt',
                             '--embedding_file_path','data/'+i[0]+'/general2-300/reduced_vectors.txt',
                             '--category',i[0],
                             '--date',date,
                             '--rootdir','./predict/'+date+'/input/'+i[0]+'_topred'])
        if i[0] == 'fushi':
            subprocess.call([python, 
                             'at_lstm.py',
                             '--dataset','data/'+i[0]+'/data_to_train.txt',
                             '--testset','data/'+i[0]+'/data_to_test.txt',
                             '--embedding_file_path','data/'+i[0]+'/general2-300/reduced_vectors.txt',
                             '--category',i[0],
                             '--date',date,
                             '--rootdir','./predict/'+date+'/input/'+i[0]+'_topred'])
