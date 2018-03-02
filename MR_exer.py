# -*- coding: utf-8 -*-
"""
Created on Sun Feb 11 18:40:26 2018

@author: Administrator
"""


import sys

for line in sys.stdin:
    ss = line.strip().split(' ')
    for word in ss:
        print('\t'.join([word.strip(), '1']))




import sys

cur_word = None
sums = 0

for line in sys.stdin:
    word, cnt = line.strip().split('\t')
    
    if cur_word == None:
        cur_word = word
        
    if cur_word != word:
        print('\t'.join([cur_word, str(sums)]))
        
        cur_word = word
        sums = 0
        
    sums += int(cnt)