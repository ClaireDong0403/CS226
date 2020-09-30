# -*- coding: utf-8 -*-
"""
Created on Mon Dec  2 23:06:18 2019

@author: Blitz0613
"""
import numpy as np
import collections as cls

res=[]

file = open("C:/Users/Blitz0613/Desktop/cs226/1.txt")
 
for line in file:
    
    res.append(line.split(','))
    
myarray = np.asarray(res)   

state=myarray[:,0]

word=myarray[:,1]

dict1=cls.defaultdict(list)

for i in range(len(state)):
    
    dict1[state[i]].append(word[i])

counting_result=[]
    
for x in dict1.keys():
    
    count=cls.Counter(dict1[x]).most_common(3)
    
    counting_result.append(count) 
    

    
