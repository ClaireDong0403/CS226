# -*- coding: utf-8 -*-
"""
Created on Tue Dec  3 21:24:56 2019

@author: Blitz0613
"""

import pandas
import matplotlib.pyplot as plt
from matplotlib.pyplot import figure
import numpy as np
from numpy import random
from collections import Counter

label=genrelist

a=[]

b=[]

for i in range(len(label)):
    
    a.append(random.randint(0, 100))

for i in range(len(label)):
    
    b.append(random.randint(0, 100))

width = 0.35       



index = np.arange(len(label))
figure(figsize=(28, 14), dpi=80, facecolor='w', edgecolor='k')
plt.bar(index-width, np.transpose(a), width, label='Weekday')
plt.bar(index + width, np.transpose(b), width, label='Weekend')
plt.xlabel('Genre', fontsize=30)
plt.ylabel('Preference', fontsize=30)
plt.xticks(index, label, fontsize=20, rotation=30)
plt.yticks( fontsize=20)
plt.legend(loc='best', prop={'size': 15})
plt.show()
