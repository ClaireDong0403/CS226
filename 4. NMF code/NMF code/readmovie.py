# -*- coding: utf-8 -*-
"""
Created on Tue Dec  3 13:50:45 2019

@author: Blitz0613
"""

import pandas as pd 


data = pd.read_csv("C:/Users/Blitz0613/Desktop/cs226/genredata.csv",encoding = "ISO-8859-1") 

genre=data.iloc[:,1]

s=set()

res=[]

for x in genre:    
    
    x=str(x)
    
    if x!='nan':
    
        a=x.split("|")
        
        for i in a:
            
            s.add(i)

genrelist=list(s)
        
        
