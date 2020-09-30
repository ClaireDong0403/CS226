# -*- coding:utf-8 -*-
import re
import sys
from string import digits
 


fp = open("C:/Baturu/cs226 data/rawData/data006.json", "r")
#fp = open("C:/Baturu/test/data000.json", "r")
data = fp.readlines()

file=open("C:/Baturu/cs226 data/data006clean.json", "w")
#file=open("C:/Baturu/test/data000clean.json", "w")
 
for line in data:
    if len(line) == 1:   #if this line is empty then skip
        continue
    else:
    #time   
    #############################################################    
        char_11='\"created_at\":'
        nPos11=line.find(char_11)
        nPos11=nPos11+18
        string = line[nPos11:nPos11+30].split(',')
        list11 = string[0]
        list11 = list11[:-1]
        file.write(list11)
        file.write('\"\"\"')
        
    #user_id    
    #############################################################    
        char_10='\"user\":'
        nPos10=line.find(char_10)
        nPos10=nPos10+13
        string = line[nPos10:nPos10+30].split(',')
        list10 = string[0]
        #print(list10)
        file.write(list10)
        file.write('\"\"\"')
    
    #text    
    #############################################################
        newline = line
        list0=''
        char_0='\"quoted_status\"'
        char_1='\"full_text\"'
        char_14=',\"text\":'
        nPos0=line.find(char_0)
        if(nPos0 != -1):
            nPos0 = nPos0 + 16
            newline = line[nPos0:]
            nPos1=newline.find(char_1)
            if(nPos1 != -1):
                nPos1=nPos1+13              #the end of "full_text":"
                char_5='\",\"'
                nPos5=newline[nPos1:].find(char_5) #the position of last" of "full_text":""
        #        print(line[nPos1:nPos1+nPos5])      #nPos1+nPos5 = tthe position of last" of "full_text":""
                list0 = newline[nPos1:nPos1+nPos5]
                newline = newline[nPos1+nPos5:]
            else:
                nPos1=newline.find(char_14)
                nPos1=nPos1+9              #the end of "text":"
                string = newline[nPos1:nPos1+200].split('","')
                list0 = string[0]
                newline = newline[nPos1:]
        
        nPos1=newline.find(char_1)
        if(nPos1 != -1):
            nPos1=nPos1+13              #the end of "full_text":"
            char_5='\",\"'
            nPos5=newline[nPos1:].find(char_5) #the position of last" of "full_text":""
            list1 = newline[nPos1:nPos1+nPos5]
        else:
            nPos1=line.find(char_14)
            nPos1=nPos1+9              #the end of "text":"
            string = line[nPos1:nPos1+200].split('","')
            list1 = string[0]
        
 
        file.write(list1)
        if(list0 !=''):
            file.write('\"\"')
            file.write(list0)
        file.write('\"\"\"')
        
        


    #hashtags
    #############################################################
        char_6='\"hashtags\"'  #hashtag is empty
        nPos6=line.find(char_6)
        nPos6=nPos6+12         #the position { of"hashtags": [{}]

        char_8=']'
        nPos8=line[nPos6:].find(char_8)
        if nPos8 < 3:
    #        print(" ")
            file.write(' ')
            file.write('\"\"\"')

        else:  #hashtags no empty

            char_7='}]'
            nPos7=line[nPos6:].find(char_7) #the position of }] of "hashtags":[{},{},{}]

            #string = line[nPos6:nPos6+nPos7+1]
            string = line[nPos6:nPos6+nPos7].split('},{')
            for list3 in string:
                char_12='\"text\":'
                nPos12=list3.find(char_12)
                nPos12=nPos12+8
                list3 = list3[nPos12:nPos12+30].split('\",\"')
                list3 = list3[0]
                #print(list3)
                file.write(list3)
                file.write(',')
            #print(string[0])
            file.write('\"\"\"')
    
    #city name: this is correct because the coordination of this tweets is before retweet
    #############################################################
        char_9='\"full_name\"'  #find city name
        nPos9=line.find(char_9)
        nPos9=nPos9+13         #the position : of"full_name":"
        string = line[nPos9:nPos9+200].split('"')
        list4 = string[0]
        file.write(list4)
        file.write('\"\"\"')
#        print(list4)


    
    #coordination
    #############################################################  
        m = re.findall(r"bounding_box",line)  #find "bounding_box"
        if m:
            char_2='\"bounding_box\"'
            nPos2=line.find(char_2)
            nPos2=nPos2 + 14 #the position of: of "bounding_box": 
        #    print(nPos2)

            char_4 = '\"coordinates\"'
            nPos4 = line[nPos2:].find(char_4)
            nPos4 = nPos4 +14   #the position of: of "coordinates":


            char_3='}'
            nPos3=line[nPos2:].find(char_3) #the position of } of "bounding_box":{ }
        #    print(line[nPos2+nPos4:nPos2+nPos3])      #nPos2+nPos3 = the } of "bounding_box":{}
            list2 = line[nPos2+nPos4:nPos2+nPos3]
            list2 = list2.replace('[','')
            list2 = list2.replace(']','')
            list2 = list2.split(',')
        #    print(list2)

            longitude = format((float(list2[0]) + float(list2[2]) + float(list2[4]) + float(list2[6])) / 4, '.6f')
            latitude = format((float(list2[1]) + float(list2[3]) + float(list2[5]) + float(list2[7])) / 4, '.6f')
    #        print(longitude,latitude)
            longitude = str(longitude)
            latitude = str(latitude)            
            file.write(longitude)
            file.write(',')
            file.write(latitude)
        else:  #if do not have the imformation of place, skip
            file.write(' ')
            continue
    ##############################################################
        file.write('\n')


fp.close()
file.close()