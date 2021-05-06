# -*- coding: utf-8 -*-
"""
Created on Thu May  6 21:40:06 2021

@author: EINAV YAACOV
"""
import json
def momid (name:str) -> str:   
    if name in idlist:
        return idlist[name]
    else:
        idCounter =+1
        idlist.append(idCounter)
        return idCounter
count = 0
idlist = []
idCounter = 0 
listWahtsApp = list ()
metaData = dict()
whatsAppChat = open('Birthday.txt', 'r', encoding = 'utf-8')
for line in whatsAppChat:
    if 'נוצרה על ידי' in line:
       date = line[:16]
       metaData ['date'] = date
       text = "נוצרה על ידי"
       textInLine = line[16:].split(text)[0]
       metaData ['chat_name'] = textInLine
       telNum = line[16:].split(text)[1]
       metaData['creator'] = telNum
       print(metaData)   
    lineInChat = dict()
    if line[:8] == "6.4.2021" or line[:8] == "7.4.2021":
        if ":" in line:
            if (line.count(":")) == 1:
                continue
            else:
                dateTime = line[:16]    
                name = line[17:].split(":")[0]
        try:
            message = line[17:].split(":")[1]
        except:
            message = " "
        lineInChat['dateTime'] = dateTime
        lineInChat['id'] = momid(name)
        lineInChat['message'] = message
        listWahtsApp.append(lineInChat)

text_Line = name + ".json"
Dic_Dictionaries ={"messages": listWahtsApp , "metadata": metaData}
#Create a file with the data
with open(text_Line, "w") as fill:
    json.dump(Dic_Dictionaries, fill)





        
    

"""
    if "את" not in lineInChat[line].split[16][1].split(" - ")[1].split(":")[0].split(" "):
    ##b=lineInChat[line].split(":")[1].split(" - ")[1].split(":")[0]
    """