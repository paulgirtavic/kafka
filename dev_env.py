import csv 
import json
import pandas as pd 

csvFilePath = 'data.csv'
df = pd.read_csv('application_record.csv')
print(len(df)-len(df.drop_duplicates()))
print(df.head)
print(len(df['ID'])-len(df['ID'].drop_duplicates()))
""" 
def f():
    jsonArray = []
      
    #read csv file
    with open(csvFilePath, encoding='utf-8') as csvf: 
        #load csv file data using csv library's dictionary reader
        csvReader = csv.DictReader(csvf) 

        #convert each csv row into python dict
        for row in csvReader: 
            print(row['ID'])
            #add this python dict to json array
            jsonArray.append(row)
            fjson = json.dumps(row, indent=4)
            #print(fjson)
  
    #convert python jsonArray to JSON String and write to file
    
    jsonString = json.dumps(jsonArray, indent=4)
    print()      


f() """