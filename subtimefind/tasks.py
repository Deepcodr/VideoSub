import os
import boto3
import csv
import json
from boto3.dynamodb.conditions import Key, Attr
from celery import shared_task
from botocore.client import Config
from django.conf import settings

# session = configureboto3session

dynamodb=session.resource('dynamodb',endpoint_url=settings.DB_URL)
s3bucket=session.resource('s3','ap-south-1',config=Config(s3={'addressing_style': 'path'}))
substable=dynamodb.Table(settings.DB_NAME)


@shared_task(bind=True)
def gettime(self,queryval,filename):
    # filename='Test.mp4'
    # gsitable= dynamodb.Table('SUBSTORE')
    print(queryval)
    response= substable.scan(
        IndexName="TimeSub",
        # KeyConditionExpression= Key('VideoName').eq(f'{filename}'),
        FilterExpression=Key('VideoName').eq(f'{filename}') & Attr('SubString').contains(f' {queryval} ')
    )

    print(response['Items'])
    timelist=[]
    
    for item in response['Items']:
        timelist.append(item['TimeSegment'])
    
    return timelist

@shared_task(bind=True)
def csvtojson(self,filename):
    data = []
    
    csvfilename=os.path.join(settings.BASE_DIR,"media\\test.csv")
    with open(csvfilename, encoding='utf-8') as csvf:
        csvReader = csv.DictReader(csvf)
        for rows in csvReader:
            key = rows['ID']
            rows['ID']=int(rows['ID'])
            data.append(rows)

    jsonfile=os.path.join(settings.BASE_DIR,"media\\test.json")
    with open(jsonfile, 'w', encoding='utf-8') as jsonf:
        jsonf.write(json.dumps(data, indent=4))
    
    os.remove(csvfilename)
    importjson.delay(filename)


@shared_task(bind=True)
def getsubs(self,filename):
    
    subfilename=os.path.join(settings.BASE_DIR,f"media\{filename[:-4]}.txt")
    file = open(subfilename,"r+")
    lines= file.readlines()

    linelist=[]
    for line in lines:
        splitline=line.split('|')
        splitline[0]=splitline[0][:-4]
        splitline[1]=splitline[1][:-4]
        splitline.remove("POP")
        # splitline.pop(1)
        # print(splitline)
        linelist.append(splitline)

    file.truncate(0)
    csvfilename=os.path.join(settings.BASE_DIR,"media\\test.csv")
    file=open(csvfilename,'w+')
    file.write("ID,VideoName,TimeSegment,SubString\n")
    i=1
    for line in linelist:
        writel=""
        writel+=str(i)+","+filename+","
        writel+=line[0]+"->"+line[1]
        line[2]=line[2].replace(",","")
        line[2]=line[2].encode("ascii","ignore")
        line[2]=line[2].decode()
        line[2]=line[2][:1]+" "+line[2][1:]
        writel+=","+line[2]
        file.writelines(writel)
        i=i+1
    
    os.remove(subfilename)
    csvtojson.delay(filename)

@shared_task(bind=True)
def importjson(self,filename):
    jsonfile=os.path.join(settings.BASE_DIR,"media\\test.json")
    with open(jsonfile) as json_file:
        subs_list = json.load(json_file)
        
    for subs in subs_list:
        substable.put_item(Item=subs)

    os.remove(jsonfile)
    uploadtos3.delay(filename)

@shared_task(bind=True)
def getvidnames(self):
    response= substable.scan()
    # print(response['Items'])
    vidnamelist=[]
    for item in response['Items']:
        # print(item['VideoName'])
        if item['VideoName'] not in vidnamelist:
            vidnamelist.append(item['VideoName'])

    return vidnamelist

@shared_task(bind=True)
def uploadtos3(self,filename):
    # s3bucket.Bucket("bezentask").download_file("media/erd.png","erdfile.png")
    vidfilepath=os.path.join(settings.BASE_DIR,f"media\\{filename}")
    s3bucket.Bucket("bezentask").upload_file(vidfilepath,f"{filename}")
    os.remove(vidfilepath)