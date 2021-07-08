import requests
import json
import datetime
import boto3
from datetime import timedelta


client = boto3.client('kinesis')


def getDateFromINMET(date,hour):

    baseString = "https://apitempo.inmet.gov.br/estacao/dados/"
    stringRequest = baseString + date +"/"+ hour+ "00"
    print(stringRequest)
    response = requests.get(stringRequest)
    if response.status_code == 200:
        posts = json.loads(response.text) #load data into a dict of objects, posts
        return posts
    else:
        return{"Erro":"Error"}

def lambda_handler(event, context):
    
    dateTime = (datetime.datetime.now() - timedelta(hours=3))
    print(dateTime)
    date, hours = str(dateTime).split(" ")
    hour, minutes, seconds = hours.split(":")
    hour = str(hour)
    print(hour)
    dic={}
    cont=0
    inmetData = getDateFromINMET(date,hour)
    kinesisRecords = [] 
    for x in inmetData:
        if(x['UF'] == "PE"):
            uf = x["DC_NOME"] #if x["DC_NOME"] != None else "0"
            tempMax = x["TEM_MAX"] #if x["TEM_MAX"] != None else "0" #maximo
            prec = x["CHUVA"] #if x["CHUVA"] != None else "0" # acumulado
            temMin = x["TEM_MIN"] #if x["TEM_MIN"] != None else "0"  # minima
            umiRel = x["UMD_INS"] #if x["UMD_INS"] != None else "0"  # media
            radG = x["RAD_GLO"] #if x["RAD_GLO"] != None else "0"  # acumulado
            cdEstacao = x["CD_ESTACAO"] #if x["CD_ESTACAO"] != None else "0" # codigo da estacao
            horaMedicao = x["HR_MEDICAO"] #if x["HR_MEDICAO"] != None else "0"
            latitude = x["VL_LATITUDE"] #if x["VL_LATITUDE"] != None else "0"
            longitude = x["VL_LONGITUDE"] #if x["VL_LONGITUDE"] != None else "0"
            
            dic[uf] = {"CODIGO_ESTACAO":cdEstacao,'NOME_ESTACAO':uf,'LATITUDE':latitude, 'LONGITUDE':longitude
                ,'HORAMEDICAO':horaMedicao, "TEMPERATURA_MAX": tempMax, "PRECIPITACAO_TOTAL":prec,'TEMPERATURA_MIM':temMin
                , 'UMIDADE_RELATIVA':umiRel,'RADIACAO_GLOBAL':radG}
            teste = 1
            j = json.dumps(dic[uf])
            encodedValues = bytes(j, 'utf-8')
            kinesisRecord = { "Data": encodedValues, "PartitionKey": str(teste) }
            kinesisRecords.append(kinesisRecord)
            teste+=1
    response = client.put_records( Records=kinesisRecords, StreamName = "NovoLambdaToAnalytics")
    print(response)
    for x in dic:
        print(dic[x])
    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps(response)
    }