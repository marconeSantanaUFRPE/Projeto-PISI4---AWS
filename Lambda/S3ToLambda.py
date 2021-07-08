import json
import boto3
import base64
import ast
import datetime


def MaxAndMin(string):
    s3 = boto3.resource('s3')
    var = 'TEMPERATURA_MIM' if string == '/tempmin' else "TEMPERATURA_MAX"
    CODESTACAO = 'CODIGO_ESTACAO_MIN' if string == '/tempmin' else "CODIGO_ESTACAO_MAX"

    LONG = 'LONGITUDE_MIN' if string == '/tempmin' else "LONGITUDE_MAX"
    LATI = 'LATITUDE_MIN' if string == '/tempmin' else "LATITUDE_MAX"

    
    bucket = 'testebucket4242'
    my_bucket = s3.Bucket(bucket)
    s3 = boto3.client('s3')
    dic = {}
    get_last_modified = lambda obj: int(obj['LastModified'].strftime('%s'))
    objs = s3.list_objects_v2(Bucket=bucket)['Contents']
    last_added = [obj['Key'] for obj in sorted(objs, key=get_last_modified, reverse=True)][0]
    response = s3.get_object(Bucket=bucket, Key=last_added)
    content = response['Body'].read().decode('utf-8')
    a = json.loads(content)
    print(a)
    horario = (a['HORAMEDICAO'])
    codigoEstacao = (a[CODESTACAO])
    latitude = (a[LATI])
    longitude = (a[LONG])
    valor = (a[var])
    nome = (a['NOME_ESTACAO_MAX'])
    dic[codigoEstacao] = {'CODIGO_ESTACAO':codigoEstacao, 'NOME_ESTACAO': nome, "LATITUDE": latitude, "LONGITUDE":longitude,'HORARIO_COLETA': horario, 'VALOR_OBSERVADO':valor}
    return dic
    
def radUmiOrMax(string):
    
    dicRef = {"/umidmed": "UMIDADE_RELATIVA", "/radiacao": "RADIACAO_GLOBAL" , "/precipitacao": "PRECIPITACAO_TOTAL"}
    s3 = boto3.resource('s3')
    #Eita
    bucket = 'testebucket4242'
    my_bucket = s3.Bucket(bucket)
    s3 = boto3.client('s3')
    dic = {}
    acumulado=0
    allFiles = my_bucket.objects.all()
    for file in allFiles :
        key = (file.key)
        today0 = datetime.date.today()
        today = datetime.datetime.combine(today0, datetime.time(0, 0))
        #Menos 3 horas
        dataLeitura = (str(file.last_modified - datetime.timedelta(hours=3)).split(" ")[0])
        
        a = ((datetime.datetime.strptime(dataLeitura, '%Y-%m-%d')))
        #oxe
        if a!=today:
            print("Arquivo de nãoHoje")
            print(file.key)
    
            teste = (str(file.last_modified))
            print(teste)
        else:
            print("Arquivo de hoje")
            print(file.key)
            teste = (str(file.last_modified))
            print(teste)
            response = s3.get_object(Bucket=bucket, Key=key)
            content = response['Body'].read().decode('utf-8')
            print('---------')
            a = json.loads(content)
            print(a)
            variavel = (a[dicRef[string]])
            print(variavel)
            
    return variavel

def lambda_handler(event, context):
            
    route = event['rawPath']
    #route = '/umidmed'
    retorno = {}
    
    if route == '/precipitacao':
        acumulado = radUmiOrMax(route)
        retorno = {'PRECIPITACAO TOTAL(mm)':str(acumulado)}
        
    elif route == '/radiacao':
        acumulado = radUmiOrMax(route)
                    
        retorno = {'RADIACAO GLOBAL (W/m2)':str(acumulado)}
        
        
    elif route == '/tempmax':
        retorno = MaxAndMin(route)
     
    elif route == '/tempmin':
        retorno = MaxAndMin(route)
        
        
    elif route == '/umidmed':
        acumulado = radUmiOrMax(route)
        retorno = {'UMIDADE RELATIVA DO AR INSTANTANEA (%)':str(acumulado)}

    else:
        retorno = '404'
    
    return {
        'statusCode': 200,
        'body': json.dumps(retorno)
    }
