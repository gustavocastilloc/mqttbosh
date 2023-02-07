from paho.mqtt import client as mqtt_client
import random 
import time
import psycopg2
import threading
from datetime import datetime
broker = '192.168.30.50' # AQUI VA LA IP
port = 1883
topic = "camiva/onvif-ej/RuleEngine/CountAggregation/Counter/&1/Mangos"
topic2= "camiva/onvif-ej/RuleEngine/CountAggregation/OccupancyCounter/&1/OcupaciónCaja"
client_id = f'python-mqtt-{random.randint(0, 1000)}'
# username = 'emqx'
# password = 'public'
#cont=0
def insertarDatos(timestamp,counter):
    try:
        connection = psycopg2.connect(user="postgres",
                              password="*****",
                              host="url",
                              port="5432",
                              database = "metadatasystem")
        cursor = connection.cursor()
        insert_script = "INSERT INTO mangoscantidad(timestamp_date,cantidad) values(%s,%s)"
        insert_values = (timestamp,counter)
        cursor.execute(insert_script,insert_values)
        connection.commit()
        #resultado = cursor.fetchall()
        #print(resultado)
        #time.sleep(0.3)
    except Exception as ex:
        print("Ocurrió un error con la base de datos remota "+ex)
    finally:
        connection.close()
def insertarDatosCaja(timestamp,counter,totalporcaja,tamano):
    try:
        connection = psycopg2.connect(user="postgres",
                              password="*******",
                              host="url",
                              port="5432",
                              database = "metadatasystem")
        cursor = connection.cursor()
        insert_script = "INSERT INTO cajascantidad(timestamp_date,cantidadtotal,totalporcaja,tamaño) values(%s,%s,%s,%s)"
        insert_values = (timestamp,counter,totalporcaja,tamano)
        cursor.execute(insert_script,insert_values)
        connection.commit()
        #resultado = cursor.fetchall()
        #print(resultado)
        #time.sleep(0.3)
    except Exception as ex:
        print("Ocurrió un error con la base de datos remota "+ex)
    finally:
        connection.close()
def getValue():
    try:
        connection = psycopg2.connect(user="postgres",
                              password="*****",
                              host="url",
                              port="5432",
                              database = "metadatasystem")
        cursor = connection.cursor()
        select_script = "select * from cajascantidad order by caja_id desc limit 1;"
        cursor.execute(select_script)
        connection.commit()
        resultado = cursor.fetchall()
        cantidad= resultado[0][-3]
        return cantidad
        #time.sleep(0.3)
    except Exception as ex:
        print("Ocurrió un error con la base de datos remota "+ex)
    finally:
        connection.close()
def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)
    # Set Connecting Client ID
    client = mqtt_client.Client(client_id)
    #client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client
lista=[]
ctiempo=[]

def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        #print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
        #print(msg.topic)
        #print(msg.payload.decode())
        
        if(msg.topic.__contains__('Mangos')):
            timestamp = msg.payload.decode().split(",")[0].split('"')[-2]
            count=int(msg.payload.decode().split(",")[-1].split('"')[-2])
            print("mangos=",count)
            print("time mangos=",timestamp)
            lista.append(count)
            ctiempo.append(timestamp)
            insertarDatos(timestamp,count)
            print('row added mango')
        elif(msg.topic.__contains__('Caja')):
            ocupacion=int(msg.payload.decode().split(",")[-1].split('"')[-2])
            timestamp2 = msg.payload.decode().split(",")[0].split('"')[-2]
            print("ocupacion",ocupacion)
            print("time 2=",timestamp2)
            ultimacantidad=getValue()
            if(ocupacion>0 and lista[-1]!=ultimacantidad):
                totalporcaja=abs(lista[-1]-ultimacantidad)
                print("inicio---->",ultimacantidad)
                print("inicio,fin-->",lista[-1])
                print("Total por caja-->",totalporcaja)
                #print(ultimacantidad)
                if(10<=totalporcaja<=14):
                    tamano='MEDIANO'
                    insertarDatosCaja(timestamp2,lista[-1],totalporcaja,tamano)
                    print("row added cajascantidad")
                elif(6<=totalporcaja<10):
                    tamano='GRANDE'
                    insertarDatosCaja(timestamp2,lista[-1],totalporcaja,tamano)
                    print("row added cajascantidad")
                else:
                    tamano='DESCONOCIDO'
                    insertarDatosCaja(timestamp2,lista[-1],totalporcaja,tamano)
                
                print('*********row added**********')
                
        print(len(lista))
        if(len(lista)==10):
            lista[:-2]=[]
        
    client.subscribe(topic)
    client.subscribe(topic2)
    client.on_message = on_message
    
def run():
    client = connect_mqtt()
    subscribe(client)
    client.loop_forever()
    


if __name__ == '__main__':
    run()
