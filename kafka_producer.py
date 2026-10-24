#Productor de transacciones - Bancolombia

import json
import random
import time
from kafka import KafkaProducer

producer=KafkaProducer(

bootstrap_servers=['localhost:9092'],
     value_serializer=lambda v:
json.dumps(v).encode('utf-8')
)

TOPICO = "transacciones_bancolombia"

sucursales=["Medellin","Bogota","Cali","Barranquilla","Pereira"]
tipos_transaccion=["retiro","deposito","transferencia","pago_servicio"]
estados=["aprobada","pendiente","rechazada"]
print("="*60)
print("enviando transacciones de bancolombia a kafka...")
print(f"Topico: {TOPICO}")
print("=" * 60)

for i in range(1,11):
    id_transaccion=f"BANCO-{1000+i}"
    fecha=time.strftime("%Y-%m-%d")
    hora=time.strftime("%H:%M:%S")
    valor=round(random.uniform(50000, 2000000), 2)
    tipo=random.choice(tipos_transaccion)
    sede=random.choice(sucursales)
    estado=random.choice(estados)
    cuenta=f"*{random.randint(1000, 9999)}"

    transaccion={
         "id_transaccion": id_transaccion,
         "fecha":fecha,
         "valor":valor,
         "tipo":tipo,
         "sucursal":sede,
         "estado":estado,
         "cuenta_origen":cuenta
    }

    producer.send(TOPICO,value=transaccion)
