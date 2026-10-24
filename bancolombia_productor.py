from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

sucursales = ["Medellín", "Bogotá", "Cali", "Barranquilla", "Pereira"]
tipos = ["retiro", "deposito", "transferencia", "pago_servicio"]

print("Enviando transacciones de Bancolombia...")

for i in range(1, 11):
    transaccion = {
        "id_transaccion": f"BANCO-{1000+i}",
        "fecha": time.strftime("%Y-%m-%d"),
        "hora": time.strftime("%H:%M:%S"),
        "valor": round(random.uniform(50000, 2000000), 2),
        "tipo": random.choice(tipos),
        "sucursal": random.choice(sucursales),
        "estado": "completada"
    }
    producer.send("transacciones_bancolombia", transaccion)
    print(f"Enviado: {transaccion['id_transaccion']}")
    time.sleep(1)

producer.flush()
print("Todas las transacciones enviadas")
