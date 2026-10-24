from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transacciones_bancolombia',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Esperando transacciones...")
print("=" * 60)

for mensaje in consumer:
    datos = mensaje.value
    print(f"{datos['id_transaccion']}")
    print(f"{datos['fecha']} {datos['hora']}")
    print(f"Valor: ${datos['valor']:,}")
    print(f"Tipo: {datos['tipo']}")
    print(f"Sucursal: {datos['sucursal']}")
    print("-" * 60)
