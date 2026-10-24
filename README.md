Tarea 3: Procesamiento de Datos en Tiempo Real — Bancolombia
Descripción del Proyecto
Este proyecto implementa un sistema de procesamiento de datos en tiempo real utilizando **Apache Kafka** como plataforma de mensajería y **Apache Spark Streaming** para el análisis y procesamiento de flujos de datos. El caso de uso simula el monitoreo de transacciones bancarias de Bancolombia, permitiendo el envío, recepción y análisis continuo de información.

Arquitectura del Sistema
El sistema está compuesto por cuatro componentes principales:

- **Apache Zookeeper:** Coordina y gestiona los servicios de Kafka
- **Apache Kafka:** Plataforma de transmisión y almacenamiento de mensajes en tiempo real
- **Productor:** Genera y envía las transacciones bancarias al sistema
- **Consumidor:** Recibe y procesa los datos de las transacciones entrantes

Estructura del Repositorio

| Archivo | Descripción |
|---------|-------------|
| `bancolombia_productor.py` | Genera y envía transacciones simuladas a Kafka |
| `bancolombia_consumidor.py` | Recibe y muestra las transacciones en tiempo real |
| `bancolombia_spark_consumer.py` | Procesamiento y análisis de datos con Spark Streaming |

Tecnologías Utilizadas

| Tecnología | Versión | Propósito |
|------------|---------|-----------|
| **Python** | 3.x | Lenguaje de programación |
| **Apache Kafka** | 2.13-3.7.1 | Broker de mensajes en tiempo real |
| **Apache Spark** | 3.5.3 | Procesamiento distribuido |
| **Hadoop** | 3 | Almacenamiento y gestión de datos |

