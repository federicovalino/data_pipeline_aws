"""
Producer simulado de clickstream para el Kinesis Data Stream.

Envía N eventos (100 por defecto) usando el user_id como PartitionKey:
- Alta cardinalidad: muchos usuarios distintos → la carga se reparte entre shards
  (evita el hot shard que genera una clave constante como "user_event").
- Orden por usuario: todos los eventos de un mismo user_id caen en el mismo shard,
  así que se conserva el orden de su sesión.

Uso:
    pip install boto3
    python scripts/producer.py --stream-name dataplatform-dev-stream
    python scripts/producer.py --stream-name dataplatform-dev-stream --count 100 --users 50

Modo continuo (pre-entrega 4, para alimentar las ventanas de Flink):
    python scripts/producer.py --stream-name dataplatform-dev-stream --count 600 --users 20 --interval 0.5 --late-rate 0.05
    --interval    segundos entre eventos (flujo continuo: las ventanas de 1 minuto se van cerrando)
    --late-rate   fracción de eventos con event_ts atrasado 20-120 s (simula red móvil / reintentos)
"""

import argparse
import json
import random
import time
import uuid
from collections import Counter
from datetime import datetime, timedelta, timezone

import boto3

EVENT_TYPES = ["page_view", "click", "add_to_cart", "checkout"]
PAGES = ["/", "/catalogo", "/producto/123", "/producto/456", "/carrito", "/checkout"]
BATCH_SIZE = 500  # máximo de registros por llamada a PutRecords


def build_event(user_id: str, late_rate: float = 0.0) -> dict:
    event_time = datetime.now(timezone.utc)
    if random.random() < late_rate:
        # Evento "tardío": se generó antes pero llega ahora (red móvil, reintentos)
        event_time -= timedelta(seconds=random.randint(20, 120))
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": user_id,
        "event_type": random.choice(EVENT_TYPES),
        "page": random.choice(PAGES),
        "event_ts": event_time.isoformat(),
    }


def send_continuous(client, args, users) -> None:
    """Un evento cada --interval segundos: flujo sostenido para las ventanas de Flink."""
    shard_counter: Counter = Counter()
    for i in range(args.count):
        event = build_event(random.choice(users), args.late_rate)
        response = client.put_record(
            StreamName=args.stream_name,
            Data=(json.dumps(event) + "\n").encode("utf-8"),
            PartitionKey=event["user_id"],
        )
        shard_counter[response["ShardId"]] += 1
        if (i + 1) % 20 == 0:
            print(f"  {i + 1}/{args.count} enviados | último: {event['user_id']} {event['event_type']} {event['event_ts']}")
        time.sleep(args.interval)
    print(f"Stream: {args.stream_name} | enviados: {args.count}")
    for shard_id, n in sorted(shard_counter.items()):
        print(f"  {shard_id}: {n} registros")


def main() -> None:
    parser = argparse.ArgumentParser(description="Producer simulado para Kinesis")
    parser.add_argument("--stream-name", required=True)
    parser.add_argument("--count", type=int, default=100, help="cantidad de eventos")
    parser.add_argument("--users", type=int, default=50, help="cantidad de usuarios distintos")
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--interval", type=float, default=0.0,
                        help="segundos entre eventos (0 = envío en lote, como en la pre-entrega 2)")
    parser.add_argument("--late-rate", type=float, default=0.0,
                        help="fracción de eventos con event_ts atrasado (para probar watermarks)")
    args = parser.parse_args()

    client = boto3.client("kinesis", region_name=args.region)
    users = [f"user-{i:04d}" for i in range(args.users)]

    if args.interval > 0:
        send_continuous(client, args, users)
        return

    records = []
    for _ in range(args.count):
        event = build_event(random.choice(users))
        records.append(
            {
                "Data": (json.dumps(event) + "\n").encode("utf-8"),  # \n: un JSON por línea en S3
                "PartitionKey": event["user_id"],
            }
        )

    shard_counter: Counter = Counter()
    failed = 0
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i : i + BATCH_SIZE]
        response = client.put_records(StreamName=args.stream_name, Records=batch)
        failed += response["FailedRecordCount"]
        for result in response["Records"]:
            if "ShardId" in result:
                shard_counter[result["ShardId"]] += 1
            else:
                print(f"  rechazado: {result.get('ErrorCode')} {result.get('ErrorMessage')}")
        time.sleep(0.2)

    print(f"Stream: {args.stream_name}")
    print(f"Enviados: {len(records) - failed} / {len(records)}  (fallidos: {failed})")
    print("Distribución por shard:")
    for shard_id, n in sorted(shard_counter.items()):
        print(f"  {shard_id}: {n:4d} registros  {'#' * (n // 2)}")


if __name__ == "__main__":
    main()
