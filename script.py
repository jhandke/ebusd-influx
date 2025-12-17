from influxdb_client import InfluxDBClient
from influxdb_client.domain.write_precision import WritePrecision
import requests
import threading
import os
import sys

INFLUXDB_HOST = os.environ.get("INFLUXDB_HOST")
if INFLUXDB_HOST is None:
    print("Missing environment variable INFLUXDB_HOST", file=sys.stderr)
    sys.exit(-1)
INFLUXDB_ORG = os.environ.get("INFLUXDB_ORG")
if INFLUXDB_ORG is None:
    print("Missing environment variable INFLUXDB_ORG.", file=sys.stderr)
    sys.exit(-1)
INFLUXDB_TOKEN = os.environ.get("INFLUXDB_TOKEN")
if INFLUXDB_TOKEN is None:
    print("Missing environment variable INFLUXDB_TOKEN.", file=sys.stderr)
    sys.exit(-1)
INFLUXDB_BUCKET = os.environ.get("INFLUXDB_BUCKET")
if INFLUXDB_BUCKET is None:
    print("Missing environment variable INFLUXDB_BUCKET", file=sys.stderr)
    sys.exit(-1)

EBUSD_API_BASE = os.environ.get("EBUSD_API_BASE")
if EBUSD_API_BASE is None:
    print("Missing environment variable EBUSD_API_BASE", file=sys.stderr)
    sys.exit(-1)
EBUSD_CIRCUIT = os.environ.get("EBUSD_CIRCUIT")
if EBUSD_CIRCUIT is None:
    print("Missing environment variable EBUSD_CIRCUIT", file=sys.stderr)
    sys.exit(-1)
_EBUSD_MESSAGES = os.environ.get("EBUSD_MESSAGES")
if _EBUSD_MESSAGES is None:
    print("Missing environment variable EBUSD_MESSAGES", file=sys.stderr)
    sys.exit(-1)
EBUSD_MESSAGES = _EBUSD_MESSAGES.split(',')


boolean_mapping = {"on": True, "off": False}

last_sent_timestamps = {}

influx_client = InfluxDBClient(url=INFLUXDB_HOST, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
influx = influx_client.write_api()

def write_to_influx(message_name: str, message_value: str|float|int, timestamp: int):
    if not timestamp > last_sent_timestamps.get(message_name, 0):
        print(f"Value {message_name} not updated.")
        return
    if type(message_value) is str:
        if message_value in ("on", "off"):
            boolean_value = boolean_mapping.get(message_value)
            line = f"heating {message_name}={"true" if boolean_value else "false"} {timestamp}"
            print(line)
        else:
            line = f"heating {message_name}=\"{message_value}\" {timestamp}"
            print(line)
    elif type(message_value) is float or int:
        line = f"heating {message_name}={message_value} {timestamp}"
        print(line)
    else:
        print(f"Unhandled message_value data type: {type(message_value)}.")
        return
    last_sent_timestamps.update({message_name: timestamp})
    influx.write(bucket=INFLUXDB_BUCKET, record=line, write_precision=WritePrecision.S)

def run():
    for message in EBUSD_MESSAGES:
        ebusd_response = requests.get(f"{EBUSD_API_BASE}/data/{EBUSD_CIRCUIT}/{message}?&exact=true&maxage=20")
        if not ebusd_response.ok:
            print(f"Bad status code: {ebusd_response.status_code}, {ebusd_response.text}")
            continue
        json = ebusd_response.json()
        if "bai" not in json:
            print(f"Value for {message} is unchanged.")
            continue
        timestamp = json[EBUSD_CIRCUIT]["messages"][message]["lastup"]
        fields = json[EBUSD_CIRCUIT]["messages"][message]["fields"]
        if "value" in fields:
            value = fields["value"]["value"]
            print(value)
        elif "temp" in fields:
            value = fields["temp"]["value"]
            print(value)
        else:
            print("No matching field type found.")
            continue
        write_to_influx(message_name=message, message_value=value, timestamp=timestamp)
    print(80*"-")
            
            
ticker = threading.Event()
while not ticker.wait(10):
    run()