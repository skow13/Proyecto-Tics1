import os
import json
import psycopg2
import paho.mqtt.client as mqtt
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURACION -------------------------------------------------------
DB_CONFIG = {
    "host":     os.getenv("DB_HOST"),
    "dbname":   os.getenv("DB_NAME"),
    "user":     os.getenv("DB_USER"),
    "password": os.getenv("DB_PASS")
}

MQTT_BROKER = os.getenv("MQTT_BROKER")
MQTT_TOPIC  = os.getenv("MQTT_TOPIC")
MQTT_USER   = os.getenv("MQTT_USER")
MQTT_PASS   = os.getenv("MQTT_PASS")

# --- BASE DE DATOS -------------------------------------------------------
def get_connection():
    return psycopg2.connect(**DB_CONFIG)

def registrar_medida(mac, temperatura, humedad, modo):
    try:
        conn = get_connection()
        cur  = conn.cursor()
        cur.execute(
            "SELECT * FROM registrar_medida(%s, %s, %s, %s)",
            (mac, temperatura, humedad, modo)
        )
        resultado = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        print(f"Medida guardada -> ID: {resultado[0]} | {temperatura}C | {humedad}%")
    except psycopg2.Error as e:
        print(f"Error DB: {e}")

# --- CALLBACKS MQTT ------------------------------------------------------
def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        print(f"Conectado al broker MQTT: {MQTT_BROKER}")
        client.subscribe(MQTT_TOPIC)
        print(f"Suscrito al topico: {MQTT_TOPIC}")
    else:
        print(f"Error al conectar. Codigo: {reason_code}")

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
        print(f"\nMensaje recibido: {payload}")

        mac         = payload.get("mac")
        temperatura = payload.get("temperatura")
        humedad     = payload.get("humedad")
        modo        = payload.get("modo", "wifi")

        if not all([mac, temperatura, humedad]):
            print("Payload incompleto, se descarta.")
            return

        registrar_medida(mac, temperatura, humedad, modo)

    except json.JSONDecodeError:
        print(f"Mensaje no es JSON valido: {msg.payload}")
    except Exception as e:
        print(f"Error inesperado: {e}")

def on_disconnect(client, userdata, flags, reason_code, properties):
    if reason_code != 0:
        print("Desconexion inesperada del broker. Reconectando...")

# --- MAIN ----------------------------------------------------------------
if __name__ == "__main__":
    print("=== Suscriptor MQTT -> PostgreSQL ===")

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.username_pw_set(MQTT_USER, MQTT_PASS)
    client.tls_set()
    client.on_connect    = on_connect
    client.on_message    = on_message
    client.on_disconnect = on_disconnect

    try:
        client.connect(MQTT_BROKER, 8883, keepalive=60)
        client.loop_forever()
    except KeyboardInterrupt:
        print("\nDetenido manualmente.")
        client.disconnect()
    except Exception as e:
        print(f"Error de conexion: {e}")