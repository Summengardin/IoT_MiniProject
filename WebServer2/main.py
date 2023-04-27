import paho.mqtt.client as mqtt
import psycopg2

# Define database-connection (postgresql)
print("Connecting to database...")
conn = psycopg2.connect(
    host="localhost",
    database="db_gui",
    user="postgres",
    password="master",
    port="5434")
print("Successfully connected to database!")

# Define mqtt-settings.
MQTT_BROKER = "158.38.67.229"
MQTT_PORT = 1883
MQTT_USERNAME = 'ntnu'
MQTT_PASSWORD = 'ntnuais2103'
MQTT_KEEPALIVE = 5

# Define mqtt-topics
# subscribe
topic_temperature_filtered = 'RTUIot/marsi/Temp/filtered'
topic_pressure_filtered = 'RTUIot/marsi/Press/filtered'

topic_temperature_aggr = 'RTUIot/marsi/Temp/aggr'
topic_pressure_aggr = 'RTUIot/marsi/Press/aggr'

# Define mqtt-client
mqtt_client = mqtt.Client()
mqtt_client.username_pw_set(username=MQTT_USERNAME, password=MQTT_PASSWORD)


# Define "on_connect"-function
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Successfully connected to mqtt-broker!")
        mqtt_client.subscribe(topic_temperature_filtered)
        mqtt_client.subscribe(topic_pressure_filtered)

        mqtt_client.subscribe(topic_temperature_aggr)
        mqtt_client.subscribe(topic_pressure_aggr)
    else:
        print(f"Bad connection. Code: {rc}")


# Define "on_message"-function
def on_message(client, userdata, _msg):
    # ======= DECODE MESSAGE =======
    msg = dict(
        topic=_msg.topic,
        # payload[0] = time or date, payload[1] = value
        payload=_msg.payload.decode().split(',')
    )
    print(msg)

    query = None

    # ============== TEMPERATURE ==================
    if msg['topic'] == topic_temperature_filtered:
        query = f"""INSERT INTO temperature_log (timestamp, value) VALUES(%s, %s);"""
    elif msg['topic'] == topic_pressure_filtered:
        query = f"""INSERT INTO pressure_log (timestamp, value) VALUES(%s, %s);"""
    elif msg['topic'] == topic_temperature_aggr:
        query = f"""MERGE INTO temperature_aggr AS T
	                USING (SELECT '{msg['payload'][0]}'::date AS date, {msg['payload'][1]} AS value_max, {msg['payload'][2]} AS 
                            value_min, {msg['payload'][3]} AS value_avg 
                            ) AS S 
                    ON (T.date = S.date)
                    WHEN MATCHED THEN 
                        UPDATE SET 
                            value_max = S.value_max,
                            value_min = S.value_min, 
                            value_avg = S.value_avg  
                    WHEN NOT MATCHED THEN 
                        INSERT (date, value_min, 
                            value_max, value_avg) VALUES (S.date, S.value_min, S.value_max, S.value_avg);"""
    elif msg['topic'] == topic_pressure_aggr:
        query = f"""MERGE INTO pressure_aggr AS T
        	                USING (SELECT '{msg['payload'][0]}'::date AS date, {msg['payload'][1]} AS value_max, {msg['payload'][2]} AS 
                                    value_min, {msg['payload'][3]} AS value_avg 
                                    ) AS S 
                            ON (T.date = S.date)
                            WHEN MATCHED THEN 
                                UPDATE SET 
                                    value_max = S.value_max,
                                    value_min = S.value_min, 
                                    value_avg = S.value_avg  
                            WHEN NOT MATCHED THEN 
                                INSERT (date, value_min, 
                                    value_max, value_avg) VALUES (S.date, S.value_min, S.value_max, S.value_avg);"""

    try:
        with conn.cursor() as cur:
            cur.execute(query, msg['payload'])
            conn.commit()
    except psycopg2.DatabaseError as error:
        print(error)
        return


# Connect to mqtt-broker
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message
print("Connecting to mqtt-broker...")
mqtt_client.connect(MQTT_BROKER, MQTT_PORT, MQTT_KEEPALIVE)

if __name__ == '__main__':
    mqtt_client.loop_forever()
