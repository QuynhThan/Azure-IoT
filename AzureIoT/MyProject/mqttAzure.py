from paho.mqtt import client as mqtt
import time
import ssl
import calendar
import datetime
import json
import random
import requests


def on_subscribe(client, userdata, mid, granted_qos):
    print('Subscribed for m' + str(mid))


def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))


def on_message(client, userdata, message):
    print("Received message '" + str(message.payload) + "' on topic '" +
          message.topic + "' with QoS " + str(message.qos))


def on_log(client, userdata, level, buf):
    print("log: ", buf)


def getTempAndHumid(x):
    # Lấy nhiệt độ từ Arduino về

    # api trả về kết quả là nhiệt độ mới nhất của arduino
    api_thingspeak = 'https://api.thingspeak.com/channels/1872197/feeds.json?api_key=WROWFQDPD8S8IS3Y&results=1'

    #api_thingspeak = 'https://api.thingspeak.com/channels/1872197/fields/1.json?results=2'
    temperature = requests.get(api_thingspeak).json()

    last_id = temperature['channel']['last_entry_id']

    lastest_temperature = temperature['feeds'][0]
    messageId = last_id
    dt = datetime.datetime.now()
    device_name = "ESP8266 and DHT11"
    temperature = (float(lastest_temperature['field1'])*1.8) + 32
    humidity = float(lastest_temperature['field2'])
    if x == 1:
        humidity += 100
    tmp = {'id': str(last_id),
           'datetime': str(dt),
           'messageId': str(messageId),
           'deviceId': str(device_id),
           'temperature': str(temperature),
           'humidity': str(humidity), }
    return tmp


device_id = "cuoikyDevice1"  # Add device id
iot_hub_name = "cuoikyIothub"  # Add iot hub name
# Create a sas token of iot hub device
sas_token = "SharedAccessSignature sr=cuoikyIothub.azure-devices.net%2Fdevices%2FcuoikyDevice1&sig=Ukj5dYfFI%2B4QkpeAj7SrL%2BSeRbrtjJlQm0jF%2Bs1pEZ4%3D&se=1672152561"  # Add sas token string
client = mqtt.Client(client_id=device_id,
                     protocol=mqtt.MQTTv311,  clean_session=False)
client.on_log = on_log
client.tls_set_context(context=None)

# Set up client credentials
username = "{}.azure-devices.net/{}/api-version=2018-06-30".format(
    iot_hub_name, device_id)
client.username_pw_set(username=username, password=sas_token)

# Connect to the Azure IoT Hub
client.on_connect = on_connect
client.connect(iot_hub_name+".azure-devices.net", port=8883)

#messageId = 0
# Publish ========== Temp and humid get in thinkspeaks from arduino
time.sleep(1)
for x in range(100):
    exp = datetime.datetime.utcnow()
    abcstring1 = getTempAndHumid(x)
    data_out1 = json.dumps(abcstring1)
    client.publish("devices/{device_id}/messages/events/".format(
        device_id=device_id), payload=data_out1, qos=1, retain=False)
    print("Publishing on devices/" + device_id + "/messages/events/", data_out1)
    time.sleep(20)

# Subscribe
client.on_message = on_message
client.on_subscribe = on_subscribe
client.subscribe(
    "devices/{device_id}/messages/devicebound/#".format(device_id=device_id))

client.loop_forever()
