import paho.mqtt.client as mqtt
import requests
from datetime import date, datetime
import json
import os
from hashlib import md5
from datetime import timedelta
import sys

CACHE_DIR = "cache"
CACHE_DURATION = timedelta(hours=24)
MQTT_BROKER = os.environ.get('MQTT_BROKER', 'localhost')
MQTT_PORT = int(os.environ.get('MQTT_PORT', '1883'))
MQTT_USERNAME = os.environ.get('MQTT_USERNAME', None)
MQTT_PASSWORD = os.environ.get('MQTT_PASSWORD', None)
MQTT_CA = os.environ.get('MQTT_CA')

# Cache functions remain the same
def get_cache_path(url, params):
    if not os.path.exists(CACHE_DIR):
        os.makedirs(CACHE_DIR)
    cache_key = f"{url}_{json.dumps(params, sort_keys=True)}"
    cache_filename = md5(cache_key.encode()).hexdigest() + ".json"
    return os.path.join(CACHE_DIR, cache_filename)

def get_cached_response(cache_path):
    if not os.path.exists(cache_path):
        return None
    
    with open(cache_path, 'r') as f:
        cache_data = json.load(f)
        
    if datetime.fromisoformat(cache_data['timestamp']) + CACHE_DURATION < datetime.now():
        return None
        
    return cache_data['data']

def save_to_cache(cache_path, data):
    cache_data = {
        'timestamp': datetime.now().isoformat(),
        'data': data
    }
    with open(cache_path, 'w') as f:
        json.dump(cache_data, f)

def get_holiday_name_for_date(check_date):
    first_day = date(check_date.year, check_date.month, 1)
    if check_date.month == 12:
        last_day = date(check_date.year + 1, 1, 1)
    else:
        last_day = date(check_date.year, check_date.month + 1, 1)

    url = "https://openholidaysapi.org/PublicHolidays"
    params = {
        "countryIsoCode": "DE",
        "languageIsoCode": "DE",
        "subdivisionCode": "DE-BY",
        "validFrom": first_day.strftime("%Y-%m-%d"),
        "validTo": last_day.strftime("%Y-%m-%d")
    }

    cache_path = get_cache_path(url, params)
    cached_data = get_cached_response(cache_path)
    
    if cached_data is not None:
        holidays = cached_data
    else:
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            holidays = response.json()
            save_to_cache(cache_path, holidays)
        except requests.RequestException:
            return None

    def is_holiday_today(holiday, date_str):
        start = datetime.strptime(holiday['startDate'], '%Y-%m-%d').date()
        end = datetime.strptime(holiday['endDate'], '%Y-%m-%d').date()
        check_date = datetime.strptime(date_str, '%Y-%m-%d').date()
        return start <= check_date <= end

    date_str = check_date.strftime("%Y-%m-%d")
    for holiday in holidays:
        if is_holiday_today(holiday, date_str):
            german_name = next((name['text'] for name in holiday['name'] if name['language'] == 'DE'), '')
            return {
                'startDate': holiday['startDate'],
                'endDate': holiday['endDate'],
                'name': german_name
            }
    return None

def on_connect(client, userdata, flags, reason_code, properties):
    if not reason_code.is_failure:
        print("Connected to MQTT broker")
        publish_holiday_info(client)
    else:
        print(f"Failed to connect to MQTT broker: {reason_code}")
        sys.exit(1)

pending = set()

def on_publish(client, userdata, mid, reason_code, properties):
    pending.discard(mid)
    if not pending:
        client.disconnect()

def publish(client, topic, payload):
    pending.add(client.publish(topic, payload, qos=1, retain=True).mid)

def publish_holiday_info(client):
    today = datetime.today().date()
    holiday = get_holiday_name_for_date(today)

    if holiday:
        publish(client, "events/holiday/name", holiday['name'])
        full_info = {
            "current_date": today.isoformat(),
            "current_holiday": holiday
        }
        publish(client, "events/holiday/full", json.dumps(full_info))
        print("Holiday information published successfully")
    else:
        # "0" clears the holiday for Loxone
        publish(client, "events/holiday/name", "0")
        publish(client, "events/holiday/full", json.dumps({"date": today.isoformat(), "holiday": None}))
        print("No holiday found for today")

def main():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5)
    client.on_connect = on_connect
    client.on_publish = on_publish

    try:
        if MQTT_USERNAME is not None and MQTT_PASSWORD is not None:
            print(f"Setting MQTT credentials: {MQTT_USERNAME}")
            client.username_pw_set(MQTT_USERNAME, str(MQTT_PASSWORD))

        if MQTT_CA:
            client.tls_set(ca_certs=MQTT_CA)

        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        client.loop_forever()
    except Exception as e:
        print(f"Error connecting to MQTT broker: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
