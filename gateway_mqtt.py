import time
import json
import subprocess 
import pymongo 
import firebase_admin
from firebase_admin import credentials, firestore
import paho.mqtt.client as mqtt
from datetime import datetime, timezone 
import threading
from concurrent.futures import ThreadPoolExecutor 

# ================= 1. CẤU HÌNH & KHỞI TẠO GLOBAL =================
SERVICE_ACCOUNT_KEY = 'serviceAccountKey.json'
MQTT_BROKER = "localhost" 
MQTT_PORT = 1883
MQTT_TOPIC_SUB = "home/+/+" 

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "smarthome_db"

# Cấu hình Hotspot (OPEN - KHÔNG PASS)
HOTSPOT_SSID = "SmartHome_Hub"
HOTSPOT_PASS = "" 
HOTSPOT_IP   = "10.42.0.1/24"
INTERFACE    = "wlan0"

# [FIX] Khởi tạo các đối tượng GLOBAL ở đây để mọi hàm đều dùng được
executor_db = ThreadPoolExecutor(max_workers=5) # Giảm worker I/O

# [QUAN TRỌNG] Khởi tạo Client MQTT ngay tại đây (Global Scope)
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

# --- BỘ NHỚ CACHE & RATE LIMIT ---
CACHED_AUTOMATIONS = {} 
CACHED_SCHEDULES = []   
CACHED_DEVICE_STATES = {} 
MANUAL_CONTROL_CACHE = {}
MANUAL_OVERRIDE_DURATION = 2 

CACHED_SENSORS = {} 
SAFETY_STATE = {}
SAFETY_MUTE_TIMEOUT = 10 
SAFETY_REPEAT_INTERVAL = 10 
CACHED_RFID_CARDS = {} 

LAST_DB_WRITE = {} 
DB_WRITE_INTERVAL = 3.0 

# ================= 2. DATABASE =================
print("[GATEWAY] Khởi động chế độ TURBO (Global Client)...")
try:
    mongo_client = pymongo.MongoClient(MONGO_URI)
    local_db = mongo_client[DB_NAME]
    local_db['sensor_readings'].create_index([("timestamp", pymongo.DESCENDING)])
    print(f"MongoDB OK")
except Exception as e: print(f"MongoDB Error: {e}")

try:
    if not firebase_admin._apps:
        cred = credentials.Certificate(SERVICE_ACCOUNT_KEY)
        firebase_admin.initialize_app(cred)
    db = firestore.client()
    print("Firebase OK")
except Exception as e: print(f"Firebase Error: {e}"); exit()

# ================= 3. QUẢN LÝ WIFI =================
def ensure_hotspot_active():
    try:
        subprocess.run(f"sudo nmcli connection delete '{HOTSPOT_SSID}'", shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        subprocess.run(f"sudo nmcli dev disconnect {INTERFACE}", shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        create_cmds = [
            f"sudo nmcli con add type wifi ifname {INTERFACE} con-name '{HOTSPOT_SSID}' autoconnect yes ssid '{HOTSPOT_SSID}'",
            f"sudo nmcli con modify '{HOTSPOT_SSID}' 802-11-wireless.mode ap",
            f"sudo nmcli con modify '{HOTSPOT_SSID}' 802-11-wireless.band bg",
            f"sudo nmcli con modify '{HOTSPOT_SSID}' 802-11-wireless.channel 6",
            f"sudo nmcli con modify '{HOTSPOT_SSID}' 802-11-wireless.cloned-mac-address permanent",
            f"sudo nmcli con modify '{HOTSPOT_SSID}' remove wifi-sec", # Mạng Open
            f"sudo nmcli con modify '{HOTSPOT_SSID}' ipv4.addresses {HOTSPOT_IP}",
            f"sudo nmcli con modify '{HOTSPOT_SSID}' ipv4.gateway {HOTSPOT_IP.split('/')[0]}",
            f"sudo nmcli con modify '{HOTSPOT_SSID}' ipv4.method manual",
            f"sudo nmcli con modify '{HOTSPOT_SSID}' connection.autoconnect-priority 100"
        ]
        for cmd in create_cmds: subprocess.run(cmd, shell=True, check=True)
        subprocess.run(f"sudo nmcli con up '{HOTSPOT_SSID}'", shell=True, check=True)
        print(f"Hotspot '{HOTSPOT_SSID}' đã sẵn sàng!")
    except Exception as e: print(f"Hotspot Error: {e}")

def check_wifi_status():
    try:
        cmd = "nmcli -t -f ACTIVE,SSID,MODE dev wifi | grep '^yes' | grep ':infrastructure'"
        output = subprocess.check_output(cmd, shell=True).decode().strip()
        if output: return output.split(':')[1], 'connected'
    except: pass
    try:
        subprocess.check_call(["ping", "-c", "1", "8.8.8.8"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return "Ethernet/Wired", 'connected'
    except: return "N/A", 'disconnected'

def update_firebase_wifi_status(ssid, status):
    try: db.collection('system_status').document('wifi').set({'current_ssid': ssid if ssid else 'N/A', 'status': status, 'last_check': firestore.SERVER_TIMESTAMP}, merge=True)
    except: pass

def _task_connect_wifi(ssid, password, doc_ref):
    cmd = f"sudo nmcli dev wifi connect '{ssid}' password '{password}'" if password else f"sudo nmcli dev wifi connect '{ssid}'"
    try:
        subprocess.run(cmd, shell=True, check=True, timeout=45)
        doc_ref.update({'status': 'success'})
        update_firebase_wifi_status(ssid, 'connected')
    except Exception as e:
        doc_ref.update({'status': 'failed', 'error': str(e)})
    finally:
        try: ensure_hotspot_active() 
        except: pass

# ================= 4. DB TASKS (THROTTLED & FIXED) =================
def _task_update_sensors(room_id, data, timestamp):
    try:
        # Firebase Realtime
        batch = db.batch()
        for k, v in data.items():
            ref = db.collection('rooms').document(room_id).collection('sensors').document(k)
            batch.set(ref, {'value': v, 'lastUpdate': timestamp, 'type': k}, merge=True)
        batch.commit()

        # Firebase History
        for k, v in data.items():
            if k in ['temperature', 'humidity', 'gas', 'co2', 'tvoc']:
                db.collection('sensor_readings').add({'roomId': room_id, 'type': k, 'value': v, 'timestamp': timestamp})

        # MongoDB History
        local_now = datetime.now()
        insert_list = []
        for k, v in data.items():
            if k in ['temperature', 'humidity', 'gas', 'co2', 'tvoc']:
                insert_list.append({'roomId': room_id, 'type': k, 'value': float(v), 'timestamp': local_now})
        if insert_list:
            local_db['sensor_readings'].insert_many(insert_list)
            
    except Exception as e: print(f"DB Error: {e}")

def _task_update_status(room_id, device_id, is_on, timestamp):
    try:
        db.collection('rooms').document(room_id).collection('devices').document(device_id).update({
            'isOn': is_on, 'status': 'online', 'details': "Đang bật" if is_on else "Đã tắt",
            'updatedAt': timestamp, 'lastControlSource': 'device_feedback'
        })
    except Exception as e: print(f"DB Status Error: {e}")

def _task_save_alert(data):
    try: db.collection('system_alerts').add(data)
    except: pass

# ================= 5. LOGIC TỰ ĐỘNG HÓA =================
class DeviceMapper:
    @staticmethod
    def get_target_id(room_id, device_type):
        mapping = {
            'kitchen_01': {'fan': 'fan_kt_1', 'light': 'light_kt_1'},
            'living_room_01': {'fan': 'fan_lv_1', 'light': 'light_lv_1', 'ac': 'fan_lv_1'},
            'bedroom_01': {'fan': 'fan_bd_1', 'light': 'light_bd_1', 'ac': 'fan_bd_1'}
        }
        return mapping.get(room_id, {}).get(device_type)

class AutomationController:
    @staticmethod
    def process(client, room_id, sensor_data):
        rule = CACHED_AUTOMATIONS.get(room_id)
        if not rule or not rule.get('enabled', False): return
        
        def try_execute(device_type, current_val, threshold):
            target_id = DeviceMapper.get_target_id(room_id, device_type)
            if not target_id: return
            for sched in CACHED_SCHEDULES:
                if sched.get('enabled') and sched.get('deviceId') == target_id: return
            last_manual = MANUAL_CONTROL_CACHE.get(target_id)
            if last_manual and (datetime.now() - last_manual).total_seconds() < MANUAL_OVERRIDE_DURATION: return
            
            should_on = current_val > threshold
            cache_key = f"{room_id}_{target_id}"
            
            if CACHED_DEVICE_STATES.get(cache_key) != should_on:
                client.publish(f"home/{room_id}/command", json.dumps({"device": target_id, "action": "turn_on" if should_on else "turn_off", "source": "automation"}))
                print(f"[AUTO-FAST] {room_id}: {device_type} {current_val} > {threshold}")

        if 'temperature' in sensor_data:
            val = float(sensor_data['temperature'])
            if rule.get('acThreshold'): try_execute('ac', val, float(rule['acThreshold']))
            elif rule.get('fanThreshold'): try_execute('fan', val, float(rule['fanThreshold']))
            if rule.get('lightThreshold'): try_execute('light', val, float(rule['lightThreshold']))

        if 'gas' in sensor_data and rule.get('gasThreshold'):
            if float(sensor_data['gas']) > float(rule['gasThreshold']):
                target_id = DeviceMapper.get_target_id(room_id, 'fan')
                if target_id: client.publish(f"home/{room_id}/command", json.dumps({"device": target_id, "action": "turn_on", "source": "safety"}))

# ================= 6. MQTT MESSAGE HANDLER =================
def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode(); topic = msg.topic; data = json.loads(payload)
        parts = topic.split('/'); room_id = parts[1]; category = parts[2]
        ts_fb = firestore.SERVER_TIMESTAMP 

        if category == 'sensors':
            if room_id not in CACHED_SENSORS: CACHED_SENSORS[room_id] = {}
            CACHED_SENSORS[room_id].update(data)
            
            # [FAST] Chạy Automation ngay
            AutomationController.process(client, room_id, data)
            
            # [SLOW] Ghi DB có giới hạn
            now_ts = time.time()
            last_write = LAST_DB_WRITE.get(room_id, 0)
            if now_ts - last_write > DB_WRITE_INTERVAL:
                LAST_DB_WRITE[room_id] = now_ts
                executor_db.submit(_task_update_sensors, room_id, data, ts_fb)
            
        elif category == 'status':
            dev_id = data.get('deviceId')
            if dev_id:
                CACHED_DEVICE_STATES[f"{room_id}_{dev_id}"] = data.get('isOn', False)
                executor_db.submit(_task_update_status, room_id, dev_id, data.get('isOn', False), ts_fb)
                
        elif category == 'alert':
             executor_db.submit(_task_save_alert, {'roomId': room_id, 'type': data.get('type'), 'message': data.get('message'), 'level': 'critical', 'timestamp': ts_fb})
             
        elif category == 'auth':
            uid = str(data.get('cardUid') or data.get('fingerprintId') or data.get('uid'))
            is_valid = False; owner_name = "Khách vãng lai"
            if uid in CACHED_RFID_CARDS:
                user_info = CACHED_RFID_CARDS[uid]
                if user_info.get('isActive', True): is_valid = True; owner_name = user_info.get('ownerName', 'Không tên')
            
            if is_valid:
                print(f"[ACCESS] {room_id}: OK -> {owner_name}")
                client.publish(f"home/{room_id}/command", json.dumps({"action": "open_door", "message": f"Hello {owner_name}"}))
            else:
                print(f"[ACCESS] {room_id}: DENIED -> {uid}")
                client.publish(f"home/{room_id}/command", json.dumps({"action": "access_denied"}))
            executor_db.submit(lambda: db.collection('access_logs').add({'roomId': room_id, 'uid': uid, 'userName': owner_name, 'action': 'open_door' if is_valid else 'attempt_failed', 'success': is_valid, 'timestamp': ts_fb}))

    except Exception as e: print(f"Msg Error: {e}")

# ================= 7. WEB CONTROL HANDLER =================
def process_web_control(client, change_doc):
    try:
        d = change_doc.to_dict()
        if d.get('lastControlSource') == 'device_feedback': return
        doc_id = change_doc.id; rid = change_doc.reference.parent.parent.id
        web_on = d.get('isOn', False)
        
        real_on = CACHED_DEVICE_STATES.get(f"{rid}_{doc_id}")
        if real_on is None or web_on != real_on:
            print(f"[WEB-CMD] {rid} -> {doc_id}: {web_on}")
            MANUAL_CONTROL_CACHE[doc_id] = datetime.now()
            client.publish(f"home/{rid}/command", json.dumps({"device": doc_id, "action": "turn_on" if web_on else "turn_off"}))
    except Exception as e: print(f"Web Control Error: {e}")

# ================= 8. FIREBASE LISTENERS =================
def on_device_control_change(col, changes, rt):
    for c in changes:
        if c.type.name == 'MODIFIED':
            # [FIX] client giờ đã là biến toàn cục, có thể dùng ở bất kỳ đâu
            executor_db.submit(process_web_control, client, c.document)

def on_automations_change(col, changes, rt):
    for c in changes:
        data = c.document.to_dict()
        if not data: continue
        rid = data.get('roomId')
        if not rid: continue
        if c.type.name == 'REMOVED':
            if rid in CACHED_AUTOMATIONS: del CACHED_AUTOMATIONS[rid]
        else: CACHED_AUTOMATIONS[rid] = data
        print(f"Auto Rule Update: {rid}")

def on_schedules_change(col, changes, rt):
    global CACHED_SCHEDULES
    CACHED_SCHEDULES = [{'id': d.id, **d.to_dict()} for d in col]

def on_wifi_command_change(doc, changes, rt):
    for c in changes:
        if c.type.name == 'MODIFIED':
            d = c.document.to_dict()
            if d.get('status') == 'pending':
                executor_db.submit(_task_connect_wifi, d.get('ssid'), d.get('password'), c.document.reference)

def on_rfid_cards_change(col, changes, rt):
    for c in changes:
        doc_id = c.document.id; data = c.document.to_dict()
        if c.type.name == 'REMOVED':
            if doc_id in CACHED_RFID_CARDS: del CACHED_RFID_CARDS[doc_id]
            # [FIX] client toàn cục
            client.publish("home/entrance_01/command", json.dumps({"action": "delete_user", "uid": doc_id}))
        else:
            if data.get('isActive', True): CACHED_RFID_CARDS[doc_id] = data
            elif doc_id in CACHED_RFID_CARDS: del CACHED_RFID_CARDS[doc_id]

def on_register_command_change(doc, changes, rt):
    for c in changes:
        if c.type.name == 'MODIFIED' and c.document.to_dict().get('status') == 'waiting':
            client.publish("home/entrance_01/command", json.dumps({"action": "enroll"}))
            c.document.reference.update({'status': 'processing'})

def on_alert_change(col, changes, rt):
    for c in changes:
        data = c.document.to_dict()
        if c.type.name == 'MODIFIED' and data.get('isResolved') == True:
            room = data.get('roomId')
            if room: 
                client.publish(f"home/{room}/command", json.dumps({"action": "mute_alarm"}))
                if room not in SAFETY_STATE: SAFETY_STATE[room] = {}
                SAFETY_STATE[room]['muted'] = True; SAFETY_STATE[room]['mute_time'] = datetime.now()

def network_monitor_worker():
    while True:
        try:
            ssid, status = check_wifi_status()
            update_firebase_wifi_status(ssid, status)
        except: pass
        time.sleep(30) 

def safety_watchdog_loop(mqtt_client):
    while True:
        try:
            now = datetime.now()
            room_id = 'kitchen_01'
            sensors = CACHED_SENSORS.get(room_id, {})
            safety = SAFETY_STATE.get(room_id, {'muted': False, 'mute_time': None, 'last_alert': None})
            rule = CACHED_AUTOMATIONS.get(room_id, {})
            if rule:
                gas_threshold = float(rule.get('gasThreshold', 600)) 
                current_gas = float(sensors.get('gas', 0))
                is_fire = sensors.get('fire_detected', False)
                if (current_gas > gas_threshold) or is_fire:
                    if safety.get('muted'):
                        if (now - safety['mute_time']).total_seconds() > SAFETY_MUTE_TIMEOUT:
                            safety['muted'] = False
                            mqtt_client.publish(f"home/{room_id}/command", json.dumps({"action": "unmute"})) 
                    else:
                        last = safety.get('last_alert')
                        if not last or (now - last).total_seconds() > SAFETY_REPEAT_INTERVAL:
                            executor_db.submit(_task_save_alert, {'roomId': room_id, 'type': 'fire' if is_fire else 'gas', 'message': "NGUY HIỂM!", 'level': 'critical', 'timestamp': firestore.SERVER_TIMESTAMP})
                            safety['last_alert'] = now
                SAFETY_STATE[room_id] = safety
            time.sleep(1)
        except: time.sleep(5)

def scheduler_worker(mqtt_client):
    while True:
        try:
            now = datetime.now()
            current_hhmm = now.strftime("%H:%M")
            for sched in list(CACHED_SCHEDULES):
                if (sched.get('enabled') and sched.get('time') == current_hhmm and sched.get('deviceId')):
                    mqtt_client.publish(f"home/{sched['roomId']}/command", json.dumps({"device": sched['deviceId'], "action": sched['action'], "source": "schedule"}))
                    MANUAL_CONTROL_CACHE[sched['deviceId']] = datetime.now()
                    executor_db.submit(lambda sid: db.collection('schedules').document(sid).delete(), sched['id'])
                    CACHED_SCHEDULES.remove(sched)
            time.sleep(1.0 - (time.time() % 1.0))
        except: time.sleep(1)

# ================= 9. MAIN EXECUTION =================
def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        print("MQTT Broker Connected!")
        client.subscribe(MQTT_TOPIC_SUB)
    else: print(f"MQTT Connection Failed: {reason_code}")

if __name__ == "__main__":
    ensure_hotspot_active()

    # Gán callback vào client toàn cục
    client.on_connect = on_connect
    client.on_message = on_message

    # Listeners
    rooms = ['living_room_01', 'entrance_01', 'kitchen_01', 'bedroom_01']
    for r in rooms: db.collection('rooms').document(r).collection('devices').on_snapshot(on_device_control_change)

    db.collection('automations').on_snapshot(on_automations_change)
    db.collection('schedules').on_snapshot(on_schedules_change)
    db.collection('system_alerts').order_by('timestamp', direction=firestore.Query.DESCENDING).limit(5).on_snapshot(on_alert_change)
    db.collection('commands').document('wifi_setup').on_snapshot(on_wifi_command_change)
    db.collection('commands').document('entrance_register').on_snapshot(on_register_command_change)
    db.collection('rfid_cards').on_snapshot(on_rfid_cards_change)

    # Threads
    threading.Thread(target=scheduler_worker, args=(client,), daemon=True).start()
    threading.Thread(target=safety_watchdog_loop, args=(client,), daemon=True).start()
    threading.Thread(target=network_monitor_worker, daemon=True).start()

    try: 
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        client.loop_forever()
    except KeyboardInterrupt: 
        print("Stopping...")
        executor_db.shutdown(wait=False)