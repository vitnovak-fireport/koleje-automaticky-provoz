import base64
import json
import time
from datetime import datetime

import paho.mqtt.client as mqtt

# ==========================================
# KONFIGURACE TLAČÍTKA (Mutelcor MTC-PB01)
# ==========================================
TLACITKO_OPCODE_ALARM = 0x01  # OpCode stisku v payloadu tlačítka (0x00 = heartbeat)
TLACITKO_ALARM_PAMET = 60  # s - opakovaný alarm se stejným Alarm ID během této doby je tentýž stisk


class Tlacitko:
    """LoRaWAN tlačítko pro kolemjdoucí přes MQTT The Things Industries: příjem stisků, potvrzování alarmů,
    počítání stisků a napětí baterie. Co stisk udělá, rozhoduje funkce pri_stisku."""

    def __init__(self, broker, port, user, password, topic, zapis, pri_stisku, casova_zona=None):
        """
        :param topic: topic uplinků, např. v3/<aplikace>@<tenant>/devices/+/up
        :param zapis: funkce pro zápis zprávy do logu
        :param pri_stisku: funkce zavolaná při novém stisku (opakování téhož alarmu ji nevolá)
        :param casova_zona: časové pásmo pro čas posledního stisku a počítání stisků po dnech
        """
        self.broker = broker
        self.port = port
        self.user = user
        self.password = password
        self.topic = topic
        self.zapis = zapis
        self.pri_stisku = pri_stisku
        self.casova_zona = casova_zona
        self.zapnute = all((broker, user, password, topic))  # Bez přístupových údajů je tlačítko vypnuté

        self.pripojeno = False
        self.stisky = 0  # Počet stisků dnes
        self._den = None
        self.posledni = None  # Čas posledního stisku HH:MM:SS
        self.baterie = None  # Napětí baterie tlačítka (V)
        self._alarmy = {}  # Alarm ID -> time.time() poslední zprávy s ním (tlačítko alarm opakuje)

        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message

    def pripojit(self):
        """Připojí se k MQTT tlačítka na pozadí. Bez proměnných TLACITKO_MQTT_* je tlačítko vypnuté."""
        if not self.zapnute:
            self.zapis("Tlačítko je vypnuté (chybí TLACITKO_MQTT_BROKER, _USER, _PASS nebo _TOPIC).")
            return
        self.client.username_pw_set(self.user, self.password)
        self.client.tls_set()
        self.client.connect_async(self.broker, self.port, 60)
        self.client.loop_start()

    def zapocitej_stisk(self):
        """Započítá stisk do dnešního počtu a zapamatuje si jeho čas (i stisk z webu)."""
        ted = datetime.now(self.casova_zona)
        if ted.date() != self._den:
            self._den, self.stisky = ted.date(), 0
        self.stisky += 1
        self.posledni = ted.strftime("%H:%M:%S")

    def on_connect(self, client, userdata, flags, reason_code, properties):
        self.zapis(f"Připojeno k MQTT tlačítka {self.broker} s kódem {reason_code}")
        self.pripojeno = not reason_code.is_failure
        client.subscribe(self.topic)

    def on_disconnect(self, client, userdata, flags, reason_code, properties):
        self.zapis(f"Odpojeno od MQTT tlačítka ({reason_code})")
        self.pripojeno = False

    def on_message(self, client, userdata, msg):
        """Uplink tlačítka. Stisk je OpCode Alarm, ostatní zprávy (heartbeat) jen zapíšeme.
        Prázdné uplinky (tlačítko je posílá po přijetí downlinku) přeskočíme."""
        try:
            uplink = json.loads(msg.payload.decode("utf-8"))["uplink_message"]
            data = base64.b64decode(uplink.get("frm_payload", ""))
            if not data:
                return
            napeti, opcode, alarm_id = self.dekoduj(data)
            if napeti is not None:
                self.baterie = napeti
            if opcode != TLACITKO_OPCODE_ALARM:
                self.zapis(f"Tlačítko poslalo zprávu bez stisku ({data.hex()}), baterie {napeti} V.")
                return
            if alarm_id is None:  # Bez Alarm ID nejde alarm potvrdit ani poznat opakování
                self.pri_stisku()
                return
            self._potvrd_alarm(msg.topic, alarm_id)
            ted = time.time()
            self._alarmy = {a: t for a, t in self._alarmy.items() if ted - t < TLACITKO_ALARM_PAMET}
            novy = alarm_id not in self._alarmy
            self._alarmy[alarm_id] = ted
            if novy:
                self.pri_stisku()
        except Exception as e:
            self.zapis(f"Chyba při zpracování zprávy tlačítka: {e}")

    @staticmethod
    def dekoduj(data):
        """
        Payload Mutelcor LoRaButton (podle dekodéru výrobce): verze (1 B), napětí v 10 mV (2 B), OpCode (1 B),
        u alarmu pak [maska tlačítek (1 B)] [Alarm ID (2 B)] [UID ...].
        Vrací (napětí ve V, OpCode, Alarm ID), chybějící údaje jako None.
        """
        napeti = int.from_bytes(data[1:3], "big") / 100 if len(data) >= 3 else None
        opcode = data[3] if len(data) >= 4 else None
        alarm_id = None
        if opcode == TLACITKO_OPCODE_ALARM:
            pos = 4
            if pos < len(data) and pos + 2 != len(data):
                pos += 1  # Maska stisknutých tlačítek
            if pos + 1 < len(data):
                alarm_id = int.from_bytes(data[pos:pos + 2], "big")
        return napeti, opcode, alarm_id

    def _potvrd_alarm(self, topic, alarm_id):
        """Potvrdí alarm downlinkem 02 60 AlarmID, jinak ho tlačítko každých 10 s opakuje.
        Downlink nahradí čekající ve frontě TTI, aby se při opakování nehromadily."""
        payload = base64.b64encode(bytes([0x02, 0x60]) + alarm_id.to_bytes(2, "big")).decode()
        topic_down = topic.removesuffix("/up") + "/down/replace"
        self.client.publish(topic_down, json.dumps({"downlinks": [{"f_port": 1, "frm_payload": payload}]}), qos=1)
