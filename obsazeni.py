import json
import time

import paho.mqtt.client as mqtt


class Obsazenost:
    """Detekce obsazení segmentů kolejiště přes MQTT (zprávy {"segment_blocked": true/false}
    na topicu .../<číslo segmentu>)."""

    def __init__(self, broker, port, topic, zapis, vychozi):
        """
        :param topic: odebíraný topic, např. koleje/segment/+ (+ znamená jakékoliv číslo segmentu)
        :param zapis: funkce pro zápis zprávy do logu
        :param vychozi: předpokládané obsazení segmentů před první zprávou, např. {1: False, 3: True}
        """
        self.broker = broker
        self.port = port
        self.topic = topic
        self.zapis = zapis

        self.segmenty = dict(vychozi)  # Číslo segmentu -> obsazený? (slovník se jen mění, nenahrazuje)
        self.zmeneno = {segment: 0.0 for segment in vychozi}  # Čas poslední změny stavu segmentu (time.time())
        self.pripojeno = False

        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message

    def pripojit(self):
        """Připojí se k MQTT na pozadí (při výpadku se klient sám znovu připojuje)."""
        self.client.connect_async(self.broker, self.port, 60)
        self.client.loop_start()

    def odpojit(self):
        self.client.loop_stop()

    def nastav(self, segment_id, obsazeno):
        """Zapíše hlášený stav segmentu a při změně si poznamená její čas."""
        if self.segmenty.get(segment_id) != obsazeno:
            self.zmeneno[segment_id] = time.time()
        self.segmenty[segment_id] = obsazeno

    def on_connect(self, client, userdata, flags, reason_code, properties):
        self.zapis(f"Připojeno k MQTT {self.broker} s kódem {reason_code}")
        self.pripojeno = not reason_code.is_failure
        client.subscribe(self.topic)

    def on_disconnect(self, client, userdata, flags, reason_code, properties):
        self.zapis(f"Odpojeno od MQTT ({reason_code})")
        self.pripojeno = False

    def on_message(self, client, userdata, msg):
        """Zpracuje příchozí JSON zprávy o obsazení z MQTT."""
        try:
            # Příklad topicu: koleje/segment/1
            segment_id = int(msg.topic.split('/')[-1])
            payload = json.loads(msg.payload.decode('utf-8'))
            self.nastav(segment_id, payload.get("segment_blocked", False))
            # Odkomentujte pro detailní debug výpis
            # print(f"Senzor: Segment {segment_id} -> {self.segmenty[segment_id]}")
        except Exception as e:
            self.zapis(f"Chyba při zpracování zprávy MQTT: {e}")
