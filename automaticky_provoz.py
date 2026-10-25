import paho.mqtt.client as mqtt
import base64
import json
import os
import threading
import time
from collections import deque
from datetime import datetime
from zoneinfo import ZoneInfo

from z21 import Z21, NAPAJENI_ZAPNUTO

# ==========================================
# 1. KONFIGURACE (Upravte před spuštěním, nebo nastavte proměnnými prostředí)
# ==========================================
MQTT_BROKER = os.environ.get("MQTT_BROKER", "mqtt.drozdak.cz")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_TOPIC = os.environ.get("MQTT_TOPIC", "koleje/segment/+")  # + znamená jakékoliv číslo segmentu

# LoRaWAN tlačítko pro kolemjdoucí (Mutelcor MTC-PB01) přes MQTT The Things Industries.
# Přístupové údaje jen z proměnných prostředí - bez nich je tlačítko vypnuté.
TLACITKO_MQTT_BROKER = os.environ.get("TLACITKO_MQTT_BROKER", "")
TLACITKO_MQTT_PORT = int(os.environ.get("TLACITKO_MQTT_PORT", "8883"))  # TLS
TLACITKO_MQTT_USER = os.environ.get("TLACITKO_MQTT_USER", "")
TLACITKO_MQTT_PASS = os.environ.get("TLACITKO_MQTT_PASS", "")  # API klíč TTI (čtení uplinků, zápis downlinků)
TLACITKO_MQTT_TOPIC = os.environ.get("TLACITKO_MQTT_TOPIC", "")  # Topic uplinků, např. v3/<aplikace>@<tenant>/devices/+/up
TLACITKO_ZAPNUTE = all((TLACITKO_MQTT_BROKER, TLACITKO_MQTT_USER, TLACITKO_MQTT_PASS, TLACITKO_MQTT_TOPIC))
TLACITKO_OPCODE_ALARM = 0x01  # OpCode stisku v payloadu tlačítka (0x00 = heartbeat)
TLACITKO_ALARM_PAMET = 60  # s - opakovaný alarm se stejným Alarm ID během této doby je tentýž stisk
HOUKANI_DELKA = 2  # s - jak dlouho houkačka zní
HOUKANI_PAUZA = 10  # s - nejkratší rozestup houkání z tlačítka

Z21_IP = os.environ.get("Z21_IP", "192.168.0.111")
Z21_PORT = int(os.environ.get("Z21_PORT", "21105"))  # Časování a protokol Z21 jsou v z21.py

NAPAJENI_PO_OBNOVENI = 3  # s - po obnovení napájení počkat, než se srovnají hlášení obsazenosti

# Výhybky ve stanici a jejich nastavení pro výjezd z jednotlivých staničních kolejí (0 = rovně, 1 = odbočka)
VYHYBKY = (5, 6)
CESTY = {
    3: {5: 1, 6: 0},  # Výjezd z koleje 3
    4: {5: 0, 6: 1},  # Výjezd z koleje 4
}

# Časové pásmo pro časy v záznamu a u výhybek (nezávisle na nastavení systému, Raspberry Pi bývá v UTC)
CASOVA_ZONA = ZoneInfo(os.environ.get("CASOVA_ZONA", "Europe/Prague"))

# Ochrana proti falešnému uvolnění úseku (na výhybce)
REQUIRED_FALSE_COUNT = 3  # Počet nutných po sobě jdoucích False stavů ze segmentů 1 a 2

# Lokomotivy podle DCC adresy (ve webu se vybírají ze seznamu) a čísla funkcí jejich dekodérů
LOKOMOTIVY = {
    3: {"nazev": "Bardotka", "funkce": {"houkacka": 10}},
    4: {"nazev": "Bangle", "funkce": {"houkacka": 10}},
    5: {"nazev": "Brejlovec", "funkce": {"houkacka": 10}},
    6: {"nazev": "Banán", "funkce": {"houkacka": 10}},
    7: {"nazev": "Vectron", "funkce": {"houkacka": 3}},
}

# Výchozí nastavení provozu - ve webové aplikaci se dá měnit za běhu
VYCHOZI_NASTAVENI = {
    # Lokomotivy
    "loco_seg3": 6,         # Adresa lokomotivy, která začíná na segmentu 3
    "loco_seg4": 3,         # Adresa lokomotivy, která začíná na segmentu 4
    "speed_seg3": 127,      # Rychlost (0-127)
    "speed_seg4": 127,      # Rychlost (0-127)
    "rychlost_vjezdu": 60,  # Rychlost při vjezdu do stanice

    # Provoz: "nepretrzity" = vlaky se střídají pořád, "na_tlacitko" = další vlak vyjede až po stisku tlačítka
    "rezim": "nepretrzity",

    # Zastávka na předělu segmentů 1 a 2 (jen pro vlaky, které ze stanice vjedou nejdříve do segmentu 1)
    # "ano" = vlaky zastaví, "ne" = projedou, "na_znameni" = zastaví, jen když někdo stiskne tlačítko
    "zastavka": "ano" if os.environ.get("ZASTAVKA_POVOLENA", "1") == "1" else "ne",
    "zastavka_cekani": 15,             # Doba stání na zastávce (s)
    "zastavka_rychlost_dojezdu": 30,   # Plíživá rychlost od vjezdu čela do segmentu 2 až do zastavení (0-127)
}


class Zastaveno(Exception):
    """Provoz byl zastaven uživatelem."""


class Provoz:
    """Automatický provoz dvou vlaků mezi stanicí (segmenty 3 a 4) a okruhem (segmenty 1 a 2)."""

    def __init__(self, nastaveni=None):
        self.nastaveni = dict(VYCHOZI_NASTAVENI)
        self.nastaveni.update(nastaveni or {})

        # Globální stav segmentů (aktualizováno přes MQTT)
        self.segments = {
            1: False,
            2: False,
            3: True,  # Výchozí stav - předpokládáme, že tam vlaky stojí
            4: True
        }
        # Čas poslední změny stavu každého segmentu (time.time())
        self.segment_changed_at = {1: 0.0, 2: 0.0, 3: 0.0, 4: 0.0}

        # Naučené charakteristiky vlaků podle adresy lokomotivy:
        # smer_pres_seg1 (vjíždí nejdříve do segmentu 1?)
        self.uceni = {}

        self.faze = "Zastaveno"
        self.log = deque(maxlen=200)
        self.mqtt_pripojeno = False
        self.zastavit_ve_stanici = False

        # Jízda na okruh a zastávka na znamení (čte je i obsluha tlačítka z vlákna MQTT)
        self.jedouci_loco = None  # Lokomotiva, která vyjela ze stanice a ještě se nevrátila
        self.zastavka_rozhodnuta = False  # Čelo vlaku už vjelo do segmentu 2 - o zastavení je rozhodnuto
        self.zastavka_pozadana = False  # Někdo tlačítkem požádal o zastavení na znamení
        self._zastavka_zamek = threading.Lock()
        self.navrat = {}  # Lokomotiva -> time.time() posledního návratu do stanice
        self.ceka_na_tlacitko = False  # Oba vlaky stojí ve stanici a čekají na stisk (režim na tlačítko)
        self._vyjezd = threading.Event()  # Stisk tlačítka, který vypraví další vlak

        # Tlačítko pro kolemjdoucí
        self.tlacitko_pripojeno = False
        self.tlacitko_stisky = 0  # Počet stisků dnes
        self._tlacitko_den = None
        self.tlacitko_posledni = None  # Čas posledního stisku HH:MM:SS
        self.tlacitko_baterie = None  # Napětí baterie tlačítka (V)
        self._alarmy = {}  # Alarm ID -> time.time() poslední zprávy s ním (tlačítko alarm opakuje)
        self._houkani_cas = 0.0

        self._stop = threading.Event()
        self._vlakno = None

        # Centrála Z21 - povely lokomotivám a výhybkám, hlášení výhybek a napájení kolejiště
        self.z21 = Z21(Z21_IP, Z21_PORT, VYHYBKY, self.zapis, nazev=self.nazev, ted=self.ted)

        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message

        self.tlacitko_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.tlacitko_client.on_connect = self.tlacitko_on_connect
        self.tlacitko_client.on_disconnect = self.tlacitko_on_disconnect
        self.tlacitko_client.on_message = self.tlacitko_on_message

    def zapis(self, zprava):
        """Vypíše zprávu do konzole a uloží ji do logu pro webové rozhraní."""
        print(zprava)
        self.log.append((self.ted(), zprava.strip()))

    @staticmethod
    def ted():
        """Aktuální čas HH:MM:SS v časovém pásmu CASOVA_ZONA."""
        return datetime.now(CASOVA_ZONA).strftime("%H:%M:%S")

    @staticmethod
    def nazev(loco):
        """Jméno lokomotivy pro výpisy, např. 'Banán (6)'."""
        return f"{LOKOMOTIVY[loco]['nazev']} ({loco})" if loco in LOKOMOTIVY else f"Lokomotiva {loco}"

    def _cekej(self, sekundy):
        """
        Jako time.sleep, ale při zastavení provozu okamžitě skončí.
        Když je kolejiště bez proudu, počká na obnovení napájení (provoz se pozastaví).
        """
        if self._stop.wait(sekundy):
            raise Zastaveno()
        if self.z21.napajeni not in (None, NAPAJENI_ZAPNUTO):
            self._pozastav()

    def _pozastav(self):
        """Počká na obnovení napájení kolejiště a pak lokomotivám znovu pošle jejich rychlosti."""
        faze = self.faze
        self.faze = f"Pozastaveno - kolejiště: {self.z21.napajeni}"
        self.zapis(f"Kolejiště je bez proudu ({self.z21.napajeni}), provoz čeká na obnovení napájení.")
        while self.z21.napajeni != NAPAJENI_ZAPNUTO:
            if self._stop.wait(0.2):
                raise Zastaveno()
        self.zapis(f"Napájení kolejiště obnoveno, za {NAPAJENI_PO_OBNOVENI} s pokračuji.")
        if self._stop.wait(NAPAJENI_PO_OBNOVENI):
            raise Zastaveno()
        # Nouzové zastavení v Z21 vynuluje rychlosti lokomotiv, proto je pošleme znovu
        for loco in (self.nastaveni["loco_seg3"], self.nastaveni["loco_seg4"]):
            if loco in self.z21.rychlosti:
                self.z21.set_loco_speed(loco, self.z21.rychlosti[loco], self.z21.smery.get(loco, 1))
        self.faze = faze

    # ==========================================
    # 2. POVELY PRO Z21 (protokol je v z21.py)
    # ==========================================
    def set_turnout(self, address, state):
        """Přepne výhybku a počká, až přestavení Z21 potvrdí. Bez potvrzení provoz skončí chybou -
        vlak se nesmí rozjet přes nepřestavenou výhybku."""
        povel = self.z21.set_turnout(address, state)
        while not povel.vyrizeno.is_set():
            self._cekej(0.05)
        if not povel.potvrzeno:
            raise RuntimeError(f"Z21 nepotvrdila přestavení výhybky {address}")

        self._cekej(1)

    def cesta(self):
        """Na kterou staniční kolej jsou výhybky nastavené (3, 4), nebo None."""
        for kolej, polohy in CESTY.items():
            if all(self.z21.vyhybky[a]["stav"] == stav for a, stav in polohy.items()):
                return kolej
        return None

    def zahoukej(self, loco):
        """Krátce zahouká lokomotivou (funkce "houkacka" v LOKOMOTIVY). Vrací False, pokud nezahoukala."""
        funkce = LOKOMOTIVY.get(loco, {}).get("funkce", {}).get("houkacka")
        if funkce is None:
            self.zapis(f"{self.nazev(loco)} nemá houkačku.")
            return False
        if time.time() - self._houkani_cas < HOUKANI_PAUZA:
            return False
        self._houkani_cas = time.time()
        self.z21.set_loco_function(loco, funkce, True)
        threading.Timer(HOUKANI_DELKA, self.z21.set_loco_function, (loco, funkce, False)).start()
        return True

    # ==========================================
    # 3. MQTT CALLBACKY
    # ==========================================
    def pripojit_mqtt(self):
        """Připojí se k MQTT na pozadí (při výpadku se klient sám znovu připojuje)."""
        self.client.connect_async(MQTT_BROKER, MQTT_PORT, 60)
        self.client.loop_start()

    def on_connect(self, client, userdata, flags, reason_code, properties):
        self.zapis(f"Připojeno k MQTT {MQTT_BROKER} s kódem {reason_code}")
        self.mqtt_pripojeno = not reason_code.is_failure
        client.subscribe(MQTT_TOPIC)

    def on_disconnect(self, client, userdata, flags, reason_code, properties):
        self.zapis(f"Odpojeno od MQTT ({reason_code})")
        self.mqtt_pripojeno = False

    def on_message(self, client, userdata, msg):
        """Zpracuje příchozí JSON zprávy o obsazení z MQTT."""
        try:
            # Příklad topicu: koleje/segment/1
            segment_id = int(msg.topic.split('/')[-1])
            payload = json.loads(msg.payload.decode('utf-8'))
            is_blocked = payload.get("segment_blocked", False)

            if self.segments.get(segment_id) != is_blocked:
                self.segment_changed_at[segment_id] = time.time()
            self.segments[segment_id] = is_blocked
            # Odkomentujte pro detailní debug výpis
            # print(f"Senzor: Segment {segment_id} -> {is_blocked}")
        except Exception as e:
            self.zapis(f"Chyba při zpracování zprávy MQTT: {e}")

    # ==========================================
    # 3b. TLAČÍTKO PRO KOLEMJDOUCÍ (LoRaWAN přes MQTT The Things Industries)
    # ==========================================
    def pripojit_tlacitko(self):
        """Připojí se k MQTT tlačítka na pozadí. Bez proměnných TLACITKO_MQTT_* je tlačítko vypnuté."""
        if not TLACITKO_ZAPNUTE:
            self.zapis("Tlačítko je vypnuté (chybí TLACITKO_MQTT_BROKER, _USER, _PASS nebo _TOPIC).")
            return
        self.tlacitko_client.username_pw_set(TLACITKO_MQTT_USER, TLACITKO_MQTT_PASS)
        self.tlacitko_client.tls_set()
        self.tlacitko_client.connect_async(TLACITKO_MQTT_BROKER, TLACITKO_MQTT_PORT, 60)
        self.tlacitko_client.loop_start()

    def tlacitko_on_connect(self, client, userdata, flags, reason_code, properties):
        self.zapis(f"Připojeno k MQTT tlačítka {TLACITKO_MQTT_BROKER} s kódem {reason_code}")
        self.tlacitko_pripojeno = not reason_code.is_failure
        client.subscribe(TLACITKO_MQTT_TOPIC)

    def tlacitko_on_disconnect(self, client, userdata, flags, reason_code, properties):
        self.zapis(f"Odpojeno od MQTT tlačítka ({reason_code})")
        self.tlacitko_pripojeno = False

    def tlacitko_on_message(self, client, userdata, msg):
        """Uplink tlačítka. Stisk je OpCode Alarm, ostatní zprávy (heartbeat) jen zapíšeme.
        Prázdné uplinky (tlačítko je posílá po přijetí downlinku) přeskočíme."""
        try:
            uplink = json.loads(msg.payload.decode("utf-8"))["uplink_message"]
            data = base64.b64decode(uplink.get("frm_payload", ""))
            if not data:
                return
            napeti, opcode, alarm_id = self.dekoduj_tlacitko(data)
            if napeti is not None:
                self.tlacitko_baterie = napeti
            if opcode != TLACITKO_OPCODE_ALARM:
                self.zapis(f"Tlačítko poslalo zprávu bez stisku ({data.hex()}), baterie {napeti} V.")
                return
            if alarm_id is None:  # Bez Alarm ID nejde alarm potvrdit ani poznat opakování
                self.stisk_tlacitka()
                return
            self._potvrd_alarm(msg.topic, alarm_id)
            ted = time.time()
            self._alarmy = {a: t for a, t in self._alarmy.items() if ted - t < TLACITKO_ALARM_PAMET}
            novy = alarm_id not in self._alarmy
            self._alarmy[alarm_id] = ted
            if novy:
                self.stisk_tlacitka()
        except Exception as e:
            self.zapis(f"Chyba při zpracování zprávy tlačítka: {e}")

    @staticmethod
    def dekoduj_tlacitko(data):
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
        self.tlacitko_client.publish(topic_down, json.dumps({"downlinks": [{"f_port": 1, "frm_payload": payload}]}), qos=1)

    def stisk_tlacitka(self):
        """
        Někdo stiskl tlačítko. Podle situace:
        1. oba vlaky čekají ve stanici na stisk -> vyjede vlak, který stojí nejdéle,
        2. vlak jede přes segment 1 a ještě nezpomaluje před zastávkou -> zastaví na znamení,
        3. jinak zahouká jedoucí lokomotiva (nebo ta, která vyjede příště).
        """
        dnes = datetime.now(CASOVA_ZONA).date()
        if dnes != self._tlacitko_den:
            self._tlacitko_den, self.tlacitko_stisky = dnes, 0
        self.tlacitko_stisky += 1
        self.tlacitko_posledni = self.ted()

        if self.bezi() and self.ceka_na_tlacitko:
            self.zapis(f"Tlačítko: vypravuji vlak {self.nazev(self._pristi_loko())}.")
            self._vyjezd.set()
            return

        loco = self.jedouci_loco
        if self.bezi() and loco is not None and self.nastaveni["zastavka"] == "na_znameni":
            with self._zastavka_zamek:
                # Směr vlaku ještě nemusí být naučený - pak žádost podržíme a rozhodne se při vjezdu do segmentu 2
                mozna = (not self.zastavka_rozhodnuta and not self.zastavka_pozadana
                         and self.uceni.get(loco, {}).get("smer_pres_seg1", True))
                if mozna:
                    self.zastavka_pozadana = True
            if mozna:
                self.zapis(f"Tlačítko: vlak {self.nazev(loco)} zastaví na zastávce na znamení.")
                return

        loco = loco if loco is not None else self._pristi_loko()
        if self.zahoukej(loco):
            self.zapis(f"Tlačítko: {self.nazev(loco)} houká.")

    def _pristi_loko(self):
        """Lokomotiva, která stojí ve stanici nejdéle (vyjede příště)."""
        loco3, loco4 = self.nastaveni["loco_seg3"], self.nastaveni["loco_seg4"]
        return loco3 if self.navrat.get(loco3, 0) <= self.navrat.get(loco4, 0) else loco4

    # ==========================================
    # 4. ZASTÁVKA NA PŘEDĚLU SEGMENTŮ 1 A 2
    # ==========================================
    def cekej_na_uvolneni(self, segment_id):
        """
        Počká, až je segment volný REQUIRED_FALSE_COUNT kontrol po sobě (ochrana proti probliknutí).
        Vrací čas, kdy se segment skutečně uvolnil.
        """
        false_count = 0
        while false_count < REQUIRED_FALSE_COUNT:
            false_count = false_count + 1 if self.segments[segment_id] == False else 0
            self._cekej(0.1)
        return self.segment_changed_at[segment_id]

    def zjisti_smer(self, loco):
        """Při výjezdu ze stanice zjistí, jestli vlak vjede nejdříve do segmentu 1, nebo 2."""
        info = self.uceni.setdefault(loco, {})
        if "smer_pres_seg1" in info:
            return
        if self.segments[1] or self.segments[2]:
            if self.segments[1] and self.segments[2]:
                prvni = 1 if self.segment_changed_at[1] <= self.segment_changed_at[2] else 2
            else:
                prvni = 1 if self.segments[1] else 2
            info["smer_pres_seg1"] = prvni == 1
            self.zapis(f"Učení: {self.nazev(loco)} vjíždí ze stanice nejdříve do segmentu {prvni}.")

    def ma_zastavku(self, loco):
        """Vlak staví na zastávce, jen pokud vjíždí nejdříve do segmentu 1 a zastávka je v provozu
        (u zastávky na znamení jen po stisku tlačítka)."""
        zastavka = self.nastaveni["zastavka"]
        if zastavka == "ne" or not self.uceni.get(loco, {}).get("smer_pres_seg1", False):
            return False
        return zastavka == "ano" or self.zastavka_pozadana

    def zastavka_mozna(self, loco):
        """Může vlak ještě zastavit na zastávce? (Vjíždí nejdříve do segmentu 1 a o zastavení ještě nepadlo rozhodnutí.)"""
        return (not self.zastavka_rozhodnuta and self.nastaveni["zastavka"] != "ne"
                and self.uceni.get(loco, {}).get("smer_pres_seg1", False))

    def rozhodni_zastavku(self, loco):
        """Čelo vlaku vjelo do segmentu 2 - poslední chvíle pro rozhodnutí o zastavení. Vrací, jestli vlak zastaví."""
        with self._zastavka_zamek:
            self.zastavka_rozhodnuta = True
            zastavi = self.ma_zastavku(loco)
            nevyhoveno = self.zastavka_pozadana and not zastavi
        if nevyhoveno:  # Zastavení na znamení u vlaku, který zastávkou jen projíždí
            self.zapis(f"Vlak {self.nazev(loco)} zastávkou jen projíždí, místo zastavení houká.")
            self.zahoukej(loco)
        return zastavi

    def zpomal_pred_zastavkou(self, loco):
        """Čelo vlaku vjelo do segmentu 2 - hned zpomalíme na plíživou rychlost. Vrací čas zpomalení."""
        self.zapis(f"Čelo vlaku {self.nazev(loco)} vjelo do segmentu 2, zpomaluji před zastávkou.")
        self.z21.set_loco_speed(loco, self.nastaveni["zastavka_rychlost_dojezdu"])
        return time.time()

    def zastav_na_zastavce(self, loco, speed, t_zpomaleni=None):
        """
        Zastaví vlak tak, aby jeho konec stál těsně za předělem segmentů 1 a 2,
        počká zastavka_cekani a pošle vlak dál.
        t_zpomaleni: čas, kdy už vlak zpomalil (None = zatím nezpomalil, počkáme na vjezd do segmentu 2
        a teprve pak se rozhodne, jestli vlak zastaví).
        """
        if t_zpomaleni is None:
            while self.segments[2] == False:
                self._cekej(0.05)
            if not self.rozhodni_zastavku(loco):
                return
            t_zpomaleni = self.zpomal_pred_zastavkou(loco)

        t_konec = self.cekej_na_uvolneni(1)
        self.zapis(f"Konec vlaku {self.nazev(loco)} minul předěl. ZASTAVUJI NA ZASTÁVCE.")
        self.z21.set_loco_speed(loco, 0)
        self.zapis(f"Dojezd od zpomalení po uvolnění segmentu 1 trval {max(0.0, t_konec - t_zpomaleni):.2f} s.")

        self.faze = f"Vlak {self.nazev(loco)} stojí na zastávce"
        self._cekej(self.nastaveni["zastavka_cekani"])
        self.zapis(f"Vlak {self.nazev(loco)} odjíždí ze zastávky.")
        self.z21.set_loco_speed(loco, speed)

    # ==========================================
    # 5. HLAVNÍ LOGIKA (Stavový automat) - OPRAVENO
    # ==========================================
    def bezi(self):
        return self._vlakno is not None and self._vlakno.is_alive()

    def spustit(self):
        """Spustí automatický provoz na pozadí. Oba vlaky musí stát ve stanici."""
        if self.bezi():
            return
        self._stop.clear()
        self.zastavit_ve_stanici = False
        self._vlakno = threading.Thread(target=self._beh, daemon=True)
        self._vlakno.start()

    def zastavit(self):
        """Okamžitě ukončí automatický provoz a zastaví oba vlaky."""
        self._stop.set()
        if self.bezi() and threading.current_thread() is not self._vlakno:
            self._vlakno.join()
        self.zapis("Zastavuji vlaky.")
        self.z21.set_loco_speed(self.nastaveni["loco_seg3"], 0)
        self.z21.set_loco_speed(self.nastaveni["loco_seg4"], 0)
        self.faze = "Zastaveno"

    def _beh(self):
        try:
            self.main_loop()
        except Zastaveno:
            self.zapis("Automatický provoz zastaven.")
        except Exception as e:
            self.zapis(f"Chyba v automatickém provozu: {e}")
            self.z21.set_loco_speed(self.nastaveni["loco_seg3"], 0)
            self.z21.set_loco_speed(self.nastaveni["loco_seg4"], 0)
            self.faze = "Chyba - zastaveno"
        finally:
            self.jedouci_loco = None
            self.ceka_na_tlacitko = False

    def _vyjizdi(self, loco):
        """Vlak vyjíždí ze stanice - nová jízda, o zastávce zatím nepadlo rozhodnutí."""
        with self._zastavka_zamek:
            self.jedouci_loco = loco
            self.zastavka_rozhodnuta = False
            self.zastavka_pozadana = False

    def _vratil_se(self, loco):
        self.navrat[loco] = time.time()
        self.jedouci_loco = None

    def _dalsi_vyjezd(self, pristi):
        """
        Vlak dojel do stanice. Vrací stav, kterým provoz pokračuje (0 = výjezd ze segmentu 3, 10 = ze segmentu 4),
        nebo None, pokud má provoz skončit.
        V nepřetržitém provozu po pauze vyjede druhý vlak (pristi), v režimu na tlačítko až po stisku.
        """
        if self.zastavit_ve_stanici:
            return None
        self.faze = "Pauza ve stanici"
        self._cekej(4)
        return self._cekej_na_tlacitko(pristi)

    def _cekej_na_tlacitko(self, pristi):
        """V režimu na tlačítko počká na stisk a vrátí stav výjezdu vlaku, který stojí nejdéle.
        Jinak (i po přepnutí do nepřetržitého provozu během čekání) vrátí pristi."""
        if self.nastaveni["rezim"] != "na_tlacitko":
            return pristi
        self._vyjezd.clear()
        self.ceka_na_tlacitko = True
        self.faze = "Čeká na stisk tlačítka"
        self.zapis("Oba vlaky stojí ve stanici, čekám na stisk tlačítka.")
        try:
            while not self._vyjezd.is_set():
                if self.zastavit_ve_stanici:
                    return None
                if self.nastaveni["rezim"] != "na_tlacitko":
                    return pristi
                self._cekej(0.2)
        finally:
            self.ceka_na_tlacitko = False
        return 0 if self._pristi_loko() == self.nastaveni["loco_seg3"] else 10

    def main_loop(self):
        self.zapis("Spouštím automatický provoz...")
        segments = self.segments

        # Stavy:
        # 0: Vlak 3 startuje, 1: Vlak 3 opouští stanici, 2: Vlak 3 je na trati, 3: Vlak 3 dojíždí
        # 10: Vlak 4 startuje, 11: Vlak 4 opouští stanici, 12: Vlak 4 je na trati, 13: Vlak 4 dojíždí
        clear_start_time = None
        zastavka_zpomaleni = None  # Čas, kdy vlak zpomalil před zastávkou (None = ještě nezpomalil)

        self._cekej(2)  # Načtení výchozího stavu z MQTT
        state = self._cekej_na_tlacitko(0)
        if state is None:
            self.zapis("Oba vlaky stojí ve stanici, automatický provoz ukončen.")
            self.faze = "Zastaveno ve stanici"
            return

        while True:
            n = self.nastaveni  # Nastavení se dá měnit z webu za běhu, proto ho čteme v každém kroku
            loco3, loco4 = n["loco_seg3"], n["loco_seg4"]

            # ======================================
            # JÍZDA LOKOMOTIVY ZE SEGMENTU 3
            # ======================================
            if state == 0:
                self.zapis("\n--- FAZE 0: Výjezd lokomotivy ze Segmentu 3 ---")
                self.faze = f"Vlak {self.nazev(loco3)} vyjíždí ze segmentu 3"
                self._vyjizdi(loco3)
                for vyhybka, poloha in CESTY[3].items():
                    self.set_turnout(vyhybka, poloha)
                self._cekej(1)  # Počkat na přehození

                self.z21.set_loco_speed(loco3, n["speed_seg3"], direction=1)
                state = 1
                self.zapis("Čekám, až vlak úplně opustí stanici (Segment 3)...")

            elif state == 1:
                self.zjisti_smer(loco3)
                # Zpomalit hned při vjezdu čela do segmentu 2, i když konec vlaku ještě opouští stanici
                if segments[2] == True and not self.zastavka_rozhodnuta and self.rozhodni_zastavku(loco3):
                    zastavka_zpomaleni = self.zpomal_pred_zastavkou(loco3)
                if segments[3] == False:
                    if clear_start_time is None:
                        clear_start_time = time.time()  # Začneme měřit

                    # Pokud je úsek volný déle než 1.5 sekundy
                    elif (time.time() - clear_start_time) >= 1.5:
                        self.zapis("Vlak 3 je bezpečně na okruhu.")
                        if zastavka_zpomaleni is not None or self.zastavka_mozna(loco3):
                            self.zastav_na_zastavce(loco3, n["speed_seg3"], zastavka_zpomaleni)
                        zastavka_zpomaleni = None
                        self.faze = f"Vlak {self.nazev(loco3)} je na okruhu"
                        state = 2
                        clear_start_time = None
                else:
                    clear_start_time = None  # Falešný poplach, resetujeme časovač

            elif state == 2:
                # Vlak krouží po Segmentu 1 a 2. Čekáme na návrat na Segment 3 (čelo vlaku vjíždí do stanice)
                if segments[3] == True:
                    self.zapis("Čelo vlaku 3 je zpět ve stanici, čekám na dojezd konce vlaku z okruhu...")
                    self.faze = f"Vlak {self.nazev(loco3)} vjíždí do stanice"
                    self.z21.set_loco_speed(loco3, n["rychlost_vjezdu"])
                    state = 3

            elif state == 3:
                # Kontrola uvolnění úseku s ochranou proti falešnému hlášení (mrtvý bod výhybky)
                if segments[1] == False and segments[2] == False:
                    if clear_start_time is None:
                        clear_start_time = time.time()  # Začneme měřit
                    elif (time.time() - clear_start_time) >= 1.5:
                        self.zapis("Lokomotiva 3 se celá vrátila a minula výhybku. ZASTAVUJI.")
                        self.z21.set_loco_speed(loco3, 0)
                        self._vratil_se(loco3)
                        state = self._dalsi_vyjezd(10)
                        if state is None:
                            self.zapis("Oba vlaky stojí ve stanici, automatický provoz ukončen.")
                            self.faze = "Zastaveno ve stanici"
                            return
                        clear_start_time = None
                else:
                    clear_start_time = None
                    # ======================================

            # JÍZDA LOKOMOTIVY ZE SEGMENTU 4
            # ======================================
            elif state == 10:
                self.zapis("\n--- FAZE 10: Výjezd lokomotivy ze Segmentu 4 ---")
                self.faze = f"Vlak {self.nazev(loco4)} vyjíždí ze segmentu 4"
                self._vyjizdi(loco4)
                for vyhybka, poloha in CESTY[4].items():
                    self.set_turnout(vyhybka, poloha)
                self._cekej(1)

                # Nastavte směr (direction=0 nebo 1) podle toho, jak máte okruh zapojený
                self.z21.set_loco_speed(loco4, n["speed_seg4"], direction=1)
                state = 11
                self.zapis("Čekám, až vlak úplně opustí stanici (Segment 4)...")

            elif state == 11:
                self.zjisti_smer(loco4)
                # Zpomalit hned při vjezdu čela do segmentu 2, i když konec vlaku ještě opouští stanici
                if segments[2] == True and not self.zastavka_rozhodnuta and self.rozhodni_zastavku(loco4):
                    zastavka_zpomaleni = self.zpomal_pred_zastavkou(loco4)
                # Kontrola uvolnění úseku s ochranou proti falešnému hlášení (mrtvý bod výhybky)
                if segments[4] == False:
                    if clear_start_time is None:
                        clear_start_time = time.time()  # Začneme měřit
                    elif (time.time() - clear_start_time) >= 1.5:
                        self.zapis("Vlak 4 je bezpečně na okruhu.")
                        if zastavka_zpomaleni is not None or self.zastavka_mozna(loco4):
                            self.zastav_na_zastavce(loco4, n["speed_seg4"], zastavka_zpomaleni)
                        zastavka_zpomaleni = None
                        self.faze = f"Vlak {self.nazev(loco4)} je na okruhu"
                        state = 12
                        clear_start_time = None
                else:
                    clear_start_time = None

            elif state == 12:
                if segments[4] == True:
                    self.zapis("Čelo vlaku 4 je zpět ve stanici, čekám na dojezd konce vlaku z okruhu...")
                    self.faze = f"Vlak {self.nazev(loco4)} vjíždí do stanice"
                    self.z21.set_loco_speed(loco4, n["rychlost_vjezdu"])
                    state = 13

            elif state == 13:
                # Kontrola uvolnění úseku s ochranou proti falešnému hlášení (mrtvý bod výhybky)
                if segments[1] == False and segments[2] == False:
                    if clear_start_time is None:
                        clear_start_time = time.time()  # Začneme měřit
                    elif (time.time() - clear_start_time) >= 1.5:
                        self.zapis("Lokomotiva 4 se celá vrátila a minula výhybku. ZASTAVUJI.")
                        self.z21.set_loco_speed(loco4, 0)
                        self._vratil_se(loco4)
                        state = self._dalsi_vyjezd(0)
                        if state is None:
                            self.zapis("Oba vlaky stojí ve stanici, automatický provoz ukončen.")
                            self.faze = "Zastaveno ve stanici"
                            return
                        clear_start_time = None
                else:
                    clear_start_time = None

            self._cekej(0.1)  # Rychlejší reakční doba než v předchozím kódu


# ==========================================
# START SKRIPTU (bez webového rozhraní)
# ==========================================
if __name__ == "__main__":
    provoz = Provoz()
    provoz.pripojit_mqtt()
    provoz.pripojit_tlacitko()
    provoz.z21.pripojit()
    provoz.spustit()

    try:
        while provoz.bezi():
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\nUkončuji skript...")
        provoz.zastavit()
        provoz.client.loop_stop()
        provoz.z21.zavrit()
