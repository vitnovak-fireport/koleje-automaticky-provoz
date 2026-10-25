import paho.mqtt.client as mqtt
import base64
import socket
import struct
import json
import os
import threading
import time
from collections import deque
from datetime import datetime
from zoneinfo import ZoneInfo

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
Z21_PORT = int(os.environ.get("Z21_PORT", "21105"))
Z21_KEEPALIVE = 30  # s - Z21 zapomene klienta po 60 s bez komunikace, proto se pravidelně ozýváme
Z21_STAV_INTERVAL = 5  # s - jak často se ptát centrály na stav napájení kolejiště
Z21_POTVRZENI_TIMEOUT = 0.5  # s - po této době bez potvrzení od Z21 se povel pošle znovu
Z21_POKUSY = 10  # Kolikrát nejvýše povel poslat, než to vzdáme
VYHYBKA_PULZ = 0.15  # s - jak dlouho je cívka výhybky sepnutá (bez vypnutí Z21 ignoruje další přestavení)
Z21_MERENI_PLATNOST = 5  # s - Z21 posílá napětí a proud každou sekundu, starší měření už nezobrazujeme

# Stav napájení kolejiště podle Z21
NAPAJENI_ZAPNUTO = "zapnuto"
NAPAJENI_VYPNUTO = "vypnuto"
NAPAJENI_NOUZOVE = "nouzové zastavení"
NAPAJENI_ZKRAT = "zkrat"
NAPAJENI_PO_OBNOVENI = 3  # s - po obnovení napájení počkat, než se srovnají hlášení obsazenosti

# Výhybky ve stanici a jejich nastavení pro výjezd z jednotlivých staničních kolejí (0 = rovně, 1 = odbočka)
VYHYBKY = (5, 6)
CESTY = {
    3: {5: 1, 6: 0},  # Výjezd z koleje 3
    4: {5: 0, 6: 1},  # Výjezd z koleje 4
}
POLOHY_VYHYBKY = {0: "rovně", 1: "odbočka"}

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


class Povel:
    """Povel pro Z21, který čeká na potvrzení. Bez potvrzení se posílá znovu (UDP pakety se můžou ztratit)."""

    def __init__(self, ocekavano, pakety, popis, deaktivace=None):
        self.ocekavano = ocekavano  # Co má Z21 nahlásit, aby byl povel potvrzený
        self.pakety = pakety        # Povel a dotaz na stav, posílají se vždy spolu
        self.deaktivace = deaktivace  # Paket poslaný VYHYBKA_PULZ po povelu (vypnutí cívky výhybky)
        self.popis = popis
        self.pokusy = 1
        self.odeslano = time.time()
        self.potvrzeno = False
        self.vyrizeno = threading.Event()  # Potvrzeno, nebo vzdáno


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
        self.rychlosti = {}  # Poslední rychlost odeslaná každé lokomotivě
        self.smery = {}  # Poslední směr odeslaný každé lokomotivě
        self.lokomotivy_z21 = {}  # Rychlost a směr lokomotiv, jak je hlásí Z21: adresa -> (rychlost, směr)
        # Stav výhybek: stav (0/1/None = neznámý), cas (HH:MM:SS), zdroj ("skript" = povel ze skriptu, "Z21" = hlášení centrály)
        self.vyhybky = {adresa: {"stav": None, "cas": None, "zdroj": None} for adresa in VYHYBKY}
        self.z21_posledni_zprava = None  # time.time() poslední zprávy od Z21
        self.napajeni = None  # Stav napájení kolejiště (NAPAJENI_*), None = zatím neznámý
        self.mereni = None  # Poslední LAN_SYSTEMSTATE od Z21 (napětí, proud, teplota), viz mereni_napajeni()
        self.mereni_cas = 0.0  # time.time() posledního měření

        # Povely čekající na potvrzení od Z21: klíč ("loko"/"vyhybka", adresa) -> Povel
        self._povely = {}
        self._povely_zamek = threading.Lock()
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

        # UDP Socket pro Z21 (navázaný na pevný lokální port, aby šlo přijímat odpovědi centrály)
        self.udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_sock.bind(("", 0))

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
        if self.napajeni not in (None, NAPAJENI_ZAPNUTO):
            self._pozastav()

    def _pozastav(self):
        """Počká na obnovení napájení kolejiště a pak lokomotivám znovu pošle jejich rychlosti."""
        faze = self.faze
        self.faze = f"Pozastaveno - kolejiště: {self.napajeni}"
        self.zapis(f"Kolejiště je bez proudu ({self.napajeni}), provoz čeká na obnovení napájení.")
        while self.napajeni != NAPAJENI_ZAPNUTO:
            if self._stop.wait(0.2):
                raise Zastaveno()
        self.zapis(f"Napájení kolejiště obnoveno, za {NAPAJENI_PO_OBNOVENI} s pokračuji.")
        if self._stop.wait(NAPAJENI_PO_OBNOVENI):
            raise Zastaveno()
        # Nouzové zastavení v Z21 vynuluje rychlosti lokomotiv, proto je pošleme znovu
        for loco in (self.nastaveni["loco_seg3"], self.nastaveni["loco_seg4"]):
            if loco in self.rychlosti:
                self.set_loco_speed(loco, self.rychlosti[loco], self.smery.get(loco, 1))
        self.faze = faze

    # ==========================================
    # 2. FUNKCE PRO Z21 (UDP Protokol)
    # ==========================================
    @staticmethod
    def checksum(data):
        """Vypočítá XOR kontrolní součet pro Z21 protokol."""
        cs = 0
        for byte in data:
            cs ^= byte
        return cs

    def set_turnout(self, address, state):
        """
        Přepne DCC výhybku přes Z21 centrálu.

        :param address: Adresa výhybky (1, 2, 3...)
        :param state: 0 pro rovně (přímý směr), 1 pro odbočku
        """
        # Z21 (Roco) adresuje výhybky od 0, proto odečítáme 1 (FADA adresování)
        z21_addr = address - 1

        # Rozdělení adresy na High a Low byte
        addr_high = (z21_addr >> 8) & 0xFF
        addr_low = z21_addr & 0xFF

        # Sestavení řídícího bytu (DB2)
        # Bit 7 = 1
        # Bit 3 = 1 (Aktivace cívky / zapnuto)
        # Bit 0 = state (0 nebo 1 podle směru)
        # 0x88 (binárně 10001000) je základ pro aktivní stav
        db2_active = 0x88 | (state & 0x01)

        # Výpočet kontrolního součtu (XOR) pro aktivní povel
        checksum_active = 0x53 ^ addr_high ^ addr_low ^ db2_active

        # Sestavení paketu pro zapnutí cívky
        # Hlavička: [Délka LSB, Délka MSB, Typ LSB, Typ MSB] -> [0x09, 0x00, 0x40, 0x00]
        # Data: [0x53 (LAN_X_SET_TURNOUT), AH, AL, DB2]
        packet_active = bytes([0x09, 0x00, 0x40, 0x00, 0x53, addr_high, addr_low, db2_active, checksum_active])

        # Vypínací paket (Bit 3 = 0) - pošle se VYHYBKA_PULZ po zapnutí. Bez něj Z21 považuje výstup
        # za stále sepnutý a další přestavení téže výhybky ignoruje (ověřeno na skutečné Z21).
        db2_inactive = 0x80 | (state & 0x01)
        checksum_inactive = 0x53 ^ addr_high ^ addr_low ^ db2_inactive
        packet_inactive = bytes([0x09, 0x00, 0x40, 0x00, 0x53, addr_high, addr_low, db2_inactive, checksum_inactive])

        # Odeslání povelu centrále spolu s dotazem na polohu - polohu uložíme, až ji Z21 potvrdí
        povel = self._posli_s_potvrzenim(("vyhybka", address), state,
                                         [packet_active, self._paket_dotaz_vyhybka(address)],
                                         f"výhybka {address}: {POLOHY_VYHYBKY.get(state, state)}",
                                         deaktivace=packet_inactive)
        self.zapis(f"-> Výhybka {address}: {POLOHY_VYHYBKY.get(state, state)} ({state})")

        # Vlak se nesmí rozjet přes nepřestavenou výhybku - bez potvrzení provoz skončí chybou
        while not povel.vyrizeno.is_set():
            self._cekej(0.05)
        if not povel.potvrzeno:
            raise RuntimeError(f"Z21 nepotvrdila přestavení výhybky {address}")

        self._cekej(1)

    def uloz_stav_vyhybky(self, address, state, zdroj):
        """Zapamatuje si polohu výhybky. Změnu, kterou nahlásí Z21 (např. ruční přestavení), zapíše do logu."""
        if address not in self.vyhybky:
            return
        puvodni = self.vyhybky[address]["stav"]
        if zdroj == "Z21" and state == puvodni:
            return  # Jen další hlášení stejné polohy (např. odpověď na dotaz) - ponecháme původní zdroj
        self.vyhybky[address] = {"stav": state, "cas": self.ted(), "zdroj": zdroj}
        if zdroj == "Z21" and state != puvodni:
            self.zapis(f"Z21 hlásí: Výhybka {address}: {POLOHY_VYHYBKY.get(state, 'neznámá poloha')} ({state})")

    def cesta(self):
        """Na kterou staniční kolej jsou výhybky nastavené (3, 4), nebo None."""
        for kolej, polohy in CESTY.items():
            if all(self.vyhybky[a]["stav"] == stav for a, stav in polohy.items()):
                return kolej
        return None

    def z21_odpovida(self):
        return self.z21_posledni_zprava is not None and time.time() - self.z21_posledni_zprava < 2 * Z21_KEEPALIVE + 10

    def mereni_napajeni(self):
        """Napětí, proud a výkon hlavního výstupu Z21, nebo None, pokud čerstvé měření chybí.
        Booster na B-Busu nic nehlásí, jeho odběr v hodnotách není."""
        if self.mereni is None or time.time() - self.mereni_cas > Z21_MERENI_PLATNOST:
            return None
        return dict(self.mereni, vykon_w=round(self.mereni["napeti_v"] * self.mereni["proud_a"], 1))

    def pripojit_z21(self):
        """Přihlásí se u Z21 k hlášením o výhybkách, zjistí jejich polohu a dál poslouchá na pozadí."""
        threading.Thread(target=self._z21_smycka, daemon=True).start()

    def _z21_prihlaseni(self):
        # LAN_SET_BROADCASTFLAGS (nahrazují předchozí, ne přidávají):
        # 0x00000001 = hlášení o jízdě a přestavování (mj. LAN_X_TURNOUT_INFO)
        # 0x00000100 = LAN_SYSTEMSTATE_DATACHANGED každou sekundu (napětí, proud, teplota)
        self.udp_sock.sendto(bytes([0x08, 0x00, 0x50, 0x00, 0x01, 0x01, 0x00, 0x00]), (Z21_IP, Z21_PORT))

    @staticmethod
    def _paket_dotaz_vyhybka(address):
        # LAN_X_GET_TURNOUT_INFO
        z21_addr = address - 1
        addr_high, addr_low = (z21_addr >> 8) & 0xFF, z21_addr & 0xFF
        return bytes([0x08, 0x00, 0x40, 0x00, 0x43, addr_high, addr_low, 0x43 ^ addr_high ^ addr_low])

    def _z21_dotaz_vyhybka(self, address):
        self.udp_sock.sendto(self._paket_dotaz_vyhybka(address), (Z21_IP, Z21_PORT))

    def _z21_dotaz_stav(self):
        # LAN_X_GET_STATUS - odpovědí je LAN_X_STATUS_CHANGED se stavem napájení kolejiště
        self.udp_sock.sendto(bytes([0x07, 0x00, 0x40, 0x00, 0x21, 0x24, 0x05]), (Z21_IP, Z21_PORT))

    def _posli_s_potvrzenim(self, klic, ocekavano, pakety, popis, deaktivace=None):
        """Pošle povel centrále a zařadí ho mezi povely čekající na potvrzení (novější povel nahradí starší)."""
        povel = Povel(ocekavano, pakety, popis, deaktivace)
        with self._povely_zamek:
            stary = self._povely.get(klic)
            self._povely[klic] = povel
        if stary:
            stary.vyrizeno.set()
        self._odesli(povel)
        return povel

    def _odesli(self, povel):
        for paket in povel.pakety:
            self.udp_sock.sendto(paket, (Z21_IP, Z21_PORT))
        if povel.deaktivace:
            threading.Timer(VYHYBKA_PULZ, self.udp_sock.sendto, (povel.deaktivace, (Z21_IP, Z21_PORT))).start()

    def _potvrd(self, klic, shoda):
        """Z21 nahlásila stav - pokud odpovídá čekajícímu povelu (shoda(ocekavano) je True), povel je potvrzený."""
        with self._povely_zamek:
            povel = self._povely.get(klic)
            if povel is None or not shoda(povel.ocekavano):
                return False
            del self._povely[klic]
        povel.potvrzeno = True
        povel.vyrizeno.set()
        if povel.pokusy > 1:
            self.zapis(f"Z21 potvrdila povel ({povel.popis}) až na {povel.pokusy}. pokus.")
        return True

    def _ceka_na_potvrzeni(self, klic):
        with self._povely_zamek:
            return klic in self._povely

    def _z21_opakuj(self):
        """Povely, které Z21 do Z21_POTVRZENI_TIMEOUT nepotvrdila, pošle znovu, nebo je po Z21_POKUSY vzdá."""
        ted = time.time()
        znovu, vzdane = [], []
        with self._povely_zamek:
            for klic, povel in list(self._povely.items()):
                if ted - povel.odeslano < Z21_POTVRZENI_TIMEOUT:
                    continue
                if povel.pokusy >= Z21_POKUSY:
                    del self._povely[klic]
                    vzdane.append(povel)
                else:
                    povel.pokusy += 1
                    povel.odeslano = ted
                    znovu.append(povel)
        for povel in znovu:
            if povel.pokusy == 2:
                self.zapis(f"Z21 nepotvrdila povel ({povel.popis}), posílám znovu.")
            self._odesli(povel)
        for povel in vzdane:
            self.zapis(f"CHYBA: Z21 ani po {Z21_POKUSY} pokusech nepotvrdila povel ({povel.popis})!")
            povel.vyrizeno.set()

    def _z21_smycka(self):
        self.udp_sock.settimeout(0.1)  # Krátký timeout, aby se včas opakovaly nepotvrzené povely
        posledni_prihlaseni = 0
        posledni_dotaz_stav = 0
        while True:
            try:
                if time.time() - posledni_prihlaseni >= Z21_KEEPALIVE:
                    self._z21_prihlaseni()
                    if posledni_prihlaseni == 0 or not self.z21_odpovida():
                        for adresa in VYHYBKY:
                            self._z21_dotaz_vyhybka(adresa)
                    posledni_prihlaseni = time.time()
                if time.time() - posledni_dotaz_stav >= Z21_STAV_INTERVAL:
                    self._z21_dotaz_stav()
                    # Z21 první pakety od nového klienta často zahodí, proto se ptáme, dokud polohu nezjistíme
                    for adresa in VYHYBKY:
                        if self.vyhybky[adresa]["stav"] is None:
                            self._z21_dotaz_vyhybka(adresa)
                    posledni_dotaz_stav = time.time()
                self._z21_opakuj()
                data, _ = self.udp_sock.recvfrom(1024)
                self._z21_zpracuj(data)
            except socket.timeout:
                continue
            except OSError:
                if self.udp_sock.fileno() == -1:
                    return  # Socket byl zavřen při ukončení skriptu
                time.sleep(1)

    def _z21_zpracuj(self, data):
        """Projde zprávy od Z21 (jeden UDP paket jich může obsahovat víc) a vybere hlášení o výhybkách,
        lokomotivách a napájení kolejiště."""
        self.z21_posledni_zprava = time.time()
        i = 0
        while i + 4 <= len(data):
            delka = data[i] | (data[i + 1] << 8)
            if delka < 4:
                return
            hlavicka = data[i + 2] | (data[i + 3] << 8)
            zprava = data[i + 4:i + delka]
            i += delka
            if hlavicka == 0x84 and len(zprava) >= 12:
                self._z21_systemstate(zprava)
                continue
            if hlavicka != 0x40 or not zprava:
                continue
            x = zprava[0]
            # LAN_X_TURNOUT_INFO: 0x43, FAdr_MSB, FAdr_LSB, 000000ZZ (01 = poloha 0, 10 = poloha 1)
            if x == 0x43 and delka == 9:
                adresa = ((zprava[1] << 8) | zprava[2]) + 1
                stav = {0b01: 0, 0b10: 1}.get(zprava[3] & 0x03)
                potvrzeno = stav is not None and self._potvrd(("vyhybka", adresa), lambda o: o == stav)
                self.uloz_stav_vyhybky(adresa, stav, "skript" if potvrzeno else "Z21")
            # LAN_X_LOCO_INFO: 0xEF, Adr_MSB, Adr_LSB, 0000BFFF (FFF = 4 -> 128 kroků), RVVVVVVV, ...
            elif x == 0xEF and len(zprava) >= 5:
                adresa = ((zprava[1] & 0x3F) << 8) | zprava[2]
                self._z21_loko_info(adresa, zprava[4] & 0x7F, 1 if zprava[4] & 0x80 else 0, (zprava[3] & 0x07) == 4)
                self._z21_loko_funkce(adresa, zprava)
            # LAN_X_BC_TRACK_POWER_OFF / _ON / _SHORT_CIRCUIT
            elif x == 0x61 and len(zprava) >= 2:
                stav = {0x00: NAPAJENI_VYPNUTO, 0x01: NAPAJENI_ZAPNUTO, 0x08: NAPAJENI_ZKRAT}.get(zprava[1])
                if stav:
                    self._uloz_napajeni(stav)
            # LAN_X_BC_STOPPED (nouzové zastavení)
            elif x == 0x81 and len(zprava) >= 2 and zprava[1] == 0x00:
                self._uloz_napajeni(NAPAJENI_NOUZOVE)
            # LAN_X_STATUS_CHANGED: 0x62, 0x22, stav (0x01 nouzové zastavení, 0x02 bez napětí, 0x04 zkrat)
            elif x == 0x62 and len(zprava) >= 3 and zprava[1] == 0x22:
                stav = zprava[2]
                if stav & 0x04:
                    self._uloz_napajeni(NAPAJENI_ZKRAT)
                elif stav & 0x02:
                    self._uloz_napajeni(NAPAJENI_VYPNUTO)
                elif stav & 0x01:
                    self._uloz_napajeni(NAPAJENI_NOUZOVE)
                else:
                    self._uloz_napajeni(NAPAJENI_ZAPNUTO)

    def _z21_systemstate(self, zprava):
        """LAN_SYSTEMSTATE_DATACHANGED: MainCurrent, ProgCurrent, FilteredMainCurrent (mA), Temperature (°C),
        SupplyVoltage, VCCVoltage (mV), vše little-endian. VCCVoltage je napětí, které jde do kolejí."""
        proud, _, proud_vyhlazeny, teplota, napeti_zdroje, napeti = struct.unpack_from("<hhhhHH", zprava)
        self.mereni = {
            "napeti_v": round(napeti / 1000, 2),
            "napeti_zdroje_v": round(napeti_zdroje / 1000, 2),
            "proud_a": round(proud_vyhlazeny / 1000, 3),  # Okamžitý proud hodně kolísá, zobrazujeme vyhlazený
            "proud_okamzity_a": round(proud / 1000, 3),
            "teplota_c": teplota,
        }
        self.mereni_cas = time.time()

    @staticmethod
    def _stejna_jizda(ocekavano, rychlost, smer, kroky128):
        """Odpovídá rychlost a směr nahlášené Z21 odeslanému povelu?"""
        rychlost_o, smer_o = ocekavano
        if smer != smer_o:
            return False
        if kroky128:
            return rychlost == rychlost_o
        return (rychlost > 0) == (rychlost_o > 0)  # Jiný počet kroků - porovnáme jen jede/stojí

    def _z21_loko_info(self, adresa, rychlost, smer, kroky128):
        """Z21 hlásí stav lokomotivy - potvrdí čekající povel, nebo zapíše změnu z jiného ovladače."""
        predchozi = self.lokomotivy_z21.get(adresa)
        self.lokomotivy_z21[adresa] = (rychlost, smer)
        if self._potvrd(("loko", adresa), lambda o: self._stejna_jizda(o, rychlost, smer, kroky128)):
            return
        if (adresa in self.rychlosti and (rychlost, smer) != predchozi
                and not self._ceka_na_potvrzeni(("loko", adresa))
                and not self._stejna_jizda((self.rychlosti[adresa], self.smery.get(adresa, 1)), rychlost, smer, kroky128)):
            self.zapis(f"Z21 hlásí: {self.nazev(adresa)}: Rychlost {rychlost}, Směr {smer} (změněno jiným ovladačem?)")

    @staticmethod
    def _stav_funkce(zprava, cislo):
        """Stav funkce F<cislo> z LAN_X_LOCO_INFO (DB4 = 0DSLFGHJ: L = F0, J-F = F1-F4, DB5-DB7 = F5-F28),
        None, pokud ji zpráva nenese."""
        if cislo <= 4:
            index, bit = 5, 4 if cislo == 0 else cislo - 1
        else:
            index, bit = 6 + (cislo - 5) // 8, (cislo - 5) % 8
        return bool(zprava[index] >> bit & 1) if index < len(zprava) else None

    def _z21_loko_funkce(self, adresa, zprava):
        """Potvrdí čekající povely funkcí lokomotivy podle stavu funkcí, který Z21 hlásí v LAN_X_LOCO_INFO."""
        with self._povely_zamek:
            cekajici = [klic for klic in self._povely if klic[0] == "funkce" and klic[1] == adresa]
        for klic in cekajici:
            stav = self._stav_funkce(zprava, klic[2])
            if stav is not None:
                self._potvrd(klic, lambda o: o == stav)

    def _uloz_napajeni(self, stav):
        if stav != self.napajeni:
            self.napajeni = stav
            self.zapis(f"Z21 hlásí: napájení kolejiště {stav}")

    def set_loco_speed(self, address, speed, direction=1):
        """
        Nastaví rychlost lokomotivy.
        address: DCC adresa
        speed: 0 (stop) až 126
        direction: 1 (vpřed), 0 (vzad)
        """
        # LAN_X_SET_LOCO_DRIVE (128 speed steps)
        DB0 = 0x13
        DB1, DB2 = self._loko_adresa(address)

        dir_bit = 128 if direction else 0
        DB3 = dir_bit | speed if speed > 0 else dir_bit

        data = [0xE4, DB0, DB1, DB2, DB3]
        cs = self.checksum(data)
        packet = bytes([0x0A, 0x00, 0x40, 0x00] + data + [cs])

        self.rychlosti[address] = speed
        self.smery[address] = direction
        self._posli_s_potvrzenim(("loko", address), (speed, direction), [packet, self._paket_dotaz_loko(address)],
                                 f"{self.nazev(address)}: rychlost {speed}")
        self.zapis(f"-> {self.nazev(address)}: Rychlost {speed}, Směr {direction}")

    @staticmethod
    def _loko_adresa(address):
        """Vyšší a nižší bajt adresy lokomotivy pro povely Z21."""
        db1 = (address >> 8) & 0x3F
        if db1 > 0: db1 |= 0xC0  # Dlouhá adresa
        return db1, address & 0xFF

    def _paket_dotaz_loko(self, address):
        # LAN_X_GET_LOCO_INFO - Z21 odpoví stavem lokomotivy a přihlásí nás k jejím dalším změnám
        dotaz = [0xE3, 0xF0, *self._loko_adresa(address)]
        return bytes([0x09, 0x00, 0x40, 0x00] + dotaz + [self.checksum(dotaz)])

    def set_loco_function(self, address, cislo, zapnuto):
        """Zapne nebo vypne funkci dekodéru F<cislo> (0-31), např. houkačku."""
        # LAN_X_SET_LOCO_FUNCTION: 0xE4, 0xF8, Adr_MSB, Adr_LSB, TTNNNNNN (TT: 00 = vypnout, 01 = zapnout)
        data = [0xE4, 0xF8, *self._loko_adresa(address), (0x40 if zapnuto else 0x00) | (cislo & 0x3F)]
        packet = bytes([0x0A, 0x00, 0x40, 0x00] + data + [self.checksum(data)])
        stav = "zapnuto" if zapnuto else "vypnuto"
        self._posli_s_potvrzenim(("funkce", address, cislo), zapnuto, [packet, self._paket_dotaz_loko(address)],
                                 f"{self.nazev(address)}: F{cislo} {stav}")
        self.zapis(f"-> {self.nazev(address)}: F{cislo} {stav}")

    def zahoukej(self, loco):
        """Krátce zahouká lokomotivou (funkce "houkacka" v LOKOMOTIVY). Vrací False, pokud nezahoukala."""
        funkce = LOKOMOTIVY.get(loco, {}).get("funkce", {}).get("houkacka")
        if funkce is None:
            self.zapis(f"{self.nazev(loco)} nemá houkačku.")
            return False
        if time.time() - self._houkani_cas < HOUKANI_PAUZA:
            return False
        self._houkani_cas = time.time()
        self.set_loco_function(loco, funkce, True)
        threading.Timer(HOUKANI_DELKA, self.set_loco_function, (loco, funkce, False)).start()
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
        self.set_loco_speed(loco, self.nastaveni["zastavka_rychlost_dojezdu"])
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
        self.set_loco_speed(loco, 0)
        self.zapis(f"Dojezd od zpomalení po uvolnění segmentu 1 trval {max(0.0, t_konec - t_zpomaleni):.2f} s.")

        self.faze = f"Vlak {self.nazev(loco)} stojí na zastávce"
        self._cekej(self.nastaveni["zastavka_cekani"])
        self.zapis(f"Vlak {self.nazev(loco)} odjíždí ze zastávky.")
        self.set_loco_speed(loco, speed)

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
        self.set_loco_speed(self.nastaveni["loco_seg3"], 0)
        self.set_loco_speed(self.nastaveni["loco_seg4"], 0)
        self.faze = "Zastaveno"

    def _beh(self):
        try:
            self.main_loop()
        except Zastaveno:
            self.zapis("Automatický provoz zastaven.")
        except Exception as e:
            self.zapis(f"Chyba v automatickém provozu: {e}")
            self.set_loco_speed(self.nastaveni["loco_seg3"], 0)
            self.set_loco_speed(self.nastaveni["loco_seg4"], 0)
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

                self.set_loco_speed(loco3, n["speed_seg3"], direction=1)
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
                    self.set_loco_speed(loco3, n["rychlost_vjezdu"])
                    state = 3

            elif state == 3:
                # Kontrola uvolnění úseku s ochranou proti falešnému hlášení (mrtvý bod výhybky)
                if segments[1] == False and segments[2] == False:
                    if clear_start_time is None:
                        clear_start_time = time.time()  # Začneme měřit
                    elif (time.time() - clear_start_time) >= 1.5:
                        self.zapis("Lokomotiva 3 se celá vrátila a minula výhybku. ZASTAVUJI.")
                        self.set_loco_speed(loco3, 0)
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
                self.set_loco_speed(loco4, n["speed_seg4"], direction=1)
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
                    self.set_loco_speed(loco4, n["rychlost_vjezdu"])
                    state = 13

            elif state == 13:
                # Kontrola uvolnění úseku s ochranou proti falešnému hlášení (mrtvý bod výhybky)
                if segments[1] == False and segments[2] == False:
                    if clear_start_time is None:
                        clear_start_time = time.time()  # Začneme měřit
                    elif (time.time() - clear_start_time) >= 1.5:
                        self.zapis("Lokomotiva 4 se celá vrátila a minula výhybku. ZASTAVUJI.")
                        self.set_loco_speed(loco4, 0)
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
    provoz.pripojit_z21()
    provoz.spustit()

    try:
        while provoz.bezi():
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\nUkončuji skript...")
        provoz.zastavit()
        provoz.client.loop_stop()
        provoz.udp_sock.close()
