import paho.mqtt.client as mqtt
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

# Lokomotivy podle DCC adresy (ve webu se vybírají ze seznamu)
LOKOMOTIVY = {
    3: "Bardotka",
    4: "Bangle",
    5: "Brejlovec",
    6: "Banán",
    7: "Vectron",
}

# Výchozí nastavení provozu - ve webové aplikaci se dá měnit za běhu
VYCHOZI_NASTAVENI = {
    # Lokomotivy
    "loco_seg3": 6,         # Adresa lokomotivy, která začíná na segmentu 3
    "loco_seg4": 3,         # Adresa lokomotivy, která začíná na segmentu 4
    "speed_seg3": 127,      # Rychlost (0-127)
    "speed_seg4": 127,      # Rychlost (0-127)
    "rychlost_vjezdu": 60,  # Rychlost při vjezdu do stanice

    # Zastávka na předělu segmentů 1 a 2 (jen pro vlaky, které ze stanice vjedou nejdříve do segmentu 1)
    "zastavka_povolena": os.environ.get("ZASTAVKA_POVOLENA", "1") == "1",  # True = vlaky zastaví na zastávce
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

        self._stop = threading.Event()
        self._vlakno = None

        # UDP Socket pro Z21 (navázaný na pevný lokální port, aby šlo přijímat odpovědi centrály)
        self.udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_sock.bind(("", 0))

        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message

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
        return f"{LOKOMOTIVY[loco]} ({loco})" if loco in LOKOMOTIVY else f"Lokomotiva {loco}"

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
                self._z21_loko_info(((zprava[1] & 0x3F) << 8) | zprava[2],
                                    zprava[4] & 0x7F, 1 if zprava[4] & 0x80 else 0, (zprava[3] & 0x07) == 4)
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
        DB1 = (address >> 8) & 0x3F  # Vyšší bajt adresy
        if DB1 > 0: DB1 |= 0xC0  # Dlouhá adresa
        DB2 = address & 0xFF  # Nižší bajt adresy

        dir_bit = 128 if direction else 0
        DB3 = dir_bit | speed if speed > 0 else dir_bit

        data = [0xE4, DB0, DB1, DB2, DB3]
        cs = self.checksum(data)
        packet = bytes([0x0A, 0x00, 0x40, 0x00] + data + [cs])

        # LAN_X_GET_LOCO_INFO - Z21 odpoví stavem lokomotivy a přihlásí nás k jejím dalším změnám
        dotaz = [0xE3, 0xF0, DB1, DB2]
        dotaz_packet = bytes([0x09, 0x00, 0x40, 0x00] + dotaz + [self.checksum(dotaz)])

        self.rychlosti[address] = speed
        self.smery[address] = direction
        self._posli_s_potvrzenim(("loko", address), (speed, direction), [packet, dotaz_packet],
                                 f"{self.nazev(address)}: rychlost {speed}")
        self.zapis(f"-> {self.nazev(address)}: Rychlost {speed}, Směr {direction}")

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
        """Vlak staví na zastávce, jen pokud je zastávka povolená a vlak vjíždí nejdříve do segmentu 1."""
        return self.nastaveni["zastavka_povolena"] and self.uceni.get(loco, {}).get("smer_pres_seg1", False)

    def zpomal_pred_zastavkou(self, loco):
        """Čelo vlaku vjelo do segmentu 2 - hned zpomalíme na plíživou rychlost. Vrací čas zpomalení."""
        self.zapis(f"Čelo vlaku {self.nazev(loco)} vjelo do segmentu 2, zpomaluji před zastávkou.")
        self.set_loco_speed(loco, self.nastaveni["zastavka_rychlost_dojezdu"])
        return time.time()

    def zastav_na_zastavce(self, loco, speed, t_zpomaleni=None):
        """
        Zastaví vlak tak, aby jeho konec stál těsně za předělem segmentů 1 a 2,
        počká zastavka_cekani a pošle vlak dál.
        t_zpomaleni: čas, kdy už vlak zpomalil (None = zatím nezpomalil, počkáme na vjezd do segmentu 2).
        """
        if t_zpomaleni is None:
            while self.segments[2] == False:
                self._cekej(0.05)
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

    def main_loop(self):
        self.zapis("Spouštím automatický provoz...")
        segments = self.segments

        # Stavy:
        # 0: Vlak 3 startuje, 1: Vlak 3 opouští stanici, 2: Vlak 3 je na trati, 3: Vlak 3 dojíždí
        # 10: Vlak 4 startuje, 11: Vlak 4 opouští stanici, 12: Vlak 4 je na trati, 13: Vlak 4 dojíždí
        state = 0
        clear_start_time = None
        zastavka_zpomaleni = None  # Čas, kdy vlak zpomalil před zastávkou (None = ještě nezpomalil)

        self._cekej(2)  # Načtení výchozího stavu z MQTT

        while True:
            n = self.nastaveni  # Nastavení se dá měnit z webu za běhu, proto ho čteme v každém kroku
            loco3, loco4 = n["loco_seg3"], n["loco_seg4"]

            # ======================================
            # JÍZDA LOKOMOTIVY ZE SEGMENTU 3
            # ======================================
            if state == 0:
                self.zapis("\n--- FAZE 0: Výjezd lokomotivy ze Segmentu 3 ---")
                self.faze = f"Vlak {self.nazev(loco3)} vyjíždí ze segmentu 3"
                for vyhybka, poloha in CESTY[3].items():
                    self.set_turnout(vyhybka, poloha)
                self._cekej(1)  # Počkat na přehození

                self.set_loco_speed(loco3, n["speed_seg3"], direction=1)
                state = 1
                self.zapis("Čekám, až vlak úplně opustí stanici (Segment 3)...")

            elif state == 1:
                self.zjisti_smer(loco3)
                # Zpomalit hned při vjezdu čela do segmentu 2, i když konec vlaku ještě opouští stanici
                if self.ma_zastavku(loco3) and zastavka_zpomaleni is None and segments[2] == True:
                    zastavka_zpomaleni = self.zpomal_pred_zastavkou(loco3)
                if segments[3] == False:
                    if clear_start_time is None:
                        clear_start_time = time.time()  # Začneme měřit

                    # Pokud je úsek volný déle než 1.5 sekundy
                    elif (time.time() - clear_start_time) >= 1.5:
                        self.zapis("Vlak 3 je bezpečně na okruhu.")
                        if self.ma_zastavku(loco3):
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
                        if self.zastavit_ve_stanici:
                            self.zapis("Oba vlaky stojí ve stanici, automatický provoz ukončen.")
                            self.faze = "Zastaveno ve stanici"
                            return
                        self.faze = "Pauza ve stanici"
                        self._cekej(4)
                        state = 10
                        clear_start_time = None
                else:
                    clear_start_time = None
                    # ======================================

            # JÍZDA LOKOMOTIVY ZE SEGMENTU 4
            # ======================================
            elif state == 10:
                self.zapis("\n--- FAZE 10: Výjezd lokomotivy ze Segmentu 4 ---")
                self.faze = f"Vlak {self.nazev(loco4)} vyjíždí ze segmentu 4"
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
                if self.ma_zastavku(loco4) and zastavka_zpomaleni is None and segments[2] == True:
                    zastavka_zpomaleni = self.zpomal_pred_zastavkou(loco4)
                # Kontrola uvolnění úseku s ochranou proti falešnému hlášení (mrtvý bod výhybky)
                if segments[4] == False:
                    if clear_start_time is None:
                        clear_start_time = time.time()  # Začneme měřit
                    elif (time.time() - clear_start_time) >= 1.5:
                        self.zapis("Vlak 4 je bezpečně na okruhu.")
                        if self.ma_zastavku(loco4):
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
                        if self.zastavit_ve_stanici:
                            self.zapis("Oba vlaky stojí ve stanici, automatický provoz ukončen.")
                            self.faze = "Zastaveno ve stanici"
                            return
                        self.faze = "Pauza ve stanici"
                        self._cekej(4)
                        state = 0
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
