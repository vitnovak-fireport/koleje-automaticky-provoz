import os
import threading
import time
import urllib.request
from collections import deque
from datetime import datetime
from zoneinfo import ZoneInfo

from obsazeni import Obsazenost
from tlacitko import Tlacitko
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
HOUKANI_DELKA = 2  # s - jak dlouho houkačka zní
HOUKANI_PAUZA = 10  # s - nejkratší rozestup houkání z tlačítka

# Mikroslužba pro přehrávání zvuku do reproduktoru (jen v režimu na tlačítko u vlaku vypraveného stiskem)
ZVUK_URL = os.environ.get("ZVUK_URL", "http://localhost:8080")
ZVUK_TIMEOUT = 3  # s

Z21_IP = os.environ.get("Z21_IP", "192.168.0.111")
Z21_PORT = int(os.environ.get("Z21_PORT", "21105"))  # Časování a protokol Z21 jsou v z21.py

NAPAJENI_PO_OBNOVENI = 3  # s - po obnovení napájení počkat, než se srovnají hlášení obsazenosti

# Výhybky ve stanici a jejich nastavení pro výjezd z jednotlivých staničních kolejí (0 = rovně, 1 = odbočka)
VYHYBKY = (5, 6)
CESTY = {
    3: {5: 1, 6: 0},  # Výjezd z koleje 3
    4: {5: 0, 6: 1},  # Výjezd z koleje 4
}

# Osvětlení obou tunelů: DCC adresa dekodéru a číslo funkce (stav hlásí Z21, zobrazuje se na schématu)
OSVETLENI_TUNELU = (3, 1)

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
    "vyjezd_bez_tlacitka": 60,  # Po kolika s vyjede vlak i bez stisku tlačítka (0 = čekat na stisk neomezeně)

    # Zastávka na předělu segmentů 1 a 2 (jen pro vlaky, které ze stanice vjedou nejdříve do segmentu 1)
    # "ano" = vlaky zastaví, "ne" = projedou, "na_znameni" = zastaví, jen když někdo stiskne tlačítko
    "zastavka": "ano" if os.environ.get("ZASTAVKA_POVOLENA", "1") == "1" else "ne",
    "zastavka_cekani": 15,             # Doba stání na zastávce (s)
    "zastavka_rychlost_dojezdu": 30,   # Plíživá rychlost od vjezdu čela do segmentu 2 až do zastavení (0-127)

    # Zvuky do reproduktoru (mikroslužba ZVUK_URL): "ano" = hrají, "ne" = žádné volání
    "zvuky": "ano",
}


class Zastaveno(Exception):
    """Provoz byl zastaven uživatelem."""


class Provoz:
    """Automatický provoz dvou vlaků mezi stanicí (segmenty 3 a 4) a okruhem (segmenty 1 a 2)."""

    def __init__(self, nastaveni=None):
        self.nastaveni = dict(VYCHOZI_NASTAVENI)
        self.nastaveni.update(nastaveni or {})

        # Naučené charakteristiky vlaků podle adresy lokomotivy:
        # smer_pres_seg1 (vjíždí nejdříve do segmentu 1?)
        self.uceni = {}

        self.faze = "Zastaveno"
        self.log = deque(maxlen=200)
        self.zastavit_ve_stanici = False

        # Jízda na okruh a zastávka na znamení (čte je i obsluha tlačítka z vlákna MQTT)
        self.jedouci_loco = None  # Lokomotiva, která vyjela ze stanice a ještě se nevrátila
        self.zastavka_rozhodnuta = False  # Čelo vlaku už vjelo do segmentu 2 - o zastavení je rozhodnuto
        self.zastavka_pozadana = False  # Někdo tlačítkem požádal o zastavení na znamení
        self._zastavka_zamek = threading.Lock()
        self.navrat = {}  # Lokomotiva -> time.time() posledního návratu do stanice
        self.ceka_na_tlacitko = False  # Oba vlaky stojí ve stanici a čekají na stisk (režim na tlačítko)
        self._vyjezd = threading.Event()  # Stisk tlačítka, který vypraví další vlak
        self.jizda_tlacitkem = False  # Jedoucí vlak vypravil stisk tlačítka (ne vyjezd_bez_tlacitka) - hrají zvuky
        self._hlaseni_zastavky = False  # Při vjezdu do segmentu 1 ještě ohlásit zastávku na znamení

        self._houkani_cas = 0.0

        self._stop = threading.Event()
        self._vlakno = None

        # Centrála Z21 - povely lokomotivám a výhybkám, hlášení výhybek a napájení kolejiště
        self.z21 = Z21(Z21_IP, Z21_PORT, VYHYBKY, self.zapis, nazev=self.nazev, ted=self.ted,
                       lokomotivy=lambda: {OSVETLENI_TUNELU[0], self.nastaveni["loco_seg3"], self.nastaveni["loco_seg4"]})

        # Detekce obsazení segmentů přes MQTT
        self.obsazenost = Obsazenost(MQTT_BROKER, MQTT_PORT, MQTT_TOPIC, self.zapis, vychozi={
            1: False,
            2: False,
            3: True,  # Výchozí stav - předpokládáme, že tam vlaky stojí
            4: True
        })

        # Tlačítko pro kolemjdoucí (LoRaWAN přes MQTT The Things Industries)
        self.tlacitko = Tlacitko(TLACITKO_MQTT_BROKER, TLACITKO_MQTT_PORT, TLACITKO_MQTT_USER, TLACITKO_MQTT_PASS,
                                 TLACITKO_MQTT_TOPIC, self.zapis, self.stisk_tlacitka, casova_zona=CASOVA_ZONA)

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
    # 2b. ZVUKY DO REPRODUKTORU (mikroslužba ZVUK_URL)
    # ==========================================
    def prehraj(self, cesta):
        """Na pozadí zavolá mikroslužbu pro přehrávání zvuku, např. cesta="/play". Chyba provoz nezastaví."""
        if self.nastaveni["zvuky"] != "ano":
            return
        def volej():
            try:
                with urllib.request.urlopen(ZVUK_URL + cesta, timeout=ZVUK_TIMEOUT):
                    pass
            except Exception as e:
                self.zapis(f"Zvuk {cesta} se nepodařilo přehrát: {e}")
        threading.Thread(target=volej, daemon=True).start()

    # ==========================================
    # 3. TLAČÍTKO PRO KOLEMJDOUCÍ (příjem zpráv je v tlacitko.py)
    # ==========================================
    def stisk_tlacitka(self):
        """
        Někdo stiskl tlačítko. Podle situace:
        1. oba vlaky čekají ve stanici na stisk -> vyjede vlak, který stojí nejdéle,
        2. vlak jede přes segment 1 a ještě nezpomaluje před zastávkou -> zastaví na znamení,
        3. jinak zahouká jedoucí lokomotiva (nebo ta, která vyjede příště).
        """
        self.tlacitko.zapocitej_stisk()

        if self.bezi() and self.ceka_na_tlacitko:
            self.zapis(f"Tlačítko: vypravuji vlak {self.nazev(self._pristi_loko())}.")
            self._vyjezd.set()
            self.prehraj("/play")
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
                if self.jizda_tlacitkem:
                    self.prehraj("/announce?file=zastavime")
                return

        loco = loco if loco is not None else self._pristi_loko()
        if self.zahoukej(loco):
            self.zapis(f"Tlačítko: {self.nazev(loco)} houká.")

    def lze_menit_lokomotivy(self):
        """Lokomotivy v segmentech jde měnit jen tehdy, když žádný vlak nejede po trati."""
        return not self.bezi() or self.jedouci_loco is None

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
            false_count = false_count + 1 if self.obsazenost.segmenty[segment_id] == False else 0
            self._cekej(0.1)
        return self.obsazenost.zmeneno[segment_id]

    def zjisti_smer(self, loco):
        """Při výjezdu ze stanice zjistí, jestli vlak vjede nejdříve do segmentu 1, nebo 2."""
        info = self.uceni.setdefault(loco, {})
        if "smer_pres_seg1" in info:
            return
        segmenty, zmeneno = self.obsazenost.segmenty, self.obsazenost.zmeneno
        if segmenty[1] or segmenty[2]:
            if segmenty[1] and segmenty[2]:
                prvni = 1 if zmeneno[1] <= zmeneno[2] else 2
            else:
                prvni = 1 if segmenty[1] else 2
            info["smer_pres_seg1"] = prvni == 1
            self.zapis(f"Učení: {self.nazev(loco)} vjíždí ze stanice nejdříve do segmentu {prvni}.")

    def ohlas_zastavku(self, loco):
        """Vlak vypravený tlačítkem vjel ze stanice do segmentu 1 - ohlásí, že může zastavit na znamení."""
        segmenty = self.obsazenost.segmenty
        if not self._hlaseni_zastavky or not (segmenty[1] or segmenty[2]):
            return
        self._hlaseni_zastavky = False  # Hlásí se jen jednou za jízdu, a to při vjezdu na okruh
        if (self.nastaveni["zastavka"] == "na_znameni" and self.zastavka_mozna(loco)
                and not self.zastavka_pozadana):
            self.prehraj("/announce?file=zastavka-na-znameni")

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
            while self.obsazenost.segmenty[2] == False:
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
            self.jizda_tlacitkem = False
            self._hlaseni_zastavky = False

    def _vyjizdi(self, loco):
        """Vlak vyjíždí ze stanice - nová jízda, o zastávce zatím nepadlo rozhodnutí."""
        with self._zastavka_zamek:
            self.jedouci_loco = loco
            self.zastavka_rozhodnuta = False
            self.zastavka_pozadana = False
        self._hlaseni_zastavky = self.jizda_tlacitkem

    def _vratil_se(self, loco):
        self.navrat[loco] = time.time()
        self.jedouci_loco = None
        self.jizda_tlacitkem = False

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
        """V režimu na tlačítko počká na stisk (nejdéle vyjezd_bez_tlacitka s) a vrátí stav výjezdu vlaku,
        který stojí nejdéle. Jinak (i po přepnutí do nepřetržitého provozu během čekání) vrátí pristi."""
        self.jizda_tlacitkem = False
        if self.nastaveni["rezim"] != "na_tlacitko":
            return pristi
        self._vyjezd.clear()
        self.ceka_na_tlacitko = True
        self.faze = "Čeká na stisk tlačítka"
        self.zapis("Oba vlaky stojí ve stanici, čekám na stisk tlačítka.")
        zacatek = time.time()
        try:
            while not self._vyjezd.is_set():
                if self.zastavit_ve_stanici:
                    return None
                if self.nastaveni["rezim"] != "na_tlacitko":
                    return pristi
                limit = self.nastaveni["vyjezd_bez_tlacitka"]  # Dá se měnit z webu i během čekání
                if limit and time.time() - zacatek >= limit:
                    self.zapis(f"Nikdo nestiskl tlačítko {limit} s, vypravuji vlak {self.nazev(self._pristi_loko())}.")
                    break
                self._cekej(0.2)
        finally:
            self.ceka_na_tlacitko = False
        self.jizda_tlacitkem = self._vyjezd.is_set()  # Vypravil ho stisk, ne vyjezd_bez_tlacitka
        return 0 if self._pristi_loko() == self.nastaveni["loco_seg3"] else 10

    def main_loop(self):
        self.zapis("Spouštím automatický provoz...")
        segments = self.obsazenost.segmenty

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
                self.ohlas_zastavku(loco3)
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
                self.ohlas_zastavku(loco4)
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
    provoz.obsazenost.pripojit()
    provoz.tlacitko.pripojit()
    provoz.z21.pripojit()
    provoz.spustit()

    try:
        while provoz.bezi():
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\nUkončuji skript...")
        provoz.zastavit()
        provoz.obsazenost.odpojit()
        provoz.z21.zavrit()
