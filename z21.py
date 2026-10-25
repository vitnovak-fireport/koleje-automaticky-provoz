import socket
import struct
import threading
import time

# ==========================================
# KONFIGURACE PROTOKOLU Z21
# ==========================================
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

POLOHY_VYHYBKY = {0: "rovně", 1: "odbočka"}


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


class Z21:
    """Komunikace s centrálou Z21 (UDP): povely lokomotivám a výhybkám s potvrzováním, hlášení výhybek,
    lokomotiv a napájení kolejiště."""

    def __init__(self, ip, port, vyhybky, zapis, nazev=None, ted=None):
        """
        :param vyhybky: adresy výhybek, jejichž polohu sledujeme
        :param zapis: funkce pro zápis zprávy do logu
        :param nazev: funkce, která z adresy lokomotivy udělá jméno pro výpisy
        :param ted: funkce vracející aktuální čas HH:MM:SS (u poloh výhybek)
        """
        self.adresa = (ip, port)
        self.zapis = zapis
        self.nazev = nazev or (lambda adresa: f"Lokomotiva {adresa}")
        self.ted = ted or (lambda: time.strftime("%H:%M:%S"))

        self.rychlosti = {}  # Poslední rychlost odeslaná každé lokomotivě
        self.smery = {}  # Poslední směr odeslaný každé lokomotivě
        self.lokomotivy_z21 = {}  # Rychlost a směr lokomotiv, jak je hlásí Z21: adresa -> (rychlost, směr)
        # Stav výhybek: stav (0/1/None = neznámý), cas (HH:MM:SS), zdroj ("skript" = povel ze skriptu, "Z21" = hlášení centrály)
        self.vyhybky = {adresa: {"stav": None, "cas": None, "zdroj": None} for adresa in vyhybky}
        self.posledni_zprava = None  # time.time() poslední zprávy od Z21
        self.napajeni = None  # Stav napájení kolejiště (NAPAJENI_*), None = zatím neznámý
        self.mereni = None  # Poslední LAN_SYSTEMSTATE od Z21 (napětí, proud, teplota), viz mereni_napajeni()
        self.mereni_cas = 0.0  # time.time() posledního měření

        # Povely čekající na potvrzení od Z21: klíč ("loko"/"vyhybka"/"funkce", adresa, ...) -> Povel
        self._povely = {}
        self._povely_zamek = threading.Lock()

        # UDP Socket pro Z21 (navázaný na pevný lokální port, aby šlo přijímat odpovědi centrály)
        self.udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_sock.bind(("", 0))

    @staticmethod
    def checksum(data):
        """Vypočítá XOR kontrolní součet pro Z21 protokol."""
        cs = 0
        for byte in data:
            cs ^= byte
        return cs

    def pripojit(self):
        """Přihlásí se u Z21 k hlášením o výhybkách, zjistí jejich polohu a dál poslouchá na pozadí."""
        threading.Thread(target=self._smycka, daemon=True).start()

    def zavrit(self):
        self.udp_sock.close()

    def odpovida(self):
        return self.posledni_zprava is not None and time.time() - self.posledni_zprava < 2 * Z21_KEEPALIVE + 10

    def mereni_napajeni(self):
        """Napětí, proud a výkon hlavního výstupu Z21, nebo None, pokud čerstvé měření chybí.
        Booster na B-Busu nic nehlásí, jeho odběr v hodnotách není."""
        if self.mereni is None or time.time() - self.mereni_cas > Z21_MERENI_PLATNOST:
            return None
        return dict(self.mereni, vykon_w=round(self.mereni["napeti_v"] * self.mereni["proud_a"], 1))

    # ==========================================
    # POVELY
    # ==========================================
    def set_turnout(self, address, state):
        """
        Přepne DCC výhybku přes Z21 centrálu. Nečeká na potvrzení - vrací Povel, na jehož vyrizeno lze počkat.

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
        return povel

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

    def set_loco_function(self, address, cislo, zapnuto):
        """Zapne nebo vypne funkci dekodéru F<cislo> (0-31), např. houkačku."""
        # LAN_X_SET_LOCO_FUNCTION: 0xE4, 0xF8, Adr_MSB, Adr_LSB, TTNNNNNN (TT: 00 = vypnout, 01 = zapnout)
        data = [0xE4, 0xF8, *self._loko_adresa(address), (0x40 if zapnuto else 0x00) | (cislo & 0x3F)]
        packet = bytes([0x0A, 0x00, 0x40, 0x00] + data + [self.checksum(data)])
        stav = "zapnuto" if zapnuto else "vypnuto"
        self._posli_s_potvrzenim(("funkce", address, cislo), zapnuto, [packet, self._paket_dotaz_loko(address)],
                                 f"{self.nazev(address)}: F{cislo} {stav}")
        self.zapis(f"-> {self.nazev(address)}: F{cislo} {stav}")

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

    @staticmethod
    def _paket_dotaz_vyhybka(address):
        # LAN_X_GET_TURNOUT_INFO
        z21_addr = address - 1
        addr_high, addr_low = (z21_addr >> 8) & 0xFF, z21_addr & 0xFF
        return bytes([0x08, 0x00, 0x40, 0x00, 0x43, addr_high, addr_low, 0x43 ^ addr_high ^ addr_low])

    def _prihlaseni(self):
        # LAN_SET_BROADCASTFLAGS (nahrazují předchozí, ne přidávají):
        # 0x00000001 = hlášení o jízdě a přestavování (mj. LAN_X_TURNOUT_INFO)
        # 0x00000100 = LAN_SYSTEMSTATE_DATACHANGED každou sekundu (napětí, proud, teplota)
        self.udp_sock.sendto(bytes([0x08, 0x00, 0x50, 0x00, 0x01, 0x01, 0x00, 0x00]), self.adresa)

    def _dotaz_vyhybka(self, address):
        self.udp_sock.sendto(self._paket_dotaz_vyhybka(address), self.adresa)

    def _dotaz_stav(self):
        # LAN_X_GET_STATUS - odpovědí je LAN_X_STATUS_CHANGED se stavem napájení kolejiště
        self.udp_sock.sendto(bytes([0x07, 0x00, 0x40, 0x00, 0x21, 0x24, 0x05]), self.adresa)

    # ==========================================
    # POTVRZOVÁNÍ POVELŮ
    # ==========================================
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
            self.udp_sock.sendto(paket, self.adresa)
        if povel.deaktivace:
            threading.Timer(VYHYBKA_PULZ, self.udp_sock.sendto, (povel.deaktivace, self.adresa)).start()

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

    def _opakuj(self):
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

    # ==========================================
    # PŘÍJEM HLÁŠENÍ
    # ==========================================
    def _smycka(self):
        self.udp_sock.settimeout(0.1)  # Krátký timeout, aby se včas opakovaly nepotvrzené povely
        posledni_prihlaseni = 0
        posledni_dotaz_stav = 0
        while True:
            try:
                if time.time() - posledni_prihlaseni >= Z21_KEEPALIVE:
                    self._prihlaseni()
                    if posledni_prihlaseni == 0 or not self.odpovida():
                        for adresa in self.vyhybky:
                            self._dotaz_vyhybka(adresa)
                    posledni_prihlaseni = time.time()
                if time.time() - posledni_dotaz_stav >= Z21_STAV_INTERVAL:
                    self._dotaz_stav()
                    # Z21 první pakety od nového klienta často zahodí, proto se ptáme, dokud polohu nezjistíme
                    for adresa in self.vyhybky:
                        if self.vyhybky[adresa]["stav"] is None:
                            self._dotaz_vyhybka(adresa)
                    posledni_dotaz_stav = time.time()
                self._opakuj()
                data, _ = self.udp_sock.recvfrom(1024)
                self._zpracuj(data)
            except socket.timeout:
                continue
            except OSError:
                if self.udp_sock.fileno() == -1:
                    return  # Socket byl zavřen při ukončení skriptu
                time.sleep(1)

    def _zpracuj(self, data):
        """Projde zprávy od Z21 (jeden UDP paket jich může obsahovat víc) a vybere hlášení o výhybkách,
        lokomotivách a napájení kolejiště."""
        self.posledni_zprava = time.time()
        i = 0
        while i + 4 <= len(data):
            delka = data[i] | (data[i + 1] << 8)
            if delka < 4:
                return
            hlavicka = data[i + 2] | (data[i + 3] << 8)
            zprava = data[i + 4:i + delka]
            i += delka
            if hlavicka == 0x84 and len(zprava) >= 12:
                self._systemstate(zprava)
                continue
            if hlavicka != 0x40 or not zprava:
                continue
            x = zprava[0]
            # LAN_X_TURNOUT_INFO: 0x43, FAdr_MSB, FAdr_LSB, 000000ZZ (01 = poloha 0, 10 = poloha 1)
            if x == 0x43 and delka == 9:
                adresa = ((zprava[1] << 8) | zprava[2]) + 1
                stav = {0b01: 0, 0b10: 1}.get(zprava[3] & 0x03)
                potvrzeno = stav is not None and self._potvrd(("vyhybka", adresa), lambda o: o == stav)
                self._uloz_stav_vyhybky(adresa, stav, "skript" if potvrzeno else "Z21")
            # LAN_X_LOCO_INFO: 0xEF, Adr_MSB, Adr_LSB, 0000BFFF (FFF = 4 -> 128 kroků), RVVVVVVV, ...
            elif x == 0xEF and len(zprava) >= 5:
                adresa = ((zprava[1] & 0x3F) << 8) | zprava[2]
                self._loko_info(adresa, zprava[4] & 0x7F, 1 if zprava[4] & 0x80 else 0, (zprava[3] & 0x07) == 4)
                self._loko_funkce(adresa, zprava)
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

    def _uloz_stav_vyhybky(self, address, state, zdroj):
        """Zapamatuje si polohu výhybky. Změnu, kterou nahlásí Z21 (např. ruční přestavení), zapíše do logu."""
        if address not in self.vyhybky:
            return
        puvodni = self.vyhybky[address]["stav"]
        if zdroj == "Z21" and state == puvodni:
            return  # Jen další hlášení stejné polohy (např. odpověď na dotaz) - ponecháme původní zdroj
        self.vyhybky[address] = {"stav": state, "cas": self.ted(), "zdroj": zdroj}
        if zdroj == "Z21" and state != puvodni:
            self.zapis(f"Z21 hlásí: Výhybka {address}: {POLOHY_VYHYBKY.get(state, 'neznámá poloha')} ({state})")

    def _systemstate(self, zprava):
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

    def _loko_info(self, adresa, rychlost, smer, kroky128):
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

    def _loko_funkce(self, adresa, zprava):
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
