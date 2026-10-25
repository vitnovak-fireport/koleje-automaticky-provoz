import json
import os
import threading

from flask import Flask, jsonify, request, send_from_directory
from waitress import create_server

from automaticky_provoz import Provoz, VYCHOZI_NASTAVENI, LOKOMOTIVY, VYHYBKY, CESTY, MQTT_BROKER, Z21_IP

# ==========================================
# KONFIGURACE WEBU (proměnné prostředí)
# ==========================================
WEB_PORT = int(os.environ.get("WEB_PORT", "8081"))    # Ovládání - jen do domácí sítě
VIEW_PORT = int(os.environ.get("VIEW_PORT", "8082"))  # Sledování jen pro čtení - lze vystavit do internetu
DATA_DIR = os.environ.get("DATA_DIR", os.path.join(os.path.dirname(os.path.abspath(__file__)), "data"))
SOUBOR_NASTAVENI = os.path.join(DATA_DIR, "nastaveni.json")
AUTOSTART = os.environ.get("AUTOSTART", "0") == "1"  # 1 = po startu kontejneru rovnou rozjet provoz

# Na kterou staniční kolej vede která poloha výhybky, např. {"5": {"0": 4, "1": 3}} - pro schéma na webu
VYHYBKY_KOLEJE = {
    str(vyhybka): {str(polohy[vyhybka]): kolej for kolej, polohy in CESTY.items()}
    for vyhybka in VYHYBKY
}

# Povolené rozsahy nastavení, která jdou měnit z webu
ROZSAHY = {
    "speed_seg3": (0, 127),
    "speed_seg4": (0, 127),
    "rychlost_vjezdu": (0, 127),
    "zastavka_cekani": (0, 600),
    "zastavka_rychlost_dojezdu": (0, 127),
}

# Nastavení s výběrem z pevných možností
MOZNOSTI = {
    "rezim": ("nepretrzity", "na_tlacitko"),
    "zastavka": ("ano", "ne", "na_znameni"),
}


def nacti_nastaveni():
    """Načte uložené nastavení (soubor v DATA_DIR přežije restart kontejneru)."""
    try:
        with open(SOUBOR_NASTAVENI, encoding="utf-8") as f:
            ulozene = json.load(f)
        if "zastavka_povolena" in ulozene and "zastavka" not in ulozene:  # Nastavení ze starší verze
            ulozene["zastavka"] = "ano" if ulozene["zastavka_povolena"] else "ne"
        return {k: v for k, v in ulozene.items() if k in VYCHOZI_NASTAVENI}
    except (OSError, ValueError):
        return {}


def uloz_nastaveni(nastaveni):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(SOUBOR_NASTAVENI, "w", encoding="utf-8") as f:
        json.dump(nastaveni, f, indent=2, ensure_ascii=False)


def over_nastaveni(data):
    """Zkontroluje změny nastavení z webu. Vrací (změny, chyba)."""
    zmeny = {}
    for klic, hodnota in data.items():
        if klic in MOZNOSTI:
            if hodnota not in MOZNOSTI[klic]:
                return None, f"{klic} musí být jedno z {', '.join(MOZNOSTI[klic])}"
            zmeny[klic] = hodnota
        elif klic in ("loco_seg3", "loco_seg4"):
            if hodnota not in LOKOMOTIVY or isinstance(hodnota, bool):
                return None, f"{klic} musí být jedna z lokomotiv {sorted(LOKOMOTIVY)}"
            zmeny[klic] = hodnota
        elif klic in ROZSAHY:
            od, do = ROZSAHY[klic]
            if not isinstance(hodnota, int) or isinstance(hodnota, bool) or not od <= hodnota <= do:
                return None, f"{klic} musí být celé číslo {od}-{do}"
            zmeny[klic] = hodnota
        else:
            return None, f"Neznámé nastavení: {klic}"
    loco3 = zmeny.get("loco_seg3", provoz.nastaveni["loco_seg3"])
    loco4 = zmeny.get("loco_seg4", provoz.nastaveni["loco_seg4"])
    if loco3 == loco4:
        return None, "V segmentech 3 a 4 nemůže stát stejná lokomotiva"
    return zmeny, None


provoz = Provoz(nacti_nastaveni())
app = Flask(__name__, static_folder="static")


@app.get("/")
def index():
    return send_from_directory(app.static_folder, "index.html")


@app.get("/api/stav")
def stav():
    return jsonify({
        "bezi": provoz.bezi(),
        "faze": provoz.faze,
        "zastavit_ve_stanici": provoz.zastavit_ve_stanici,
        "mqtt_pripojeno": provoz.mqtt_pripojeno,
        "tlacitko": stav_tlacitka(),
        "z21_odpovida": provoz.z21.odpovida(),
        "napajeni": provoz.z21.napajeni,
        "mereni": provoz.z21.mereni_napajeni(),
        "mqtt_broker": MQTT_BROKER,
        "z21_ip": Z21_IP,
        "segmenty": {str(k): v for k, v in provoz.segments.items()},
        "vyhybky": {str(k): v for k, v in provoz.z21.vyhybky.items()},
        "cesta": provoz.cesta(),
        "vyhybky_koleje": VYHYBKY_KOLEJE,
        "uceni": {str(k): v for k, v in provoz.uceni.items()},
        "nastaveni": provoz.nastaveni,
        "lokomotivy": {str(k): v["nazev"] for k, v in LOKOMOTIVY.items()},
        "log": list(provoz.log)[-60:],
    })


def stav_tlacitka():
    return {
        "zapnute": provoz.tlacitko.zapnute,
        "pripojeno": provoz.tlacitko.pripojeno,
        "ceka": provoz.ceka_na_tlacitko,
        "stisky_dnes": provoz.tlacitko.stisky,
        "posledni": provoz.tlacitko.posledni,
        "baterie_v": provoz.tlacitko.baterie,
    }


@app.post("/api/spustit")
def spustit():
    provoz.spustit()
    return jsonify(ok=True)


@app.post("/api/zastavit")
def zastavit():
    provoz.zastavit()
    return jsonify(ok=True)


@app.post("/api/zastavit-ve-stanici")
def zastavit_ve_stanici():
    if provoz.bezi():
        provoz.zastavit_ve_stanici = True
        provoz.zapis("Provoz se ukončí, až vlak dojede do stanice.")
    return jsonify(ok=True)


@app.post("/api/tlacitko")
def tlacitko():
    """Stisk tlačítka z webu - na vyzkoušení bez chůze k tlačítku."""
    provoz.zapis("Tlačítko stisknuto z ovládání.")
    provoz.stisk_tlacitka()
    return jsonify(ok=True)


@app.post("/api/zapomenout")
def zapomenout():
    provoz.uceni.clear()
    provoz.zapis("Naučené charakteristiky vlaků smazány, při příští jízdě se naučí znovu.")
    return jsonify(ok=True)


@app.post("/api/nastaveni")
def nastaveni():
    zmeny, chyba = over_nastaveni(request.get_json(silent=True) or {})
    if chyba:
        return jsonify(ok=False, chyba=chyba), 400
    zmeneno = {k: v for k, v in zmeny.items() if provoz.nastaveni.get(k) != v}
    nove = dict(provoz.nastaveni)
    nove.update(zmeny)
    provoz.nastaveni = nove  # Nahrazení celého slovníku - běžící automat si ho přečte v dalším kroku
    uloz_nastaveni(nove)
    if zmeneno:
        provoz.zapis("Nastavení změněno: " + ", ".join(f"{k} = {v}" for k, v in zmeneno.items()))
    return jsonify(ok=True, nastaveni=nove)


# ==========================================
# SLEDOVÁNÍ JEN PRO ČTENÍ (samostatný port, žádné ovládání)
# ==========================================
view_app = Flask(__name__, static_folder=None)

# Zprávy, které do veřejného sledování nepatří (nastavení, adresy serverů, chyby)
SKRYTE_ZPRAVY = ("Nastavení změněno", "Připojeno k MQTT", "Odpojeno od MQTT", "Chyba",
                 "Tlačítko poslalo", "Tlačítko je vypnuté", "Tlačítko stisknuto z ovládání")


@view_app.get("/")
def view_index():
    return send_from_directory(app.static_folder, "view.html")


@view_app.get("/static/<any('styl.css', 'kolejiste.js'):soubor>")
def view_static(soubor):
    return send_from_directory(app.static_folder, soubor)


@view_app.get("/api/stav")
def view_stav():
    n = provoz.nastaveni
    lokomotivy = []
    for loco, kolej in ((n["loco_seg3"], 3), (n["loco_seg4"], 4)):
        lokomotivy.append({
            "adresa": loco,
            "jmeno": LOKOMOTIVY[loco]["nazev"] if loco in LOKOMOTIVY else f"Lokomotiva {loco}",
            "kolej": kolej,
            "rychlost": provoz.z21.rychlosti.get(loco, 0),
        })
    return jsonify({
        "bezi": provoz.bezi(),
        "faze": provoz.faze,
        "zastavka": n["zastavka"],
        "rezim": n["rezim"],
        "tlacitko": {
            "zapnute": provoz.tlacitko.zapnute,
            "ceka": provoz.ceka_na_tlacitko,
            "stisky_dnes": provoz.tlacitko.stisky,
        },
        "segmenty": {str(k): v for k, v in provoz.segments.items()},
        "vyhybky": {str(k): v for k, v in provoz.z21.vyhybky.items()},
        "cesta": provoz.cesta(),
        "vyhybky_koleje": VYHYBKY_KOLEJE,
        "lokomotivy": lokomotivy,
        "mereni": provoz.z21.mereni_napajeni(),
        "log": [z for z in provoz.log if not z[1].startswith(SKRYTE_ZPRAVY)][-30:],
    })


if __name__ == "__main__":
    provoz.pripojit_mqtt()
    provoz.tlacitko.pripojit()
    provoz.z21.pripojit()
    if AUTOSTART:
        provoz.spustit()
    ovladani = create_server(app, host="0.0.0.0", port=WEB_PORT, threads=4)
    sledovani = create_server(view_app, host="0.0.0.0", port=VIEW_PORT, threads=4)
    print(f"Ovládání běží na http://0.0.0.0:{WEB_PORT}")
    print(f"Sledování (jen pro čtení) běží na http://0.0.0.0:{VIEW_PORT}")
    threading.Thread(target=ovladani.run, daemon=True).start()
    sledovani.run()
