# koleje-automaticky-provoz

Stavědlo Drozďák – automatický provoz.

Sledování jen pro čtení běží na <https://stavedlo.drozdak.cz>.

Webová aplikace pro automatický provoz dvou vlaků (stanice = segmenty 3 a 4, okruh = segmenty 1 a 2)
přes centrálu Z21 a MQTT detekci obsazení. Ovládá se z prohlížeče na mobilu nebo počítači.

## Spuštění na Raspberry Pi (Podman)

Na Pi stáhni repozitář a sestav obraz:

```bash
git clone git@github.com:vitnovak-fireport/koleje-automaticky-provoz.git
```

```bash
cd koleje-automaticky-provoz && podman build -t koleje-automaticky-provoz .
```

(Pro `git@github.com:` musí mít Pi SSH klíč přidaný na GitHubu. U veřejného repozitáře jde i
`git clone https://github.com/vitnovak-fireport/koleje-automaticky-provoz.git`.)

```bash
podman run -d --name koleje-automaticky-provoz --restart=always -p 8081:8081 -p 8082:8082 -v koleje-automaticky-provoz-data:/data -e Z21_IP=192.168.0.111 -e MQTT_BROKER=mqtt.drozdak.cz koleje-automaticky-provoz
```

Aplikace má dvě webová rozhraní:

- **Ovládání** na `http://<adresa-pi>:8081` (např. `http://raspberrypi.local:8081`) – spouštění,
  zastavování a nastavení. Jen pro domácí síť.
- **Sledování jen pro čtení** na `http://<adresa-pi>:8082` – obsazení segmentů, lokomotivy
  na kolejišti a co se právě děje. Neumí nic ovládat ani měnit, takže ho lze vystavit do internetu.

Logy: `podman logs -f koleje-automaticky-provoz`.

### Aktualizace na novou verzi

```bash
cd ~/koleje-automaticky-provoz && git pull && podman build -t koleje-automaticky-provoz .
```

```bash
podman rm -f koleje-automaticky-provoz
```

Pak znovu stejný `podman run` jako výše. Nastavení ve volume `koleje-automaticky-provoz-data` zůstane.

### Automatický start po restartu Pi

`--restart=always` u rootless Podmanu potřebuje zapnutou službu:

```bash
systemctl --user enable --now podman-restart.service
```

```bash
sudo loginctl enable-linger $USER
```

## Proměnné prostředí

| Proměnná | Výchozí | Význam |
|---|---|---|
| `Z21_IP` | `192.168.0.111` | Adresa centrály Z21 |
| `Z21_PORT` | `21105` | UDP port Z21 |
| `MQTT_BROKER` | `mqtt.drozdak.cz` | MQTT broker s obsazením segmentů |
| `MQTT_PORT` | `1883` | Port MQTT |
| `MQTT_TOPIC` | `koleje/segment/+` | Topic obsazení |
| `WEB_PORT` | `8081` | Port ovládání |
| `VIEW_PORT` | `8082` | Port sledování jen pro čtení |
| `ZASTAVKA_POVOLENA` | `1` | Výchozí stav zastávky (`1` = vlaky zastaví, `0` = projedou), pokud ještě není uložené nastavení |
| `TLACITKO_MQTT_BROKER` | *(prázdné)* | MQTT The Things Industries s LoRaWAN tlačítkem |
| `TLACITKO_MQTT_PORT` | `8883` | Port MQTT tlačítka (TLS) |
| `TLACITKO_MQTT_USER` | *(prázdné)* | Uživatel MQTT tlačítka (`<aplikace>@<tenant>` v TTI) |
| `TLACITKO_MQTT_PASS` | *(prázdné)* | API klíč TTI – čtení uplinků a zápis downlinků |
| `TLACITKO_MQTT_TOPIC` | *(prázdné)* | Topic uplinků tlačítka, např. `v3/<aplikace>@<tenant>/devices/+/up` |

Bez `TLACITKO_MQTT_BROKER`, `_USER`, `_PASS` a `_TOPIC` je tlačítko vypnuté.
| `AUTOSTART` | `0` | `1` = po startu kontejneru rovnou rozjet provoz |
| `CASOVA_ZONA` | `Europe/Prague` | Časové pásmo pro časy v záznamu a u výhybek (nezávisle na čase systému) |

Rychlosti, adresy lokomotiv a zastávku jde měnit ve webu za běhu; ukládají se do `/data/nastaveni.json`.

## Tlačítko pro kolemjdoucí

LoRaWAN tlačítko Mutelcor MTC-PB01 posílá stisk přes The Things Industries. Aplikace každý alarm potvrdí
downlinkem (jinak ho tlačítko opakuje) a opakované zprávy se stejným Alarm ID bere jako jeden stisk.
Heartbeat jen zapíše do záznamu. Stisk podle situace:

1. V režimu **na tlačítko** čekají oba vlaky ve stanici – vyjede ten, který stojí nejdéle. Když nikdo
   nestiskne, vyjede sám po době nastavené v ovládání (výchozí 60 s, 0 = čekat na stisk neomezeně).
2. Zastávka je **na znamení** a vlak jede přes segment 1, ale ještě nevjel do segmentu 2 – zastaví na zastávce.
3. Jinak lokomotiva zahouká (funkce `houkacka` v `LOKOMOTIVY` v `automaticky_provoz.py`).

Přístupové údaje předej jen proměnnými prostředí, do `podman run` přidej `-e TLACITKO_MQTT_BROKER=…
-e TLACITKO_MQTT_USER=… -e TLACITKO_MQTT_PASS=… -e TLACITKO_MQTT_TOPIC=…` (nebo je dej do souboru a použij
`--env-file`, který necommituj).
V ovládání jde tlačítko vyzkoušet i z webu.

## Bez kontejneru

```bash
pip install -r requirements.txt
```

```bash
python app.py
```

Původní režim bez webu (Ctrl+C zastaví vlaky): `python automaticky_provoz.py`.

## Sledování z internetu přes Cloudflare Tunnel

V Cloudflare Zero Trust (Networks → Tunnels) vytvoř tunel, zkopíruj jeho token a na Pi spusť:

```bash
podman run -d --name cloudflared --restart=always --network host docker.io/cloudflare/cloudflared:latest tunnel --no-autoupdate run --token <TOKEN>
```

V nastavení tunelu přidej Public Hostname (např. `vlacky.tvojedomena.cz`) se službou
`http://localhost:8082`. **Nikdy do tunelu nedávej port 8081** – to je ovládání bez přihlašování.

Sledování na portu 8082 nemá žádné ovládací adresy (POST), neukazuje IP adresu Z21 ani MQTT
brokeru, nastavení ani chybové hlášky.

## Bezpečnost

Ovládání (port 8081) nemá přihlašování – kdokoli ve stejné síti může vlaky ovládat. Nepřesměrovávej ho
z internetu ani ho nedávej do Cloudflare Tunnelu; pro vzdálené ovládání použij VPN (např. Tailscale/WireGuard)
nebo ho v Cloudflare schovej za Access s přihlášením.
