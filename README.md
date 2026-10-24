# Zahradní železnice – automatický provoz

Webová aplikace pro automatický provoz dvou vlaků (stanice = segmenty 3 a 4, okruh = segmenty 1 a 2)
přes centrálu Z21 a MQTT detekci obsazení. Ovládá se z prohlížeče na mobilu nebo počítači.

## Spuštění na Raspberry Pi (Podman)

Zkopíruj složku projektu na Pi (např. `scp -r zahradni-zeleznice pi@raspberrypi.local:`), pak na Pi:

```bash
cd zahradni-zeleznice
podman build -t zeleznice .
```

```bash
podman run -d --name zeleznice --restart=always -p 8081:8081 -p 8082:8082 -v zeleznice-data:/data -e Z21_IP=192.168.0.111 -e MQTT_BROKER=mqtt.drozdak.cz zeleznice
```

Aplikace má dvě webová rozhraní:

- **Ovládání** na `http://<adresa-pi>:8081` (např. `http://raspberrypi.local:8081`) – spouštění,
  zastavování a nastavení. Jen pro domácí síť.
- **Sledování jen pro čtení** na `http://<adresa-pi>:8082` – obsazení segmentů, lokomotivy
  na kolejišti a co se právě děje. Neumí nic ovládat ani měnit, takže ho lze vystavit do internetu.

Logy: `podman logs -f zeleznice`. Aktualizace po změně kódu: `podman build` znovu, pak
`podman rm -f zeleznice` a znovu `podman run`.

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
| `ZASTAVKA_POVOLENA` | `1` | Výchozí stav zastávky (`1`/`0`), pokud ještě není uložené nastavení |
| `AUTOSTART` | `0` | `1` = po startu kontejneru rovnou rozjet provoz |

Rychlosti, adresy lokomotiv a zastávku jde měnit ve webu za běhu; ukládají se do `/data/nastaveni.json`.

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
