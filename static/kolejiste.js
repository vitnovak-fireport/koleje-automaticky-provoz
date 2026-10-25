// Schéma kolejiště: čtyřhranný okruh, dole dvoukolejná stanice (koleje 3 a 4).
// Segment 2 = levá polovina horní strany + levá strana + levé zhlaví,
// segment 1 = pravá polovina horní strany + pravá strana + spodní strana + pravé zhlaví.
// Zastávka je na horní straně těsně za předělem segmentů 1 a 2.
// Výhybka 6 je na levém zhlaví, výhybka 5 na pravém. Zvýrazněný jazyk ukazuje, na kterou kolej je přestavená.
// Dva tunely (každý 1 m) v segmentu 1: na horní straně za zastávkou a na pravé straně. Svítí-li, jsou žluté.
const KOLEJISTE_SVG = `
<svg viewBox="0 0 400 245" role="img" aria-label="Schéma kolejiště">
  <g class="seg-kolej" data-seg="2">
    <path d="M250 35 H40 Q20 35 20 55 V170 Q20 190 40 190 H75"/>
    <path d="M45 190 C60 190 65 215 85 215"/>
  </g>
  <g class="seg-kolej" data-seg="1">
    <path d="M250 35 H360 Q380 35 380 55 V195 Q380 215 360 215 H235"/>
    <path d="M245 197 C252 203 258 215 275 215"/>
  </g>
  <g class="seg-kolej" data-seg="3">
    <path d="M75 190 H225 C235 190 240 193 245 197"/>
  </g>
  <g class="seg-kolej" data-seg="4">
    <path d="M85 215 H235"/>
  </g>
  <g class="vyhybka" data-vyhybka="6">
    <path data-kolej="3" d="M45 190 H68"/>
    <path data-kolej="4" d="M45 190 C54 190 59.4 199 66.6 206.2"/>
  </g>
  <g class="vyhybka" data-vyhybka="5">
    <path data-kolej="4" d="M275 215 H252"/>
    <path data-kolej="3" d="M275 215 C264.8 215 258.6 210.7 253.7 205.9"/>
  </g>
  <g class="tunely">
    <title>Tunely</title>
    <rect x="262" y="26" width="46" height="18" rx="4"/>
    <rect x="371" y="70" width="18" height="50" rx="4"/>
  </g>
  <g class="predel">
    <line x1="250" y1="27" x2="250" y2="43"/>
    <line x1="75" y1="182" x2="75" y2="198"/>
    <line x1="85" y1="207" x2="85" y2="223"/>
    <line x1="235" y1="207" x2="235" y2="223"/>
    <line x1="240" y1="203" x2="250" y2="191"/>
  </g>
  <rect class="nastupiste" x="178" y="20" width="68" height="6" rx="2"/>
  <text class="popisek-maly" x="212" y="14" text-anchor="middle">Zastávka</text>
  <text class="popisek" x="38" y="118">2</text>
  <text class="popisek" x="354" y="118">1</text>
  <text class="popisek" data-popisek="3" x="150" y="181" text-anchor="middle">3</text>
  <text class="popisek" data-popisek="4" x="160" y="238" text-anchor="middle">4</text>
  <text class="popisek-maly" data-vyhybka-popisek="6" x="38" y="238" text-anchor="middle">výh. 6</text>
  <text class="popisek-maly" data-vyhybka-popisek="5" x="292" y="238" text-anchor="middle">výh. 5</text>
</svg>`;

// Vloží schéma do prvku (jednou), obarví obsazené segmenty a nakreslí polohu výhybek.
// segmenty: {"1": true, ...}, lokomotivy: {"3": "Banán", "4": "Vectron"} - jména u staničních kolejí,
// vyhybky: {"5": {stav: 0|1|null}}, vyhybkyKoleje: {"5": {"0": 4, "1": 3}} - kam vede která poloha,
// osvetleniTunelu: true | false | null (neznámé)
function vykresliKolejiste(el, segmenty, lokomotivy, vyhybky, vyhybkyKoleje, osvetleniTunelu) {
  if (!el.firstElementChild) el.innerHTML = KOLEJISTE_SVG;
  const tunely = el.querySelector(".tunely");
  tunely.classList.toggle("sviti", osvetleniTunelu === true);
  tunely.classList.toggle("neznama", osvetleniTunelu == null);
  tunely.querySelector("title").textContent = osvetleniTunelu == null
    ? "Tunely – osvětlení neznámé" : `Tunely – osvětlení ${osvetleniTunelu ? "svítí" : "zhasnuté"}`;
  for (const [id, obsazeno] of Object.entries(segmenty)) {
    const seg = el.querySelector(`.seg-kolej[data-seg="${id}"]`);
    if (seg) seg.classList.toggle("obsazeno", obsazeno);
  }
  for (const kolej of ["3", "4"]) {
    const popisek = el.querySelector(`[data-popisek="${kolej}"]`);
    popisek.textContent = lokomotivy && lokomotivy[kolej] ? `${kolej} · ${lokomotivy[kolej]}` : kolej;
  }
  for (const g of el.querySelectorAll(".vyhybka")) {
    const adresa = g.dataset.vyhybka;
    const stav = vyhybky && vyhybky[adresa] ? vyhybky[adresa].stav : null;
    const kolej = stav === null ? undefined : (vyhybkyKoleje[adresa] || {})[stav];
    for (const vetev of g.querySelectorAll("path")) {
      vetev.classList.toggle("aktivni", String(kolej) === vetev.dataset.kolej);
    }
    g.classList.toggle("neznama", kolej === undefined);
    el.querySelector(`[data-vyhybka-popisek="${adresa}"]`).textContent =
      kolej === undefined ? `výh. ${adresa} ?` : `výh. ${adresa} → ${kolej}`;
  }
}

// Vypíše polohy výhybek a na kterou staniční kolej je nastavená cesta.
// vyhybky: {"5": {stav: 0|1|null, cas: "HH:MM:SS", zdroj: "skript"|"Z21"}}, cesta: 3 | 4 | null
function vykresliVyhybky(el, vyhybky, cesta) {
  const polohy = { 0: "rovně", 1: "odbočka" };
  const zdroje = { skript: "povel skriptu", Z21: "hlášení Z21" };
  const radky = Object.entries(vyhybky).map(([adresa, v]) => {
    const radek = document.createElement("div");
    radek.className = "loko";
    const nazev = document.createElement("b");
    nazev.textContent = `Výhybka ${adresa}`;
    const info = document.createElement("span");
    info.className = "muted";
    info.textContent = v.stav === null
      ? "poloha zatím neznámá"
      : `${polohy[v.stav]} (${v.stav}) · ${v.cas} · ${zdroje[v.zdroj] || v.zdroj}`;
    radek.append(nazev, info);
    return radek;
  });
  const souhrn = document.createElement("div");
  souhrn.className = "cesta";
  const znamy = Object.values(vyhybky).every((v) => v.stav !== null);
  souhrn.textContent = cesta
    ? `Cesta nastavena na kolej ${cesta}`
    : znamy ? "Výhybky nejsou nastavené na žádnou staniční kolej" : "Cesta zatím neznámá";
  el.replaceChildren(souhrn, ...radky);
}
