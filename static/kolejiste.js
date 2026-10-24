// Schéma kolejiště: čtyřhranný okruh, dole dvoukolejná stanice (koleje 3 a 4).
// Segment 2 = levá polovina horní strany + levá strana + levé zhlaví,
// segment 1 = pravá polovina horní strany + pravá strana + spodní strana + pravé zhlaví.
// Zastávka je na horní straně těsně za předělem segmentů 1 a 2.
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
</svg>`;

// Vloží schéma do prvku (jednou) a obarví obsazené segmenty.
// segmenty: {"1": true, ...}, lokomotivy: {"3": "Banán", "4": "Vectron"} - jména u staničních kolejí
function vykresliKolejiste(el, segmenty, lokomotivy) {
  if (!el.firstElementChild) el.innerHTML = KOLEJISTE_SVG;
  for (const [id, obsazeno] of Object.entries(segmenty)) {
    const seg = el.querySelector(`.seg-kolej[data-seg="${id}"]`);
    if (seg) seg.classList.toggle("obsazeno", obsazeno);
  }
  for (const kolej of ["3", "4"]) {
    const popisek = el.querySelector(`[data-popisek="${kolej}"]`);
    popisek.textContent = lokomotivy && lokomotivy[kolej] ? `${kolej} · ${lokomotivy[kolej]}` : kolej;
  }
}
