import os
import csv
import re
import time
import math
import unicodedata
import urllib.parse
from dataclasses import dataclass
from typing import List, Optional, Tuple

import requests

# =========================
# Pastas de entrada/saída
# =========================
INPUT_DIR = "input"
OUTPUT_DIR = "output"
INPUT_FILE = os.path.join(INPUT_DIR, "links.txt")
SAIDA_CSV_PTBR = os.path.join(OUTPUT_DIR, "resultado_excel_ptbr.csv")  # ';' como separador, ponto como decimal
SAIDA_FALHAS = os.path.join(OUTPUT_DIR, "links_falhos.txt")            # lista dos links que falharam

# =========================
# Configuração de rede (segura)
# =========================
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"
)
HEADERS = {"User-Agent": USER_AGENT, "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8"}

# Whitelist de hosts permitidos (evita seguir redirecionamento para domínios suspeitos)
ALLOWED_HOSTS = {
    "maps.app.goo.gl",
    "www.google.com",
    "google.com",
    "www.google.com.br",
    "google.com.br",
    "maps.google.com",
    "www.google.com.br",
}

# Nominatim (respeite as políticas)
NOMINATIM_BASE = "https://nominatim.openstreetmap.org/reverse"
NOMINATIM_HEADERS = {
    "User-Agent": "MapsLinksExtractor/1.3 (contato: seu-email@exemplo.com)"
}
NOMINATIM_DELAY_SECONDS = 1.0

# Overpass para achar POIs próximos quando cair em vias
OVERPASS_URL = "https://overpass-api.de/api/interpreter"
OVERPASS_HEADERS = {
    "User-Agent": "MapsLinksExtractor/1.3 (contato: seu-email@exemplo.com)"
}
OVERPASS_DELAY_SECONDS = 1.0

# =========================
# Utilidades
# =========================
@dataclass
class Resultado:
    lugar: str
    lat: Optional[float]
    lon: Optional[float]
    link: str

LATLON_AT = re.compile(r"@(-?\d+(?:\.\d+)?),\s*(-?\d+(?:\.\d+)?)")
LAT_3D = re.compile(r"!3d(-?\d+(?:\.\d+)?)")
LON_4D = re.compile(r"!4d(-?\d+(?:\.\d+)?)")

# Palavras-chave que indicam via/rodovia/acesso (não é POI “nomeado”)
VIA_KEYWORDS = (
    "rodovia", "estrada", "avenida", "rua", "acesso", "br-", "br ", "alça",
    "linha", "viaduto", "trevo", "marginal", "r.", "av.", "br-", "km "
)

def is_probably_road(name: str) -> bool:
    s = name.strip().lower()
    return any(k in s for k in VIA_KEYWORDS)

def sanitize_text(s: str) -> str:
    """Normaliza e remove caracteres que quebram CSV (controle/zero-width/emoji)."""
    if not s:
        return s
    s = unicodedata.normalize("NFKC", s.replace("+", " ")).strip()
    s = "".join(ch for ch in s if unicodedata.category(ch) not in {"Cc", "Cf", "Cs"})
    s = s.replace("\r", " ").replace("\n", " ").replace("\t", " ")
    s = re.sub(r"\s{2,}", " ", s)
    for bad in ("™", "®", "©"):
        s = s.replace(bad, "")
    return s.strip()

def safe_follow(url: str, timeout: float = 12.0) -> str:
    """
    Segue redirecionamentos de shortlinks do Google com whitelist de domínios.
    Não baixa HTML completo; apenas HEAD/GET leve.
    """
    def allowed(u: str) -> bool:
        try:
            host = urllib.parse.urlparse(u).hostname or ""
        except Exception:
            return False
        host = host.lower()
        return any(host == h or host.endswith("." + h) for h in ALLOWED_HOSTS)

    with requests.Session() as sess:
        sess.headers.update(HEADERS)
        try:
            # Primeiro HEAD (mais leve). Se não vier Location, cairá no GET.
            r = sess.head(url, allow_redirects=True, timeout=timeout)
            final_url = r.url
            if not allowed(final_url):
                return url
            return final_url
        except Exception:
            try:
                r = sess.get(url, allow_redirects=True, timeout=timeout)
                final_url = r.url
                if not allowed(final_url):
                    return url
                return final_url
            except Exception:
                return url

def extrair_lat_lon(url_final: str) -> Tuple[Optional[float], Optional[float]]:
    m = LATLON_AT.search(url_final)
    if m:
        return float(m.group(1)), float(m.group(2))
    m_lat = LAT_3D.search(url_final)
    m_lon = LON_4D.search(url_final)
    if m_lat and m_lon:
        return float(m_lat.group(1)), float(m_lon.group(1))
    parsed = urllib.parse.urlparse(url_final)
    q = urllib.parse.parse_qs(parsed.query)
    locs = q.get("q") or q.get("query") or []
    for val in locs:
        mm = re.search(r"(-?\d+(?:\.\d+)?),\s*(-?\d+(?:\.\d+)?)", val)
        if mm:
            return float(mm.group(1)), float(mm.group(2))
    return None, None

def extrair_nome_do_url(url_final: str) -> Optional[str]:
    """
    Tenta pegar o nome a partir de /maps/place/<NOME>/…
    Só aceita se não parecer via/rodovia/acesso.
    """
    try:
        path = urllib.parse.urlparse(url_final).path
        parts = [p for p in path.split("/") if p]
        for i, p in enumerate(parts):
            if p == "place" and i + 1 < len(parts):
                nome = urllib.parse.unquote(parts[i + 1])
                nome = sanitize_text(nome)
                # Ignorar se for coordenada ou se parecer via
                if nome and not re.match(r"-?\d+(\.\d+)?,\s*-?\d+(\.\d+)?", nome) and not is_probably_road(nome):
                    return nome
    except Exception:
        pass
    return None

def reverse_geocode(lat: float, lon: float) -> Tuple[Optional[str], Optional[str]]:
    """
    Usa Nominatim para obter nome e tipo do elemento.
    Retorna (nome, classe_tipo) para permitir detectar vias.
    """
    try:
        params = {
            "lat": lat,
            "lon": lon,
            "format": "jsonv2",
            "addressdetails": 1,
            "namedetails": 1,
            "accept-language": "pt-BR",
            "zoom": 18,
        }
        r = requests.get(NOMINATIM_BASE, params=params, headers=NOMINATIM_HEADERS, timeout=20)
        if r.status_code == 200:
            data = r.json()
            # namedetails pode trazer o nome “local”
            nome = data.get("namedetails", {}).get("name") or data.get("name") or data.get("display_name")
            nome = sanitize_text(nome or "")
            classe = data.get("category") or data.get("class")  # "highway", "amenity", etc.
            tipo = data.get("type")
            classetype = f"{classe}:{tipo}" if classe and tipo else (classe or tipo)
            return (nome if nome else None, classetype)
    except Exception:
        return (None, None)
    finally:
        time.sleep(NOMINATIM_DELAY_SECONDS)
    return (None, None)

def haversine_m(lat1, lon1, lat2, lon2):
    R = 6371000.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
    return 2*R*math.atan2(math.sqrt(a), math.sqrt(1-a))

def buscar_poi_proximo(lat: float, lon: float, raio_m: int = 120) -> Optional[str]:
    """
    Consulta Overpass para obter POIs nomeados próximos (amenity/shop/office/industrial/man_made, etc.).
    Retorna o nome do POI mais próximo.
    """
    # Raio em metros
    query = f"""
    [out:json][timeout:20];
    (
      node(around:{raio_m},{lat},{lon})[name];
      way(around:{raio_m},{lat},{lon})[name];
      relation(around:{raio_m},{lat},{lon})[name];
    );
    out center tags;
    """
    try:
        resp = requests.post(OVERPASS_URL, data={"data": query}, headers=OVERPASS_HEADERS, timeout=25)
        if resp.status_code != 200:
            return None
        data = resp.json()
        candidatos = []
        for el in data.get("elements", []):
            tags = el.get("tags", {})
            nome = tags.get("name")
            if not nome:
                continue
            nome = sanitize_text(nome)
            # Ignorar vias (se tag indicar highway)
            if "highway" in tags or is_probably_road(nome):
                continue
            # Pegar coordenadas (node: lat/lon; way/relation: center)
            if el.get("type") == "node":
                lat2, lon2 = el.get("lat"), el.get("lon")
            else:
                center = el.get("center") or {}
                lat2, lon2 = center.get("lat"), center.get("lon")
            if lat2 is None or lon2 is None:
                continue
            dist = haversine_m(lat, lon, lat2, lon2)
            candidatos.append((dist, nome))
        if not candidatos:
            return None
        candidatos.sort(key=lambda x: x[0])
        return candidatos[0][1]  # nome mais próximo
    except Exception:
        return None
    finally:
        time.sleep(OVERPASS_DELAY_SECONDS)

def resolver_nome_correto(lat: Optional[float], lon: Optional[float], nome_url: Optional[str]) -> str:
    """
    Resolve o nome final priorizando:
    1) Nome do URL (se não for via)
    2) Nominatim namedetails (se não for via)
    3) Overpass: POI nomeado mais próximo
    4) display_name do Nominatim como fallback
    """
    if nome_url and not is_probably_road(nome_url):
        return nome_url

    if lat is None or lon is None:
        return "(indisponível)"

    nome_osm, classetype = reverse_geocode(lat, lon)
    # Se o Nominatim trouxe nome e não parece via, use
    if nome_osm and not is_probably_road(nome_osm):
        # Mas se a classe for "highway:*", preferir POI próximo
        if classetype and isinstance(classetype, str) and classetype.startswith("highway"):
            poi = buscar_poi_proximo(lat, lon)
            return poi or nome_osm
        return nome_osm

    # Tentar POI próximo
    poi = buscar_poi_proximo(lat, lon)
    if poi:
        return poi

    # Fallback final
    return nome_osm or "(indisponível)"

def processar_link(link: str) -> Resultado:
    try:
        url_final = safe_follow(link)
        lat, lon = extrair_lat_lon(url_final)
        nome_url = extrair_nome_do_url(url_final)
        nome = resolver_nome_correto(lat, lon, nome_url)
        return Resultado(lugar=nome, lat=lat, lon=lon, link=link)
    except Exception:
        return Resultado(lugar="(indisponível)", lat=None, lon=None, link=link)

# ---------- Formatação de coordenadas (2 casas, ponto decimal) ----------
def fmt2(valor: Optional[float]) -> str:
    """Retorna string com 2 casas decimais e ponto como separador (xx.xx)."""
    return f"{valor:.2f}" if isinstance(valor, float) else ""

# ---------- Leitura dos links do arquivo ----------
def carregar_links(arquivo: str) -> List[str]:
    """Lê links do arquivo texto (um por linha) e ignora linhas vazias/espacos."""
    if not os.path.exists(arquivo):
        raise FileNotFoundError(f"Arquivo de entrada não encontrado: {arquivo}")
    links: List[str] = []
    with open(arquivo, "r", encoding="utf-8") as f:
        for linha in f:
            url = linha.strip()
            if url:
                links.append(url)
    return links

# ---------- Geração do CSV PT-BR (mantida) ----------
def gerar_csv_ptbr(resultados: List[Resultado], caminho: str) -> None:
    # UTF-8 com BOM e ';' como separador, PONTO como decimal (como solicitado)
    with open(caminho, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(
            f,
            delimiter=";",          # separador de colunas
            quotechar='"',
            quoting=csv.QUOTE_MINIMAL,
            lineterminator="\n",
        )
        w.writerow(["lugar", "latitude", "longitude", "link"])
        for r in resultados:
            w.writerow([
                r.lugar,
                fmt2(r.lat),  # 2 casas, ponto
                fmt2(r.lon),  # 2 casas, ponto
                r.link
            ])

def salvar_falhas(falhas: List[str], caminho: str) -> None:
    """Salva os links que falharam (sem lat/lon) em um arquivo TXT, um por linha."""
    if not falhas:
        with open(caminho, "w", encoding="utf-8") as f:
            f.write("")
        return
    with open(caminho, "w", encoding="utf-8") as f:
        for link in falhas:
            f.write(f"{link}\n")

def main():
    # Garante pastas
    os.makedirs(INPUT_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Carrega links do arquivo
    LINKS = carregar_links(INPUT_FILE)
    print(f"Processando {len(LINKS)} links…")

    resultados: List[Resultado] = []
    falhas: List[str] = []
    vistos = set()
    for i, link in enumerate(LINKS, start=1):
        if link in vistos:
            continue
        vistos.add(link)

        res = processar_link(link)
        slat = fmt2(res.lat)
        slon = fmt2(res.lon)
        ok = (res.lat is not None and res.lon is not None)
        status = "OK" if ok else "FALHA"

        print(f"[{i:02d}] {status} | {res.lugar} | {slat},{slon} | {res.link}")
        resultados.append(res)

        if not ok:
            falhas.append(res.link)

        time.sleep(0.25)

    # Somente CSV PT-BR na pasta output
    gerar_csv_ptbr(resultados, SAIDA_CSV_PTBR)

    # Salva as falhas
    salvar_falhas(falhas, SAIDA_FALHAS)

    print(f"\n✅ Gerado: {SAIDA_CSV_PTBR} (ponto-e-vírgula; coordenadas em xx.xx)")
    print(f"⚠️  Links com falha: {len(falhas)} (listados em {SAIDA_FALHAS})")

if __name__ == "__main__":
    main()
