import os
import csv
import re
import time
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
# Configuração de rede
# =========================
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"
)
HEADERS = {"User-Agent": USER_AGENT, "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8"}

# Nominatim (respeite as políticas)
NOMINATIM_BASE = "https://nominatim.openstreetmap.org/reverse"
NOMINATIM_HEADERS = {
    "User-Agent": "MapsLinksExtractor/1.2 (contato: seu-email@exemplo.com)"
}
NOMINATIM_DELAY_SECONDS = 1.0

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

def expandir(url: str) -> str:
    """Segue o redirecionamento do shortlink SEM baixar HTML completo."""
    with requests.Session() as sess:
        sess.headers.update(HEADERS)
        try:
            r = sess.get(url, allow_redirects=True, timeout=20)
            return r.url
        except Exception:
            try:
                r = sess.head(url, allow_redirects=True, timeout=20)
                return r.headers.get("location", url) or url
            except Exception:
                return url  # devolve o original se não der

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
    """Tenta pegar o nome a partir de /maps/place/<NOME>/…"""
    try:
        path = urllib.parse.urlparse(url_final).path
        parts = [p for p in path.split("/") if p]
        for i, p in enumerate(parts):
            if p == "place" and i + 1 < len(parts):
                nome = urllib.parse.unquote(parts[i + 1])
                nome = sanitize_text(nome)
                if nome and not re.match(r"-?\d+(\.\d+)?,\s*-?\d+(\.\d+)?", nome):
                    return nome
    except Exception:
        pass
    return None

def reverse_geocode(lat: float, lon: float) -> Optional[str]:
    """Usa Nominatim para display_name amigável."""
    try:
        params = {"lat": lat, "lon": lon, "format": "jsonv2"}
        r = requests.get(NOMINATIM_BASE, params=params, headers=NOMINATIM_HEADERS, timeout=25)
        if r.status_code == 200:
            data = r.json()
            nome = data.get("name") or data.get("display_name")
            return sanitize_text(nome or "")
    except Exception:
        return None
    finally:
        time.sleep(NOMINATIM_DELAY_SECONDS)
    return None

def processar_link(link: str) -> Resultado:
    try:
        url_final = expandir(link)
        lat, lon = extrair_lat_lon(url_final)
        nome = extrair_nome_do_url(url_final)
        if (not nome) and (lat is not None and lon is not None):
            nome = reverse_geocode(lat, lon)
        if not nome:
            nome = "(indisponível)"
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
        # Se quiser, ainda criamos o arquivo vazio para rastreabilidade
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
        # status com 2 casas no log também
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
