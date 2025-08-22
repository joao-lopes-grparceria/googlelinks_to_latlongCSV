"""
Extrator de dados a partir de links do Google Maps.

Funcionalidades:
- Segue redirecionamentos de shortlinks do Google com whitelist de domínios seguros.
- Extrai latitude/longitude do URL final (vários formatos comuns do Google Maps).
- Resolve o nome do local priorizando: nome do URL > Nominatim > POI próximo (Overpass).
- Gera CSV no padrão brasileiro (separador ';', lat/lon com 2 casas decimais e ponto).
- Registra links que falharam (sem lat/lon) em um TXT.

Observações:
- Respeite os termos de uso do Nominatim/Overpass (inclua um User-Agent com contato real).
- Ajuste os delays se necessário, sem sobrecarregar os serviços.
"""

from __future__ import annotations

import os
import re
import csv
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

DIRETORIO_ENTRADA = "input"
DIRETORIO_SAIDA = "output"
ARQUIVO_LINKS = os.path.join(DIRETORIO_ENTRADA, "links.txt")
ARQUIVO_CSV_PTBR = os.path.join(DIRETORIO_SAIDA, "resultado_excel_ptbr.csv")  # separador ';', decimal com ponto
ARQUIVO_FALHAS = os.path.join(DIRETORIO_SAIDA, "links_falhos.txt")            # links que falharam

# =========================
# Configuração de rede (segura)
# =========================

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"
)
HEADERS_PADRAO = {"User-Agent": USER_AGENT, "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8"}

# Whitelist de hosts permitidos (evita seguir redirecionamentos para domínios suspeitos)
HOSTS_PERMITIDOS = {
    "maps.app.goo.gl",
    "www.google.com",
    "google.com",
    "www.google.com.br",
    "google.com.br",
    "maps.google.com",
}

# =========================
# Nominatim (respeite as políticas)
# =========================

NOMINATIM_URL = "https://nominatim.openstreetmap.org/reverse"
NOMINATIM_HEADERS = {
    # >>>> SUBSTITUA PELO SEU CONTATO REAL (e.g. e-mail ou site do projeto) <<<<
    "User-Agent": "MapsLinksExtractor/1.3 (contato: seu-email@exemplo.com)"
}
NOMINATIM_DELAY_SEGUNDOS = 1.0

# =========================
# Overpass (POIs próximos)
# =========================

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
OVERPASS_HEADERS = {
    # >>>> SUBSTITUA PELO SEU CONTATO REAL (e.g. e-mail ou site do projeto) <<<<
    "User-Agent": "MapsLinksExtractor/1.3 (contato: seu-email@exemplo.com)"
}
OVERPASS_DELAY_SEGUNDOS = 1.0

# =========================
# Modelos e utilidades
# =========================

@dataclass
class Resultado:
    """Representa o resultado processado de um link."""
    lugar: str
    lat: Optional[float]
    lon: Optional[float]
    link: str


# Padrões para extrair coordenadas de diferentes formatos de URL do Google Maps
PADRAO_LATLON_ARROBA = re.compile(r"@(-?\d+(?:\.\d+)?),\s*(-?\d+(?:\.\d+)?)")
PADRAO_LAT_3D = re.compile(r"!3d(-?\d+(?:\.\d+)?)")
PADRAO_LON_4D = re.compile(r"!4d(-?\d+(?:\.\d+)?)")

# Palavras-chave que indicam via/rodovia/acesso (não é POI “nomeado”)
PALAVRAS_CHAVE_VIA = (
    "rodovia", "estrada", "avenida", "rua", "acesso", "br-", "br ",
    "alça", "linha", "viaduto", "trevo", "marginal", "r.", "av.", "km "
)


def eh_provavel_via(nome: str) -> bool:
    """Heurística simples para identificar nomes que parecem vias/rodovias."""
    s = nome.strip().lower()
    return any(chave in s for chave in PALAVRAS_CHAVE_VIA)


def limpar_texto(texto: str) -> str:
    """
    Normaliza e remove caracteres problemáticos para CSV (controle/zero-width/emoji).
    Também reduz espaçamentos múltiplos.
    """
    if not texto:
        return texto
    s = unicodedata.normalize("NFKC", texto.replace("+", " ")).strip()
    s = "".join(ch for ch in s if unicodedata.category(ch) not in {"Cc", "Cf", "Cs"})
    s = s.replace("\r", " ").replace("\n", " ").replace("\t", " ")
    s = re.sub(r"\s{2,}", " ", s)
    for marca in ("™", "®", "©"):
        s = s.replace(marca, "")
    return s.strip()


def _host_permitido(url: str) -> bool:
    """Verifica se o host do `url` está dentro da whitelist de domínios permitidos."""
    try:
        host = urllib.parse.urlparse(url).hostname or ""
    except Exception:
        return False
    host = host.lower()
    return any(host == permitido or host.endswith("." + permitido) for permitido in HOSTS_PERMITIDOS)


def seguir_redirecionamento_seguro(url_inicial: str, timeout: float = 12.0) -> str:
    """
    Segue redirecionamentos de shortlinks do Google com whitelist de domínios.
    Tenta HEAD (leve); se falhar, faz GET. Retorna a URL final (ou a original, se algo der errado).
    """
    with requests.Session() as sessao:
        sessao.headers.update(HEADERS_PADRAO)
        try:
            resp = sessao.head(url_inicial, allow_redirects=True, timeout=timeout)
            url_final = resp.url
            return url_final if _host_permitido(url_final) else url_inicial
        except Exception:
            # fallback para GET
            try:
                resp = sessao.get(url_inicial, allow_redirects=True, timeout=timeout)
                url_final = resp.url
                return url_final if _host_permitido(url_final) else url_inicial
            except Exception:
                return url_inicial


def extrair_lat_lon(url_final: str) -> Tuple[Optional[float], Optional[float]]:
    """
    Extrai latitude/longitude do URL final. Suporta formatos:
    - .../@<lat>,<lon>,...
    - ...!3d<lat>...!4d<lon>...
    - querystring q= ou query= contendo "<lat>,<lon>"
    """
    # 1) @lat,lon
    m = PADRAO_LATLON_ARROBA.search(url_final)
    if m:
        return float(m.group(1)), float(m.group(2))

    # 2) !3d<lat> e !4d<lon>
    m_lat = PADRAO_LAT_3D.search(url_final)
    m_lon = PADRAO_LON_4D.search(url_final)
    if m_lat and m_lon:
        return float(m_lat.group(1)), float(m_lon.group(1))

    # 3) Querystring q= ou query=
    parsed = urllib.parse.urlparse(url_final)
    params = urllib.parse.parse_qs(parsed.query)
    candidatos = params.get("q") or params.get("query") or []
    for valor in candidatos:
        mm = re.search(r"(-?\d+(?:\.\d+)?),\s*(-?\d+(?:\.\d+)?)", valor)
        if mm:
            return float(mm.group(1)), float(mm.group(2))

    return None, None


def extrair_nome_da_url(url_final: str) -> Optional[str]:
    """
    Tenta obter o nome a partir de /maps/place/<NOME>/… no path.
    Ignora se for coordenada pura ou se parecer via/rodovia.
    """
    try:
        path = urllib.parse.urlparse(url_final).path
        partes = [p for p in path.split("/") if p]
        for i, p in enumerate(partes):
            if p == "place" and i + 1 < len(partes):
                nome = urllib.parse.unquote(partes[i + 1])
                nome = limpar_texto(nome)
                if not nome:
                    continue
                # ignora se for coordenada ou “via”
                if re.match(r"-?\d+(\.\d+)?,\s*-?\d+(\.\d+)?", nome):
                    continue
                if eh_provavel_via(nome):
                    continue
                return nome
    except Exception:
        pass
    return None


def geocodificar_reverso(lat: float, lon: float) -> Tuple[Optional[str], Optional[str]]:
    """
    Usa Nominatim para obter dados do ponto.
    Retorna (nome, classe_tipo) onde classe_tipo ex.: "highway:tertiary" ou "amenity:restaurant".
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
        resp = requests.get(NOMINATIM_URL, params=params, headers=NOMINATIM_HEADERS, timeout=20)
        if resp.status_code == 200:
            data = resp.json()
            nome = data.get("namedetails", {}).get("name") or data.get("name") or data.get("display_name")
            nome = limpar_texto(nome or "")
            classe = data.get("category") or data.get("class")
            tipo = data.get("type")
            classe_tipo = f"{classe}:{tipo}" if classe and tipo else (classe or tipo)
            return (nome if nome else None, classe_tipo)
    except Exception:
        return (None, None)
    finally:
        time.sleep(NOMINATIM_DELAY_SEGUNDOS)

    return (None, None)


def _haversine_metros(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Distância aproximada entre dois pontos (lat/lon) na Terra em metros (fórmula de Haversine)."""
    raio_terra_m = 6_371_000.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlon / 2) ** 2
    return 2 * raio_terra_m * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def buscar_poi_proximo(lat: float, lon: float, raio_metros: int = 120) -> Optional[str]:
    """
    Consulta Overpass para obter POIs nomeados próximos (node/way/relation com tag [name]).
    Ignora elementos que pareçam vias. Retorna o nome do POI mais próximo.
    """
    consulta = f"""
    [out:json][timeout:20];
    (
      node(around:{raio_metros},{lat},{lon})[name];
      way(around:{raio_metros},{lat},{lon})[name];
      relation(around:{raio_metros},{lat},{lon})[name];
    );
    out center tags;
    """
    try:
        resp = requests.post(OVERPASS_URL, data={"data": consulta}, headers=OVERPASS_HEADERS, timeout=25)
        if resp.status_code != 200:
            return None

        dados = resp.json()
        candidatos: List[Tuple[float, str]] = []

        for elemento in dados.get("elements", []):
            tags = elemento.get("tags", {})
            nome = tags.get("name")
            if not nome:
                continue

            nome = limpar_texto(nome)
            # Ignorar vias claramente marcadas como highway ou que pareçam via
            if "highway" in tags or eh_provavel_via(nome):
                continue

            # Coordenadas (node tem lat/lon; way/relation usam "center")
            if elemento.get("type") == "node":
                lat2, lon2 = elemento.get("lat"), elemento.get("lon")
            else:
                centro = elemento.get("center") or {}
                lat2, lon2 = centro.get("lat"), centro.get("lon")

            if lat2 is None or lon2 is None:
                continue

            distancia = _haversine_metros(lat, lon, float(lat2), float(lon2))
            candidatos.append((distancia, nome))

        if not candidatos:
            return None

        candidatos.sort(key=lambda x: x[0])
        return candidatos[0][1]  # nome do POI mais próximo

    except Exception:
        return None
    finally:
        time.sleep(OVERPASS_DELAY_SEGUNDOS)


def resolver_nome_final(lat: Optional[float], lon: Optional[float], nome_url: Optional[str]) -> str:
    """
    Define o nome final do local seguindo a prioridade:
    1) Nome do URL (se não parecer via)
    2) Nome do Nominatim (se não parecer via; se for highway, tenta POI próximo)
    3) Nome de POI próximo (Overpass)
    4) Fallback: nome do Nominatim (display_name) ou "(indisponível)"
    """
    if nome_url and not eh_provavel_via(nome_url):
        return nome_url

    if lat is None or lon is None:
        return "(indisponível)"

    nome_osm, classe_tipo = geocodificar_reverso(lat, lon)

    # Se o Nominatim trouxe algo e não parece via, priorize
    if nome_osm and not eh_provavel_via(nome_osm):
        # Se classe é "highway:*", prefira tentar um POI próximo
        if classe_tipo and isinstance(classe_tipo, str) and classe_tipo.startswith("highway"):
            poi = buscar_poi_proximo(lat, lon)
            return poi or nome_osm
        return nome_osm

    # Tentar POI próximo
    poi = buscar_poi_proximo(lat, lon)
    if poi:
        return poi

    # Fallback final
    return nome_osm or "(indisponível)"


def formatar_duas_casas(valor: Optional[float]) -> str:
    """
    Retorna string com 2 casas decimais e PONTO como separador (xx.xx).
    Caso `valor` seja None, retorna string vazia.
    """
    return f"{valor:.2f}" if isinstance(valor, float) else ""


def carregar_links(caminho_arquivo: str) -> List[str]:
    """
    Lê links do arquivo texto (um por linha).
    Ignora linhas vazias e espaços em excesso.
    """
    if not os.path.exists(caminho_arquivo):
        raise FileNotFoundError(f"Arquivo de entrada não encontrado: {caminho_arquivo}")

    links: List[str] = []
    with open(caminho_arquivo, "r", encoding="utf-8") as arquivo:
        for linha in arquivo:
            url = linha.strip()
            if url:
                links.append(url)
    return links


def gerar_csv_ptbr(resultados: List[Resultado], caminho_csv: str) -> None:
    """
    Gera o CSV com cabeçalho em português, separador ';' e coordenadas com 2 casas decimais.
    Codificação: UTF-8 com BOM (facilita abertura no Excel).
    """
    with open(caminho_csv, "w", newline="", encoding="utf-8-sig") as arquivo:
        escritor = csv.writer(
            arquivo,
            delimiter=";",          # separador de colunas
            quotechar='"',
            quoting=csv.QUOTE_MINIMAL,
            lineterminator="\n",
        )
        escritor.writerow(["lugar", "latitude", "longitude", "link"])
        for r in resultados:
            escritor.writerow([
                r.lugar,
                formatar_duas_casas(r.lat),
                formatar_duas_casas(r.lon),
                r.link,
            ])


def salvar_falhas(links_falhos: List[str], caminho_txt: str) -> None:
    """
    Salva os links que falharam (sem lat/lon) em um arquivo TXT, um por linha.
    Se não houver falhas, cria/zera o arquivo.
    """
    with open(caminho_txt, "w", encoding="utf-8") as arquivo:
        if not links_falhos:
            arquivo.write("")
            return
        for link in links_falhos:
            arquivo.write(f"{link}\n")


def processar_link(link: str) -> Resultado:
    """
    Processa um único link:
    - Segue redirecionamentos (seguros).
    - Extrai lat/lon.
    - Resolve o nome final do local.
    """
    try:
        url_final = seguir_redirecionamento_seguro(link)
        lat, lon = extrair_lat_lon(url_final)
        nome_url = extrair_nome_da_url(url_final)
        nome_final = resolver_nome_final(lat, lon, nome_url)
        return Resultado(lugar=nome_final, lat=lat, lon=lon, link=link)
    except Exception:
        return Resultado(lugar="(indisponível)", lat=None, lon=None, link=link)


def main() -> None:
    """Fluxo principal da aplicação."""
    # Garante estrutura de pastas
    os.makedirs(DIRETORIO_ENTRADA, exist_ok=True)
    os.makedirs(DIRETORIO_SAIDA, exist_ok=True)

    # Carrega links do arquivo
    links = carregar_links(ARQUIVO_LINKS)
    print(f"Processando {len(links)} link(s)…")

    resultados: List[Resultado] = []
    links_falhos: List[str] = []
    vistos: set[str] = set()

    for indice, link in enumerate(links, start=1):
        if link in vistos:
            continue
        vistos.add(link)

        resultado = processar_link(link)
        slat = formatar_duas_casas(resultado.lat)
        slon = formatar_duas_casas(resultado.lon)
        ok = (resultado.lat is not None and resultado.lon is not None)
        status = "OK" if ok else "FALHA"

        print(f"[{indice:02d}] {status} | {resultado.lugar} | {slat},{slon} | {resultado.link}")
        resultados.append(resultado)

        if not ok:
            links_falhos.append(resultado.link)

        # Pausa curta entre links (educado c/ serviços externos e menos bloqueios)
        time.sleep(0.25)

    # Geração do CSV PT-BR na pasta de saída
    gerar_csv_ptbr(resultados, ARQUIVO_CSV_PTBR)

    # Salva as falhas
    salvar_falhas(links_falhos, ARQUIVO_FALHAS)

    print(f"\n✅ Gerado: {ARQUIVO_CSV_PTBR} (separador ';'; coordenadas em xx.xx)")
    print(f"⚠️ Links com falha: {len(links_falhos)} (listados em {ARQUIVO_FALHAS})")


if __name__ == "__main__":
    main()
