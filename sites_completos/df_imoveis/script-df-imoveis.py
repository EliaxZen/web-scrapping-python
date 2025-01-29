import os
import re
import time
import logging
from typing import List, Optional, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm
from distrito_federal_setor import setores as setores_list

# Configurações globais
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
}

BASE_URL = "https://www.dfimoveis.com.br/aluguel/df/todos/imoveis?pagina="
NUM_PAGES = 1365
MAX_RETRIES = 5
BACKOFF_FACTOR = 2
INITIAL_WAIT = 5
MAX_WORKERS = min(20, (os.cpu_count() or 1) * 4)  # Aumentado o limite de workers

# Estruturas de dados otimizadas
setores_set = set(setores_list)
tipos_imovel = [
    ("apartamento", "Apartamento"),
    ("casa-condominio", "Casa Condomínio"),
    ("hotel-flat", "Flat"),
    ("lote-terreno", "Lote Terreno"),
    ("ponto-comercial", "Ponto Comercial"),
    ("casa", "Casa"),
    ("galpo", "Galpão"),
    ("garagem", "Garagem"),
    ("flat", "Flat"),
    ("kitnet", "Kitnet"),
    ("loja", "Loja"),
    ("loteamento", "Loteamento"),
    ("prdio", "Prédio"),
    ("predio", "Prédio"),
    ("sala", "Sala"),
    ("rural", "Zona Rural"),
    ("lancamento", "Lançamento"),
]

COLUNAS = [
    "Título",
    "Subtítulo",
    "Link",
    "Preço",
    "Área",
    "Quarto",
    "Suite",
    "Vaga",
    "Imobiliária",
]


def criar_sessao() -> requests.Session:
    """Cria e configura uma sessão HTTP com política de retry."""
    session = requests.Session()
    retry = Retry(
        total=MAX_RETRIES,
        backoff_factor=BACKOFF_FACTOR,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.headers.update(HEADERS)
    return session


def buscar_pagina(sessao: requests.Session, pagina: int) -> Optional[bytes]:
    """Obtém o conteúdo de uma página com tratamento de erros robusto."""
    url = f"{BASE_URL}{pagina}"
    tentativas = 0
    espera = INITIAL_WAIT

    while tentativas <= MAX_RETRIES:
        try:
            resposta = sessao.get(url, timeout=30)
            resposta.raise_for_status()
            return resposta.content
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                logging.warning(
                    f"Erro 429 na página {pagina}. Tentando novamente em {espera}s..."
                )
                time.sleep(espera)
                espera *= BACKOFF_FACTOR
                tentativas += 1
            else:
                logging.error(
                    f"Erro HTTP {e.response.status_code} na página {pagina}: {e}"
                )
                return None
        except requests.exceptions.RequestException as e:
            logging.error(f"Erro de conexão na página {pagina}: {e}")
            tentativas += 1
            time.sleep(espera)
            espera *= BACKOFF_FACTOR

    logging.error(f"Falha definitiva na página {pagina} após {MAX_RETRIES} tentativas.")
    return None


def parsear_imovel(imovel: Any) -> Optional[List[str]]:
    """Extrai dados de um imóvel com verificações robustas de elementos."""
    try:
        # Elementos obrigatórios
        titulo = imovel.find("h2", class_="new-title").text.strip()
        link = f"https://www.dfimoveis.com.br{imovel['href']}"

        # Elementos opcionais
        subtitulo_elemento = imovel.find("h3", class_="new-simple phrase")
        subtitulo = subtitulo_elemento.text.strip() if subtitulo_elemento else ""

        # Processamento de preço
        preco_elemento = imovel.find("div", class_="new-price")
        preco_texto = preco_elemento.find("h4").text.strip() if preco_elemento else ""
        preco = re.sub(r"\D", "", preco_texto) or "0"

        # Processamento de área
        area_elemento = imovel.find("li", class_="m-area")
        area_texto = (
            area_elemento.text.replace("m²", "").strip() if area_elemento else ""
        )
        metro_match = re.search(r"\d+", area_texto)
        area = metro_match.group() if metro_match else "0"

        # Detalhes adicionais
        detalhes = {"quarto": "0", "suite": "0", "vaga": "0"}
        lista_detalhes = imovel.find("ul", class_="new-details-ul")
        if lista_detalhes:
            for item in lista_detalhes.findAll("li"):
                texto = item.text.lower()
                if "quartos" in texto:
                    detalhes["quarto"] = re.search(r"\d+", texto).group() or "0"
                elif "suítes" in texto:
                    detalhes["suite"] = re.search(r"\d+", texto).group() or "0"
                elif "vagas" in texto:
                    detalhes["vaga"] = re.search(r"\d+", texto).group() or "0"

        # Imobiliária
        imobiliaria_elemento = imovel.find("div", class_="new-anunciante")
        imobiliaria_img = (
            imobiliaria_elemento.find("img", alt=True) if imobiliaria_elemento else None
        )
        imobiliaria = imobiliaria_img["alt"] if imobiliaria_img else ""

        return [
            titulo,
            subtitulo,
            link,
            preco,
            area,
            detalhes["quarto"],
            detalhes["suite"],
            detalhes["vaga"],
            imobiliaria,
        ]

    except AttributeError as e:
        logging.debug(f"Elemento não encontrado no imóvel: {e}")
        return None
    except Exception as e:
        logging.error(f"Erro crítico ao processar imóvel: {e}", exc_info=True)
        return None


def processar_conteudo_pagina(conteudo: bytes) -> List[List[str]]:
    """Processa o conteúdo HTML de uma página extraindo dados de imóveis."""
    soup = BeautifulSoup(conteudo, "html.parser")
    imoveis = soup.findAll("a", class_="new-card")
    return [imovel for imovel in (parsear_imovel(i) for i in imoveis) if imovel]


def extrair_setor(titulo: str) -> str:
    """Identifica o setor do imóvel usando busca otimizada em conjunto."""
    return next((p for p in titulo.upper().split() if p in setores_set), "OUTRO")


def classificar_tipo(link: str) -> str:
    """Classifica o tipo de imóvel usando busca ordenada otimizada."""
    for key, value in tipos_imovel:
        if key in link:
            return value
    return "OUTROS"


def processar_dados(df: pd.DataFrame) -> pd.DataFrame:
    """Realiza transformações e limpezas no DataFrame."""
    # Conversão numérica
    numeric_cols = ["Preço", "Área", "Quarto", "Suite", "Vaga"]
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")

    # Filtragem e cálculos
    df = df.dropna(subset=["Preço", "Área"]).query("Preço > 0 and Área > 0").copy()
    df["M2"] = (df["Preço"] / df["Área"]).round(2)

    # Classificações
    df["Setor"] = df["Título"].apply(extrair_setor).astype("category")
    df["Tipo"] = df["Link"].apply(classificar_tipo).astype("category")

    return df


def main():
    """Fluxo principal de execução do programa."""
    logging.info("Iniciando coleta de dados...")

    imoveis_coletados = []
    with criar_sessao() as sessao:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(buscar_pagina, sessao, p): p
                for p in range(1, NUM_PAGES + 1)
            }

            for future in tqdm(
                as_completed(futures),
                total=len(futures),
                desc="Coletando páginas",
                unit="página",
            ):
                conteudo = future.result()
                if conteudo:
                    imoveis_coletados.extend(processar_conteudo_pagina(conteudo))

    logging.info("Processando dados coletados...")
    df = pd.DataFrame(imoveis_coletados, columns=COLUNAS)
    df = processar_dados(df)

    # Salvamento otimizado
    diretorio_saida = os.path.join("base_de_dados_excel", "df_imoveis_data_base")
    os.makedirs(diretorio_saida, exist_ok=True)
    caminho_saida = os.path.join(diretorio_saida, "df_imoveis_aluguel_01_2025.xlsx")

    df.to_excel(caminho_saida, index=False, engine="openpyxl")
    logging.info(f"Dados salvos com sucesso em: {caminho_saida}")


if __name__ == "__main__":
    main()
