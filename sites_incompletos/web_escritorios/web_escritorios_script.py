import os
import logging
import pandas as pd
import random
from typing import List
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    ElementClickInterceptedException,
)
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime
import time
import re
from bs4 import BeautifulSoup
import requests

# Configuração do logger
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger()

# Configurações globais
CONFIGURACOES = {
    "Venda - Escritórios": "https://webescritorios.com.br/comprar/",
    "Venda - Industriais": "https://webindustrial.com.br/comprar/",
    "Aluguel - Escritórios": "https://webescritorios.com.br/alugar/",
    "Aluguel - Industriais": "https://webindustrial.com.br/alugar/",
}


def configurar_driver(headless=True):
    """Configura o WebDriver para o Selenium."""
    options = webdriver.ChromeOptions()
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-extensions")
    options.add_argument("--start-maximized")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36"
    )
    if headless:
        options.add_argument("--headless")
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)


def carregar_pagina(driver: webdriver.Chrome, url: str) -> bool:
    """Carrega uma página e tenta resolver problemas de carregamento."""
    max_retentativas = 3
    for tentativa in range(max_retentativas):
        try:
            driver.get(url)
            time.sleep(random.uniform(2, 4))  # Simula comportamento humano
            return True
        except Exception as e:
            logger.warning(
                f"Erro ao carregar a página (tentativa {tentativa + 1}/{max_retentativas}): {e}"
            )
            time.sleep(random.uniform(2, 5))
    logger.error("Falha ao carregar a página após múltiplas tentativas.")
    return False


def clicar_no_proximo(driver: webdriver.Chrome) -> bool:
    """Clica no botão 'Próximo' para ir para a próxima página."""
    try:
        botao_proximo = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "proximo"))
        )
        driver.execute_script("arguments[0].scrollIntoView(true);", botao_proximo)
        WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable(botao_proximo)
        ).click()
        time.sleep(random.uniform(2, 4))  # Delay para evitar bloqueios
        return True
    except (TimeoutException, NoSuchElementException, ElementClickInterceptedException):
        logger.info("Botão 'Próximo' não encontrado ou sem mais páginas.")
        return False


def processar_pagina(driver: webdriver.Chrome, categoria: str, tipo_transacao: str):
    """Processa a página atual do Selenium e retorna os dados extraídos."""
    resultados = []
    try:
        conteudo_html = driver.page_source
        resultados = processar_pagina_html(conteudo_html, categoria, tipo_transacao)
    except Exception as e:
        logger.error(f"Erro ao processar a página com Selenium: {e}")
    return resultados


def processar_pagina_html(conteudo_html, categoria, tipo_transacao):
    """Processa o HTML de uma página e retorna os dados extraídos."""
    resultados = []
    try:
        soup = BeautifulSoup(conteudo_html, "html.parser")
        elementos = soup.select("div.col-xs-12.col-sm-6.col-lg-4.grid-offer-col")
        for elemento in elementos:
            informacoes = extrair_informacoes_imovel_html(
                elemento, categoria, tipo_transacao
            )
            if informacoes:
                resultados.append(informacoes)
    except Exception as e:
        logger.error(f"Erro ao processar a página: {e}")
    return resultados


def normalizar_numero(valor):
    """Normaliza valores numéricos (preço, área)."""
    try:
        valor = re.sub(r"[^\d,.-]", "", valor)  # Remove caracteres não numéricos
        valor = valor.replace(".", "").replace(",", ".")  # Formato numérico padrão
        return float(valor) if valor else 0  # Converte para float
    except ValueError:
        return 0  # Retorna 0 em caso de erro


def extrair_informacoes_imovel_html(elemento, categoria, tipo_transacao):
    """Extrai informações de um imóvel a partir do BeautifulSoup."""
    try:
        titulo = elemento.select_one("h2.grid-offer-title").text.strip()
        descricao = elemento.select_one("p.grid-descricao-imovel").text.strip()
        preco_texto = elemento.select_one("div.grid-price").text.strip()
        area_texto = elemento.select_one("div.type-anuncio-destak-novo p").text.strip()
        link = elemento.select_one("a")["href"]

        preco = normalizar_numero(preco_texto)
        area = normalizar_numero(area_texto)

        return {
            "Categoria": categoria,
            "Transação": tipo_transacao,
            "Título": titulo,
            "Descrição": descricao,
            "Preço": preco,
            "Área (m²)": area,
            "Preço por M2": preco / area if area > 0 else 0,
            "Link": link,
        }
    except Exception as e:
        logger.error(f"Erro ao extrair informações: {e}")
        return None


def salvar_para_excel(dados, nome_arquivo):
    """Salva os dados coletados em um arquivo Excel."""
    df = pd.DataFrame(dados)
    with pd.ExcelWriter(nome_arquivo, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Dados Imóveis")
        for idx, coluna in enumerate(df.columns):
            max_largura = max(df[coluna].astype(str).apply(len).max(), len(coluna)) + 2
            writer.sheets["Dados Imóveis"].set_column(idx, idx, max_largura)


def coletar_dados_via_html(categoria, url):
    """Coleta dados de imóveis iterando pelas páginas através da URL."""
    resultados = []
    pagina = 1
    while True:
        url_pagina = f"{url}?pagina={pagina}"
        logger.info(f"Processando URL: {url_pagina}")
        resposta = requests.get(url_pagina, headers={"User-Agent": "Mozilla/5.0"})
        if resposta.status_code != 200:
            logger.error(f"Erro ao acessar a página: {url_pagina}")
            break
        conteudo_html = resposta.text
        dados_pagina = processar_pagina_html(
            conteudo_html, categoria, "Venda" if "comprar" in url else "Aluguel"
        )
        if not dados_pagina:  # Para quando não há mais dados
            break
        resultados.extend(dados_pagina)
        pagina += 1
    return resultados


def coletar_dados(categoria, url):
    """Executa o scraping de todas as páginas combinando Selenium e requests."""
    driver = configurar_driver(headless=True)
    resultados = []
    try:
        if carregar_pagina(driver, url):
            tipo_transacao = "Venda" if "comprar" in url else "Aluguel"
            pagina_atual = 1
            while True:
                logger.info(f"Processando página {pagina_atual}")
                resultados.extend(processar_pagina(driver, categoria, tipo_transacao))
                if not clicar_no_proximo(driver):
                    break
                pagina_atual += 1
    finally:
        driver.quit()

    # Complementar com requests + BeautifulSoup
    resultados.extend(coletar_dados_via_html(categoria, url))
    return resultados


def principal():
    """Função principal para iniciar o scraping."""
    pasta_saida = os.path.join(os.getcwd(), "imoveis_scraped_selenium")
    os.makedirs(pasta_saida, exist_ok=True)
    data_hoje = datetime.now().strftime("%Y-%m-%d")

    for categoria, url in CONFIGURACOES.items():
        logger.info(f"Iniciando scraping para categoria: {categoria}")
        dados_categoria = coletar_dados(categoria, url)
        if dados_categoria:
            tipo_transacao, tipo_imovel = categoria.split(" - ")
            nome_arquivo = (
                f"{tipo_imovel.lower()}_{tipo_transacao.lower()}_{data_hoje}.xlsx"
            )
            caminho_arquivo = os.path.join(pasta_saida, nome_arquivo)
            salvar_para_excel(dados_categoria, caminho_arquivo)

    logger.info("Scraping completo.")


if __name__ == "__main__":
    principal()
