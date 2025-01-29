import os
import time
import re
import random
import logging
from typing import List, Optional, Tuple
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    ElementClickInterceptedException,
    WebDriverException,
)
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
from tqdm import tqdm
import argparse

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# Configurações globais
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36",
]

COLUNAS = [
    "Título",
    "Link",
    "Avenida",
    "Setor",
    "Cidade",
    "Estado",
    "Preço",
    "Área",
    "Quartos",
    "Banheiros",
    "Vagas",
]


class WebScraper:
    def __init__(self, headless: bool = True):
        self.driver = self._configurar_driver(headless)
        self.wait = WebDriverWait(self.driver, 10)

    def _configurar_driver(self, headless: bool) -> webdriver.Chrome:
        """Configura e retorna uma instância do WebDriver."""
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument(f"user-agent={random.choice(USER_AGENTS)}")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option("useAutomationExtension", False)

        if headless:
            chrome_options.add_argument("--headless")

        try:
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=chrome_options)
            logger.info("Driver configurado com sucesso.")
            return driver
        except WebDriverException as e:
            logger.error(f"Erro ao iniciar o WebDriver: {e}")
            raise

    def aceitar_cookies(self) -> None:
        """Aceita cookies se o botão estiver presente."""
        try:
            self.wait.until(
                EC.element_to_be_clickable((By.ID, "cookie-notifier-cta"))
            ).click()
            logger.info("Cookies aceitos.")
        except TimeoutException:
            logger.debug("Botão de cookies não encontrado ou já aceito.")
        except Exception as e:
            logger.error(f"Erro ao tentar aceitar cookies: {e}")

    def carregar_pagina(self, url: str, max_tentativas: int = 3) -> bool:
        """Carrega uma página com tratamento de erros robusto."""
        for tentativa in range(max_tentativas):
            try:
                self.driver.get(url)
                return True
            except Exception as e:
                logger.warning(
                    f"Erro ao carregar a página (tentativa {tentativa + 1}/{max_tentativas}): {e}"
                )
                time.sleep(random.uniform(1, 2))
        logger.error("Falha ao carregar a página após múltiplas tentativas.")
        return False

    def rolar_pagina(self) -> None:
        """Rola a página até o final para carregar todos os elementos."""
        altura_anterior = self.driver.execute_script(
            "return document.body.scrollHeight"
        )
        while True:
            self.driver.execute_script(
                "window.scrollTo(0, document.body.scrollHeight);"
            )
            time.sleep(random.uniform(1, 1.5))
            nova_altura = self.driver.execute_script(
                "return document.body.scrollHeight"
            )
            if nova_altura == altura_anterior:
                break
            altura_anterior = nova_altura

    def extrair_dados_pagina(self) -> List[List[str]]:
        """Extrai os dados de cada imóvel na página."""
        imoveis = []
        try:
            anuncios = self.driver.find_elements(By.CLASS_NAME, "listing-item")
            for anuncio in anuncios:
                try:
                    dados = self._processar_anuncio(anuncio)
                    if dados:
                        imoveis.append(dados)
                except Exception as e:
                    logger.error(f"Erro ao processar anúncio: {e}")
        except Exception as e:
            logger.error(f"Erro ao buscar anúncios: {e}")
        return imoveis

    def _processar_anuncio(self, anuncio) -> Optional[List[str]]:
        """Processa um anúncio individual e retorna os dados."""
        try:
            titulo = anuncio.find_element(By.CLASS_NAME, "property-title").text
            link = anuncio.find_element(By.TAG_NAME, "a").get_attribute("href")
            preco = self._processar_preco(anuncio)
            area = self._processar_area(anuncio)
            quartos = self._processar_detalhe(anuncio, "property-rooms")
            banheiros = self._processar_detalhe(anuncio, "property-bathrooms")
            vagas = self._processar_detalhe(anuncio, "property-parking")
            endereco = anuncio.find_element(By.CLASS_NAME, "property-address").text

            return [
                titulo,
                link,
                *self._processar_endereco(endereco),
                preco,
                area,
                quartos,
                banheiros,
                vagas,
            ]
        except NoSuchElementException as e:
            logger.debug(f"Elemento não encontrado no anúncio: {e}")
            return None
        except Exception as e:
            logger.error(f"Erro ao processar anúncio: {e}")
            return None

    def _processar_preco(self, anuncio) -> float:
        """Processa e converte o preço."""
        try:
            preco_texto = anuncio.find_element(By.CLASS_NAME, "property-price").text
            return float(
                preco_texto.replace("R$", "").replace(".", "").replace(",", ".")
            )
        except:
            return 0.0

    def _processar_area(self, anuncio) -> float:
        """Processa e converte a área."""
        try:
            area_texto = anuncio.find_element(By.CLASS_NAME, "property-area").text
            return float(
                area_texto.replace("m²", "").replace(".", "").replace(",", ".")
            )
        except:
            return 0.0

    def _processar_detalhe(self, anuncio, classe: str) -> int:
        """Processa detalhes como quartos, banheiros e vagas."""
        try:
            texto = anuncio.find_element(By.CLASS_NAME, classe).text
            return int(re.search(r"\d+", texto).group())
        except:
            return 0

    def _processar_endereco(self, endereco: str) -> Tuple[str, str, str, str]:
        """Processa o endereço em componentes."""
        partes = endereco.split(",")
        return (
            partes[0].strip() if len(partes) > 0 else "",
            partes[1].strip() if len(partes) > 1 else "",
            partes[2].strip() if len(partes) > 2 else "",
            partes[3].strip() if len(partes) > 3 else "",
        )

    def fechar(self) -> None:
        """Fecha o driver."""
        self.driver.quit()


def processar_dados(df: pd.DataFrame) -> pd.DataFrame:
    """Realiza transformações e limpezas no DataFrame."""
    # Conversão numérica
    for coluna in ["Preço", "Área", "Quartos", "Banheiros", "Vagas"]:
        df[coluna] = pd.to_numeric(df[coluna], errors="coerce").fillna(0)

    # Filtragem e cálculos
    df = df[(df["Preço"] > 0) & (df["Área"] > 0)].copy()
    df["M2"] = (df["Preço"] / df["Área"]).round(2)

    return df


def salvar_dados(df: pd.DataFrame, base_nome: str) -> None:
    """Salva os dados em CSV e Excel."""
    csv_path = f"{base_nome}.csv"
    excel_path = f"{base_nome}.xlsx"

    df.to_csv(csv_path, index=False, encoding="utf-8")
    logger.info(f"Dados salvos em CSV: {csv_path}")

    df.to_excel(excel_path, index=False)
    logger.info(f"Dados salvos em Excel: {excel_path}")


def main(num_paginas: int, max_imoveis: int) -> None:
    """Função principal de execução do scraper."""
    scraper = WebScraper()
    url_base = "https://www.vivareal.com.br/aluguel/tocantins/palmas/"

    try:
        if not scraper.carregar_pagina(url_base):
            return

        scraper.aceitar_cookies()
        dados_imoveis = []

        with tqdm(total=num_paginas, desc="Progresso") as pbar:
            for pagina in range(1, num_paginas + 1):
                if not scraper.carregar_pagina(f"{url_base}?pagina={pagina}"):
                    break

                scraper.rolar_pagina()
                dados_pagina = scraper.extrair_dados_pagina()
                dados_imoveis.extend(dados_pagina)
                pbar.update(1)

                if len(dados_imoveis) >= max_imoveis:
                    logger.info(f"Limite de {max_imoveis} imóveis atingido.")
                    break

                time.sleep(random.uniform(0.5, 1))

        df = pd.DataFrame(dados_imoveis[:max_imoveis], columns=COLUNAS)
        df = processar_dados(df)
        salvar_dados(df, "palmas_vivareal_aluguel")

    except Exception as e:
        logger.error(f"Erro durante a execução: {e}")
    finally:
        scraper.fechar()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Web Scraper de Imóveis")
    parser.add_argument(
        "--paginas", type=int, default=5, help="Número de páginas para processar"
    )
    parser.add_argument(
        "--max-imoveis",
        type=int,
        default=50,
        help="Número máximo de imóveis para coletar",
    )

    args = parser.parse_args()
    main(num_paginas=args.paginas, max_imoveis=args.max_imoveis)
