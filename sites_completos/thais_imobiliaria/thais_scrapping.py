import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
from tqdm import tqdm
from distrito_federal_setor import setores
import time

def extrair_setor(titulo):
    palavras = titulo.split()
    palavras_upper = [palavra.upper() for palavra in palavras]
    for palavra in palavras_upper:
        if palavra in setores:
            return palavra
    return "OUTRO"

def configurar_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-software-rasterizer")
    chrome_options.add_argument("--disable-setuid-sandbox")
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

def scrape_imoveis(driver, num_clicks):
    lista_de_imoveis = []

    try:
        driver.get("https://www.thaisimobiliaria.com.br/imoveis/para-alugar")

        driver.execute_script("document.getElementById('cookies-component').remove();")

        WebDriverWait(driver, 10).until(
            EC.visibility_of_element_located((By.CSS_SELECTOR, ".card_split_vertically.borderHover"))
        )

        for _ in tqdm(range(num_clicks), desc="Clicando no botão 'Ver Mais'"):
            try:
                botao_ver_mais = WebDriverWait(driver, 10).until(
                    EC.visibility_of_element_located((By.CSS_SELECTOR, ".btn.btn-md.btn-primary.btn-next"))
                )
                botao_ver_mais.click()
                time.sleep(0.5)
            except (NoSuchElementException, TimeoutException):
                print("Botão 'Ver Mais' não encontrado. Todos os imóveis foram carregados.")
                break

            # Coletar os dados dos imóveis a cada clique no botão "Ver Mais"
            page_content = driver.page_source
            site = BeautifulSoup(page_content, "html.parser")
            imoveis = site.find_all("a", attrs={"class": "card_split_vertically borderHover"})

            for imovel in imoveis:
                titulo = imovel.find("h2", attrs={"class": "card_split_vertically__location"})
                titulo_text = titulo.text.strip() if titulo else None
                setor = extrair_setor(titulo_text)

                link = "https://www.thaisimobiliaria.com.br" + imovel["href"]

                tipo = imovel.find("p", attrs={"class": "card_split_vertically__type"})
                tipo_text = tipo.text.strip() if tipo else None

                preco_area = imovel.find("div", attrs={"class": "card_split_vertically__value-container"})
                preco = preco_area.find("p", attrs={"class": "card_split_vertically__value"}).text.strip() if preco_area else "Preço não especificado"
                preco = "".join(filter(str.isdigit, preco))

                if not preco or preco == "0":
                    continue

                metro = imovel.find("li", attrs={"class": "card_split_vertically__spec"})
                metro_text = metro.text.replace("m²", "").strip() if metro else None
                metro_text = "".join(filter(str.isdigit, metro_text))

                quarto_suite_banheiro_vaga = imovel.find("ul", attrs={"class": "card_split_vertically__specs"})
                if quarto_suite_banheiro_vaga:
                    lista = quarto_suite_banheiro_vaga.findAll("li")
                    quarto = suite = banheiro = vaga = 0

                    for item in lista:
                        text_lower = item.text.lower()
                        if "quarto" in text_lower or "quartos" in text_lower:
                            quarto = int(item.text.split()[0])
                        elif "suíte" in text_lower or "suítes" in text_lower:
                            suite = int(item.text.split()[0])
                        elif "banheiro" in text_lower or "banheiros" in text_lower:
                            banheiro = int(item.text.split()[0])
                        elif "vaga" in text_lower or "vagas" in text_lower:
                            vaga = int(item.text.split()[0])
                else:
                    quarto = suite = banheiro = vaga = 0

                if link not in [imovel[2] for imovel in lista_de_imoveis]:
                    lista_de_imoveis.append([titulo_text, tipo_text, link, preco, metro_text, quarto, suite, banheiro, vaga, setor])

        return lista_de_imoveis

    except Exception as e:
        print(f"Ocorreu um erro durante o scraping: {e}")
        return lista_de_imoveis

    finally:
        driver.quit()

def salvar_excel(dataframe):
    dataframe.to_excel(r"C:\Users\galva\OneDrive\Documentos\GitHub\web-scrapping-com-python\thais_imobiliaria\thais_imobiliaria_aluguel.xlsx", index=False)

def main(num_clicks):
    driver = configurar_driver()
    lista_de_imoveis = scrape_imoveis(driver, num_clicks)

    df_imovel = pd.DataFrame(
        lista_de_imoveis,
        columns=["Título", "Tipo", "Link", "Preço", "Metro Quadrado", "Quarto", "Suite", "Banheiro", "Vaga", "Setor"]
    )

    df_imovel["Preço"] = pd.to_numeric(df_imovel["Preço"], errors="coerce")
    df_imovel["Metro Quadrado"] = pd.to_numeric(df_imovel["Metro Quadrado"], errors="coerce")
    df_imovel["Quarto"] = pd.to_numeric(df_imovel["Quarto"], errors="coerce")
    df_imovel["Suite"] = pd.to_numeric(df_imovel["Suite"], errors="coerce")
    df_imovel["Banheiro"] = pd.to_numeric(df_imovel["Banheiro"], errors="coerce")
    df_imovel["Vaga"] = pd.to_numeric(df_imovel["Vaga"], errors="coerce")

    if df_imovel.isnull().values.any():
        print("Existem valores nulos no DataFrame. Lidar com eles conforme necessário.")

    df_imovel["M2"] = df_imovel["Preço"] / df_imovel["Metro Quadrado"]
    df_imovel[["Quarto", "Suite", "Banheiro", "Vaga", "M2"]] = df_imovel[["Quarto", "Suite", "Banheiro", "Vaga", "M2"]].fillna(0)

    salvar_excel(df_imovel)

    print(df_imovel)

if __name__ == "__main__":
    NUM_CLICKS = 4  # Defina o número de cliques desejado
    main(NUM_CLICKS)
