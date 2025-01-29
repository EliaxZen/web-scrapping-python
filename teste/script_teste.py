import time
import random
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import NoSuchElementException, TimeoutException


# Função para configurar o navegador com WebDriver Manager e Selenium
def configurar_navegador():
    chrome_options = Options()
    chrome_options.add_argument(
        "--headless"
    )  # Executar sem abrir o navegador (opcional)
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--incognito")  # Modo anônimo
    chrome_options.add_argument("start-maximized")  # Maximizar a janela
    chrome_options.add_argument("--disable-infobars")
    chrome_options.add_argument("--disable-extensions")

    # Simular diferentes user-agents
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Safari/537.36",
        "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0",
    ]
    chrome_options.add_argument(f"user-agent={random.choice(user_agents)}")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)

    return driver


# Função para aceitar cookies
def aceitar_cookies(driver):
    try:
        time.sleep(2)  # Esperar um tempo para o banner de cookies carregar
        aceitar_botao = driver.find_element(By.ID, "adopt-accept-all-button")
        aceitar_botao.click()
        print("Cookies aceitos.")
    except NoSuchElementException:
        print("Banner de cookies não encontrado. Continuando sem aceitar cookies...")


# Função para extrair dados de uma página com Selenium
def extrair_dados_pagina(driver, url):
    driver.get(url)

    # Tentar aceitar os cookies
    aceitar_cookies(driver)

    time.sleep(
        random.uniform(2, 5)
    )  # Espera aleatória para simular comportamento humano

    # Rolagem da página para carregar todos os anúncios
    for i in range(3):  # Número de vezes para rolar a página
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(random.uniform(2, 5))  # Espera entre rolagens

    imoveis = []

    try:
        # Captura os blocos de anúncios (verificando os seletores mais recentes)
        anuncios = driver.find_elements(
            By.CSS_SELECTOR, "div[data-testid='listing-card']"
        )

        if not anuncios:
            print(f"Nenhum anúncio encontrado em {url}.")
            return []

        for anuncio in anuncios:
            try:
                # Extraindo o link do imóvel
                link_tag = anuncio.find_element(By.CSS_SELECTOR, "a")
                link = link_tag.get_attribute("href") if link_tag else None

                # Extraindo o título
                try:
                    titulo_tag = anuncio.find_element(By.CSS_SELECTOR, "h2")
                    titulo = (
                        titulo_tag.text.strip()
                        if titulo_tag
                        else "Título não encontrado"
                    )
                except NoSuchElementException:
                    titulo = "Título não disponível"

                # Extraindo o preço
                try:
                    preco_tag = anuncio.find_element(
                        By.CSS_SELECTOR, "span[data-testid='ad-price']"
                    )
                    preco = (
                        preco_tag.text.strip() if preco_tag else "Preço não encontrado"
                    )
                except NoSuchElementException:
                    preco = "Preço não disponível"

                # Extraindo detalhes do imóvel (quartos, metros, vagas)
                try:
                    detalhes_tag = anuncio.find_elements(
                        By.CSS_SELECTOR, "span[data-testid='ad-attribute']"
                    )
                    detalhes = (
                        [detalhe.text.strip() for detalhe in detalhes_tag]
                        if detalhes_tag
                        else []
                    )
                except NoSuchElementException:
                    detalhes = []

                # Distribuindo detalhes
                quartos = detalhes[0] if len(detalhes) > 0 else None
                metros = detalhes[1] if len(detalhes) > 1 else None
                vagas = detalhes[2] if len(detalhes) > 2 else None

                imoveis.append(
                    {
                        "Título": titulo,
                        "Preço": preco,
                        "Quartos": quartos,
                        "Área (m²)": metros,
                        "Vagas de Garagem": vagas,
                        "Link": link,
                    }
                )

            except NoSuchElementException as e:
                print(f"Erro ao processar anúncio: {e}")
                continue

    except TimeoutException:
        print(f"Tempo de carregamento excedido em {url}.")

    return imoveis


# Função para coletar dados de várias páginas
def coletar_dados_olx(driver, paginas=5):
    base_url = "https://www.olx.com.br/imoveis/venda/estado-to?o={}"
    todos_imoveis = []

    for pagina in range(1, paginas + 1):
        print(f"Coletando dados da página {pagina}...")
        url = base_url.format(pagina)
        imoveis = extrair_dados_pagina(driver, url)

        if imoveis:
            todos_imoveis.extend(imoveis)
        else:
            print(f"Erro ou página vazia na página {pagina}. Pulando...")

        time.sleep(
            random.uniform(3, 7)
        )  # Espera aleatória para evitar bloqueio do servidor

    return todos_imoveis


# Função principal para executar o processo e salvar os dados em Excel
def main():
    num_paginas = 5  # Defina o número de páginas a serem coletadas
    driver = configurar_navegador()

    try:
        dados_imoveis = coletar_dados_olx(driver, num_paginas)

        if not dados_imoveis:
            print("Nenhum dado foi coletado.")
            return

        # Convertendo os dados em um DataFrame do pandas
        df = pd.DataFrame(dados_imoveis)

        # Salvando os dados em um arquivo Excel
        df.to_excel("dados_imoveis_olx_selenium_atualizado.xlsx", index=False)
        print(
            "Dados salvos com sucesso no arquivo 'dados_imoveis_olx_selenium_atualizado.xlsx'."
        )

    finally:
        driver.quit()  # Fecha o navegador após a execução


if __name__ == "__main__":
    main()
