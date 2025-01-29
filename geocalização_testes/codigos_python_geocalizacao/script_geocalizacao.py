import pandas as pd
import googlemaps
from tqdm import tqdm
import time

# Inicializa o cliente da API do Google com a sua chave de API
API_KEY = "AIzaSyAZhtQa1PFSauX5gTQaDXkVFcMVIxhi-IU"  # Substitua pela sua chave de API do Google Maps
gmaps = googlemaps.Client(key=API_KEY)


# Função para buscar o CEP utilizando a API do Google Geocoding
def buscar_cep_google(endereco):
    try:
        # Faz uma pausa de 0.5 segundos entre requisições para evitar sobrecarregar o serviço
        time.sleep(0.5)

        # Faz a requisição para a API Google Geocoding
        geocode_result = gmaps.geocode(endereco)

        # Verifica se obteve resultados
        if geocode_result:
            for result in geocode_result:
                for component in result["address_components"]:
                    if "postal_code" in component["types"]:
                        return component["long_name"]
            return "CEP não encontrado"
        else:
            return "Nenhuma resposta da API"

    except Exception as e:
        # Captura erros relacionados à conexão ou processamento da API
        return f"Erro: {e}"


# Função para formatar o endereço a partir das colunas relevantes
def formatar_endereco(linha):
    # Combina as colunas que compõem o endereço
    endereco = f"{linha['Endereço']}, {linha['Região Administrativa']}".strip()
    return endereco


# Função principal para processar os endereços
def processar_enderecos(arquivo_excel):
    # Carrega o arquivo Excel
    df = pd.read_excel(arquivo_excel)

    # Verifica se as colunas necessárias existem
    colunas_necessarias = ["Região Administrativa", "Endereço"]
    if not all(coluna in df.columns for coluna in colunas_necessarias):
        raise ValueError(
            f"Uma ou mais colunas necessárias não foram encontradas no arquivo Excel: {colunas_necessarias}"
        )

    # Inicializa uma nova coluna para armazenar os CEPs
    df["CEP"] = None

    # Usando tqdm para a barra de progresso
    for i, linha in tqdm(df.iterrows(), total=len(df), desc="Buscando CEPs"):
        # Verifica se já há um CEP preenchido (para evitar reprocessamento)
        if pd.isna(df.at[i, "CEP"]):
            # Formata o endereço antes de enviar à API
            endereco_formatado = formatar_endereco(linha)
            cep = buscar_cep_google(endereco_formatado)
            df.at[i, "CEP"] = cep

    # Salva o DataFrame atualizado em um novo arquivo Excel
    novo_arquivo = "imoveis_com_cep_google.xlsx"
    df.to_excel(novo_arquivo, index=False)

    print(f"Processo concluído. O arquivo com os CEPs foi salvo como '{novo_arquivo}'.")


# Caminho do arquivo Excel (modifique conforme necessário)
arquivo_excel = r"C:\Users\Elias\Documents\Web Scrapping\web-scrapping-com-python\geocalização_testes\codigos_python_geocalizacao\Imoveis_Terracap_Cleaned.xlsx"

# Chama a função para processar os endereços
processar_enderecos(arquivo_excel)
