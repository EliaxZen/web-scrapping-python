import pandas as pd
import os


def converter_csv_para_excel(caminho_csv, caminho_excel=None, nome_aba="Sheet1"):
    """
    Converte um arquivo CSV para o formato Excel (.xlsx) com tratamento para campos vazios e aspas.

    Parâmetros:
    caminho_csv (str): Caminho do arquivo CSV que será convertido.
    caminho_excel (str, opcional): Caminho de destino do arquivo Excel. Se não for fornecido, o Excel será salvo no mesmo diretório do CSV.
    nome_aba (str, opcional): Nome da aba no Excel. Padrão é 'Sheet1'.
    """
    try:
        # Verifica se o arquivo CSV existe
        if not os.path.exists(caminho_csv):
            print(f"Erro: Arquivo CSV '{caminho_csv}' não encontrado.")
            return

        # Lê o arquivo CSV com tratamento para aspas e delimitadores
        df = pd.read_csv(
            caminho_csv,
            quotechar='"',
            sep=",",
            skip_blank_lines=True,
            on_bad_lines="skip",
        )

        # Define o nome do arquivo Excel caso não seja informado
        if caminho_excel is None:
            caminho_excel = os.path.splitext(caminho_csv)[0] + ".xlsx"

        # Exporta para Excel
        df.to_excel(caminho_excel, index=False, sheet_name=nome_aba)

        print(
            f"Conversão bem-sucedida! O arquivo Excel foi salvo em '{caminho_excel}'."
        )

    except Exception as e:
        print(f"Ocorreu um erro durante a conversão: {e}")


# Exemplo de uso
if __name__ == "__main__":
    # Caminho do arquivo CSV
    caminho_arquivo_csv = r"C:\Users\Elias\Documents\Web Scrapping\web-scrapping-com-python\geocalização_testes\samambaia- comercio.csv"

    # Caminho de saída do arquivo Excel (opcional)
    caminho_arquivo_excel = "samanbaia-comercio.xlsx"

    # Nome da aba do Excel (opcional)
    nome_da_aba = "Dados"

    # Converter CSV para Excel
    converter_csv_para_excel(
        caminho_csv=caminho_arquivo_csv,
        caminho_excel=caminho_arquivo_excel,
        nome_aba=nome_da_aba,
    )
