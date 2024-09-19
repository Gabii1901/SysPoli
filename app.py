from flask import Flask, render_template, request, redirect, url_for, session
from dbfread import DBF
import pandas as pd
import os
import tempfile
import pickle

app = Flask(__name__)
app.secret_key = 'supersecretkey'  # Necessário para usar sessões

# Função para salvar o arquivo carregado temporariamente no sistema
def salvar_arquivo_temporario(file, extension=None):
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=extension)
    file.save(temp_file.name)
    return temp_file.name

# Função para ler toda a tabela de um arquivo .DBF
def ler_dbf_completo(caminho):
    try:
        # Abre o arquivo DBF ignorando a falta de arquivos de memo
        dbf_table = DBF(caminho, ignore_missing_memofile=True)
        
        # Lê todos os registros e colunas do arquivo DBF
        registros = [record for record in dbf_table]
        
        # Converte para DataFrame
        df = pd.DataFrame(registros)
        
        print(f"Colunas disponíveis no arquivo {caminho}: {df.columns.tolist()}")
        print(f"Primeiros registros lidos do arquivo {caminho}:\n{df.head()}")
        
        return df
    except Exception as e:
        print(f"Erro ao ler o arquivo {caminho}: {str(e)}")
        return pd.DataFrame({'Erro': [str(e)]})

# Função para salvar DataFrame em um arquivo temporário
def salvar_dataframe_temporario(df):
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.pkl')
    with open(temp_file.name, 'wb') as f:
        pickle.dump(df, f)
    return temp_file.name

# Função para carregar DataFrame de um arquivo temporário
def carregar_dataframe_temporario(caminho):
    with open(caminho, 'rb') as f:
        df = pickle.load(f)
    return df

# Função para extrair o número da NF-e corretamente da chave
def extrair_numero_nfe(chave_nfe):
    # O número da NF-e é representado por 9 dígitos da posição 26 até 34 da chave
    # Corrigido para capturar os dígitos corretos da chave
    return chave_nfe[25:34] if len(chave_nfe) >= 34 else None


# Rota para carregar e validar a tabela de documentos fiscais
@app.route('/', methods=['GET', 'POST'])
def validar_documentos():
    if request.method == 'POST':
        # Recebe o arquivo de documentos fiscais
        documentos_files = request.files.getlist('documentos_files')

        if len(documentos_files) == 0:
            return render_template('validacao_documentos.html', error="Por favor, selecione os arquivos .DBF e .FPT dos documentos fiscais.")

        # Filtra arquivos .DBF e .FPT para documentos fiscais
        caminho_documentos_dbf = None
        caminho_documentos_fpt = None
        for file in documentos_files:
            if file.filename.lower().endswith('.dbf'):
                caminho_documentos_dbf = salvar_arquivo_temporario(file, '.dbf')
            elif file.filename.lower().endswith('.fpt'):
                caminho_documentos_fpt = salvar_arquivo_temporario(file, '.fpt')

        # Lê o arquivo .DBF completo (todas as colunas)
        documentos_df = ler_dbf_completo(caminho_documentos_dbf)

        # Verifica se houve erro na leitura do arquivo
        if 'Erro' in documentos_df.columns:
            return render_template('validacao_documentos.html', error=f"Erro ao processar o arquivo: {documentos_df['Erro'][0]}")

        # Adiciona uma nova coluna com o número da NF-e extraído da chave
        documentos_df['Numero_NFE'] = documentos_df['CHAVE'].apply(extrair_numero_nfe)

        # Salva o DataFrame em um arquivo temporário
        caminho_df_temp = salvar_dataframe_temporario(documentos_df)
        
        # Armazena o caminho do arquivo temporário na sessão
        session['caminho_dataframe'] = caminho_df_temp

        # Remove os arquivos temporários após a leitura
        os.remove(caminho_documentos_dbf)
        if caminho_documentos_fpt:
            os.remove(caminho_documentos_fpt)

        # Redireciona para a próxima etapa de filtragem
        return redirect(url_for('filtrar_documentos'))

    return render_template('validacao_documentos.html')

# Rota para a página de filtragem de documentos
@app.route('/filtrar', methods=['GET', 'POST'])
def filtrar_documentos():
    # Verifica se o caminho do DataFrame está na sessão
    if 'caminho_dataframe' not in session:
        return redirect(url_for('validar_documentos'))

    # Carrega o DataFrame a partir do caminho temporário armazenado
    caminho_df_temp = session['caminho_dataframe']
    documentos_filtrados = carregar_dataframe_temporario(caminho_df_temp)

    # Converte a coluna 'DATAEMIS' para datetime
    try:
        documentos_filtrados['DATAEMIS'] = pd.to_datetime(documentos_filtrados['DATAEMIS'], errors='coerce')
    except Exception as e:
        return render_template('filtragem.html', error=f"Erro ao converter datas: {str(e)}")

    # Verifica o DataFrame antes de aplicar os filtros
    print("DataFrame antes dos filtros:")
    print(documentos_filtrados.head())

    if request.method == 'POST':
        # Converte as datas fornecidas no formulário para datetime
        data_inicial = request.form.get('data_inicial')
        data_final = request.form.get('data_final')

        if data_inicial:
            try:
                data_inicial = pd.to_datetime(data_inicial)
            except Exception as e:
                return render_template('filtragem.html', error=f"Erro na data inicial: {str(e)}")

        if data_final:
            try:
                data_final = pd.to_datetime(data_final)
            except Exception as e:
                return render_template('filtragem.html', error=f"Erro na data final: {str(e)}")

        fornecedor_nome = request.form.get('fornecedor')
        cnpj = request.form.get('cnpj')

        # Aplica os filtros conforme necessário
        if data_inicial:
            documentos_filtrados = documentos_filtrados[documentos_filtrados['DATAEMIS'] >= data_inicial]
        if data_final:
            documentos_filtrados = documentos_filtrados[documentos_filtrados['DATAEMIS'] <= data_final]
        if fornecedor_nome:
            documentos_filtrados = documentos_filtrados[documentos_filtrados['NOME'].str.contains(fornecedor_nome, case=False, na=False)]
        if cnpj:
            documentos_filtrados = documentos_filtrados[documentos_filtrados['CNPJCPF'].str.contains(cnpj, case=False, na=False)]

        # Verifica o DataFrame após aplicar os filtros
        print("DataFrame após aplicar os filtros:")
        print(documentos_filtrados.head())

    return render_template('filtragem.html', documentos=documentos_filtrados.to_dict('records'))

if __name__ == '__main__':
    app.run(debug=True)
