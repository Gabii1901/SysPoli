from flask import Flask, render_template, request, redirect, url_for, session, send_file
from flask_session import Session
from dbfread import DBF
import pandas as pd
import os
from io import BytesIO
from datetime import datetime
import logging

# Configuração de Logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),  # Para console
        logging.FileHandler("app.log", mode='w')  # Para arquivo
    ]
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = 'supersecretkey'

# Configuração do Flask-Session
app.config['SESSION_TYPE'] = 'filesystem'
app.config['SESSION_PERMANENT'] = False
app.config['SESSION_FILE_DIR'] = os.path.join(os.getcwd(), 'flask_session')
Session(app)

# Caminhos fixos para as tabelas das empresas (sem a empresa 2)
CAMINHOS_DOCUMENTOS = [
    r'C:\polioeste\Genesis\Dados\E01\MANIFESTODOCUMENTOSFISCAIS.DBF',  # Polioeste - SC
    r'C:\polioeste\Genesis\Dados\E03\MANIFESTODOCUMENTOSFISCAIS.DBF',  # Agro Log
    r'C:\polioeste\Genesis\Dados\E04\MANIFESTODOCUMENTOSFISCAIS.DBF',  # Polioeste - PR
    r'C:\polioeste\Genesis\Dados\E05\MANIFESTODOCUMENTOSFISCAIS.DBF'   # Polioeste - RS
]

# Caminho fixo para a tabela de fornecedores
CAMINHO_FIXO_TABELA_FORNECEDORES = r'C:\polioeste\Genesis\Dados\FORNECEDORES.DBF'

# Função para gerar a senha dinâmica
def gerar_senha():
    data_atual = datetime.now()
    ano = str(data_atual.year)[-2:]  # Pega os dois últimos dígitos do ano
    dia = str(data_atual.day).zfill(2)  # Garante que o dia tenha dois dígitos
    senha = f"{ano}POLI{dia}"
    return senha

# Função para ler uma tabela de documentos fiscais
def ler_tabela_dbf(caminho):
    try:
        dbf_table = DBF(caminho, ignore_missing_memofile=True)
        registros = [{coluna: str(record[coluna]).strip() for coluna in dbf_table.field_names} for record in dbf_table]
        df = pd.DataFrame(registros)

        if df.empty:
            logger.warning(f"Nenhum dado encontrado no arquivo {caminho}")
        else:
            logger.info(f"{len(df)} registros encontrados no arquivo {caminho}")
        
        if 'DATAEMIS' in df.columns:
            df['DATAEMIS'] = pd.to_datetime(df['DATAEMIS'], errors='coerce', format='%Y-%m-%d')

        return df
    except Exception as e:
        logger.error(f"Erro ao ler o arquivo DBF: {e}")
        return pd.DataFrame({'Erro': [str(e)]})

# Função para ler a tabela de fornecedores
def ler_fornecedores():
    try:
        fornecedores_table = DBF(CAMINHO_FIXO_TABELA_FORNECEDORES, ignore_missing_memofile=True)
        registros_fornecedores = [{coluna: str(record[coluna]).strip() for coluna in fornecedores_table.field_names} for record in fornecedores_table]
        df_fornecedores = pd.DataFrame(registros_fornecedores)

        # Log das colunas para diagnóstico
        logger.info(f"Colunas da tabela de fornecedores: {df_fornecedores.columns.tolist()}")

        # Verifica se há dados na tabela de fornecedores
        if df_fornecedores.empty:
            logger.warning(f"Nenhum dado encontrado na tabela de fornecedores {CAMINHO_FIXO_TABELA_FORNECEDORES}")
        else:
            logger.info(f"{len(df_fornecedores)} registros encontrados na tabela de fornecedores")
        
        # Combina CNPJ e CPF em uma única coluna chamada 'CNPJCPF'
        df_fornecedores['CNPJCPF'] = df_fornecedores.apply(
            lambda row: row['CNPJ'] if pd.notnull(row['CNPJ']) else row['CPF'], axis=1
        )
        
        return df_fornecedores
    except Exception as e:
        logger.error(f"Erro ao ler a tabela de fornecedores: {e}")
        return pd.DataFrame({'Erro': [str(e)]})

# Função para extrair o número da NF-e a partir da chave
def extrair_numero_nfe(chave):
    if len(chave) >= 44:
        return chave[25:34].strip()  # Extrai os dígitos da NF-e
    return 'Número Inválido'

# Função para ler todas as tabelas de documentos fiscais e combiná-las com a coluna "Empresa"
def ler_tabelas_documentos_empresas():
    try:
        # Lista para armazenar os DataFrames de todas as empresas
        lista_dfs_empresas = []

        # Definindo os nomes das empresas
        empresas = ['Polioeste - SC', 'Agro Log', 'Polioeste - PR', 'Polioeste - RS']
        
        # Itera sobre cada caminho da lista de empresas e lê os documentos fiscais
        for index, caminho in enumerate(CAMINHOS_DOCUMENTOS):
            df_documentos = ler_tabela_dbf(caminho)
            if not df_documentos.empty:
                df_documentos['Empresa'] = empresas[index]  # Adiciona a coluna "Empresa" com o nome da empresa
                lista_dfs_empresas.append(df_documentos)
        
        # Combina todos os DataFrames em um único DataFrame
        if lista_dfs_empresas:
            df_documentos_combinado = pd.concat(lista_dfs_empresas, ignore_index=True)
            return df_documentos_combinado
        else:
            logger.error("Nenhum dado encontrado nas tabelas de documentos fiscais das empresas.")
            return pd.DataFrame()
    
    except Exception as e:
        logger.error(f"Erro ao combinar as tabelas de documentos fiscais: {e}")
        return pd.DataFrame({'Erro': [str(e)]})

# Função para relacionar documentos fiscais com fornecedores
def relacionar_documentos_com_fornecedores(df_documentos, df_fornecedores):
    try:
        # Realiza o merge com base na coluna 'CNPJCPF' e traz o código do fornecedor 'CODFOR'
        df_merged = pd.merge(df_documentos, df_fornecedores[['CNPJCPF', 'CODFOR']], on='CNPJCPF', how='left')

        # Se não houver fornecedor correspondente, o código será em branco
        df_merged['CODFOR'] = df_merged['CODFOR'].fillna('')

        # Adicionar o número da NF-e extraído da coluna 'CHAVE'
        df_merged['Numero_NFE'] = df_merged['CHAVE'].apply(extrair_numero_nfe)

        return df_merged
    except Exception as e:
        logger.error(f"Erro ao relacionar documentos fiscais com fornecedores: {e}")
        return df_documentos

# Rota de login
@app.route('/', methods=['GET', 'POST'])
def login():
    senha_correta = gerar_senha()  # Gera a senha dinâmica com base na data
    if request.method == 'POST':
        senha_inserida = request.form.get('senha')
        if senha_inserida == senha_correta:
            session['logado'] = True
            return redirect(url_for('validar_arquivos'))
        else:
            return render_template('login.html', error='Senha incorreta.')
    return render_template('login.html')

# Rota principal para validação de arquivos
@app.route('/validar', methods=['GET'])
def validar_arquivos():
    if not session.get('logado'):
        return redirect(url_for('login'))

    # Ler a tabela de fornecedores
    df_fornecedores = ler_fornecedores()

    # Ler as tabelas de documentos fiscais de todas as empresas
    df_documentos = ler_tabelas_documentos_empresas()

    # Verifica se houve erro na leitura das tabelas
    if 'Erro' in df_documentos.columns:
        return render_template('validacao_documentos.html', error=df_documentos['Erro'][0])
    if 'Erro' in df_fornecedores.columns:
        return render_template('validacao_documentos.html', error=df_fornecedores['Erro'][0])

    # Relacionar os documentos fiscais com os fornecedores e extrair o número da NF-e
    df_documentos_relacionados = relacionar_documentos_com_fornecedores(df_documentos, df_fornecedores)

    session['documentos_df'] = df_documentos_relacionados.to_dict('records')
    return redirect(url_for('filtrar_documentos'))

# Rota para filtrar documentos
@app.route('/filtrar', methods=['GET', 'POST'])
def filtrar_documentos():
    if 'documentos_df' not in session:
        return redirect(url_for('validar_arquivos'))

    documentos_df = pd.DataFrame(session['documentos_df'])

    # Obter o mês atual para os valores padrão
    hoje = datetime.today()
    primeiro_dia_mes = hoje.replace(day=1).strftime('%Y-%m-%d')
    ultimo_dia_mes = hoje.strftime('%Y-%m-%d')

    # Captura os valores dos campos de filtro
    data_inicial = request.form.get('data_inicial', primeiro_dia_mes)
    data_final = request.form.get('data_final', ultimo_dia_mes)
    fornecedor = request.form.get('fornecedor', '')
    cnpj = request.form.get('cnpj', '')

    # Aplica o filtro por data
    if data_inicial:
        documentos_df['DATAEMIS'] = pd.to_datetime(documentos_df['DATAEMIS'], errors='coerce')
        documentos_df = documentos_df[documentos_df['DATAEMIS'] >= pd.to_datetime(data_inicial)]
    
    if data_final:
        documentos_df = documentos_df[documentos_df['DATAEMIS'] <= pd.to_datetime(data_final)]

    # Aplica o filtro por fornecedor
    if fornecedor:
        documentos_df = documentos_df[documentos_df['NOME'].str.contains(fornecedor, case=False, na=False)]

    # Aplica o filtro por CNPJ/CPF
    if cnpj:
        documentos_df = documentos_df[documentos_df['CNPJCPF'].str.contains(cnpj, case=False, na=False)]

    # Armazena os documentos filtrados na sessão
    session['documentos_filtrados'] = documentos_df.to_dict('records')

    # Renderiza a página de filtragem com os documentos filtrados
    return render_template('filtragem.html', 
                           documentos=documentos_df.to_dict('records'), 
                           data_inicial=data_inicial,
                           data_final=data_final,
                           fornecedor=fornecedor,
                           cnpj=cnpj,
                           default_data_inicial=primeiro_dia_mes,
                           default_data_final=ultimo_dia_mes)

# Rota para exportar documentos filtrados para Excel
@app.route('/exportar', methods=['GET'])
def exportar_documentos():
    if 'documentos_filtrados' not in session:
        return redirect(url_for('filtrar_documentos'))

    documentos_filtrados = pd.DataFrame(session['documentos_filtrados'])
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')

    # Reordenar e renomear colunas para o Excel
    documentos_filtrados = documentos_filtrados[['DATAEMIS', 'CNPJCPF', 'NOME', 'Numero_NFE', 'CHAVE', 'CODFOR', 'Empresa']]
    documentos_filtrados.columns = ['Data Emissão', 'CNPJ/CPF', 'Fornecedor', 'Número NF-E', 'Chave NF-E', 'Código do Fornecedor', 'Empresa']

    documentos_filtrados.to_excel(writer, index=False, sheet_name='Documentos Filtrados')
    writer.close()

    output.seek(0)
    return send_file(output, download_name='documentos_filtrados.xlsx', as_attachment=True)

if __name__ == '__main__':
    app.run(debug=True)
