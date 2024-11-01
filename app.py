from flask import Flask, render_template, request, redirect, url_for, session, send_file
from flask_session import Session
from dbfread import DBF
import pandas as pd
import os
from io import BytesIO
from datetime import datetime
import logging
import webview
import threading
import time
import webbrowser
import configparser



# Configuração de Logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("app.log", mode='w')
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

# Caminhos fixos para as tabelas das empresas
CAMINHOS_DOCUMENTOS = [
    r'C:\Genesis\Dados\E01\MANIFESTODOCUMENTOSFISCAIS.DBF',  # Polioeste - SC
    r'C:\Genesis\Dados\E03\MANIFESTODOCUMENTOSFISCAIS.DBF',  # Agro Log
    r'C:\Genesis\Dados\E04\MANIFESTODOCUMENTOSFISCAIS.DBF',  # Polioeste - PR
    r'C:\Genesis\Dados\E05\MANIFESTODOCUMENTOSFISCAIS.DBF'   # Polioeste - RS
]

# Caminho fixo para a tabela de fornecedores
CAMINHO_FIXO_TABELA_FORNECEDORES = r'C:\Genesis\Dados\FORNECEDORES.DBF'

# Função para gerar a senha dinâmica
def gerar_senha():
    data_atual = datetime.now()
    ano = str(data_atual.year)[-2:]
    dia = str(data_atual.day).zfill(2)
    senha = f"{ano}POLI{dia}"
    return senha

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

        # Verificar e carregar as colunas 'VALOR' e 'MANIFESTO' para mapeamento de Status
        if 'VALOR' in df.columns:
            # Log para verificar os primeiros valores de 'VALOR' antes da conversão
            df['VALOR'] = pd.to_numeric(df['VALOR'], errors='coerce').fillna(0)  # Converte para numérico e substitui NaN por 0
        else:
            logger.warning("Coluna 'VALOR' não encontrada no arquivo DBF.")

        if 'MANIFESTO' in df.columns:
            status_map = {
                'C': 'Confirmação da Operação',
                'R': 'Operação não realizada',
                'N': 'Não manifestada',
                'O': 'Ciência da Operação',
                'D': 'Desconhecimento da Operação'
            }
            df['Status'] = df['MANIFESTO'].map(status_map).fillna('Status Desconhecido')
        else:
            logger.warning("Coluna 'MANIFESTO' não encontrada no arquivo DBF.")

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

        if df_fornecedores.empty:
            logger.warning(f"Nenhum dado encontrado na tabela de fornecedores {CAMINHO_FIXO_TABELA_FORNECEDORES}")
        else:
            logger.info(f"{len(df_fornecedores)} registros encontrados na tabela de fornecedores")
        
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
        return chave[25:34].strip()
    return 'Número Inválido'

# Função para ler todas as tabelas de documentos fiscais e combiná-las com a coluna "Empresa"
def ler_tabelas_documentos_empresas():
    try:
        lista_dfs_empresas = []
        empresas = [
            {'razao_social': 'POLIOESTE - REPRESENTAÇÃO E DISTRIBUIÇÃO LTDA', 'cnpj': '29.191.948/0001-53', 'codigo': 1},
            {'razao_social': 'AGRO LOG TRANSPORTES LTDA', 'cnpj': '36.383.327/0001-37', 'codigo': 3},
            {'razao_social': 'POLIOESTE - REPRESENTACAO E DISTRIBUICAO LTDA', 'cnpj': '29.191.948/0003-15', 'codigo': 4},
            {'razao_social': 'POLIOESTE - REPRESENTACAO E DISTRIBUICAO LTDA', 'cnpj': '29.191.948/0004-04', 'codigo': 5}
        ]

        for index, caminho in enumerate(CAMINHOS_DOCUMENTOS):
            df_documentos = ler_tabela_dbf(caminho)
            if not df_documentos.empty:
                df_documentos['Razao_Social'] = empresas[index]['razao_social']
                df_documentos['CNPJ_Empresa'] = empresas[index]['cnpj']
                df_documentos['Codigo_Empresa'] = empresas[index]['codigo']
                lista_dfs_empresas.append(df_documentos)

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
        df_merged = pd.merge(df_documentos, df_fornecedores[['CNPJCPF', 'CODFOR']], on='CNPJCPF', how='left')
        df_merged['CODFOR'] = df_merged['CODFOR'].fillna('')
        df_merged['Numero_NFE'] = df_merged['CHAVE'].apply(extrair_numero_nfe)
        return df_merged
    except Exception as e:
        logger.error(f"Erro ao relacionar documentos fiscais com fornecedores: {e}")
        return df_documentos

# Rota de login
@app.route('/', methods=['GET', 'POST'])
def login():
    senha_correta = gerar_senha()
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

    df_fornecedores = ler_fornecedores()
    df_documentos = ler_tabelas_documentos_empresas()

    if 'Erro' in df_documentos.columns:
        return render_template('validacao_documentos.html', error=df_documentos['Erro'][0])
    if 'Erro' in df_fornecedores.columns:
        return render_template('validacao_documentos.html', error=df_fornecedores['Erro'][0])

    df_documentos_relacionados = relacionar_documentos_com_fornecedores(df_documentos, df_fornecedores)
    session['documentos_df'] = df_documentos_relacionados.to_dict('records')
    return redirect(url_for('filtrar_documentos'))

@app.route('/filtrar', methods=['GET', 'POST'])
def filtrar_documentos():
    if 'documentos_df' not in session:
        return redirect(url_for('validar_arquivos'))

    documentos_df = pd.DataFrame(session['documentos_df'])
    hoje = datetime.today()
    primeiro_dia_mes = hoje.replace(day=1).strftime('%Y-%m-%d')
    ultimo_dia_mes = hoje.strftime('%Y-%m-%d')

    data_inicial = request.form.get('data_inicial', primeiro_dia_mes)
    data_final = request.form.get('data_final', ultimo_dia_mes)
    fornecedor = request.form.get('fornecedor', '')
    cnpj = request.form.get('cnpj', '')
    codigo_empresa = request.form.get('codigo_empresa', '')
    cnpj_empresa = request.form.get('cnpj_empresa', '')

    if data_inicial:
        documentos_df['DATAEMIS'] = pd.to_datetime(documentos_df['DATAEMIS'], errors='coerce')
        documentos_df = documentos_df[documentos_df['DATAEMIS'] >= pd.to_datetime(data_inicial)]
    
    if data_final:
        documentos_df = documentos_df[documentos_df['DATAEMIS'] <= pd.to_datetime(data_final)]

    if fornecedor:
        documentos_df = documentos_df[documentos_df['NOME'].str.contains(fornecedor, case=False, na=False)]
    if cnpj:
        documentos_df = documentos_df[documentos_df['CNPJCPF'].str.contains(cnpj, case=False, na=False)]
    if codigo_empresa:
        documentos_df = documentos_df[documentos_df['Codigo_Empresa'].astype(str) == codigo_empresa]
    if cnpj_empresa:
        documentos_df = documentos_df[documentos_df['CNPJ_Empresa'].str.contains(cnpj_empresa, case=False, na=False)]

    session['documentos_filtrados'] = documentos_df.to_dict('records')

    return render_template('filtragem.html', 
                           documentos=documentos_df.to_dict('records'), 
                           data_inicial=data_inicial,
                           data_final=data_final,
                           fornecedor=fornecedor,
                           cnpj=cnpj,
                           codigo_empresa=codigo_empresa,
                           cnpj_empresa=cnpj_empresa,
                           default_data_inicial=primeiro_dia_mes,
                           default_data_final=ultimo_dia_mes)


# Rota para exportar documentos filtrados para Excel
@app.route('/exportar', methods=['GET'])
def exportar_documentos():
    if 'documentos_filtrados' not in session:
        return redirect(url_for('filtrar_documentos'))

    # Converte os documentos filtrados armazenados na sessão para um DataFrame
    documentos_filtrados = pd.DataFrame(session['documentos_filtrados'])

    # Mapeia o status a partir da coluna 'MANIFESTO' se ainda não estiver mapeado
    status_map = {
        'C': 'Confirmação da Operação',
        'R': 'Operação não realizada',
        'N': 'Não manifestada',
        'O': 'Ciência da Operação',
        'D': 'Desconhecimento da Operação'
    }
    # Aplica o mapeamento diretamente na coluna 'MANIFESTO' para criar a coluna 'Status' com as descrições
    documentos_filtrados['Status'] = documentos_filtrados['MANIFESTO'].map(status_map).fillna('Status Desconhecido')

    # Cria um buffer para o arquivo Excel
    output = BytesIO()

    # Usa o gerenciador de contexto para garantir que o arquivo Excel seja fechado corretamente
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        documentos_filtrados = documentos_filtrados[['DATAEMIS', 'Codigo_Empresa', 'CNPJ_Empresa', 'CODFOR', 'CNPJCPF', 'NOME', 'Numero_NFE', 'VALOR', 'Status', 'CHAVE']]
        documentos_filtrados.columns = ['Data Emissão', 'Código da Empresa', 'CNPJ da Empresa', 'Código do Fornecedor', 'CNPJ/CPF', 'Fornecedor', 'Número NF-E', 'Valor', 'Status', 'Chave NF-e']

        documentos_filtrados.to_excel(writer, index=False, sheet_name='Documentos Filtrados')

    # Move o ponteiro para o início do buffer antes de enviar o arquivo
    output.seek(0)

    return send_file(output, download_name='documentos_filtrados.xlsx', as_attachment=True)

# Função para rodar o servidor Flask em uma thread separada
def run_flask():
    app.run(use_reloader=False)

# Classe para expor a API ao pywebview
class API:
    def abrir_navegador_externo(self, url):
        # Obter a URL completa do servidor Flask
        full_url = f"http://127.0.0.1:5000{url}"
        # Abrir a URL no navegador externo
        webbrowser.open(full_url)

if __name__ == '__main__':
    # Iniciar o Flask em uma thread separada
    flask_thread = threading.Thread(target=run_flask)
    flask_thread.daemon = True
    flask_thread.start()

    # Criar uma janela webview apontando para o servidor Flask, com a API associada
    window = webview.create_window('Polisys', 'http://127.0.0.1:5000', js_api=API())
    # Iniciar o webview
    webview.start()
