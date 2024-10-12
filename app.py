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

# Função para gerar a senha dinâmica
def gerar_senha():
    data_atual = datetime.now()
    ano = str(data_atual.year)[-2:]  # Pega os dois últimos dígitos do ano
    dia = str(data_atual.day).zfill(2)  # Garante que o dia tenha dois dígitos
    senha = f"{ano}POLI{dia}"
    return senha

# Função para ler arquivos DBF
def ler_tabela_dbf(caminho):
    try:
        dbf_table = DBF(caminho, ignore_missing_memofile=True)
        registros = [{coluna: str(record[coluna]).strip() for coluna in dbf_table.field_names} for record in dbf_table]
        df = pd.DataFrame(registros)
        
        # Verifique se a leitura está correta
        if df.empty:
            logger.warning(f"Nenhum dado encontrado no arquivo {caminho}")
        else:
            logger.info(f"{len(df)} registros encontrados no arquivo {caminho}")
        
        # Verificar se a coluna de data está no formato correto
        if 'DATAEMIS' in df.columns:
            df['DATAEMIS'] = pd.to_datetime(df['DATAEMIS'], errors='coerce', format='%Y-%m-%d')
        
        return df
    except Exception as e:
        logger.error(f"Erro ao ler o arquivo DBF: {e}")
        return pd.DataFrame({'Erro': [str(e)]})

# Função para extrair número da NF-e
def extrair_numero_nfe(chave):
    if len(chave) >= 44:
        return chave[25:34].strip()
    return 'Número Inválido'

# Função para salvar arquivos temporários
def salvar_arquivo_temporario(file):
    temp_dir = os.path.join(os.getcwd(), 'temp_files')
    os.makedirs(temp_dir, exist_ok=True)
    caminho_arquivo = os.path.join(temp_dir, file.filename)
    file.save(caminho_arquivo)
    return caminho_arquivo

# Rota de login
@app.route('/', methods=['GET', 'POST'])
def login():
    senha_correta = gerar_senha()  # Senha dinâmica
    if request.method == 'POST':
        senha_inserida = request.form.get('senha')
        if senha_inserida is None or senha_inserida == '':
            logger.warning('Nenhuma senha foi inserida.')
            return render_template('login.html', error='Por favor, insira a senha.')
        if senha_inserida == senha_correta:
            session['logado'] = True
            logger.info('Login bem-sucedido.')
            return redirect(url_for('validar_arquivos'))
        else:
            logger.warning('Senha incorreta.')
            return render_template('login.html', error='Senha incorreta. Tente novamente.')
    return render_template('login.html')

# Rota principal para validação de arquivos
@app.route('/validar', methods=['GET', 'POST'])
def validar_arquivos():
    if not session.get('logado'):
        logger.warning('Acesso não autorizado. Redirecionando para login.')
        return redirect(url_for('login'))

    if request.method == 'POST':
        documentos_files = request.files.getlist('documentos_files')
        if not documentos_files or len(documentos_files) == 0:
            logger.error('Nenhum arquivo foi enviado.')
            return render_template('validacao_documentos.html', error='Por favor, selecione os arquivos .DBF.')
        
        caminho_documentos = None
        for file in documentos_files:
            if file.filename.lower().endswith('.dbf'):
                logger.info(f"Arquivo recebido: {file.filename}")
                caminho_documentos = salvar_arquivo_temporario(file)

        if not caminho_documentos:
            logger.error('Arquivo de documentos não encontrado.')
            return render_template('validacao_documentos.html', error='Arquivo de documentos não encontrado.')

        # Ler os documentos e armazenar na sessão
        df_documentos = ler_tabela_dbf(caminho_documentos)

        if 'Erro' in df_documentos.columns:
            logger.error(f"Erro ao processar o arquivo: {df_documentos['Erro'][0]}")
            return render_template('validacao_documentos.html', error=df_documentos['Erro'][0])

        logger.info(f"Documentos carregados: {df_documentos.shape[0]} registros")
        session['documentos_df'] = df_documentos.to_dict('records')
        return redirect(url_for('filtrar_documentos'))

    return render_template('validacao_documentos.html')

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

    # Variáveis de filtros com valores iniciais ou valores do formulário
    data_inicial = request.form.get('data_inicial', primeiro_dia_mes)
    data_final = request.form.get('data_final', ultimo_dia_mes)
    fornecedor = request.form.get('fornecedor', '')
    cnpj = request.form.get('cnpj', '')

    # Filtragem dos documentos com base nos parâmetros
    if data_inicial:
        documentos_df['DATAEMIS'] = pd.to_datetime(documentos_df['DATAEMIS'], errors='coerce')
        documentos_df = documentos_df[documentos_df['DATAEMIS'] >= pd.to_datetime(data_inicial)]
    
    if data_final:
        documentos_df = documentos_df[documentos_df['DATAEMIS'] <= pd.to_datetime(data_final)]

    if fornecedor:
        documentos_df = documentos_df[documentos_df['NOME'].str.contains(fornecedor, case=False, na=False)]

    if cnpj:
        documentos_df = documentos_df[documentos_df['CNPJCPF'].str.contains(cnpj, case=False, na=False)]

    # Adicionar número da NF-E
    documentos_df['Numero_NFE'] = documentos_df['CHAVE'].apply(extrair_numero_nfe)

    # Verificar se os documentos estão sendo filtrados corretamente
    logger.info(f"Documentos filtrados: {documentos_df.shape[0]} registros")

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
        logger.error("Nenhum documento filtrado disponível para exportação.")
        return redirect(url_for('filtrar_documentos'))

    # Recupera os documentos filtrados da sessão
    documentos_filtrados = pd.DataFrame(session['documentos_filtrados'])

    # Verificar se a coluna 'Numero_NFE' existe, caso contrário, adicioná-la
    if 'Numero_NFE' not in documentos_filtrados.columns:
        logger.info("Adicionando a coluna 'Numero_NFE'")
        documentos_filtrados['Numero_NFE'] = documentos_filtrados['CHAVE'].apply(extrair_numero_nfe)

    # Verificar se todas as colunas necessárias estão presentes
    colunas_necessarias = ['DATAEMIS', 'CNPJCPF', 'NOME', 'Numero_NFE', 'CHAVE']
    if not all(coluna in documentos_filtrados.columns for coluna in colunas_necessarias):
        logger.error("Algumas colunas necessárias estão faltando no DataFrame.")
        return redirect(url_for('filtrar_documentos'))

    # Configuração do Excel
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')

    # Reordenar e renomear colunas para o Excel
    documentos_filtrados = documentos_filtrados[['DATAEMIS', 'CNPJCPF', 'NOME', 'Numero_NFE', 'CHAVE']]
    documentos_filtrados.columns = ['Data Emissão', 'CNPJ/CPF', 'Fornecedor', 'Número NF-E', 'Chave NF-E']

    # Exportar os dados para o Excel
    documentos_filtrados.to_excel(writer, index=False, sheet_name='Documentos Filtrados')
    writer.close()

    output.seek(0)
    return send_file(output, download_name='documentos_filtrados.xlsx', as_attachment=True)

if __name__ == '__main__':
    app.run(debug=True)
