from flask import Flask, render_template, request, redirect, url_for, send_file, session
from flask_session import Session
from dbfread import DBF
import pandas as pd
from io import BytesIO
import os
import logging
from datetime import datetime

app = Flask(__name__)
app.secret_key = 'supersecretkey'

# Configuração do Flask-Session para armazenar as sessões no sistema de arquivos
app.config['SESSION_TYPE'] = 'filesystem'
app.config['SESSION_PERMANENT'] = False
app.config['SESSION_FILE_DIR'] = os.path.join(os.getcwd(), 'flask_session')
app.config['SESSION_FILE_THRESHOLD'] = 500
Session(app)

# Configuração do logging
logging.basicConfig(filename='app.log', level=logging.DEBUG, 
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)

# Função para ler arquivos DBF
def ler_tabela_dbf(caminho):
    try:
        logger.info(f"Carregando arquivo DBF: {caminho}")
        dbf_table = DBF(caminho, ignore_missing_memofile=True)
        registros = [{coluna: str(record[coluna]).strip() for coluna in dbf_table.field_names} for record in dbf_table]
        df = pd.DataFrame(registros)
        
        # Certificar-se de que a coluna de data está no formato correto
        if 'DATAEMIS' in df.columns:
            df['DATAEMIS'] = pd.to_datetime(df['DATAEMIS'], errors='coerce', format='%Y-%m-%d')
            logger.debug(f"Coluna 'DATAEMIS' convertida corretamente")
        
        logger.debug(f"Primeiras linhas do DataFrame: {df.head()}")
        return df
    except Exception as e:
        logger.error(f"Erro ao ler o arquivo DBF: {e}")
        return pd.DataFrame({'Erro': [str(e)]})

# Função para extrair número da NF-e
def extrair_numero_nfe(chave):
    if len(chave) >= 44:
        return chave[25:34].strip()
    return 'Número Inválido'

# Rota principal para validação de arquivos
@app.route('/', methods=['GET', 'POST'])
def validar_arquivos():
    if request.method == 'POST':
        documentos_files = request.files.getlist('documentos_files')
        if not documentos_files:
            logger.warning("Nenhum arquivo selecionado")
            return render_template('validacao_documentos.html', error='Por favor, selecione os arquivos .DBF e .FPT.')

        caminho_documentos = None
        for file in documentos_files:
            if file.filename.lower().endswith('.dbf'):
                caminho_documentos = salvar_arquivo_temporario(file)

        if not caminho_documentos:
            logger.warning("Arquivo de documentos não encontrado.")
            return render_template('validacao_documentos.html', error='Arquivo de documentos não encontrado.')

        df_documentos = ler_tabela_dbf(caminho_documentos)
        
        if 'Erro' in df_documentos.columns:
            logger.error(f"Erro ao processar os documentos: {df_documentos['Erro'][0]}")
            return render_template('validacao_documentos.html', error=df_documentos['Erro'][0])

        session['documentos_df'] = df_documentos.to_dict('records')
        logger.info("Documentos carregados com sucesso.")
        return redirect(url_for('filtrar_documentos'))

    return render_template('validacao_documentos.html')

# Rota para filtrar documentos
@app.route('/filtrar', methods=['GET', 'POST'])
def filtrar_documentos():
    if 'documentos_df' not in session:
        logger.warning("Sessão de documentos não encontrada, redirecionando.")
        return redirect(url_for('validar_arquivos'))

    documentos_df = pd.DataFrame(session['documentos_df'])

    # Obter o mês atual
    hoje = datetime.today()
    primeiro_dia_mes = hoje.replace(day=1).strftime('%Y-%m-%d')
    ultimo_dia_mes = hoje.strftime('%Y-%m-%d')

    # Variáveis de filtros com valores iniciais vazios ou valores padrão
    data_inicial = request.form.get('data_inicial', '')
    data_final = request.form.get('data_final', '')
    fornecedor = request.form.get('fornecedor', '')
    cnpj = request.form.get('cnpj', '')

    # Definir valores padrão se não forem fornecidos
    default_data_inicial = primeiro_dia_mes
    default_data_final = ultimo_dia_mes

    if request.method == 'POST':
        logger.info(f"Filtrando documentos com os parâmetros: Data Inicial={data_inicial}, Data Final={data_final}, Fornecedor={fornecedor}, CNPJ/CPF={cnpj}")

        if data_inicial:
            try:
                # Pandas consegue interpretar automaticamente o formato ISO (YYYY-MM-DD)
                data_inicial = pd.to_datetime(data_inicial, errors='coerce')
                documentos_df['DATAEMIS'] = pd.to_datetime(documentos_df['DATAEMIS'], errors='coerce')
                documentos_df = documentos_df[documentos_df['DATAEMIS'] >= data_inicial]
            except Exception as e:
                logger.error(f"Erro ao processar a data inicial: {e}")
        else:
            data_inicial = default_data_inicial

        if data_final:
            try:
                data_final = pd.to_datetime(data_final, errors='coerce')
                documentos_df = documentos_df[documentos_df['DATAEMIS'] <= data_final]
            except Exception as e:
                logger.error(f"Erro ao processar a data final: {e}")
        else:
            data_final = default_data_final

        if fornecedor:
            documentos_df = documentos_df[documentos_df['NOME'].str.contains(fornecedor, case=False, na=False)]

        if cnpj:
            documentos_df = documentos_df[documentos_df['CNPJCPF'].str.contains(cnpj, case=False, na=False)]

        # Adiciona a coluna de número NF-E
        documentos_df['Numero_NFE'] = documentos_df['CHAVE'].apply(extrair_numero_nfe)

        # Atualiza os documentos filtrados na sessão
        session['documentos_filtrados'] = documentos_df.to_dict('records')
        logger.info(f"Filtragem realizada com sucesso. Total de documentos: {len(documentos_df)}")

    return render_template('filtragem.html', 
                           documentos=documentos_df.to_dict('records'), 
                           data_inicial=data_inicial.strftime('%Y-%m-%d') if data_inicial else default_data_inicial,
                           data_final=data_final.strftime('%Y-%m-%d') if data_final else default_data_final,
                           fornecedor=fornecedor,
                           cnpj=cnpj,
                           default_data_inicial=default_data_inicial,
                           default_data_final=default_data_final)

# Rota para exportar documentos filtrados para Excel
@app.route('/exportar', methods=['GET'])
def exportar_documentos():
    if 'documentos_filtrados' not in session:
        logger.warning("Nenhum documento filtrado encontrado na sessão.")
        return redirect(url_for('filtrar_documentos'))

    documentos_filtrados = pd.DataFrame(session['documentos_filtrados'])

    # Configuração do Excel
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')

    # Reordenar e Renomear colunas para Excel conforme a visualização na tabela HTML
    documentos_filtrados = documentos_filtrados[['DATAEMIS', 'CNPJCPF', 'NOME', 'Numero_NFE', 'CHAVE']]
    documentos_filtrados.columns = ['Data Emissão', 'CNPJ/CPF', 'Fornecedor', 'Número NF-E', 'Chave NF-E']

    # Formatar a coluna de "Data Emissão" para o formato dia/mês/ano horas:minutos:segundos
    documentos_filtrados['Data Emissão'] = documentos_filtrados['Data Emissão'].dt.strftime('%d/%m/%Y %H:%M:%S')

    # Exportar os dados para Excel
    documentos_filtrados.to_excel(writer, index=False, sheet_name='Documentos Filtrados')

    # Formatação do Excel
    worksheet = writer.sheets['Documentos Filtrados']
    for col_num, value in enumerate(documentos_filtrados.columns.values):
        worksheet.write(0, col_num, value)
        worksheet.set_column(col_num, col_num, 25)  # Ajusta a largura das colunas para acomodar a data e hora

    writer.close()
    output.seek(0)

    logger.info("Documentos exportados para Excel com sucesso.")
    return send_file(output, download_name='documentos_filtrados.xlsx', as_attachment=True)


# Função para salvar arquivo temporariamente
def salvar_arquivo_temporario(file):
    temp_dir = os.path.join(os.getcwd(), 'temp_files')
    os.makedirs(temp_dir, exist_ok=True)
    caminho_arquivo = os.path.join(temp_dir, file.filename)
    file.save(caminho_arquivo)
    logger.info(f"Arquivo salvo em: {caminho_arquivo}")
    return caminho_arquivo

if __name__ == '__main__':
    app.run(debug=True)
