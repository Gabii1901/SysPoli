from flask import Flask, render_template, request, redirect, url_for, send_file, session
from flask_session import Session
from dbfread import DBF
import pandas as pd
from io import BytesIO
import os

app = Flask(__name__)
app.secret_key = 'supersecretkey'

# Configuração do Flask-Session para armazenar as sessões no sistema de arquivos
app.config['SESSION_TYPE'] = 'filesystem'
app.config['SESSION_PERMANENT'] = False
app.config['SESSION_FILE_DIR'] = os.path.join(os.getcwd(), 'flask_session')
app.config['SESSION_FILE_THRESHOLD'] = 500
Session(app)


def ler_tabela_dbf(caminho):
    try:
        dbf_table = DBF(caminho, ignore_missing_memofile=True)
        registros = [{coluna: str(record[coluna]).strip() for coluna in dbf_table.field_names} for record in dbf_table]
        df = pd.DataFrame(registros)
        return df
    except Exception as e:
        return pd.DataFrame({'Erro': [str(e)]})


def extrair_numero_nfe(chave):
    if len(chave) >= 44:
        return chave[25:34].strip()
    return 'Número Inválido'


@app.route('/', methods=['GET', 'POST'])
def validar_arquivos():
    if request.method == 'POST':
        documentos_files = request.files.getlist('documentos_files')
        if not documentos_files:
            return render_template('validacao_documentos.html', error='Por favor, selecione os arquivos .DBF e .FPT.')

        caminho_documentos = None
        for file in documentos_files:
            if file.filename.lower().endswith('.dbf'):
                caminho_documentos = salvar_arquivo_temporario(file)

        if not caminho_documentos:
            return render_template('validacao_documentos.html', error='Arquivo de documentos não encontrado.')

        df_documentos = ler_tabela_dbf(caminho_documentos)
        
        if 'Erro' in df_documentos.columns:
            return render_template('validacao_documentos.html', error=df_documentos['Erro'][0])

        session['documentos_df'] = df_documentos.to_dict('records')

        return redirect(url_for('filtrar_documentos'))

    return render_template('validacao_documentos.html')


@app.route('/filtrar', methods=['GET', 'POST'])
def filtrar_documentos():
    if 'documentos_df' not in session:
        return redirect(url_for('validar_arquivos'))

    documentos_df = pd.DataFrame(session['documentos_df'])

    if request.method == 'POST':
        data_inicial = request.form.get('data_inicial')
        data_final = request.form.get('data_final')
        fornecedor = request.form.get('fornecedor')
        cnpj = request.form.get('cnpj')

        if data_inicial:
            documentos_df['DATAEMIS'] = pd.to_datetime(documentos_df['DATAEMIS'], errors='coerce', format='%Y-%m-%d')
            data_inicial = pd.to_datetime(data_inicial, format='%Y-%m-%d')
            documentos_df = documentos_df[documentos_df['DATAEMIS'] >= data_inicial]

        if data_final:
            data_final = pd.to_datetime(data_final, format='%Y-%m-%d')
            documentos_df = documentos_df[documentos_df['DATAEMIS'] <= data_final]

        if fornecedor:
            documentos_df = documentos_df[documentos_df['NOME'].str.contains(fornecedor, case=False, na=False)]

        if cnpj:
            documentos_df = documentos_df[documentos_df['CNPJCPF'].str.contains(cnpj, case=False, na=False)]

        # Adiciona a coluna de número NF-E
        documentos_df['Numero_NFE'] = documentos_df['CHAVE'].apply(extrair_numero_nfe)

        # Atualiza os documentos filtrados na sessão
        session['documentos_filtrados'] = documentos_df.to_dict('records')

    return render_template('filtragem.html', documentos=documentos_df.to_dict('records'))


@app.route('/exportar', methods=['GET'])
def exportar_documentos():
    if 'documentos_filtrados' not in session:
        return redirect(url_for('filtrar_documentos'))

    documentos_filtrados = pd.DataFrame(session['documentos_filtrados'])

    # Configuração do Excel
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')

    # Reordenar e Renomear colunas para Excel conforme a visualização na tabela HTML
    documentos_filtrados = documentos_filtrados[['DATAEMIS', 'CNPJCPF', 'NOME', 'Numero_NFE', 'CHAVE']]
    documentos_filtrados.columns = ['Data Emissão', 'CNPJ/CPF', 'Fornecedor', 'Número NF-E', 'Chave NF-E']

    documentos_filtrados.to_excel(writer, index=False, sheet_name='Documentos Filtrados')

    # Formatação do Excel
    worksheet = writer.sheets['Documentos Filtrados']
    for col_num, value in enumerate(documentos_filtrados.columns.values):
        worksheet.write(0, col_num, value)
        worksheet.set_column(col_num, col_num, 20)  # Ajusta a largura das colunas

    writer.close()
    output.seek(0)

    return send_file(output, download_name='documentos_filtrados.xlsx', as_attachment=True)


def salvar_arquivo_temporario(file):
    temp_dir = os.path.join(os.getcwd(), 'temp_files')
    os.makedirs(temp_dir, exist_ok=True)
    caminho_arquivo = os.path.join(temp_dir, file.filename)
    file.save(caminho_arquivo)
    return caminho_arquivo


if __name__ == '__main__':
    app.run(debug=True)
