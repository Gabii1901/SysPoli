from flask import Flask, render_template, request, redirect, url_for, session
from dbfread import DBF

app = Flask(__name__)
app.secret_key = 'supersecretkey'  # Necessário para usar sessões

# Função para ler o arquivo .DBF enviado (usando o 'stream' do FileStorage)
def ler_arquivo_dbf(arquivo):
    dbf = DBF(arquivo.stream)  # Usa o stream do FileStorage para ler o conteúdo
    registros = []
    for record in dbf:
        registros.append(record)
    return registros

# Rota para validar e processar os arquivos DBF
@app.route('/', methods=['GET', 'POST'])
def validar_arquivos():
    if request.method == 'POST':
        fornecedores_file = request.files['fornecedores_file']
        documentos_file = request.files['documentos_file']
        
        # Verifica se os arquivos foram carregados
        if not fornecedores_file or not documentos_file:
            return render_template('validacao.html', error="Por favor, selecione ambos os arquivos.")

        # Processa os arquivos .DBF usando o stream
        fornecedores = ler_arquivo_dbf(fornecedores_file)
        documentos = ler_arquivo_dbf(documentos_file)

        # Armazena os resultados na sessão para usar na página de filtragem
        session['fornecedores'] = fornecedores
        session['documentos'] = documentos

        # Redireciona para a página de filtragem
        return redirect(url_for('filtrar_documentos'))

    return render_template('validacao.html')

# Rota para a página de filtragem de documentos
@app.route('/filtrar', methods=['GET', 'POST'])
def filtrar_documentos():
    # Verifica se os dados dos arquivos foram carregados
    if 'fornecedores' not in session or 'documentos' not in session:
        return redirect(url_for('validar_arquivos'))

    if request.method == 'POST':
        # Aqui você pode aplicar filtros aos dados de 'fornecedores' e 'documentos'
        data_inicial = request.form.get('data_inicial')
        data_final = request.form.get('data_final')
        fornecedor = request.form.get('fornecedor')
        cnpj = request.form.get('cnpj')

        # Aqui você aplicaria os filtros de data, fornecedor e CNPJ aos dados
        # Por enquanto, como exemplo, estamos apenas retornando uma mensagem simples
        return render_template('filtragem.html', resultado="Filtragem aplicada com sucesso.")
    
    return render_template('filtragem.html')

if __name__ == '__main__':
    app.run(debug=True)
