from flask import Flask, render_template, request, redirect, url_for, session
from dbfread import DBF

app = Flask(__name__)
app.secret_key = 'supersecretkey'  # Necessário para usar sessões

# Função para ler colunas específicas de um arquivo .DBF
def ler_arquivo_dbf(arquivo, colunas):
    dbf = DBF(arquivo.stream)  # Usa o stream do FileStorage para ler o conteúdo
    registros = []
    for record in dbf:
        # Adiciona ao dicionário apenas as colunas especificadas
        registros.append({coluna: record[coluna] for coluna in colunas if coluna in record})
    return registros

# Função para relacionar os dados dos documentos fiscais com os fornecedores
def relacionar_documentos_fornecedores(documentos, fornecedores):
    # Cria um dicionário para mapeamento rápido dos fornecedores usando CNPJ/CPF como chave
    fornecedores_dict = {}
    for f in fornecedores:
        cnpj_cpf = f.get('Cnpj') or f.get('Cpf')  # Usa CNPJ, se não, usa CPF
        if cnpj_cpf:
            fornecedores_dict[cnpj_cpf] = f

    # Relaciona cada documento ao fornecedor correspondente usando CNPJ/CPF
    documentos_relacionados = []
    for doc in documentos:
        cnpj_cpf = doc.get('Cnpjcpf')
        fornecedor = fornecedores_dict.get(cnpj_cpf, None)
        
        # Se encontrar o fornecedor correspondente, adiciona ao resultado
        if fornecedor:
            documentos_relacionados.append({
                'Chave': doc.get('Chave'),
                'Dataemis': doc.get('Dataemis'),
                'Cnpjcpf': doc.get('Cnpjcpf'),
                'NomeDocumento': doc.get('Nome'),
                'FornecedorNome': fornecedor.get('Nome'),
                'FornecedorCnpj': fornecedor.get('Cnpj'),
                'FornecedorCpf': fornecedor.get('Cpf')
            })

    return documentos_relacionados

# Rota para validar e processar os arquivos DBF
@app.route('/', methods=['GET', 'POST'])
def validar_arquivos():
    if request.method == 'POST':
        fornecedores_file = request.files['fornecedores_file']
        documentos_file = request.files['documentos_file']
        
        # Verifica se os arquivos foram carregados
        if not fornecedores_file or not documentos_file:
            return render_template('validacao.html', error="Por favor, selecione ambos os arquivos.")

        # Defina as colunas específicas para cada arquivo
        colunas_fornecedores = ['Codfor', 'Nome', 'Cnpj', 'Cpf']
        colunas_documentos = ['Chave', 'Dataemis', 'Cnpjcpf', 'Nome']

        # Processa os arquivos .DBF com as colunas específicas
        fornecedores = ler_arquivo_dbf(fornecedores_file, colunas_fornecedores)
        documentos = ler_arquivo_dbf(documentos_file, colunas_documentos)

        # Relaciona os documentos com os fornecedores
        documentos_relacionados = relacionar_documentos_fornecedores(documentos, fornecedores)

        # Armazena os resultados na sessão para usar na página de filtragem
        session['documentos_relacionados'] = documentos_relacionados

        # Redireciona para a página de filtragem
        return redirect(url_for('filtrar_documentos'))

    return render_template('validacao.html')

# Rota para a página de filtragem de documentos
@app.route('/filtrar', methods=['GET', 'POST'])
def filtrar_documentos():
    # Verifica se os dados dos documentos relacionados foram carregados
    if 'documentos_relacionados' not in session:
        return redirect(url_for('validar_arquivos'))

    documentos_relacionados = session['documentos_relacionados']

    if request.method == 'POST':
        # Aplique os filtros e exiba os resultados
        data_inicial = request.form.get('data_inicial')
        data_final = request.form.get('data_final')
        fornecedor_nome = request.form.get('fornecedor')
        cnpj = request.form.get('cnpj')

        # Aplica os filtros nos documentos relacionados
        documentos_filtrados = [doc for doc in documentos_relacionados if (
            (not data_inicial or (doc['Dataemis'] >= data_inicial)) and
            (not data_final or (doc['Dataemis'] <= data_final)) and
            (not fornecedor_nome or (fornecedor_nome.lower() in doc['FornecedorNome'].lower())) and
            (not cnpj or (cnpj in doc['FornecedorCnpj'] or cnpj in doc['FornecedorCpf']))
        )]

        return render_template('filtragem.html', documentos=documentos_filtrados)

    return render_template('filtragem.html', documentos=documentos_relacionados)

if __name__ == '__main__':
    app.run(debug=True)