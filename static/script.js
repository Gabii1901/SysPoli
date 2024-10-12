document.getElementById('form-documentos').addEventListener('submit', function (e) { 
    e.preventDefault();
    
    const formData = new FormData(this);

    fetch('/validar', {
        method: 'POST',
        body: formData
    })
    .then(response => response.text())
    .then(data => {
        const resultDiv = document.getElementById('result');
        resultDiv.innerHTML = data;
    })
    .catch(error => console.error('Erro:', error));
});
