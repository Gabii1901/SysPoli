document.getElementById('form').addEventListener('submit', function (e) {
    e.preventDefault();
    
    const formData = new FormData(document.getElementById('form'));

    fetch('/documentos', {
        method: 'POST',
        body: formData
    })
    .then(response => response.json())
    .then(data => {
        const resultDiv = document.getElementById('result');
        resultDiv.innerHTML = JSON.stringify(data, null, 2);
    })
    .catch(error => console.error('Error:', error));
});

function exportar() {
    // Função de exportação, se necessário
}
