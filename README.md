# Como usar

1. **Instale as dependências necessárias:**

   ```bash
   pip install requests
   ```

2. **Coloque um arquivo **`links.txt`** com **um link por linha** na pasta **`input`\*\*.

3. **Execute o script:**

   ```bash
   python main.py
   ```

4. **Resultados:**
   - Arquivo com os dados extraídos: **`output/resultado_excel_ptbr.csv`**  
     (separador `;`, coordenadas com 2 casas decimais)
   - Links que **não deram certo**: **`output/links_falhos.txt`** (um por linha)
