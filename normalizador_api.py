from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
import io
import re

# ==========================================================
# CÓPIA DAS FUNÇÕES CORE DO SEU app.py
# (Estas funções devem ser idênticas às do seu app.py para manter a lógica)
# ==========================================================

def extrair_cep_bruto(texto):
    # [COPIE A FUNÇÃO extrair_cep_bruto AQUI]
    if not isinstance(texto, str): return None
    texto_limpo = texto.replace('"', '').replace("'", "").strip()
    match_formatado = re.search(r'\b\d{2}[. ]?\d{3}-\d{3}\b', texto_limpo)
    if match_formatado: return re.sub(r'\D', '', match_formatado.group(0))
    match_palavra = re.search(r'(?:CEP|C\.E\.P).{0,5}?(\d{8})', re.sub(r'[-.]', '', texto_limpo), re.IGNORECASE)
    if match_palavra: return match_palavra.group(1)
    match_8_digitos = re.search(r'(?<!\d)(\d{8})(?!\d)', texto_limpo)
    if match_8_digitos: return match_8_digitos.group(1)
    match_7_digitos = re.search(r'(?<!\d)(\d{7})(?!\d)', texto_limpo)
    if match_7_digitos: return "0" + match_7_digitos.group(1)
    return None

def extrair_numero_inteligente(texto):
    # [COPIE A FUNÇÃO extrair_numero_inteligente AQUI]
    if not isinstance(texto, str): return ""
    texto_upper = texto.upper().replace('"', '').strip()
    lista_proibida = [
        r'APTO', r'APT', r'AP', r'APARTAMENTO', r'APART',  r'LOTE', r'LT', r'LOT',
        r'CASA', r'CS', r'CN', r'BLOCO', r'BL', r'SALA', r'SL', r'CJ', r'CONJUNTO',
        r'LOJA', r'LJ', r'ANDAR', r'AND', r'UNIDADE', r'UNID', r'FRENTE', r'FD', 
        r'FUNDOS', r'FDS', r'QD', r'QUADRA', r'BOX', r'GARAGEM', r'KM'
    ]
    regex_proibidos = r'\b(?:' + '|'.join(lista_proibida) + r')\.?\s*\d+[A-Z]?\b'
    texto_upper = re.sub(regex_proibidos, '', texto_upper, flags=re.IGNORECASE)
    texto_upper = re.sub(r'\b\d{5}[-.]?\d{3}\b', '', texto_upper)
    texto_limpo_numeros = re.sub(r'\d{7,}', '', texto_upper)
    def eh_valido(n): return len(n) <= 6
    if re.search(r'\b(S/N|SN|S\.N|SEM N|S-N)\b', texto_limpo_numeros): return "S/N"
    match_antes_virgula = re.search(r'\b(\d+)\s*,', texto_limpo_numeros)
    if match_antes_virgula and eh_valido(match_antes_virgula.group(1)): return match_antes_virgula.group(1)
    match_hifen = re.search(r'\s[-–]\s*(\d+)\s*(?:[-–]|$)', texto_limpo_numeros)
    if match_hifen and eh_valido(match_hifen.group(1)): return match_hifen.group(1)
    match_meio = re.search(r',\s*(\d+)\s*(?:-|,|;|/|AP|BL)', texto_limpo_numeros)
    if match_meio and eh_valido(match_meio.group(1)): return match_meio.group(1)
    match_n = re.search(r'(?:nº|n|num)\.?\s*(\d+)', texto_limpo_numeros, re.IGNORECASE)
    if match_n and eh_valido(match_n.group(1)): return match_n.group(1)
    match_virgula = re.search(r',\s*(\d+)', texto_limpo_numeros)
    if match_virgula and eh_valido(match_virgula.group(1)): return match_virgula.group(1)
    match_fim = re.search(r'\s(\d+)$', texto_limpo_numeros)
    if match_fim and eh_valido(match_fim.group(1)): return match_fim.group(1)
    numeros_soltos = re.findall(r'\d+', texto_limpo_numeros)
    for n in numeros_soltos:
        if eh_valido(n): return n
    return ""

def gerar_status(cep, numero):
    # [COPIE A FUNÇÃO gerar_status AQUI]
    status = []
    if not cep: status.append("FALTA CEP") 
    if not numero: status.append("FALTA NÚMERO")
    elif numero == "S/N": status.append("S/N (Manual)")
    
    if not status: return "OK"
    return " ".join(status)

def processar_planilha(df):
    # [MODIFICAÇÃO: Esta função usa a lógica de processamento do seu app.py, mas sem os parâmetros Streamlit]
    df = df.copy()
    
    # 1. Padroniza colunas (o robô Streamlit usa um mapa, aqui usamos nomes padrão do front)
    # Requisito do Frontend: As colunas de entrada devem ser 'Endereço Completo' e 'A/C' (aos cuidados)
    # Assumimos que o front está enviando a planilha com essas colunas.
    
    col_endereco = 'Endereço Completo'
    col_ac = 'A/C'
    
    # 2. Extrações (CEP e Número)
    df[col_endereco] = df[col_endereco].astype(str)
    
    df['CEP_Final'] = df[col_endereco].apply(extrair_cep_bruto)
    df['Numero_Final'] = df[col_endereco].apply(extrair_numero_inteligente)
    
    # 3. Limpeza do Logradouro
    def limpar_texto(row):
        txt = str(row[col_endereco]).replace('"', '').replace("'", "")
        cep = row['CEP_Final']
        num = row['Numero_Final']
        
        if cep:
            txt = re.sub(rf'{cep[:5]}.?{cep[5:]}', '', txt) 
            txt = re.sub(rf'{cep}', '', txt)
            if cep.startswith('0'):
                cep_sem_zero = cep[1:]
                txt = re.sub(rf'{cep_sem_zero}', '', txt)
            
        if num and num != "S/N":
            txt = re.sub(rf'\b{num}\b', '', txt)
            
        txt = re.sub(r'\bCEP\b[:.]?', '', txt, flags=re.IGNORECASE)
        txt = re.sub(r'\s[-–]\s*$', '', txt)
        return txt.strip(' ,;-.')

    df['Logradouro_Final'] = df.apply(limpar_texto, axis=1)
    
    # 4. Status
    df['STATUS_SISTEMA'] = df.apply(lambda x: gerar_status(x['CEP_Final'], x['Numero_Final']), axis=1)
    
    return df

# ==========================================================
# CAMADA FLASK API
# ==========================================================

app = Flask(__name__)

# CONFIGURAÇÃO CRÍTICA DO CORS
CORS(app, resources={r"/*": {"origins": [
    "https://entregas.elobrindes.com.br", 
    "http://localhost:5500", 
    "http://127.0.0.1:5500"
]}})

@app.route('/normalizar', methods=['POST'])
def handle_normalizacao_api():
    if 'import-file' not in request.files and 'file' not in request.files:
        return jsonify({"error": "Nenhum arquivo enviado. Campo esperado: 'file'"}), 400
    
    file = request.files.get('file') or request.files.get('import-file')
    
    # Mapeamento de colunas de entrada esperadas
    col_endereco = request.form.get('col_endereco', 'Endereço Completo')
    col_ac = request.form.get('col_ac', 'A/C')

    try:
        if file.filename.endswith('.csv'):
            df = pd.read_csv(file)
        else:
            df = pd.read_excel(file)
            
        df = df.astype(str).replace('nan', '')
        
        # Renomeia colunas para o padrão do robô (temporário para o processamento)
        df.rename(columns={col_endereco: 'Endereço Completo', col_ac: 'A/C'}, inplace=True)

        if 'Endereço Completo' not in df.columns:
            return jsonify({"error": f"Coluna de endereço ('{col_endereco}') não encontrada após renomear."}), 400

        df_processado = processar_planilha(df)

        # --- CONVERSÃO PARA O FORMATO JSON ESPERADO PELO index33.html ---
        results = []
        for index, row in df_processado.iterrows():
            results.append({
                "status": row['STATUS_SISTEMA'],
                "cep": row['CEP_Final'],
                "logradouro": row['Logradouro_Final'],
                "numero": row['Numero_Final'],
                "bairro": "", # O robô não extrai bairro/cidade diretamente, mas o front espera
                "cidade": "", # O robô não extrai bairro/cidade diretamente
                "estado": "", # O robô não extrai UF diretamente
                "complemento": "", # O robô não extrai complemento
                "apelido_local": row.get('A/C', ''), 
                "fullAddress": row['Endereço Completo']
            })

        return jsonify(results)
        
    except Exception as e:
        return jsonify({"error": f"Erro interno de processamento no servidor: {e.__class__.__name__}: {str(e)}"}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
