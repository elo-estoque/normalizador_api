from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
import io
import re
import unicodedata

# ==========================================================
# FUNÇÕES CORE (LÓGICA ROBUSTA DO APP.PY)
# ==========================================================

def remover_acentos(txt):
    if not isinstance(txt, str): return str(txt)
    return unicodedata.normalize('NFKD', txt).encode('ASCII', 'ignore').decode('ASCII')

def extrair_cep_bruto(texto):
    if not isinstance(texto, str): return None
    texto_limpo = texto.replace('"', '').replace("'", "").strip()
    # Padrão 1: CEP Formatado
    match_formatado = re.search(r'\b\d{2}[. ]?\d{3}-\d{3}\b', texto_limpo)
    if match_formatado: return re.sub(r'\D', '', match_formatado.group(0))
    # Padrão 2: Palavra CEP
    match_palavra = re.search(r'(?:CEP|C\.E\.P).{0,5}?(\d{8})', re.sub(r'[-.]', '', texto_limpo), re.IGNORECASE)
    if match_palavra: return match_palavra.group(1)
    # Padrão 3: 8 digitos soltos
    match_8_digitos = re.search(r'(?<!\d)(\d{8})(?!\d)', texto_limpo)
    if match_8_digitos: return match_8_digitos.group(1)
    # Padrão 4: 7 digitos (erro comum)
    match_7_digitos = re.search(r'(?<!\d)(\d{7})(?!\d)', texto_limpo)
    if match_7_digitos: return "0" + match_7_digitos.group(1)
    return None

def formatar_cep(cep_bruto):
    if not cep_bruto or len(cep_bruto) != 8:
        return ""
    return f"{cep_bruto[:5]}-{cep_bruto[5:]}"

def extrair_uf(texto):
    if not isinstance(texto, str): return ''
    match = re.search(r'\b([A-Z]{2})\b$', texto.strip())
    if match: return match.group(1)
    return ''

def classificar_regiao(uf):
    sul = ['RS', 'SC', 'PR']
    sudeste = ['SP', 'RJ', 'MG', 'ES']
    centro_oeste = ['MT', 'MS', 'GO', 'DF']
    nordeste = ['BA', 'PI', 'MA', 'CE', 'RN', 'PB', 'PE', 'AL', 'SE']
    norte = ['AC', 'RO', 'AM', 'RR', 'PA', 'AP', 'TO']
    uf = str(uf).upper().strip()
    if uf in sul: return 'Sul'
    if uf in sudeste: return 'Sudeste'
    if uf in centro_oeste: return 'Centro-Oeste'
    if uf in nordeste: return 'Nordeste'
    if uf in norte: return 'Norte'
    return ''

def extrair_numero_inteligente(texto):
    if not isinstance(texto, str): return ""
    texto_upper = texto.upper().replace('"', '').strip()
    
    # LISTA COMPLETA DE PROIBIÇÕES (A mesma do app.py que funciona bem)
    lista_proibida = [
        r'APTO', r'APT', r'AP', r'APARTAMENTO', r'APART',  r'LOTE', r'LT', r'LOT',
        r'CASA', r'CS', r'CN', r'BLOCO', r'BL', r'SALA', r'SL', r'CJ', r'CONJUNTO',
        r'LOJA', r'LJ', r'ANDAR', r'AND', r'UNIDADE', r'UNID', r'FRENTE', r'FD', 
        r'FUNDOS', r'FDS', r'QD', r'QUADRA', r'BOX', r'GARAGEM', r'KM'
    ]
    regex_proibidos = r'\b(?:' + '|'.join(lista_proibida) + r')\.?\s*\d+[A-Z]?\b'
    texto_upper = re.sub(regex_proibidos, '', texto_upper, flags=re.IGNORECASE)
    
    # Remove CEPs
    texto_upper = re.sub(r'\b\d{5}[-.]?\d{3}\b', '', texto_upper)
    texto_limpo_numeros = re.sub(r'\d{7,}', '', texto_upper)
    
    def eh_valido(n): return len(n) <= 6
    
    # Lógica de extração
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
    status = []
    if not cep: status.append("FALTA CEP") 
    if not numero: status.append("FALTA NÚMERO")
    elif numero == "S/N": status.append("S/N (Manual)")
    
    if not status: return "OK"
    return " ".join(status)

# --- DETECÇÃO AUTOMÁTICA (Necessária pq a API não tem interface de seleção) ---
def detectar_colunas(df):
    col_map = {'endereco': None, 'ac': None}
    cols_norm = {c: remover_acentos(str(c)).lower().strip() for c in df.columns}
    
    keywords_end = ['endereco', 'logradouro', 'rua', 'address', 'local', 'entrega', 'destino']
    keywords_ac = ['a/c', 'aos cuidados', 'destinatario', 'nome', 'cliente', 'recebedor']

    for key in keywords_end:
        for original, norm in cols_norm.items():
            if key in norm:
                col_map['endereco'] = original
                break
        if col_map['endereco']: break
        
    for key in keywords_ac:
        for original, norm in cols_norm.items():
            if key in norm:
                col_map['ac'] = original
                break
        if col_map['ac']: break
        
    return col_map

def processar_planilha_api(df):
    df = df.copy()
    
    # 1. Filtra linhas vazias (CORREÇÃO DO PROBLEMA DAS 1000 LINHAS)
    df.dropna(how='all', inplace=True)
    
    # 2. Detecção de colunas
    mapa = detectar_colunas(df)
    col_end = mapa['endereco'] or df.columns[0] # Fallback: primeira coluna
    col_ac = mapa['ac'] or 'A/C (Não detectado)'
    
    if col_ac not in df.columns:
        df[col_ac] = '' # Cria vazia se não achar

    # Renomeia para facilitar
    df.rename(columns={col_end: 'Endereço Original', col_ac: 'A/C_Final'}, inplace=True)
    
    # Garante string
    df['Endereço Original'] = df['Endereço Original'].astype(str).replace('nan', '')
    df['A/C_Final'] = df['A/C_Final'].astype(str).replace('nan', '')
    
    # Filtro extra para remover linhas onde o endereço ficou vazio após conversão
    df = df[df['Endereço Original'].str.strip() != '']

    # 3. Extrações (USANDO A LÓGICA FORTE)
    df['CEP_Bruto'] = df['Endereço Original'].apply(extrair_cep_bruto)
    df['CEP_Final'] = df['CEP_Bruto'].apply(formatar_cep)
    df['Numero_Final'] = df['Endereço Original'].apply(extrair_numero_inteligente)
    
    # 4. Limpeza Logradouro
    def limpar_logradouro(row):
        txt = row['Endereço Original'].replace('"', '').replace("'", "")
        cep = row['CEP_Bruto']
        num = row['Numero_Final']
        
        if cep:
            txt = re.sub(rf'{cep[:5]}.?{cep[5:]}', '', txt) 
            txt = re.sub(rf'{cep}', '', txt)
            if cep.startswith('0'): txt = re.sub(rf'{cep[1:]}', '', txt)
            
        if num and num != "S/N":
            txt = re.sub(rf'\b{num}\b', '', txt)
            
        txt = re.sub(r'\bCEP\b[:.]?', '', txt, flags=re.IGNORECASE)
        txt = re.sub(r'\s[-–]\s*$', '', txt)
        match_uf = re.search(r'\b([A-Z]{2})\b$', txt.strip())
        if match_uf: txt = re.sub(r'\b[A-Z]{2}\b$', '', txt)
            
        return txt.strip(' ,;/-')

    df['Logradouro_Final'] = df.apply(limpar_logradouro, axis=1)
    
    # 5. Geografia (Bairro/Cidade/UF)
    def separar_geo(texto_orig):
        uf = extrair_uf(texto_orig)
        cidade = ''
        bairro = ''
        if uf:
            partes = re.split(r'[,-/]', texto_orig)
            partes = [p.strip() for p in partes if p.strip()]
            if partes and partes[-1].upper() == uf: partes.pop()
            if partes: cidade = partes[-1]
            if len(partes) > 1: bairro = partes[-2]
        return pd.Series([bairro, cidade, uf])

    df[['Bairro_Final', 'Cidade_Final', 'UF_Final']] = df['Endereço Original'].apply(separar_geo)
    df['Regiao_Final'] = df['UF_Final'].apply(classificar_regiao)
    
    # 6. Status
    df['Status_Sistema'] = df.apply(lambda x: gerar_status(x['CEP_Final'], x['Numero_Final']), axis=1)
    
    return df

# ==========================================================
# FLASK API
# ==========================================================

app = Flask(__name__)
CORS(app)

@app.route('/normalizar', methods=['POST'])
def handle_normalizacao_api():
    if 'import-file' not in request.files and 'file' not in request.files:
        return jsonify({"error": "Nenhum arquivo enviado."}), 400
    
    file = request.files.get('file') or request.files.get('import-file')
    
    try:
        # Leitura Inteligente do Arquivo
        if file.filename.endswith('.csv'):
            try:
                df = pd.read_csv(file, encoding='utf-8', sep=';')
                if len(df.columns) < 2:
                    file.seek(0)
                    df = pd.read_csv(file, encoding='utf-8', sep=',')
            except:
                file.seek(0)
                df = pd.read_csv(file, encoding='latin1', sep=';')
        else:
            df = pd.read_excel(file)
            
        # Processa
        df_processado = processar_planilha_api(df)

        # Mapeia para o JSON esperado pelo Frontend (index.html)
        results = []
        for index, row in df_processado.iterrows():
            results.append({
                "status": row['Status_Sistema'],
                "cep": row['CEP_Final'],
                "logradouro": row['Logradouro_Final'],
                "numero": row['Numero_Final'],
                "bairro": row['Bairro_Final'], 
                "cidade": row['Cidade_Final'], 
                "estado": row['UF_Final'], 
                "regiao": row['Regiao_Final'],
                "complemento": "", # Complemento é difícil extrair com precisão sem base, deixa vazio
                "apelido_local": row['A/C_Final'], 
                "fullAddress": row['Endereço Original']
            })

        return jsonify(results)
        
    except Exception as e:
        return jsonify({"error": f"Erro no servidor: {str(e)}"}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
