from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
import io
import re
import unicodedata

# ==========================================================
# FUNÇÕES CORE (Lógica de Normalização)
# ==========================================================

def remover_acentos(txt):
    if not isinstance(txt, str): return str(txt)
    return unicodedata.normalize('NFKD', txt).encode('ASCII', 'ignore').decode('ASCII')

def extrair_cep_bruto(texto):
    if not isinstance(texto, str): return None
    texto_limpo = texto.replace('"', '').replace("'", "").strip()
    # Padrão 1: CEP Formatado (00.000-000 ou 00000-000)
    match_formatado = re.search(r'\b\d{2}[. ]?\d{3}-\d{3}\b', texto_limpo)
    if match_formatado: return re.sub(r'\D', '', match_formatado.group(0))
    # Padrão 2: Palavra CEP seguida de numeros
    match_palavra = re.search(r'(?:CEP|C\.E\.P).{0,5}?(\d{8})', re.sub(r'[-.]', '', texto_limpo), re.IGNORECASE)
    if match_palavra: return match_palavra.group(1)
    # Padrão 3: 8 digitos soltos (mais arriscado, mas útil)
    match_8_digitos = re.search(r'(?<!\d)(\d{8})(?!\d)', texto_limpo)
    if match_8_digitos: return match_8_digitos.group(1)
    return None

def extrair_numero_inteligente(texto):
    if not isinstance(texto, str): return ""
    texto_upper = texto.upper().replace('"', '').strip()
    # Remove palavras que confundem (APTO, BLOCO, etc)
    lista_proibida = [
        r'APTO', r'APT', r'AP', r'APARTAMENTO', r'LOTE', r'LT', 
        r'CASA', r'CS', r'BLOCO', r'BL', r'SALA', r'SL', r'CJ', 
        r'LOJA', r'LJ', r'ANDAR', r'UNIDADE', r'FD', r'FDS', r'KM'
    ]
    regex_proibidos = r'\b(?:' + '|'.join(lista_proibida) + r')\.?\s*\d+[A-Z]?\b'
    texto_upper = re.sub(regex_proibidos, '', texto_upper, flags=re.IGNORECASE)
    
    # Remove CEPs para não confundir com número
    texto_upper = re.sub(r'\b\d{5}[-.]?\d{3}\b', '', texto_upper)
    texto_limpo_numeros = re.sub(r'\d{7,}', '', texto_upper) # Remove telefones/CNPJ
    
    def eh_valido(n): return len(n) <= 6
    
    # Casos S/N
    if re.search(r'\b(S/N|SN|S\.N|SEM N|S-N)\b', texto_limpo_numeros): return "S/N"
    
    # Padrões comuns de número (vírgula, n°, hifen)
    match_antes_virgula = re.search(r'\b(\d+)\s*,', texto_limpo_numeros)
    if match_antes_virgula and eh_valido(match_antes_virgula.group(1)): return match_antes_virgula.group(1)
    
    match_n = re.search(r'(?:nº|n|num)\.?\s*(\d+)', texto_limpo_numeros, re.IGNORECASE)
    if match_n and eh_valido(match_n.group(1)): return match_n.group(1)
    
    match_virgula = re.search(r',\s*(\d+)', texto_limpo_numeros)
    if match_virgula and eh_valido(match_virgula.group(1)): return match_virgula.group(1)
    
    match_fim = re.search(r'\s(\d+)$', texto_limpo_numeros)
    if match_fim and eh_valido(match_fim.group(1)): return match_fim.group(1)
    
    return ""

def gerar_status(cep, numero):
    status = []
    if not cep: status.append("FALTA CEP") 
    if not numero: status.append("FALTA NÚMERO")
    elif numero == "S/N": status.append("S/N (Manual)")
    
    if not status: return "OK"
    return " ".join(status)

# --- NOVA FUNÇÃO: Detecção Automática de Colunas ---
def detectar_colunas(df):
    """
    Tenta adivinhar quais colunas são o Endereço e o Destinatário
    baseado em palavras-chave comuns.
    """
    col_map = {'endereco': None, 'ac': None}
    
    # Normaliza nomes das colunas para facilitar busca (remove acentos, lower)
    cols_norm = {c: remover_acentos(str(c)).lower().strip() for c in df.columns}
    
    # Palavras-chave para Endereço (ordem de prioridade)
    keywords_end = ['endereco', 'logradouro', 'rua', 'address', 'local', 'entrega', 'destino']
    # Palavras-chave para Nome/A/C
    keywords_ac = ['a/c', 'aos cuidados', 'destinatario', 'nome', 'cliente', 'recebedor', 'contact', 'razao']

    # Busca Endereço
    for key in keywords_end:
        for original, norm in cols_norm.items():
            if key in norm:
                col_map['endereco'] = original
                break
        if col_map['endereco']: break
        
    # Busca A/C
    for key in keywords_ac:
        for original, norm in cols_norm.items():
            if key in norm:
                col_map['ac'] = original
                break
        if col_map['ac']: break
        
    return col_map

def processar_planilha(df):
    df = df.copy()
    
    # 1. Detecção Inteligente de Colunas
    mapa = detectar_colunas(df)
    
    col_end = mapa['endereco']
    col_ac = mapa['ac']
    
    # Se não achou, tenta pegar a primeira coluna de texto grande como endereço
    if not col_end:
        # Fallback: primeira coluna que não seja vazia
        col_end = df.columns[0]
        
    if not col_ac:
        # Fallback: cria uma coluna genérica se não achar nome
        col_ac = 'Destinatario_Generico'
        df[col_ac] = 'A/C'

    # Renomeia para padronizar interno
    df.rename(columns={col_end: 'Endereço Completo', col_ac: 'A/C'}, inplace=True)
    
    # Garante que as colunas existem e são string
    df['Endereço Completo'] = df['Endereço Completo'].astype(str).replace('nan', '')
    df['A/C'] = df['A/C'].astype(str).replace('nan', '')

    # 2. Processamento
    df['CEP_Final'] = df['Endereço Completo'].apply(extrair_cep_bruto)
    df['Numero_Final'] = df['Endereço Completo'].apply(extrair_numero_inteligente)
    
    # 3. Limpeza do Logradouro (Remove CEP e Numero do texto)
    def limpar_texto(row):
        txt = str(row['Endereço Completo']).replace('"', '').replace("'", "")
        cep = row['CEP_Final']
        num = row['Numero_Final']
        
        if cep:
            txt = re.sub(rf'{cep[:5]}.?{cep[5:]}', '', txt) 
            txt = re.sub(rf'{cep}', '', txt)
        if num and num != "S/N":
            txt = re.sub(rf'\b{num}\b', '', txt)
            
        txt = re.sub(r'\bCEP\b[:.]?', '', txt, flags=re.IGNORECASE)
        txt = re.sub(r'\s[-–]\s*$', '', txt)
        return txt.strip(' ,;-.')

    df['Logradouro_Final'] = df.apply(limpar_texto, axis=1)
    df['STATUS_SISTEMA'] = df.apply(lambda x: gerar_status(x['CEP_Final'], x['Numero_Final']), axis=1)
    
    return df

# ==========================================================
# FLASK API
# ==========================================================

app = Flask(__name__)
CORS(app) # Libera CORS para qualquer origem (mais fácil para dev/prod mistos)

@app.route('/normalizar', methods=['POST'])
def handle_normalizacao_api():
    if 'import-file' not in request.files and 'file' not in request.files:
        return jsonify({"error": "Nenhum arquivo enviado."}), 400
    
    file = request.files.get('file') or request.files.get('import-file')
    
    try:
        if file.filename.endswith('.csv'):
            try:
                df = pd.read_csv(file, encoding='utf-8', sep=';') # Tenta ponto-e-virgula primeiro (comum BR)
                if len(df.columns) < 2:
                    file.seek(0)
                    df = pd.read_csv(file, encoding='utf-8', sep=',')
            except:
                file.seek(0)
                df = pd.read_csv(file, encoding='latin1', sep=';')
        else:
            df = pd.read_excel(file)
            
        df = df.astype(str).replace('nan', '')
        
        # Processa com a nova inteligência de colunas
        df_processado = processar_planilha(df)

        # Formata retorno JSON para o Frontend
        results = []
        for index, row in df_processado.iterrows():
            results.append({
                "status": row['STATUS_SISTEMA'],
                "cep": row['CEP_Final'],
                "logradouro": row['Logradouro_Final'],
                "numero": row['Numero_Final'],
                "bairro": "", 
                "cidade": "", 
                "estado": "", 
                "complemento": "", 
                "apelido_local": row['A/C'], 
                "fullAddress": row['Endereço Completo']
            })

        return jsonify(results)
        
    except Exception as e:
        return jsonify({"error": f"Erro no servidor: {str(e)}"}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
