import pandas as pd
import re
import io
import json
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from typing import Optional

# --- CONFIGURAÇÃO DA API ---
app = FastAPI(title="ELO-Normalizador API", description="API do Robô Blindado 3.4 para normalização de endereços.")

# Permite que seu front-end (index.html) acesse essa API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Em produção, troque "*" pelo seu domínio (https://elo-brindes...)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- FUNÇÕES DE EXTRAÇÃO (O ROBÔ BLINDADO 3.4) ---
# (Cópia exata da lógica do app.py para garantir fidelidade)

def extrair_cep_bruto(texto):
    if not isinstance(texto, str): return None
    texto_limpo = texto.replace('"', '').replace("'", "").strip()
    
    # 1. Padrão Formatado
    match_formatado = re.search(r'\b\d{2}[. ]?\d{3}-\d{3}\b', texto_limpo)
    if match_formatado:
         return re.sub(r'\D', '', match_formatado.group(0))
    
    # 2. Palavra "CEP"
    match_palavra = re.search(r'(?:CEP|C\.E\.P).{0,5}?(\d{8})', re.sub(r'[-.]', '', texto_limpo), re.IGNORECASE)
    if match_palavra:
        return match_palavra.group(1)

    # 3. 8 dígitos SOLTOS
    match_8_digitos = re.search(r'(?<!\d)(\d{8})(?!\d)', texto_limpo)
    if match_8_digitos:
        return match_8_digitos.group(1)
        
    # 4. SALVA VIDAS (7 dígitos)
    match_7_digitos = re.search(r'(?<!\d)(\d{7})(?!\d)', texto_limpo)
    if match_7_digitos:
        return "0" + match_7_digitos.group(1)
        
    return None

def extrair_numero_inteligente(texto):
    if not isinstance(texto, str): return ""
    texto_upper = texto.upper().replace('"', '').strip()

    # VACINA ANTI-COMPLEMENTO
    lista_proibida = [
        r'APTO', r'APT', r'AP', r'APARTAMENTO', r'APART', r'LOTE', r'LT', r'LOT',
        r'CASA', r'CS', r'CN', r'BLOCO', r'BL', r'SALA', r'SL', r'CJ', r'CONJUNTO',
        r'LOJA', r'LJ', r'ANDAR', r'AND', r'UNIDADE', r'UNID', r'FRENTE', r'FD', 
        r'FUNDOS', r'FDS', r'QD', r'QUADRA', r'BOX', r'GARAGEM', r'KM'
    ]
    regex_proibidos = r'\b(?:' + '|'.join(lista_proibida) + r')\.?\s*\d+[A-Z]?\b'
    texto_upper = re.sub(regex_proibidos, '', texto_upper, flags=re.IGNORECASE)

    # TRAVA DE SEGURANÇA
    texto_upper = re.sub(r'\b\d{5}[-.]?\d{3}\b', '', texto_upper) # Remove CEPs
    texto_limpo_numeros = re.sub(r'\d{7,}', '', texto_upper) # Remove telefones/longos

    def eh_valido(n): return len(n) <= 6

    # BUSCAS PADRÃO
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
    if not cep: status.append("🔴 CEP?") 
    if not numero: status.append("⚠️ NÚMERO?")
    elif numero == "S/N": status.append("⚪ S/N")
    if not status: return "✅ OK"
    return " ".join(status)

def processar_dataframe(df, col_map):
    df = df.copy()
    # 1. Cria ID Sequencial
    df['ID_Personalizado'] = [f'ID_{i+1}' for i in range(len(df))]
    
    # 2. Mapeia colunas (Lida com colunas opcionais ou None)
    df['Nome_Final'] = df[col_map['nome']] if col_map.get('nome') else ""
    df['Cidade_Final'] = df[col_map['cidade']] if col_map.get('cidade') else ""
    df['UF_Final'] = df[col_map['uf']] if col_map.get('uf') else ""
    df['Regiao_Final'] = df[col_map['regiao']] if col_map.get('regiao') else ""
    df['Bairro_Final'] = df[col_map['bairro']] if col_map.get('bairro') else "" 
    
    col_endereco = col_map['endereco']
    
    # 3. Extrações
    df[col_endereco] = df[col_endereco].astype(str)
    df['CEP_Final'] = df[col_endereco].apply(extrair_cep_bruto)
    df['Numero_Final'] = df[col_endereco].apply(extrair_numero_inteligente)
    
    # 4. Limpeza Logradouro
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
    df['Complemento_Final'] = ""
    df['Aos_Cuidados_Final'] = ""
    df['STATUS_SISTEMA'] = df.apply(lambda x: gerar_status(x['CEP_Final'], x['Numero_Final']), axis=1)
    
    # Ordena colocando erros primeiro
    df = df.sort_values(by=['STATUS_SISTEMA'], ascending=False)
    
    return df

# --- ENDPOINTS DA API ---

@app.get("/")
def health_check():
    return {"status": "online", "robot": "Blindado 3.4"}

@app.post("/analisar_colunas")
async def analisar_arquivo(file: UploadFile = File(...)):
    """
    Recebe o arquivo e retorna as colunas encontradas + sugestões de mapeamento.
    Use isso para preencher os <select> no frontend.
    """
    if not file.filename.endswith('.xlsx'):
        raise HTTPException(status_code=400, detail="Apenas arquivos .xlsx são permitidos")
    
    try:
        contents = await file.read()
        df = pd.read_excel(io.BytesIO(contents))
        cols = [str(c) for c in df.columns] # Garante strings
        
        # Lógica de sugestão automática
        sugestoes = {
            "endereco": next((c for c in cols if any(x in c.lower() for x in ['endereço', 'endereco'])), cols[0] if cols else None),
            "nome": next((c for c in cols if any(x in c.lower() for x in ['nome', 'clube', 'loja'])), None),
            "cidade": next((c for c in cols if any(x in c.lower() for x in ['cidade', 'city'])), None),
            "uf": next((c for c in cols if any(x in c.lower() for x in ['uf', 'estado'])), None),
            "regiao": next((c for c in cols if any(x in c.lower() for x in ['regiao', 'região'])), None),
            "bairro": next((c for c in cols if any(x in c.lower() for x in ['bairro'])), None)
        }
        
        return {
            "colunas_disponiveis": cols,
            "sugestoes": sugestoes
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao ler arquivo: {str(e)}")

@app.post("/processar")
async def processar(
    tipo_saida: str = Form(...), # 'triagem' ou 'final'
    mapa: str = Form(...),       # JSON string ex: '{"endereco": "Col A", "nome": "Col B"}'
    file: UploadFile = File(...)
):
    """
    Processa o arquivo com base no mapa de colunas e retorna o Excel pronto para download.
    """
    try:
        # Parse do mapa (frontend envia JSON stringify)
        col_map = json.loads(mapa)
        
        if not col_map.get('endereco'):
            raise HTTPException(status_code=400, detail="A coluna de Endereço é obrigatória.")

        contents = await file.read()
        df = pd.read_excel(io.BytesIO(contents))
        df = df.astype(str).replace('nan', '') # Limpeza inicial igual ao app.py
        
        # O ROBÔ TRABALHA AQUI
        df_processado = processar_dataframe(df, col_map)
        
        # PREPARAÇÃO DO ARQUIVO FINAL
        output = io.BytesIO()
        
        if tipo_saida == 'triagem':
            # Exporta Triagem (com Status e ID, ordenado por erro)
            # Colunas para mostrar na triagem (replicando app.py)
            cols_to_show = [
                "STATUS_SISTEMA", "ID_Personalizado", "Nome_Final", "CEP_Final", 
                "Logradouro_Final", "Numero_Final", "Complemento_Final", 
                "Bairro_Final", "Cidade_Final", "UF_Final", "Regiao_Final", 
                "Aos_Cuidados_Final", col_map['endereco']
            ]
            # Filtra apenas colunas existentes para evitar erro
            cols_validas = [c for c in cols_to_show if c in df_processado.columns]
            
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                df_processado[cols_validas].to_excel(writer, index=False, sheet_name='Triagem')
                filename = "Triagem_Enderecos.xlsx"
                
        else:
            # Exporta Final (Reordenado por ID original, layout limpo)
            # Recria a ordem original baseada no número do ID
            df_final = df_processado.copy()
            df_final['__sort_id'] = df_final['ID_Personalizado'].apply(lambda x: int(x.split('_')[1]) if '_' in x else 0)
            df_final = df_final.sort_values('__sort_id')
            
            col_end_orig = col_map['endereco']
            
            # Seleciona e Renomeia (Igual ao app.py)
            cols_final = [
                "ID_Personalizado", "Nome_Final", "CEP_Final", "Logradouro_Final",
                "Numero_Final", "Complemento_Final", "Bairro_Final", "Cidade_Final",
                "UF_Final", "Regiao_Final", "Aos_Cuidados_Final", col_end_orig
            ]
            
            # Filtra caso alguma coluna opcional não exista
            cols_final = [c for c in cols_final if c in df_final.columns]
            
            df_final = df_final[cols_final]
            
            nomes_finais = [
                "ID", "Nome (Clube)", "CEP", "Logradouro", 
                "N°", "Complemento", "Bairro", "Cidade", "UF", "Região", 
                "Aos Cuidados", "Endereço Original"
            ]
            
            # Ajusta tamanho da lista de nomes se faltou alguma coluna
            if len(df_final.columns) == len(nomes_finais):
                df_final.columns = nomes_finais
            
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                df_final.to_excel(writer, index=False, sheet_name='Envio')
                filename = "Lote_Final_Normalizado.xlsx"

        output.seek(0)
        
        return StreamingResponse(
            output, 
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )

    except Exception as e:
        print(f"Erro: {e}") # Log no console do servidor
        raise HTTPException(status_code=500, detail=f"Erro interno no processamento: {str(e)}")
