import pandas as pd
import re
import io
import json
import logging
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

# --- LOGGING PARA DEBUG ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- CONFIGURAÇÃO DA API ---
app = FastAPI(title="ELO-Normalizador API", description="API do Robô Blindado 3.6 - Com Mapeamento")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- FUNÇÕES DE EXTRAÇÃO (REGEX) ---

def extrair_cep_bruto(texto):
    if not isinstance(texto, str): return None
    texto_limpo = texto.replace('"', '').replace("'", "").strip()
    
    match_formatado = re.search(r'\b\d{2}[. ]?\d{3}-\d{3}\b', texto_limpo)
    if match_formatado:
         return re.sub(r'\D', '', match_formatado.group(0))
    
    match_palavra = re.search(r'(?:CEP|C\.E\.P).{0,5}?(\d{8})', re.sub(r'[-.]', '', texto_limpo), re.IGNORECASE)
    if match_palavra:
        return match_palavra.group(1)

    match_8_digitos = re.search(r'(?<!\d)(\d{8})(?!\d)', texto_limpo)
    if match_8_digitos:
        return match_8_digitos.group(1)
        
    match_7_digitos = re.search(r'(?<!\d)(\d{7})(?!\d)', texto_limpo)
    if match_7_digitos:
        return "0" + match_7_digitos.group(1)
        
    return None

def extrair_numero_inteligente(texto):
    if not isinstance(texto, str): return ""
    texto_upper = texto.upper().replace('"', '').strip()

    lista_proibida = [
        r'APTO', r'APT', r'AP', r'APARTAMENTO', r'APART', r'LOTE', r'LT', r'LOT',
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
    status = []
    if not cep: status.append("🔴 CEP?") 
    if not numero: status.append("⚠️ NÚMERO?")
    elif numero == "S/N": status.append("⚪ S/N")
    if not status: return "✅ OK"
    return " ".join(status)

def processar_dataframe(df, col_map):
    df = df.copy()
    df['ID_Personalizado'] = [f'ID_{i+1}' for i in range(len(df))]
    
    # Mapeamento seguro das colunas opcionais
    # Verifica se a chave existe no mapa, se tem valor, e se a coluna existe no DF
    df['Nome_Final'] = df[col_map['nome']] if col_map.get('nome') and col_map['nome'] in df.columns else ""
    df['Cidade_Final'] = df[col_map['cidade']] if col_map.get('cidade') and col_map['cidade'] in df.columns else ""
    df['UF_Final'] = df[col_map['uf']] if col_map.get('uf') and col_map['uf'] in df.columns else ""
    df['Regiao_Final'] = df[col_map['regiao']] if col_map.get('regiao') and col_map['regiao'] in df.columns else ""
    df['Bairro_Final'] = df[col_map['bairro']] if col_map.get('bairro') and col_map['bairro'] in df.columns else "" 
    
    # Endereço é obrigatório, mas protegemos contra falhas
    col_endereco = col_map.get('endereco')
    if not col_endereco or col_endereco not in df.columns:
        # Se não achou a coluna mapeada, tenta a primeira como fallback
        col_endereco = df.columns[0]
    
    df[col_endereco] = df[col_endereco].astype(str)
    
    # Aplica as IAs de extração
    df['CEP_Final'] = df[col_endereco].apply(extrair_cep_bruto)
    df['Numero_Final'] = df[col_endereco].apply(extrair_numero_inteligente)
    
    # Função interna para limpar o texto do logradouro removendo o que já foi extraído
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
    
    df = df.sort_values(by=['STATUS_SISTEMA'], ascending=False)
    return df

# --- ENDPOINTS ---

@app.get("/")
def health_check():
    logger.info("Health check chamado!")
    return {"status": "online", "robot": "Blindado 3.6"}

# ROTA 1: Ler o arquivo e dizer quais colunas existem (Popula o Modal)
@app.post("/analisar_colunas")
async def analisar_arquivo(file: UploadFile = File(...)):
    if not file.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(status_code=400, detail="Apenas arquivos Excel (.xlsx, .xls) são permitidos")
    
    try:
        contents = await file.read()
        df = pd.read_excel(io.BytesIO(contents))
        cols = [str(c) for c in df.columns]
        
        # Inteligência para sugerir o mapeamento inicial
        sugestoes = {
            "endereco": next((c for c in cols if any(x in c.lower() for x in ['endereço', 'endereco', 'rua', 'logradouro'])), cols[0] if cols else None),
            "nome": next((c for c in cols if any(x in c.lower() for x in ['nome', 'clube', 'loja', 'cliente', 'destinatario'])), None),
            "cidade": next((c for c in cols if any(x in c.lower() for x in ['cidade', 'city', 'municipio'])), None),
            "uf": next((c for c in cols if any(x in c.lower() for x in ['uf', 'estado'])), None),
            "regiao": next((c for c in cols if any(x in c.lower() for x in ['regiao', 'região'])), None),
            "bairro": next((c for c in cols if any(x in c.lower() for x in ['bairro'])), None)
        }
        
        return {"colunas_disponiveis": cols, "sugestoes": sugestoes}
    except Exception as e:
        logger.error(f"Erro ao analisar: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao ler arquivo: {str(e)}")

# ROTA 2: Recebe o arquivo + O MAPA ESCOLHIDO e devolve o JSON para a tela
@app.post("/preview_importacao")
async def preview_importacao(
    mapa: str = Form(None), 
    file: UploadFile = File(...)
):
    try:
        contents = await file.read()
        df = pd.read_excel(io.BytesIO(contents))
        df = df.astype(str).replace('nan', '')
        cols = [str(c) for c in df.columns]

        # SE O USUÁRIO MANDOU O MAPA PELO MODAL, USA ELE.
        if mapa:
            col_map = json.loads(mapa)
            mapa_final = {
                "endereco": col_map.get('endereco', ''),
                "nome": col_map.get('nome', ''),
                "cidade": col_map.get('cidade', ''),
                "uf": col_map.get('uf', ''),
                "bairro": col_map.get('bairro', ''),
                "regiao": col_map.get('regiao', '')
            }
        else:
            # Fallback: Tenta adivinhar se não mandou mapa (segurança)
            mapa_final = {
                "endereco": next((c for c in cols if any(x in c.lower() for x in ['endereço', 'endereco', 'logradouro', 'rua'])), cols[0] if cols else None),
                "nome": next((c for c in cols if any(x in c.lower() for x in ['nome', 'clube', 'loja'])), ""),
                "cidade": next((c for c in cols if any(x in c.lower() for x in ['cidade', 'city'])), ""),
                "uf": next((c for c in cols if any(x in c.lower() for x in ['uf', 'estado'])), ""),
                "bairro": next((c for c in cols if any(x in c.lower() for x in ['bairro'])), ""),
                "regiao": next((c for c in cols if any(x in c.lower() for x in ['regiao'])), "")
            }

        # Processa os dados
        df_processado = processar_dataframe(df, mapa_final)

        resultado = []
        for _, row in df_processado.iterrows():
            apelido = row['Nome_Final'] if row['Nome_Final'] else "Cliente"
            
            # Pega o endereço original
            col_orig = mapa_final.get('endereco')
            endereco_full = row[col_orig] if col_orig and col_orig in row else ""

            resultado.append({
                "status": row['STATUS_SISTEMA'],
                "apelido_local": apelido,
                "cep": row['CEP_Final'],
                "logradouro": row['Logradouro_Final'],
                "numero": row['Numero_Final'],
                "bairro": row['Bairro_Final'],
                "cidade": row['Cidade_Final'],
                "estado": row['UF_Final'],
                "complemento": row['Complemento_Final'],
                "fullAddress": endereco_full
            })

        return resultado

    except Exception as e:
        logger.error(f"Erro no preview: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao processar preview: {str(e)}")

# ROTA 3: Processamento antigo para download de Excel (Mantido para compatibilidade)
@app.post("/processar")
async def processar(
    tipo_saida: str = Form(...),
    mapa: str = Form(...),
    file: UploadFile = File(...)
):
    try:
        col_map = json.loads(mapa)
        if not col_map.get('endereco'):
            raise HTTPException(status_code=400, detail="A coluna de Endereço é obrigatória.")

        contents = await file.read()
        df = pd.read_excel(io.BytesIO(contents))
        df = df.astype(str).replace('nan', '')
        
        df_processado = processar_dataframe(df, col_map)
        output = io.BytesIO()
        
        filename = "Processado.xlsx"

        if tipo_saida == 'triagem':
            cols_to_show = [
                "STATUS_SISTEMA", "ID_Personalizado", "Nome_Final", "CEP_Final", 
                "Logradouro_Final", "Numero_Final", "Complemento_Final", 
                "Bairro_Final", "Cidade_Final", "UF_Final", "Regiao_Final", 
                "Aos_Cuidados_Final", col_map['endereco']
            ]
            cols_validas = [c for c in cols_to_show if c in df_processado.columns]
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                df_processado[cols_validas].to_excel(writer, index=False, sheet_name='Triagem')
                filename = "Triagem_Enderecos.xlsx"
        else:
            df_final = df_processado.copy()
            df_final['__sort_id'] = df_final['ID_Personalizado'].apply(lambda x: int(x.split('_')[1]) if '_' in x else 0)
            df_final = df_final.sort_values('__sort_id')
            
            col_end_orig = col_map['endereco']
            cols_final = [
                "ID_Personalizado", "Nome_Final", "CEP_Final", "Logradouro_Final",
                "Numero_Final", "Complemento_Final", "Bairro_Final", "Cidade_Final",
                "UF_Final", "Regiao_Final", "Aos_Cuidados_Final", col_end_orig
            ]
            cols_final = [c for c in cols_final if c in df_final.columns]
            df_final = df_final[cols_final]
            
            nomes_finais = [
                "ID", "Nome (Clube)", "CEP", "Logradouro", 
                "N°", "Complemento", "Bairro", "Cidade", "UF", "Região", 
                "Aos Cuidados", "Endereço Original"
            ]
            
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
        logger.error(f"Erro ao processar: {e}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")
