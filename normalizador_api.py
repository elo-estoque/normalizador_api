import pandas as pd
import re
import io
import json
import logging
import os
import httpx
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional, List

# --- LOGGING ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- CONFIGURAÇÃO DA API ---
app = FastAPI(title="ELO-API", description="Normalizador + Gerenciador de Estoque Seguro")

# --- CONFIGURAÇÃO DE CORS (A CORREÇÃO DO ERRO VERMELHO) ---
# Isso diz ao navegador que seu site pode falar com essa API
origins = [
    "https://entregas.elobrindes.com.br",     # Seu site oficial
    "https://www.entregas.elobrindes.com.br", # Variação com www
    "https://admin-entregas.elobrindes.com.br", # O próprio Directus (as vezes necessário)
    "http://localhost:8000",                  # Para seus testes locais
    "http://127.0.0.1:8000",
    "*"                                       # Fallback para garantir
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS", "PATCH", "DELETE"],
    allow_headers=["*"],
)

# --- VARIÁVEIS DE AMBIENTE (Lê do seu arquivo .env ou do servidor) ---
# Certifique-se que no seu servidor essas variáveis existem!
DIRECTUS_URL = os.environ.get("DIRECTUS_URL", "https://admin-entregas.elobrindes.com.br")
# Você precisa criar uma variável chamada DIRECTUS_ADMIN_TOKEN no seu servidor
# ou usar a SECRET se ela for um token estático.
DIRECTUS_ADMIN_TOKEN = os.environ.get("DIRECTUS_ADMIN_TOKEN") 

# Validação de segurança ao iniciar
if not DIRECTUS_ADMIN_TOKEN:
    logger.warning("⚠️ ALERTA: DIRECTUS_ADMIN_TOKEN não encontrado nas variáveis de ambiente! A API de estoque falhará.")

# --- MODELS ---
class PedidoItem(BaseModel):
    produto_id: int
    quantidade: int
    endereco_id: int
    estoque_pai_id: str
    lote_estoque_id: Optional[int] = None
    lote_descricao: Optional[str] = None

class PedidoRequest(BaseModel):
    organization_id: str
    user_id: str
    data_postagem: str
    itens: List[PedidoItem]

# --- LÓGICA DE ESTOQUE (BACKEND SEGURO) ---
async def baixar_estoque_seguro(item: PedidoItem, client: httpx.AsyncClient):
    headers = {
        "Authorization": f"Bearer {DIRECTUS_ADMIN_TOKEN}",
        "Content-Type": "application/json"
    }
    
    # 1. Busca Estoque Pai
    resp_pai = await client.get(f"{DIRECTUS_URL}/items/estoque_cliente/{item.estoque_pai_id}", headers=headers)
    
    if resp_pai.status_code != 200:
        logger.error(f"Erro Directus Pai: {resp_pai.text}")
        raise HTTPException(status_code=404, detail=f"Estoque {item.estoque_pai_id} não encontrado.")
    
    dados_pai = resp_pai.json()['data']
    qtd_atual_pai = int(dados_pai.get('quantidade_disponivel', 0))
    
    if qtd_atual_pai < item.quantidade:
        raise HTTPException(status_code=400, detail=f"Saldo insuficiente (Geral). Disp: {qtd_atual_pai}")

    # 2. Busca e Baixa Estoque Filho (Lote) se houver
    if item.lote_estoque_id:
        resp_lote = await client.get(f"{DIRECTUS_URL}/items/estoque_lotes/{item.lote_estoque_id}", headers=headers)
        if resp_lote.status_code == 200:
            dados_lote = resp_lote.json()['data']
            qtd_atual_lote = int(dados_lote.get('quantidade', 0))
            
            if qtd_atual_lote < item.quantidade:
                 raise HTTPException(status_code=400, detail=f"Saldo insuficiente no Lote. Disp: {qtd_atual_lote}")
            
            await client.patch(
                f"{DIRECTUS_URL}/items/estoque_lotes/{item.lote_estoque_id}",
                headers=headers,
                json={"quantidade": qtd_atual_lote - item.quantidade}
            )

    # 3. Baixa Estoque Pai
    await client.patch(
        f"{DIRECTUS_URL}/items/estoque_cliente/{item.estoque_pai_id}",
        headers=headers,
        json={"quantidade_disponivel": qtd_atual_pai - item.quantidade}
    )

# --- ENDPOINTS ---

@app.get("/")
def health_check():
    return {"status": "online", "system": "Elo Brindes API"}

@app.post("/api/finalizar_envio")
async def finalizar_envio(pedido: PedidoRequest):
    if not DIRECTUS_ADMIN_TOKEN:
        raise HTTPException(status_code=500, detail="Configuração de Token inválida no servidor.")

    headers = {
        "Authorization": f"Bearer {DIRECTUS_ADMIN_TOKEN}",
        "Content-Type": "application/json"
    }

    async with httpx.AsyncClient() as client:
        try:
            # 1. Cria Lote
            nome_lote = f"Envio Portal - {len(pedido.itens)} itens"
            lote_payload = {
                "nome_lote": nome_lote,
                "status": "pendente", 
                "quantidade_total": sum(i.quantidade for i in pedido.itens),
                "organization_id": pedido.organization_id,
                "user_created": pedido.user_id,
                "data_criacao": pedido.data_postagem
            }
            
            resp_lote = await client.post(f"{DIRECTUS_URL}/items/lotes_envio", headers=headers, json=lote_payload)
            if resp_lote.status_code not in [200, 201]:
                logger.error(f"Erro Directus Lote: {resp_lote.text}")
                raise HTTPException(status_code=500, detail="Erro ao criar registro do lote.")
            
            novo_lote_id = resp_lote.json()['data']['id']
            
            # 2. Processa Itens
            for item in pedido.itens:
                await baixar_estoque_seguro(item, client)
                
                obs = f"[REF_LOTE:{item.estoque_pai_id}|{item.lote_estoque_id or 0}]"
                if item.lote_descricao: obs += f" [{item.lote_descricao}]"

                solic_payload = {
                    "tipo": "SOLICITACAO_ENVIO",
                    "status": "pendente",
                    "lote_id": novo_lote_id,
                    "organization_id": pedido.organization_id,
                    "produto_id": item.produto_id,
                    "quantidade": item.quantidade,
                    "endereco_id": item.endereco_id,
                    "user_created": pedido.user_id,
                    "observacoes": f"{obs} Saída via API. Postagem: {pedido.data_postagem}"
                }
                await client.post(f"{DIRECTUS_URL}/items/solicitacoes", headers=headers, json=solic_payload)
            
            return {"status": "success", "lote_id": novo_lote_id}

        except HTTPException as he:
            raise he
        except Exception as e:
            logger.error(f"Erro crítico: {e}")
            raise HTTPException(status_code=500, detail=str(e))

# --- MODULO DE IMPORTAÇÃO (NORMALIZADOR) MANTIDO ---
def extrair_cep_bruto(texto):
    if not isinstance(texto, str): return None
    texto = texto.replace('"', '').replace("'", "").strip()
    match = re.search(r'\b\d{2}[. ]?\d{3}-\d{3}\b', texto)
    if match: return re.sub(r'\D', '', match.group(0))
    match8 = re.search(r'(?<!\d)(\d{8})(?!\d)', texto)
    if match8: return match8.group(1)
    return None

def extrair_numero_inteligente(texto):
    if not isinstance(texto, str): return ""
    texto = texto.upper().replace('"', '').strip()
    texto = re.sub(r'\b(APTO|BLOCO|SALA|CJ|KM)\.?\s*\d+[A-Z]?\b', '', texto, flags=re.IGNORECASE)
    texto = re.sub(r'\d{5}[-.]?\d{3}', '', texto)
    if re.search(r'\b(S/N|SN|SEM N)\b', texto): return "S/N"
    match = re.search(r',\s*(\d+)', texto)
    if match: return match.group(1)
    match_fim = re.search(r'\s(\d+)$', texto)
    if match_fim: return match_fim.group(1)
    return ""

def gerar_status(cep, numero):
    status = []
    if not cep: status.append("🔴 CEP?") 
    if not numero: status.append("⚠️ NÚMERO?")
    elif numero == "S/N": status.append("⚪ S/N")
    return " ".join(status) if status else "✅ OK"

def processar_dataframe(df, col_map):
    df = df.copy()
    col_end = col_map.get('endereco', df.columns[0])
    df[col_end] = df[col_end].astype(str)
    df['CEP_Final'] = df[col_end].apply(extrair_cep_bruto)
    df['Numero_Final'] = df[col_end].apply(extrair_numero_inteligente)
    df['Nome_Final'] = df[col_map['nome']] if col_map.get('nome') in df.columns else ""
    df['Cidade_Final'] = df[col_map['cidade']] if col_map.get('cidade') in df.columns else "N/A"
    df['UF_Final'] = df[col_map['uf']] if col_map.get('uf') in df.columns else "N/A"
    df['Bairro_Final'] = df[col_map['bairro']] if col_map.get('bairro') in df.columns else "N/A"
    
    def limpar(row):
        t = str(row[col_end])
        if row['CEP_Final']: t = t.replace(row['CEP_Final'], '')
        if row['Numero_Final'] and row['Numero_Final'] != "S/N": t = t.replace(row['Numero_Final'], '')
        return t.strip(' ,;-.')
    
    df['Logradouro_Final'] = df.apply(limpar, axis=1)
    df['STATUS_SISTEMA'] = df.apply(lambda x: gerar_status(x['CEP_Final'], x['Numero_Final']), axis=1)
    return df

@app.post("/analisar_colunas")
async def analisar_arquivo(file: UploadFile = File(...)):
    try:
        c = await file.read()
        df = pd.read_excel(io.BytesIO(c))
        cols = [str(x) for x in df.columns]
        sugestoes = {
            "endereco": next((c for c in cols if any(x in c.lower() for x in ['endereço', 'endereco', 'rua'])), None),
            "nome": next((c for c in cols if any(x in c.lower() for x in ['nome', 'destinatario'])), None),
            "cidade": next((c for c in cols if any(x in c.lower() for x in ['cidade', 'municipio'])), None),
            "uf": next((c for c in cols if any(x in c.lower() for x in ['uf', 'estado'])), None),
        }
        return {"colunas_disponiveis": cols, "sugestoes": sugestoes}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/preview_importacao")
async def preview_importacao(mapa: str = Form(...), file: UploadFile = File(...)):
    try:
        c = await file.read()
        df = pd.read_excel(io.BytesIO(c))
        m = json.loads(mapa)
        df_p = processar_dataframe(df, m)
        res = []
        for _, row in df_p.iterrows():
            res.append({
                "status": row['STATUS_SISTEMA'],
                "apelido_local": row['Nome_Final'],
                "cep": row['CEP_Final'],
                "logradouro": row['Logradouro_Final'],
                "numero": row['Numero_Final'],
                "bairro": row['Bairro_Final'],
                "cidade": row['Cidade_Final'],
                "estado": row['UF_Final'],
                "fullAddress": row[m.get('endereco', df.columns[0])]
            })
        return res
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
