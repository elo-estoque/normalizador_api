import pandas as pd
import re
import io
import json
import logging
import os
import httpx
import uvicorn
from fastapi import FastAPI, UploadFile, File, Form, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List, Dict, Any

# --- LOGGING ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- CONFIGURAÇÃO DA API ---
app = FastAPI(
    title="ELO-API",
    description="Backend Seguro Elo Brindes para gestão de pedidos e normalização de endereços.",
    version="1.4.0"
)

# --- CORREÇÃO DO CORS ---
origins = [
    "https://entregas.elobrindes.com.br",
    "https://www.entregas.elobrindes.com.br",
    "http://localhost:8000",
    "*" 
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS", "PATCH", "DELETE"],
    allow_headers=["*"],
)

# --- VARIÁVEIS DE AMBIENTE ---
# URL corrigida sem www conforme sua indicação
DIRECTUS_URL = os.environ.get("DIRECTUS_URL", "https://admin-entregas.elobrindes.com.br")
DIRECTUS_ADMIN_TOKEN = os.environ.get("DIRECTUS_ADMIN_TOKEN")

if not DIRECTUS_ADMIN_TOKEN:
    logger.warning("⚠️ ALERTA: Token de Admin não encontrado! A API de estoque falhará.")

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

async def restaurar_estoque(item: PedidoItem, client: httpx.AsyncClient, headers: Dict[str, str]):
    try:
        resp_pai = await client.get(f"{DIRECTUS_URL}/items/estoque_cliente/{item.estoque_pai_id}", headers=headers)
        if resp_pai.status_code == 200:
            qtd_atual_pai = int(resp_pai.json()['data'].get('quantidade_disponivel', 0))
            await client.patch(
                f"{DIRECTUS_URL}/items/estoque_cliente/{item.estoque_pai_id}",
                headers=headers,
                json={"quantidade_disponivel": qtd_atual_pai + item.quantidade}
            )
        
        if item.lote_estoque_id:
            resp_lote = await client.get(f"{DIRECTUS_URL}/items/estoque_lotes/{item.lote_estoque_id}", headers=headers)
            if resp_lote.status_code == 200:
                qtd_atual_lote = int(resp_lote.json()['data'].get('quantidade', 0))
                await client.patch(
                    f"{DIRECTUS_URL}/items/estoque_lotes/{item.lote_estoque_id}",
                    headers=headers,
                    json={"quantidade": qtd_atual_lote + item.quantidade}
                )
        logger.info(f"Rollback executado com sucesso para o item: {item.produto_id}")
    except Exception as e:
        logger.error(f"⚠️ ERRO CRÍTICO NO ROLLBACK: Falha ao restaurar estoque para {item.produto_id}. Erro: {e}")

async def baixar_estoque_seguro(item: PedidoItem, client: httpx.AsyncClient, headers: Dict[str, str]):
    resp_pai = await client.get(f"{DIRECTUS_URL}/items/estoque_cliente/{item.estoque_pai_id}", headers=headers)
    
    if resp_pai.status_code != 200:
        logger.error(f"Erro Directus Pai ({item.estoque_pai_id}): {resp_pai.text}")
        raise HTTPException(status_code=404, detail=f"Estoque Pai {item.estoque_pai_id} não encontrado.")
    
    dados_pai = resp_pai.json().get('data', {})
    qtd_atual_pai = int(dados_pai.get('quantidade_disponivel', 0))
    
    if qtd_atual_pai < item.quantidade:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Saldo insuficiente (Geral) para {item.produto_id}. Disp: {qtd_atual_pai}")

    if item.lote_estoque_id:
        resp_lote = await client.get(f"{DIRECTUS_URL}/items/estoque_lotes/{item.lote_estoque_id}", headers=headers)
        if resp_lote.status_code == 200:
            dados_lote = resp_lote.json().get('data', {})
            qtd_atual_lote = int(dados_lote.get('quantidade', 0))
            
            if qtd_atual_lote < item.quantidade:
                 raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Saldo insuficiente no Lote {item.lote_estoque_id}. Disp: {qtd_atual_lote}")
            
            resp_patch_lote = await client.patch(
                f"{DIRECTUS_URL}/items/estoque_lotes/{item.lote_estoque_id}",
                headers=headers,
                json={"quantidade": qtd_atual_lote - item.quantidade}
            )
            if resp_patch_lote.status_code not in [200, 204]:
                logger.error(f"Falha ao dar patch no lote {item.lote_estoque_id}. Erro: {resp_patch_lote.text}")
                raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Falha ao atualizar o estoque de lote.")

    resp_patch_pai = await client.patch(
        f"{DIRECTUS_URL}/items/estoque_cliente/{item.estoque_pai_id}",
        headers=headers,
        json={"quantidade_disponivel": qtd_atual_pai - item.quantidade}
    )
    if resp_patch_pai.status_code not in [200, 204]:
        logger.error(f"Falha ao dar patch no estoque pai {item.estoque_pai_id}. Erro: {resp_patch_pai.text}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Falha ao atualizar o estoque principal.")

# --- ENDPOINTS ---

@app.get("/")
def health_check():
    return {"status": "online", "system": "Elo Brindes API", "version": "1.4.0"}

@app.post("/api/finalizar_envio", status_code=status.HTTP_201_CREATED)
async def finalizar_envio(pedido: PedidoRequest):
    if not DIRECTUS_ADMIN_TOKEN:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Token de Admin não configurado no servidor.")

    headers = {
        "Authorization": f"Bearer {DIRECTUS_ADMIN_TOKEN}",
        "Content-Type": "application/json"
    }
    
    itens_processados_com_sucesso = []
    novo_lote_id = None

    async with httpx.AsyncClient() as client:
        try:
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
                raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao criar registro do lote.")
            
            novo_lote_id = resp_lote.json().get('data', {}).get('id')
            
            for item in pedido.itens:
                await baixar_estoque_seguro(item, client, headers)
                itens_processados_com_sucesso.append(item)
                
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
                resp_solic = await client.post(f"{DIRECTUS_URL}/items/solicitacoes", headers=headers, json=solic_payload)

                if resp_solic.status_code not in [200, 201]:
                    logger.error(f"Erro ao criar solicitação para produto {item.produto_id}: {resp_solic.text}")
                    raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Falha ao criar solicitação para o produto {item.produto_id}.")
            
            return {"status": "success", "lote_id": novo_lote_id, "message": "Pedido e baixas de estoque finalizados com sucesso."}

        except HTTPException as he:
            if itens_processados_com_sucesso:
                logger.warning(f"Executando rollback após falha de HTTP: {he.detail}")
                for item in itens_processados_com_sucesso:
                    await restaurar_estoque(item, client, headers)
            raise he
        
        except Exception as e:
            if itens_processados_com_sucesso:
                logger.error(f"Erro crítico inesperado. Executando rollback. Erro: {e}")
                for item in itens_processados_com_sucesso:
                    await restaurar_estoque(item, client, headers)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Erro interno no processamento do pedido: {str(e)}")

# --- LEGADO: NORMALIZADOR ---
def extrair_cep_bruto(texto: Any) -> Optional[str]:
    if not isinstance(texto, str): return None
    texto = texto.upper().replace('"', '').replace("'", "").strip()
    match = re.search(r'\b\d{2}[. ]?\d{3}-\d{3}\b', texto)
    if match: return re.sub(r'\D', '', match.group(0))
    match8 = re.search(r'(?<!\d)(\d{8})(?!\d)', texto)
    if match8: return match8.group(1)
    return None

def extrair_numero_inteligente(texto: Any) -> str:
    if not isinstance(texto, str): return ""
    texto = texto.upper().replace('"', '').strip()
    texto_limpo = re.sub(r'\b(APTO|BLOCO|SALA|CJ|KM|LT|QD)\.?\s*\d+[A-Z]?\b', '', texto, flags=re.IGNORECASE)
    texto_limpo = re.sub(r'\d{5}[-.]?\d{3}', '', texto_limpo)
    if re.search(r'\b(S/N|SN|SEM N|SEM NUMERO)\b', texto_limpo): return "S/N"
    match = re.search(r',\s*(\d+)\b', texto_limpo)
    if match: return match.group(1)
    match_fim = re.search(r'\s(\d+)\s*$', texto_limpo.strip())
    if match_fim: return match_fim.group(1)
    return ""

def gerar_status(cep: Optional[str], numero: str) -> str:
    status_list = []
    if not cep: status_list.append("🔴 CEP?") 
    if not numero: status_list.append("⚠️ NÚMERO?")
    elif numero == "S/N": status_list.append("⚪ S/N")
    return " ".join(status_list) if status_list else "✅ OK"

def processar_dataframe(df: pd.DataFrame, col_map: Dict[str, str]) -> pd.DataFrame:
    df_p = df.copy()
    col_end = col_map.get('endereco')
    if not col_end or col_end not in df_p.columns:
         raise ValueError(f"Coluna de endereço '{col_end}' não encontrada no arquivo.")
    df_p[col_end] = df_p[col_end].astype(str).fillna("")
    df_p['CEP_Final'] = df_p[col_end].apply(extrair_cep_bruto)
    df_p['Numero_Final'] = df_p[col_end].apply(extrair_numero_inteligente)
    df_p['Nome_Final'] = df_p.get(col_map.get('nome', ''), "").astype(str).fillna("")
    df_p['Cidade_Final'] = df_p.get(col_map.get('cidade', ''), "N/A").astype(str).fillna("N/A")
    df_p['UF_Final'] = df_p.get(col_map.get('uf', ''), "N/A").astype(str).fillna("N/A")
    df_p['Bairro_Final'] = df_p.get(col_map.get('bairro', ''), "N/A").astype(str).fillna("N/A")
    def limpar_logradouro(row):
        t = str(row[col_end])
        if row['CEP_Final']: t = t.replace(row['CEP_Final'], '').replace(re.sub(r'\D', '', row['CEP_Final']), '')
        if row['Numero_Final'] and row['Numero_Final'] != "S/N": t = re.sub(r'(,\s*)?\b' + re.escape(row['Numero_Final']) + r'\b', '', t, flags=re.IGNORECASE)
        t = re.sub(r'\b(APTO|BLOCO|SALA|CJ|KM|LT|QD)\.?\s*\d+[A-Z]?\b', '', t, flags=re.IGNORECASE)
        return t.strip(' ,;-.')
    df_p['Logradouro_Final'] = df_p.apply(limpar_logradouro, axis=1)
    df_p['STATUS_SISTEMA'] = df_p.apply(lambda x: gerar_status(x['CEP_Final'], x['Numero_Final']), axis=1)
    return df_p

@app.post("/analisar_colunas")
async def analisar_arquivo(file: UploadFile = File(...)):
    try:
        content = await file.read()
        with io.BytesIO(content) as buffer:
            df = pd.read_excel(buffer)
        cols = [str(x) for x in df.columns]
        sugestoes = {
            "endereco": next((c for c in cols if any(x in c.lower() for x in ['endereço', 'endereco', 'rua', 'logradouro'])), None),
            "nome": next((c for c in cols if any(x in c.lower() for x in ['nome', 'destinatario', 'cliente'])), None),
            "cidade": next((c for c in cols if any(x in c.lower() for x in ['cidade', 'municipio'])), None),
            "uf": next((c for c in cols if any(x in c.lower() for x in ['uf', 'estado', 'st'])), None),
            "bairro": next((c for c in cols if any(x in c.lower() for x in ['bairro'])), None),
        }
        return {"colunas_disponiveis": cols, "sugestoes": sugestoes}
    except Exception as e:
        logger.error(f"Erro ao analisar colunas: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Erro ao analisar o arquivo: {str(e)}")

@app.post("/preview_importacao")
async def preview_importacao(mapa: str = Form(...), file: UploadFile = File(...)):
    try:
        content = await file.read()
        with io.BytesIO(content) as buffer:
            df = pd.read_excel(buffer)
        m = json.loads(mapa)
        if 'endereco' not in m or not m['endereco']:
             raise ValueError("A coluna de endereço deve ser mapeada.")
        df_p = processar_dataframe(df, m)
        coluna_endereco_original = m['endereco']
        res = []
        for _, row in df_p.head(50).iterrows():
            res.append({
                "status": row['STATUS_SISTEMA'],
                "apelido_local": row['Nome_Final'],
                "cep": row['CEP_Final'],
                "logradouro": row['Logradouro_Final'],
                "numero": row['Numero_Final'],
                "bairro": row['Bairro_Final'],
                "cidade": row['Cidade_Final'],
                "estado": row['UF_Final'],
                "fullAddress": row[coluna_endereco_original]
            })
        return res
    except ValueError as ve:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve))
    except Exception as e:
        logger.error(f"Erro ao gerar preview de importação: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Erro ao processar a importação: {str(e)}")

# INICIALIZAÇÃO PROGRAMÁTICA (Necessária se for rodar via python main.py no Docker)
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
