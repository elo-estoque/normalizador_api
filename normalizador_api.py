import pandas as pd
import re
import io
import json
import logging
import os
import httpx # Necessário para falar com o Directus de forma assíncrona
from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Header, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional, List

# --- CONFIGURAÇÃO ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="ELO-API", description="Normalizador + Gerenciador de Estoque Seguro")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- CONFIGURAÇÕES DO DIRECTUS (Coloque suas chaves aqui ou em variaveis de ambiente) ---
DIRECTUS_URL = os.environ.get("DIRECTUS_URL", "https://admin-entregas.elobrindes.com.br")
# TOKEN DE ADMIN (Service Role) - Esse token TEM PERMISSÃO para editar estoque
DIRECTUS_ADMIN_TOKEN = os.environ.get("DIRECTUS_ADMIN_TOKEN", "SEU_TOKEN_ADMIN_AQUI")

# --- MODELS PARA O PEDIDO ---
class PedidoItem(BaseModel):
    produto_id: int
    quantidade: int
    endereco_id: int
    estoque_pai_id: str # UUID do estoque_cliente
    lote_estoque_id: Optional[int] = None # ID do estoque_lotes (se houver)
    lote_descricao: Optional[str] = None

class PedidoRequest(BaseModel):
    itens: List[PedidoItem]
    organization_id: str
    data_postagem: str
    user_id: str # ID do usuário que pediu

# --- FUNÇÕES AUXILIARES DE ESTOQUE ---

async def baixar_estoque_seguro(item: PedidoItem):
    headers = {
        "Authorization": f"Bearer {DIRECTUS_ADMIN_TOKEN}",
        "Content-Type": "application/json"
    }
    
    async with httpx.AsyncClient() as client:
        # 1. BUSCA O ESTOQUE PAI (Cliente)
        resp_pai = await client.get(f"{DIRECTUS_URL}/items/estoque_cliente/{item.estoque_pai_id}", headers=headers)
        if resp_pai.status_code != 200:
            raise Exception(f"Estoque PAI não encontrado: {item.estoque_pai_id}")
        
        dados_pai = resp_pai.json()['data']
        qtd_atual_pai = int(dados_pai.get('quantidade_disponivel', 0))
        
        if qtd_atual_pai < item.quantidade:
            raise Exception(f"Saldo insuficiente no Estoque Geral. Disp: {qtd_atual_pai}, Req: {item.quantidade}")

        # 2. BUSCA E VALIDA O LOTE FILHO (Se for selecionado)
        if item.lote_estoque_id:
            resp_lote = await client.get(f"{DIRECTUS_URL}/items/estoque_lotes/{item.lote_estoque_id}", headers=headers)
            if resp_lote.status_code != 200:
                raise Exception(f"Lote {item.lote_estoque_id} não encontrado.")
            
            dados_lote = resp_lote.json()['data']
            qtd_atual_lote = int(dados_lote.get('quantidade', 0))
            
            if qtd_atual_lote < item.quantidade:
                raise Exception(f"Saldo insuficiente no Lote '{dados_lote.get('descricao')}'. Disp: {qtd_atual_lote}")
            
            # 3. BAIXA NO FILHO
            await client.patch(
                f"{DIRECTUS_URL}/items/estoque_lotes/{item.lote_estoque_id}",
                headers=headers,
                json={"quantidade": qtd_atual_lote - item.quantidade}
            )

        # 4. BAIXA NO PAI
        await client.patch(
            f"{DIRECTUS_URL}/items/estoque_cliente/{item.estoque_pai_id}",
            headers=headers,
            json={"quantidade_disponivel": qtd_atual_pai - item.quantidade}
        )

# --- ENDPOINTS ---

@app.get("/")
def health_check():
    return {"status": "online", "mode": "Secure Backend"}

# ROTA SEGURA: O Frontend chama essa rota para finalizar o envio
@app.post("/api/finalizar_envio")
async def finalizar_envio(pedido: PedidoRequest):
    try:
        headers = {
            "Authorization": f"Bearer {DIRECTUS_ADMIN_TOKEN}",
            "Content-Type": "application/json"
        }
        
        async with httpx.AsyncClient() as client:
            
            # 1. CRIA O LOTE DE ENVIO (Container)
            nome_lote = f"Envio via API - {len(pedido.itens)} itens"
            lote_payload = {
                "nome_lote": nome_lote,
                "status": "pendente", # Já entra como pendente pois o estoque será baixado agora
                "quantidade_total": sum(i.quantidade for i in pedido.itens),
                "organization_id": pedido.organization_id,
                "user_created": pedido.user_id
            }
            
            resp_lote = await client.post(f"{DIRECTUS_URL}/items/lotes_envio", headers=headers, json=lote_payload)
            if resp_lote.status_code not in [200, 201]:
                raise HTTPException(status_code=500, detail=f"Erro ao criar lote: {resp_lote.text}")
            
            novo_lote_id = resp_lote.json()['data']['id']
            
            # 2. PROCESSA CADA ITEM (Baixa estoque + Cria Solicitação)
            erros = []
            sucessos = 0
            
            for item in pedido.itens:
                try:
                    # A. Tenta Baixar o Estoque (Pai e Filho)
                    await baixar_estoque_seguro(item)
                    
                    # B. Cria a Solicitação no Directus
                    solicitacao_payload = {
                        "tipo": "SOLICITACAO_ENVIO",
                        "status": "pendente",
                        "lote_id": novo_lote_id,
                        "organization_id": pedido.organization_id,
                        "produto_id": item.produto_id,
                        "quantidade": item.quantidade,
                        "endereco_id": item.endereco_id,
                        "user_created": pedido.user_id,
                        "observacoes": f"Processado via API Segura. Lote/Variação: {item.lote_descricao or 'Geral'}"
                    }
                    await client.post(f"{DIRECTUS_URL}/items/solicitacoes", headers=headers, json=solicitacao_payload)
                    sucessos += 1
                    
                except Exception as e:
                    logger.error(f"Erro item: {e}")
                    erros.append(str(e))
            
            if len(erros) == len(pedido.itens):
                # Se tudo falhou
                raise HTTPException(status_code=400, detail=f"Falha em todos os itens: {erros[0]}")
                
            return {
                "status": "success", 
                "lote_id": novo_lote_id, 
                "processados": sucessos, 
                "erros": erros
            }

    except Exception as e:
        logger.error(f"Erro crítico: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ... (MANTENHA AQUI AS ROTAS ANTIGAS DE NORMALIZAÇÃO /analisar_colunas e /preview_importacao) ...
# Copie as funções extrair_cep_bruto, extrair_numero_inteligente, processar_dataframe e as rotas antigas
# do arquivo que você mandou anteriormente para cá, logo abaixo.
