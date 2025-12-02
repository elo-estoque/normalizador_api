import streamlit as st
import pandas as pd
import re
import io

# Configuração da página
st.set_page_config(page_title="Normalizador de Endereços", layout="wide")

# Título
st.title("🚚 ELO-Normalizador Automático de Endereços (CEP + Layout Final)")
st.markdown("Faça upload da sua planilha para separar Logradouro, Número, Bairro, Cidade, UF e gerar o CEP formatado.")

# --- FUNÇÕES DE EXTRAÇÃO (REGEX) ---

def limpar_texto(texto):
    if not isinstance(texto, str):
        return str(texto)
    return texto.strip()

def extrair_cep(texto):
    if not isinstance(texto, str):
        return None
    
    # Padrão 1: CEP com hífen (ex: 05415-050)
    match_com_hifen = re.search(r'\b\d{5}-\d{3}\b', texto)
    if match_com_hifen:
        return match_com_hifen.group(0)
    
    # Padrão 2: CEP sem hífen (8 dígitos seguidos) - ex: 05415050
    # Evita pegar telefones ou CNPJ filtrando sequências maiores
    match_sem_hifen = re.search(r'\b\d{8}\b', texto)
    if match_sem_hifen:
        cep = match_sem_hifen.group(0)
        return f"{cep[:5]}-{cep[5:]}"
    
    return None

def extrair_uf(texto):
    if not isinstance(texto, str): return ''
    # Procura por UF no final da string ou isolada (ex: /SP, - SP, ou SP no fim)
    match = re.search(r'\b([A-Z]{2})\b$', texto.strip())
    if match:
        return match.group(1)
    return ''

def extrair_numero(texto):
    if not isinstance(texto, str): return ''
    # Tenta achar "nº 123", "n 123", ", 123" ou número isolado que não seja CEP
    # Remove o CEP da string antes de buscar o número para não confundir
    cep = extrair_cep(texto)
    temp_text = texto
    if cep:
        temp_text = texto.replace(cep, '').replace(cep.replace('-', ''), '')
    
    # Procura padrão de número
    match = re.search(r'(?:nº|n°|num|numero|número|n\.|,)\s*(\d+)', temp_text, re.IGNORECASE)
    if match:
        return match.group(1)
    
    # Tenta pegar número isolado no fim da string (ex: Rua Tal, 123)
    match_fim = re.search(r'\b(\d+)\s*$', temp_text)
    if match_fim:
        return match_fim.group(1)
    
    return "S/N"

def classificar_regiao(uf):
    sul = ['RS', 'SC', 'PR']
    sudeste = ['SP', 'RJ', 'MG', 'ES']
    centro_oeste = ['MT', 'MS', 'GO', 'DF']
    nordeste = ['BA', 'PI', 'MA', 'CE', 'RN', 'PB', 'PE', 'AL', 'SE']
    norte = ['AC', 'RO', 'AM', 'RR', 'PA', 'AP', 'TO']
    
    uf = uf.upper().strip()
    if uf in sul: return 'Sul'
    if uf in sudeste: return 'Sudeste'
    if uf in centro_oeste: return 'Centro-Oeste'
    if uf in nordeste: return 'Nordeste'
    if uf in norte: return 'Norte'
    return 'Indefinido'

# --- FUNÇÃO PRINCIPAL DE PROCESSAMENTO ---

def processar_planilha(df, col_endereco, col_nome, col_ac):
    df = df.copy()
    
    # 1. Normalizar Endereço Original
    df['Endereço Original'] = df[col_endereco].apply(limpar_texto)
    
    # 2. Extrair CEP
    df['CEP_Final'] = df['Endereço Original'].apply(extrair_cep)
    
    # 3. Tentar limpar CEP do endereço para facilitar extração de logradouro
    def remover_cep_do_texto(row):
        texto = row['Endereço Original']
        cep = row['CEP_Final']
        if cep:
            texto = texto.replace(cep, '').replace(cep.replace('-', ''), '')
        return texto.strip(' ,-')
    
    df['Endereço_Sem_CEP'] = df.apply(remover_cep_do_texto, axis=1)
    
    # 4. Extrair Número
    df['Numero_Final'] = df['Endereço_Sem_CEP'].apply(extrair_numero)
    
    # 5. Logradouro (Simplificado: Tudo antes do número ou vírgula)
    def extrair_logradouro(texto):
        # Pega tudo antes da primeira vírgula ou do primeiro número
        match = re.split(r',|nº|n°|\d+', texto)[0]
        return match.strip()
    
    df['Logradouro_Final'] = df['Endereço_Sem_CEP'].apply(extrair_logradouro)
    
    # 6. Complemento (O que sobra depois do número)
    # Lógica simplificada: Se tem numero, o que vem depois pode ser complemento/bairro
    def extrair_complemento(row):
        texto = row['Endereço_Sem_CEP']
        num = row['Numero_Final']
        if num and num != "S/N" and num in texto:
            partes = texto.split(num, 1)
            if len(partes) > 1:
                return partes[1].strip(' ,-')
        return ''
    
    df['Complemento_Bruto'] = df.apply(extrair_complemento, axis=1)
    
    # 7. Separar Bairro, Cidade e UF (Baseado em posição comum: Bairro - Cidade / UF)
    # Esta parte é a mais difícil sem uma base de dados de CEPS. Vamos tentar heurísticas.
    
    def separar_geografia(texto):
        # Tenta pegar UF (2 letras maiúsculas no fim)
        uf = extrair_uf(texto)
        cidade = ''
        bairro = ''
        
        resto = texto
        if uf:
            # Remove UF do fim
            resto = re.sub(r'\b'+uf+r'\b', '', texto).strip(' ,/-')
            
        # Assume que o que sobrou no final é a cidade
        partes = re.split(r'[,-]', resto)
        partes = [p.strip() for p in partes if p.strip()]
        
        if len(partes) >= 1:
            cidade = partes[-1]
        if len(partes) >= 2:
            bairro = partes[-2]
        
        # Limpeza extra se bairro for muito curto ou parecer complemento
        if len(bairro) < 3 and len(partes) >= 3:
             bairro = partes[-3] # Tenta pegar anterior
             
        return pd.Series([bairro, cidade, uf])

    # Aplica separação na coluna original ou no complemento bruto? 
    # Melhor aplicar no endereço original processado para ter contexto
    df[['Bairro_Final', 'Cidade_Final', 'UF_Final']] = df['Endereço_Sem_CEP'].apply(separar_geografia)
    
    # 8. Região
    df['Regiao_Final'] = df['UF_Final'].apply(classificar_regiao)
    
    # 9. Complemento Final (Refinado)
    df['Complemento_Final'] = df['Complemento_Bruto'].apply(lambda x: x if len(x) < 20 else '') # Se for muito longo, provavelmente é bairro/cidade
    
    # 10. Mapear Nome e A/C
    df['Nome_Final'] = df[col_nome] if col_nome else ''
    df['Departamento'] = df[col_ac] if col_ac else ''
    
    # 11. Status (Validação)
    def validar(row):
        erros = []
        if not row['CEP_Final']: erros.append("Falta CEP")
        if not row['Numero_Final']: erros.append("Falta Número")
        if not row['UF_Final']: erros.append("Falta UF")
        if not erros: return "OK"
        return ", ".join(erros)
        
    df['Status_Sistema'] = df.apply(validar, axis=1)
    
    # --- ORGANIZAÇÃO FINAL DAS COLUNAS (ORDEM CORREIOS) ---
    
    # Gerar IDs sequenciais
    df['ID'] = [f'ID_{i}' for i in range(len(df))]
    
    # 1. Definir a ordem exata das colunas internas
    colunas_ordenadas = [
        'ID', 
        'Nome_Final', 
        'CEP_Final', 
        'Logradouro_Final', 
        'Numero_Final', 
        'Complemento_Final', 
        'Bairro_Final', 
        'Cidade_Final', 
        'UF_Final', 
        'Regiao_Final', 
        'Departamento', 
        'Endereço Original'
    ]
    
    # 2. Filtrar o dataframe para ter apenas essas colunas
    df_export = df[colunas_ordenadas].copy()
    
    # 3. Renomear para os cabeçalhos finais exigidos
    df_export.columns = [
        'ID',
        'Nome (Clube)',
        'CEP',
        'Logradouro',
        'N°',
        'Complemento',
        'Bairro',
        'Cidade',
        'UF',
        'Região',
        'Aos cuidados',
        'Endereço Original'
    ]
    
    return df_export

# --- DOWNLOADER ---
def convert_df(df):
    output = io.BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    df.to_excel(writer, index=False, sheet_name='Sheet1')
    writer.close()
    processed_data = output.getvalue()
    return processed_data

# --- INTERFACE ---

uploaded_file = st.file_uploader("Escolha um arquivo Excel (.xlsx) ou CSV", type=['xlsx', 'csv'])

if uploaded_file is not None:
    try:
        if uploaded_file.name.endswith('.csv'):
            df = pd.read_csv(uploaded_file)
        else:
            df = pd.read_excel(uploaded_file)
            
        st.write("### Pré-visualização dos dados originais:")
        st.dataframe(df.head())
        
        # Seleção de Colunas
        cols = df.columns.tolist()
        
        c1, c2, c3 = st.columns(3)
        col_endereco = c1.selectbox("Selecione a coluna de ENDEREÇO COMPLETO:", cols, index=0)
        col_nome = c2.selectbox("Coluna de Nome (Destinatário):", [None] + cols, index=1 if len(cols)>1 else 0)
        col_ac = c3.selectbox("Coluna A/C (Departamento):", [None] + cols, index=len(cols)-1 if len(cols)>2 else 0)
        
        if st.button("Processar e Normalizar"):
            with st.spinner('A IA está analisando os endereços...'):
                df_processado = processar_planilha(df, col_endereco, col_nome, col_ac)
                
                st.success("Processamento concluído!")
                
                st.write("### Dados Normalizados:")
                # Exibe a tabela sem estilização de status, pois a coluna Status foi removida para o layout final
                st.dataframe(df_processado)
                
                # Botões de Download
                csv = df_processado.to_csv(index=False).encode('utf-8-sig')
                excel_data = convert_df(df_processado)
                
                c_down1, c_down2 = st.columns(2)
                
                c_down1.download_button(
                    label="📥 Baixar em CSV",
                    data=csv,
                    file_name='enderecos_normalizados_correios.csv',
                    mime='text/csv',
                )
                
                c_down2.download_button(
                    label="📥 Baixar em Excel",
                    data=excel_data,
                    file_name='enderecos_normalizados_correios.xlsx',
                    mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                )
                
    except Exception as e:
        st.error(f"Erro ao ler o arquivo: {e}")
