import pandas as pd
pd.set_option('display.max_columns', None)

# -*- coding: utf-8 -*-
"""
CNPJ (RFB) – pipeline download + unificação mensal:
1) Descobre e baixa ZIPs do mês (2 hosts, índice ou fallback canônico), com retry + resume
2) Unifica por tabela e adiciona header padronizado (layout oficial RFB), mesmo com arquivos internos sem .csv

Metadados/colunas (RFB): https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf
Padrão de nomes canônicos por mês (ex. 2024-09): ver README público com exemplos:
https://github.com/jonathands/dados-abertos-receita-cnpj
"""

from pathlib import Path
import os, re, io, time, sys, html, socket, ssl
import urllib.request, urllib.parse
from urllib.error import HTTPError, URLError
import zipfile

# =========================
# CONFIGURAÇÕES
# =========================
MONTH = "2025-09"  # ajuste o mês (YYYY-MM) // futuramente deixarei como variavel automatica com dtid preciso primeiro descobrir que dia que a receita sobe os arquviso
ROOT = Path.cwd() / f"cnpj_{MONTH}"
DOWNLOAD_DIR = ROOT / "zips"
OUTPUT_DIR = ROOT / "unificado"

# Recriar CSVs finais do zero?
FORCE_OVERWRITE = False

# Exibir nomes internos dos ZIPs quando não houver *.csv?
DEBUG_LIST_NONCSV = True

# Hosts oficiais (tenta nessa ordem)
HOSTS = [
    "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/{month}/",
    "https://dadosabertos.rfb.gov.br/CNPJ/dados_abertos_cnpj/{month}/",
]

# Timeouts e retentativas
DEFAULT_TIMEOUT = 300          # seg
READ_CHUNK = 1024 * 1024       # 1 MiB
MAX_RETRIES = 8
BACKOFF_BASE = 2.0
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Python-urllib/3.x"

socket.setdefaulttimeout(DEFAULT_TIMEOUT)

# =========================
# LAYOUT OFICIAL (HEADERS)
# =========================
# Fonte: metadados oficiais da RFB (separador ';') //do dicionario de dados da receita
HEADERS = {
    "empresas": [
        "cnpj_basico","razao_social","natureza_juridica",
        "qualificacao_responsavel","capital_social","porte",
        "ente_federativo_responsavel"
    ],
    "estabelecimentos": [
        "cnpj_basico","cnpj_ordem","cnpj_dv","matriz_filial","nome_fantasia",
        "situacao_cadastral","data_situacao_cadastral","motivo_situacao_cadastral",
        "nome_cidade_exterior","pais","data_inicio_atividade",
        "cnae_fiscal_principal","cnae_fiscal_secundaria","tipo_logradouro","logradouro",
        "numero","complemento","bairro","cep","uf","municipio",
        "ddd1","telefone1","ddd2","telefone2","ddd_fax","fax",
        "correio_eletronico","situacao_especial","data_situacao_especial"
    ],
    "socios": [
        "cnpj_basico","identificador_socio","nome_socio_razao_social",
        "cnpj_cpf_socio","qualificacao_socio","data_entrada_sociedade",
        "pais","cpf_representante_legal","nome_representante",
        "qualificacao_representante_legal","faixa_etaria"
    ],
    "simples": [
        "cnpj_basico","opcao_pelo_simples","data_opcao_simples",
        "data_exclusao_simples","opcao_pelo_mei","data_opcao_mei","data_exclusao_mei"
    ],
    "cnaes": ["codigo","descricao"],
    "naturezas_juridicas": ["codigo","descricao"],
    "municipios": ["codigo","descricao"],
    "paises": ["codigo","descricao"],
    "qualificacoes_socios": ["codigo","descricao"],
    "motivos": ["codigo","descricao"],  
}

EXPECTED_COLS = {
    "empresas": 7,
    "estabelecimentos": 30,
    "socios": 11,
    "simples": 7,
    "cnaes": 2,
    "naturezas_juridicas": 2,
    "municipios": 2,
    "paises": 2,
    "qualificacoes_socios": 2,
    "motivos": 2,
}

NAME_HINTS = {
    "empresas": ["empre", "empresa"],
    "estabelecimentos": ["estab"],
    "socios": ["socio", "sócio", "qsa"],
    "simples": ["simples"],
    "cnaes": ["cnae"],
    "naturezas_juridicas": ["natju", "natureza", "nat_juridica"],
    "municipios": ["munic", "municipio", "município"],
    "paises": ["pais", "país"],
    "qualificacoes_socios": ["qualif", "qual_socios", "qualificacao"],
    "motivos": ["motivo"],
}

# Nomes canônicos por mês (fallback)
NOMES_PADROES = (
    [f"Empresas{i}.zip" for i in range(10)] +
    [f"Estabelecimentos{i}.zip" for i in range(10)] +
    [f"Socios{i}.zip" for i in range(10)] +
    ["Simples.zip", "Cnaes.zip", "Naturezas.zip", "Municipios.zip",
     "Paises.zip", "Qualificacoes.zip", "Motivos.zip"]
)

# =========================
# HTTP/IO
# =========================
def build_opener():
    handlers = [
        urllib.request.ProxyHandler(),  # usa HTTP(S)_PROXY do ambiente, se houver
        urllib.request.HTTPCookieProcessor()
    ]
    opener = urllib.request.build_opener(*handlers)
    opener.addheaders = [("User-Agent", USER_AGENT)]
    return opener

def open_url(url, timeout=DEFAULT_TIMEOUT, headers=None):
    opener = build_opener()
    req = urllib.request.Request(url, headers=headers or {})
    return opener.open(req, timeout=timeout)

def fetch_zip_links_from_index(base_url: str):
    try:
        with open_url(base_url) as resp:
            page = resp.read().decode("utf-8", errors="ignore")
        hrefs = re.findall(r'href=\'"[\'"]', page, flags=re.I)
        links = []
        for h in hrefs:
            if h.lower().endswith(".zip"):
                links.append(urllib.parse.urljoin(base_url, html.unescape(h)))
        return sorted(set(links))
    except Exception:
        return []

def discover_all_zip_links(month: str, hosts):
    found = []
    for h in hosts:
        base = h.format(month=month)
        links = fetch_zip_links_from_index(base)
        if links:
            print(f"Host {base} -> {len(links)} ZIP(s) listados.")
            found.extend(links)
    return sorted(set(found))

def _stream_copy(resp, fhandle, chunk):
    while True:
        data = resp.read(chunk)
        if not data:
            break
        fhandle.write(data)

def download_file_resumable(url: str, dest: Path,
                            max_retries=MAX_RETRIES,
                            chunk=READ_CHUNK,
                            timeout=DEFAULT_TIMEOUT):
    """Download com retentativas + retomada (HTTP Range)."""
    if dest.exists() and dest.stat().st_size > 0:
        return "skipped"
    tmp = dest.with_suffix(dest.suffix + ".part")
    offset = tmp.stat().st_size if tmp.exists() else 0

    for attempt in range(1, max_retries+1):
        headers = {}
        if offset > 0:
            headers["Range"] = f"bytes={offset}-"
        try:
            with open_url(url, timeout=timeout, headers=headers) as r:
                status = getattr(r, "status", getattr(r, "code", 200))
                if offset > 0 and status == 200:
                    # Servidor ignorou Range: recomeçar do zero
                    with open(tmp, "wb") as _:
                        pass
                    offset = 0
                    with open_url(url, timeout=timeout) as r2, open(tmp, "ab") as f:
                        _stream_copy(r2, f, chunk)
                else:
                    with open(tmp, "ab") as f:
                        _stream_copy(r, f, chunk)
            tmp.replace(dest)
            return "baixado"
        except HTTPError as e:
            if e.code == 404:
                return "not_found"
            print(f"  - HTTPError {e.code} (tentativa {attempt}/{max_retries})")
        except (URLError, ssl.SSLError, socket.timeout, TimeoutError) as e:
            print(f"  - Timeout/Conexão ({type(e).__name__}) na tentativa {attempt}/{max_retries}")
        time.sleep(min(int(BACKOFF_BASE ** attempt), 60))
        offset = tmp.stat().st_size if tmp.exists() else 0
    return "erro"

def human(nbytes):
    if nbytes is None:
        return "?"
    units = ["B","KiB","MiB","GiB","TiB"]
    i = 0; x = float(nbytes)
    while x >= 1024 and i < len(units)-1:
        x /= 1024; i += 1
    return f"{x:.1f} {units[i]}"

def human_time(s):
    m, s = divmod(int(s), 60); h, m = divmod(m, 60)
    return f"{h:d}h{m:02d}m{s:02d}s"

# =========================
#  DETECÇÃO DE ARQUIVOS
# =========================
def looks_like_table(first_bytes: bytes):
    """Checa se a 1ª linha parece CSV ';' e retorna (tabela, ncols)."""
    try:
        line = first_bytes.decode("latin-1", errors="ignore").splitlines()[0] if first_bytes else ""
    except Exception:
        line = ""
    if not line or ";" not in line:
        return (None, 0)
    ncols = line.count(";") + 1
    # Desempate 7 colunas: 'simples' tem 2º campo S/N
    if ncols == 7:
        parts = line.split(";")
        if len(parts) >= 2 and parts[1] in ("S", "N", ""):
            return ("simples", ncols)
        return ("empresas", ncols)
    for k, exp in EXPECTED_COLS.items():
        if ncols == exp:
            return (k, ncols)
    return (None, ncols)

def guess_table_by_name(filename: str):
    """Sugestão de tabela pelo nome do arquivo (heurística)."""
    name = filename.lower()
    for k, hints in NAME_HINTS.items():
        if any(h in name for h in hints):
            return k
    return None

# =========================
# UNIFICAÇÃO
# =========================
def ensure_out(out_path: Path, header_cols):
    new_file = not out_path.exists() or out_path.stat().st_size == 0
    f = open(out_path, "ab")
    if new_file:
        f.write((";".join(header_cols) + "\n").encode("latin-1", errors="ignore"))
    return f

def append_member(zp: zipfile.ZipFile, member: str, out_path: Path, header_cols):
    """Copia conteúdo do arquivo interno para o CSV final (sem header na origem)."""
    with zp.open(member, "r") as src, ensure_out(out_path, header_cols) as out:
        while True:
            chunk = src.read(1024*1024)
            if not chunk:
                break
            out.write(chunk)

def choose_candidates(zf: zipfile.ZipFile):
    """Todos os arquivos não-diretório, maiores primeiro (tendem a ser os de dados)."""
    infos = [zi for zi in zf.infolist() if not zi.is_dir()]
    bad_ext = {".md5", ".sha1", ".pdf"}
    filtered = []
    for zi in infos:
        name_lower = zi.filename.lower()
        if any(name_lower.endswith(ext) for ext in bad_ext):
            continue
        filtered.append(zi)
    return sorted(filtered, key=lambda z: z.file_size or 0, reverse=True)

def unify_month(zips_dir: Path, out_dir: Path, force_overwrite=False):
    if force_overwrite:
        for f in out_dir.glob("*.csv"):
            try: f.unlink()
            except: pass

    zips = sorted(zips_dir.glob("*.zip"))
    if not zips:
        print("Nenhum ZIP encontrado para unificar.")
        return

    for idx, zp in enumerate(zips, 1):
        print(f"[{idx}/{len(zips)}] {zp.name}")
        try:
            with zipfile.ZipFile(zp, "r") as zf:
                # 1) Tentar membros *.csv (qualquer case)
                members = [m for m in zf.namelist() if m.lower().endswith(".csv")]
                if not members:
                    if DEBUG_LIST_NONCSV:
                        print("  - Conteúdo (sem .csv explícito):")
                        for zi in zf.infolist():
                            if not zi.is_dir():
                                print(f"    • {zi.filename}  ({human(zi.file_size)})")

                    # 2) Examinar candidatos (maiores primeiro) e decidir por conteúdo
                    candidates = choose_candidates(zf)
                    if not candidates:
                        print(f"  - AVISO: ZIP sem arquivos de dados: {zp.name}")
                        continue

                    handled_any = False
                    for zi in candidates:
                        # puxa as primeiras ~64 KiB para inferir
                        with zf.open(zi, "r") as fpeek:
                            head = fpeek.read(65536)
                        table_by_data, _ = looks_like_table(head)
                        table_by_name = guess_table_by_name(zi.filename)
                        table = table_by_data or table_by_name
                        if table and table in HEADERS:
                            out_path = out_dir / f"{table}.csv"
                            append_member(zf, zi.filename, out_path, HEADERS[table])
                            print(f"  - OK: {zi.filename} → {out_path.name} (detectado: {table})")
                            handled_any = True
                    if not handled_any:
                        print(f"  - AVISO: Não consegui identificar dados em {zp.name}. Pulando.")
                    continue

                # 3) Caminho “normal”: havia *.csv
                for m in members:
                    with zf.open(m, "r") as fpeek:
                        head = fpeek.read(65536)
                    table_by_data, _ = looks_like_table(head)
                    table_by_name = guess_table_by_name(m)
                    table = table_by_data or table_by_name
                    if table is None or table not in HEADERS:
                        print(f"  - AVISO: {m} não identificado (conteúdo inesperado). Pulando.")
                        continue
                    out_path = out_dir / f"{table}.csv"
                    append_member(zf, m, out_path, HEADERS[table])
                    print(f"  - OK: {m} → {out_path.name} (detectado: {table})")

        except zipfile.BadZipFile:
            print(f"  - ERRO: ZIP corrompido: {zp}")

# =========================
# ORQUESTRAÇÃO
# =========================
def main():
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Mês: {MONTH}")
    print("Download dir:", DOWNLOAD_DIR)
    print("Output dir:", OUTPUT_DIR)

    # 1) tentar listar via índice
    links = discover_all_zip_links(MONTH, HOSTS)
    print(f"ZIPs encontrados via índice: {len(links)}")

    # 2) se não listou, montar plano canônico
    if links:
        download_plan = links
    else:
        print("\nNenhum link listado. Vamos tentar pelos nomes canônicos conhecidos...")
        seen = set(); download_plan = []
        for host_tpl in HOSTS:
            base = host_tpl.format(month=MONTH)
            for fn in NOMES_PADROES:
                url = urllib.parse.urljoin(base, fn)
                if url not in seen:
                    seen.add(url)
                    download_plan.append(url)

    # 3) baixar com resume + retries
    status_ct = {"downloaded":0, "skipped":0, "not_found":0, "error":0}
    t0 = time.time()
    for i, url in enumerate(download_plan, 1):
        dest = DOWNLOAD_DIR / Path(urllib.parse.urlparse(url).path).name
        st = download_file_resumable(url, dest)
        status_ct[st] = status_ct.get(st, 0) + 1
        if st != "not_found":
            print(f"[{i}] {dest.name}: {st}")
    print("\nResumo download:", status_ct, f"(tempo: {human_time(time.time()-t0)})")

    # 4) unificar por tabela com header padronizado
    unify_month(DOWNLOAD_DIR, OUTPUT_DIR, force_overwrite=FORCE_OVERWRITE)

    # 5) listar saídas
    outs = sorted(OUTPUT_DIR.glob("*.csv"))
    print("\nConcluído. CSVs finais em:", OUTPUT_DIR.resolve())
    if outs:
        for f in outs:
            try: size = f.stat().st_size
            except: size = None
            print(f" - {f.name} ({human(size)})")
    else:
        print("Nenhum CSV final encontrado – verifique os avisos acima.")

# Executa
if __name__ == "__main__":
    main()



#######################################################
#separação por UF
#######################################################
#Código para dividir o arquivo estabelecimentos.csv por UF e salvar em arquivos separados com no máximo
import pandas as pd
import os

CHUNKSIZE = 100_000
MAX_LINHAS = 1_000_000
PASTA_SAIDA = "estabelecimentos_por_uf"
os.makedirs(PASTA_SAIDA, exist_ok=True)

# Dicionário para controle de buffers e contadores por UF
buffers = {}
contadores = {}

chunks = pd.read_csv(
    'cnpj_2025-09/unificado/estabelecimentos.csv',
    sep=';',
    encoding='latin1',
    dtype=str,
    low_memory=False,
    chunksize=CHUNKSIZE
)

for chunk in chunks:
    for uf in chunk['uf'].unique():
        df_uf = chunk[chunk['uf'] == uf]
        if uf not in buffers:
            buffers[uf] = []
            contadores[uf] = 1
        buffers[uf].append(df_uf)
        total = sum(len(df) for df in buffers[uf])
        # Se atingiu ou passou 1 milhão de linhas, salva e limpa buffer
        while total >= MAX_LINHAS:
            df_concat = pd.concat(buffers[uf], ignore_index=True)
            df_to_save = df_concat.iloc[:MAX_LINHAS]
            nome = f"{PASTA_SAIDA}/estabelecimentos_{uf}{contadores[uf] if contadores[uf] > 1 else ''}.csv"
            df_to_save.to_csv(nome, sep=';', index=False, encoding='latin1')
            print(f"Salvo {nome} com {len(df_to_save)} linhas")
            # Atualiza buffer para o restante
            buffers[uf] = [df_concat.iloc[MAX_LINHAS:]]
            total = sum(len(df) for df in buffers[uf])
            contadores[uf] += 1

# Salva o restante de cada UF
for uf, dfs in buffers.items():
    if dfs and sum(len(df) for df in dfs) > 0:
        df_concat = pd.concat(dfs, ignore_index=True)
        nome = f"{PASTA_SAIDA}/estabelecimentos_{uf}{contadores[uf] if contadores[uf] > 1 else ''}.csv"
        df_concat.to_csv(nome, sep=';', index=False, encoding='latin1')
        print(f"Salvo {nome} com {len(df_concat)} linhas")

from unidecode import unidecode

def padroniza_df(df, ignorar="email"):
    # Deixa tudo maiúsculo
    df = df.applymap(lambda x: x.upper() if isinstance(x, str) else x)
    # Remove acentuação e ç
    df = df.applymap(lambda x: unidecode(x) if isinstance(x, str) else x)
    # Substitui ( e ) por - exceto colunas com o substring especificado
    for col in df.columns:
        if ignorar.lower() not in col.lower():
            df[col] = df[col].apply(lambda x: x.replace('(', '-').replace(')', '-') if isinstance(x, str) else x)
    return df

PASTA_SAIDA = "estabelecimentos_por_uf"

for nome_arquivo in os.listdir(PASTA_SAIDA):
    if nome_arquivo.startswith("estabelecimentos_") and nome_arquivo.endswith(".csv"):
        # Extrai a UF do nome do arquivo
        uf = nome_arquivo.split("_")[1][:2]
        pasta_uf = os.path.join(PASTA_SAIDA, uf)
        os.makedirs(pasta_uf, exist_ok=True)
        caminho_arquivo = os.path.join(PASTA_SAIDA, nome_arquivo)
        # Lê, padroniza e salva no diretório correto
        df = pd.read_csv(caminho_arquivo, sep=";", encoding="latin1", dtype=str, low_memory=False)
        df = padroniza_df(df)
        novo_caminho = os.path.join(pasta_uf, nome_arquivo)
        df.to_csv(novo_caminho, sep=";", index=False, encoding="latin1")
        print(f"Arquivo salvo em: {novo_caminho}")
        os.remove(caminho_arquivo)



import pandas as pd
import os
from unidecode import unidecode

CHUNKSIZE = 100_000
MAX_LINHAS = 1_000_000
PASTA_UNIFICADO = "cnpj_2025-09/unificado"
TIPOS = ["empresas", "socios", "cnaes"]  # Adicione outros tipos se necessário

def padroniza_df(df, ignorar="email"):
    df = df.applymap(lambda x: x.upper() if isinstance(x, str) else x)
    df = df.applymap(lambda x: unidecode(x) if isinstance(x, str) else x)
    for col in df.columns:
        if ignorar.lower() not in col.lower():
            df[col] = df[col].apply(lambda x: x.replace('(', '-').replace(')', '-') if isinstance(x, str) else x)
    return df

for tipo in TIPOS:
    arquivo = os.path.join(PASTA_UNIFICADO, f"{tipo}.csv")
    if not os.path.exists(arquivo):
        print(f"Arquivo não encontrado: {arquivo}")
        continue

    pasta_saida = f"{tipo}_partes"
    os.makedirs(pasta_saida, exist_ok=True)
    buffers = []
    contador = 1

    chunks = pd.read_csv(
        arquivo,
        sep=';',
        encoding='latin1',
        dtype=str,
        low_memory=False,
        chunksize=CHUNKSIZE
    )

    for chunk in chunks:
        buffers.append(chunk)
        total = sum(len(df) for df in buffers)
        while total >= MAX_LINHAS:
            df_concat = pd.concat(buffers, ignore_index=True)
            df_to_save = df_concat.iloc[:MAX_LINHAS]
            df_to_save = padroniza_df(df_to_save)
            nome = f"{pasta_saida}/{tipo}{contador if contador > 1 else ''}.csv"
            df_to_save.to_csv(nome, sep=';', index=False, encoding='latin1')
            print(f"Salvo {nome} com {len(df_to_save)} linhas")
            buffers = [df_concat.iloc[MAX_LINHAS:]]
            total = sum(len(df) for df in buffers)
            contador += 1

    # Salva o restante
    if buffers and sum(len(df) for df in buffers) > 0:
        df_concat = pd.concat(buffers, ignore_index=True)
        df_concat = padroniza_df(df_concat)
        nome = f"{pasta_saida}/{tipo}{contador if contador > 1 else ''}.csv"
        df_concat.to_csv(nome, sep=';', index=False, encoding='latin1')
        print(f"Salvo {nome} com {len(df_concat)} linhas")
