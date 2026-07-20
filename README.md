# RFB CNPJ Data Pipeline

Pipeline em Python para baixar, identificar, consolidar e particionar os dados abertos de CNPJ publicados pela Receita Federal do Brasil.

## Problema resolvido

A base pública de CNPJ é distribuída mensalmente em dezenas de arquivos ZIP, separados por entidade e, em alguns casos, sem extensões internas consistentes. Trabalhar com esse material exige tratar downloads longos, arquivos fragmentados, layouts diferentes e volumes que não devem ser carregados integralmente em memória.

O script deste repositório cobre as seguintes etapas:

1. descobre os arquivos do mês em hosts oficiais;
2. executa downloads com retentativas e retomada por HTTP Range;
3. identifica o tipo de tabela por nome e quantidade de colunas;
4. extrai e consolida os arquivos por entidade;
5. adiciona os cabeçalhos oficiais da RFB;
6. divide arquivos grandes em partes menores;
7. organiza estabelecimentos por UF;
8. padroniza campos textuais para consumo analítico.

## Tecnologias

- Python 3;
- pandas;
- urllib e zipfile;
- Unidecode;
- processamento em chunks.

## Arquivo principal

- `tratamento.py`: concentra o fluxo de download, extração, consolidação, particionamento e padronização.

## Configuração

No início do arquivo, ajuste a competência no formato `YYYY-MM`:

```python
MONTH = "2025-09"
```

Os diretórios de download e saída são criados a partir da pasta de execução:

```text
cnpj_YYYY-MM/
├── zips/
└── unificado/
```

## Instalação

```bash
python -m venv .venv

# Windows
.venv\Scripts\activate

# Linux/macOS
source .venv/bin/activate

pip install -r requirements.txt
```

## Execução

```bash
python tratamento.py
```

## Decisões técnicas

- **Downloads retomáveis:** arquivos parciais usam extensão `.part` e podem continuar após falhas de rede.
- **Retentativas com backoff:** reduz falhas em downloads demorados.
- **Detecção de layout:** combina heurísticas de nome e quantidade de colunas.
- **Processamento em chunks:** evita carregar arquivos completos na memória.
- **Cabeçalhos padronizados:** utiliza o layout público da Receita Federal.

## Limitações atuais

- a competência ainda é definida diretamente no código;
- as etapas estão reunidas em um único arquivo;
- a execução completa pode consumir bastante disco e tempo de rede;
- alterações no layout publicado pela RFB podem exigir atualização dos cabeçalhos e heurísticas;
- não há testes automatizados neste estágio.

## Próximos passos

- separar download, extração, transformação e particionamento em módulos;
- expor a competência por argumento de linha de comando;
- adicionar validações de quantidade de linhas e arquivos;
- criar testes para classificação dos layouts;
- adicionar logging estruturado.

## Fonte dos dados

Os arquivos e metadados utilizados pelo projeto são públicos e disponibilizados pela Receita Federal do Brasil.