# Dashboard Conversão — V24

Dashboard web estático para importar bases CSV de **Conversão ND** e **Conversão BL**, validar a estrutura, cruzar municípios com a tabela de critérios incorporada e gerar análises operacionais.

O projeto é **autossuficiente**: `index.html` contém o dashboard e `memory-fix.js` executa a importação incremental em Web Worker. Não há framework, CDN, API ou etapa de build; o pacote pode ser aberto localmente ou publicado no GitHub Pages.

## Recursos

- Importação de CSV ND e BL diretamente no navegador.
- Leitura incremental em blocos de 2 MB dentro de Web Worker, sem manter arquivo bruto, texto completo ou objetos por linha.
- Processamento sequencial: ND e BL nunca são lidos simultaneamente; as análises só são renderizadas após o término das duas bases.
- Leitura em UTF-8, com fallback para Windows-1252.
- Conversão individual para JSON colunar compacto v4, gravado em fluxo quando o navegador oferece a API de arquivos.
- Nomes `nd-aa-mm.json` e `bl-aa-mm.json` obtidos de `NR_ANO_MES`, com manifesto em `dados/manifest.json`.
- Tabela de critérios incorporada com 510 registros de municípios.
- Cruzamento por `COD_MUNICIPIO`, com indicação de registros cruzados, não localizados ou conflitantes.
- KPIs de venda bruta, instalação, conversão e prazo.
- Rankings por regional, cluster e subcluster.
- Comparativo acumulado D-1 × D-2 para ND e BL.
- Análise diária com gráfico, zoom e resumo gerencial.
- Acompanhamento mês a mês por cluster, subcluster e cidade.
- Exportações de tabelas para Excel e seções para PDF via impressão do navegador.
- Layout responsivo para desktop e dispositivos móveis.

## Execução rápida

### Opção 1 — abrir diretamente

Abra `index.html` em um navegador moderno.

### Opção 2 — servidor local recomendado

```bash
python3 serve.py --port 8000
```

Depois acesse:

```text
http://localhost:8000
```

## Uso

1. Na aba **Base Analítica**, selecione o CSV da Conversão ND.
2. Selecione o CSV da Conversão BL.
3. Aguarde a leitura dos dois arquivos.
4. Use **Converter CSV ND/BL** se desejar gerar os JSONs compactos.
5. A validação das análises ocorre automaticamente quando as duas bases são aprovadas.
6. Navegue pelas abas **Conversão**, **Análise Diária**, **Acompanhamento Mês x Mês**, **Dados da Conversão** e **Tabela de Critério**.

### OneDrive local e JSON v4

- **Selecionar pasta do OneDrive** autoriza o dashboard a ler `manifest.json`, JSON v4, `.json.gz` e arquivos divididos diretamente da pasta sincronizada, sem publicar bases no GitHub.
- Após a autorização de leitura/gravação no Edge ou Chrome, **Baixar JSON ND/BL** grava `nd-aa-mm.json` e `bl-aa-mm.json` diretamente na pasta e atualiza `manifest.json`.
- **Atualizar pasta selecionada** força a releitura; com a opção automática marcada, o manifesto é verificado a cada 60 segundos.
- Os seletores individuais de JSON e o fluxo manual CSV → JSON continuam disponíveis.
- Sem File System Access API, baixe os JSONs e **manifest.json** e mova-os manualmente para a pasta.
- **Limpar base ND** e **Limpar base BL** removem somente a base escolhida.

Consulte [docs/AUTOMACAO_ONEDRIVE_GITHUB.md](docs/AUTOMACAO_ONEDRIVE_GITHUB.md) para o manifesto, automação local e publicação segura do código.

> Os arquivos CSV e os dados processados permanecem na memória do navegador. A aplicação não envia as bases para servidores externos.

## Formato dos dados

- Separador: ponto e vírgula (`;`).
- Codificação: UTF-8 ou Windows-1252.
- Datas reconhecidas: `AAAA-MM-DD` ou `AAAA/MM/DD`.
- Decimais em `QT`: vírgula ou ponto.
- Cabeçalhos devem manter os nomes esperados pela aplicação.

Consulte [docs/GUIA_DE_DADOS.md](docs/GUIA_DE_DADOS.md) para as colunas obrigatórias, recomendadas e regras de processamento.

## Publicação no GitHub Pages

O pacote inclui um workflow pronto em `.github/workflows/pages.yml`.

1. Crie o repositório e envie estes arquivos para a branch `main`.
2. No GitHub, abra **Settings → Pages**.
3. Em **Build and deployment → Source**, selecione **GitHub Actions**.
4. Execute o workflow **Publicar no GitHub Pages** ou faça um novo push em `main`.

Veja o passo a passo em [docs/PUBLICACAO_GITHUB_PAGES.md](docs/PUBLICACAO_GITHUB_PAGES.md).

## Validação técnica

Validação estrutural, sem dependências:

```bash
python3 tests/static_check.py
```

Teste funcional com Chromium:

```bash
python3 -m pip install -r requirements-dev.txt
python3 -m playwright install chromium
python3 tests/smoke_test.py
```

## Estrutura do repositório

```text
.
├── .github/
│   ├── ISSUE_TEMPLATE/bug_report.yml
│   ├── pull_request_template.md
│   └── workflows/
│       ├── pages.yml
│       └── quality.yml
├── dados/
│   ├── README.md
│   └── manifest.json
├── docs/
│   ├── GUIA_DE_DADOS.md
│   └── PUBLICACAO_GITHUB_PAGES.md
├── tests/
│   ├── json_download_test.py
│   ├── load_test.py
│   ├── smoke_test.py
│   └── static_check.py
├── .gitattributes
├── .gitignore
├── .nojekyll
├── CHANGELOG.md
├── CONTRIBUTING.md
├── SECURITY.md
├── VALIDACAO.md
├── index.html
├── memory-fix.js
├── requirements-dev.txt
└── serve.py
```

## Segurança e governança

- Não publique CSVs operacionais, JSONs gerados, relatórios exportados ou qualquer arquivo com dados pessoais ou corporativos.
- O `.gitignore` já exclui formatos comuns de dados e exportações.
- Antes de tornar o repositório público, confirme as regras internas de segurança, marca, propriedade intelectual e licenciamento.
- Consulte [SECURITY.md](SECURITY.md).

## Autoria

Material identificado no dashboard como criado por **OTAVIO J. FELISBERTO — GRUPO SC — GRUPO CLUSTER FLORIANÓPOLIS — Diretoria de Serviços Técnicos | Instalações RPS**.
