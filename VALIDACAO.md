# Relatório de validação — V24

Data da validação: 2026-09-30.

## Resultado

O fluxo incremental foi validado em Chromium, tanto por servidor local quanto por abertura direta via `file://`. Não houve erro de console, erro de página, travamento do navegador ou falha de memória nos testes concluídos.

## Teste de carga

### ND real de grande volume

- Tamanho: 156.502.588 bytes.
- Linhas processadas: 280.734.
- Combinações compactas mantidas: 244.273.
- Tempo observado: 12,00 s.
- Maior latência de uma sondagem da interface: 0,233 s.
- Maior heap JavaScript observado na página principal durante a etapa: 77,0 MB.
- Amostras de responsividade durante o processamento: 24.

### Arquivo indicado como BL, mas com schema incompatível

- Tamanho: 130.511.596 bytes.
- O Worker leu o cabeçalho, detectou imediatamente a ausência das colunas obrigatórias e encerrou a etapa sem carregar o arquivo inteiro.
- Mensagem confirmada: `Colunas obrigatorias ausentes: COD_MUNICIPIO, NM_INDICADOR_NEGOCIO, NM_MARCA, DT, DT_VENDA, QT`.
- A interface permaneceu responsiva durante a rejeição.

### Fluxo completo ND + BL em sequência

Como o arquivo indicado como BL não tinha o schema de conversão exigido, o teste técnico completo reutilizou o CSV de conversão com schema compatível na posição BL. Isso valida carga, memória, processamento sequencial, cruzamento e renderização; não substitui a validação de conteúdo de uma base BL de negócio válida.

- Segundo arquivo de carga: 156.502.588 bytes.
- Linhas processadas na etapa BL técnica: 280.734.
- Combinações compactas na etapa BL técnica: 244.273.
- Tempo observado: 10,13 s.
- Maior latência de sondagem: 0,543 s.
- Maior heap JavaScript observado na página principal durante a etapa: 155,0 MB.
- Resultado combinado: 488.546 combinações.
- Arrays intermediários `P` e `PB` após o cruzamento: 0 registros retidos.
- Worker ativo após o término: nenhum.
- Ranking Subcluster renderizado: 106 linhas no teste de carga.
- Heap da página principal após todas as análises: 218,3 MB.
- Erros de console: 0.
- Erros de página: 0.

## JSONs individuais

Uma amostra temporária de 1.000 linhas do CSV real foi usada para validar os downloads:

| Tipo | Arquivo | Linhas de origem | Combinações | Tamanho |
|---|---|---:|---:|---:|
| ND | `nd-26-07.json` | 1.000 | 891 | 33.060 bytes |
| BL | `bl-26-07.json` | 1.000 | 891 | 33.060 bytes |

Os dois arquivos foram abertos e validados como JSON colunar v4, com `NR_ANO_MES` no conjunto de campos e sem erros de navegador.

## Abertura direta

O pacote também foi aberto por `file://`, recebeu duas bases de amostra, concluiu as análises e renderizou o Ranking Subcluster sem erros de console ou página.

## Fluxo OneDrive e JSON v4

Teste automatizado adicional confirmou, sem erros de console ou página:

- CSV ND/BL → `nd-26-09.json` e `bl-26-09.json`.
- Leitura de `manifest.json` por seletor de pasta.
- ND dividido em duas partes `.json.gz` e BL em `.json.gz`.
- Carga sequencial no Web Worker, cruzamento e Ranking Subcluster.
- Limpeza seletiva da base ND e recarga por seleção individual de JSON.
- Gravação direta em `FileSystemDirectoryHandle` e atualização de `manifest.json`.

Comandos: `python3 tests/onedrive_json_flow_test.py` e `python3 tests/direct_folder_save_test.py`.

## Limite da validação de negócio BL

O arquivo grande indicado como BL não possui o schema esperado pelo dashboard. A correção trata essa situação sem travamento e sem leitura integral, mas uma validação semântica de indicadores BL depende de um CSV BL que contenha as colunas obrigatórias documentadas em `docs/GUIA_DE_DADOS.md`.
