# Histórico de alterações

## V24 OneDrive — 2026-09-30

- Adicionada seleção autorizada de pasta local sincronizada pelo OneDrive, com leitura de `manifest.json` e JSONs ND/BL sem publicação das bases no GitHub.
- Adicionadas importação individual de JSON colunar v4, leitura de `.json.gz` e união de arquivos divididos no Web Worker.
- Adicionada gravação direta de `nd-aa-mm.json` e `bl-aa-mm.json` na pasta autorizada, com atualização segura do manifesto e alternativa manual.
- Adicionados **Limpar base ND**, **Limpar base BL**, atualização manual e verificação automática da pasta.
- Mantidos cálculos, análises, filtros, auditoria, rankings Regional/Cluster/Subcluster e exportações existentes.

## V24 — 2026-09-30

- Substituída a leitura integral dos CSVs por Web Worker com blocos de 2 MB.
- Agregação realizada durante a leitura; texto bruto e `ArrayBuffer` temporário são liberados a cada bloco.
- ND e BL são processados sequencialmente, sem renderização das análises durante a segunda importação.
- JSONs passam a ser gerados individualmente e em fluxo, com nomes `nd-aa-mm.json` e `bl-aa-mm.json` baseados em `NR_ANO_MES`.
- Preservados `dados/manifest.json`, análises existentes e adicionado Ranking Subcluster.
- Cruzamento com critérios passa a ceder o controle à interface em lotes.

## V22 — 2026-09-30

- Preparado pacote completo para repositório GitHub.
- Mantido o dashboard autossuficiente em `index.html`.
- Adicionados workflows de validação e publicação no GitHub Pages.
- Adicionados guia de dados, instruções de publicação e segurança.
- Adicionados testes estrutural e funcional.
- Adicionados arquivos de colaboração e proteção contra commit de bases operacionais.
