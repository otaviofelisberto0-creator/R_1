# Contribuição

## Fluxo recomendado

1. Crie uma branch a partir de `main`.
2. Faça alterações pequenas e focadas.
3. Execute os testes estruturais e funcionais.
4. Valide manualmente as abas afetadas com arquivos autorizados.
5. Abra um pull request descrevendo cálculos, filtros e exportações impactados.

## Verificações obrigatórias

```bash
python3 tests/static_check.py
python3 -m pip install -r requirements-dev.txt
python3 -m playwright install chromium
python3 tests/smoke_test.py
```

## Regras de dados

- Nunca adicione bases CSV/TXT reais ao Git.
- Nunca adicione JSONs gerados, planilhas ou PDFs exportados.
- Use evidências anonimizadas ao relatar problemas.
- Não altere fórmulas ou critérios sem documentar a regra de negócio.

## Estilo técnico

- Preserve a execução estática, sem build e sem dependências remotas de runtime.
- Mantenha compatibilidade com abertura local e GitHub Pages.
- Garanta contraste, navegação por teclado e layout responsivo.
- Evite alterar nomes de colunas consumidos pelo dashboard.
- Atualize `CHANGELOG.md` e a documentação correspondente.
