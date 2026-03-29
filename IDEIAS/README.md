# Pipeline de Ideias via YouTube

Este diretório pode receber transcripts públicos de um canal do YouTube para montar um acervo de referências sobre produto, crescimento, monetização e mecânicas que funcionam em app development.

## Como rodar

```bash
python3 scripts/youtube_channel_ingest.py \
  "https://www.youtube.com/@starterstory/videos" \
  --months 12
```

Opções úteis:

- `--languages pt,pt-BR,en,en-US`: define a prioridade dos idiomas buscados.
- `--translate-to pt`: tenta traduzir transcripts translatables para PT se necessário.
- `--limit 10`: processa só os primeiros vídeos após o filtro temporal.
- `--output-root IDEIAS/youtube`: muda o diretório de saída.

## Estrutura de saída

O script cria uma árvore como esta:

```text
IDEIAS/youtube/<canal>/
  channel.json
  index.json
  failures.json
  videos/
    2026-03-01--titulo-video--abc123/
      metadata.json
      transcript.json
      transcript.txt
      ideas.md
```

## Como usar o material

1. Leia `index.json` para ver tudo que foi coletado.
2. Abra os `transcript.txt` dos vídeos mais promissores.
3. Preencha `ideas.md` com:
   - hook
   - problema atacado
   - insight aplicável a app development
   - sinal de monetização/distribuição
   - experimento que vale testar

## Limitações

- O fluxo depende de transcripts/legendas públicos do YouTube.
- Se um vídeo não tiver transcript disponível, ele cai em `failures.json`.
- O feed público do canal costuma expor os uploads recentes; o filtro de `--months` é aplicado em cima desse feed.
