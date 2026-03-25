# FFmpeg Video Processor API

API de processamento de vídeo com FFmpeg para deploy no Railway.

## Deploy no Railway

1. Crie uma conta em [railway.app](https://railway.app)
2. Clique em **"New Project"** → **"Deploy from GitHub repo"**
3. Conecte este repositório (ou faça upload da pasta `railway-ffmpeg-api`)
4. Configure as variáveis de ambiente:
   - `API_KEY` — uma chave secreta para proteger a API (ex: gere com `openssl rand -hex 32`)
5. Railway detecta o Dockerfile automaticamente e faz o deploy
6. Copie a URL gerada (ex: `https://seu-app.railway.app`)
7. No app Lovable, vá em **Configurações** e cole a URL do servidor

## Endpoints

### `GET /health`
Health check. Retorna `{ status: "ok", ffmpeg: true }`

### `POST /api/upload-assets`
Upload dos assets (popup, áudio, música). Retorna `sessionId`.

**Headers:** `x-api-key: SUA_CHAVE`
**Body (multipart):** `popupMedia`, `popupAudio`, `bgMusic`

### `POST /api/process`
Processa um vídeo com os assets da sessão.

**Headers:** `x-api-key: SUA_CHAVE`
**Body (multipart):**
- `video` — arquivo do vídeo
- `sessionId` — ID retornado pelo upload-assets
- `config` — JSON string com configurações

### `POST /api/process-url`
Processa vídeo direto de URL (sem upload).

**Headers:** `x-api-key: SUA_CHAVE`
**Body (JSON):**
```json
{
  "sessionId": "...",
  "videoUrl": "https://...",
  "config": { ... }
}
```

### `DELETE /api/session/:sessionId`
Limpa assets da sessão.

## Variáveis de Ambiente

| Variável | Descrição | Padrão |
|----------|-----------|--------|
| `PORT` | Porta do servidor | 3000 |
| `API_KEY` | Chave de autenticação | (vazio = sem auth) |
