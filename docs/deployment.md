# Deployment

Notion webhooks require a publicly accessible HTTPS endpoint. Here are several options to expose your NHook server.

## Option 1: Cloudflare Tunnel (Recommended for Home/Local)

Cloudflare Tunnel exposes your local server without opening ports or configuring NAT.

### Setup

1. Install cloudflared:
   ```bash
   # macOS
   brew install cloudflared

   # Linux
   curl -L https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64 -o cloudflared
   chmod +x cloudflared
   ```

2. Authenticate:
   ```bash
   cloudflared tunnel login
   ```

3. Create a tunnel:
   ```bash
   cloudflared tunnel create nhook
   ```

4. Configure DNS (use your domain in Cloudflare):
   ```bash
   cloudflared tunnel route dns nhook nhook.yourdomain.com
   ```

5. Create config file `~/.cloudflared/config.yml`:
   ```yaml
   tunnel: <tunnel-id>
   credentials-file: ~/.cloudflared/<tunnel-id>.json

   ingress:
     - hostname: nhook.yourdomain.com
       service: http://localhost:8000
     - service: http_status:404
   ```

6. Run the tunnel:
   ```bash
   # Terminal 1: Start NHook
   uv run nhook

   # Terminal 2: Start tunnel
   cloudflared tunnel run nhook
   ```

Your webhook URL: `https://nhook.yourdomain.com/webhooks/notion`

### Run as Service (Linux)

```bash
sudo cloudflared service install
sudo systemctl enable cloudflared
sudo systemctl start cloudflared
```

## Option 2: Tailscale Funnel

If you use Tailscale, Funnel can expose your server publicly.

### Setup

1. Enable Funnel in Tailscale admin console
2. Run:
   ```bash
   # Start NHook
   uv run nhook

   # In another terminal
   tailscale funnel 8000
   ```

Your webhook URL: `https://<machine-name>.<tailnet>.ts.net/webhooks/notion`

## Option 3: ngrok (Quick Testing)

For quick testing during development:

```bash
# Install ngrok
brew install ngrok  # or download from ngrok.com

# Start NHook
uv run nhook

# In another terminal
ngrok http 8000
```

Use the provided HTTPS URL (changes each restart unless you have a paid plan).

## Option 4: VPS/Cloud Deployment

### Docker

1. Build the image (if using Nix flake):
   ```bash
   nix build .#dockerImage
   docker load < result
   ```

   Or create a `Dockerfile`:
   ```dockerfile
   FROM python:3.12-slim

   WORKDIR /app

   # Install uv
   RUN pip install uv

   # Copy project files
   COPY pyproject.toml uv.lock ./
   COPY src/ ./src/

   # Install dependencies
   RUN uv sync --frozen

   # Run server
   CMD ["uv", "run", "nhook"]
   ```

2. Run with Docker Compose:
   ```yaml
   # docker-compose.yml
   version: "3.8"
   services:
     nhook:
       build: .
       ports:
         - "8000:8000"
       environment:
         - WEBHOOK_SECRET_KEY=${WEBHOOK_SECRET_KEY}
         - NOTION_API_TOKEN=${NOTION_API_TOKEN}
       restart: unless-stopped
   ```

3. Run behind Nginx with SSL (Let's Encrypt):
   ```nginx
   server {
       listen 443 ssl http2;
       server_name nhook.yourdomain.com;

       ssl_certificate /etc/letsencrypt/live/nhook.yourdomain.com/fullchain.pem;
       ssl_certificate_key /etc/letsencrypt/live/nhook.yourdomain.com/privkey.pem;

       location / {
           proxy_pass http://127.0.0.1:8000;
           proxy_set_header Host $host;
           proxy_set_header X-Real-IP $remote_addr;
           proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
           proxy_set_header X-Forwarded-Proto $scheme;
       }
   }
   ```

### Fly.io

1. Install flyctl:
   ```bash
   curl -L https://fly.io/install.sh | sh
   ```

2. Create `fly.toml`:
   ```toml
   app = "nhook"
   primary_region = "iad"

   [build]
     builder = "paketobuildpacks/builder:base"

   [env]
     PORT = "8000"

   [http_service]
     internal_port = 8000
     force_https = true

   [[http_service.checks]]
     path = "/health"
     interval = "30s"
     timeout = "5s"
   ```

3. Set secrets and deploy:
   ```bash
   fly secrets set WEBHOOK_SECRET_KEY=your-secret
   fly secrets set NOTION_API_TOKEN=secret_xxx
   fly deploy
   ```

### Railway / Render

Both support Python apps with minimal configuration. Set environment variables in their dashboards.

## Notion Automation Setup

1. Go to your Gastos database in Notion
2. Click the lightning bolt (⚡) → "New automation"
3. Configure trigger:
   - **When**: "Property edited"
   - **Property**: "Date"
4. Add action:
   - **Action**: "Send webhook"
   - **URL**: Your public URL (e.g., `https://nhook.yourdomain.com/webhooks/notion`)
   - **Headers**: Add `X-Calvo-Key` with your secret value
   - **Body**: Include `id` and `Date` properties

Example webhook body template:
```json
{
  "id": "{page_id}",
  "Date": {Date}
}
```

## Security Considerations

1. **Always use HTTPS** - Notion sends sensitive data
2. **Keep `WEBHOOK_SECRET_KEY` secret** - Rotate if compromised
3. **Restrict Notion token scope** - Only grant access to required databases
4. **Monitor logs** - Watch for unauthorized access attempts
5. **Rate limiting** - Consider adding rate limiting for production

## Health Checks

All deployment options should use the health endpoint for monitoring:

```bash
curl https://nhook.yourdomain.com/health
# {"status": "ok"}
```

## Environment Variables

Create a `.env` file (never commit to git):

```env
WEBHOOK_SECRET_KEY=your-strong-secret-key-here
NOTION_API_TOKEN=secret_xxxxx
CRONOGRAMA_DATABASE_ID=2f5f6e7f-0572-80e3-a411-000be22f385d
GASTOS_DATABASE_ID=2e2f6e7f-0572-8010-9fb8-000b7db49de1
HOST=0.0.0.0
PORT=8000
DEBUG=false
```

## Comparison of Options

| Option | Best For | SSL | Cost | Persistence |
|--------|----------|-----|------|-------------|
| Cloudflare Tunnel | Home server, always-on | Auto | Free | Yes |
| Tailscale Funnel | Tailscale users | Auto | Free | Yes |
| ngrok | Quick testing | Auto | Free (limited) | No |
| VPS + Nginx | Production | Manual/Let's Encrypt | $5+/mo | Yes |
| Fly.io | Serverless production | Auto | Free tier | Yes |
