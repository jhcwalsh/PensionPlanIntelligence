# Mac Mini Hosting Runbook

**Status: built and verified 30 August 2026. Reboot test passed.**

The Mini serves containerised Python apps at subdomains of
lazyeconomist.com via Cloudflare Tunnel, and recovers unattended after a
restart — confirmed by a cold reboot with nobody at the machine.

This document is now a reference for running and extending the setup,
not a build guide.

---

## As built

| | |
|---|---|
| Host | Mac Mini, Apple Silicon (arm64), 460GB disk |
| Hostname | `JHCW-mini.local` (10.0.0.110) |
| User | `jameswalsh` |
| Network | Ethernet, 950/350 Mbps |
| Runtime | OrbStack (Docker only — no Kubernetes, no Linux VM) |
| Tunnel | Cloudflare, **dashboard-managed** |
| Team domain | `white-heart-9f2e.cloudflareaccess.com` |
| Apps directory | `~/apps/<name>/` |
| Dev machine | Windows + PyCharm, deploys over SSH |

### Decisions made during the build

- **Coolify skipped.** It needs a Linux VM on macOS. Cloudflare Tunnel
  routes straight to container ports, so no reverse proxy or PaaS layer is
  needed. Revisit only if manual deploys become tedious across many apps.
- **Dashboard-managed tunnel**, not a local `config.yml`. Routing config
  lives in Cloudflare rather than in git; the upside is adding hostnames
  from a phone. Do not mix the two approaches.
- **FileVault off**, auto-login on. Required for unattended reboot
  recovery. Revisit if client-confidential material ever lands on the disk.
- **No DHCP reservation.** The Xfinity gateway wouldn't allow it. Doesn't
  matter — the tunnel dials outbound and `JHCW-mini.local` resolves over
  Bonjour. Tailscale is the fix if a stable address is ever wanted.

### Host settings applied

```
pmset -a sleep 0 disksleep 0 displaysleep 10
pmset -a autorestart 1
pmset -a womp 1
systemsetup -setcomputersleep Never
systemsetup -setremotelogin on
```

Auto-login enabled in System Settings → Users & Groups.
OrbStack set to start at login.

---

## Adding a new app

Steps 1–2 are per app forever; steps 3–4 are one-time per app.

### 1. In PyCharm — three files in the project root

`Dockerfile`:

```dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["streamlit", "run", "app.py", "--server.port=8502", "--server.address=0.0.0.0", "--server.headless=true"]
```

`docker-compose.yml`:

```yaml
services:
  myapp:
    build: .
    ports:
      - "8502:8502"
    restart: unless-stopped
    env_file:
      - .env      # omit if the app needs no secrets
```

`.dockerignore`:

```
.venv
__pycache__
.git
.env
```

`restart: unless-stopped` is what makes reboot recovery work. Don't omit it.

Check `requirements.txt` is current and `.env` is gitignored, then push.

### 2. On the Mini — first deployment

```bash
mkdir -p ~/apps/myapp && cd ~/apps/myapp
git clone git@github.com:jhcwalsh/REPO.git .
# create ~/apps/myapp/.env by hand if the app needs secrets
docker compose up -d --build
docker compose logs -f
curl -sI http://localhost:8502 | head -1
```

### 3. Public hostname (dashboard)

Zero Trust → Networks → Tunnels & Mesh → the tunnel →
**Published application routes** → Add:

- Subdomain: `myapp`
- Domain: `lazyeconomist.com`
- Path: empty
- Type: **HTTP** (not HTTPS — the hop to the container is local)
- URL: `localhost:8502` (no scheme prefix)

DNS is written automatically.

### 4. Access policy (dashboard) — skip only if the app should be public

Access controls → Applications → Create new application → Self-hosted →
**Public DNS** → destination `myapp` + `lazyeconomist.com`.

Then create the policy **from inside the application flow**, not from the
Policies page. Set **both** the selector and the value — see gotchas below.

### Port allocation

| Port | App |
|------|------------------|
| 8501 | test (teardown pending) |
| 8502 | — |
| 8503 | — |
| 8504 | — |

---

## Updating an app

Push from PyCharm, then from Windows PowerShell:

```powershell
deploy myapp
```

Where `deploy` is in `$PROFILE`:

```powershell
function deploy {
    param($app)
    ssh jameswalsh@JHCW-mini.local "cd ~/apps/$app && /usr/bin/git pull && /usr/local/bin/docker compose up -d --build"
}
```

**Absolute paths are deliberate.** Non-interactive SSH sessions don't
source `.zprofile`, so `docker` isn't on PATH and a bare command fails with
`command not found`. This bites scripted deploys but not interactive
sessions, which is why it's easy to miss.

---

## Gotchas found the hard way

**Access policies save with an empty rule.** Creating a policy from the
Policies page and leaving the selector blank produces a policy that saves
without complaint and silently denies everyone. The symptom is "That
account does not have access" that no amount of email-matching fixes.
Create policies from inside the application flow, where the form validates.

**The application form's Preview panel lies.** It showed "No destinations
assigned" for a destination that had in fact registered. Trust the saved
application, not the preview.

**Safari shares one private session** across all private windows. Opening
another private window does not give a clean auth state — close them all
first.

**Team domain name is cosmetic.** `white-heart-9f2e` vs a custom name has
no bearing on whether authentication works.

**The Xfinity gateway blocks local admin.** `10.0.0.1` serves an Xfinity
portal rather than a router UI, so no DHCP reservations. Not worth fighting.

---

## Reboot recovery chain

Verified working. If the site is ever down after a restart, check in this
order — each step depends on the one before it:

1. `ssh jameswalsh@JHCW-mini.local` — did it boot?
2. `who` — did auto-login happen? (nothing runs without a user session)
3. `docker ps` — did OrbStack start, and did containers restart?
4. `sudo launchctl list | grep cloudflared` — is the tunnel daemon up?
5. `tail -50 /Library/Logs/com.cloudflare.cloudflared.err.log`

---

## Outstanding

- **Backups.** Nothing yet. Time Machine to an external drive, and a
  nightly `pg_dump` to R2 or B2 before any real database lands here. Time
  Machine snapshots of a running database are not reliably restorable.
- **Tear down the test app.** `cd ~/apps/test && docker compose down`, then
  delete the `test` hostname and its Access application, freeing port 8501.
- **Tailscale**, if remote SSH from outside the house is ever wanted.
- **Team domain rename**, cosmetic, before anything client-facing.
