#!/bin/bash
cat << 'EOF'

  ╔══════════════════════════════════════════════════════════════╗
  ║              SCOUTLINE — BettingAssistantBG                  ║
  ╚══════════════════════════════════════════════════════════════╝

  ── DEPLOY ────────────────────────────────────────────────────
  cd ~/betting_assistant
  git pull                          # pull latest changes
  docker compose up --build -d      # rebuild & restart
  docker compose down               # stop the app

  ── LOGS & STATUS ─────────────────────────────────────────────
  docker ps                         # running containers
  docker logs -f betting_assistant-scoutline-1
  docker logs --tail 50 betting_assistant-scoutline-1

  ── USERS ─────────────────────────────────────────────────────
  docker exec -it betting_assistant-scoutline-1 \
    python3 manage_users.py list
  docker exec -it betting_assistant-scoutline-1 \
    python3 manage_users.py create <username>
  docker exec -it betting_assistant-scoutline-1 \
    python3 manage_users.py passwd <username>
  docker exec -it betting_assistant-scoutline-1 \
    python3 manage_users.py delete <username>

  ── GIT ───────────────────────────────────────────────────────
  git status                        # what changed
  git log --oneline -10             # recent commits
  git pull                          # pull from GitHub
  git diff                          # see unstaged changes

  ── FIREWALL ──────────────────────────────────────────────────
  ufw status                        # current rules
  ufw allow <port>/tcp              # open a port
  ufw deny <port>/tcp               # close a port

  ── SYSTEM ────────────────────────────────────────────────────
  df -h                             # disk usage
  free -h                           # memory usage
  htop                              # live resource monitor

  APP → http://178.104.200.107:8081/scoutline.html

EOF
