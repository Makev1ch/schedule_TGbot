set -euo pipefail

APP_DIR="${1:-/home/$USER/schedule-bot}"
SERVICE_NAME="${2:-schedule-bot}"

sudo apt update
sudo apt install -y python3 python3-venv python3-pip

cd "$APP_DIR"
python3 -m venv .venv
./.venv/bin/python -m pip install -r requirements.txt

if [[ ! -f /etc/schedule-bot.env ]]; then
  echo "Создай /etc/schedule-bot.env (BOT_TOKEN=..., ADMIN_USER_ID=...) и повтори запуск."
  exit 1
fi

sudo cp "$APP_DIR/deploy/schedule-bot.service" "/etc/systemd/system/$SERVICE_NAME.service"
sudo systemctl daemon-reload
sudo systemctl enable --now "$SERVICE_NAME.service"
sudo systemctl status "$SERVICE_NAME.service" --no-pager
