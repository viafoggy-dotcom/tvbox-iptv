#!/data/data/com.termux/files/usr/bin/sh
# Установка на приставку (Android + Termux). В Termux:
#   curl -sL https://raw.githubusercontent.com/viafoggy-dotcom/tvbox-iptv/main/tools/setup.sh | sh
# Ход установки пишется в ~/iptv-agg/setup.log (и копия в /sdcard/iptv-agg/, если есть доступ к памяти).
BASE=https://raw.githubusercontent.com/viafoggy-dotcom/tvbox-iptv/main/box
mkdir -p ~/iptv-agg ~/.termux/boot
LOG=~/iptv-agg/setup.log
exec >"$LOG" 2>&1
set -x
export DEBIAN_FRONTEND=noninteractive
# без рекомендуемых: иначе python тянет clang/llvm (~470 МБ) — на приставке 3 ГБ всего
APT="apt-get -y --no-install-recommends -o Dpkg::Options::=--force-confnew"
$APT update && $APT upgrade && $APT install python || { echo "SETUP FAILED"; exit 1; }
apt-get clean

cd ~/iptv-agg
for f in agg.py channels.py config.json run.sh manifest.json; do
    curl -sfL "$BASE/$f" -o "$f.tmp" && mv "$f.tmp" "$f" || { echo "SETUP FAILED: $f"; exit 1; }
done
python -c 'import json; print(json.load(open("manifest.json"))["version"])' > version
python agg.py config.json --selftest || { echo "SETUP FAILED: selftest"; exit 1; }
chmod +x run.sh
# exec, не «nohup … &»: Termux:Boot убивает фоновые процессы, когда скрипт завершается
cat > ~/.termux/boot/iptv-agg <<'EOF'
#!/data/data/com.termux/files/usr/bin/sh
termux-wake-lock
exec ~/iptv-agg/run.sh >/dev/null 2>&1
EOF
chmod +x ~/.termux/boot/iptv-agg
pkill -f iptv-agg/run.sh; pkill -f "agg.py config.json"
nohup sh ~/.termux/boot/iptv-agg >/dev/null 2>&1 &
echo "SETUP OK"
mkdir -p /sdcard/iptv-agg 2>/dev/null && cp "$LOG" /sdcard/iptv-agg/setup.log 2>/dev/null
true
