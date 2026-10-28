#!/data/data/com.termux/files/usr/bin/sh
# Держит сборщик запущенным и применяет обновления, которые agg.py скачал и проверил в next/.
# Если новая версия трижды подряд падает в первые 2 минуты — откат на prev/ и версия помечается плохой.
cd ~/iptv-agg || exit 1
fails=0
while true; do
    if [ -f next/version ]; then
        rm -rf prev && mkdir prev
        for f in agg.py channels.py config.json run.sh version; do [ -f $f ] && cp $f prev/; done
        # через временный файл + mv: работающий sh продолжает читать старый run.sh, а не переписанный на ходу
        for f in next/*; do cp "$f" "$f.tmp" && mv "$f.tmp" "./$(basename $f)"; done
        rm -rf next
        chmod +x run.sh
        echo "$(date '+%m-%d %H:%M:%S') обновлено до версии $(cat version)" >> agg.log
        exec sh ~/iptv-agg/run.sh
    fi
    [ -f agg.log ] && [ $(wc -c < agg.log) -gt 1000000 ] && tail -c 200000 agg.log > agg.log.1 && mv agg.log.1 agg.log
    start=$(date +%s)
    python agg.py config.json >> agg.log 2>&1
    if [ $(( $(date +%s) - start )) -lt 120 ]; then fails=$((fails + 1)); else fails=0; fi
    if [ $fails -ge 3 ] && [ -d prev ]; then
        cp version bad_version 2>/dev/null
        for f in prev/*; do cp "$f" "$f.tmp" && mv "$f.tmp" "./$(basename $f)"; done
        rm -rf prev
        echo "$(date '+%m-%d %H:%M:%S') откат: версия $(cat bad_version) падает" >> agg.log
        fails=0
        exec sh ~/iptv-agg/run.sh
    fi
    cp agg.log /sdcard/iptv-agg/agg.log 2>/dev/null
    sleep 10
done
