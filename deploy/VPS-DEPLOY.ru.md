# Развёртывание fedarisha-сервера на VPS

Инструкция для нашего форка движка туннеля
([репозиторий](https://github.com/thevladimirzh/s3-bypass-protocol)).
Сервер — это «мозг» туннеля: он принимает сессии, а трафик идёт напрямую
между клиентом и S3-хранилищем. Поэтому сервер stateless и не хранит данных
пользователей.

Зачем свой сервер: в upstream-версии writer бросал файл после трёх попыток
загрузки, а читатель идёт строго по порядку и не может перескочить
пропущенный объект — одна потерянная запись «подвешивала» туннель на
минуты. Наша версия не бросает файлы. Проверено: 600 МБ шестью параллельными
потоками, ноль обрывов.

---

## Что понадобится

1. **VPS** с Debian/Ubuntu. Нужен доступ по SSH и `sudo`. Одного ядра и
   512 МБ RAM достаточно — сервер почти не считает, он ходит в S3 по API.
2. **Бакет** в S3-совместимом хранилище (подойдёт любой: S3, Yandex Object
   Storage, VK Cloud, Selectel и т.п.). Нужны: имя бакета, endpoint,
   регион и пара ключей (access/secret) с правами на этот бакет.
3. **Отдельный префикс** в бакете для этого развёртывания, например
   `fedarisha/my-server/`. Не используйте префикс, где работает upstream:
   оба сервера следят за своим префиксом, и кто первый запишет ACK сессии —
   тот и заберёт клиента.

**Про закрытый порт.** Открывать порт наружу не нужно: клиент общается с
сервером только через бакет (запись hello-файла и чтение ACK). В конфиге
сервера стоит `listen: 127.0.0.1` — это нормально, так и оставляем.

---

## Шаг 1. Установка

```bash
VERSION=v0.1.0-fork.3
SHA256=72c6005321446cc1ab4fe60f85e20576bfe8907ed36fdbc53e272868b94454e1

curl -fLO "https://github.com/thevladimirzh/s3-bypass-protocol/releases/download/${VERSION}/Xray-linux-64.zip"
echo "${SHA256}  Xray-linux-64.zip" | sha256sum -c -   # должен пройти
```

Если не совпало — **не продолжайте**, скачайте заново.

Дальше установщик из репозитория:

```bash
git clone https://github.com/thevladimirzh/s3-bypass-protocol.git
cd s3-bypass-protocol
VERSION=v0.1.0-fork.3 SHA256=72c6005321446cc1ab4fe60f85e20576bfe8907ed36fdbc53e272868b94454e1 \
  sudo -E ./deploy/install-server.sh
```

Что он делает: создаёт системного пользователя `s3bypass`, кладёт бинарь в
`/opt/s3bypass-protocol/xray`, ставит systemd-юнит с ограничениями
(no-new-privileges, read-only FS, только журнал). Конфиг он **не создаёт** —
чтобы переустановка не затерела рабочие ключи.

---

## Шаг 2. Конфигурация сервера

`/etc/s3bypass-protocol/server.json`, владелец `root:s3bypass`, права `0640`:

```json
{
  "log": { "loglevel": "info" },
  "inbounds": [
    {
      "tag": "fedarisha-in",
      "listen": "127.0.0.1",
      "port": 8443,
      "protocol": "fedarisha",
      "settings": {
        "storage": {
          "type": "s3",
          "bucket": "my-bucket",
          "endpoint": "storage.yandexcloud.net",
          "region": "ru-central1",
          "accessKey": "ВСТАВЬТЕ_КЛЮЧ",
          "secretKey": "ВСТАВЬТЕ_СЕКРЕТ",
          "prefix": "fedarisha/my-server/",
          "sessionsDir": "sessions"
        },
        "clients": [
          { "id": "vasya", "level": 1 },
          { "id": "petya", "level": 1 }
        ],
        "userLevel": 1
      }
    }
  ],
  "outbounds": [
    { "tag": "direct", "protocol": "freedom", "settings": {} }
  ]
}
```

- `id` в `clients` — **логины пользователей**. Кто неlisted, того сервер
  отвергнет (в журнале будет `rejected: user "..." not allowed`).
- `prefix` обязан заканчиваться на `/`.
- Права: `sudo chown root:s3bypass server.json && sudo chmod 640 server.json`.

---

## Шаг 3. Запуск и проверка

```bash
sudo systemctl enable --now s3bypass-protocol
systemctl status s3bypass-protocol
journalctl -u s3bypass-protocol -f
```

При старте в журнале должно быть:

```
[fedarisha] inbound "fedarisha-in": configuring S3 bucket lifecycle (prefix: fedarisha/my-server/, expire: 1d)
[Warning] core: Xray 26.9.30 started
```

Если этого нет — смотрите раздел «Если не работает».

### Быстрая проверка, что установка сошлась

```bash
systemctl is-active s3bypass-protocol            # ожидается: active
sudo /opt/s3bypass-protocol/xray run -c /etc/s3bypass-protocol/server.json --help >/dev/null 2>&1; echo $?
sudo ss -ltnp | grep 8443                        # порт занят на 127.0.0.1
```

Если `systemctl is-active` даёт `failed`, почти всегда виноваты пути:
юнит ожидает ровно `/opt/s3bypass-protocol` и `/etc/s3bypass-protocol`.

---

## Шаг 4. Конфиг клиента

У пользователя в клиентском конфиге блок `outbounds` с `protocol:
"fedarisha"` должен указывать на **тот же бакет, ключи и префикс** и на
**ваш логин как директорию**:

```json
{
  "protocol": "fedarisha",
  "settings": {
    "storage": {
      "type": "s3",
      "bucket": "my-bucket",
      "endpoint": "storage.yandexcloud.net",
      "region": "ru-central1",
      "accessKey": "...",
      "secretKey": "...",
      "prefix": "fedarisha/my-server/",
      "sessionsDir": "vasya/sessions"
    }
  }
}
```

**Главная ловушка, на которую ушла у нас час:** сервер всегда работает в
режиме «много пользователей» — он обходит `<логин>/sessions` внутри
префикса. Если клиент пишет просто в `sessions/`, сервер сессию **никогда не
увидит**, а клиент будет молча ждать ответа. Поэтому `sessionsDir` клиента
обязательно начинается с логина, а в `clients` сервера этот логин есть.

Готовый клиентский конфиг можно собрать из существующего скриптом в
репозитории (`scripts/build-isolated-pair.mjs`) — он ничего не печатает в
терминал, кроме имён ключей.

---

## Что смотреть в журнале

| Строка | Что значит |
|---|---|
| `new session XXXX in vasya/sessions/XXXX` | handshake прошёл, всё в порядке |
| `upload retry N for ...` | **норма** под нагрузкой: файл не загрузился с первого раза и ушёл в повтор. Это гарантия доставки в действии |
| `upload ERR ...` | файл потерян, сессия «залипнет» — ищите причину в доступе к бакету или в сети |
| `no ACK for session ... waiting longer` (в клиенте) | сервер не подхватил сессию: проверьте префикс, `clients` и `sessionsDir` |
| `hole at seq N ...` (в клиенте) | писатель не отдал файл. У нас это должно вылечиться ретраями; если повторяется — смотрите `upload ERR` на сервере |
| `idle timeout` | клиент молчал дольше `idleTimeoutSec` |

---

## Если не работает

**Сессии не появляются, клиент пишет `no ACK`.** Самая частая причина —
расхождение префикса или логина. Проверьте по порядку:
1. у клиента `sessionsDir` начинается с логина из `clients` сервера;
2. `prefix` совпадает посимвольно и заканчивается на `/`;
3. логин есть в `clients` (регистр важен);
4. у ключей есть права на чтение, запись и удаление в этом префиксе.

Проверить, что сервер вообще видит бакет:
```bash
sudo -u s3bypass journalctl -u s3bypass-protocol -n 50 --no-pager
```

**Ошибка доступа к бакету.** Проверьте endpoint и регион: для Yandex
Object Storage `region: ru-central1`, для VK Cloud `region: eu-east1`,
для AWS — регион бакета вида `eu-central-1`. Частая ошибка — регион
сервера вместо региона бакета.

**Порт занят.** Сервер слушает только `127.0.0.1:8443`. Проверьте:
`sudo ss -ltnp | grep 8443`.

**Не стартует.** `sudo journalctl -u s3bypass-protocol -n 100 --no-pager`
и `sudo /opt/s3bypass-protocol/xray run -c /etc/s3bypass-protocol/server.json`
запустит его на переднем плане и покажет ошибку.

---

## Обновление

```bash
VERSION=... SHA256=... sudo -E ./deploy/install-server.sh
sudo systemctl restart s3bypass-protocol
```

Конфиг при обновлении не трогается. Прежде чем раздавать обновление
пользователям, прогоните нагрузочный тест: шесть параллельных загрузок по
50 МБ должны пройти целиком.

---

## Безопасность

- Ключи в конфиге — это доступ к бакету. Права `640`, владелец
  `root:s3bypass`, никаких копий в репозитории.
- Заведите отдельного пользователя в бакете (или хотя бы отдельный
  префикс) под этого сервера.
- Ничего открывать в firewall не нужно: наружу сервер не слушает.
- `systemd`-юнит работает от непривилегированного `s3bypass` с
  `NoNewPrivileges`, `ProtectSystem=strict` и `PrivateTmp`.

## Лицензия

MPL-2.0. Форк `Fedarisha/Xray-core-fedarisha`, который сам форк
XTLS/Xray-core. Текст лицензии и атрибуция — в `LICENSE` и `README.md`
репозитория.