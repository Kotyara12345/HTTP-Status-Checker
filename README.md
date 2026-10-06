# Описание программы

## Назначение

**HTTP Status & Redirect Checker (Windows Pro)** — это GUI-приложение на Python/Tkinter для массовой проверки HTTP-статусов и цепочек редиректов URL-адресов. Ориентировано на SEO-специалистов, вебмастеров и администраторов, которым нужно быстро оценить доступность и поведение большого списка страниц (до 500 за раз).

## Основные возможности

### 1. Ввод и нормализация URL
- Поле ввода до 500 URL (по одному в строке).
- Функция `build_candidate_urls()` автоматически достраивает протокол: если строка введена без схемы, генерируется очередь кандидатов (`https://` → `http://`, с `www.` и без него).
- Поддержка IPv6, IDN-домены (кириллица через `idna`), безопасное экранирование не-ASCII символов через `ensure_ascii_url()`.
- Опциональная дедупликация (по хосту + пути, игнорируя `www.` и стандартные порты).

### 2. Движок проверки (asyncio + httpx)
- Полностью асинхронный движок, работающий в отдельном потоке с event loop (`AsyncRunner`).
- **Проверка цепочки редиректов** до 15 хопов (`MAX_REDIRECTS`), ручное следование без автопереходов — фиксируется каждый хоп с URL, статусом и временем.
- Защита от петель редиректов через нормализацию и множество посещённых URL.
- **Per-host rate limiter** (`AsyncPerHostRateLimiter`): гарантированная пауза между запросами к одному хосту с добавлением случайного джиттера до 10% от задержки.
- Семафор для ограничения параллелизма.

### 3. HTTP-запросы
- Методы **GET** и **HEAD** (при HEAD с статусами 403/405/501 — автоматический fallback на GET).
- Настройка `Connect Timeout`, `Read Timeout`, задержки на хост, уровня параллелизма.
- Опционально: HTTP/2, игнорирование ошибок SSL (по умолчанию включено).
- Извлечение SSL-ошибок из цепочки исключений (`extract_ssl_cause`).
- Частичная загрузка тела через `Range: bytes=0-1024` + дренирование первого чанка — экономит трафик.

### 4. User-Agent
- 12 готовых пресетов: Яндекс.Бот, Googlebot (desktop/smartphone), Bingbot, DuckDuckBot, Chrome, Firefox и др.
- Возможность ввести свой UA вручную.

### 5. GUI (Tkinter)
- Таблица результатов (`ttk.Treeview`) с цветовой маркировкой: 2xx — зелёный, 3xx — жёлтый, 4xx — оранжевый, ошибки — красный.
- Прогресс-бар и статусная строка.
- **Двойной клик по строке** открывает окно `ChainDetailWindow` с детальной цепочкой хопов.
- Предупреждение при нагрузке на один хост (порог 50 совпадений).
- Асинхронное заполнение таблицы чанками (`TREE_INSERT_CHUNK = 100`) — UI не «зависает».
- Поллинг очереди результатов каждые 30 мс.

### 6. Экспорт CSV
- Диалог выбора разделителя (`;`, `,`, табуляция), кодировки (`utf-8-sig`, `utf-8`, `cp1251`).
- Фильтр «только ошибки (4xx/5xx/ERR)».
- Защита от CSV-инъекций (экранирование строк, начинающихся с `=`, `+`, `-`, `@`).

### 7. Завершение и остановка
- Кнопка «Стоп» — корректная отмена задач через `task.cancel()`.
- При закрытии окна — `shutdown()` с таймаутом ожидания `CLOSE_GRACE_SECONDS = 3.0`.

---

# Сборка портативного EXE

## 1. Требования

- **Python 3.11+** (для `asyncio.TaskGroup`; на 3.9–3.10 работает fallback через `gather`).
- Windows 10/11 x64.
- Установленный pip.

## 2. Установка зависимостей

Откройте PowerShell/cmd и выполните:

```bat
python -m pip install --upgrade pip
pip install httpx h2 pyinstaller
```

- `httpx` — HTTP-клиент (обязательно).
- `h2` — поддержка HTTP/2 (опционально, но лучше поставить — флаг `HTTP2_AVAILABLE` станет `True`).
- `pyinstaller` — сборщик.

> Проверьте версию: `python --version` должна быть ≥ 3.9, желательно ≥ 3.11.

## 3. Подготовка файла

Сохраните исходник как `checker.py` в отдельной папке, например `C:\build\`.

## 4. Сборка одной командой

### Вариант A — простой one-file EXE

```bat
python -m PyInstaller --noconfirm --clean --onefile --windowed ^
  --name "HTTP_Status_Checker" ^
  --collect-all httpx ^
  --collect-all h2 ^
  checker.py
```

### Вариант B — рекомендуемый (быстрее стартует, стабильнее)

Собираем в режиме **onedir** (папка с exe + DLL) — запуск происходит мгновенно, а не через 3–5 секунд распаковки во временную папку.

```bat
python -m PyInstaller --noconfirm --clean --onedir --windowed ^
  --name "HTTP_Status_Checker" ^
  --collect-all httpx ^
  --collect-all h2 ^
  --hidden-import certifi ^
  --hidden-import h2 ^
  --hidden-import hpack ^
  --hidden-import hyperframe ^
  checker.py
```

### Пояснения к ключам

| Ключ | Назначение |
|---|---|
| `--onefile` | Один exe-файл (всё внутри). Долгий старт. |
| `--onedir` | Папка `dist\HTTP_Status_Checker\` с exe и зависимостями. Быстрый старт. |
| `--windowed` / `--noconsole` | Без консольного окна (для GUI). |
| `--name` | Имя итогового файла/папки. |
| `--collect-all httpx` | Собирает все подмодули и данные httpx (важно — иначе бывает `ModuleNotFoundError`). |
| `--collect-all h2` | Подтягивает HTTP/2-зависимости. |
| `--hidden-import certifi` | Сертификаты для HTTPS (используются httpx по умолчанию). |
| `--clean` | Очищает кэш сборки. |
| `--noconfirm` | Без запросов подтверждения. |

## 5. Результат

- При `--onefile`: `dist\HTTP_Status_Checker.exe` — один файл ~30–40 МБ.
- При `--onedir`: `dist\HTTP_Status_Checker\` — папка с exe и подкаталогами. Для переноски на другой ПК достаточно скопировать всю папку целиком.

## 6. Файл `.spec` (для повторных сборок)

После первого запуска PyInstaller создаст `HTTP_Status_Checker.spec`. Дальнейшие сборки можно делать так:

```bat
pyinstaller --noconfirm HTTP_Status_Checker.spec
```

При необходимости отредактируйте `hiddenimports` в spec-файле:

```python
hiddenimports=[
    'httpx', 'h2', 'hpack', 'hyperframe', 'certifi', 'idna',
],
```

## 7. Устранение возможных проблем

| Проблема | Решение |
|---|---|
| `ModuleNotFoundError: No module named 'httpx._...'` | Добавить `--collect-all httpx` и `--collect-submodules httpx`. |
| Не работают HTTPS-запросы | Добавить `--add-data "<path-to-certifi>\cacert.pem;certifi"` или `--collect-data certifi`. |
| `HTTP/2` недоступен в чекбоксе | Убедиться, что `h2` установлен **до** сборки, и добавить `--hidden-import h2`. |
| Антивирус ругается на onefile-EXE | Использовать `--onedir`, либо подписать exe сертификатом. |
| Окно не появляется (мгновенный выход) | Собрать без `--windowed`, запустить exe из консоли и посмотреть трейсбек. |
| Долгий старт onefile | Перейти на `--onedir`. |

## 8. Автоматизация сборки (опционально)

Создайте `build.bat` рядом с `checker.py`:

```bat
@echo off
setlocal
python -m pip install --upgrade pip
pip install httpx h2 pyinstaller
pyinstaller --noconfirm --clean --onedir --windowed ^
  --name "HTTP_Status_Checker" ^
  --collect-all httpx ^
  --collect-all h2 ^
  --hidden-import certifi ^
  --hidden-import h2 ^
  checker.py
echo.
echo Готово: dist\HTTP_Status_Checker\HTTP_Status_Checker.exe
pause
```

## 9. Проверка портативности

Чтобы убедиться, что exe действительно портативный:
1. Скопируйте папку (или exe) на чистую Windows-машину без Python.
2. Запустите, введите пару URL (например `example.com`, `google.com`).
3. Проверьте: результаты появляются, экспорт CSV работает, HTTP/2-чекбокс активен.

Готово — у вас на руках автономный инструмент, не требующий установки Python у конечного пользователя.
