# Как опубликовать `wb-orders` на GitHub с Windows 11

Целевой репозиторий: <https://github.com/eztwokey/wb-orders>

## 1. Подготовка

Установите Git for Windows: <https://git-scm.com/download/win>. Затем откройте новое окно PowerShell:

```powershell
git --version
```

Один раз настройте автора commits:

```powershell
git config --global user.name "eztwokey"
git config --global user.email "EMAIL_ИЗ_НАСТРОЕК_GITHUB"
```

Можно использовать GitHub `noreply` email из `GitHub → Settings → Emails`, если не хотите публиковать основной адрес.

## 2. Откройте правильный каталог

Распакуйте архив, например в `C:\dev\wb-orders`, и перейдите в каталог с `go.mod`:

```powershell
cd C:\dev\wb-orders
Get-ChildItem
```

На этом уровне должны находиться `README.md`, `go.mod`, `docker-compose.yml`, `cmd`, `internal` и `migrations`. Не создавайте репозиторий уровнем выше, иначе на GitHub появится лишняя вложенная папка.

## 3. Проверьте секреты

Не создавайте настоящий `.env` до первого commit. `.env` исключен через `.gitignore`, но проверка все равно обязательна.

В проекте допустим `deploy/k8s/secret.example`: это шаблон без настоящего пароля. Не добавляйте:

- Personal Access Token;
- настоящий `DATABASE_URL`;
- kubeconfig;
- Kubernetes Secret с реальными credentials;
- `.env`;
- содержимое password manager.

## 4. Первый commit

```powershell
git init
git branch -M main
git add .
git status
git diff --cached --stat
git commit -m "feat: publish wb-orders portfolio project"
```

Перед `git commit` внимательно посмотрите `git status`. В commit должны войти source code, migrations, tests, Docker/Kubernetes manifests, documentation и CI configuration.

## 5. Подключение GitHub и push

```powershell
git remote add origin https://github.com/eztwokey/wb-orders.git
git remote -v
git push -u origin main
```

При первой отправке Git Credential Manager обычно открывает браузер для входа в GitHub. Пароль GitHub в PowerShell вводить не нужно.

После успешной команды откройте:

<https://github.com/eztwokey/wb-orders>

## 6. Если `origin already exists`

Проверьте текущий адрес:

```powershell
git remote -v
```

Если он неправильный:

```powershell
git remote set-url origin https://github.com/eztwokey/wb-orders.git
git push -u origin main
```

## 7. Если push отклонен из-за существующего README

Это означает, что при создании GitHub repository был автоматически добавлен commit. Не используйте `push --force`, не посмотрев содержимое remote.

Сначала получите remote history:

```powershell
git fetch origin
git log --oneline --all --decorate -10
```

Если в remote находится только автоматически созданный README/License и ничего ценного, объедините истории:

```powershell
git pull origin main --allow-unrelated-histories --no-rebase
```

Если Git сообщит о конфликте `README.md`, оставьте содержимое README из подготовленного проекта:

```powershell
git checkout --ours README.md
git add README.md
git commit -m "chore: merge initial GitHub repository"
git push -u origin main
```

`--ours` здесь безопасен только если вы действительно хотите оставить локальный portfolio README.

## 8. Что настроить на странице GitHub

Откройте repository, нажмите значок шестеренки рядом с `About` и заполните:

**Description:**

```text
Event-driven backend обработки заказов на Go: PostgreSQL, Kafka, Transactional Outbox, идемпотентность и ограниченная конкурентность.
```

**Website:** оставить пустым, пока нет deployment.

**Topics:**

```text
golang
postgresql
apache-kafka
event-driven
transactional-outbox
idempotency
worker-pool
kubernetes
gitlab-ci
```

Проверьте, что repository имеет видимость `Public` в `Settings → General → Danger Zone → Change repository visibility`.

## 9. Что увидит проверяющий

На первой странице должны быть видны:

- понятное назначение проекта;
- architecture diagram;
- честное описание at-least-once semantics;
- инструкции локального запуска;
- failure scenarios;
- tests и race detector;
- ADR;
- Docker, Kubernetes и GitLab CI/CD;
- disclaimer, что проект не является официальным сервисом Wildberries.

`.gitlab-ci.yml` оставлен осознанно. GitHub является публичной витриной, а pipeline предназначен для импорта или mirror repository в GitLab. Не добавляйте GitHub Actions только ради зеленой галочки, если вы хотите демонстрировать именно GitLab CI/CD.

## 10. Следующие изменения после первого push

Не складывайте все будущие изменения в один commit. Пример нормального рабочего цикла:

```powershell
git status
git add ПУТЬ_К_ИЗМЕНЕННЫМ_ФАЙЛАМ
git diff --cached
git commit -m "docs: clarify outbox failure scenarios"
git push
```

Хорошие commit prefixes для этого проекта:

- `feat:` — новая возможность;
- `fix:` — исправление;
- `test:` — тесты;
- `docs:` — документация;
- `refactor:` — изменение структуры без смены поведения;
- `build:` — Docker, CI и сборка;
- `chore:` — техническое обслуживание.

Не создавайте искусственную историю задним числом. Один честный initial commit и последующие осмысленные изменения выглядят лучше набора фиктивных commits.
