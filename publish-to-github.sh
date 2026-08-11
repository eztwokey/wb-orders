#!/usr/bin/env bash

set -Eeuo pipefail

readonly DEFAULT_GITHUB_USER="eztwokey"
readonly DEFAULT_REPOSITORY="wb-orders"
readonly DEFAULT_COMMIT_MESSAGE="feat: initial wb-orders implementation"

die() {
    printf '\nОшибка: %s\n' "$1" >&2
    exit 1
}

require_command() {
    command -v "$1" >/dev/null 2>&1 || die "не найдена команда '$1'. Установите Git for Windows и запустите скрипт из Git Bash."
}

prompt_with_default() {
    local prompt="$1"
    local default_value="$2"
    local result

    read -r -p "$prompt [$default_value]: " result
    printf '%s' "${result:-$default_value}"
}

confirm() {
    local answer
    read -r -p "$1 [y/N]: " answer
    [[ "$answer" == "y" || "$answer" == "Y" ]]
}

require_command git

printf '%s\n' \
    "============================================================" \
    " Публикация wb-orders в GitHub" \
    "============================================================" \
    "Скрипт не запрашивает и не сохраняет пароль или GitHub token." \
    "Авторизация выполняется Git Credential Manager через браузер." \
    ""

current_directory="$(pwd)"
project_directory="$(prompt_with_default "Путь к каталогу проекта" "$current_directory")"

# Git Bash принимает пути D:/project. Пользовательский Windows-путь
# с обратными слешами преобразуется в тот же переносимый формат.
project_directory="${project_directory//\\//}"

[[ -d "$project_directory" ]] || die "каталог '$project_directory' не существует."
cd "$project_directory"

[[ -f "go.mod" ]] || die "в '$project_directory' не найден go.mod. Откройте каталог, где находятся go.mod и README.md."
[[ -f "README.md" ]] || die "в '$project_directory' не найден README.md."

github_user="$(prompt_with_default "GitHub username" "$DEFAULT_GITHUB_USER")"
repository="$(prompt_with_default "Название GitHub-репозитория" "$DEFAULT_REPOSITORY")"

default_author_name="$(git config --global user.name 2>/dev/null || true)"
default_author_name="${default_author_name:-$github_user}"
author_name="$(prompt_with_default "Имя автора commit" "$default_author_name")"

default_author_email="$(git config --global user.email 2>/dev/null || true)"
if [[ -n "$default_author_email" ]]; then
    author_email="$(prompt_with_default "Email автора commit" "$default_author_email")"
else
    read -r -p "Email автора commit: " author_email
fi
[[ -n "$author_email" ]] || die "email автора не может быть пустым."

commit_message="$(prompt_with_default "Сообщение commit" "$DEFAULT_COMMIT_MESSAGE")"
remote_url="https://github.com/${github_user}/${repository}.git"

printf '\nПроверьте настройки:\n'
printf '  Проект:      %s\n' "$project_directory"
printf '  Репозиторий: %s\n' "$remote_url"
printf '  Автор:       %s <%s>\n' "$author_name" "$author_email"
printf '  Commit:      %s\n\n' "$commit_message"

confirm "Продолжить подготовку commit?" || die "операция отменена пользователем."

if [[ ! -d ".git" ]]; then
    printf '\nИнициализация локального Git-репозитория...\n'
    git init
fi

git config user.name "$author_name"
git config user.email "$author_email"
git branch -M main

if git remote get-url origin >/dev/null 2>&1; then
    git remote set-url origin "$remote_url"
else
    git remote add origin "$remote_url"
fi

git add -A

sensitive_files=()
while IFS= read -r staged_file; do
    case "$staged_file" in
        .env|*/.env|*.pem|*.key|*/id_rsa|*/id_ed25519|*kubeconfig*)
            sensitive_files+=("$staged_file")
            ;;
    esac
done < <(git diff --cached --name-only --diff-filter=ACMR)

if ((${#sensitive_files[@]} > 0)); then
    printf '\nОбнаружены потенциально секретные файлы:\n' >&2
    printf '  %s\n' "${sensitive_files[@]}" >&2
    die "уберите эти файлы из commit и добавьте их в .gitignore."
fi

if secret_matches="$(git grep --cached -n -E 'ghp_[A-Za-z0-9]+|github_pat_[A-Za-z0-9_]+|AKIA[0-9A-Z]{16}|BEGIN (RSA |OPENSSH )?PRIVATE KEY' -- . 2>/dev/null)" && [[ -n "$secret_matches" ]]; then
    printf '\nОбнаружены строки, похожие на credentials:\n%s\n' "$secret_matches" >&2
    die "возможные credentials нельзя публиковать."
fi

printf '\nФайлы, подготовленные к commit:\n'
git status --short
printf '\nСтатистика изменений:\n'
git diff --cached --stat

if git diff --cached --quiet; then
    if git rev-parse --verify HEAD >/dev/null 2>&1; then
        printf '\nНовых изменений для commit нет.\n'
        if confirm "Отправить существующую ветку main в GitHub?"; then
            if git push -u origin main; then
                printf '\nГотово: %s\n' "https://github.com/${github_user}/${repository}"
                exit 0
            fi
            die "push не выполнен. Проверьте адрес репозитория и авторизацию GitHub."
        fi
        exit 0
    fi
    die "Git не нашел файлов для первого commit."
fi

confirm "Создать commit и отправить его в GitHub?" || die "commit и push отменены. Подготовленные файлы остались в staging area."

git commit -m "$commit_message"

printf '\nОтправка ветки main в %s...\n' "$remote_url"
if ! git push -u origin main; then
    printf '%s\n' \
        "" \
        "Push отклонен. Не используйте --force автоматически." \
        "Проверьте, что репозиторий существует и создан без README/License." \
        "Если remote уже содержит commit, выполните:" \
        "  git pull origin main --allow-unrelated-histories --no-rebase" \
        "затем разрешите возможные конфликты и повторите git push." >&2
    exit 1
fi

printf '\nУспешно опубликовано:\n  https://github.com/%s/%s\n' "$github_user" "$repository"
