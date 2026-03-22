# cinemaabyss proxy-service

Proxy-service реализует API Gateway для учебного проекта КиноБездна и поддерживает паттерн Strangler Fig для постепенной миграции домена `movies` из монолита в микросервис.

## Переменные окружения

- `PORT` — порт запуска сервиса, по умолчанию `8000`
- `MONOLITH_URL` — базовый URL монолита
- `MOVIES_SERVICE_URL` — базовый URL сервиса movies
- `EVENTS_SERVICE_URL` — базовый URL сервиса events
- `GRADUAL_MIGRATION` — `true` или `false`
- `MOVIES_MIGRATION_PERCENT` — процент запросов к `/api/movies`, отправляемых в `movies-service`

## Маршрутизация

- `GET /health` — собственный endpoint proxy
- `/api/events*` — всегда в `events-service`
- `/api/movies/health` — всегда в `movies-service`
- `/api/movies` — по gradual migration:
  - если `GRADUAL_MIGRATION=false`, весь трафик идёт в монолит
  - если `GRADUAL_MIGRATION=true`, трафик делится между монолитом и `movies-service`
- всё остальное — в монолит

## Алгоритм gradual migration

Используется детерминированное распределение через счётчик и `mod 100`:

- `MOVIES_MIGRATION_PERCENT=0` — 100% запросов идут в монолит
- `MOVIES_MIGRATION_PERCENT=50` — 50 из каждых 100 запросов идут в `movies-service`
- `MOVIES_MIGRATION_PERCENT=100` — 100% запросов идут в `movies-service`

## Запуск локально

```bash
go run .
```

## Сборка Docker
```bash
docker build -t cinemaabyss-proxy-service .
docker run --rm -p 8000:8000 \
  -e PORT=8000 \
  -e MONOLITH_URL=http://localhost:8080 \
  -e MOVIES_SERVICE_URL=http://localhost:8081 \
  -e EVENTS_SERVICE_URL=http://localhost:8082 \
  -e GRADUAL_MIGRATION=true \
  -e MOVIES_MIGRATION_PERCENT=50 \
  cinemaabyss-proxy-service
```