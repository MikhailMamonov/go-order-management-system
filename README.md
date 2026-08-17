# 🛒 Order Management System (OMS)

**Микросервисная система управления заказами на Go** с распределёнными транзакциями (Saga), event-driven архитектурой и высоконагруженными интеграциями.

> 💡 **Примечание:** Этот проект — переписанная на Go версия моей Java-системы ([оригинал](https://github.com/MikhailMamonov/order-management-system)). Цель — глубокое изучение экосистемы Go через решение реальных инженерных задач.

## 🎯 О проекте

OMS решает классическую проблему e-commerce: как гарантировать консистентность данных при создании заказа, когда нужно:
1. Проверить наличие товара (Inventory Service)
2. Зарезервировать остатки (Inventory Service)
3. Обработать платёж (Payment Service)
4. Отправить уведомление клиенту (Notification Service)

Всё это происходит асинхронно, с возможными отказами сервисов и откатами транзакций.

---

## 🏗 Архитектура

```
┌─────────────────────────────────────────────────────────────┐
│                    Client (Postman/cURL)                    │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
                  ┌─────────────────────┐
                  │  API Gateway (Nginx) │
                  │      Port: 80       │
                  └──────────┬──────────┘
                             │
            ┌────────────────┼────────────────┐
            │                │                │
            ▼                ▼                ▼
     ┌────────────┐   ┌──────────────┐  ┌────────────────┐
     │   Order    │   │  Inventory   │  │  Notification  │
     │  Service   │   │   Service    │  │    Service     │
     │ (Port 8081)│   │ (Port 8082)  │  │  (Port 8083)   │
     └──────┬─────┘   └──────┬───────┘  └────────┬───────┘
            │                │                   │
            │                ▼                   │
            │         ┌────────────┐             │
            │         │   Redis    │             │
            │         │ (Port 6379)│             │
            │         └────────────┘             │
            │                                    │
            ▼                                    ▼
┌───────────────────────────────────────────────────────────┐
│              Apache Kafka (Redpanda)                      │
│                    Port: 9092                             │
│  Topics: order.created, inventory.reserved,               │
│          payment.processed, order.completed               │
└───────────────────────────────────────────────────────────┘
                           │
                           ▼
          ┌────────────────────────────────┐
          │   PostgreSQL (3 databases)     │
          │          Port: 5432            │
          │  ─ order_db                    │
          │  ─ inventory_db                │
          │  ─ notification_db             │
          └────────────────────────────────┘
```

### 🔄 Паттерн Saga (Choreography)

Система реализует распределённую транзакцию через хореографию событий:

```
  Order Service         Inventory Service        Payment Service
       │                        │                        │
       │──── OrderCreated ─────>│                        │
       │                        │                        │
       │                        │──── Check Stock ──────>│
       │                        │                        │
       │                        │<──── Stock OK ─────────│
       │                        │                        │
       │<── InventoryReserved ──│                        │
       │                        │                        │
       │──────────── ProcessPayment ────────────────────>│
       │                                                │
       │<──────────── PaymentProcessed ─────────────────│
       │                        │                        │
       │──── OrderCompleted ──>│                        │
       │                        │                        │
```

Если на любом этапе происходит ошибка, система запускает **компенсирующие транзакции** (отмена резерва, возврат средств).

---

## 🛠 Технологический стек

| Категория | Технология | Зачем используем |
|-----------|-----------|------------------|
| **Язык** | Go 1.21 | Основной язык разработки |
| **Веб-фреймворк** | [Gin](https://github.com/gin-gonic/gin) | Быстрый HTTP-роутер для REST API |
| **gRPC** | [gRPC-Go](https://grpc.io/docs/languages/go/) | Межсервисное взаимодействие (синхронное) |
| **База данных** | PostgreSQL 15 | Хранение бизнес-данных |
| **Кэширование** | Redis | Кэш остатков товаров на складе |
| **Брокер сообщений** | Kafka (Redpanda) | Асинхронная коммуникация между сервисами |
| **Миграции** | [golang-migrate](https://github.com/golang-migrate/migrate) | Управление схемой БД |
| **Конфигурация** | [Viper](https://github.com/spf13/viper) | Работа с конфигами (YAML, ENV) |
| **Логирование** | [Zap](https://github.com/uber-go/zap) | Структурированные логи |
| **Контейнеризация** | Docker, Docker Compose | Локальное развёртывание всей инфраструктуры |

---

## 📁 Структура проекта

```
go-order-management-system/
├── cmd/                          # Entry points для каждого сервиса
│   ├── order-service/
│   ├── inventory-service/
│   └── notification-service/
├── internal/                     # Приватная логика (не экспортируется)
│   ├── order/
│   │   ├── handler/              # HTTP/gRPC handlers
│   │   ├── service/              # Бизнес-логика
│   │   ├── repository/           # Работа с БД
│   │   └── model/                # Доменные модели
│   ├── inventory/
│   └── notification/
├── pkg/                          # Переиспользуемые пакеты
│   ├── kafka/                    # Kafka producer/consumer
│   ├── redis/                    # Redis client wrapper
│   ├── postgres/                 # PostgreSQL connection pool
│   └── grpc/                     # gRPC utilities
├── migrations/                   # SQL-миграции для каждой БД
│   ├── order_db/
│   ├── inventory_db/
│   └── notification_db/
├── configs/                      # Конфигурационные файлы (YAML)
├── docker-compose.yml            # Инфраструктура (Postgres, Redis, Kafka)
├── Dockerfile                    # Сборка Go-сервисов
└── Makefile                      # Автоматизация рутинных задач
```

### 🎯 Принципы архитектуры

- **Clean Architecture:** Разделение на слои (Handler → Service → Repository)
- **Dependency Injection:** Все зависимости передаются через конструкторы
- **Interface Segregation:** Репозитории и сервисы определены через интерфейсы
- **Single Responsibility:** Каждый сервис отвечает за свою доменную область

---

## 🚀 Быстрый старт

### Требования
- Docker и Docker Compose
- Go 1.21+
- Make (опционально)

### Запуск

1. **Клонируйте репозиторий:**
```bash
git clone https://github.com/MikhailMamonov/go-order-management-system.git
cd go-order-management-system
```

2. **Поднимите инфраструктуру:**
```bash
docker-compose up -d postgres redis kafka
```

3. **Примените миграции:**
```bash
make migrate-up
```

4. **Запустите сервисы:**
```bash
# В отдельных терминах:
make run-order
make run-inventory
make run-notification
```

Или соберите и запустите всё через Docker:
```bash
docker-compose up --build
```

---

## 📡 API Endpoints

### Order Service (Port 8081)

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/v1/orders` | Создать новый заказ |
| `GET` | `/api/v1/orders/:id` | Получить заказ по ID |
| `GET` | `/api/v1/orders` | Список всех заказов |
| `PUT` | `/api/v1/orders/:id/cancel` | Отменить заказ |

**Пример запроса:**
```bash
curl -X POST http://localhost:8081/api/v1/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customer_id": "123",
    "items": [
      {"product_id": "1", "quantity": 2},
      {"product_id": "2", "quantity": 1}
    ]
  }'
```

### Inventory Service (Port 8082)

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/v1/inventory/:product_id` | Проверить остаток товара |
| `POST` | `/api/v1/inventory/reserve` | Зарезервировать товар |
| `POST` | `/api/v1/inventory/release` | Освободить резерв |

### gRPC (Inventory Service)

```protobuf
service InventoryService {
  rpc CheckStock(CheckStockRequest) returns (CheckStockResponse);
  rpc ReserveStock(ReserveStockRequest) returns (ReserveStockResponse);
  rpc ReleaseStock(ReleaseStockRequest) returns (ReleaseStockResponse);
}
```

---

## 🔑 Ключевые архитектурные решения

### 1. 🎭 Circuit Breaker для внешних вызовов
При недоступности Kafka или Redis сервис не падает, а работает в degraded mode с fallback в PostgreSQL.

### 2. ⚡ Кэширование в Redis
Остатки товаров кэшируются в Redis с TTL 5 минут. При изменении остатка кэш инвалидируется через Kafka-событие.

### 3. 🧪 Тестируемость
- **Unit-тесты** для бизнес-логики (coverage > 70%)
- **Integration-тесты** для работы с БД и Kafka
- **Mocks** через [gomock](https://github.com/golang/mock)

### 4. 📊 Observability
- Структурированные логи через Zap (JSON-формат)
- Health check endpoints для каждого сервиса
- Метрики Prometheus (в планах)

### 5. 🛡 Graceful Shutdown
При получении SIGTERM сервис:
1. Прекращает принимать новые запросы
2. Завершает текущие транзакции
3. Закрывает соединения с БД и Kafka

---

## ✅ Что реализовано

- [x] Микросервисная архитектура с 3 независимыми сервисами
- [x] REST API на Gin с валидацией входных данных
- [x] gRPC для синхронного взаимодействия между сервисами
- [x] Apache Kafka для асинхронной коммуникации (Saga pattern)
- [x] PostgreSQL с отдельными БД для каждого сервиса
- [x] Redis для кэширования остатков товаров
- [x] Docker Compose для локального развёртывания
- [x] SQL-миграции через golang-migrate
- [x] Конфигурация через YAML + ENV (Viper)
- [x] Структурированное логирование (Zap)
- [x] Graceful shutdown
- [x] Unit-тесты для бизнес-логики

---

## 🚧 В планах (Roadmap)

- [ ] Добавить Payment Service с интеграцией платёжного шлюза
- [ ] Реализовать Notification Service (email, Telegram, push)
- [ ] Добавить Circuit Breaker через [gobreaker](https://github.com/sony/gobreaker)
- [ ] Внедрить OpenTelemetry для distributed tracing
- [ ] Добавить метрики Prometheus + дашборды Grafana
- [ ] Настроить CI/CD через GitHub Actions
- [ ] Добавить rate limiting и аутентификацию через JWT
- [ ] Написать интеграционные тесты с testcontainers-go

---

## 📚 Полезные ссылки

- [Оригинальная Java-версия](https://github.com/MikhailMamonov/order-management-system)
- [Go by Example](https://gobyexample.com/)
- [Designing Data-Intensive Applications](https://dataintensive.net/) (Martin Kleppmann)
- [Microservices Patterns](https://www.manning.com/books/microservices-patterns) (Chris Richardson)

---

## 👤 Автор

**Михаил Мамонов**  
Backend-разработчик с 7-летним опытом (Java/C# → Go)

- 📧 Email: [mamon201071@gmail.com](mailto:mamon201071@gmail.com)
- 💬 Telegram: [@Mikhail_M20](https://t.me/Mikhail_M20)
- 💼 GitHub: [MikhailMamonov](https://github.com/MikhailMamonov)

---

## 📄 Лицензия

Этот проект распространяется под лицензией [GPL-3.0](LICENSE).