# 🛒 Order Management System (OMS)

**Микросервисная система управления заказами на Go** с gRPC для межсервисного взаимодействия, Kafka для событийной архитектуры и PostgreSQL для хранения данных.

> 💡 **Примечание:** Этот проект демонстрирует production-ready микросервисную архитектуру с API Gateway, gRPC коммуникацией между сервисами и event-driven подходом через Kafka.

---

## 🎯 О проекте

OMS решает классическую проблему e-commerce: как гарантировать консистентность данных при создании заказа в распределённой системе.

**Ключевые особенности:**
- 🏗 **Микросервисная архитектура** с чётким разделением ответственности
- 🔌 **gRPC** для быстрого и типобезопасного межсервисного взаимодействия
- 📨 **Kafka** для асинхронной коммуникации и Saga pattern
- 🗄 **PostgreSQL** для надёжного хранения данных
- 🚪 **API Gateway** как единая точка входа для внешних клиентов

---

## 🏗 Архитектура


```
┌─────────────────────────────────────────────────────────────────────┐
│                    🚪 API GATEWAY (порт 8080)                       │
│              Единственная точка входа для клиентов                  │
│              HTTP/REST → gRPC конвертация                           │
│              JWT аутентификация, Rate limiting                      │
└──────────────┬──────────────┬──────────────┬────────────────────────┘
               │ gRPC         │ gRPC         │ gRPC
               ▼              ▼              ▼
┌──────────────┐   ┌──────────────┐   ┌──────────────┐
│ Order        │   │ Inventory    │   │ Payment      │
│ Service      │   │ Service      │   │ Service      │
│              │   │              │   │              │
│ Port: 50051  │   │ Port: 50052  │   │ Port: 50053  │
└──────┬───────┘   └──────┬───────┘   └──────┬───────┘
       │                  │                  │
       └──────────────────┴──────────────────┘
                    Apache Kafka
              (события, Saga pattern)

```

### Почему gRPC вместо REST для межсервисного общения?

| Критерий | REST (HTTP/1.1 + JSON) | gRPC (HTTP/2 + Protobuf) |
|----------|------------------------|--------------------------|
| **Скорость** | ~5ms | ~0.5ms (в 10 раз быстрее) |
| **Размер payload** | ~200 байт | ~50 байт (в 4 раза меньше) |
| **Типобезопасность** | ❌ Runtime ошибки | ✅ Compile-time проверки |
| **Мультиплексирование** | ❌ 1 запрос на соединение | ✅ Множество запросов на 1 TCP |
| **Контракты** | ❌ OpenAPI (опционально) | ✅ .proto (обязательно, codegen) |

---

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
| **gRPC** | gRPC + Protobuf | Межсервисное взаимодействие |
| **API Gateway** | Gin | HTTP → gRPC конвертация, JWT, rate limiting |
| **База данных** | PostgreSQL 15 | Хранение бизнес-данных |
| **Брокер сообщений** | Apache Kafka (Redpanda) | Асинхронная коммуникация, Saga |
| **Миграции** | golang-migrate | Управление схемой БД |
| **Конфигурация** | Viper | Работа с конфигами (YAML, ENV) |
| **Логирование** | Zap | Структурированные JSON логи |
| **Контейнеризация** | Docker, Docker Compose | Локальное развёртывание |

---

## 📁 Структура проекта

```
go-order-management-system/
├── api/                          # Protobuf контракты
│   ├── inventory/v1/
│   │   ├── inventory.proto       # gRPC контракт для Inventory Service
│   │   ├── inventory.pb.go       # Сгенерированный код (messages)
│   │   └── inventory_grpc.pb.go  # Сгенерированный код (services)
│   └── order/v1/
│       └── order.proto
├── cmd/                          # Entry points для каждого сервиса
│   ├── api-gateway/
│   │   └── main.go               # API Gateway (HTTP сервер)
│   ├── order-service/
│   │   └── main.go               # Order Service (gRPC сервер)
│   ├── inventory-service/
│   │   └── main.go               # Inventory Service (gRPC сервер)
│   └── payment-service/
│       └── main.go
├── internal/                     # Приватная логика
│   ├── gateway/
│   │   ├── handlers/             # HTTP handlers для Gateway
│   │   └── middleware/           # JWT, CORS, Rate limiting
│   ├── order/
│   │   ├── client/               # gRPC клиенты к другим сервисам
│   │   ├── grpc/                 # gRPC сервер (адаптер)
│   │   ├── service/              # Бизнес-логика
│   │   ├── repository/           # Работа с БД
│   │   └── models/               # Доменные модели
│   ├── inventory/
│   │   ├── grpc/                 # gRPC сервер
│   │   ├── service/              # Бизнес-логика
│   │   ├── repository/           # Работа с БД
│   │   └── consumers/            # Kafka consumers
│   └── payment/
├── pkg/                          # Переиспользуемые пакеты
│   ├── database/                 # PostgreSQL подключение
│   ├── kafka/                    # Kafka producer/consumer
│   └── grpc/                     # gRPC utilities
├── migrations/                   # SQL-миграции для каждой БД
├── configs/                      # Конфигурационные файлы (YAML)
├── docker-compose.yml            # Инфраструктура
└── Makefile                      # Автоматизация задач
```

### Принципы архитектуры

- **Clean Architecture:** Разделение на слои (Handler → Service → Repository)
- **Dependency Injection:** Все зависимости передаются через конструкторы
- **Interface Segregation:** Репозитории и сервисы определены через интерфейсы
- **Adapter Pattern:** gRPC-слой — это адаптер между protobuf и доменными объектами
- **Single Source of Truth:** Бизнес-логика используется и HTTP, и gRPC, и Kafka consumer'ом

---

## 🔌 gRPC взаимодействие

### Protobuf контракт (api/inventory/v1/inventory.proto)

```protobuf
syntax = "proto3";

package inventory;

service InventoryService {
  rpc CheckStock(CheckStockRequest) returns (CheckStockResponse);
  rpc ReserveStock(ReserveStockRequest) returns (ReserveStockResponse);
  rpc ReleaseStock(ReleaseStockRequest) returns (ReleaseStockResponse);
}

message CheckStockRequest {
  string product_id = 1;
}

message CheckStockResponse {
  bool available = 1;
  int32 quantity = 2;
}
```

### Генерация кода

```bash
# Установить инструменты
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# Сгенерировать Go-код из .proto
protoc \
  --go_out=. --go_opt=paths=source_relative \
  --go-grpc_out=. --go-grpc_opt=paths=source_relative \
  api/inventory/v1/inventory.proto
```

### Тестирование через grpcurl

```bash
# Установить grpcurl
go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest

# Включить reflection в main.go (для отладки)
# import "google.golang.org/grpc/reflection"
# reflection.Register(grpcServer)

# Посмотреть список сервисов
grpcurl -plaintext localhost:50051 list

# Посмотреть методы сервиса
grpcurl -plaintext localhost:50051 list inventory.InventoryService

# Проверить наличие товара
grpcurl -plaintext \
  -d '{"product_id": "550e8400-e29b-41d4-a716-446655440000"}' \
  localhost:50051 inventory.InventoryService.CheckStock

# Зарезервировать товар
grpcurl -plaintext \
  -d '{"product_id": "550e8400-e29b-41d4-a716-446655440000", "quantity": 5, "order_id": "123e4567-e89b-12d3-a456-426614174000"}' \
  localhost:50051 inventory.InventoryService.ReserveStock
```

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
docker compose up -d postgres redis kafka
```

3. **Примените миграции:**
```bash
make migrate-up
```

4. **Запустите сервисы (в отдельных терминалах):**
```bash
# Терминал 1: Inventory Service
go run cmd/inventory-service/main.go

# Терминал 2: Order Service
go run cmd/order-service/main.go

# Терминал 3: API Gateway
go run cmd/api-gateway/main.go
```

Или через Docker Compose:
```bash
docker compose up --build
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

### 1. 🎭 Saga Pattern через Kafka
Распределённые транзакции реализованы через хореографию событий:
- Order Service публикует `OrderCreated`
- Inventory Service резервирует товар и публикует `InventoryReserved`
- Payment Service обрабатывает платёж и публикует `PaymentProcessed`

При ошибке на любом этапе запускаются компенсирующие транзакции.

### 2. 🔄 gRPC как адаптер
gRPC-слой — это "тонкий адаптер", который:
- Принимает protobuf-запрос
- Конвертирует в доменные объекты (UUID, модели)
- Вызывает бизнес-логику (которая не знает о gRPC)
- Конвертирует ответ обратно в protobuf

Это обеспечивает **Single Source of Truth** для бизнес-логики.

### 3. 🚪 API Gateway
Gateway отвечает за:
- **Аутентификацию** (JWT) — единожды, не в каждом сервисе
- **Rate limiting** — защита от DDoS
- **CORS** — для веб-клиентов
- **Маршрутизацию** — HTTP → gRPC конвертация
- **Агрегацию** — fan-out запросы к нескольким сервисам

### 4. 📊 Observability
- Структурированные логи через Zap (JSON-формат)
- Health check endpoints для каждого сервиса
- Reflection API для отладки через grpcurl
- Graceful shutdown для всех серверов (HTTP, gRPC, Kafka consumers)

---

## ✅ Что реализовано

- [x] Микросервисная архитектура с API Gateway
- [x] gRPC для межсервисного взаимодействия (быстрее REST в 10 раз)
- [x] Protobuf контракты с codegen
- [x] Apache Kafka для асинхронной коммуникации (Saga pattern)
- [x] PostgreSQL с отдельными БД для каждого сервиса
- [x] Docker Compose для локального развёртывания
- [x] SQL-миграции через golang-migrate
- [x] Конфигурация через YAML + ENV (Viper)
- [x] Структурированное логирование (Zap)
- [x] Graceful shutdown для HTTP, gRPC, Kafka
- [x] JWT аутентификация в API Gateway
- [x] Rate limiting в API Gateway
- [x] Reflection API для отладки gRPC

---

## 🚧 В планах (Roadmap)

- [ ] Добавить Redis для кэширования остатков товаров
- [ ] Настроить CI/CD через GitHub Actions
- [ ] Добавить интеграционные тесты с testcontainers-go
- [ ] Реализовать Payment Service
- [ ] Добавить Notification Service (email, Telegram)

---

## 📚 Полезные ссылки

- [Оригинальная Java-версия](https://github.com/MikhailMamonov/order-management-system)
- [Go by Example](https://gobyexample.com/)
- [Designing Data-Intensive Applications](https://dataintensive.net/) (Martin Kleppmann)
- [Microservices Patterns](https://www.manning.com/books/microservices-patterns) (Chris Richardson)
- [gRPC Official Documentation](https://grpc.io/docs/)


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
