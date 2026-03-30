# Практическое занятие №14
# Саттаров Булат Рамилевич ЭФМО-01-25
# Реализация очереди задач (producer–consumer): retries, DLQ, идемпотентность

## Сборка и запуск проекта

```bash
cd deploy
сp .env.example .env
docker compose up -d --build
```

### 1. Описание очередей: основная и DLQ
В работе используются две очереди RabbitMQ:

- `task_jobs` — основная очередь задач;
- `task_jobs_dlq` — очередь для сообщений, которые не удалось обработать.

Связь между ними сделана через dead-letter exchange.  
Основная очередь объявляется с параметрами `x-dead-letter-exchange` и `x-dead-letter-routing-key`, поэтому при `nack(requeue=false)` сообщение автоматически уходит в DLQ.

Имена очередей и параметры задаются через `deploy/.env`:

```env
RABBIT_URL=amqp://guest:guest@rabbitmq:5672/
QUEUE_NAME=task_jobs
DLX_NAME=task_jobs_dlx
DLQ_NAME=task_jobs_dlq
WORKER_PREFETCH=1
MAX_ATTEMPTS=3
PROCESSING_MIN_MS=2000
PROCESSING_MAX_MS=5000
```

Объявление очередей находится в `services/tasks/internal/rabbit/producer.go`:

```go
args := amqp.Table{}
if topology.DLXName != "" {
    args["x-dead-letter-exchange"] = topology.DLXName
    if topology.DLQName != "" {
        args["x-dead-letter-routing-key"] = topology.DLQName
    }
}

_, err := ch.QueueDeclare(
    topology.MainQueue,
    true,
    false,
    false,
    false,
    args,
)

q, err := ch.QueueDeclare(
    topology.DLQName,
    true,
    false,
    false,
    false,
    nil,
)

return ch.QueueBind(
    q.Name,
    q.Name,
    topology.DLXName,
    false,
    nil,
)
```

Здесь `task_jobs` получает настройки DLX, а `task_jobs_dlq` создается как отдельная durable-очередь и привязывается к exchange.

Проверка состояния очередей:

![queues](docs/screenshots/queues.png)

### 2. Формат сообщения job: какие поля, где attempts/message_id
В очередь отправляется JSON-сообщение такого вида:

```json
{
  "job": "process_task",
  "task_id": "t_ok",
  "attempt": 1,
  "message_id": "9808cd04-fd48-43b5-8a04-26620da1e5ff"
}
```

Поля сообщения:

- `job` — тип задания;
- `task_id` — идентификатор задачи;
- `attempt` — номер текущей попытки обработки;
- `message_id` — уникальный идентификатор сообщения.

Формирование job находится в `services/tasks/internal/service/service.go`:

```go
job := ProcessTaskJob{
    Job:       "process_task",
    TaskID:    taskID,
    Attempt:   1,
    MessageID: messageID,
}

if err := s.producer.Publish(job); err != nil {
    return ProcessTaskJob{}, err
}
```

`attempt` хранится прямо в payload и при первой публикации равен `1`.  
`message_id` тоже хранится в payload и используется потом в consumer для идемпотентности.

Endpoint для постановки job в очередь добавлен в `services/tasks/internal/http/router.go`:

```go
mux.HandleFunc("/v1/jobs/process-task", handler.ProcessTaskJob)
```

Пример запроса на постановку job:

![curl request](docs/screenshots/curl.png)

### 3. Описание retry policy: сколько попыток, что считается ошибкой, есть ли задержка
В работе реализован простой учебный retry:

- максимум `3` попытки;
- между обработками есть имитация тяжелой операции с задержкой от `2` до `5` секунд;
- отдельная retry-queue не используется;
- если попытки еще не закончились, сообщение публикуется заново в основную очередь;
- если лимит исчерпан, сообщение уходит в DLQ.

Основная retry-логика находится в `services/worker/cmd/main.go`:

```go
if err := p.process(job); err != nil {
    if job.Attempt >= p.maxAttempts {
        return msg.Nack(false, false)
    }

    job.Attempt++
    if republishErr := p.publishRetry(job); republishErr != nil {
        return errors.Join(err, republishErr)
    }

    return msg.Ack(false)
}
```

Если обработка завершилась ошибкой и лимит достигнут, worker делает `Nack(false, false)`,
RabbitMQ отправляет сообщение в DLQ, если лимит не достигнут, `attempt` увеличивается и сообщение заново отправляется в `task_jobs`.

Cценарии ошибок тоже заданы в `services/worker/cmd/main.go`:

```go
switch {
case taskID == "t_fail":
    return errors.New("simulated permanent failure")
case taskID == "t_flaky" && job.Attempt < 2:
    return errors.New("simulated transient failure")
case strings.HasSuffix(taskID, "3"):
    return errors.New("simulated deterministic failure for ids ending with 3")
default:
    return nil
}
```

То есть:

- `t_flaky` — временная ошибка, на второй попытке сообщение проходит;
- `t_fail` — постоянная ошибка, поэтому после 3 попыток сообщение уходит в DLQ;
- `task_id`, который заканчивается на `3`, тоже считается ошибочным.

Демонстрация retry:

![retry logs](docs/screenshots/t_flaky.png)

Демонстрация попадания в DLQ:

![dlq logs](docs/screenshots/t_fail.png)

### 4. Как устроена идемпотентность: что является ключом, где хранится, как проверяется
В работе ключом идемпотентности является `message_id`.

- worker получает сообщение;
- перед обработкой проверяет, встречался ли уже этот `message_id`;
- если да, сообщение считается дублем и повторно не выполняется;
- если нет, после успешной обработки `message_id` сохраняется в памяти.

Проверка дублей находится в `services/worker/cmd/main.go`:

```go
if p.processedIDs.Exists(job.MessageID) {
    log.Printf("duplicate message ignored: message_id=%s task_id=%s", job.MessageID, job.TaskID)
    return msg.Ack(false)
}

// ...

p.processedIDs.Mark(job.MessageID)
```

Демонстрация идемпотентности:

![duplicate message](docs/screenshots/ignore_duplicate.png)

### 5. Демонстрация логами: успешная обработка, несколько ретраев, попадание в DLQ
Ниже показаны три основных сценария, которые требуются в работе.

Успешная обработка `t_ok`:

![success logs](docs/screenshots/t_ok.png)

Здесь worker получает сообщение, обрабатывает его и завершает без ошибок с первой попытки.

Несколько ретраев на `t_flaky`:

![flaky logs](docs/screenshots/t_flaky.png)

Здесь первая попытка падает, затем worker делает retry и на второй попытке задача проходит успешно.

Попадание в DLQ на `t_fail`:

![fail logs](docs/screenshots/t_fail.png)

Здесь видно, что сообщение трижды завершается ошибкой, после чего достигается лимит попыток и сообщение переводится в `task_jobs_dlq`.
