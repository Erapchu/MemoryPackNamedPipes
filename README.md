# MemoryPack over Named Pipes (.NET 10)

Стенд измеряет доставку погодных push-уведомлений от одного сервера множеству клиентов через Named Pipes. Один запуск клиента создаёт заданное число независимых pipe-подключений.

## Что измеряется

- клиент: сериализация запросов, десериализация уведомлений и end-to-end доставка (mean/p50/p95/p99, микросекунды), а также bytes allocated per operation;
- сервер: сериализация broadcast-модели и десериализация клиентских запросов;
- количество сообщений, байтов и отброшенных уведомлений медленных клиентов;
- одинаковый сценарий для MemoryPack и MessagePack.

Сервер сериализует `WeatherUpdate` **один раз на формат**, а не на клиента. Арендованный из `ArrayPool<byte>` буфер совместно используется всеми writer-циклами через reference counting. Заголовки и входящие payload-буферы тоже берутся из пула. У каждого клиента bounded-очередь; при переполнении сообщение отбрасывается и учитывается в `dropped`, поэтому один медленный клиент не создаёт неограниченное потребление памяти.

`.NET Remoting` не включён: технология относится к .NET Framework и не поддерживается в современном .NET. MessagePack даёт практически полезную альтернативу с тем же transport-сценарием.

## Запуск

В первом терминале:

```powershell
dotnet run -c Release --project src/Weather.Server -- --interval-ms 10
```

MemoryPack, 100 клиентов, 30 секунд:

```powershell
dotnet run -c Release --project src/Weather.Client -- --serializer memorypack --clients 100 --duration 30 --quiet
```

Затем тот же сценарий для MessagePack:

```powershell
dotnet run -c Release --project src/Weather.Client -- --serializer messagepack --clients 100 --duration 30 --quiet
```

Можно одновременно запустить обе группы: сервер выполнит одну сериализацию на каждый активный формат за tick. Сервер принимает `--pipe`, `--interval-ms`, `--queue`; клиент — `--pipe`, `--serializer`, `--clients`, `--duration`, `--request-ms`, `--quiet`.

Для стабильного сравнения используйте `Release`, сначала прогрейте процесс, запускайте форматы по очереди с одинаковыми параметрами и не смешивайте результаты разных нагрузок. End-to-end timestamp основан на `Stopwatch`, что корректно для процессов на одной машине — Named Pipes здесь локальные.

## Протокол

Frame имеет 6-байтовый заголовок: `message kind` (1), `serializer kind` (1), little-endian длина payload (4), затем payload. После `Hello` клиент периодически отправляет `WeatherRequest`, а сервер независимо публикует `WeatherUpdate` как push-нотификацию.
