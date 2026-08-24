---
title: "Использование"
weight: 40
---

## Как запросить бакет

Компонент Deckhouse создаёт `SystemBucket` в своём namespace:

```yaml
apiVersion: storage.deckhouse.io/v1alpha1
kind: SystemBucket
metadata:
  name: loki
  namespace: d8-monitoring
spec: {}
```

Контроллер создаёт бакет в системном хранилище, выпускает для него ключ доступа и кладёт рядом Secret:

```console
$ kubectl -n d8-monitoring get sb
NAME   BUCKET                 PHASE   SECRET               READY   AGE
loki   d8-monitoring-loki     Ready   loki-s3-credentials  True    30s
```

В Secret лежат стандартные переменные подключения к S3, поэтому нагрузка может забрать их через `envFrom`:

```yaml
    envFrom:
      - secretRef:
          name: loki-s3-credentials
```

| Ключ | Значение |
|------|----------|
| `S3_ENDPOINT` | Внутрикластерный адрес системного хранилища. |
| `S3_REGION` | Регион, который сообщает эндпойнт (`garage`). |
| `S3_BUCKET` | Фактическое имя бакета. |
| `AWS_ACCESS_KEY_ID` | Идентификатор ключа доступа, выпущенного для этого бакета. |
| `AWS_SECRET_ACCESS_KEY` | Секретный ключ. Хранится только здесь — бэкенд не умеет его отдать повторно. |

## Имена бакетов

`spec.bucketName` необязателен. Если он не задан, бакет называется `<namespace>-<name>`: пространство имён бакетов в бэкенде плоское, а `SystemBucket` — namespaced, поэтому два компонента, попросивших `backups`, не должны оказаться в одном бакете.

Задавайте имя явно только тогда, когда конкретное имя нужно чему-то за пределами кластера. Два `SystemBucket` не могут указывать на один бакет — второй отклонит admission webhook.

## Ротация учётных данных

Измените значение аннотации `storage.deckhouse.io/rotate`:

```shell
kubectl -n d8-monitoring annotate systembucket loki \
  storage.deckhouse.io/rotate="$(date +%s)" --overwrite
```

Контроллер выпустит новую пару ключей, обновит Secret и отзовёт предыдущий ключ. В `status.observedRotation` записано обработанное значение, в `status.lastRotationTime` — время выпуска. Если нагрузка не перечитывает Secret сама, перезапустите её.

То же происходит при удалении Secret с учётными данными: он принадлежит `SystemBucket`, поэтому его удаление заново запускает реконсиляцию и выпускается новая пара ключей.

## Удаление бакета

```shell
kubectl -n d8-monitoring delete systembucket loki
```

{{< alert level="danger" >}}
Удаляется бакет и все объекты в нём. Ни политики хранения, ни корзины нет — убедитесь, что данные больше не нужны или скопированы в другое место.
{{< /alert >}}

Ресурс держит finalizer до тех пор, пока бакет и его ключ доступа не исчезнут из бэкенда, — то есть недоступность хранилища задержит удаление, а не оставит в нём осиротевшие данные.

## Статус и диагностика

В `status.conditions` стадии идут по порядку:

| Condition | Что значит `False` |
|-----------|--------------------|
| `StorageReady` | Data plane Garage ещё не обслуживает запросы. В сообщении указано, чего он ждёт (раскатки подов, сходимости layout). Остальное не может начаться. |
| `BucketReady` | Бакет не удалось создать в бэкенде. |
| `CredentialsReady` | Не удалось выпустить ключ доступа или записать Secret. |
| `Ready` | Агрегат перечисленного выше. |

`SystemBucket` в namespace без лейбла `heritage: deckhouse` отклоняется вебхуком. Если такой ресурс всё же появился (например, создан, пока вебхук был недоступен), контроллер откажется его обслуживать и скажет об этом через `StorageReady=False` с причиной `NamespaceNotPlatform`.

Посмотреть на само хранилище:

```shell
kubectl -n d8-sds-object-system get pods,sts,pvc
kubectl -n d8-sds-object-system logs deploy/controller -c controller
```
