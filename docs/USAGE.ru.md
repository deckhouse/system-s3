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
spec:
  size: 10Gi
```

`size` — сколько бакет просит. Значение по умолчанию (`1Gi`) подходит компоненту, который хранит немного; если нужно больше, указывайте явно. Из этого же значения складывается размер томов системного хранилища: оно заказывает сумму размеров всех `SystemBucket`, поэтому увеличение здесь и есть то, от чего растут тома. Запись сверх `size` бэкенд отклоняет.

`size` можно увеличивать в любой момент и нельзя уменьшать — API-сервер такую правку отклонит: выделенные под больший размер тома не сжимаются, а квота ниже того, что уже лежит в бакете, только сломала бы запись в него. Вступил ли увеличенный размер в силу, показывает условие `SizeApplied`: `True`, когда бэкенд применил новую квоту и тома хранилища выросли под неё; `Growing`, пока растут; `CannotGrow`, когда вырасти не могут (обычно у StorageClass нет `allowVolumeExpansion`), — в сообщении сказано, что изменить. Бакет всё это время остаётся `Ready`.

```console
$ kubectl -n d8-monitoring get sb loki -o jsonpath='{.status.conditions[?(@.type=="SizeApplied")]}'
```

Контроллер создаёт бакет в системном хранилище и кладёт рядом с ресурсом `ConfigMap` со всем, что нужно, чтобы до бакета добраться:

```console
$ kubectl -n d8-monitoring get sb
NAME   BUCKET               PHASE   CONNECTION           READY   AGE
loki   d8-monitoring-loki   Ready   loki-s3-connection   True    30s
```

| Ключ | Значение |
|------|----------|
| `S3_ENDPOINT` | Внутрикластерный адрес системного хранилища, `https://`. |
| `S3_REGION` | Регион, который объявляет эндпоинт (`garage`). |
| `S3_BUCKET` | Фактическое имя бакета. |
| `S3_CA_CERT` | CA, которым подписан S3-эндпоинт. Собственный для модуля, как и `S3_STS_CA_CERT` (сейчас это один и тот же CA), поэтому по умолчанию ему в кластере никто не доверяет. |
| `S3_STS_ENDPOINT` | Куда обменивать токен ServiceAccount на учётные данные. |
| `S3_STS_AUDIENCE` | Значение `audience` (адресат токена), которое нужно запросить для токена ServiceAccount, предъявляемого на `S3_STS_ENDPOINT`, — всегда `system-s3-sts`. Указывается в спецификации пода, в projected-томе с токеном (см. ниже): из ConfigMap Kubernetes его не возьмёт. Эндпоинт принимает только токены, выпущенные для этого адресата, поэтому токен пода по умолчанию будет отвергнут. Такой токен не аутентифицирует в API-сервере: утёкший, он позволяет только запросить учётные данные, которые его ServiceAccount и так вправе получить. |
| `S3_STS_CA_CERT` | CA, которым подписан эндпоинт выдачи. Он собственный для модуля, поэтому по умолчанию ему в кластере никто не доверяет. |

**Учётных данных в нём нет — и больше их нет нигде.** Долгоживущий ключ модуль не
выпускает ни для кого, включая владельца бакета. Учётные данные получают все
одинаково: предъявляют токен своего ServiceAccount на эндпоинте выдачи и получают
пару ключей, которую бэкенд перестаёт принимать через час.

Адреса берите через `envFrom` этого ConfigMap, токен монтируйте, а оба CA
смонтируйте в отдельный каталог:

```yaml
    envFrom:
      - configMapRef:
          name: loki-s3-connection
    env:
      # Go: системные корни плюс CA модуля, для всех TLS-клиентов процесса.
      # /etc/ssl/certs остаётся в списке, потому что SSL_CERT_DIR заменяет
      # каталоги по умолчанию, а не дополняет их.
      - name: SSL_CERT_DIR
        value: /etc/ssl/certs:/etc/system-s3/ca
    volumeMounts:
      - name: s3-token
        mountPath: /var/run/secrets/system-s3
        readOnly: true
      - name: s3-ca
        mountPath: /etc/system-s3/ca
        readOnly: true
    volumes:
      - name: s3-token
        projected:
          sources:
            - serviceAccountToken:
                path: token
                audience: system-s3-sts     # S3_STS_AUDIENCE
                expirationSeconds: 3600
      - name: s3-ca
        configMap:
          name: loki-s3-connection
          items:                            # только CA, PEM-файлами
            - {key: S3_CA_CERT, path: s3-ca.crt}
            - {key: S3_STS_CA_CERT, path: sts-ca.crt}
```

**S3-эндпоинт работает только по TLS**, с сертификатом от собственного CA модуля,
и клиент должен этому CA доверять — открытого порта, на который можно
откатиться, нет.

- **Go** (minio-go, aws-sdk-go-v2, всё на `crypto/tls`): примера выше достаточно.
  Go на Linux собирает системные корни из файла-бандла *и* из всех файлов в
  каталогах `SSL_CERT_DIR`, поэтому CA модуля добавляются к публичным, и любой
  клиент процесса, который пользуется системными корнями, доверяет обоим
  эндпоинтам без единой строки кода. Системные корни Go читает один раз за
  процесс: новый CA вступает в силу после рестарта. CA долгоживущий; продление
  серверного сертификата его не меняет.
- **AWS CLI и boto3**: `AWS_CA_BUNDLE=/etc/system-s3/ca/s3-ca.crt`. Переменная
  заменяет бандл SDK, а не дополняет его, так что клиенту, который ходит ещё и
  на публичные эндпоинты, нужен бандл с обоими.
- **Инструменты на OpenSSL** (curl, `ssl` в Python): `SSL_CERT_DIR` не поможет —
  OpenSSL ищет сертификаты в каталоге по именам-хешам, которых в смонтированном
  ConfigMap нет. Укажите файл: `curl --cacert /etc/system-s3/ca/s3-ca.crt`,
  `SSL_CERT_FILE`, `REQUESTS_CA_BUNDLE`.
- **Не через `subPath` в `/etc/ssl/certs`.** Том с `subPath` не обновляется при
  изменении ConfigMap, и под останется со старым CA после его замены модулем.

## Как получить учётные данные

Обмен — это `AssumeRoleWithWebIdentity`, который умеет любая клиентская
библиотека S3:

```go
creds, _ := credentials.NewSTSWebIdentity(
    stsEndpoint+"/d8-monitoring/loki",              // S3_STS_ENDPOINT + /<namespace>/<имя>
    func() (*credentials.WebIdentityToken, error) {
        token, err := os.ReadFile("/var/run/secrets/system-s3/token")
        return &credentials.WebIdentityToken{Token: string(token)}, err
    },
)
```

Эндпоинт возвращает пару ключей, которую Garage перестаёт принимать в момент,
указанный в ответе (по умолчанию через час; `DurationSeconds` — от 15 минут до 12
часов). Session token не возвращается и не нужен: это настоящая пара ключей,
запрос подписывается ею обычным образом.

**Право спросить — это право в RBAC**, и компоненту, который создал
`SystemBucket`, оно нужно так же, как всем остальным: владение ресурсом — это
право править ресурс, а не читать объекты в его бакете.

```yaml
# Два субресурса: читать объекты в бакете и править ресурс SystemBucket —
# разные права.
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: loki-bucket-access
  namespace: d8-monitoring
rules:
  - apiGroups: ["storage.deckhouse.io"]
    resources: ["systembuckets/read", "systembuckets/write"]
    resourceNames: ["loki"]
    verbs: ["get"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: loki-bucket-access
  namespace: d8-monitoring
roleRef: {apiGroup: rbac.authorization.k8s.io, kind: Role, name: loki-bucket-access}
subjects:
  - kind: ServiceAccount
    name: loki
    namespace: d8-monitoring
```

Другому компоненту, которому нужен тот же бакет, выдаётся своё такое же право с
субъектом в его пространстве имён. Ничего между ними не копируется, а отобрать
доступ — удалить RoleBinding: выданное истекает само, а следующий запрос получает
отказ.

**Полученный ключ надо переиспользовать до истечения срока.** Каждый запрос
выпускает новый ключ, и он доживает свой час независимо от того, нужен ли ещё
клиенту, — в метаданных, которые Garage реплицирует на все узлы. Поэтому на бакет
разрешено не больше 50 живых временных ключей: клиент, который просит учётные
данные на каждый запрос, упрётся в этот предел и получит отказ с объяснением.
Правильный клиент держит один ключ и обновляет его ближе к концу срока.

## Имена бакетов

`spec.bucketName` необязателен. Если он не задан, бакет называется `<namespace>-<name>`: пространство имён бакетов в бэкенде плоское, а `SystemBucket` — namespaced, поэтому два компонента, попросивших `backups`, не должны оказаться в одном бакете.

Задавайте имя явно только тогда, когда конкретное имя нужно чему-то за пределами кластера. Два `SystemBucket` не могут указывать на один бакет: второй отклонит admission webhook, а если вебхук обойдён (два одновременных создания или кластер, где вебхук не установлен) — откажет и контроллер, выставив `BucketReady=False` и указав, кому бакет принадлежит. Такому ресурсу нужно задать другой `spec.bucketName`.

Отказ здесь не перестраховка: `SystemBucket` владеет своими данными безраздельно, политики reclaim нет, поэтому два владельца одного бакета — это две нагрузки, пишущие в одно хранилище, и удаление любой из них уносит объекты другой.

Каким бы способом имя ни получилось, оно должно быть таким, какое принимает S3: от 3 до 63 символов, строчные латинские буквы, цифры и дефисы, начинается и заканчивается буквой или цифрой. Проблема возникает именно с выведенным именем, потому что его не набирают, а составляют: namespace и имя ресурса длиной до 63 символов каждое дают в сумме до 127. Если пара слишком длинная, задайте `spec.bucketName`:

```console
$ kubectl -n d8-observability-long-term-metrics apply -f systembucket.yaml
Error from server: admission webhook denied the request: the bucket name this
SystemBucket maps to,
"d8-observability-long-term-metrics-prometheus-remote-write-backend", cannot be
used: it is 66 characters long, and S3 bucket names stop at 63. With
spec.bucketName unset the name is derived as <namespace>-<name> …
```

Контроллер отказывает по той же причине, поэтому ресурс, созданный при выключенном вебхуке, показывает `BucketReady=False` с указанием, что задать, а не оставляет ответ бэкенду.

## Удаление бакета

```shell
kubectl -n d8-monitoring delete systembucket loki
```

{{< alert level="danger" >}}
Удаляется бакет и все объекты в нём. Ни политики хранения, ни корзины нет — убедитесь, что данные больше не нужны или скопированы в другое место.
{{< /alert >}}

Ресурс держит finalizer до тех пор, пока бакет не исчезнет из бэкенда, — то есть недоступность хранилища задержит удаление, а не оставит в нём осиротевшие данные.

## Статус и диагностика

В `status.conditions` стадии идут по порядку:

| Condition | Что значит `False` |
|-----------|--------------------|
| `StorageReady` | Data plane Garage ещё не обслуживает запросы. В сообщении указано, чего он ждёт (раскатки подов, сходимости layout). Остальное не может начаться. |
| `BucketReady` | Бакет не удалось создать в бэкенде. |
| `ConnectionReady` | Не удалось записать `ConfigMap` с параметрами подключения. |
| `Ready` | Агрегат перечисленного выше. |

`SystemBucket` в namespace без лейбла `heritage: deckhouse` отклоняется вебхуком. Если такой ресурс всё же появился (например, создан, пока вебхук был недоступен), контроллер откажется его обслуживать и скажет об этом через `StorageReady=False` с причиной `NamespaceNotPlatform`.

Посмотреть на само хранилище:

```shell
kubectl -n d8-system-s3 get pods,sts,pvc
kubectl -n d8-system-s3 logs deploy/controller -c controller
```

## Выключение модуля и снос кластера

Выключение защищено: Deckhouse не примет `enabled: false`, пока на `ModuleConfig` нет подтверждения, — вместе с модулем уезжает data plane и всё, что в нём хранится.

```shell
kubectl annotate moduleconfig system-s3 modules.deckhouse.io/allow-disabling=true
kubectl patch moduleconfig system-s3 --type merge -p '{"spec":{"enabled":false}}'
```

Что остаётся после выключения, а что нет:

- ресурсы `SystemBucket` остаются — замороженными в последнем статусе, — и их CRD тоже: выключение модуля не удаляет ни то, ни другое. Свои финализаторы модуль при выгрузке снимает, поэтому такие ресурсы можно удалить руками;
- `PersistentVolumeClaim` дата-плейна уходят вместе с пространством имён, а судьбу томов за ними решает политика `StorageClass`: `Delete` удалит их вместе со всем, что в них лежало, `Retain` оставит. Модуль в это не вмешивается намеренно: своего слоя томов у него больше нет, и переопределять чужую политику на выходе он не вправе;
- конфигурации вебхуков удаляются: admission-вебхук, у которого не осталось бэкенда, блокировал бы работу с ресурсами, которые он стережёт.

{{< alert level="warning" >}}
**Перед сносом кластера модуль нужно выключить.** `dhctl destroy` удаляет все `PersistentVolumeClaim` и ждёт их исчезновения, а `StatefulSet` data plane пересоздаёт свои claim'ы, пока существует, — шаг падает по таймауту, и кластер остаётся снесённым наполовину. Выключение модуля убирает `StatefulSet`, и снос проходит до конца.
{{< /alert >}}

{{< alert level="info" >}}
Пока модуль находится на стадии `Experimental`, для повторного включения после выключения нужно разрешение платформы: добавьте `system-s3` в `spec.settings.allowedExperimentalModules` в `ModuleConfig` `deckhouse` (или включите `allowExperimentalModules: true`), и только затем ставьте `enabled: true`.
{{< /alert >}}
