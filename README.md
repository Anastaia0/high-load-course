# Template for the HighLoad course
This project is based on [Tiny Event Sourcing library](https://github.com/andrsuh/tiny-event-sourcing)

### Run PostgreSql
This example uses Postgres as an implementation of the Event store. You can see it in `pom.xml`:

```
<dependency>
    <groupId>ru.quipy</groupId>
    <artifactId>tiny-postgres-event-store-spring-boot-starter</artifactId>
    <version>${tiny.es.version}</version>
</dependency>
```

Thus, you have to run Postgres in order to test this example. Postgres service is included in  `docker-compose` file that we have in the root of the project.

# More comprehensive information about the course, project, how to run tests is here:

https://andrsuh.notion.site/2595d535059281d8a815c2cb3875c376?source=copy_link

https://andrsuh.notion.site/2625d5350592801aaf88c7c95302d10c?source=copy_link

### Run the infrastructure
Set of the services you need to start developing and testing process is following:
- Bombardier - service that is in charge of emulation the store's clients activity (creates the incoming load). Also serves as a third-party payment system.
- Postgres DBMS
- Prometheus + Grafana - metrics collection and visualization services

You can run all beforementioned services by the following command:
```
docker compose -f docker-compose.yml up
```

### Run the application
To make the application run you can start the main class `OnlineShopApplication`. It is not being launched as a docker contained to simplify and speed up the devevopment process as it is easier for you to refactor the application and re-run it immediately in the IDE.


### If you want to pull changes from the main repository into your fork

The command ```git remote -v``` should include the following lines:

```
upstream        https://github.com/andrsuh/high-load-course.git (fetch)
upstream        https://github.com/andrsuh/high-load-course.git (push)
```

If not, add the upstream remote:
```git remote add upstream https://github.com/andrsuh/high-load-course.git```

To pull changes from the main repository, run the following commands:

```
git fetch upstream
# switch to the main branch of your fork. Make sure the branch has no uncommitted changes to avoid conflicts
git checkout main 
# merge changes from the main repository into your main branch
git merge upstream/main 
```

## Case 1

### Первый прогон

![Первый прогон](docs/case-1/before.jpg)

### После

![После](docs/case-1/after.jpg)

## Case 3

`acc-5` · 2 покупки/с · 500 покупок · `processingTimeMillis = 60000`.

[Расчёты, собственные метрики и результаты прогонов](docs/case-3/README.md).

**Локально получено 500/500.** Оплаты отправляются через
`PUT /external/process/bulk`: до пяти покупок в одном запросе, накопление не более
двух секунд. Исходные дедлайны, суммы и лимиты аккаунта сохраняются.

| Прогон | Успешные покупки | HTTP-запросы | Оплаты у провайдера | Расходы |
| --- | ---: | ---: | ---: | ---: |
| До | 105/500 (21%) | 500 | 500 | 15 000 |
| После локально | **500/500 (100%)** | **117** | **500** | **15 000** |
| На учебном стенде | Ещё не запускался | — | — | — |

**17 тестовых случаев прошли.** Итоговый локальный прогон:
15.10.2026, 17:27:58–17:32:21 МСК. Ошибок, таймаутов и нарушений лимитов нет,
очередь завершена. Промежуточные 298/500 и расчёты обеих схем сохранены в отчёте.
Удалённая проверка остаётся отдельным этапом.

### До

![Кейс 3: исходный прогон](docs/case-3/before.jpg)

### После: 500/500

![Кейс 3: 500 успешных покупок и 117 HTTP-пакетов](docs/case-3/bulk.jpg)
