# team-1-project

Участники команды 1:
1) Евгения Золотухина
2) Наталья Мурчич
3) Фидан Ахундова
4) Александр Иванов
---
🚀 Запуск проекта

Проект разворачивается с помощью Docker Compose и включает следующие сервисы:

- PostgreSQL 
- PostgreSQL для Airflow 
- Apache Airflow
- pgAdmin 

Запуск:

```bash
docker-compose up --build
```
---
🛠️ pgAdmin

pgAdmin используется для просмотра и проверки таблиц PostgreSQL.

URL: http://localhost:8080

Контейнер: team1_pgadmin

Данные для входа:

- Email	admin@example.com
- Password	admin
- Password for DB: postgres
---
🌀 Apache Airflow

Airflow используется для оркестрации загрузки данных и построения витрин.

Web UI: http://localhost:8081

Контейнер: team1_airflow

Executor: LocalExecutor

Данные для входа в Airflow UI:

- Username:	admin
- Password:	admin