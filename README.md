## Работа с Clickhouse
1. Установить [Docker Desktop](https://www.docker.com/products/docker-desktop/) и [DBeaver](https://dbeaver.io/download/)
1. [Скачать репозиторий](https://github.com/semcha/netology-pwh/archive/refs/heads/master.zip) и разархивировать, либо склонировать его себе с помощью git-клиента (рекомендую [fork](https://fork.dev/))
1. Перейти в папку clickhouse `cd ./clickhouse`
1. Запустить контейнер с Clickhouse командой `docker-compose up -d`
1. Подключиться к Clickhouse с помощью DBeaver:
    - Тип подключения: Clickhouse
    - Хост: localhost
    - База данных: raw_layer
    - Имя пользователя: admin
    - Пароль: admin
1. Выполнить скрипт `clickhouse_data.sql` для загрузки исходных данных в слой `raw_layer`
1. Выполнить скрипт `clickhouse_lecture.sql` по шагам для закрепления материала
1. Остановить и удалить контейнер можно c помощью интерфейса Docker Desktop (вкладка Containers)