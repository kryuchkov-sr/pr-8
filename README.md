# Практическая работа 8. Анализ метода загрузки данных

## Цель:
Определить наиболее эффективный метод загрузки данных (малых и больших объемов) из CSV-файлов в СУБД PostgreSQL, сравнивая время выполнения для методов: pandas.to_sql(), psycopg2.copy_expert() (с файлом и с io.StringIO), и пакетная вставка (psycopg2.extras.execute_values).

## Задачи:

1.  **Подключиться** к предоставленной базе данных PostgreSQL.
{
      "cell_type": "code",
      "execution_count": 5,
      "id": "Yh_PVEybaJnK",
      "metadata": {
        "id": "Yh_PVEybaJnK"
      },
      "outputs": [
        {
          "name": "stdout",
          "output_type": "stream",
          "text": [
            "Libraries installed and imported successfully.\n"
          ]
        }
      ],
      "source": [
        "\n",
        "# @markdown Установка и импорт необходимых библиотек.\n",
        "\n",
        "\n",
        "print(\"Libraries installed and imported successfully.\")\n",
        "\n",
        "# Database connection details (replace with your actual credentials if different)\n",
        "DB_USER = \"postgres\"\n",
        "DB_PASSWORD = \"111\"\n",
        "DB_HOST = \"localhost\"\n",
        "DB_PORT = \"5432\"\n",
        "DB_NAME = \"lect_08_bda_big_data\"\n",
        "\n",
        "# CSV File Paths (Ensure these files are uploaded to your Colab environment)\n",
        "small_csv_path = 'upload_test_data.csv'\n",
        "big_csv_path = 'upload_test_data_big.csv' # Corrected filename\n",
        "\n",
        "# Table name in PostgreSQL\n",
        "table_name = 'sales_data'"
      ]
    },
    {
      "cell_type": "code",
      "execution_count": 6,
      "id": "dnkk57BaaJvv",
      "metadata": {
        "id": "dnkk57BaaJvv"
      },
      "outputs": [
        {
          "name": "stdout",
          "output_type": "stream",
          "text": [
            "Connecting to PostgreSQL database using psycopg2...\n",
            "PostgreSQL server information:\n",
            "{'user': 'postgres', 'channel_binding': 'prefer', 'dbname': 'lect_08_bda_big_data', 'host': 'localhost', 'port': '5432', 'options': '', 'sslmode': 'prefer', 'sslcompression': '0', 'sslcertmode': 'allow', 'sslsni': '1', 'ssl_min_protocol_version': 'TLSv1.2', 'gssencmode': 'disable', 'krbsrvname': 'postgres', 'gssdelegation': '0', 'target_session_attrs': 'any', 'load_balance_hosts': 'disable'} \n",
            "\n",
            "Successfully connected to: PostgreSQL 17.3 on x86_64-windows, compiled by msvc-19.42.34436, 64-bit\n",
            "\n",
            "Creating SQLAlchemy engine...\n",
            "SQLAlchemy engine created successfully.\n"
          ]
        }
      ],
      "source": [
        "# @title # 3. Database Connection Test\n",
        "# @markdown Проверка соединения с базой данных PostgreSQL.\n",
        "\n",
        "connection = None\n",
        "cursor = None\n",
        "engine = None # For pandas.to_sql\n",
        "\n",
        "try:\n",
        "    # Establish connection using psycopg2\n",
        "    print(\"Connecting to PostgreSQL database using psycopg2...\")\n",
        "    connection = psycopg2.connect(user=DB_USER,\n",
        "                                  password=DB_PASSWORD,\n",
        "                                  host=DB_HOST,\n",
        "                                  port=DB_PORT,\n",
        "                                  database=DB_NAME)\n",
        "    connection.autocommit = False # Important for COPY and batch inserts within transactions\n",
        "    cursor = connection.cursor()\n",
        "\n",
        "    print(\"PostgreSQL server information:\")\n",
        "    print(connection.get_dsn_parameters(), \"\\n\")\n",
        "    cursor.execute(\"SELECT version();\")\n",
        "    record = cursor.fetchone()\n",
        "    print(f\"Successfully connected to: {record[0]}\\n\")\n",
        "\n",
        "    # Create SQLAlchemy engine for pandas\n",
        "    print(\"Creating SQLAlchemy engine...\")\n",
        "    engine_url = f\"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}\"\n",
        "    engine = create_engine(engine_url)\n",
        "    print(\"SQLAlchemy engine created successfully.\")\n",
        "\n",
        "\n",
        "except (Exception, Error) as error:\n",
        "    print(f\"Error while connecting to PostgreSQL: {error}\")\n",
        "    # Ensure resources are closed even if connection fails partially\n",
        "    if cursor:\n",
        "        cursor.close()\n",
        "    if connection:\n",
        "        connection.close()\n",
        "    if engine:\n",
        "        engine.dispose() # Close SQLAlchemy engine pool\n",
        "    connection, cursor, engine = None, None, None # Reset variables\n",
        "\n",
        "# We keep the connection open for the rest of the script.\n",
        "# It will be closed in the final step."
      ]
    },
    {
      "cell_type": "code",
      "execution_count": 7,
      "id": "yOT4zuqDaJyo",
      "metadata": {
        "id": "yOT4zuqDaJyo"
      },
      "outputs": [
        {
          "name": "stdout",
          "output_type": "stream",
          "text": [
            "--- Analyzing upload_test_data.csv ---\n",
            "First 5 rows:\n",
            "   id  quantity  cost  total_revenue\n",
            "0   1        33  0.50          16.50\n",
            "1   2        16  6.92         110.72\n",
            "2   3        17  7.77         132.09\n",
            "3   4        17  7.73         131.41\n",
            "4   5         9  3.07          27.63\n",
            "\n",
            "Data Types (inferred by Pandas):\n",
            "<class 'pandas.core.frame.DataFrame'>\n",
            "RangeIndex: 5 entries, 0 to 4\n",
            "Data columns (total 4 columns):\n",
            " #   Column         Non-Null Count  Dtype  \n",
            "---  ------         --------------  -----  \n",
            " 0   id             5 non-null      int64  \n",
            " 1   quantity       5 non-null      int64  \n",
            " 2   cost           5 non-null      float64\n",
            " 3   total_revenue  5 non-null      float64\n",
            "dtypes: float64(2), int64(2)\n",
            "memory usage: 292.0 bytes\n",
            "None\n",
            "\n",
            "--- Analyzing upload_test_data_big.csv ---\n",
            "First 5 rows:\n",
            "   id  quantity  cost  total_revenue\n",
            "0   1        33  0.50          16.50\n",
            "1   2        16  6.92         110.72\n",
            "2   3        17  7.77         132.09\n",
            "3   4        17  7.73         131.41\n",
            "4   5         9  3.07          27.63\n",
            "\n",
            "Data Types (inferred by Pandas):\n",
            "<class 'pandas.core.frame.DataFrame'>\n",
            "RangeIndex: 5 entries, 0 to 4\n",
            "Data columns (total 4 columns):\n",
            " #   Column         Non-Null Count  Dtype  \n",
            "---  ------         --------------  -----  \n",
            " 0   id             5 non-null      int64  \n",
            " 1   quantity       5 non-null      int64  \n",
            " 2   cost           5 non-null      float64\n",
            " 3   total_revenue  5 non-null      float64\n",
            "dtypes: float64(2), int64(2)\n",
            "memory usage: 292.0 bytes\n",
            "None\n"
          ]
        }
      ],
3.  **Проанализировать структуру** исходных CSV-файлов ([upload_test_data.csv](https://github.com/BosenkoTM/SQL-for-Begginer-Data-Analytics/blob/main/practice/pr-5-2-upload-data-from-pandas-to-sql-main/upload_test_data.csv) , [upload_test_data_big.csv](https://github.com/BosenkoTM/SQL-for-Begginer-Data-Analytics/blob/main/practice/pr-5-2-upload-data-from-pandas-to-sql-main/upload_test_data_big.csv)).
4.  **Создать эскизы ER-диаграмм** для таблиц, соответствующих структуре CSV-файлов.
5.  **Реализовать** три различных метода загрузки данных в PostgreSQL(**pandas.to_sql()**, **copy_expert()**, **io.StringIO**).
6.  **Измерить время**, затраченное каждым методом на загрузку данных из малого файла (`upload_test_data.csv`).
7.  **Измерить время**, затраченное каждым методом на загрузку данных из большого файла (`upload_test_data_big.csv`).
8.  **Визуализировать** результаты сравнения времени загрузки с помощью гистограммы (`matplotlib`).
9.  **Сделать выводы** об эффективности каждого метода для разных объемов данных.


Результаты представить в виде отдельного файла ERD-диаграммы для простых и больших данных из PostgreSQL с расширением `FIO_ERD_test_data.jpg`, `FIO_ERD_test_data_big.jpg`;
Ссылку на дашборд в `Yandex DataLens` или файл с расширением `.ipynb`.

## Индивидуальные задания.

| Вариант | Задание 1: Настройка таблиц | Задание 2: Загрузка малых данных (`sales_small`) | Задание 3: Загрузка больших данных (`sales_big`) | Задание 4: SQL Анализ | Задание 5: Python/Colab Анализ и Визуализация |
| :------ | :-------------------------- | :----------------------------------------------- | :------------------------------------------------ | :-------------------- | :---------------------------------------------- |
| **14**  | Создать таблицы `sales_small`, `sales_big`. | Метод: `copy_expert (file)`                    | Метод: `copy_expert (file)`                       | SQL: Выбрать `id`, `cost` для 5 записей с самой низкой `cost` (>0) в `sales_big`. | Python: Построить гистограмму `cost` из `sales_small`. |
