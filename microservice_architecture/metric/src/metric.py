import pika
import json
import time

# Заголовок для CSV-лога
table_header = 'id,y_true,y_pred,absolute_error'

# Логируем начало работы
try:
    with open('./logs/labels_log.txt', 'a') as log:
        log.write("Log file started\n")
    with open('./logs/metric_log.csv', 'w') as log:
        log.write(table_header + '\n')
except Exception as e:
    print('Error during creating file:', e)

# Списки для хранения данных
true_dicts = []
pred_dicts = []

def log_metrics(true_dict, pred_dict):
    """ Логируем метрики в CSV файл """
    id = true_dict['id']
    y_true = true_dict['body']
    y_pred = pred_dict['body']
    absolute_error = abs(y_true - y_pred)

    with open('./logs/metric_log.csv', 'a') as log:
        log.write(f'{id},{y_true},{y_pred},{absolute_error}\n')

    print(f"Pair found: {id} (y_true: {y_true}, y_pred: {y_pred}, absolute_error: {absolute_error})")

def process_dict(true_dict):
    """ Обрабатываем словарь правды, ищем соответствующие предсказания """
    id = true_dict['id']
    body = true_dict['body']
    print(f'Processing true dict - id: {id}, body: {body}')

    # Находим пару в предсказаниях
    matching_pred = next((pred for pred in pred_dicts if pred['id'] == id), None)
    if matching_pred:
        log_metrics(true_dict, matching_pred)
        pred_dicts.remove(matching_pred)  # Удаляем, чтобы избежать дубликатов
    else:
        true_dicts.append(true_dict)  # Добавляем в список, если пары нет

def process_pred(pred_dict):
    """ Обрабатываем предсказание, ищем соответствующие значения правды """
    id = pred_dict['id']
    body = pred_dict['body']
    print(f'Processing pred dict - id: {id}, body: {body}')

    # Находим пару в истинных значениях
    matching_true = next((true for true in true_dicts if true['id'] == id), None)
    if matching_true:
        log_metrics(matching_true, pred_dict)
        true_dicts.remove(matching_true)  # Удаляем, чтобы избежать дубликатов
    else:
        pred_dicts.append(pred_dict)  # Добавляем в список, если пары нет

# Основной цикл для подключения к очередям
print("Starting metric logging...")
while True:
    try:
        # Подключаемся к RabbitMQ
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
        channel = connection.channel()

        # Объявляем очереди
        channel.queue_declare(queue='y_true')
        channel.queue_declare(queue='y_pred')

        # Функции обратного вызова для обработки сообщений
        channel.basic_consume(
            queue='y_true',
            on_message_callback=lambda ch, method, properties, body: process_dict(json.loads(body)),
            auto_ack=True
        )

        channel.basic_consume(
            queue='y_pred',
            on_message_callback=lambda ch, method, properties, body: process_pred(json.loads(body)),
            auto_ack=True
        )
        
        print('...Waiting for messages, press CTRL+C to exit')
        channel.start_consuming()

    except pika.exceptions.AMQPConnectionError as e:
        time.sleep(10)
        print(f'Could not connect to RabbitMQ: {e}')
    except Exception as e:
        print(f'Error: {e}')