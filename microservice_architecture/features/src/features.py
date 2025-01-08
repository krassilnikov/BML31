import pika
import numpy as np
import json
import time
from datetime import datetime
from sklearn.datasets import load_diabetes

print("Начало работы с данными...")

# Загружаем датасет о диабете один раз перед циклом
X, y = load_diabetes(return_X_y=True)

# Создаём бесконечный цикл для отправки сообщений в очередь
while True:
    try:
        # Генерируем случайный индекс для выборки данных
        random_index = np.random.randint(0, X.shape[0])

        # Устанавливаем подключение к RabbitMQ
        connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
        channel = connection.channel()

        # Обеспечиваем наличие необходимых очередей
        channel.queue_declare(queue='y_true')
        channel.queue_declare(queue='features')

        # Формируем сообщения
        message_id = datetime.timestamp(datetime.now())
        message_y_true = {
            'id': message_id,
            'body': int(y[random_index])  # Приводим к целому числу для ясности
        }

        message_features = {
            'id': message_id,
            'body': X[random_index].tolist()  # Преобразуем в список
        }

        # Отправляем сообщения в очереди
        channel.basic_publish(exchange='', routing_key='y_true', body=json.dumps(message_y_true))
        print('Сообщение с правильным ответом отправлено в очередь')

        channel.basic_publish(exchange='', routing_key='features', body=json.dumps(message_features))
        print('Сообщение с вектором признаков отправлено в очередь')

        # Закрываем подключение
        connection.close()

        # Задержка перед следующей итерацией
        time.sleep(10)
    except Exception as e:
        print('Ошибка подключения к очереди:', e)
        time.sleep(10)