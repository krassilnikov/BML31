import pika
import pickle
import numpy as np
import json

print("Model started")

# Загружаем сериализованную модель
with open('myfile.pkl', 'rb') as pkl_file:
    regressor = pickle.load(pkl_file)

def process_features(ch, method, properties, body):
    """Обрабатывает входные данные из очереди и отправляет предсказания в другую очередь."""
    print(f'\n\nReceived feature vector: {body}\n')
    
    # Декодируем входные данные
    features_set = json.loads(body)
    message_id = features_set['id']
    features = np.array(features_set['body']).reshape(1, -1)
    
    # Генерируем предсказание
    prediction = regressor.predict(features)[0]

    # Формируем сообщение для отправки
    message_y_pred = {
        'id': message_id,
        'body': prediction
    }

    # Отправляем предсказание в очередь y_pred
    ch.basic_publish(
        exchange='',
        routing_key='y_pred',
        body=json.dumps(message_y_pred)
    )
    print(f'Sent prediction {prediction} to queue y_pred')

def main():
    while True:
        try:
            # Настраиваем подключение к RabbitMQ
            connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
            channel = connection.channel()

            # Объявляем очереди
            channel.queue_declare(queue='features')
            channel.queue_declare(queue='y_pred')

            # Начинаем прослушивание очереди features
            channel.basic_consume(
                queue='features',
                on_message_callback=process_features,
                auto_ack=True
            )
            print('...Waiting for messages. To exit, press CTRL+C')
            channel.start_consuming()

        except Exception as e:
            print('Failed to connect to the queue:', e)

if __name__ == "__main__":
    main()