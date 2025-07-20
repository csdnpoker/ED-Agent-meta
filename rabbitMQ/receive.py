import pika

# 替换为RabbitMQ服务器的公网IP
rabbitmq_host = '106.120.188.128'

connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host))
channel = connection.channel()

# 声明队列
channel.queue_declare(queue='meta_queue')

# 消息处理回调函数
def callback(ch, method, properties, body):
    print(f"[x] Received: {body.decode()}")

# 启动监听
channel.basic_consume(queue='meta_queue',
                      on_message_callback=callback,
                      auto_ack=True)

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()