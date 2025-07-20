import pika

# 替换为RabbitMQ服务器的公网IP
rabbitmq_host = '106.120.188.128'

connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host))
channel = connection.channel()

# 声明队列（如果不存在则自动创建）
channel.queue_declare(queue='sub1_queue')
channel.queue_declare(queue='sub2_queue')
channel.queue_declare(queue='sub3_queue')
channel.queue_declare(queue='sub4_queue')
channel.queue_declare(queue='sub5_queue')
channel.queue_declare(queue='sub6_queue')
channel.queue_declare(queue='sub7_queue')
channel.queue_declare(queue='sub8_queue')
channel.queue_declare(queue='sub9_queue')
channel.queue_declare(queue='sub10_queue')
channel.queue_declare(queue='sub11_queue')
channel.queue_declare(queue='sub12_queue')
channel.queue_declare(queue='sub13_queue')
channel.queue_declare(queue='meta_queue')

# 发送消息,消息内容根据发送主机修改
mes='Hello from meta'
print("[x] Sent 'Hello from meta!'")
channel.basic_publish(exchange='',
                      routing_key='sub1_queue',
                      body=mes)
channel.basic_publish(exchange='',
                      routing_key='sub2_queue',
                      body=mes)
channel.basic_publish(exchange='',
                      routing_key='sub3_queue',
                      body=mes)
channel.basic_publish(exchange='',
                      routing_key='sub4_queue',
                      body=mes)
channel.basic_publish(exchange='',
                      routing_key='sub5_queue',
                      body=mes)
channel.basic_publish(exchange='',
                      routing_key='sub6_queue',
                      body=mes)
channel.basic_publish(exchange='',
                      routing_key='sub7_queue',
                      body=mes)
channel.basic_publish(exchange='',
                      routing_key='sub8_queue',
                      body=mes)
channel.basic_publish(exchange='',
                      routing_key='sub9_queue',
                      body=mes)
channel.basic_publish(exchange='',
                      routing_key='sub10_queue',
                      body=mes)
channel.basic_publish(exchange='',
                      routing_key='sub11_queue',
                      body=mes)
channel.basic_publish(exchange='',
                      routing_key='sub12_queue',
                      body=mes)
channel.basic_publish(exchange='',
                      routing_key='sub13_queue',
                      body=mes)
channel.basic_publish(exchange='',
                      routing_key='meta_queue',
                      body=mes)


connection.close()

