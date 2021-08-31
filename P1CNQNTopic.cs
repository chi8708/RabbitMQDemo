using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RabbitMQDemo
{
    //主题模式  生产者1  消费者N 队列N
    class P1CNQNTopic
    {
        public static void TopicSendMsg()
        {
            //创建连接
            using (var connection = RabbitMQHelper.GetConnection())
            {
                //创建信道
                using (var channel = connection.CreateModel())
                {
                    //声明交换机对象,fanout类型
                    string exchangeName = "P1CNQNTopic_exchange";
                    channel.ExchangeDeclare(exchangeName, ExchangeType.Topic);
                    //队列名
                    string queueName1 = "P1CNQNTopic_queue1";
                    string queueName2 = "P1CNQNTopic_queue2";
                    //路由名
                    string routingKey1 = "*.orange.*";
                    string routingKey2 = "*.*.rabbit";
                    string routingKey3 = "lazy.#";
                    channel.QueueDeclare(queueName1, true, false, false);
                    channel.QueueDeclare(queueName2, true, false, false);

                    //把创建的队列绑定交换机,routingKey指定routingKey
                    channel.QueueBind(queue: queueName1, exchange: exchangeName, routingKey: routingKey1);
                    channel.QueueBind(queue: queueName2, exchange: exchangeName, routingKey: routingKey2);
                    channel.QueueBind(queue: queueName2, exchange: exchangeName, routingKey: routingKey3);
                    //向交换机写10条消息
                    for (int i = 0; i < 10; i++)
                    {
                        string message = $"RabbitMQ Direct {i + 1} Message";
                        var body = Encoding.UTF8.GetBytes(message);
                        channel.BasicPublish(exchangeName, routingKey: "aaa.orange.rabbit", null, body);
                        channel.BasicPublish(exchangeName, routingKey: "lazy.aa.rabbit", null, body);
                        Console.WriteLine($"发送Topic消息:{message}");
                    }
                }
            }
        }

        public static void TopicConsumer()
        {
            string queueName = "P1CNQNTopic_queue1";
            var connection = RabbitMQHelper.GetConnection();
            {
                //创建信道
                var channel = connection.CreateModel();
                {
                    //创建队列
                    channel.QueueDeclare(queueName, durable: true, exclusive: false, autoDelete: false, arguments: null);
                    var consumer = new EventingBasicConsumer(channel);
                    //prefetchCount:1来告知RabbitMQ,不要同时给一个消费者推送多于 N 个消息，也确保了消费速度和性能
                    channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);
                    int i = 1;
                    int index = new Random().Next(10);
                    consumer.Received += (model, ea) =>
                    {
                        //处理业务
                        var message = Encoding.UTF8.GetString(ea.Body.ToArray());
                        Console.WriteLine($"{i},消费者:{index},队列{queueName}消费消息长度:{message.Length},内容:{message}");
                        channel.BasicAck(ea.DeliveryTag, false); //消息ack确认，告诉mq这条队列处理完，可以从mq删除了
                        i++;
                    };
                    channel.BasicConsume(queueName, autoAck: false, consumer);
                }
            }
        }
    }
}
