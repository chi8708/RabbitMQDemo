using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RabbitMQDemo
{
    /// 发布订阅模式  生产者1  消费者N 队列N
    /// Pulish/Subscribe，无选择接收消息，一个消息生产者，一个交换机（交换机类型为fanout），多个消息队列，多个消费者。称为发布/订阅模式
    class P1CNQN
    {
        /// <summary>
        /// 发布订阅， 扇形队列
        /// </summary>
        public static void FanoutSendMsg()
        {
            //创建连接
            using (var connection = RabbitMQHelper.GetConnection())
            {
                //创建信道
                using (var channel = connection.CreateModel())
                {
                    string exchangeName = "P1CNQN_exchange";
                    //创建交换机,fanout类型
                    channel.ExchangeDeclare(exchangeName, ExchangeType.Fanout,durable:true);//持久化设置
                    string queueName1 = "P1CNQN_queue1";
                    string queueName2 = "P1CNQN_queue2";
                    string queueName3 = "P1CNQN_queue3";
                    //创建队列
                    channel.QueueDeclare(queueName1, false, false, false);// 消息持久化 需在声明时设置
                    channel.QueueDeclare(queueName2, false, false, false);
                    channel.QueueDeclare(queueName3, false, false, false);

                    //把创建的队列绑定交换机,routingKey不用给值，给了也没意义的
                    channel.QueueBind(queue: queueName1, exchange: exchangeName, routingKey: "");
                    channel.QueueBind(queue: queueName2, exchange: exchangeName, routingKey: "");
                    channel.QueueBind(queue: queueName3, exchange: exchangeName, routingKey: "");
                    var properties = channel.CreateBasicProperties();
                    properties.Persistent = true; //消息持久化 没有作用?
                    //向交换机写10条消息
                    for (int i = 0; i < 10; i++)
                    {
                        string message = $"RabbitMQ Fanout {i + 1} Message";
                        var body = Encoding.UTF8.GetBytes(message);
                        channel.BasicPublish(exchangeName, routingKey: "",basicProperties: properties, body);
                        Console.WriteLine($"发送Fanout消息:{message}");
                    }
                }
            }
        }
        public static void FanoutConsumer1()
        {
            string queueName = "P1CNQN_queue1";
            var connection = RabbitMQHelper.GetConnection();
            {
                //创建信道
                var channel = connection.CreateModel();
                {
                    //创建队列
                    channel.QueueDeclare(queueName, durable: false, exclusive: false, autoDelete: false, arguments: null);
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

        public static void FanoutConsumer2()
        {
            string queueName = "P1CNQN_queue2";
            var connection = RabbitMQHelper.GetConnection();
            {
                //创建信道
                var channel = connection.CreateModel();
                {
                    //创建队列
                    channel.QueueDeclare(queueName, durable: false, exclusive: false, autoDelete: false, arguments: null);
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
