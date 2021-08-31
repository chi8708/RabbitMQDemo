using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RabbitMQDemo
{
    /// <summary>
    /// RPC模式,服务端监听。客户端发消息给服务端 等待回应。
    /// </summary>
    public class RPCServer
    {
        public static void RpcHandle()
        {

            var connection = RabbitMQHelper.GetConnection();
            {
                var channel = connection.CreateModel();
                {
                    string queueName = "rpc_queue";
                    channel.QueueDeclare(queue: queueName, durable: false,
                      exclusive: false, autoDelete: false, arguments: null);
                    channel.BasicQos(0, 1, false);
                    var consumer = new EventingBasicConsumer(channel);
                    channel.BasicConsume(queue: queueName,
                      autoAck: false, consumer: consumer);
                    Console.WriteLine("【服务端】等待RPC请求...");

                    consumer.Received += (model, ea) =>
                    {
                        string response = null;

                        var body = ea.Body.ToArray();
                        var props = ea.BasicProperties;
                        var replyProps = channel.CreateBasicProperties();
                        replyProps.CorrelationId = props.CorrelationId;

                        try
                        {
                            var message = Encoding.UTF8.GetString(body);
                            Console.WriteLine($"【服务端】接收到数据:{ message},开始处理");
                            response = $"消息:{message},处理完成";
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine("错误:" + e.Message);
                            response = "";
                        }
                        finally
                        {
                            var responseBytes = Encoding.UTF8.GetBytes(response);
                            channel.BasicPublish(exchange: "", routingKey: props.ReplyTo,
                              basicProperties: replyProps, body: responseBytes);
                            channel.BasicAck(deliveryTag: ea.DeliveryTag,
                              multiple: false);
                        }
                    };
                }
            }
        }

    }
}
