using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RabbitMQDemo
{
    public class RabbitMQHelper
    {
        private static ConnectionFactory factory;
        private static object lockObj = new object();
        /// <summary>
        /// 获取单个RabbitMQ连接
        /// </summary>
        /// <returns></returns>
        public static IConnection GetConnection()
        {
            if (factory == null)
            {
                lock (lockObj)
                {
                    if (factory == null)
                    {
                        factory = new ConnectionFactory
                        {
                            HostName = "127.0.0.1",//ip
                            Port = 5672,//端口
                            UserName = "cts",//账号
                            Password = "123123",//密码
                           VirtualHost = "/" //虚拟主机
                        };
                    }
                }
            }
            return factory.CreateConnection();
        }
    }
}
