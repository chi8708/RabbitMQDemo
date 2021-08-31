using System;

namespace RabbitMQDemo
{
    class Program
    {
        static void Main(string[] args)
        {
            //简单模式
            // P1C1.SimpleSendMsg();
            // P1C1.SimpleConsumer();

            //工作者模式
            // P1CN.WorkerSendMsg();
            // P1CN.WorkerConsumer();

            //发布订阅模式
            //P1CNQN.FanoutSendMsg();
            //P1CNQN.FanoutConsumer1();
            //P1CNQN.FanoutConsumer2();

            //路由模式
            // P1CNQNRoute.DirectSendMsg();
            // P1CNQNRoute.DirectConsumer();

            //主题模式
            // P1CNQNTopic.TopicSendMsg();
            // P1CNQNTopic.TopicConsumer();

            //RPC模式
            //启动服务端，正常逻辑是在另一个程序
            RPCServer.RpcHandle();
            //实例化客户端
            var rpcClient = new RPCClient();
            string message = $"消息id:{new Random().Next(1, 1000)}";
            Console.WriteLine($"【客服端】RPC请求中，{message}");
            //向服务端发送消息，等待回复
            var response = rpcClient.Call(message);
            Console.WriteLine("【客服端】收到回复响应:{0}", response);
            rpcClient.Close();
            Console.ReadKey();


            Console.ReadKey();
        }
    }
}
