using ConsensusProject.App;
using ConsensusProject.Messages;
using Google.Protobuf;
using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;

namespace ConsensusProject.Abstractions
{
    public class PerfectLink : Abstraction
    {
        private string _id;
        private Config _config;
        private AppLogger _logger;
        private TcpListener _server = null;
        private AppProccess _appProccess;

        public PerfectLink(string id, AppProccess appProccess, Config c)
        {
            _id = id;
            _config = c;
            _appProccess = appProccess;
            _logger = new AppLogger(_config, id);

            IPAddress localAddr = IPAddress.Parse(_config.NodeIpAddress);
            _server = new TcpListener(localAddr, _config.NodePort);
            
            new Thread(() => {
                _server.Start();
                StartListener(); 
            }).Start();
        }

        public bool Handle(Message message)
        {
            Message networkMessage;
            string destinationHost;
            int destinationPort;
            if(message.Type == Message.Types.Type.PlSend)
            {
                destinationHost = message.PlSend.Destination.Host;
                destinationPort = message.PlSend.Destination.Port;

                networkMessage = new Message
                {
                    MessageUuid = Guid.NewGuid().ToString(),
                    AbstractionId = message.AbstractionId,
                    SystemId = message.SystemId,
                    Type = Message.Types.Type.NetworkMessage,

                    NetworkMessage = new NetworkMessage
                    {
                        SenderHost = _config.NodeIpAddress,
                        SenderListeningPort = _config.NodePort,
                        Message = message.PlSend.Message,
                    },
                };
            }
            else if(message.Type == Message.Types.Type.AppRegistration || message.Type == Message.Types.Type.AppDecide)
            {
                destinationHost = _config.HubIpAddress;
                destinationPort = _config.HubPort;

                networkMessage = new Message
                {
                    MessageUuid = Guid.NewGuid().ToString(),
                    Type = Message.Types.Type.NetworkMessage,
                    SystemId = message.SystemId,

                    NetworkMessage = new NetworkMessage
                    {
                        SenderHost = _config.NodeIpAddress,
                        SenderListeningPort = _config.NodePort,
                        Message = message,
                    },
                };
            }
            else
            {
                return false;
            }

            return SendMessage(networkMessage, destinationHost, destinationPort);
        }

        public bool SendMessage(Message message, string host, int port)
        {
            try
            {
                _logger.LogInfo(message.Type.ToString());
                using (TcpClient client = new TcpClient(host, port))
                {
                    using (var ms = new MemoryStream())
                    using (NetworkStream stream = client.GetStream())
                    {
                        var byteArray = message.ToByteArray();

                        var bufferLength = BitConverter.GetBytes(byteArray.Length);
                        Array.Reverse(bufferLength);
                        var finalArray = bufferLength.Concat(byteArray).ToArray();
                        stream.Write(finalArray, 0, finalArray.Length);
                    }
                }
                _logger.LogInfo($"Sent to {host}:{port} a message ");
                return true;
            }
            catch (Exception e)
            {
                _logger.LogError($"Exception: {e}");
                return false;
            }
        }

        public void StartListener()
        {
            try
            {
                while (true)
                {
                    TcpClient client = _server.AcceptTcpClient();
                    HandleDeivce(client);
                }
            }
            catch (SocketException e)
            {
                _logger.LogError($"SocketException: {e}");
                _server.Stop();
            }
        }
        private void HandleDeivce(Object obj)
        {
            TcpClient client = (TcpClient)obj;
            try
            {
                Message message;
                using (NetworkStream stream = client.GetStream())
                {
                    byte[] bufferLength = new byte[4];
                    stream.Read(bufferLength, 0, 4);
                    Array.Reverse(bufferLength);

                    var length = BitConverter.ToInt32(bufferLength);

                    byte[] objectArray = new byte[length];

                    stream.Read(objectArray, 0, length);

                    message = Message.Parser.ParseFrom(objectArray);
                }

                Message newMessage;
                if (message.NetworkMessage.Message.Type == Message.Types.Type.AppPropose)
                {
                    newMessage = message.NetworkMessage.Message;
                }
                else
                {
                    newMessage = new Message
                    {
                        MessageUuid = message.MessageUuid,
                        SystemId = message.SystemId,
                        AbstractionId = message.AbstractionId,
                        Type = Message.Types.Type.PlDeliver,

                        PlDeliver = new PlDeliver
                        {
                            Message = message.NetworkMessage.Message,
                            Sender = new ProcessId
                            {
                                Host = message.NetworkMessage.SenderHost,
                                Port = message.NetworkMessage.SenderListeningPort,
                            }
                        },
                    };
                }
                
                
                _logger.LogInfo($"Received from {message.NetworkMessage.SenderHost}:{message.NetworkMessage.SenderListeningPort} a message.");
                
                _appProccess.EnqueMessage(newMessage);
                
            }
            catch (Exception e)
            {
                _logger.LogError($"Exception: {e}");
            }
            finally
            {
                client.Close();
            }
        }
    }
}
