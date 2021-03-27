using ConsensusProject.App;
using ConsensusProject.Messages;
using ConsensusProject.Utils;
using Google.Protobuf;
using System;
using System.Net.Sockets;
using System.Threading;

namespace ConsensusProject.Abstractions
{
    public class PerfectLink : Abstraction
    {
        private string _id;
        private Config _config;
        private AppLogger _logger;
        private AppProccess _appProccess;
        private TcpWrapper _tcpWrapper;

        public PerfectLink(string id, AppProccess appProccess, Config c)
        {
            _id = id;
            _config = c;
            _appProccess = appProccess;
            _logger = new AppLogger(_config, id);

            _tcpWrapper = new TcpWrapper(_config.NodeIpAddress, _config.NodePort);
            new Thread(() => {
                _tcpWrapper.Start();
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
                var byteArray = message.ToByteArray();
               
                _tcpWrapper.Send(host, port, byteArray);

                if (message.NetworkMessage.Message.Type != Message.Types.Type.EpfdHeartbeatReply && message.NetworkMessage.Message.Type != Message.Types.Type.EpfdHeartbeatRequest)
                    _logger.LogInfo($"Sent to {host}:{port} a {message.NetworkMessage.Message.Type} message ");

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
                    var client = _tcpWrapper.Receive();
                    HandleDeivce(client);
                }
            }
            catch (SocketException e)
            {
                _logger.LogError($"SocketException: {e}");
            }
        }
        private void HandleDeivce(byte[] byteContent)
        {
            try
            {
                Message message = Message.Parser.ParseFrom(byteContent);

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
                
                if (message.NetworkMessage.Message.Type != Message.Types.Type.EpfdHeartbeatReply && message.NetworkMessage.Message.Type != Message.Types.Type.EpfdHeartbeatRequest)
                    _logger.LogInfo($"Received from {message.NetworkMessage.SenderHost}:{message.NetworkMessage.SenderListeningPort} a {message.NetworkMessage.Message.Type} message.");
                
                _appProccess.EnqueMessage(newMessage);
                
            }
            catch (Exception e)
            {
                _logger.LogError($"Exception: {e}");
            }
        }
    }
}
