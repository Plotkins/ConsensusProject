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
        private TcpWrapper _tcpWrapper;
        private MessageBroker _messageBroker;

        public PerfectLink(string id, Config c, AppProcess appProcess, MessageBroker messageBroker)
        {
            _id = id;
            _config = c;
            _logger = new AppLogger(_config, _id, appProcess.Id);
            _messageBroker = messageBroker;
            _messageBroker.Subscribe(appProcess.Id, _id, Handle);
            _tcpWrapper = new TcpWrapper(_config.NodeIpAddress, _config.NodePort);
            new Thread(() => {
                _tcpWrapper.Start();
                StartListener(); 
            }).Start();
        }

        public bool Handle(Message message)
        {
            switch(message)
            {
                case Message m when m.Type == Message.Types.Type.PlSend:
                    return HandlePlSend(m);
                case Message m when m.Type == Message.Types.Type.BebSend:
                    return HandleBebSend(m);
                default:
                    return false;
            }
        }

        public bool HandlePlSend(Message message)
        {
            string destinationHost = message.PlSend.Destination.Host;
            int destinationPort = message.PlSend.Destination.Port;

            Message networkMessage = new Message
            {
                MessageUuid = Guid.NewGuid().ToString(),
                AbstractionId = message.PlSend.Message.AbstractionId,
                SystemId = message.PlSend.Message.SystemId,
                Type = Message.Types.Type.NetworkMessage,

                NetworkMessage = new NetworkMessage
                {
                    SenderHost = _config.NodeIpAddress,
                    SenderListeningPort = _config.NodePort,
                    Message = message.PlSend.Message,
                },
            };
            return SendMessage(networkMessage, destinationHost, destinationPort);
        }

        public bool HandleBebSend(Message message)
        {
            string destinationHost = message.BebSend.Destination.Host;
            int destinationPort = message.BebSend.Destination.Port;

            Message networkMessage = new Message
            {
                MessageUuid = Guid.NewGuid().ToString(),
                AbstractionId = AbstractionType.Beb.ToString(),
                SystemId = message.SystemId,
                Type = Message.Types.Type.NetworkMessage,

                NetworkMessage = new NetworkMessage
                {
                    SenderHost = _config.NodeIpAddress,
                    SenderListeningPort = _config.NodePort,
                    Message = message.BebSend.Message,
                },
            };
            return SendMessage(networkMessage, destinationHost, destinationPort);
        }

        public bool SendMessage(Message message, string host, int port)
        {
            try
            {
                var byteArray = message.ToByteArray();
               
                _tcpWrapper.Send(host, port, byteArray).Wait();

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

                Message newMessage = new Message
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
                
                if (message.NetworkMessage.Message.Type != Message.Types.Type.EpfdHeartbeatReply && message.NetworkMessage.Message.Type != Message.Types.Type.EpfdHeartbeatRequest)
                    _logger.LogInfo($"Received from {message.NetworkMessage.SenderHost}:{message.NetworkMessage.SenderListeningPort} a {message.NetworkMessage.Message.Type} message.");
                
                _messageBroker.SendMessage(newMessage);
                
            }
            catch (Exception e)
            {
                _logger.LogError($"Exception: {e}");
            }
        }
    }
}
