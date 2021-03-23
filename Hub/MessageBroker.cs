using ConsensusProject.App;
using ConsensusProject.Messages;
using Google.Protobuf;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;

namespace Hub
{
    public class MessageBroker
    {
        private ConcurrentDictionary<string, Message> _messagesMap = new ConcurrentDictionary<string, Message>();
        private AppLogger _logger;
        private TcpListener _server;
        private Config _config;

        public MessageBroker(Config config)
        {
            _config = config;
            IPAddress localAddr = IPAddress.Parse(_config.HubIpAddress);
            _server = new TcpListener(localAddr, _config.HubPort);
            _logger = new AppLogger(config, "hub");

            new Thread(() => {
                _server.Start();
                StartListener();
            }).Start();
        }

        public void StartListener()
        {
            try
            {
                _logger.LogInfo($"Listening to {_config.HubIpAddress}:{_config.HubPort}");
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

                _logger.LogInfo($"Received from {message.NetworkMessage.SenderHost}:{message.NetworkMessage.SenderListeningPort} a message.");

                EnqueMessage(message);

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

        public void DequeMessage(Message message)
        {
            if (!_messagesMap.TryRemove(message.MessageUuid, out _)) throw new Exception("Error removing the message.");
        }

        public void EnqueMessage(Message message)
        {
            if (!_messagesMap.TryAdd(message.MessageUuid, message)) throw new Exception("Error adding the message.");
        }

        public ICollection<Message> Messages
        {
            get { return _messagesMap.Values; }
        }
    }
}
