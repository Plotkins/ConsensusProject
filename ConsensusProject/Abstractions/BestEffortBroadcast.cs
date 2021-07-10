using ConsensusProject.App;
using ConsensusProject.Messages;
using ConsensusProject.Utils;
using System;
using System.Collections.Generic;
using System.Linq;

namespace ConsensusProject.Abstractions
{
    public class BestEffortBroadcast : Abstraction
    {
        private string _id;
        private AppProcess _appProcess;
        private Config _config;
        private AppLogger _logger;
        private MessageBroker _messageBroker;

        public BestEffortBroadcast(string id, Config config, AppProcess appProcess, MessageBroker messageBroker)
        {
            _id = id;
            _config = config;
            _logger = new AppLogger(_config, _id, appProcess.Id);
            _appProcess = appProcess;
            _messageBroker = messageBroker;
            _messageBroker.Subscribe(appProcess.Id, _id, Handle);
        }

        public bool Handle(Message message)
        {
            switch (message)
            {
                case Message m when m.Type == Message.Types.Type.BebBroadcast:
                    return HandleBebBroadcast(m);
                case Message m when m.Type == Message.Types.Type.PlDeliver:
                    return HandlePlDeliver(m);
                default:
                    return false;
            }
        }

        private bool HandleBebBroadcast(Message message)
        {
            _logger.LogInfo($"Handling BebBroadcast of Message type {message.BebBroadcast.Message.Type}");

            List<ProcessId> processesToBroadcast = new List<ProcessId>();
            if (message.BebBroadcast.Type == BebBroadcast.Types.Type.IntraShard)
            {
                processesToBroadcast = _appProcess.ShardNodes;
            }
            else if (message.BebBroadcast.Type == BebBroadcast.Types.Type.InterShard)
            {
                processesToBroadcast = _appProcess.NetworkNodes.Where(it => it.Owner != _config.Alias).ToList();
            }
            else if (message.BebBroadcast.Type == BebBroadcast.Types.Type.Network)
            {
                processesToBroadcast = _appProcess.NetworkNodes;
            }
            else if (message.BebBroadcast.Type == BebBroadcast.Types.Type.Custom)
            {
                processesToBroadcast = message.BebBroadcast.Processes.ToList();
            }

            foreach (var proccess in processesToBroadcast)
            {
                Message messageToSend = null;
                if (!proccess.Equals(_appProcess.CurrentProccess))
                {
                    messageToSend = new Message
                    {
                        MessageUuid = Guid.NewGuid().ToString(),
                        SystemId = message.SystemId,
                        AbstractionId = AbstractionType.Pl.ToString(),
                        Type = Message.Types.Type.BebSend,

                        BebSend = new BebSend
                        {
                            Message = message.BebBroadcast.Message,
                            Destination = proccess,
                        }
                    };
                }
                else
                {
                    messageToSend = new Message
                    {
                        MessageUuid = Guid.NewGuid().ToString(),
                        AbstractionId = message.BebBroadcast.Message.AbstractionId,
                        SystemId = message.BebBroadcast.Message.SystemId,
                        Type = Message.Types.Type.BebDeliver,
                        BebDeliver = new BebDeliver
                        {
                            Sender = _appProcess.CurrentProccess,
                            Message = message.BebBroadcast.Message,
                        }
                    };
                }

                _messageBroker.SendMessage(messageToSend);
            }
            return true;
        }

        private bool HandlePlDeliver(Message message)
        {
            Message bebDeliver = new Message
            {
                MessageUuid = Guid.NewGuid().ToString(),
                AbstractionId = message.PlDeliver.Message.AbstractionId,
                SystemId = message.PlDeliver.Message.SystemId,
                Type = Message.Types.Type.BebDeliver,
                BebDeliver = new BebDeliver
                {
                    Sender = message.PlDeliver.Sender,
                    Message = message.PlDeliver.Message,
                }
            };

            _messageBroker.SendMessage(bebDeliver);

            return true;
        }
    }
}
