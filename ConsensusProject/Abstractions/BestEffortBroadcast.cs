using ConsensusProject.App;
using ConsensusProject.Messages;
using System;
using System.Collections.Generic;
using System.Linq;

namespace ConsensusProject.Abstractions
{
    public class BestEffortBroadcast : Abstraction
    {
        private string _id;
        private AppProccess _appProcces;
        private Config _config;
        private AppLogger _logger;

        public BestEffortBroadcast(string id, Config config, AppProccess appProcess)
        {
            _id = id;
            _config = config;
            _logger = new AppLogger(_config, _id);
            _appProcces = appProcess;
        }

        public bool Handle(Message message)
        {
            if (message.Type == Message.Types.Type.BebBroadcast)
            {
                _logger.LogInfo($"Handling BebBroadcast of Message type {message.BebBroadcast.Message.Type}");

                List<ProcessId> processesToBroadcast = new List<ProcessId>();
                if (message.BebBroadcast.Type == BebBroadcast.Types.Type.IntraShard)
                {
                    processesToBroadcast = _appProcces.ShardNodes;
                }
                else if (message.BebBroadcast.Type == BebBroadcast.Types.Type.InterShard)
                {
                    processesToBroadcast = _appProcces.NetworkNodes.Where(it => it.Owner != _config.Alias).ToList();
                } else if (message.BebBroadcast.Type == BebBroadcast.Types.Type.Network)
                {
                    processesToBroadcast = _appProcces.NetworkNodes;
                }

                foreach (var proccess in processesToBroadcast)
                {
                    if (!proccess.Equals(_appProcces.CurrentProccess))
                    {
                        var plSend = new Message
                        {
                            MessageUuid = Guid.NewGuid().ToString(),
                            SystemId = message.SystemId,
                            AbstractionId = _id,
                            Type = Message.Types.Type.PlSend,

                            PlSend = new PlSend
                            {
                                Message = message.BebBroadcast.Message,
                                Destination = proccess,
                            }
                        };
                        _logger.LogInfo($"Sending PlSend Message to {proccess.Owner}-{proccess.Index}/{message.SystemId}.");
                        _appProcces.EnqueMessage(plSend);
                    }
                }
                return true;
            }
            else if(message.Type == Message.Types.Type.PlDeliver && message.AbstractionId == "beb")
            {
                Message bebDeliver = new Message
                {
                    MessageUuid = Guid.NewGuid().ToString(),
                    AbstractionId = message.AbstractionId,
                    SystemId = message.SystemId,
                    Type = Message.Types.Type.BebDeliver,
                    BebDeliver = new BebDeliver
                    {
                        Sender = message.PlDeliver.Sender,
                        Message = message.PlDeliver.Message,
                    }
                };
                _appProcces.EnqueMessage(bebDeliver);
                return true;
            }
            return false;
        }
    }
}
