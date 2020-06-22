using ConsensusProject.App;
using ConsensusProject.Messages;
using System;
using System.Collections.Generic;

namespace ConsensusProject.Abstractions
{
    public class BestEffortBroadcast : Abstraction
    {
        private string _id;
        private AppProccess _appProcces;
        private AppSystem _appSystem;
        private Config _config;
        private AppLogger _logger;
        private List<ProcessId> _systemProcesses;

        public BestEffortBroadcast(string id, Config config, AppProccess appProcess, AppSystem appSystem, List<ProcessId> systemProcesses)
        {
            _id = id;
            _config = config;
            _logger = new AppLogger(_config, _id, appSystem.SystemId);
            _appProcces = appProcess;
            _appSystem = appSystem;
            _systemProcesses = systemProcesses;
        }

        public bool Handle(Message message)
        {
            if (message.Type == Message.Types.Type.BebBroadcast)
            {
                _logger.LogInfo($"Handling BebBroadcast of Message type {message.BebBroadcast.Message.Type}");

                foreach (var proccess in _systemProcesses)
                {
                    var plSend = new Message
                    {
                        MessageUuid = Guid.NewGuid().ToString(),
                        SystemId = _appSystem.SystemId,
                        AbstractionId = _id,
                        Type = Message.Types.Type.PlSend,

                        PlSend = new PlSend
                        {
                            Message = message.BebBroadcast.Message,
                            Destination = proccess,
                        }
                    };

                    _logger.LogInfo($"Sending PlSend Message to {proccess.Owner}-{proccess.Index}/{_appSystem.SystemId}.");
                    _appProcces.EnqueMessage(plSend);
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
