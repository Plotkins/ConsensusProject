using ConsensusProject.App;
using ConsensusProject.Messages;
using ConsensusProject.Utils;
using System;

namespace ConsensusProject.Abstractions
{
    public class EpochChange : Abstraction
    {
        private string _id;
        private AppProcess _appProcess;
        private AppSystem _appSystem;
        private Config _config;
        private AppLogger _logger;
        private MessageBroker _messageBroker;

        //state
        private int _lastTs;
        private int _ts;

        public EpochChange(string id, Config config, AppProcess appProcess, AppSystem appSystem, MessageBroker messageBroker)
        {
            _id = id;
            _config = config;
            _logger = new AppLogger(_config, _id, appSystem.SystemId);
            _appProcess = appProcess;
            _appSystem = appSystem;
            _messageBroker = messageBroker;
            _messageBroker.Subscribe(_appSystem.SystemId, _id, Handle);

            //state
            _lastTs = 0;
            _ts = _appSystem.CurrentProcess.Rank;
        }

        public bool Handle(Message message)
        {
            switch (message)
            {
                case Message m when m.Type == Message.Types.Type.EldTrust:
                    return HandleEldTrust(m);
                case Message m when m.Type == Message.Types.Type.BebDeliver && m.BebDeliver.Message.Type == Message.Types.Type.EcNewEpoch:
                    return HandleEcNewEpoch(m);
                case Message m when m.Type == Message.Types.Type.PlDeliver && m.PlDeliver.Message.Type == Message.Types.Type.EcNack:
                    return HandleEcNach(m);
                default:
                    return false;
            }
        }

        private bool HandleEldTrust(Message message)
        {
            _logger.LogInfo($"Handling the message type {Message.Types.Type.EldTrust}.");

            _appProcess.CurrentShardLeader = message.EldTrust.Process;
            if (_appProcess.CurrentShardLeader.Equals(_appSystem.CurrentProcess))
            {
                _ts += _config.EpochIncrement;
                Message newEpoch = new Message
                {
                    MessageUuid = Guid.NewGuid().ToString(),
                    AbstractionId = AbstractionType.Beb.ToString(),
                    SystemId = _appProcess.Id,
                    Type = Message.Types.Type.BebBroadcast,
                    BebBroadcast = new BebBroadcast
                    {
                        Message = new Message
                        {
                            MessageUuid = Guid.NewGuid().ToString(),
                            AbstractionId = _id,
                            SystemId = _appSystem.SystemId,
                            Type = Message.Types.Type.EcNewEpoch,
                            EcNewEpoch = new EcNewEpoch_
                            {
                                Timestamp = _ts
                            }
                        }
                    }
                };
                _messageBroker.SendMessage(newEpoch);
            }
            return true;
        }

        private bool HandleEcNewEpoch(Message message)
        {
            _logger.LogInfo($"Handling the message type {Message.Types.Type.EcNewEpoch}.");

            if (message.BebDeliver.Sender.Equals(_appProcess.CurrentShardLeader) && message.BebDeliver.Message.EcNewEpoch.Timestamp > _lastTs)
            {
                _lastTs = message.BebDeliver.Message.EcNewEpoch.Timestamp;
                Message startEpoch = new Message
                {
                    MessageUuid = Guid.NewGuid().ToString(),
                    AbstractionId = AbstractionType.Uc.ToString(),
                    SystemId = _appSystem.SystemId,
                    Type = Message.Types.Type.EcStartEpoch,
                    EcStartEpoch = new EcStartEpoch
                    {
                        NewTimestamp = _lastTs,
                    }
                };
                _messageBroker.SendMessage(startEpoch);
            }
            else
            {
                Message nack = new Message
                {
                    MessageUuid = Guid.NewGuid().ToString(),
                    AbstractionId = AbstractionType.Pl.ToString(),
                    SystemId = _appProcess.Id,
                    Type = Message.Types.Type.PlSend,
                    PlSend = new PlSend
                    {
                        Destination = message.BebDeliver.Sender,
                        Message = new Message
                        {
                            MessageUuid = Guid.NewGuid().ToString(),
                            AbstractionId = AbstractionType.Ec.ToString(),
                            SystemId = _appSystem.SystemId,
                            Type = Message.Types.Type.EcNack,
                            EcNack = new EcNack_()
                        }
                    }
                };
                _messageBroker.SendMessage(nack);
            }
            return true;
        }
        private bool HandleEcNach(Message message)
        {
            _logger.LogInfo($"Handling the message type {Message.Types.Type.EcNack}.");

            if (_appProcess.CurrentShardLeader.Equals(_appSystem.SystemId))
            {
                _ts += _config.EpochIncrement;
                Message newEpoch = new Message
                {
                    MessageUuid = Guid.NewGuid().ToString(),
                    AbstractionId = AbstractionType.Beb.ToString(),
                    SystemId = _appProcess.Id,
                    Type = Message.Types.Type.BebBroadcast,
                    BebBroadcast = new BebBroadcast
                    {
                        Message = new Message
                        {
                            MessageUuid = Guid.NewGuid().ToString(),
                            AbstractionId = AbstractionType.Ec.ToString(),
                            SystemId = _appSystem.SystemId,
                            Type = Message.Types.Type.EcNewEpoch,
                            EcNewEpoch = new EcNewEpoch_
                            {
                                Timestamp = _ts
                            }
                        }
                    }
                };
                _messageBroker.SendMessage(newEpoch);
            }
            return true;
        }
    }
}
