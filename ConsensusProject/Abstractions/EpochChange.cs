using ConsensusProject.App;
using ConsensusProject.Messages;
using ConsensusProject.Utils;
using System;
using System.Collections.Generic;

namespace ConsensusProject.Abstractions
{
    public class EpochChange : Abstraction
    {
        private string _id;
        private AppProccess _appProcces;
        private AppSystem _appSystem;
        private Config _config;
        private AppLogger _logger;
        private List<ProcessId> _systemProcesses;

        //state
        private ProcessId _trusted;
        private int _lastTs;
        private int _ts;

        public EpochChange(string id, Config config, AppProccess appProcess, AppSystem appSystem, List<ProcessId> systemProcesses)
        {
            _id = id;
            _config = config;
            _logger = new AppLogger(_config, _id, appSystem.SystemId);
            _appProcces = appProcess;
            _appSystem = appSystem;
            _systemProcesses = systemProcesses;

            //state
            _trusted = AbstractionHelpers.GetMaxRankedProcess(_systemProcesses);
            _lastTs = 0;
            _ts = _appSystem.CurrentProccess.Rank;
        }

        public ProcessId CurrentLeader
        {
            get { return _trusted; }
        }

        public bool Handle(Message message)
        {
            switch (message)
            {
                case Message m when m.AbstractionId == "eld" && m.Type == Message.Types.Type.EldTrust:
                    return HandleEldTrust(m);
                case Message m when m.AbstractionId == "beb" && m.Type == Message.Types.Type.BebDeliver && m.BebDeliver.Message.Type == Message.Types.Type.EcNewEpoch:
                    return HandleEcNewEpoch(m);
                case Message m when m.AbstractionId == "pl" && m.Type == Message.Types.Type.PlDeliver && m.PlDeliver.Message.Type == Message.Types.Type.EcNack:
                    return HandleEcNach(m);
                default:
                    return false;
            }
        }

        private bool HandleEldTrust(Message message)
        {
            _logger.LogInfo($"Handling the message type {Message.Types.Type.EldTrust}.");

            _trusted = message.EldTrust.Process;
            if (_trusted.Equals(_appSystem.CurrentProccess))
            {
                _ts += _config.EpochIncrement;
                Message newEpoch = new Message
                {
                    MessageUuid = Guid.NewGuid().ToString(),
                    AbstractionId = "beb",
                    SystemId = _appSystem.SystemId,
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
                _appProcces.EnqueMessage(newEpoch);
            }
            return true;
        }

        private bool HandleEcNewEpoch(Message message)
        {
            _logger.LogInfo($"Handling the message type {Message.Types.Type.EcNewEpoch}.");

            if (message.BebDeliver.Sender.Equals(_trusted) && message.BebDeliver.Message.EcNewEpoch.Timestamp > _lastTs)
            {
                _lastTs = message.BebDeliver.Message.EcNewEpoch.Timestamp;
                Message startEpoch = new Message
                {
                    MessageUuid = Guid.NewGuid().ToString(),
                    AbstractionId = _id,
                    SystemId = _appSystem.SystemId,
                    Type = Message.Types.Type.EcStartEpoch,
                    EcStartEpoch = new EcStartEpoch
                    {
                        NewTimestamp = _lastTs,
                        NewLeader = _trusted
                    }
                };
                _appProcces.EnqueMessage(startEpoch);
            }
            else
            {
                Message nack = new Message
                {
                    MessageUuid = Guid.NewGuid().ToString(),
                    AbstractionId = "pl",
                    SystemId = _appSystem.SystemId,
                    Type = Message.Types.Type.PlSend,
                    PlSend = new PlSend
                    {
                        Destination = message.BebDeliver.Sender,
                        Message = new Message
                        {
                            MessageUuid = Guid.NewGuid().ToString(),
                            AbstractionId = _id,
                            SystemId = _appSystem.SystemId,
                            Type = Message.Types.Type.EcNack,
                            EcNack = new EcNack_()
                        }
                    }
                };
                _appProcces.EnqueMessage(nack);
            }
            return true;
        }
        private bool HandleEcNach(Message message)
        {
            _logger.LogInfo($"Handling the message type {Message.Types.Type.EcNack}.");

            if (_trusted.Equals(_appSystem.SystemId))
            {
                _ts += _config.EpochIncrement;
                Message newEpoch = new Message
                {
                    MessageUuid = Guid.NewGuid().ToString(),
                    AbstractionId = "beb",
                    SystemId = _appSystem.SystemId,
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
                _appProcces.EnqueMessage(newEpoch);
            }
            return true;
        }
    }
}
