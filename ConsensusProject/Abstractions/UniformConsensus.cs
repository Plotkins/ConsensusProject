using ConsensusProject.App;
using ConsensusProject.Messages;
using System;

namespace ConsensusProject.Abstractions
{
    public class UniformConsensus : Abstraction
    {
        private string _id;
        private AppProccess _appProcces;
        private AppSystem _appSystem;
        private Config _config;
        private AppLogger _logger;

        //state
        private Value _value;
        private bool _proposed;
        private bool _decided;
        private int _ets;
        private int _newTs;

        public bool Decided { get { return _decided; } }

        public UniformConsensus(string id, Config config, AppProccess appProcess, AppSystem appSystem)
        {
            _id = id;
            _config = config;
            _logger = new AppLogger(_config, _id, appSystem.SystemId);
            _appProcces = appProcess;
            _appSystem = appSystem;

            //state
            _value = new Value { Defined = false };
            _proposed = false;
            _decided = false;
            _ets = 0;
            _newTs = 0;

            EpState_ state = new EpState_
            {
                ValueTimestamp = 0,
                Value = new Value { Defined = false }
            };

            _appSystem.InitializeNewEpochConsensus(0, state);

        }

        public bool Handle(Message message)
        {
            switch (message)
            {
                case Message m when m.Type == Message.Types.Type.UcPropose:
                    return HandleUcPropose(m);
                case Message m when m.Type == Message.Types.Type.EcStartEpoch:
                    return HandleEcStartEpoch(m);
                case Message m when m.Type == Message.Types.Type.EpAborted && _ets == m.EpAborted.Ets:
                    return HandleEpAborted(m);
                case Message m when m.Type == Message.Types.Type.EpDecide && _ets == m.EpDecide.Ets:
                    return HandleEpDecide(m);
                default:
                    return false;
            }
        }

        private bool HandleUcPropose(Message message)
        {
            _logger.LogInfo($"Handling the message type {Message.Types.Type.UcPropose}.");

            if (message.UcPropose.Value.Defined)
            {
                _value = message.UcPropose.Value;

                HandleProposeValueIfLeader();

                return true;
            }
            return false;
        }

        private bool HandleEcStartEpoch(Message message)
        {
            _logger.LogInfo($"Handling the message type {Message.Types.Type.EcStartEpoch}.");

            _newTs = message.EcStartEpoch.NewTimestamp;

            Message abort = new Message
            {
                MessageUuid = Guid.NewGuid().ToString(),
                AbstractionId = $"ep{_ets}",
                SystemId = _appSystem.SystemId,
                Type = Message.Types.Type.EpAbort,
                EpAbort = new EpAbort()
            };

            _appProcces.EnqueMessage(abort);

            return true;
        }
        private bool HandleEpAborted(Message message)
        {
            _logger.LogInfo($"Handling the message type {Message.Types.Type.EpAborted}.");

            _ets = _newTs;
            _proposed = false;

            EpState_ state = new EpState_
            {
                Value = message.EpAborted.Value,
                ValueTimestamp = message.EpAborted.ValueTimestamp,
            };
            _appSystem.InitializeNewEpochConsensus(_ets, state);

            HandleProposeValueIfLeader();

            return true;
        }
        private bool HandleEpDecide(Message message)
        {
            _logger.LogInfo($"Handling the message type {Message.Types.Type.EpDecide}.");
            if (!_decided)
            {
                _decided = true;
                Message decide = new Message
                {
                    MessageUuid = Guid.NewGuid().ToString(),
                    AbstractionId = _id,
                    SystemId = _appSystem.SystemId,
                    Type = Message.Types.Type.UcDecide,
                    UcDecide = new UcDecide
                    {
                        Value = message.EpDecide.Value
                    }
                };
                _appProcces.EnqueMessage(decide);
            }
            return true;
        }

        private void HandleProposeValueIfLeader()
        {
            if(_appProcces.CurrentShardLeader.Equals(_appSystem.CurrentProccess) && _value.Defined && _proposed == false)
            {
                _logger.LogInfo($"LEADER creating a {Message.Types.Type.EpPropose} message.");
                _proposed = true;
                Message propose = new Message
                {
                    MessageUuid = Guid.NewGuid().ToString(),
                    AbstractionId = $"ep{_ets}",
                    SystemId = _appSystem.SystemId,
                    Type = Message.Types.Type.EpPropose,
                    EpPropose = new EpPropose
                    {
                        Value = _value
                    }
                };
                _appProcces.EnqueMessage(propose);
            }
        }
    }
}
