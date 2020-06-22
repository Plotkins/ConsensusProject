using ConsensusProject.App;
using ConsensusProject.Messages;
using System;
using System.Collections.Generic;

namespace ConsensusProject.Abstractions
{
    public class UniformConsensus : Abstraction
    {
        private string _id;
        private AppProccess _appProcces;
        private AppSystem _appSystem;
        private Config _config;
        private AppLogger _logger;
        private List<ProcessId> _systemProcesses;

        //state
        private Value _value;
        private bool _proposed;
        private bool _decided;
        private int _ets;
        private ProcessId _currentLeader;
        private int _newTs;
        private ProcessId _newLeader;


        public UniformConsensus(string id, Config config, AppProccess appProcess, AppSystem appSystem, List<ProcessId> systemProcesses, EpochChange epochChange)
        {
            _id = id;
            _config = config;
            _logger = new AppLogger(_config, _id, appSystem.SystemId);
            _appProcces = appProcess;
            _appSystem = appSystem;
            _systemProcesses = systemProcesses;

            //state
            _value = new Value { Defined = false };
            _proposed = false;
            _decided = false;
            _ets = 0;
            _currentLeader = epochChange.CurrentLeader;
            _newTs = 0;
            _newLeader = null;

            EpState_ state = new EpState_
            {
                ValueTimestamp = 0,
                Value = new Value { Defined = false }
            };

            _appSystem.InitializeNewEpochConsensus(0, state, _currentLeader);

        }

        public bool Handle(Message message)
        {
            switch (message)
            {
                case Message m when m.AbstractionId == "uc" && m.Type == Message.Types.Type.UcPropose:
                    return HandleUcPropose(m);
                case Message m when m.AbstractionId == "ec" && m.Type == Message.Types.Type.EcStartEpoch:
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

            _newLeader = message.EcStartEpoch.NewLeader;
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
            _currentLeader = _newLeader;
            _proposed = false;

            EpState_ state = new EpState_
            {
                Value = message.EpAborted.Value,
                ValueTimestamp = message.EpAborted.ValueTimestamp,
            };
            _appSystem.InitializeNewEpochConsensus(_ets, state, _currentLeader);

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
            if(_currentLeader.Equals(_appSystem.CurrentProccess) && _value.Defined && _proposed == false)
            {
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
