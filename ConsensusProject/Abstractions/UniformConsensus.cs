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
        private ProposeType _ucType;
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
            _ucType = ProposeType.SbacPrepare;
            _proposed = false;
            _decided = false;
            _ets = 0;
            _newTs = 0;
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

            _ucType = message.UcPropose.Type;

            var state = GenerateState(message.UcPropose);

            _appSystem.InitializeNewEpochConsensus(0, state);

            HandleProposeValueIfLeader();

            return true;
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
            if(_appProcces.CurrentShardLeader.Equals(_appSystem.CurrentProccess) && _proposed == false)
            {
                _logger.LogInfo($"LEADER creating a {Message.Types.Type.EpPropose} message.");
                _proposed = true;
                Message propose = new Message
                {
                    MessageUuid = Guid.NewGuid().ToString(),
                    AbstractionId = $"ep{_ets}",
                    SystemId = _appSystem.SystemId,
                    Type = Message.Types.Type.EpPropose,
                    EpPropose = new EpPropose()
                };
                _appProcces.EnqueMessage(propose);
            }
        }

        private EpState_ GenerateState(UcPropose ucPropose)
        {
            var value = new Value();
            TransactionAction action;
            if (_ucType == ProposeType.SbacPrepare)
            {
                action = _appProcces.GetPreparedAction(ucPropose.Transaction);
                value.Type = Value.Types.Type.SbacPrepared;
                value.SbacPrepared = new SbacPrepared
                {
                    Action = action,
                    Transaction = ucPropose.Transaction,
                    SystemId = Guid.NewGuid().ToString(),
                };
            }
            else
            {
                action = _appProcces.GetAcceptAction(ucPropose.Transaction);
                value.Type = Value.Types.Type.SbacAccept;
                value.SbacAccept = new SbacAccept
                {
                    Action = action,
                    Transaction = ucPropose.Transaction
                };
            }

            _logger.LogInfo($"{value.Type} generated with action {action} for transaction ID {ucPropose.Transaction.Id}.");

            EpState_ state = new EpState_
            {
                ValueTimestamp = 0,
                Value = value
            };

            return state;
        }
    }
}
