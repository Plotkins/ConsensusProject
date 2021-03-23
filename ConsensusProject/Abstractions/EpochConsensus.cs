using ConsensusProject.App;
using ConsensusProject.Messages;
using System;
using System.Collections.Generic;
using System.Linq;

namespace ConsensusProject.Abstractions
{
    public class EpochConsensus : Abstraction
    {
        private string _id;
        private AppProccess _appProcces;
        private AppSystem _appSystem;
        private Config _config;
        private AppLogger _logger;
        private List<ProcessId> _systemProcesses;

        //state
        private int _ets;
        private EpState_ _currentState;
        private Value _tmpVal;
        private Dictionary<int, EpState_> _states;
        private ProcessId _leader;
        private int _accepted;
        private bool _aborted;

        public EpochConsensus(string id, Config config, AppProccess appProcess, AppSystem appSystem, List<ProcessId> systemProcesses, int ets, EpState_ state, ProcessId leader)
        {
            _id = id;
            _config = config;
            _logger = new AppLogger(_config, _id, appSystem.SystemId);
            _appProcces = appProcess;
            _appSystem = appSystem;
            _systemProcesses = systemProcesses;

            //state
            _ets = ets;
            _currentState = state;
            _tmpVal = new Value { Defined = false };
            _states = new Dictionary<int, EpState_>();
            _leader = leader;
            _accepted = 0;
            _aborted = false;
        }

        public bool Handle(Message message)
        {
            if (_aborted)
            {
                return false;
            }
            switch (message)
            {
                case Message m when m.Type == Message.Types.Type.EpPropose:
                    return HandleEpPropose(m);
                case Message m when m.Type == Message.Types.Type.BebDeliver && m.BebDeliver.Message.Type == Message.Types.Type.EpRead:
                    return HandleEpRead(m);
                case Message m when m.Type == Message.Types.Type.PlDeliver && m.PlDeliver.Message.Type == Message.Types.Type.EpState:
                    return HandleEpState(m);
                case Message m when m.Type == Message.Types.Type.BebDeliver && m.BebDeliver.Message.Type == Message.Types.Type.EpWrite:
                    return HandleEpWrite(m);
                case Message m when m.Type == Message.Types.Type.PlDeliver && m.PlDeliver.Message.Type == Message.Types.Type.EpAccept:
                    return HandleEpAccept(m);
                case Message m when m.Type == Message.Types.Type.BebDeliver && m.BebDeliver.Message.Type == Message.Types.Type.EpDecided:
                    return HandleEpDecided(m);
                case Message m when m.Type == Message.Types.Type.EpAbort:
                    return HandleEpAbort(m);
                default:
                    return false;
            }
        }

        private bool HandleEpPropose(Message message)
        {
            _logger.LogInfo($"Trying to handler the message type {Message.Types.Type.EpPropose}.");
            if (IsLeader)
            {
                _logger.LogInfo($"LEADER handling the message type {Message.Types.Type.EpPropose}.");
                _tmpVal = message.EpPropose.Value;
                _currentState.Value = _tmpVal.Clone();

                Message read = new Message
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
                            Type = Message.Types.Type.EpRead,
                            EpRead = new EpRead_()
                        }
                    }
                };

                _appProcces.EnqueMessage(read);

                return true;
            }
            return false;
        }

        private bool HandleEpRead(Message message)
        {
            _logger.LogInfo($"Handling the message type {Message.Types.Type.EpRead}.");

            Message plSend = new Message
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
                        Type = Message.Types.Type.EpState,
                        EpState = _currentState.Clone()
                    },
                }
            };

            _appProcces.EnqueMessage(plSend);

            return true;
        }
        private bool HandleEpState(Message message)
        {
            if (IsLeader)
            {
                var sender = _systemProcesses.Find(it => it.Host == message.PlDeliver.Sender.Host && it.Port == message.PlDeliver.Sender.Port);
                if (sender == null) return false;
                
                _logger.LogInfo($"Handling the message type {Message.Types.Type.EpState}. Received state from {sender.Owner}-{sender.Owner} with rank {sender.Rank}");

                _states[sender.Rank] = message.PlDeliver.Message.EpState;

                HandleMajorityHit();

                return true;
            }
            return false;
        }

        private void HandleMajorityHit()
        {
            _logger.LogInfo($"Checking if majority is hit ({_states.Values.Count} > {_appSystem.NrOfProcesses / 2}).");
            if (_states.Values.Count > _appSystem.NrOfProcesses / 2)
            {
                _logger.LogInfo($"Majority hit! Creating the message type {Message.Types.Type.EpWrite}.");
                EpState_ maxState = GetHighestState(_states.Values.ToList());

                if(maxState.Value.Defined)
                {
                    _tmpVal = maxState.Value.Clone();
                }
                _states = new Dictionary<int, EpState_>();

                Message write = new Message
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
                            Type = Message.Types.Type.EpWrite,
                            EpWrite = new EpWrite_
                            {
                                Value = _tmpVal
                            }
                        }
                    }
                };

                _appProcces.EnqueMessage(write);
            }
        }

        private EpState_ GetHighestState(List<EpState_> states)
        {
            EpState_ max = new EpState_ { Value = new Value { Defined = false } };
            foreach(var state in states)
            {
                if(state.Value.Defined && state.Value.V > max.Value.V)
                {
                    max = state.Clone();
                }
            }
            return max;
        }

        private bool HandleEpWrite(Message message)
        {
            _logger.LogInfo($"Handling the message type {Message.Types.Type.EpWrite}.");
            _currentState = new EpState_
            {
                ValueTimestamp = _ets,
                Value = message.BebDeliver.Message.EpWrite.Value.Clone()
            };

            Message accept = new Message
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
                        Type = Message.Types.Type.EpAccept,
                        EpAccept = new EpAccept_()
                    }
                }
            };

            _appProcces.EnqueMessage(accept);

            return true;
        }
        private bool HandleEpAccept(Message message)
        {
            if (IsLeader)
            {
                _logger.LogInfo($"Handling the message type {Message.Types.Type.EpAccept}.");

                _accepted += 1;

                HandleMajorityAccepted();

                return true;
            }
            return false;
        }

        private void HandleMajorityAccepted()
        {
            if (_accepted > _appSystem.NrOfProcesses / 2)
            {
                _logger.LogInfo($"Majority accepted! Creating the message type {Message.Types.Type.EpDecided}.");
                Message decided = new Message
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
                            Type = Message.Types.Type.EpDecided,
                            EpDecided = new EpDecided_
                            {
                                Value = _tmpVal
                            }
                        }
                    }
                };
                
                _appProcces.EnqueMessage(decided);
            }
        }
        private bool HandleEpDecided(Message message)
        {
            _logger.LogInfo($"Handling the message type {Message.Types.Type.EpDecided}.");

            Message decide = new Message
            {
                MessageUuid = Guid.NewGuid().ToString(),
                AbstractionId = _id,
                SystemId = _appSystem.SystemId,
                Type = Message.Types.Type.EpDecide,
                EpDecide = new EpDecide
                {
                    Value = message.BebDeliver.Message.EpDecided.Value,
                    Ets = _ets,
                }
            };

            _appProcces.EnqueMessage(decide);

            return true;
        }
        private bool HandleEpAbort(Message message)
        {
            _logger.LogInfo($"Handling the message type {Message.Types.Type.EpAbort}.");
            _aborted = true;

            Message aborted = new Message
            {
                MessageUuid = Guid.NewGuid().ToString(),
                AbstractionId = _id,
                SystemId = _appSystem.SystemId,
                Type = Message.Types.Type.EpAborted,
                EpAborted = new EpAborted
                {
                    Value = _currentState.Value,
                    ValueTimestamp = _currentState.ValueTimestamp,
                    Ets = _ets
                }
            };

            _appProcces.EnqueMessage(aborted);

            return true;
        }

        public bool IsLeader { get { return _leader.Equals(_appSystem.CurrentProccess); } }
    }
}
