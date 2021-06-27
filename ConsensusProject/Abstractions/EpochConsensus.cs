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

        //state
        private int _ets;
        private EpState_ _currentState;
        private Dictionary<int, EpState_> _states = new Dictionary<int, EpState_>();
        private int _accepted;
        private bool _aborted;
        private Value _tmpVal;

        public EpochConsensus(string id, Config config, AppProccess appProcess, AppSystem appSystem, int ets, EpState_ state)
        {
            _id = id;
            _config = config;
            _logger = new AppLogger(_config, _id, appSystem.SystemId);
            _appProcces = appProcess;
            _appSystem = appSystem;

            //state
            _ets = ets;
            _currentState = state;
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
            if (_appProcces.IsLeader)
            {
                _logger.LogInfo($"LEADER handling the message type {Message.Types.Type.EpPropose}.");

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
            _logger.LogInfo($"Chacking if is leader: {_appProcces.IsLeader}");
            if (_appProcces.IsLeader)
            {
                var sender = _appProcces.ShardNodes.Find(it => it.Host == message.PlDeliver.Sender.Host && it.Port == message.PlDeliver.Sender.Port);
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
            if (_states.Values.Count == _appSystem.NrOfProcesses)
            {
                _logger.LogInfo($"Majority hit! Creating the message type {Message.Types.Type.EpWrite}.");
                var finalState = GetFinalState();

                _tmpVal = finalState.Value;

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

        private EpState_ GetFinalState()
        {
            EpState_ final = new EpState_ { Value = new Value() };
            var action = TransactionAction.Commit;
            string transactionId;
            foreach (var state in _states.Values)
            {
                if (state.Value.Type != _currentState.Value.Type)
                    action = TransactionAction.Abort;
                if (state.Value.Type == Value.Types.Type.SbacPrepared && state.Value.SbacPrepared.Action == TransactionAction.Abort)
                    action = TransactionAction.Abort;
                if (state.Value.Type == Value.Types.Type.SbacAccept && state.Value.SbacAccept.Action == TransactionAction.Abort)
                    action = TransactionAction.Abort;
            }
            if (_currentState.Value.Type == Value.Types.Type.SbacPrepared)
            {
                transactionId = _currentState.Value.SbacPrepared.Transaction.Id;
                var sbacPrepared = new SbacPrepared
                {
                    Transaction = _currentState.Value.SbacPrepared.Transaction,
                    Action = action,
                    SystemId = _currentState.Value.SbacPrepared.SystemId
                };
                
                final.Value.SbacPrepared = sbacPrepared;
                final.Value.Type = Value.Types.Type.SbacPrepared;
            }
            else
            {
                transactionId = _currentState.Value.SbacAccept.Transaction.Id;
                var sbacAccept = new SbacAccept
                {
                    Transaction = _currentState.Value.SbacAccept.Transaction,
                    Action = action,
                };

                final.Value.SbacAccept = sbacAccept;
                final.Value.Type = Value.Types.Type.SbacAccept;
            }

            _logger.LogInfo($"{final.Value.Type} generated with action {action} for transaction ID {transactionId}.");

            return final;
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
            if (_appProcces.IsLeader)
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
            if (_accepted == _appSystem.NrOfProcesses)
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
    }
}
