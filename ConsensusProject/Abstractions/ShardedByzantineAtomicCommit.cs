using ConsensusProject.App;
using ConsensusProject.Messages;
using System;
using System.Linq;

namespace ConsensusProject.Abstractions
{
    public class ShardedByzantineAtomicCommit : Abstraction
    {
        private AppProccess _appProccess;
        private AppLogger _logger;
        private Config _config;

        public ShardedByzantineAtomicCommit(AppProccess appProccess, AppLogger appLogger, Config config)
        {
            _appProccess = appProccess;
            _logger = appLogger;
            _config = config;
        }

        public bool Handle(Message message)
        {
            switch (message)
            {

                case Message m when m.Type == Message.Types.Type.PlDeliver && m.PlDeliver.Message.Type == Message.Types.Type.SbacPrepare:
                    return HandleSbacPrepare(message);
                case Message m when m.Type == Message.Types.Type.BebDeliver && m.BebDeliver.Message.Type == Message.Types.Type.SbacLocalPrepared:
                    return HandleSbacLocalPrepared(message);
                case Message m when m.Type == Message.Types.Type.UcDecide && m.UcDecide.Value.Type == Value.Types.Type.SbacPrepared:
                    return HandleUcDecideSbacPrepared(message);
                case Message m when m.Type == Message.Types.Type.UcDecide && m.UcDecide.Value.Type == Value.Types.Type.SbacAccept:
                    return HandleUcDecideSbacAccepted(message);
                default:
                    return false;
            }
        }

        private bool HandleUcDecideSbacPrepared(Message message)
        {
            _logger.LogInfo($"Handling the message type {Message.Types.Type.SbacPrepared}.");
            var localPrepared = new SbacLocalPrepared
            {
                Action = message.UcDecide.Value.SbacPrepared.Action,
                Transaction = message.UcDecide.Value.SbacPrepared.Transaction,
                ShardId = _config.Alias,
                SystemId = message.UcDecide.Value.SbacPrepared.SystemId,
            };
            if (_appProccess.IsLeader && message.UcDecide.Value.SbacPrepared.Transaction.ShardIn != message.UcDecide.Value.SbacPrepared.Transaction.ShardOut)
            {
                
                var broadcast = new Message
                {
                    MessageUuid = Guid.NewGuid().ToString(),
                    Type = Message.Types.Type.BebBroadcast,
                    AbstractionId = "beb",
                    BebBroadcast = new BebBroadcast
                    {
                        Type = BebBroadcast.Types.Type.Custom,
                        Message = new Message
                        {
                            MessageUuid = Guid.NewGuid().ToString(),
                            Type = Message.Types.Type.SbacLocalPrepared,
                            AbstractionId = "sbac",
                            SbacLocalPrepared = localPrepared
                        }
                    }
                };
                var shardId = message.UcDecide.Value.SbacPrepared.Transaction.ShardIn == _config.Alias
                    ? message.UcDecide.Value.SbacPrepared.Transaction.ShardOut
                    : message.UcDecide.Value.SbacPrepared.Transaction.ShardIn;

                broadcast.BebBroadcast.Processes.AddRange(_appProccess.GetShardNodes(shardId));

                _appProccess.EnqueMessage(broadcast);
            }

            _appProccess.AddLocalPrepared(localPrepared);

            CheckIfAllShardsPrepared(localPrepared.Transaction.Id);

            return true;
        }

        private bool HandleUcDecideSbacAccepted(Message message)
        {
            _logger.LogInfo($"Handling the message type {Message.Types.Type.SbacAccept}.");
            var transaction = message.UcDecide.Value.SbacAccept.Transaction;

            if (_appProccess.AccountLocks.ContainsKey(transaction.From)) _appProccess.AccountLocks[transaction.From] = false;
            if (_appProccess.AccountLocks.ContainsKey(transaction.To)) _appProccess.AccountLocks[transaction.To] = false;

            transaction.Status = message.UcDecide.Value.SbacAccept.Action == TransactionAction.Commit
                ? Transaction.Types.Status.Accepted
                : Transaction.Types.Status.Rejected;
            _appProccess.AddTransaction(transaction);

            _logger.LogInfo($"Consensus for transaction with Id={message.UcDecide.Value.SbacAccept.Transaction.Id} ended.");

            _appProccess.PrintAccounts();
            _appProccess.PrintTransactions();
            Message sbacAllPrepared = new Message
            {
                MessageUuid = Guid.NewGuid().ToString(),
                AbstractionId = "pl",
                Type = Message.Types.Type.PlSend,
                PlSend = new PlSend
                {
                    Destination = _appProccess.HubProcess,
                    Message = new Message
                    {
                        MessageUuid = Guid.NewGuid().ToString(),
                        SystemId = message.SystemId,
                        Type = Message.Types.Type.SbacAllPrepared,
                        SbacAllPrepared = new SbacAllPrepared
                        {
                            Action = message.UcDecide.Value.SbacAccept.Action,
                            Transaction = transaction
                        }
                    }
                }
            };

            _appProccess.EnqueMessage(sbacAllPrepared);

            return true;
        }

        public bool HandleSbacPrepare(Message message)
        {
            _logger.LogInfo($"Handling the message type {Message.Types.Type.SbacPrepare}.");
            if (!_appProccess.AppSystems.TryAdd(message.SystemId, new AppSystem(message.SystemId, _config, _appProccess)))
            {
                _logger.LogInfo($"The process is already assigned to the system with Id={message.SystemId}!");
            }
            else
            {
                _logger.LogInfo($"New system with Id={message.SystemId} added to the process!");
            }

            _logger.LogInfo($"Begining the consensus for {Message.Types.Type.SbacPrepared}.");

            _appProccess.PrintNetworkNodes();

            Message ucPropose = new Message
            {
                MessageUuid = Guid.NewGuid().ToString(),
                Type = Message.Types.Type.UcPropose,
                SystemId = message.SystemId,
                AbstractionId = "uc",

                UcPropose = new UcPropose
                {
                    Type = ProposeType.SbacPrepare,
                    Transaction = message.PlDeliver.Message.SbacPrepare.Transaction
                }
            };

            _appProccess.EnqueMessage(ucPropose);

            return true;
        }

        public bool HandleSbacLocalPrepared(Message message) {

            _logger.LogInfo($"Handling the message type {Message.Types.Type.SbacLocalPrepared}.");
            _appProccess.AddLocalPrepared(message.BebDeliver.Message.SbacLocalPrepared);

            CheckIfAllShardsPrepared(message.BebDeliver.Message.SbacLocalPrepared.Transaction.Id);

            return true;
        }

        public void CheckIfAllShardsPrepared(string transactionId)
        {
            try
            {
                var localPrepared = _appProccess.LocalPreparedPerTransaction[transactionId].FirstOrDefault(it => it.ShardId == _config.Alias);
                if (localPrepared != null && (_appProccess.LocalPreparedPerTransaction[transactionId].Any(it => it.Action == TransactionAction.Abort)
                    || _appProccess.LocalPreparedPerTransaction[transactionId].Count == 2
                    || localPrepared.Transaction.ShardIn == localPrepared.Transaction.ShardOut
                    ) && _appProccess.AppSystems.TryAdd(localPrepared.SystemId, new AppSystem(localPrepared.SystemId, _config, _appProccess)))
                {
                    _logger.LogInfo($"New system with Id={localPrepared.SystemId} added to the process!");

                    _logger.LogInfo($"Begining the consensus for {Message.Types.Type.SbacAccept}.");
                    Message ucPropose = new Message
                    {
                        MessageUuid = Guid.NewGuid().ToString(),
                        Type = Message.Types.Type.UcPropose,
                        SystemId = localPrepared.SystemId,
                        AbstractionId = "uc",

                        UcPropose = new UcPropose
                        {
                            Type = ProposeType.SbacLocalPrepared,
                            Transaction = localPrepared.Transaction
                        }
                    };

                    _appProccess.EnqueMessage(ucPropose);
                }
            }
            catch (Exception)
            {
                throw;
            }
            
        }
    }
}
