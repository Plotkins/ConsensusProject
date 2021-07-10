using ConsensusProject.App;
using ConsensusProject.Messages;
using ConsensusProject.Utils;
using System;
using System.Linq;

namespace ConsensusProject.Abstractions
{
    public class ShardedByzantineAtomicCommit : Abstraction
    {
        public string Id { get; private set; }
        private AppProcess _appProcess;
        private AppLogger _logger;
        private Config _config;
        private MessageBroker _messageBroker;

        public ShardedByzantineAtomicCommit(string id, AppProcess appProcess, Config config, MessageBroker messageBroker)
        {
            Id = id;
            _appProcess = appProcess;
            _config = config;
            _logger = new AppLogger(_config, Id, appProcess.Id);
            _messageBroker = messageBroker;
            _messageBroker.Subscribe(appProcess.Id, Id, Handle);
        }

        public bool Handle(Message message)
        {
            switch (message)
            {

                case Message m when m.Type == Message.Types.Type.PlDeliver && m.PlDeliver.Message.Type == Message.Types.Type.SbacPrepare:
                    return HandleSbacPrepare(message);
                case Message m when m.Type == Message.Types.Type.BebDeliver && m.BebDeliver.Message.Type == Message.Types.Type.SbacLocalPrepared:
                    return HandleSbacLocalPrepared(message);
                case Message m when m.Type == Message.Types.Type.UcDecide && m.UcDecide?.Value?.Type == Value.Types.Type.SbacPrepared:
                    return HandleUcDecideSbacPrepared(message);
                case Message m when m.Type == Message.Types.Type.UcDecide && m.UcDecide?.Value?.Type == Value.Types.Type.SbacAccept:
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
            };
            if (_appProcess.IsLeader && message.UcDecide.Value.SbacPrepared.Transaction.ShardIn != message.UcDecide.Value.SbacPrepared.Transaction.ShardOut)
            {
                
                var broadcast = new Message
                {
                    MessageUuid = Guid.NewGuid().ToString(),
                    AbstractionId = AbstractionType.Beb.ToString(),
                    SystemId = _appProcess.Id,
                    Type = Message.Types.Type.BebBroadcast,
                    BebBroadcast = new BebBroadcast
                    {
                        Type = BebBroadcast.Types.Type.Custom,
                        Message = new Message
                        {
                            MessageUuid = Guid.NewGuid().ToString(),
                            AbstractionId = AbstractionType.Sbac.ToString(),
                            SystemId = _appProcess.Id,
                            Type = Message.Types.Type.SbacLocalPrepared,
                            SbacLocalPrepared = localPrepared
                        }
                    }
                };
                var shardId = message.UcDecide.Value.SbacPrepared.Transaction.ShardIn == _config.Alias
                    ? message.UcDecide.Value.SbacPrepared.Transaction.ShardOut
                    : message.UcDecide.Value.SbacPrepared.Transaction.ShardIn;

                broadcast.BebBroadcast.Processes.AddRange(_appProcess.GetShardNodes(shardId));

                _messageBroker.SendMessage(broadcast);
            }

            _appProcess.AddLocalPrepared(localPrepared);

            CheckIfAllShardsPrepared(localPrepared.Transaction.Id);

            return true;
        }

        private bool HandleUcDecideSbacAccepted(Message message)
        {
            _logger.LogInfo($"Handling the message type {Message.Types.Type.SbacAccept}.");
            var transaction = message.UcDecide.Value.SbacAccept.Transaction;

            if (_appProcess.AccountLocks.ContainsKey(transaction.From)) _appProcess.AccountLocks[transaction.From] = false;
            if (_appProcess.AccountLocks.ContainsKey(transaction.To)) _appProcess.AccountLocks[transaction.To] = false;

            transaction.Status = message.UcDecide.Value.SbacAccept.Action == TransactionAction.Commit
                ? Transaction.Types.Status.Accepted
                : Transaction.Types.Status.Rejected;

            if (_appProcess.AddTransaction(transaction))
            {
                _logger.LogInfo($"Consensus for transaction with Id={message.UcDecide.Value.SbacAccept.Transaction.Id} ended.");

                _appProcess.PrintAccounts();
                _appProcess.PrintTransactions();
                Message sbacAllPrepared = new Message
                {
                    MessageUuid = Guid.NewGuid().ToString(),
                    AbstractionId = AbstractionType.Pl.ToString(),
                    SystemId = _appProcess.Id,
                    Type = Message.Types.Type.PlSend,
                    PlSend = new PlSend
                    {
                        Destination = _appProcess.HubProcess,
                        Message = new Message
                        {
                            MessageUuid = Guid.NewGuid().ToString(),
                            AbstractionId = AbstractionType.Hub.ToString(),
                            SystemId = _appProcess.Id,
                            Type = Message.Types.Type.SbacAllPrepared,
                            SbacAllPrepared = new SbacAllPrepared
                            {
                                Action = message.UcDecide.Value.SbacAccept.Action,
                                Transaction = transaction
                            }
                        }
                    }
                };

                _messageBroker.SendMessage(sbacAllPrepared);
            }

            return true;
        }

        public bool HandleSbacPrepare(Message message)
        {
            _logger.LogInfo($"Handling the message type {Message.Types.Type.SbacPrepare}.");
            var systemId = $"{message.PlDeliver.Message.SbacPrepare.Transaction.Id}-prepare";
            if (!_appProcess.AppSystems.TryAdd(systemId, new AppSystem(systemId, _config, _appProcess, _messageBroker)))
            {
                _logger.LogInfo($"The process is already assigned to the system with Id={systemId}!");
            }
            else
            {
                _logger.LogInfo($"New system with Id={systemId} added to the process!");
            }

            _logger.LogInfo($"Begining the consensus for {Message.Types.Type.SbacPrepared}.");

            _appProcess.PrintNetworkNodes();

            Message ucPropose = new Message
            {
                MessageUuid = Guid.NewGuid().ToString(),
                AbstractionId = AbstractionType.Uc.ToString(),
                SystemId = systemId,
                Type = Message.Types.Type.UcPropose,
                UcPropose = new UcPropose
                {
                    Type = ProposeType.SbacPrepare,
                    Transaction = message.PlDeliver.Message.SbacPrepare.Transaction
                }
            };

            _messageBroker.SendMessage(ucPropose);

            return true;
        }

        public bool HandleSbacLocalPrepared(Message message) {

            _logger.LogInfo($"Handling the message type {Message.Types.Type.SbacLocalPrepared}.");
            _appProcess.AddLocalPrepared(message.BebDeliver.Message.SbacLocalPrepared);

            CheckIfAllShardsPrepared(message.BebDeliver.Message.SbacLocalPrepared.Transaction.Id);

            return true;
        }

        public void CheckIfAllShardsPrepared(string transactionId)
        {
            try
            {
                var localPrepared = _appProcess.LocalPreparedPerTransaction[transactionId].FirstOrDefault();

                if (localPrepared != null && (_appProcess.LocalPreparedPerTransaction[transactionId].Any(it => it.Action == TransactionAction.Abort)
                    || _appProcess.LocalPreparedPerTransaction[transactionId].Count == 2
                    || localPrepared.Transaction.ShardIn == localPrepared.Transaction.ShardOut))
                {
                    var systemId = $"{localPrepared.Transaction.Id}-accept";
                    if (!_appProcess.AppSystems.ContainsKey(systemId))
                    {
                        _appProcess.AppSystems.TryAdd(systemId, new AppSystem(systemId, _config, _appProcess, _messageBroker));
                        _logger.LogInfo($"New system with Id={systemId} added to the process!");

                        _logger.LogInfo($"Begining the consensus for {Message.Types.Type.SbacAccept}.");
                        Message ucPropose = new Message
                        {
                            MessageUuid = Guid.NewGuid().ToString(),
                            AbstractionId = AbstractionType.Uc.ToString(),
                            SystemId = systemId,
                            Type = Message.Types.Type.UcPropose,
                            UcPropose = new UcPropose
                            {
                                Type = ProposeType.SbacLocalPrepared,
                                Transaction = localPrepared.Transaction
                            }
                        };

                        _messageBroker.SendMessage(ucPropose);
                    }
                }
            }
            catch (Exception)
            {
                _logger.LogError($"Error during CheckIfAllShardsPrepared");
            }
        }
    }
}
