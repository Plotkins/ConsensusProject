using ConsensusProject.Abstractions;
using ConsensusProject.Messages;
using ConsensusProject.Utils;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace ConsensusProject.App
{
    public class AppProcess
    {
        public string Id { get; set; }
        private Config _config;
        private AppLogger _logger;
        private List<Transaction> _transactions = new List<Transaction>();
        private readonly object balanceLock = new object();
        private MessageBroker _messageBroker;
        private ConcurrentDictionary<string, Message> _messagesMap = new ConcurrentDictionary<string, Message>();
        private ConcurrentDictionary<string, List<ProcessId>> _networkNodes = new ConcurrentDictionary<string, List<ProcessId>>();
        private ConcurrentDictionary<string, Abstraction> _abstractions = new ConcurrentDictionary<string, Abstraction>();
        private ConcurrentDictionary<string, ProcessId> _shardLeaders = new ConcurrentDictionary<string, ProcessId>();

        public ConcurrentDictionary<string, bool> AccountLocks = new ConcurrentDictionary<string, bool>();
        public ConcurrentDictionary<string, AppSystem> AppSystems { get; set; } = new ConcurrentDictionary<string, AppSystem>();
        public ConcurrentDictionary<string, List<SbacLocalPrepared>> LocalPreparedPerTransaction = new ConcurrentDictionary<string, List<SbacLocalPrepared>>();
        
        public AppProcess(Config config)
        {
            Id = "MainSystem";
            _messagesMap = new ConcurrentDictionary<string, Message>();
            AppSystems = new ConcurrentDictionary<string, AppSystem>();
            _config = config;
            _logger = new AppLogger(config, "AppProcess", Id);
            _messageBroker = new MessageBroker(config);
            InitializeCommunicationAbstractions();
        }

        ~AppProcess() => _messageBroker.UnsubscribeGroup(Id);

        public List<ProcessId> ShardNodes => _networkNodes.GetValueOrDefault(_config.Alias);
        public List<ProcessId> NetworkLeaders => _shardLeaders.Values.ToList();
        public List<ProcessId> NetworkNodes => _networkNodes.Values.SelectMany(it => it).ToList();
        public List<ProcessId> GetShardNodes(string shardId) => _networkNodes.GetValueOrDefault(shardId);

        public int NetworkVersion { get; internal set; } = 0;

        public bool IsLeader { get { return CurrentShardLeader.Equals(CurrentProccess); } }

        public ProcessId CurrentProccess => ShardNodes?.Find(it => _config.IsEqual(it));

        public ProcessId HubProcess => new ProcessId { Host = _config.HubIpAddress, Port = _config.HubPort };

        public ProcessId CurrentShardLeader
        {
            get { return _shardLeaders.TryGetValue(_config.Alias, out ProcessId process) ? process : null; }
            set { _shardLeaders[_config.Alias] = value; }
        }

        public void UpdateExternalShardLeader(ProcessId process)
        {
            _shardLeaders[process.Owner] = process;
        }

        public bool AddTransaction(Transaction transaction)
        {
            lock (balanceLock)
            {
                if (_transactions.Any(t => t.Id == transaction.Id)) return false;

                _transactions.Add(transaction);
                if (transaction.From == null)
                {
                    AccountLocks[transaction.To] = false;
                }
            }
            return true;
        }

        public void AddNewNode(ProcessId process)
        {
            if (_networkNodes.ContainsKey(process.Owner) && !_networkNodes[process.Owner].Any(it => it.Port == process.Port && it.Host == process.Host))
            {
                _networkNodes[process.Owner].Add(process);
            }
            else
            {
                _networkNodes[process.Owner] = new List<ProcessId> { process };
            }
        }

        public Dictionary<string, double> Accounts {
            get
            {
                var accounts = new Dictionary<string, double>();
                lock (balanceLock)
                {
                    foreach (var tx in _transactions.Where(it => it.Status == Transaction.Types.Status.Accepted))
                    {
                        if (!accounts.ContainsKey(tx.To) && string.IsNullOrWhiteSpace(tx.From))
                        {
                            accounts[tx.To] = tx.Amount;
                        }
                        else
                        {
                            if (accounts.ContainsKey(tx.From))
                                accounts[tx.From] -= tx.Amount;
                            if (accounts.ContainsKey(tx.To))
                                accounts[tx.To] += tx.Amount;
                        }
                    }
                }
                return accounts;
            }
        }

        public void PrintAccounts()
        {
            var output = "\n-----------ACCOUNTS----------\n";
            output += Accounts.ToList().ToStringTable(
                new string[] { "ACCOUNT", "AMOUNT" },
                p => p.Key, p => p.Value
                );
            _logger.LogInfo(output);
        }

        public void InitializeCommunicationAbstractions()
        {
            _abstractions.TryAdd(
                AbstractionType.Pl.ToString(),
                new PerfectLink(AbstractionType.Pl.ToString(), _config, this, _messageBroker
            ));
            _abstractions.TryAdd(
                AbstractionType.Beb.ToString(),
                new BestEffortBroadcast(AbstractionType.Beb.ToString(), _config, this, _messageBroker
            ));
            _abstractions.TryAdd(
                AbstractionType.Cp.ToString(),
                new ClientProxy(AbstractionType.Cp.ToString(), _config, this, _messageBroker
            ));
            _abstractions.TryAdd(
                AbstractionType.Sbac.ToString(),
                new ShardedByzantineAtomicCommit(AbstractionType.Sbac.ToString(), this, _config, _messageBroker
            ));
        }

        public void InitializeLeaderMaintenanceAbstractions()
        {
            _abstractions.TryAdd(
                AbstractionType.Eld.ToString(),
                new EventualLeaderDetector(AbstractionType.Eld.ToString(), _config, this, _messageBroker
            ));
            _abstractions.TryAdd(
                AbstractionType.Epfd.ToString(),
                new EventuallyPerfectFailureDetector(AbstractionType.Epfd.ToString(), _config, this, _messageBroker
            ));
        }

        public void PrintTransactions()
        {
            var output = "\n-----------TRANSACTIONS----------\n";
            output += _transactions.ToStringTable(
                new string[] { "TRANSACTION ID", "SOURCE ACCOUNT", "DESTINATION ACCOUNT", "AMOUNT", "STATUS"},
                p => p.Id, p => p.From, p => p.To, p => p.Amount, p => p.Status
                );
            _logger.LogInfo(output);
        }

        public void PrintNetworkNodes()
        {
            var output = "\n-----------NETWORK NODES----------\n";
            output += NetworkNodes.ToStringTable(
                new string[] { "HOST", "PORT", "SHARD ID", "INDEX", "RANK", "IS LEADER" },
                p => p.Host, p => p.Port, p => p.Owner, p => p.Index, p => p.Rank, p => _shardLeaders.TryGetValue(p.Owner, out ProcessId leader) ? leader.Equals(p) : false
                );
            _logger.LogInfo(output);
        }

        public void DequeMessage(Message message)
        {
            if (!_messagesMap.TryRemove(message.MessageUuid, out _)) _logger.LogError("Error removing the message.");
        }

        public void EnqueMessage(Message message)
        {
            if (!_messagesMap.TryAdd(message.MessageUuid, message)) _logger.LogError("Error adding the message.");
        }

        public ICollection<Message> Messages
        {
            get { return _messagesMap.Values; }
        }
        public TransactionAction GetPreparedAction(Transaction transaction)
        {
            var accounts = Accounts;

            var isFromFree = false;
            var isToFree = false;
            lock (balanceLock)
            {
                if (string.IsNullOrWhiteSpace(transaction.From))
                {
                    AccountLocks[transaction.To] = true;
                    return TransactionAction.Commit;
                }

                if (AccountLocks.ContainsKey(transaction.From) && !AccountLocks[transaction.From])
                {
                    isFromFree = true;
                    AccountLocks[transaction.From] = true;
                }
                if (AccountLocks.ContainsKey(transaction.To) && !AccountLocks[transaction.To])
                {
                    isToFree = true;
                    AccountLocks[transaction.To] = true;
                }
            }

            var existsTo = accounts.TryGetValue(transaction.To, out double toBalance);
            var existsFrom = accounts.TryGetValue(transaction.From, out double fromBalance);

            if (transaction.ShardIn != _config.Alias && transaction.ShardOut != _config.Alias)
            {
                _logger.LogInfo($"GetPreparedAction returned {TransactionAction.Abort} due to if (transaction.ShardIn != _config.Alias<{transaction.ShardIn != _config.Alias}> && transaction.ShardOut != _config.Alias<{transaction.ShardOut != _config.Alias}>)");
                return TransactionAction.Abort;
            }
            if (!existsTo && !existsFrom)
            {
                _logger.LogInfo($"GetPreparedAction returned {TransactionAction.Abort} due to if (!existsTo<{!existsTo}> && !existsFrom<{!existsFrom}>)");
                return TransactionAction.Abort;
            }
            if (existsTo && !isToFree)
            {
                _logger.LogInfo($"GetPreparedAction returned {TransactionAction.Abort} due to if (existsTo<{existsTo}> && !isToFree<{!isToFree}>)");
                return TransactionAction.Abort;
            }
            if (existsFrom && !isFromFree)
            {
                _logger.LogInfo($"GetPreparedAction returned {TransactionAction.Abort} due to if (existsFrom<{existsFrom}> && !isFromFree<{!isFromFree}>)");
                return TransactionAction.Abort;
            }
            if (existsFrom && fromBalance < transaction.Amount)
            {
                _logger.LogInfo($"GetPreparedAction returned {TransactionAction.Abort} due to if (existsFrom<{existsFrom}> && fromBalance < transaction.Amount<{fromBalance < transaction.Amount}>)");
                return TransactionAction.Abort;
            }

            return TransactionAction.Commit;
        }

        public TransactionAction GetAcceptAction(Transaction transaction)
        {
            if (LocalPreparedPerTransaction[transaction.Id].Any(it => it.Action == TransactionAction.Abort))
                return TransactionAction.Abort;

            return TransactionAction.Commit;
        }
        public void AddLocalPrepared(SbacLocalPrepared localPrepared)
        {
            if (LocalPreparedPerTransaction.ContainsKey(localPrepared.Transaction.Id))
            {
                LocalPreparedPerTransaction[localPrepared.Transaction.Id].Add(localPrepared);
            }
            else
            {
                LocalPreparedPerTransaction[localPrepared.Transaction.Id] = new List<SbacLocalPrepared> { localPrepared };
            }
        }
    }
}
