using ConsensusProject.Abstractions;
using ConsensusProject.Messages;
using ConsensusProject.Utils;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace ConsensusProject.App
{
    public class AppProccess
    {
        private Config _config;
        private AppLogger _logger;
        private List<Transaction> _transactions = new List<Transaction>();
        private ConcurrentDictionary<string, Message> _messagesMap = new ConcurrentDictionary<string, Message>();
        private ConcurrentDictionary<string, List<ProcessId>> _networkNodes = new ConcurrentDictionary<string, List<ProcessId>>();
        private Dictionary<string, Abstraction> _abstractions = new Dictionary<string, Abstraction>();
        private Dictionary<string, ProcessId> _shardLeaders = new Dictionary<string, ProcessId>();

        public ConcurrentDictionary<string, AppSystem> AppSystems { get; set; } = new ConcurrentDictionary<string, AppSystem>();
        
        public AppProccess(Config config)
        {
            _messagesMap = new ConcurrentDictionary<string, Message>();
            AppSystems = new ConcurrentDictionary<string, AppSystem>();
            _config = config;
            _logger = new AppLogger(config, "AppProccess");

            InitializeCommunicationAbstractions();
        }

        public List<ProcessId> ShardNodes => _networkNodes[_config.Alias];
        public List<ProcessId> NetworkLeaders => _shardLeaders.Values.ToList();
        public List<ProcessId> NetworkNodes => _networkNodes.Values.SelectMany(it => it).ToList();

        public void Run()
        {
            while (true)
            {
                try
                {
                    foreach (var message in Messages)
                    {
                        foreach (var abstraction in _abstractions.Values)
                        {
                            if (abstraction.Handle(message))
                            {
                                DequeMessage(message);
                            }
                        }
                    }
                } 
                catch (Exception ex)
                {
                    _logger.LogError(ex.ToString());
                }

                if(Messages.Count == 0) Thread.Sleep(1000);
            }
        }

        public bool IsLeader { get { return CurrentShardLeader.Equals(CurrentProccess); } }

        public ProcessId CurrentProccess => ShardNodes.Find(it => _config.IsEqual(it));

        public ProcessId HubProcess => new ProcessId { Host = _config.HubIpAddress, Port = _config.HubPort };

        public ProcessId CurrentShardLeader 
        { 
            get { return _shardLeaders[_config.Alias]; }
            set { _shardLeaders[_config.Alias] = value; }
        }

        public void UpdateExternalShardLeader(ProcessId process)
        {
            _shardLeaders[process.Owner] = process;
        }

        public void AddTransaction(Transaction transaction)
        {
            _transactions.Add(transaction);
        }

        public void AddNewNode(ProcessId process)
        {
            if (_networkNodes.ContainsKey(process.Owner) && !_networkNodes[process.Owner].Any(it => it.Port == process.Port && it.Host == process.Host))
            {
                _networkNodes[process.Owner].Add(process);
            } else
            {
                _networkNodes[process.Owner] = new List<ProcessId> { process };
            }
        }

        public void PrintAccounts()
        {
            var accounts = new Dictionary<string, double>();
            foreach (var tx in _transactions)
            {
                if (!accounts.ContainsKey(tx.To))
                {
                    accounts[tx.To] = tx.Amount;
                }
                else
                {
                    accounts[tx.From] -= tx.Amount;
                    accounts[tx.To] += tx.Amount;
                }
            }
            var output = "\n-----------ACCOUNTS----------\n";
            output += accounts.ToList().ToStringTable(
                new string[] { "ACCOUNT", "AMOUNT" },
                p => p.Key, p => p.Value
                );
            _logger.LogInfo(output);
        }

        public void InitializeCommunicationAbstractions()
        {
            _abstractions.Add("pl", new PerfectLink("pl", _config, EnqueMessage));
            _abstractions.Add("beb", new BestEffortBroadcast("beb", _config, this));
            _abstractions.Add("cp", new ClientProxy(_config, this));
        }

        public void InitializeLeaderMaintenanceAbstractions()
        {
            _abstractions.Add("eld", new EventualLeaderDetector("eld", _config, this));
            _abstractions.Add("epfd", new EventuallyPerfectFailureDetector("epfd", _config, this));
        }

        public void PrintTransactions()
        {
            var output = "\n-----------TRANSACTIONS----------\n";
            output += _transactions.ToStringTable(
                new string[] { "TRANSACTION ID", "SOURCE ACCOUNT", "DESTINATION ACCOUNT", "AMOUNT", },
                p => p.Id, p => p.From, p => p.To, p => p.Amount
                );
            _logger.LogInfo(output);
        }

        public void DequeMessage(Message message)
        {
            if (!_messagesMap.TryRemove(message.MessageUuid, out _)) throw new Exception("Error removing the message.");
        }

        public void EnqueMessage(Message message)
        {
            if (!_messagesMap.TryAdd(message.MessageUuid, message)) throw new Exception("Error adding the message.");
        }

        public ICollection<Message> Messages
        {
            get { return _messagesMap.Values; }
        }

    }
}
