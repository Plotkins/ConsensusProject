using ConsensusProject.Abstractions;
using ConsensusProject.Messages;
using ConsensusProject.Utils;
using System.Collections.Concurrent;

namespace ConsensusProject.App
{
    public class AppSystem
    {
        private Config _config;
        private AppLogger _logger;
        private ConcurrentDictionary<string, Abstraction> _abstractions = new ConcurrentDictionary<string, Abstraction>();
        private AppProcess _appProcess;
        private MessageBroker _messageBroker;

        public string SystemId { get; private set; }

        public AppSystem(string systemId, Config config, AppProcess appProcess, MessageBroker messageBroker)
        {
            SystemId = systemId;
            _config = config;
            _logger = new AppLogger(_config, "AppSystem", SystemId);
            _appProcess = appProcess;
            _messageBroker = messageBroker;
            _initializeAbstractions(this);
        }

        public bool Decided => ((UniformConsensus)_abstractions[AbstractionType.Uc.ToString()]).Decided;

        public ProcessId CurrentProcess
        {
            get
            {
                return _appProcess.ShardNodes.Find(it => _config.IsEqual(it));
            }
        }

        public void InitializeNewEpochConsensus(int ets, EpState_ state)
        {
            if (!_abstractions.TryAdd($"{AbstractionType.Ep}{ets}", new EpochConsensus($"{AbstractionType.Ep}{ets}", _config, _appProcess, this, ets, state, _messageBroker)))
                _logger.LogError($"Error adding a new epoch consensus with timestamp {ets}.");
        }

        public int NrOfProcesses
        {
            get
            {
                return _appProcess.ShardNodes.Count;
            }
        }

        #region Private methods
        private void _initializeAbstractions(AppSystem appSystem)
        {
            _abstractions.TryAdd(AbstractionType.Ec.ToString(), new EpochChange(AbstractionType.Ec.ToString(), _config, _appProcess, appSystem, _messageBroker));
            _abstractions.TryAdd(AbstractionType.Uc.ToString(), new UniformConsensus(AbstractionType.Uc.ToString(), _config, _appProcess, appSystem, _messageBroker));
        }
        #endregion Private methods
    }
}
