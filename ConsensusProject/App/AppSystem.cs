using ConsensusProject.Abstractions;
using ConsensusProject.Messages;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace ConsensusProject.App
{
    public class AppSystem
    {
        private Config _config;
        private AppLogger _logger;
        private ConcurrentDictionary<string, Abstraction> _abstractions = new ConcurrentDictionary<string, Abstraction>();
        private AppProccess _appProccess;
        public string SystemId { get; private set; }

        public AppSystem(string systemId, Config config, AppProccess appProccess)
        {
            SystemId = systemId;
            _config = config;
            _logger = new AppLogger(_config, "AppSystem", SystemId);
            _appProccess = appProccess;
            _initializeAbstractions(this);

            new Thread(() => EventLoop()).Start();
        }

        public bool Decided => ((UniformConsensus)_abstractions["uc"]).Decided;

        public void EventLoop()
        {
            while (!Decided)
            {
                try
                {
                    var messages = _appProccess.Messages.Where(message => message.SystemId == SystemId);
                    Parallel.ForEach(messages, (message) => {
                        foreach (Abstraction abstraction in _abstractions.Values)
                        {
                            if (abstraction.Handle(message))
                            {
                                _appProccess.DequeMessage(message);
                                break;
                            }
                        }
                    });
                }
                catch (System.Exception)
                {
                    _logger.LogError("AppSystem EventLoop error");
                }
            }
        }

        public ProcessId CurrentProccess
        {
            get
            {
                return _appProccess.ShardNodes.Find(it => _config.IsEqual(it));
            }
        }

        public void InitializeNewEpochConsensus(int ets, EpState_ state)
        {
            if (!_abstractions.TryAdd($"ep{ets}", new EpochConsensus($"ep{ets}", _config, _appProccess, this, ets, state)))
                _logger.LogError($"Error adding a new epoch consensus with timestamp {ets}.");
        }

        public int NrOfProcesses
        {
            get
            {
                return _appProccess.ShardNodes.Count;
            }
        }

        #region Private methods
        private void _initializeAbstractions(AppSystem appSystem)
        {
            _abstractions.TryAdd( "ec", new EpochChange("ec", _config, _appProccess, appSystem));
            _abstractions.TryAdd( "uc", new UniformConsensus("uc", _config, _appProccess, appSystem));
        }
        #endregion Private methods
    }
}
