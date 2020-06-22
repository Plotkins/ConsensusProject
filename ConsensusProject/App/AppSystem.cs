using ConsensusProject.Abstractions;
using ConsensusProject.Messages;
using System.Collections.Generic;

namespace ConsensusProject.App
{
    public class AppSystem
    {
        private List<ProcessId> _processes;
        private Config _config;
        private AppLogger _logger;
        private Dictionary<string, Abstraction> _abstractions;
        private AppProccess _appProccess;
        public string SystemId { get; private set; }

        public AppSystem(string systemId, Config config, AppProccess appProccess, List<ProcessId> processes)
        {
            SystemId = systemId;
            _processes = processes;
            _config = config;
            _logger = new AppLogger(_config, "AppSystem", SystemId);
            _appProccess = appProccess;
            _initializeAbstractions(this);
        }

        public void EventLoop()
        {
            foreach(var message in _appProccess.Messages)
            {
                if (message.SystemId == SystemId)
                {
                    foreach(Abstraction abstraction in _abstractions.Values)
                    {
                        if (abstraction.Handle(message))
                        {
                            _appProccess.DequeMessage(message);
                        }
                    }
                }
            }
        }

        public ProcessId CurrentProccess
        {
            get
            {
                return _processes.Find(it => _config.IsEqual(it));
            }
        }

        public void InitializeNewEpochConsensus(int ets, EpState_ state, ProcessId leader)
        {
            if (!_abstractions.TryAdd($"ep{ets}", new EpochConsensus($"ep{ets}", _config, _appProccess, this, _processes, ets, state, leader)))
                throw new System.Exception($"Error adding a new epoch consensus with timestamp {ets}.");
        }

        public int NrOfProcesses
        {
            get
            {
                return _processes.Count;
            }
        }

        #region Private methods
        private void _initializeAbstractions(AppSystem appSystem)
        {
            EpochChange epochChange = new EpochChange("ec", _config, _appProccess, appSystem, _processes);
            _abstractions = new Dictionary<string, Abstraction>();
            _abstractions.Add( "ec", epochChange);
            _abstractions.Add( "beb", new BestEffortBroadcast("beb", _config, _appProccess, appSystem, _processes) );
            _abstractions.Add( "eld", new EventualLeaderDetector("eld", _config, _appProccess, appSystem, _processes) );
            _abstractions.Add( "epfd", new EventuallyPerfectFailureDetector("epfd", _config, _appProccess, appSystem, _processes));
            _abstractions.Add( "uc", new UniformConsensus("uc", _config, _appProccess, appSystem, _processes, epochChange) );
        }
        #endregion Private methods
    }
}
