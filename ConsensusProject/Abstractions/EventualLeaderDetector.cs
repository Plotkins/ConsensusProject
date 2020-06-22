using ConsensusProject.App;
using ConsensusProject.Messages;
using ConsensusProject.Utils;
using System;
using System.Collections.Generic;

namespace ConsensusProject.Abstractions
{
    public class EventualLeaderDetector : Abstraction
    {
        private string _id;
        private AppProccess _appProcces;
        private AppSystem _appSystem;
        private Config _config;
        private AppLogger _logger;
        private List<ProcessId> _systemProcesses;

        //state
        private List<ProcessId> _suspected;
        private ProcessId _leader;

        public EventualLeaderDetector(string id, Config config, AppProccess appProcess, AppSystem appSystem, List<ProcessId> systemProcesses)
        {
            _id = id;
            _config = config;
            _logger = new AppLogger(_config, _id, appSystem.SystemId);
            _appProcces = appProcess;
            _appSystem = appSystem;
            _systemProcesses = systemProcesses;

            //state
            _suspected = new List<ProcessId>();
            _leader = null;
        }

        public bool Handle(Message message)
        {
            switch (message)
            {
                case Message m when m.Type == Message.Types.Type.EpfdSuspect:
                    return HandleEpfdSuspect(m);
                case Message m when m.Type == Message.Types.Type.EpfdRestore:
                    return HandleEpfdRestore(m);
                default:
                    return false;
            }
        }

        private bool HandleEpfdSuspect(Message message)
        {
            _logger.LogInfo($"Handling the message type {Message.Types.Type.EpfdSuspect}.");

            _suspected.Add(message.EpfdSuspect.Process);

            CheckIfLeaderRankChanged();

            return true;
        }
        private bool HandleEpfdRestore(Message message)
        {
            _logger.LogInfo($"Handling the message type {Message.Types.Type.EpfdRestore}.");

            int i = 0;
            while (i < _suspected.Count)
            {
                if (_suspected[i].Equals(message.EpfdRestore.Process))
                {
                    _suspected.RemoveAt(i);
                }
                else
                {
                    i += 1;
                }
            }

            CheckIfLeaderRankChanged();

            return true;
        }

        private void CheckIfLeaderRankChanged()
        {
            List<ProcessId> subProcs = new List<ProcessId>();
            foreach (var proc1 in _systemProcesses)
            {
                bool found = false;
                foreach(var proc2 in _suspected)
                {
                    if (proc1.Equals(proc2))
                    {
                        found = true;
                        break;
                    }
                }
                if (!found)
                {
                    subProcs.Add(proc1);
                }
            }
            var maxRankProcess = AbstractionHelpers.GetMaxRankedProcess(subProcs);
            if (maxRankProcess != null && !maxRankProcess.Equals(_leader))
            {
                _leader = maxRankProcess;
                Message trust = new Message
                {
                    MessageUuid = Guid.NewGuid().ToString(),
                    AbstractionId = _id,
                    SystemId = _appSystem.SystemId,
                    Type = Message.Types.Type.EldTrust,
                    EldTrust = new EldTrust
                    {
                        Process = _leader
                    }
                };
                _appProcces.EnqueMessage(trust);
            }
        }
    }
}
