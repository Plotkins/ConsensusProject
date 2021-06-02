using ConsensusProject.App;
using ConsensusProject.Messages;
using ConsensusProject.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace ConsensusProject.Abstractions
{
    public class EventualLeaderDetector : Abstraction
    {
        private string _id;
        private AppProccess _appProcces;
        private Config _config;
        private AppLogger _logger;

        //state
        private List<ProcessId> _suspected;

        public EventualLeaderDetector(string id, Config config, AppProccess appProcess)
        {
            _id = id;
            _config = config;
            _logger = new AppLogger(_config, _id);
            _appProcces = appProcess;

            //state
            _suspected = new List<ProcessId>();

            CheckIfLeaderRankChanged();
        }

        public bool Handle(Message message)
        {
            switch (message)
            {
                case Message m when m.Type == Message.Types.Type.EpfdSuspect:
                    return HandleEpfdSuspect(m);
                case Message m when m.Type == Message.Types.Type.EpfdRestore:
                    return HandleEpfdRestore(m);
                case Message m when m.Type == Message.Types.Type.BebDeliver && m.BebDeliver.Message.Type == Message.Types.Type.EldShardTrust:
                    return HandleEldShardTrust(m);
                case Message m when m.Type == Message.Types.Type.CpJoin:
                    return HandleCpJoin(message);
                default:
                    return false;
            }
        }

        private bool HandleCpJoin(Message message)
        {
            _logger.LogInfo($"Handling the message type {Message.Types.Type.CpJoin}.");
            CheckIfLeaderRankChanged();
            return true;
        }

        private bool HandleEldShardTrust(Message message)
        {
            _appProcces.UpdateExternalShardLeader(message.BebDeliver.Message.EldShardTrust.Process);
            return true;
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

            _suspected.Remove(message.EpfdRestore.Process);

            CheckIfLeaderRankChanged();

            return true;
        }

        private void CheckIfLeaderRankChanged()
        {
            List<ProcessId> subProcs = new List<ProcessId>();
            foreach (var proc1 in _appProcces.ShardNodes)
            {
                if (!_suspected.Contains(proc1))
                {
                    subProcs.Add(proc1);
                }
            }

            var maxRankProcess = AbstractionHelpers.GetMaxRankedProcess(subProcs);
            if (maxRankProcess != null && !maxRankProcess.Equals(_appProcces.CurrentShardLeader))
            {
                _appProcces.CurrentShardLeader = maxRankProcess;

                _logger.LogInfo($"{maxRankProcess.Owner}/{maxRankProcess.Index} is the new LEADER.");

                BroadcastEldTrustToInternalSystems();

                BroadcastEldShardTrustToExternalShardNodes();
            }
        }

        private void BroadcastEldTrustToInternalSystems()
        {
            var systemIds = _appProcces.AppSystems.ToArray().Where(it => !it.Value.Decided).Select(it => it.Key);

            foreach (var systemId in systemIds)
            {
                Message trust = new Message
                {
                    MessageUuid = Guid.NewGuid().ToString(),
                    AbstractionId = _id,
                    SystemId = systemId,
                    Type = Message.Types.Type.EldTrust,
                    EldTrust = new EldTrust
                    {
                        Process = _appProcces.CurrentShardLeader
                    }
                };
                _appProcces.EnqueMessage(trust);
            }
        }

        private void BroadcastEldShardTrustToExternalShardNodes()
        {
            var networkBroadcast = new Message
            {
                MessageUuid = Guid.NewGuid().ToString(),
                AbstractionId = "beb",
                Type = Message.Types.Type.BebBroadcast,
                BebBroadcast = new BebBroadcast
                {
                    Type = BebBroadcast.Types.Type.InterShard,
                    Message = new Message
                    {
                        MessageUuid = Guid.NewGuid().ToString(),
                        AbstractionId = _id,
                        Type = Message.Types.Type.EldShardTrust,
                        EldShardTrust = new EldShardTrust
                        {
                            Process = _appProcces.CurrentShardLeader,
                        }
                    }
                }
            };

            _appProcces.EnqueMessage(networkBroadcast);
        }
    }
}
