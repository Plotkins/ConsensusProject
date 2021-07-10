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
        private AppProcess _appProcess;
        private Config _config;
        private AppLogger _logger;
        private MessageBroker _messageBroker;

        //state
        private List<ProcessId> _suspected;

        public EventualLeaderDetector(string id, Config config, AppProcess appProcess, MessageBroker messageBroker)
        {
            _id = id;
            _config = config;
            _logger = new AppLogger(_config, _id, appProcess.Id);
            _appProcess = appProcess;
            _messageBroker = messageBroker;
            _messageBroker.Subscribe(appProcess.Id, _id, Handle);

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
            _appProcess.UpdateExternalShardLeader(message.BebDeliver.Message.EldShardTrust.Process);
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
            foreach (var proc1 in _appProcess.ShardNodes)
            {
                if (!_suspected.Contains(proc1))
                {
                    subProcs.Add(proc1);
                }
            }

            var maxRankProcess = AbstractionHelpers.GetMaxRankedProcess(subProcs);
            if (maxRankProcess != null && !maxRankProcess.Equals(_appProcess.CurrentShardLeader))
            {
                _appProcess.CurrentShardLeader = maxRankProcess;

                _logger.LogInfo($"{maxRankProcess.Owner}/{maxRankProcess.Index} is the new LEADER.");

                BroadcastEldTrustToInternalSystems();

                if (_appProcess.IsLeader)
                    BroadcastEldShardTrustToExternalShardNodes();
            }
        }

        private void BroadcastEldTrustToInternalSystems()
        {
            var systemIds = _appProcess.AppSystems.ToArray().Where(it => !it.Value.Decided).Select(it => it.Key);

            foreach (var systemId in systemIds)
            {
                Message trust = new Message
                {
                    MessageUuid = Guid.NewGuid().ToString(),
                    AbstractionId = AbstractionType.Ec.ToString(),
                    SystemId = systemId,
                    Type = Message.Types.Type.EldTrust,
                    EldTrust = new EldTrust
                    {
                        Process = _appProcess.CurrentShardLeader
                    }
                };
                _messageBroker.SendMessage(trust);
            }
        }

        private void BroadcastEldShardTrustToExternalShardNodes()
        {
            var networkBroadcast = new Message
            {
                MessageUuid = Guid.NewGuid().ToString(),
                AbstractionId = AbstractionType.Beb.ToString(),
                SystemId = _appProcess.Id,
                Type = Message.Types.Type.BebBroadcast,
                BebBroadcast = new BebBroadcast
                {
                    Type = BebBroadcast.Types.Type.InterShard,
                    Message = new Message
                    {
                        MessageUuid = Guid.NewGuid().ToString(),
                        AbstractionId = AbstractionType.Eld.ToString(),
                        SystemId = _appProcess.Id,
                        Type = Message.Types.Type.EldShardTrust,
                        EldShardTrust = new EldShardTrust
                        {
                            Process = _appProcess.CurrentShardLeader,
                        }
                    }
                }
            };

            _messageBroker.SendMessage(networkBroadcast);
        }
    }
}
