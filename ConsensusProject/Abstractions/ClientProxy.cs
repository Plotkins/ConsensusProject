using ConsensusProject.App;
using ConsensusProject.Messages;
using ConsensusProject.Utils;
using System;
using System.Collections.Generic;
using System.Linq;

namespace ConsensusProject.Abstractions
{
    public class ClientProxy : Abstraction
    {
        private string _id;
        private AppProcess _appProcess;
        private Config _config;
        private AppLogger _logger;
        private int aknowledgedCount = 0;
        private ProcessId pendingProcess;
        private MessageBroker _messageBroker;

        public ClientProxy(string id, Config config, AppProcess appProcess, MessageBroker messageBroker)
        {
            _id = id;
            _config = config;
            _logger = new AppLogger(config, _id, appProcess.Id);
            _appProcess = appProcess;
            _messageBroker = messageBroker;
            _messageBroker.Subscribe(appProcess.Id, _id, Handle);

            Message appRegister = new Message
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
                        Type = Message.Types.Type.AppRegistrationRequest,
                        AppRegistrationRequest = new AppRegistrationRequest
                        {
                            Index = _config.ProccessIndex,
                            Owner = _config.Alias,
                        }
                    }
                }
            };

            _messageBroker.SendMessage(appRegister);
        }

        public bool Handle(Message message)
        {
            switch (message)
            {
                case Message m when m.Type == Message.Types.Type.PlDeliver && m.PlDeliver.Message.Type == Message.Types.Type.AppRegistrationReply:
                    return HandleRegistrationReply(message);
                case Message m when m.Type == Message.Types.Type.BebDeliver && m.BebDeliver.Message.Type == Message.Types.Type.NewNodeRegistered:
                    return HandleNewNodeRegistered(message);
                case Message m when m.Type == Message.Types.Type.PlDeliver && m.PlDeliver.Message.Type == Message.Types.Type.NewNodeAcknowledged:
                    return HandleNewNodeAcknowledged(message);
                case Message m when m.Type == Message.Types.Type.PlDeliver && m.PlDeliver.Message.Type == Message.Types.Type.NewNodeRejected:
                    return HandleNewNodeRejected(message);
                default:
                    return false;
            }
        }
        public bool HandleNewNodeRejected(Message message)
        {
            _logger.LogInfo($"Handling the message type {Message.Types.Type.NewNodeRejected}.");

            var processesToBroadcast = message.PlDeliver.Message.NewNodeRejected.Processes.Where(node => !_appProcess.NetworkNodes.Contains(node));
            foreach (var node in message.PlDeliver.Message.NewNodeRejected.Processes.Where(node => !_appProcess.NetworkNodes.Contains(node)))
            {
                _appProcess.AddNewNode(node.Clone());
            }

            _appProcess.NetworkVersion = message.PlDeliver.Message.NewNodeRejected.NetworkVersion;

            Message newNodeRegistered = new Message
            {
                MessageUuid = Guid.NewGuid().ToString(),
                AbstractionId = _id,
                SystemId = _appProcess.Id,
                Type = Message.Types.Type.NewNodeRegistered,
                NewNodeRegistered = new NewNodeRegistered
                {
                    Process = _appProcess.CurrentProccess,
                }
            };
            newNodeRegistered.NewNodeRegistered.NetworkProcesses.AddRange(_appProcess.NetworkNodes);

            var broadcast = new BebBroadcast
            {
                Type = BebBroadcast.Types.Type.Custom,
                Message = newNodeRegistered,
            };
            broadcast.Processes.AddRange(processesToBroadcast);

            Message reply = new Message
            {
                MessageUuid = Guid.NewGuid().ToString(),
                AbstractionId = AbstractionType.Beb.ToString(),
                SystemId = _appProcess.Id,
                Type = Message.Types.Type.BebBroadcast,
                BebBroadcast = broadcast
            };

            aknowledgedCount = 0;
            _messageBroker.SendMessage(reply);

            return true;
        }

        public bool HandleNewNodeAcknowledged(Message message)
        {
            _logger.LogInfo($"Handling the message type {Message.Types.Type.NewNodeAcknowledged}. Aknowledged {aknowledgedCount + 1}/{_appProcess.NetworkNodes.Count}");

            aknowledgedCount++;

            CheckIfAllAknowledgedRegistration();

            return true;
        }

        private void CheckIfAllAknowledgedRegistration()
        {
            if (aknowledgedCount == _appProcess.NetworkNodes.Count && pendingProcess != null)
            {
                _appProcess.AddNewNode(pendingProcess.Clone());
                pendingProcess = null;
                aknowledgedCount = 0;
                _appProcess.InitializeLeaderMaintenanceAbstractions();
            }
        }

        private bool HandleNewNodeRegistered(Message message)
        {
            _logger.LogInfo($"Handling the message type {Message.Types.Type.NewNodeRegistered}.");

            Message response = null;

            var newNode = message.BebDeliver.Message.NewNodeRegistered.Process;
            var networkProcesses = message.BebDeliver.Message.NewNodeRegistered.NetworkProcesses;

            var intersection = networkProcesses.Intersect(_appProcess.NetworkNodes).ToList();
            List<ProcessId> newNodes = new List<ProcessId>();

            foreach (var node in networkProcesses.Where(node => !_appProcess.NetworkNodes.Contains(node)))
            {
                newNodes.Add(node);
            }

            if (intersection.Union(_appProcess.NetworkNodes).Count() > intersection.Count)
            {
                response = new Message
                {
                    MessageUuid = Guid.NewGuid().ToString(),
                    AbstractionId = _id,
                    SystemId = _appProcess.Id,
                    Type = Message.Types.Type.NewNodeRejected,
                    NewNodeRejected = new NewNodeRejected()
                };
                response.NewNodeRejected.Processes.AddRange(_appProcess.NetworkNodes.Union(newNodes));
            }
            else if (intersection.Union(networkProcesses).Count() >= intersection.Count && !_appProcess.NetworkNodes.Contains(newNode))
            {
                newNodes.Add(newNode);

                response = new Message
                {
                    MessageUuid = Guid.NewGuid().ToString(),
                    AbstractionId = _id,
                    SystemId = _appProcess.Id,
                    Type = Message.Types.Type.NewNodeAcknowledged,
                    NewNodeAcknowledged = new NewNodeAcknowledged(),
                };
            }

            if (response != null)
            {
                var bebDeliver = new Message
                {
                    MessageUuid = Guid.NewGuid().ToString(),
                    AbstractionId = AbstractionType.Pl.ToString(),
                    SystemId = _appProcess.Id,
                    Type = Message.Types.Type.PlSend,
                    PlSend = new PlSend
                    {
                        Destination = message.BebDeliver.Sender,
                        Message = response
                    }
                };

                _messageBroker.SendMessage(bebDeliver);
            }

            if (newNodes.Count > 0)
            {
                foreach (var node in newNodes)
                {
                    _appProcess.AddNewNode(node.Clone());
                    _logger.LogInfo($"{node.Owner}/{node.Index} is the new node added. {_appProcess.NetworkNodes.Count} total nodes available.");
                }

                Message cpJoin = new Message
                {
                    MessageUuid = Guid.NewGuid().ToString(),
                    AbstractionId = AbstractionType.Eld.ToString(),
                    SystemId = _appProcess.Id,
                    Type = Message.Types.Type.CpJoin,
                    CpJoin = new CpJoin()
                };

                _messageBroker.SendMessage(cpJoin);
            }

            return true;
        }

        private bool HandleRegistrationReply(Message message)
        {
            _logger.LogInfo($"Handling RegistrationReply");

            pendingProcess = message.PlDeliver.Message.AppRegistrationReply.NewProcess;

            if (message.PlDeliver.Message.AppRegistrationReply.Processes.Count == 0)
            {
                CheckIfAllAknowledgedRegistration();
            }
            else
            {
                foreach (var process in message.PlDeliver.Message.AppRegistrationReply.Processes)
                {
                    _appProcess.AddNewNode(process);
                }

                Message newNodeRegistered = new Message
                {
                    MessageUuid = Guid.NewGuid().ToString(),
                    AbstractionId = AbstractionType.Cp.ToString(),
                    SystemId = _appProcess.Id,
                    Type = Message.Types.Type.NewNodeRegistered,
                    NewNodeRegistered = new NewNodeRegistered
                    {
                        Process = message.PlDeliver.Message.AppRegistrationReply.NewProcess,
                    }
                };
                newNodeRegistered.NewNodeRegistered.NetworkProcesses.AddRange(_appProcess.NetworkNodes);

                Message reply = new Message
                {
                    MessageUuid = Guid.NewGuid().ToString(),
                    AbstractionId = AbstractionType.Beb.ToString(),
                    SystemId = _appProcess.Id,
                    Type = Message.Types.Type.BebBroadcast,
                    BebBroadcast = new BebBroadcast
                    {
                        Type = BebBroadcast.Types.Type.Network,
                        Message = newNodeRegistered
                    }
                };

                _messageBroker.SendMessage(reply);
            }
           
            return true;
        }
    }
}
