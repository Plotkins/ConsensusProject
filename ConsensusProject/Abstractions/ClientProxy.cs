using ConsensusProject.App;
using ConsensusProject.Messages;
using System;
using System.Collections.Generic;
using System.Linq;

namespace ConsensusProject.Abstractions
{
    public class ClientProxy : Abstraction
    {
        private AppProccess _appProccess;
        private Config _config;
        private AppLogger _logger;
        private int aknowledgedCount = 0;
        private ProcessId pendingProcess;

        public ClientProxy(Config config, AppProccess appProccess)
        {
            _config = config;
            _logger = new AppLogger(config, "cp");
            _appProccess = appProccess;

            Message appRegister = new Message
            {
                MessageUuid = Guid.NewGuid().ToString(),
                AbstractionId = "pl",
                Type = Message.Types.Type.PlSend,
                PlSend = new PlSend
                {
                    Destination = _appProccess.HubProcess,
                    Message = new Message
                    {
                        MessageUuid = Guid.NewGuid().ToString(),
                        Type = Message.Types.Type.AppRegistrationRequest,

                        AppRegistrationRequest = new AppRegistrationRequest
                        {
                            Index = _config.ProccessIndex,
                            Owner = _config.Alias,
                        }
                    }
                }
            };

            _appProccess.EnqueMessage(appRegister);
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
                case Message m when m.Type == Message.Types.Type.PlDeliver && m.PlDeliver.Message.Type == Message.Types.Type.AppPropose:
                    return HandleAppPropose(message);
                case Message m when m.Type == Message.Types.Type.UcDecide:
                    return HandleUcDecide(message);
                default:
                    return false;
            }
        }
        public bool HandleNewNodeRejected(Message message)
        {
            _logger.LogInfo($"Handling the message type {Message.Types.Type.NewNodeRejected}.");

            var processesToBroadcast = message.PlDeliver.Message.NewNodeRejected.Processes.Where(node => !_appProccess.NetworkNodes.Contains(node));
            foreach (var node in message.PlDeliver.Message.NewNodeRejected.Processes.Where(node => !_appProccess.NetworkNodes.Contains(node)))
            {
                _appProccess.AddNewNode(node.Clone());
            }

            _appProccess.NetworkVersion = message.PlDeliver.Message.NewNodeRejected.NetworkVersion;

            Message newNodeRegistered = new Message
            {
                MessageUuid = Guid.NewGuid().ToString(),
                AbstractionId = "cp",
                Type = Message.Types.Type.NewNodeRegistered,
                NewNodeRegistered = new NewNodeRegistered
                {
                    Process = _appProccess.CurrentProccess,
                }
            };
            newNodeRegistered.NewNodeRegistered.NetworkProcesses.AddRange(_appProccess.NetworkNodes);

            var broadcast = new BebBroadcast
            {
                Type = BebBroadcast.Types.Type.Custom,
                Message = newNodeRegistered,
            };
            broadcast.Processes.AddRange(processesToBroadcast);

            Message reply = new Message
            {
                MessageUuid = Guid.NewGuid().ToString(),
                AbstractionId = "beb",
                Type = Message.Types.Type.BebBroadcast,
                BebBroadcast = broadcast
            };

            aknowledgedCount = 0;
            _appProccess.EnqueMessage(reply);

            return true;
        }

        public bool HandleNewNodeAcknowledged(Message message)
        {
            _logger.LogInfo($"Handling the message type {Message.Types.Type.NewNodeAcknowledged}. Aknowledged {aknowledgedCount + 1}/{_appProccess.NetworkNodes.Count}");

            aknowledgedCount++;

            CheckIfAllAknowledgedRegistration();

            return true;
        }

        private void CheckIfAllAknowledgedRegistration()
        {
            if (aknowledgedCount == _appProccess.NetworkNodes.Count && pendingProcess != null)
            {
                _appProccess.AddNewNode(pendingProcess.Clone());
                pendingProcess = null;
                aknowledgedCount = 0;
                _appProccess.InitializeLeaderMaintenanceAbstractions();
            }
        }

        private bool HandleNewNodeRegistered(Message message)
        {
            _logger.LogInfo($"Handling the message type {Message.Types.Type.NewNodeRegistered}.");

            Message response = null;

            var newNode = message.BebDeliver.Message.NewNodeRegistered.Process;
            var networkProcesses = message.BebDeliver.Message.NewNodeRegistered.NetworkProcesses;

            var intersection = networkProcesses.Intersect(_appProccess.NetworkNodes).ToList();
            List<ProcessId> newNodes = new List<ProcessId>();

            foreach (var node in networkProcesses.Where(node => !_appProccess.NetworkNodes.Contains(node)))
            {
                newNodes.Add(node);
            }

            if (intersection.Union(_appProccess.NetworkNodes).Count() > intersection.Count)
            {
                response = new Message
                {
                    MessageUuid = Guid.NewGuid().ToString(),
                    Type = Message.Types.Type.NewNodeRejected,
                    NewNodeRejected = new NewNodeRejected()
                };
                response.NewNodeRejected.Processes.AddRange(_appProccess.NetworkNodes.Union(newNodes));
            }
            else if (intersection.Union(networkProcesses).Count() >= intersection.Count && !_appProccess.NetworkNodes.Contains(newNode))
            {
                newNodes.Add(newNode);

                response = new Message
                {
                    MessageUuid = Guid.NewGuid().ToString(),
                    Type = Message.Types.Type.NewNodeAcknowledged,
                    NewNodeAcknowledged = new NewNodeAcknowledged(),
                };
            }

            if (response != null)
            {
                var bebDeliver = new Message
                {
                    MessageUuid = Guid.NewGuid().ToString(),
                    AbstractionId = "pl",
                    Type = Message.Types.Type.PlSend,
                    PlSend = new PlSend
                    {
                        Destination = message.BebDeliver.Sender,
                        Message = response
                    }
                };

                _appProccess.EnqueMessage(bebDeliver);
            }

            if (newNodes.Count > 0)
            {
                foreach (var node in newNodes)
                {
                    _appProccess.AddNewNode(node.Clone());
                    _logger.LogInfo($"{node.Owner}/{node.Index} is the new node added. {_appProccess.NetworkNodes.Count} total nodes available.");
                }

                Message cpJoin = new Message
                {
                    MessageUuid = Guid.NewGuid().ToString(),
                    Type = Message.Types.Type.CpJoin,
                    CpJoin = new CpJoin()
                };

                _appProccess.EnqueMessage(cpJoin);
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
                    _appProccess.AddNewNode(process);
                }

                Message newNodeRegistered = new Message
                {
                    MessageUuid = Guid.NewGuid().ToString(),
                    AbstractionId = "cp",
                    Type = Message.Types.Type.NewNodeRegistered,
                    NewNodeRegistered = new NewNodeRegistered
                    {
                        Process = message.PlDeliver.Message.AppRegistrationReply.NewProcess,
                    }
                };
                newNodeRegistered.NewNodeRegistered.NetworkProcesses.AddRange(_appProccess.NetworkNodes);

                Message reply = new Message
                {
                    MessageUuid = Guid.NewGuid().ToString(),
                    AbstractionId = "beb",
                    Type = Message.Types.Type.BebBroadcast,
                    BebBroadcast = new BebBroadcast
                    {
                        Type = BebBroadcast.Types.Type.Network,
                        Message = newNodeRegistered
                    }
                };

                _appProccess.EnqueMessage(reply);
            }
           
            return true;
        }

        private bool HandleAppPropose(Message message)
        {
            if (!_appProccess.AppSystems.TryAdd(message.SystemId, new AppSystem(message.SystemId, _config, _appProccess)))
            {
                _logger.LogInfo($"The process is already assigned to the system with Id={message.SystemId}!");
            }
            else
            {
                _logger.LogInfo($"New system with Id={message.SystemId} added to the process!");
            }

            _appProccess.PrintNetworkNodes();

            Message ucPropose = new Message
            {
                MessageUuid = Guid.NewGuid().ToString(),
                Type = Message.Types.Type.UcPropose,
                SystemId = message.SystemId,
                AbstractionId = "uc",

                UcPropose = new UcPropose
                {
                    Value = message.PlDeliver.Message.AppPropose.Value.Clone()
                }
            };

            _appProccess.EnqueMessage(ucPropose);
            return true;
        }

        private bool HandleUcDecide(Message message)
        {
            Message appDecide =  new Message
            {
                MessageUuid = Guid.NewGuid().ToString(),
                AbstractionId = "pl",
                Type = Message.Types.Type.PlSend,
                PlSend = new PlSend
                {
                    Destination = _appProccess.HubProcess,
                    Message = new Message
                    {
                        MessageUuid = Guid.NewGuid().ToString(),
                        SystemId = message.SystemId,
                        Type = Message.Types.Type.AppDecide,
                        AppDecide = new AppDecide
                        {
                            Value = message.UcDecide.Value
                        }
                    }
                }
            };

            _appProccess.AddTransaction(message.UcDecide.Value.Transaction);

            _logger.LogInfo($"Consensus for transaction with Id={message.UcDecide.Value.Transaction.Id} ended.");

            _appProccess.PrintAccounts();
            _appProccess.PrintTransactions();

            _appProccess.EnqueMessage(appDecide);

            return true;
        }
    }
}
