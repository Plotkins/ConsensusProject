using ConsensusProject.App;
using ConsensusProject.Messages;
using System;

namespace ConsensusProject.Abstractions
{
    public class ClientProxy : Abstraction
    {
        private AppProccess _appProccess;
        private Config _config;
        private AppLogger _logger;

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
                case Message m when m.Type == Message.Types.Type.BebDeliver && m.BebDeliver.Message.Type == Message.Types.Type.NewNodeRegistered:
                    return HandleNewNodeRegistered(message);
                case Message m when m.Type == Message.Types.Type.PlDeliver && m.PlDeliver.Message.Type == Message.Types.Type.AppRegistrationReply:
                    return HandleRegistrationReply(message);
                case Message m when m.Type == Message.Types.Type.PlDeliver && m.PlDeliver.Message.Type == Message.Types.Type.AppPropose:
                    return HandleAppPropose(message);
                case Message m when m.Type == Message.Types.Type.PlDeliver && m.PlDeliver.Message.Type == Message.Types.Type.UcDecide:
                    return HandleUcDecide(message);
                default:
                    return false;
            }
        }

        private bool HandleNewNodeRegistered(Message message)
        {
            _appProccess.AddNewNode(message.BebDeliver.Message.NewNodeRegistered.Process);

            return true;
        }

        private bool HandleRegistrationReply(Message message)
        {
            foreach (var process in message.PlDeliver.Message.AppRegistrationReply.Processes)
            {
                _appProccess.AddNewNode(process);
            }

            Message newNodeRegistered = new Message
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
                        AbstractionId = "cp",
                        Type = Message.Types.Type.NewNodeRegistered,
                        NewNodeRegistered = new NewNodeRegistered
                        {
                            Process = _appProccess.CurrentProccess
                        }
                    }
                }
            };

            _appProccess.EnqueMessage(newNodeRegistered);

            _appProccess.InitializeLeaderMaintenanceAbstractions();

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

            Message ucPropose = new Message
            {
                MessageUuid = Guid.NewGuid().ToString(),
                Type = Message.Types.Type.UcPropose,
                SystemId = message.SystemId,
                AbstractionId = "uc",

                UcPropose = new UcPropose
                {
                    Value = message.AppPropose.Value.Clone()
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
