using ConsensusProject.Abstractions;
using ConsensusProject.Messages;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace ConsensusProject.App
{
    public class AppProccess
    {

        private PerfectLink _perfectLink;
        private Config _config;
        private AppLogger _logger;

        public ConcurrentDictionary<string, Message> _messagesMap;
        public ConcurrentDictionary<string, AppSystem> AppSystems;

        public AppProccess(Config config)
        {
            _messagesMap = new ConcurrentDictionary<string, Message>();
            AppSystems = new ConcurrentDictionary<string, AppSystem>();
            _config = config;
            _logger = new AppLogger(config, "AppProccess");

            Message appRegister = new Message
            {
                MessageUuid = Guid.NewGuid().ToString(),
                Type = Message.Types.Type.AppRegistration,

                AppRegistration = new AppRegistration
                {
                    Index = _config.ProccessIndex,
                    Owner = _config.Alias,
                }
            };

            EnqueMessage(appRegister);

            _perfectLink = new PerfectLink("pl", this, _config);
        }

        public void Run()
        {
            while (true)
            {
                try
                {
                    foreach (var message in Messages)
                    {
                        if (_perfectLink.Handle(message))
                        {
                            DequeMessage(message);
                        }
                        else if (message.Type == Message.Types.Type.AppPropose)
                        {
                            if (!AppSystems.ContainsKey(message.SystemId))
                            {
                                if (
                                        !AppSystems.TryAdd(
                                            message.SystemId,
                                            new AppSystem
                                            (
                                                message.SystemId,
                                                _config,
                                                this,
                                                message.AppPropose.Processes.ToList()
                                            )
                                        )
                                    ) throw new Exception("Error adding the system in app propose.");
                                Message ucPropose = new Message
                                {
                                    MessageUuid = Guid.NewGuid().ToString(),
                                    Type = Message.Types.Type.UcPropose,
                                    SystemId = message.SystemId,
                                    AbstractionId = "uc",

                                    UcPropose = new UcPropose
                                    {
                                        Value = new Value { Defined = true, V = message.AppPropose.Value.V }
                                    }
                                };
                                EnqueMessage(ucPropose);
                                DequeMessage(message);
                            }
                            continue;
                        }
                        else if (message.Type == Message.Types.Type.UcDecide)
                        {
                            Message appDecide = new Message
                            {
                                MessageUuid = Guid.NewGuid().ToString(),
                                SystemId = message.SystemId,
                                Type = Message.Types.Type.AppDecide,
                                AppDecide = new AppDecide
                                {
                                    Value = message.UcDecide.Value
                                }
                            };

                            EnqueMessage(appDecide);
                            DequeMessage(message);
                        }
                    }
                    if (AppSystems.Count > 0)
                    {
                        foreach (var system in AppSystems.Values)
                        {
                            system.EventLoop();
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
