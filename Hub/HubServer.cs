using ConsensusProject.App;
using ConsensusProject.Messages;
using ConsensusProject.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Hub
{
    public class HubServer
    {
        private List<ProcessId> _processes = new List<ProcessId>();
        private MessageBroker _broker;
        private AppLogger _logger;
        private Config _config;

        public HubServer(Config config)
        {
            _config = config;
            _logger = new AppLogger(config, "hub");
            _broker = new MessageBroker(config);
            new Thread(() =>
            {
                HandleMessages();
            }).Start();
        }

        public void Run()
        {
            while (true)
            {
                Console.Write("command> ");
                string cmd = Console.ReadLine();

                switch (cmd)
                {
                    case "help":
                        PrintMainMenu();
                        break;
                    case "tx":
                        MakeTransaction();
                        break;
                    case "deposit":
                        MakeDeposit();
                        break;
                    case "nodes":
                        ListAllNodes();
                        break;
                    default:
                        Console.WriteLine("Incorrect command");
                        break;
                }
            }
        }

        private void HandleMessages()
        {
            while (true)
            {
                if (_broker.Messages.Count == 0)
                {
                    Thread.Sleep(1000);
                    continue;
                }
                
                var msg = _broker.Messages.First();

                switch (msg.NetworkMessage.Message.Type)
                {
                    case Message.Types.Type.AppDecide:
                        var process = _processes.Find(it => it.Host == msg.NetworkMessage.SenderHost && it.Port == msg.NetworkMessage.SenderListeningPort);
                        _logger.LogInfo($"Transaction Id={msg.NetworkMessage.Message.AppDecide.Value.Tx.TxId} accepted by {process.Owner}-{process.Index}");
                        break;
                    case Message.Types.Type.AppRegistration:
                        RegisterProcess(msg);
                        break;
                    default:
                        Console.WriteLine("Unhandled message!");
                        break;
                }

                _broker.DequeMessage(msg);
            }
        }

        private void RegisterProcess(Message message)
        {
            var newProcess = new ProcessId
            {
                Host = message.NetworkMessage.SenderHost,
                Port = message.NetworkMessage.SenderListeningPort,
                Owner = message.NetworkMessage.Message.AppRegistration.Owner,
                Index = message.NetworkMessage.Message.AppRegistration.Index,
                Rank = _processes.Count
            };

            Console.WriteLine($"{newProcess.Owner}-{newProcess.Port}: listening to {newProcess.Host}:{newProcess.Port}");

            _processes.Add(newProcess);
        }

        private void MakeTransaction()
        {
            Console.Write("Source account: ");
            var srcAccount = Console.ReadLine();
            Console.Write("Destination account: ");
            var dstAccount = Console.ReadLine();
            Console.Write("Amount: ");
            double amount = double.Parse(Console.ReadLine());

            var appPropose = new AppPropose
            {
                Value = new Value
                {
                    Defined = true,
                    Tx = new Transaction
                    {
                        TxId = Guid.NewGuid().ToString().Substring(0, 5),
                        SrcAcc = srcAccount,
                        DstAcc = dstAccount,
                        Amount = amount
                    }
                },
            };

            appPropose.Processes.AddRange(_processes);
            Message txMsg = new Message
            {
                MessageUuid = Guid.NewGuid().ToString(),
                Type = Message.Types.Type.NetworkMessage,
                SystemId = Guid.NewGuid().ToString(),
                NetworkMessage = new NetworkMessage
                {
                    SenderHost = _config.HubIpAddress,
                    SenderListeningPort = _config.HubPort,
                    Message = new Message
                    {
                        MessageUuid = Guid.NewGuid().ToString(),
                        SystemId = Guid.NewGuid().ToString(),
                        Type = Message.Types.Type.AppPropose,
                        AppPropose = appPropose
                    }
                }
            };

            foreach (var process in _processes)
            {
                _logger.LogInfo($"Process {process.Owner}-{process.Index} will propose transaction Id={appPropose.Value.Tx.TxId}");
                _broker.SendMessage(txMsg, process.Host, process.Port);
            }
        }

        private void MakeDeposit()
        {
            Console.Write("Destination account: ");
            var dstAccount = Console.ReadLine();
            var srcAccount = string.Empty;
            Console.Write("Amount: ");
            double amount = double.Parse(Console.ReadLine());

            var appPropose = new AppPropose
            {
                Value = new Value
                {
                    Defined = true,
                    Tx = new Transaction
                    {
                        TxId = Guid.NewGuid().ToString().Substring(0, 5),
                        SrcAcc = srcAccount,
                        DstAcc = dstAccount,
                        Amount = amount
                    }
                },
            };

            appPropose.Processes.AddRange(_processes);
            Message txMsg = new Message
            {
                MessageUuid = Guid.NewGuid().ToString(),
                Type = Message.Types.Type.NetworkMessage,
                SystemId = Guid.NewGuid().ToString(),
                NetworkMessage = new NetworkMessage
                {
                    SenderHost = _config.HubIpAddress,
                    SenderListeningPort = _config.HubPort,
                    Message = new Message
                    {
                        MessageUuid = Guid.NewGuid().ToString(),
                        SystemId = Guid.NewGuid().ToString(),
                        Type = Message.Types.Type.AppPropose,
                        AppPropose = appPropose
                    }
                }
            };
            foreach (var process in _processes)
            {
                _logger.LogInfo($"Process {process.Owner}-{process.Index} will propose transaction Id={appPropose.Value.Tx.TxId}");
                _broker.SendMessage(txMsg, process.Host, process.Port);
            }
        }

        private void ListAllNodes()
        {
            string table = _processes.ToStringTable(
                new string[] { "HOST", "PORT", "OWNER", "INDEX", "RANK"},
                it => it.Host, it => it.Port, it => it.Owner, it => it.Index, it => it.Rank);
            Console.WriteLine(table);
        }

        private void PrintMainMenu()
        {
            string menu = @"
    tx - Make a transaction
    deposit - Make a deposit
    nodes - List all nodes
            ";

            System.Console.WriteLine(menu);
        }
    }
}
