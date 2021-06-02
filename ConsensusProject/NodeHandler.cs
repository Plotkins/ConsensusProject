using ConsensusProject.App;
using ConsensusProject.Messages;
using ConsensusProject.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace ConsensusProject
{
    public class NodeHandler
    {
        private Dictionary<string, Node> _nodes = new Dictionary<string, Node>();
        private Config _config;
        private TcpWrapper _tcpWrapper;

        public NodeHandler()
        {
            _config = new Config();

            _tcpWrapper = new TcpWrapper(_config.NodeHandlerIpAddress, _config.NodeHandlerPort);
            Listening();
        }

        public void Listening()
        {
            try
            {
                while (true)
                {
                    var byteContent = _tcpWrapper.Receive();
                    Message message = Message.Parser.ParseFrom(byteContent);
                    switch (message.NetworkMessage.Message.Type)
                    {
                        case Message.Types.Type.DeployNodes:
                            Deploy(message.NetworkMessage.Message.DeployNodes.Processes.ToList());
                            break;
                        case Message.Types.Type.StopNodes:
                            Stop(message.NetworkMessage.Message.StopNodes.Processes.ToList());
                            break;
                        default:
                            Console.WriteLine("Wrong message!");
                            break;
                    }
                }
            }
            catch (Exception)
            {
                throw;
            }
        }


        public void Deploy(List<ProcessId> processes)
        {
            try
            {
                var index = _nodes.Count() + 1;

                foreach (var process in processes)
                {
                    var newNode = new Node(_config.HubIpAddress, _config.HubPort, _config.HubIpAddress, process.Port, process.Owner, index);
                    _nodes.Add($"{process.Owner}-{index}", newNode);
                    index++;
                    Thread.Sleep(2000);
                }
            }
            catch
            {
                Console.WriteLine("Try again!");
            }
        }

        public void Stop(List<ProcessId> processes)
        {
            try
            {
                foreach (var process in processes)
                {
                    var nodeId = $"{process.Owner}-{process.Index}";
                    _nodes[nodeId].Stop();
                }
            }
            catch (Exception)
            {
                Console.WriteLine("Try again!");
            }
        }
    }
}
