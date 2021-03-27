using ConsensusProject.App;
using System;

namespace ConsensusProject
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                bool debug = true;

                string HubIpAddress;
                int HubPort;
                string NodeIpAddress;
                int nodePort1;
                string alias;
                int index;

                if (debug)
                {
                    HubIpAddress = "127.0.0.1";
                    HubPort = 5000;
                    NodeIpAddress = "127.0.0.1";
                    nodePort1 = 5006;
                    alias = "george";
                    index = 3;
                }
                else
                {
                    if (args.Length != 6) throw new Exception("Usage: <hubHost> <hubPort> <nodeHost> <nodePort> <alias> <index>");

                    HubIpAddress = args[0];
                    HubPort = Int32.Parse(args[1]);
                    NodeIpAddress = args[2];
                    nodePort1 = Int32.Parse(args[3]);
                    alias = args[4];
                    index = Int32.Parse(args[5]);
                }

                Config config1 = new Config
                {
                    HubIpAddress = HubIpAddress,
                    HubPort = HubPort,
                    NodeIpAddress = NodeIpAddress,
                    NodePort = nodePort1,
                    Alias = alias,
                    ProccessIndex = index,
                    Delay = 100,
                    EpochIncrement = 1
                };

                if (debug)
                {
                    NodeHandler nodeHandler = new NodeHandler(config1);
                }
                else
                {
                    AppProccess appProccess1 = new AppProccess(config1);

                    appProccess1.Run();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }
    }
}
