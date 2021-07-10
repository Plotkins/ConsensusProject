using ConsensusProject.App;
using System;

namespace ConsensusProject
{
    public class Program
    {
        static void Main(string[] args)
        {
            try
            {
                bool debug = true;

                if (!debug)
                {
                    if (args.Length != 6) throw new Exception("Usage: <hubHost> <hubPort> <nodeHost> <nodePort> <alias> <index>");

                    var config = new Config()
                    {
                        HubIpAddress = args[0],
                        HubPort = Int32.Parse(args[1]),
                        NodeIpAddress = args[2],
                        NodePort = Int32.Parse(args[3]),
                        Alias = args[4],
                        ProccessIndex = Int32.Parse(args[5]),
                        Delay = 100,
                        EpochIncrement = 1,
                    };
                    AppProcess appProcess = new AppProcess(config);
                }
                else
                {
                    NodeHandler nodeHandler = new NodeHandler();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }
    }
}
