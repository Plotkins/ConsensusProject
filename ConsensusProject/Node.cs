using ConsensusProject.App;
using System;
using System.Threading;

namespace ConsensusProject
{
    public class Node
    {
        private Thread mainThread;

        public Node(string HubIpAddress, int HubPort, string NodeIpAddress, int nodePort1, string alias, int index)
        {
            try
            {
                Config config = new Config
                {
                    HubIpAddress = HubIpAddress,
                    HubPort = HubPort,
                    NodeIpAddress = NodeIpAddress,
                    NodePort = nodePort1,
                    Alias = alias,
                    ProccessIndex = index,
                    Delay = 1000,
                    EpochIncrement = 1
                };

                AppProccess appProccess = new AppProccess(config);

                mainThread = new Thread(() =>
                {
                    appProccess.Run();
                });

                mainThread.Start();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        public void Stop()
        {
            mainThread.Abort();
        }
    }
}
