using ConsensusProject.App;

namespace Hub
{
    class Program
    {
        static void Main(string[] args)
        {
            var config = new Config
            {
                HubIpAddress = "127.0.0.1",
                HubPort = 5000,
                ProccessIndex = 15,
            };
            var hub = new HubServer(config);

            hub.Run();
        }
    }
}
