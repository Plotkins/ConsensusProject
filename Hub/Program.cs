using ConsensusProject.App;

namespace Hub
{
    class Program
    {
        static void Main(string[] args)
        {
            var config = new Config
            {
                ProccessIndex = 15,
            };
            var hub = new HubServer(config);

            hub.Run();
        }
    }
}
