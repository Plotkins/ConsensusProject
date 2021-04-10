using ConsensusProject.Messages;

namespace ConsensusProject.App
{
    public class Config
    {
        public string HubIpAddress { get; set; } = "127.0.0.1";
        public int HubPort { get; set; } = 5000;
        public string NodeHandlerIpAddress { get; set; } = "127.0.0.1";
        public int NodeHandlerPort { get; set; } = 3000;
        public string NodeIpAddress { get; set; } = "127.0.0.1";
        public int NodePort { get; set; } = 5000;
        public string Alias { get; set; }
        public int ProccessIndex { get; set; }
        public int Delay { get; set; } = 100;
        public int EpochIncrement { get; set; } = 1;
        public bool IsEqual(ProcessId processId)
        {
            return processId.Owner == Alias &&
                processId.Port == NodePort &&
                processId.Host == NodeIpAddress &&
                processId.Index == ProccessIndex;
        }
    }
}
