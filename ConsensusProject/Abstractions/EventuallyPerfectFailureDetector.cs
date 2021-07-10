using ConsensusProject.App;
using ConsensusProject.Messages;
using ConsensusProject.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ConsensusProject.Abstractions
{
    public class EventuallyPerfectFailureDetector : Abstraction
    {
        private string _id;
        private AppProcess _appProcess;
        private Config _config;
        private AppLogger _logger;

        //state
        private List<ProcessId> _alive;
        private List<ProcessId> _suspected;
        private int _delay;
        private MessageBroker _messageBroker;


        public EventuallyPerfectFailureDetector(string id, Config config, AppProcess appProcess, MessageBroker messageBroker)
        {
            _id = id;
            _config = config;
            _logger = new AppLogger(_config, _id, appProcess.Id);
            _appProcess = appProcess;
            _messageBroker = messageBroker;
            _messageBroker.Subscribe(appProcess.Id, _id, Handle);

            //state
            _alive = new List<ProcessId>(_appProcess.ShardNodes);
            _suspected = new List<ProcessId>();
            _delay = _config.Delay;

            StartTimer(_delay);
        }

        public bool Handle(Message message)
        {
            switch (message)
            {
                case Message m when m.Type == Message.Types.Type.PlDeliver && m.PlDeliver.Message.Type == Message.Types.Type.EpfdHeartbeatRequest:
                    return HandleEpfdHearthBeatRequest(m);
                case Message m when m.Type == Message.Types.Type.PlDeliver && m.PlDeliver.Message.Type == Message.Types.Type.EpfdHeartbeatReply:
                    return HandleEpfdHearthBeatReply(m);
                default:
                    return false;
            }
        }

        private bool HandleEpfdHearthBeatRequest(Message message)
        {
            Message reply = new Message
            {
                MessageUuid = Guid.NewGuid().ToString(),
                AbstractionId = AbstractionType.Pl.ToString(),
                SystemId = _appProcess.Id,
                Type = Message.Types.Type.PlSend,
                PlSend = new PlSend
                {
                    Destination = message.PlDeliver.Sender,
                    Message = new Message
                    {
                        MessageUuid = Guid.NewGuid().ToString(),
                        AbstractionId = _id,
                        SystemId = _appProcess.Id,
                        Type = Message.Types.Type.EpfdHeartbeatReply,
                        EpfdHeartbeatReply = new EpfdHeartbeatReply_(),
                    }
                }
            };
            _messageBroker.SendMessage(reply);
            return true;
        }

        private bool HandleEpfdHearthBeatReply(Message message)
        {
            var node = _appProcess.ShardNodes.FirstOrDefault(it => it.Host == message.PlDeliver.Sender.Host && it.Port == message.PlDeliver.Sender.Port);
            if(node != null) _alive.Add(node);
            return true;
        }

        private void HandleTimeout()
        {
            if(_alive.Intersect(_suspected).Count() == 0)
            {
                _delay += _config.Delay;
            }
            foreach(var procces in _appProcess.ShardNodes)
            {
                if (!_alive.Contains(procces) && !_suspected.Contains(procces))
                {
                    _suspected.Add(procces);
                    Message suspect = new Message
                    {
                        MessageUuid = Guid.NewGuid().ToString(),
                        AbstractionId = AbstractionType.Eld.ToString(),
                        SystemId = _appProcess.Id,
                        Type = Message.Types.Type.EpfdSuspect,
                        EpfdSuspect = new EpfdSuspect
                        {
                            Process = procces
                        }
                    };
                    _messageBroker.SendMessage(suspect);
                }
                else if (_alive.Contains(procces) && _suspected.Contains(procces))
                {
                    _suspected.Remove(procces);
                    Message restore = new Message
                    {
                        MessageUuid = Guid.NewGuid().ToString(),
                        AbstractionId = AbstractionType.Eld.ToString(),
                        SystemId = _appProcess.Id,
                        Type = Message.Types.Type.EpfdRestore,
                        EpfdRestore = new EpfdRestore
                        {
                            Process = procces
                        }
                    };
                    _messageBroker.SendMessage(restore);
                }

                Message request = new Message
                {
                    MessageUuid = Guid.NewGuid().ToString(),
                    AbstractionId = AbstractionType.Pl.ToString(),
                    SystemId = _appProcess.Id,
                    Type = Message.Types.Type.PlSend,
                    PlSend = new PlSend
                    {
                        Destination = procces,
                        Message = new Message
                        {
                            MessageUuid = Guid.NewGuid().ToString(),
                            AbstractionId = _id,
                            SystemId = _appProcess.Id,
                            Type = Message.Types.Type.EpfdHeartbeatRequest,
                            EpfdHeartbeatRequest = new EpfdHeartbeatRequest_()
                        }
                    }
                };
                _messageBroker.SendMessage(request);
            }
            _alive = new List<ProcessId>();
            StartTimer(_delay);
        }
        private async Task StartTimer(int delay)
        {
            await Task.Delay(delay);
            HandleTimeout();
        }
    }
}
