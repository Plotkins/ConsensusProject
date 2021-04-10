using ConsensusProject.App;
using ConsensusProject.Messages;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ConsensusProject.Abstractions
{
    public class EventuallyPerfectFailureDetector : Abstraction
    {
        private string _id;
        private AppProccess _appProcces;
        private Config _config;
        private AppLogger _logger;

        //state
        private List<ProcessId> _alive;
        private List<ProcessId> _suspected;
        private int _delay;


        public EventuallyPerfectFailureDetector(string id, Config config, AppProccess appProcess)
        {
            _id = id;
            _config = config;
            _logger = new AppLogger(_config, _id);
            _appProcces = appProcess;

            //state
            _alive = new List<ProcessId>(_appProcces.ShardNodes);
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
            //_logger.LogInfo($"Handling the message type {Message.Types.Type.EpfdHeartbeatRequest}.");
            Message reply = new Message
            {
                MessageUuid = Guid.NewGuid().ToString(),
                AbstractionId = "pl",
                Type = Message.Types.Type.PlSend,
                PlSend = new PlSend
                {
                    Destination = message.PlDeliver.Sender,
                    Message = new Message
                    {
                        MessageUuid = Guid.NewGuid().ToString(),
                        AbstractionId = _id,
                        Type = Message.Types.Type.EpfdHeartbeatReply,
                        EpfdHeartbeatReply = new EpfdHeartbeatReply_(),
                    }
                }
            };
            _appProcces.EnqueMessage(reply);
            return true;
        }

        private bool HandleEpfdHearthBeatReply(Message message)
        {
            //_logger.LogInfo($"Handling the message type {Message.Types.Type.EpfdHeartbeatReply}.");
            _alive.Add(message.PlDeliver.Sender);
            return true;
        }

        private void HandleTimeout()
        {
            if(_alive.Intersect(_suspected).Count() == 0)
            {
                _delay += _config.Delay;
                //_logger.LogInfo($"Increased delay to {_delay}.");
            }
            foreach(var procces in _appProcces.ShardNodes)
            {
                if (!_alive.Contains(procces) && !_suspected.Contains(procces))
                {
                    _suspected.Add(procces);
                    Message suspect = new Message
                    {
                        MessageUuid = Guid.NewGuid().ToString(),
                        AbstractionId = _id,
                        Type = Message.Types.Type.EpfdSuspect,
                        EpfdSuspect = new EpfdSuspect
                        {
                            Process = procces
                        }
                    };
                    _appProcces.EnqueMessage(suspect);
                }
                else if (_alive.Contains(procces) && _suspected.Contains(procces))
                {
                    _suspected.Remove(procces);
                    Message restore = new Message
                    {
                        MessageUuid = Guid.NewGuid().ToString(),
                        AbstractionId = _id,
                        Type = Message.Types.Type.EpfdRestore,
                        EpfdRestore = new EpfdRestore
                        {
                            Process = procces
                        }
                    };
                    _appProcces.EnqueMessage(restore);
                }
                Message request = new Message
                {
                    MessageUuid = Guid.NewGuid().ToString(),
                    AbstractionId = "pl",
                    Type = Message.Types.Type.PlSend,
                    PlSend = new PlSend
                    {
                        Destination = procces,
                        Message = new Message
                        {
                            MessageUuid = Guid.NewGuid().ToString(),
                            AbstractionId = _id,
                            Type = Message.Types.Type.EpfdHeartbeatRequest,
                            EpfdHeartbeatRequest = new EpfdHeartbeatRequest_()
                        }
                    }
                };
                _appProcces.EnqueMessage(request);
            }
            _alive = new List<ProcessId>();
            StartTimer(_delay);
        }
        private async Task StartTimer(int delay)
        {
            //_logger.LogInfo("Strarting timer.");
            await Task.Delay(delay);
            HandleTimeout();
        }
    }
}
