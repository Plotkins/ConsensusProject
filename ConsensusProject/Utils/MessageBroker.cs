using ConsensusProject.App;
using ConsensusProject.Messages;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;

namespace ConsensusProject.Utils
{
    public class MessageQueue
    {
        private AppLogger _logger;

        public MessageQueue(Func<Message, bool> func, AppLogger logger)
        {
            Func = func;
            Messages = new ConcurrentQueue<Message>();
            new Thread(() => Listen()).Start();
            _logger = logger;
        }

        public void Listen()
        {
            while (true)
            {
                if (Messages.IsEmpty)
                {
                    Thread.Sleep(500);
                }
                else
                {
                    try
                    {
                        if (Messages.TryPeek(out Message message) && Func(message))
                        {
                            Messages.TryDequeue(out _);
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError("Error handling the message: ", ex);
                    }
                }
            }
        }

        public ConcurrentQueue<Message> Messages { get; set; }

        public Func<Message, bool> Func { get; set; }

    }
    public class MessageBroker
    {
        private Dictionary<string, Dictionary<string, MessageQueue>> _topics;
        private Config _config;

        public MessageBroker(Config config)
        {
            _topics = new Dictionary<string, Dictionary<string, MessageQueue>>();
            _config = config;
        }

        public void Subscribe(string group, string queue, Func<Message, bool> func)
        {
            if (!_topics.ContainsKey(group))
            {
                _topics[group] = new Dictionary<string, MessageQueue>();
            }

            var logger = new AppLogger(_config, queue, group);
            _topics[group][queue.ToString()] = new MessageQueue(func, logger);
        }

        public void UnsubscribeQueue(string group, string queue)
        {
            if (_topics.ContainsKey(group) && _topics[group].ContainsKey(queue.ToString()))
            {
                _topics[group].Remove(queue.ToString());
            }
            if (_topics[group].Keys.Count == 0)
            {
                _topics.Remove(group);
            }
        }

        public void UnsubscribeGroup(string group)
        {
            if (_topics.ContainsKey(group))
            {
                _topics.Remove(group);
            }
        }

        public void SendMessage(string group, string queue, Message message)
        {
            if (_topics.ContainsKey(group) && _topics[group].ContainsKey(queue.ToString()))
            {
                _topics[group][queue.ToString()].Messages.Enqueue(message);
            }
        }

        public void SendMessage(Message message) => SendMessage(message.SystemId, message.AbstractionId, message);
    }
}
