using System;
using System.Collections.Generic;
using System.Reactive;
using System.Reactive.Linq;
using System.Threading.Tasks;

namespace ConsensusProject.Utils
{
    public static class Extensions
    {
        public static void ForEach<T>(this IEnumerable<T> ie, Action<T> action)
        {
            foreach (var i in ie)
            {
                action(i);
            }
        }

        public static string NewId => Guid.NewGuid().ToString();

        public static IDisposable SubscribeAsync<T>(this IObservable<T> source, Func<Task> asyncAction, Action<Exception> handler = null)
        {
            Func<T, Task<Unit>> wrapped = async t =>
            {
                await asyncAction();
                return Unit.Default;
            };
            if (handler == null)
                return source.SelectMany(wrapped).Subscribe(_ => { });
            else
                return source.SelectMany(wrapped).Subscribe(_ => { }, handler);
        }

        public static IDisposable SubscribeAsync<T>(this IObservable<T> source, Func<T, Task> asyncAction, Action<Exception> handler = null)
        {
            Func<T, Task<Unit>> wrapped = async t =>
            {
                await asyncAction(t);
                return Unit.Default;
            };
            if (handler == null)
                return source.SelectMany(wrapped).Subscribe(_ => { });
            else
                return source.SelectMany(wrapped).Subscribe(_ => { }, handler);
        }
    }
}
