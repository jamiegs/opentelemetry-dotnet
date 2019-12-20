// <copyright file="MongoEventSubscriber.cs" company="OpenTelemetry Authors">
// Copyright 2018, OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// </copyright>
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Core.Configuration;
using MongoDB.Driver.Core.Events;
using OpenTelemetry;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using OpenTelemetry.Trace.Configuration;
using OpenTelemetry.Trace.Export;

namespace OpenTelemetry.Collector.MongoDB
{
    /// <summary>
    /// Subscribes to mongo events and emits trace spans.
    /// </summary>
    public class MongoEventSubscriber : IEventSubscriber
    {
        private readonly IEventSubscriber subscriber;

        private readonly ITracer tracer;

        /// <summary>
        /// Initializes a new instance of the <see cref="MongoEventSubscriber"/> class.
        /// </summary>
        /// <param name="tracer">tracer to add spans to.</param>
        public MongoEventSubscriber(ITracer tracer)
        {
            this.subscriber = new ReflectionEventSubscriber(this);
            this.tracer = tracer;
        }

        /// <summary>
        /// Returns the event handler for an type of event.
        /// </summary>
        /// <typeparam name="TEvent">Type of event object.</typeparam>
        /// <param name="handler">hander.</param>
        /// <returns>True if an event handler exists for the event type.</returns>
        public bool TryGetEventHandler<TEvent>(out Action<TEvent> handler)
        {
            return this.subscriber.TryGetEventHandler(out handler);
        }

        /// <summary>
        /// Handles CommandSucceededEvent events.
        /// </summary>
        /// <param name="mongoEvent">Event.</param>
        public void Handle(CommandSucceededEvent mongoEvent)
        {
            var currentSpan = this.tracer.CurrentSpan;
            if (currentSpan.IsRecording)
            {
                currentSpan.Status = Status.Ok;
                currentSpan.SetAttribute("Reply", mongoEvent.Reply.ToString());
                currentSpan.SetAttribute("Duration", mongoEvent.Duration.ToString());
                currentSpan.End();

                if (currentSpan is IDisposable disposableSpan)
                {
                    disposableSpan.Dispose();
                }
            }
        }

        /// <summary>
        /// Handles CommandFailedEvent events.
        /// </summary>
        /// <param name="mongoEvent">Event.</param>
        public void Handle(CommandFailedEvent mongoEvent)
        {
            var currentSpan = this.tracer.CurrentSpan;
            if (currentSpan.IsRecording)
            {
                currentSpan.Status = Status.Internal;
                currentSpan.SetAttribute("Failure", mongoEvent.Failure.ToString());
                currentSpan.SetAttribute("Duration", mongoEvent.Duration.ToString());
                currentSpan.End();

                if (currentSpan is IDisposable disposableSpan)
                {
                    disposableSpan.Dispose();
                }
            }
        }

        /// <summary>
        /// Handles CommandStartedEvent events.
        /// </summary>
        /// <param name="mongoEvent">Event.</param>
        public void Handle(CommandStartedEvent mongoEvent)
        {
            if (mongoEvent.CommandName == "getLastError" || mongoEvent.CommandName == "buildInfo" || mongoEvent.CommandName == "isMaster")
            {
                return;
            }

            this.tracer.StartActiveSpan(mongoEvent.CommandName, SpanKind.Client, out var span);

            span.SetAttribute("Command", mongoEvent.Command.ToString());
            span.SetAttribute("ConnectionId", mongoEvent.ConnectionId.ToString());

            span.SetAttribute("DatabaseNamespace", mongoEvent.DatabaseNamespace.ToString());
        }
    }
}
