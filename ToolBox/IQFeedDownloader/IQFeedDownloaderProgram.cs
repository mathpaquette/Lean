/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using IQFeed.CSharpApiClient;
using IQFeed.CSharpApiClient.Lookup;
using QuantConnect.Configuration;
using QuantConnect.Data.Market;
using QuantConnect.Logging;
using QuantConnect.ToolBox.IQFeed;
using QuantConnect.Util;
using Environment = System.Environment;

namespace QuantConnect.ToolBox.IQFeedDownloader
{
    /// <summary>
    /// IQFeed Downloader Toolbox Project For LEAN Algorithmic Trading Engine.
    /// </summary>
    public static class IQFeedDownloaderProgram
    {
        private static int NumberOfConcurrentClients = 8;

        /// <summary>
        /// Primary entry point to the program. This program only supports EQUITY for now.
        /// </summary>
        public static void IQFeedDownloader(IList<string> tickers, string resolution, DateTime fromDate, DateTime toDate)
        {
            if (resolution.IsNullOrEmpty() || tickers.IsNullOrEmpty())
            {
                Console.WriteLine("IQFeedDownloader ERROR: '--tickers=' or '--resolution=' parameter is missing");
                Console.WriteLine("--tickers=eg SPY,AAPL");
                Console.WriteLine("--resolution=Tick/Second/Minute/Hour/Daily/All");
                Environment.Exit(1);
            }
            try
            {
                // Load settings from config.json
                var userName = Config.Get("iqfeed-username", "username");
                var password = Config.Get("iqfeed-password", "password");
                var productName = Config.Get("iqfeed-productName", "productname");
                var productVersion = Config.Get("iqfeed-version", "productversion");

                // Create IQFeed downloader instance
                IQFeedLauncher.Start(userName, password, productName, productVersion);
                var lookupClient = LookupClientFactory.CreateNew(NumberOfConcurrentClients);
                lookupClient.Connect();

                var universeProvider = new IQFeedDataQueueUniverseProvider();
                var historyProvider = new IQFeedHistoryProvider(lookupClient, universeProvider, true);
                var downloader = new IQFeedDataDownloader(historyProvider);

                // Process tickers in parallel
                var concurrentTickers = new ConcurrentQueue<string>(tickers);
                var concurrentTasks = new Task[NumberOfConcurrentClients];

                for (var i = 0; i < NumberOfConcurrentClients; i++)
                {
                    concurrentTasks[i] = Task.Run(() => ProcessTicker(concurrentTickers, downloader, resolution, fromDate, toDate));
                }

                // Wait for all tasks to complete
                Task.WaitAll(concurrentTasks);
            }
            catch (Exception err)
            {
                Log.Error(err);
            }
        }

        private static void ProcessTicker(ConcurrentQueue<string> tickers, IQFeedDataDownloader downloader, string resolution, DateTime fromDate, DateTime toDate)
        {
            // Load settings from command line
            var allResolutions = resolution.ToLowerInvariant() == "all";
            var castResolution = allResolutions ? Resolution.Tick : (Resolution)Enum.Parse(typeof(Resolution), resolution);
            var startDate = fromDate.ConvertToUtc(TimeZones.NewYork);
            var endDate = toDate.ConvertToUtc(TimeZones.NewYork);
            endDate = endDate.AddDays(1).AddMilliseconds(-1);

            // Load settings from config.json
            var dataDirectory = Config.Get("data-folder", "../../../Data");

            const string market = Market.USA; // TODO: check other exchanges
            
            string ticker;
            while (tickers.TryDequeue(out ticker))
            {
                // Download the data
                var symbol = Symbol.Create(ticker, SecurityType.Equity, market);
                var data = downloader.Get(symbol, castResolution, startDate, endDate);

                if (allResolutions)
                {
                    var ticks = data.Cast<Tick>().ToList();

                    // Save the data (tick resolution)
                    var writer = new LeanDataWriter(castResolution, symbol, dataDirectory);
                    writer.Write(ticks);

                    // Save the data (other resolutions)
                    foreach (var res in new[] { Resolution.Second, Resolution.Minute, Resolution.Hour, Resolution.Daily })
                    {
                        var resData = IQFeedDataDownloader.AggregateTicks(symbol, ticks, res.ToTimeSpan());

                        writer = new LeanDataWriter(res, symbol, dataDirectory);
                        writer.Write(resData);
                    }
                }
                else
                {
                    // Save the data (single resolution)
                    var writer = new LeanDataWriter(castResolution, symbol, dataDirectory);
                    writer.Write(data);
                }
            }
        }
    }
}
